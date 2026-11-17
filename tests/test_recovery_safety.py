"""Key fault-path tests for strict replica retirement.

Coverage mapped to the requested fixes:

1. happy-path retirement: confirmed-dead postmaster -> directory removed.
2. THE BUG: stop fails while the postmaster is still alive -> directory is
   preserved, ReplicaStillRunning raised, new recovery rejected with 409 and
   the old healthy replica keeps serving its original restore point.
3. retry without restart: after the stuck process is gone (fault cleared),
   POST /recoveries is accepted again.
4. dead replica: served replica that exits is explicitly taken out of
   service (replica query -> 409) but task state remains queryable.
5. service restart cleanup never deletes a live replica's directory; a
   healthy survivor is re-adopted and still serves its original point;
   after that, switching to another restore point still works.
6. single in-flight rule (physical recovery slot) is preserved.
7. pid-file liveness: stale pid (recycled pid of an unrelated process) is
   not mistaken for a running postmaster.
"""

from __future__ import annotations

import os
import socket
import time

import pytest

from conftest import force_shutdown, wait_for


# --------------------------------------------------------------------------
# 1. happy path: live, stoppable replica is retired and its directory removed
# --------------------------------------------------------------------------

def test_stop_live_replica_removes_directory_only_after_death(env, replica_factory):
    recovery = env.recovery
    work_dir, proc = replica_factory("r1", accept_stop=True)

    assert recovery._replica_running(work_dir) is True
    recovery._stop_replica(work_dir)

    assert proc.wait(timeout=10) is not None  # process really exited
    assert not os.path.exists(work_dir)       # directory removed only after exit


def test_stop_dead_replica_removes_stale_directory(env, replica_factory):
    recovery = env.recovery
    work_dir, proc = replica_factory("r1", accept_stop=True)
    force_shutdown(env.port, proc)
    assert recovery._replica_running(work_dir) is False

    recovery._stop_replica(work_dir)  # must not raise
    assert not os.path.exists(work_dir)


# --------------------------------------------------------------------------
# 2. stop failure + process still alive: the core bug regression test
# --------------------------------------------------------------------------

def test_stop_failure_preserves_directory_and_raises(env, replica_factory):
    recovery = env.recovery
    work_dir, proc = replica_factory("r1", accept_stop=False)

    assert recovery._replica_running(work_dir) is True
    with pytest.raises(recovery.ReplicaStillRunning):
        recovery._stop_replica(work_dir)

    # the old process is still alive ...
    assert proc.poll() is None
    # ... and its directory (including data files) is untouched
    assert os.path.isdir(work_dir)
    assert os.path.exists(os.path.join(work_dir, "KEEP_ME.txt"))
    assert os.path.exists(os.path.join(work_dir, "data", "postmaster.pid"))
    # the fixed port is still owned by the survivor
    probe = socket.socket()
    probe.settimeout(2)
    assert probe.connect_ex((env.config.REPLICA_HOST, env.port)) == 0
    probe.close()


def test_new_recovery_rejected_while_old_postmaster_alive(
    env, replica_factory, ready_restore_point
):
    recovery, state = env.recovery, env.state
    work_dir, proc = replica_factory("r1", accept_stop=False)

    # bookkeeping pretends r1 is the currently served successful replica
    rec_id = state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status, work_dir) "
        "VALUES ('old_point', '0/1800000', 'success', ?)",
        (work_dir,),
    )
    state.kv_set("current_recovery_id", str(rec_id))

    with pytest.raises(recovery.Conflict) as exc:
        recovery.start_recovery("before_delete")
    assert "still alive" in str(exc.value)

    # no new job row was created ...
    rows = state.q("SELECT id FROM recoveries")
    assert [r["id"] for r in rows] == [rec_id]
    # ... the old replica is still served, directory intact ...
    assert state.kv_get("current_recovery_id") == str(rec_id)
    assert state.one("SELECT status FROM recoveries WHERE id=?", (rec_id,))["status"] == "success"
    assert os.path.isdir(work_dir)
    assert proc.poll() is None


def test_demo_stop_fault_switch_blocks_retirement(env, replica_factory):
    recovery, state = env.recovery, env.state
    work_dir, proc = replica_factory("r1", accept_stop=True)
    # even though the real postmaster *would* stop, the demo fault says no
    state.kv_set("fault_stop_failure", "1")
    try:
        with pytest.raises(recovery.ReplicaStillRunning):
            recovery._stop_replica(work_dir)
        assert proc.poll() is None
        assert os.path.isdir(work_dir)
    finally:
        state.kv_set("fault_stop_failure", "")


def test_mixed_stale_dir_and_live_stuck_replica_leaves_bookkeeping_intact(
    env, replica_factory, ready_restore_point
):
    """A dead stale dir may be cleaned, but a simultaneously stuck live
    replica must keep BOTH the service pointer and its success status."""
    recovery, state = env.recovery, env.state

    # 1) the healthy, currently served replica that will refuse to stop
    live_dir, live_proc = replica_factory("r1", accept_stop=False)
    rec_id = state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status, work_dir) "
        "VALUES ('old_point', '0/1800000', 'success', ?)",
        (live_dir,),
    )
    state.kv_set("current_recovery_id", str(rec_id))

    # 2) an unrelated stale directory with no live postmaster (crash leftover)
    stale_dir = os.path.join(env.config.RECOVERY_ROOT, "r0")
    os.makedirs(os.path.join(stale_dir, "data"))

    with pytest.raises(recovery.Conflict):
        recovery.start_recovery("before_delete")

    # the stale dir was safely removed ...
    assert not os.path.exists(stale_dir)
    # ... while the live replica's bookkeeping is completely untouched
    assert state.kv_get("current_recovery_id") == str(rec_id)
    assert state.one("SELECT status FROM recoveries WHERE id=?",
                     (rec_id,))["status"] == "success"
    assert os.path.isdir(live_dir)
    assert live_proc.poll() is None
    # and it remains the served replica at its original restore point
    assert recovery.require_current()["id"] == rec_id


# --------------------------------------------------------------------------
# 3. retry without restart once the stuck process is gone
# --------------------------------------------------------------------------

def test_retry_accepted_after_process_cleared_without_restart(
    env, replica_factory, ready_restore_point, monkeypatch
):
    recovery, state, config = env.recovery, env.state, env.config
    work_dir, proc = replica_factory("r1", accept_stop=False)

    rec_id = state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status, work_dir) "
        "VALUES ('old_point', '0/1800000', 'success', ?)",
        (work_dir,),
    )
    state.kv_set("current_recovery_id", str(rec_id))

    # first attempt is rejected while the old postmaster is alive
    with pytest.raises(recovery.Conflict):
        recovery.start_recovery("before_delete")

    # operator clears the stuck process (the sandbox-equivalent of kill -9);
    # the api process itself is never restarted.
    force_shutdown(env.port, proc)

    # make the new recovery's worker succeed quickly by stubbing the heavy
    # physical-rebuild steps; the retirement gate is what we test here.
    class _FakeResult:
        returncode = 0
        stderr = ""
        stdout = ""

    monkeypatch.setattr(recovery, "_run", lambda *a, **k: _FakeResult())
    # keep the real fake-pg_ctl (correct status exit codes) instead of an
    # always-success stub, so retirement sees the dead postmaster correctly.

    started = {}

    def fake_run_recovery(new_id):
        recovery._owned_jobs.discard(new_id)
        wd = state.one("SELECT work_dir FROM recoveries WHERE id=?", (new_id,))["work_dir"]
        started["work_dir"] = wd
        state.exec(
            "UPDATE recoveries SET status='success' WHERE id=?", (new_id,)
        )
        state.kv_set("current_recovery_id", str(new_id))

    monkeypatch.setattr(recovery, "_run_recovery", fake_run_recovery)

    new_rec = recovery.start_recovery("before_delete")
    assert new_rec["id"] != rec_id
    assert wait_for(lambda: started.get("work_dir") is not None)

    # old job superseded, old directory deleted, new job accepted
    assert state.one("SELECT status FROM recoveries WHERE id=?", (rec_id,))["status"] == "superseded"
    assert not os.path.exists(work_dir)
    assert state.one("SELECT status FROM recoveries WHERE id=?",
                     (new_rec["id"],))["status"] == "success"


# --------------------------------------------------------------------------
# 4. unhealthy replica is explicitly taken out of service; state stays queryable
# --------------------------------------------------------------------------

def test_dead_served_replica_is_unserved_but_state_remains(env, replica_factory):
    recovery, state = env.recovery, env.state
    work_dir, proc = replica_factory("r1", accept_stop=True)
    rec_id = state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status, work_dir) "
        "VALUES ('old_point', '0/1800000', 'success', ?)",
        (work_dir,),
    )
    state.kv_set("current_recovery_id", str(rec_id))

    # healthy while alive: serves exactly its original restore point
    served = recovery.require_current()
    assert served["id"] == rec_id
    assert served["restore_point"] == "old_point"

    force_shutdown(env.port, proc)

    with pytest.raises(recovery.Conflict):
        recovery.require_current()
    # explicit stop of service: pointer gone ...
    assert recovery.current() is None
    # ... but the task row is still queryable and now marked failed
    row = state.one("SELECT * FROM recoveries WHERE id=?", (rec_id,))
    assert row["status"] == "failed"
    assert "not running" in (row["error"] or "")
    # stale directory of the dead replica is cleaned up
    assert not os.path.exists(work_dir)


def test_monitor_marks_unexpected_exit_failed(env, replica_factory):
    recovery, state = env.recovery, env.state
    work_dir, proc = replica_factory("r1", accept_stop=True)
    rec_id = state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status, work_dir) "
        "VALUES ('old_point', '0/1800000', 'success', ?)",
        (work_dir,),
    )
    state.kv_set("current_recovery_id", str(rec_id))

    force_shutdown(env.port, proc)
    recovery.monitor_current()

    assert recovery.current() is None
    assert state.one("SELECT status FROM recoveries WHERE id=?",
                     (rec_id,))["status"] == "failed"


# --------------------------------------------------------------------------
# 5. service restart: never delete a live replica's directory
# --------------------------------------------------------------------------

def test_restart_preserves_and_readopts_healthy_replica(
    env, replica_factory, monkeypatch
):
    recovery, state = env.recovery, env.state
    work_dir, proc = replica_factory("r1", accept_stop=True)
    rec_id = state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status, work_dir) "
        "VALUES ('old_point', '0/1800000', 'success', ?)",
        (work_dir,),
    )

    # replica is already at its target when the api comes back up
    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, *a):
            class _R:
                def fetchone(self_inner):
                    if "replay" in sql:
                        return {"replay": "0/1900000"}
                    return {"n": 42}

            return _R()

    monkeypatch.setattr(env.recovery.db, "replica", lambda: _Conn())

    recovery.cleanup_on_startup()

    # directory untouched, postmaster still alive, replica re-adopted ...
    assert os.path.isdir(work_dir)
    assert os.path.exists(os.path.join(work_dir, "KEEP_ME.txt"))
    assert proc.poll() is None
    current = recovery.current()
    assert current is not None and current["id"] == rec_id
    # ... and it still serves exactly its original restore point
    assert current["restore_point"] == "old_point"
    assert recovery.require_current()["id"] == rec_id


def test_restart_cleans_dead_replica_and_allows_switching(
    env, replica_factory, ready_restore_point
):
    recovery, state = env.recovery, env.state
    work_dir, proc = replica_factory("r1", accept_stop=True)
    rec_id = state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status, work_dir) "
        "VALUES ('old_point', '0/1800000', 'success', ?)",
        (work_dir,),
    )
    state.kv_set("current_recovery_id", str(rec_id))
    force_shutdown(env.port, proc)

    recovery.cleanup_on_startup()

    # dead replica: failed, pointer cleared, directory removed
    assert recovery.current() is None
    assert state.one("SELECT status FROM recoveries WHERE id=?",
                     (rec_id,))["status"] == "failed"
    assert not os.path.exists(work_dir)
    # switching to another restore point is not blocked
    assert not recovery._port_in_use(env.config.REPLICA_HOST, env.port)


def test_restart_never_touches_live_orphan_directory(env, replica_factory):
    recovery, state = env.recovery, env.state
    # directory on disk with NO state row at all
    work_dir, proc = replica_factory("r9", accept_stop=False)

    recovery.cleanup_on_startup()

    assert proc.poll() is None
    assert os.path.isdir(work_dir)
    assert os.path.exists(os.path.join(work_dir, "KEEP_ME.txt"))


def test_restart_resumes_inflight_recovery_to_target(
    env, replica_factory, monkeypatch
):
    """A physical recovery still in flight survives an api restart and is
    tracked to completion rather than duplicated or killed."""
    recovery, state = env.recovery, env.state
    work_dir, proc = replica_factory("r2", accept_stop=True)
    rec_id = state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status, work_dir) "
        "VALUES ('rp', '0/2000000', 'running', ?)",
        (work_dir,),
    )

    # Probe at restart sees replay below target; the resumed watcher then
    # observes one below-target tick before the target is replayed. Replay
    # stays at/above the target on every later tick (the clamped final value
    # covers the watcher polling on after success until the test finishes).
    tick = {"n": 0}
    replay_values = [None, "0/1F00000", "0/2000000"]

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, *a):
            class _R:
                def fetchone(self_inner):
                    if "replay" not in sql:
                        return {"n": 1}
                    idx = min(tick["n"], len(replay_values) - 1)
                    value = replay_values[idx]
                    tick["n"] += 1
                    return {"replay": value}

            return _R()

    monkeypatch.setattr(recovery.db, "replica", lambda: _Conn())

    recovery.cleanup_on_startup()

    # still in flight immediately after restart; the job is not failed
    assert state.one("SELECT status FROM recoveries WHERE id=?",
                     (rec_id,))["status"] == "running"
    assert proc.poll() is None
    assert os.path.isdir(work_dir)

    # the resumed watcher carries it to success and starts serving
    assert wait_for(
        lambda: state.one("SELECT status FROM recoveries WHERE id=?",
                          (rec_id,))["status"] == "success"
    )
    current = recovery.current()
    assert current is not None and current["id"] == rec_id
    assert current["restore_point"] == "rp"


def test_restart_inflight_recovery_whose_replica_dies_is_failed(
    env, replica_factory
):
    recovery, state = env.recovery, env.state
    work_dir, proc = replica_factory("r3", accept_stop=True)
    rec_id = state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status, work_dir) "
        "VALUES ('rp', '0/2000000', 'running', ?)",
        (work_dir,),
    )
    force_shutdown(env.port, proc)

    recovery.cleanup_on_startup()

    assert state.one("SELECT status FROM recoveries WHERE id=?",
                     (rec_id,))["status"] == "failed"
    assert not os.path.exists(work_dir)


# --------------------------------------------------------------------------
# 6. single in-flight rule preserved (physical recovery slot)
# --------------------------------------------------------------------------

def test_single_inflight_recovery(env, ready_restore_point):
    recovery, state = env.recovery, env.state
    state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status) "
        "VALUES ('x', '0/2000000', 'running')"
    )
    with pytest.raises(recovery.Conflict, match="already in flight"):
        recovery.start_recovery("before_delete")


def test_unknown_and_unready_restore_points(env):
    recovery, state = env.recovery, env.state
    state.kv_set("backup_status", "ready")
    with pytest.raises(recovery.NotFound):
        recovery.start_recovery("nope")
    state.exec(
        "INSERT INTO restore_points (name, lsn, wal_file, status) "
        "VALUES ('p', '0/2000000', 'f', 'pending')"
    )
    with pytest.raises(recovery.Conflict, match="not selectable"):
        recovery.start_recovery("p")


# --------------------------------------------------------------------------
# 7. liveness: stale pid file / recycled pid must not look like a postmaster
# --------------------------------------------------------------------------

def test_stale_pidfile_is_not_running(env, tmp_path):
    recovery = env.recovery
    work_dir = os.path.join(env.config.RECOVERY_ROOT, "rdead")
    data = os.path.join(work_dir, "data")
    os.makedirs(data)
    # our own pid is alive but is NOT a postmaster for this data dir
    with open(os.path.join(data, "postmaster.pid"), "w") as fh:
        fh.write(f"{os.getpid()}\n")
    assert recovery._replica_pid(work_dir) is None
    assert recovery._replica_running(work_dir) is False
    # stale directory is therefore safe to remove
    recovery._stop_replica(work_dir)
    assert not os.path.exists(work_dir)


def test_missing_pidfile_is_not_running(env):
    recovery = env.recovery
    work_dir = os.path.join(env.config.RECOVERY_ROOT, "rempty")
    os.makedirs(os.path.join(work_dir, "data"))
    assert recovery._replica_running(work_dir) is False


def test_live_postmaster_is_detected_via_pidfile(env, replica_factory):
    recovery = env.recovery
    work_dir, proc = replica_factory("r1", accept_stop=True)
    pid = recovery._replica_pid(work_dir)
    assert pid == proc.pid
    assert recovery._replica_running(work_dir) is True
