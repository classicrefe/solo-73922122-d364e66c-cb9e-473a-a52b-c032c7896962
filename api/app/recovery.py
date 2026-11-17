"""Point-in-time recovery executor.

Every recovery:
  1. retires any previously served replica. Retirement is *strict*: the old
     postmaster must be confirmed dead before its data directory is removed
     and before a new recovery is accepted. If the old process cannot be
     stopped and is still alive, retirement fails loudly, the old directory is
     left untouched (the live postmaster keeps its files open), and the new
     recovery request is rejected. Because every replica uses the same fixed
     port, accepting a new recovery while an old postmaster is alive would
     guarantee the new one fails to bind -- so that state is unreachable now;
  2. rebuilds a fresh, isolated data directory from the physical base backup;
  3. replays archived WAL up to the named restore point
     (recovery_target_action=pause keeps the replica a read-only standby);
  4. only reports success after the target LSN has actually been replayed AND
     a real query against the traffic table succeeds.

Missing WAL or a dying recovery process => the job is marked failed, the
broken directory is removed, and nothing (not even the previous replica) is
served. After the cause is fixed the operator simply retries -- no api
restart needed.

Service restart never deletes a directory whose postmaster is still alive:
a healthy replica surviving a restart is re-adopted and keeps serving its
original restore point; an unhealthy/absent one is clearly taken out of
service and its directory is cleaned up.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
import threading
import time

from . import config, db, lsn, state

log = logging.getLogger("recovery")

_start_lock = threading.Lock()
# recovery ids whose worker thread belongs to *this* api process; the monitor
# must never touch jobs owned by a live worker (e.g. a recovery still running).
_owned_jobs: set[int] = set()

# Backoff knobs (overridable in tests; production defaults match pg_ctl -t).
PG_CTL_STOP_TIMEOUT = int(os.environ.get("PG_CTL_STOP_TIMEOUT", "60"))
SIGNAL_WAIT_SEC = float(os.environ.get("SIGNAL_WAIT_SEC", "10"))
STOP_RECHECK_SLEEP = float(os.environ.get("STOP_RECHECK_SLEEP", "1"))
# Interval between replay-progress polls while a recovery is in flight.
WATCHER_POLL_INTERVAL = float(os.environ.get("WATCHER_POLL_INTERVAL", "1"))


class Conflict(Exception):
    """Maps to HTTP 409."""


class NotFound(Exception):
    """Maps to HTTP 404."""


class ReplicaStillRunning(Exception):
    """A postmaster could not be stopped; its directory must be preserved."""


# --------------------------------------------------------------------------
# subprocess helpers
# --------------------------------------------------------------------------

def _run(cmd, **kw):
    log.info("exec: %s", " ".join(cmd))
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def _as_replica_user(cmd):
    if config.REPLICA_RUN_AS:
        return ["gosu", config.REPLICA_RUN_AS] + cmd
    return cmd


def _pg_ctl(args, timeout=120):
    return _run(_as_replica_user(["pg_ctl"] + args), timeout=timeout)


def _tail(path, n=4000):
    try:
        with open(path, "r", errors="replace") as fh:
            return fh.read()[-n:]
    except OSError:
        return ""


# --------------------------------------------------------------------------
# replica process helpers
# --------------------------------------------------------------------------

def _data_dir(work_dir):
    return os.path.join(work_dir, "data")


def _read_pidfile(work_dir):
    """Return the postmaster pid recorded in postmaster.pid, or None."""
    pid_file = os.path.join(_data_dir(work_dir), "postmaster.pid")
    try:
        with open(pid_file) as fh:
            first = fh.readline().strip()
        pid = int(first)
        return pid if pid > 0 else None
    except (OSError, ValueError):
        return None


def _pid_is_postmaster(pid, work_dir):
    """True iff pid is alive and is the postmaster for *work_dir*.

    A bare kill(pid, 0) only proves some process holds that pid; pids get
    recycled, so we additionally match the postmaster command line against
    this data directory via /proc (Linux). On platforms without /proc we fall
    back to kill(pid, 0), which is what pg_ctl itself relies on.
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but is owned by another user: treat as alive.
        return True
    except OSError:
        return False
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as fh:
            cmdline = fh.read().replace(b"\x00", b" ").decode(errors="replace")
    except OSError:
        return True  # alive, unable to inspect -- assume it is ours
    # postgres is invoked as "postgres -D <data>" (or -c config_file=...);
    # the data directory path appears in its command line while running.
    return _data_dir(work_dir) in cmdline


def _replica_pid(work_dir):
    """Live postmaster pid serving *work_dir*, or None when not running."""
    pid = _read_pidfile(work_dir)
    if pid is None:
        return None
    return pid if _pid_is_postmaster(pid, work_dir) else None


def _replica_running(work_dir):
    pid = _replica_pid(work_dir)
    if pid is not None:
        return True
    # pg_ctl is only a second opinion when a pid file exists but cannot be
    # interpreted by us (e.g. malformed content). With no pid file at all,
    # nothing can own this data directory -- do not let a stale pg_ctl result
    # (or an unexpected exception) resurrect a dead replica into "running".
    if os.path.exists(os.path.join(_data_dir(work_dir), "postmaster.pid")):
        try:
            return _pg_ctl(
                ["-D", _data_dir(work_dir), "status"], timeout=20
            ).returncode == 0
        except Exception:
            return False
    return False


def _remove_work_dir(work_dir):
    shutil.rmtree(work_dir, ignore_errors=True)
    # rmtree(ignore_errors=True) can silently leave files behind (e.g. a
    # directory still held by another mount); make the outcome observable.
    if os.path.exists(work_dir):
        log.error("work directory %s could not be fully removed", work_dir)
        return False
    log.info("removed replica work directory %s", work_dir)
    return True


def _stop_replica(work_dir):
    """Stop the replica at *work_dir* and delete its directory.

    Safety contract (this is the fix for the retirement bug):
      * if no live postmaster owns the directory, it is removed directly;
      * otherwise pg_ctl fast/immediate stop is issued and the process is
        re-checked; the directory is removed ONLY after the postmaster is
        confirmed gone;
      * if every stop attempt leaves a live postmaster, ReplicaStillRunning
        is raised and the directory is preserved -- a running postmaster
        needs its files, and deleting them would corrupt a later retry.
    """
    if not os.path.exists(work_dir):
        return
    if not _replica_running(work_dir):
        log.info("no live postmaster for %s; removing stale directory", work_dir)
        _remove_work_dir(work_dir)
        return

    # Demo-only fault injection: emulate "pg_ctl stop failed and the process
    # is still alive". The live postmaster below makes every liveness check
    # fail, so the directory must be preserved and the new recovery rejected.
    if state.kv_get("fault_stop_failure"):
        log.warning("DEMO FAULT: simulating stop failure for %s", work_dir)
        raise ReplicaStillRunning(
            f"[demo fault] simulated stop failure: old replica postmaster at "
            f"{work_dir} is still alive; its directory was preserved and no "
            f"new recovery was started. Clear the fault with "
            f"PUT /demo/faults and retry."
        )

    data = _data_dir(work_dir)
    wait = str(PG_CTL_STOP_TIMEOUT)
    attempts = [
        (["-D", data, "-m", "fast", "-w", "-t", wait, "stop"],
         PG_CTL_STOP_TIMEOUT + 30),
        (["-D", data, "-m", "immediate", "-w", "-t", wait, "stop"],
         PG_CTL_STOP_TIMEOUT + 30),
    ]
    for args, timeout in attempts:
        try:
            result = _pg_ctl(args, timeout=timeout)
            if result.returncode != 0:
                log.warning(
                    "pg_ctl stop failed (rc=%s) for %s: %s",
                    result.returncode, work_dir, (result.stderr or "").strip()[-500:],
                )
        except Exception:
            log.exception("pg_ctl stop raised for %s", work_dir)
        # Re-check liveness rather than trusting pg_ctl's exit code.
        if not _replica_running(work_dir):
            _remove_work_dir(work_dir)
            return
        time.sleep(STOP_RECHECK_SLEEP)

    # Last-resort direct signal; some hardened environments make pg_ctl fail
    # while a plain kill from the same uid still works.
    pid = _replica_pid(work_dir)
    if pid is not None:
        for sig in (signal.SIGTERM, signal.SIGQUIT, signal.SIGINT):
            try:
                os.kill(pid, sig)
            except OSError as exc:
                log.warning("direct kill(%s, %s) failed: %s", pid, sig, exc)
            deadline = time.time() + SIGNAL_WAIT_SEC
            while time.time() < deadline:
                if not _replica_running(work_dir):
                    _remove_work_dir(work_dir)
                    return
                time.sleep(0.5)

    if _replica_running(work_dir):
        pid = _replica_pid(work_dir)
        raise ReplicaStillRunning(
            f"old replica postmaster at {work_dir} is still alive after fast "
            f"and immediate stop attempts"
            + (f" (pid={pid})" if pid else " (pid unknown; check postmaster.pid)")
            + "; its directory was preserved and no new recovery was started. "
            "Fix the stuck process and retry -- no service restart required."
        )
    _remove_work_dir(work_dir)


# --------------------------------------------------------------------------
# "currently served replica" pointer
# --------------------------------------------------------------------------

def current():
    row = state.one("SELECT value FROM kv WHERE key = 'current_recovery_id'")
    if not row:
        return None
    return state.one(
        "SELECT * FROM recoveries WHERE id = ? AND status = 'success'", (row["value"],)
    )


def _fail_current(message):
    row = state.one("SELECT value FROM kv WHERE key = 'current_recovery_id'")
    if not row:
        return
    rec_id = row["value"]
    state.exec(
        "UPDATE recoveries SET status='failed', error=?, finished_at=datetime('now') "
        "WHERE id = ? AND status = 'success'",
        (message, rec_id),
    )
    state.exec("DELETE FROM kv WHERE key = 'current_recovery_id'")
    rec = state.one("SELECT work_dir FROM recoveries WHERE id = ?", (rec_id,))
    if rec and rec["work_dir"]:
        # A dead replica is safe to remove; a live one must keep its files.
        try:
            _stop_replica(rec["work_dir"])
        except ReplicaStillRunning:
            log.error(
                "failed recovery %s still has a live postmaster; "
                "directory preserved", rec_id,
            )


def require_current():
    rec = current()
    if not rec:
        raise Conflict("no recovered replica is currently served; start a recovery first")
    if not _replica_running(rec["work_dir"]):
        # Confirmed dead: take it out of service explicitly and clean up.
        _fail_current("replica postgres is not running anymore")
        raise Conflict(
            "the recovered replica exited and is no longer served; "
            "its recovery has been marked failed"
        )
    return rec


def monitor_current():
    """Background watchdog: a served replica that exits must become a failure.

    Also resumes tracking of an in-flight recovery whose worker was killed by
    an api restart while its replica kept running (single replica port means
    at most one such orphan exists).
    """
    rec = current()
    if rec and not _replica_running(rec["work_dir"]):
        log.error("replica for recovery %s exited unexpectedly", rec["id"])
        _fail_current("replica postgres exited unexpectedly")
        return

    orphan = state.one(
        "SELECT * FROM recoveries WHERE status = 'running' "
        "AND work_dir IS NOT NULL ORDER BY id DESC"
    )
    if orphan and orphan["id"] not in _owned_jobs:
        _adopt_or_resume(orphan)


def _adopt_or_resume(rec):
    """Decide the fate of a 'running' recovery left by a previous api life."""
    work_dir = rec["work_dir"]
    if not _replica_running(work_dir):
        # Nothing alive: the interrupted job is a plain failure. Clean its
        # directory (if any leftover files remain) and free the single slot.
        log.warning("recovery %s did not survive restart -> failed", rec["id"])
        state.exec(
            "UPDATE recoveries SET status='failed', "
            "error=COALESCE(error, 'interrupted by api restart; replica not running'), "
            "finished_at=datetime('now') WHERE id=?",
            (rec["id"],),
        )
        try:
            _stop_replica(work_dir)
        except ReplicaStillRunning:  # race: liveness changed under us
            pass
        return

    # A live postmaster survived the restart. If it already replayed the
    # target, the replica is healthy: adopt it and keep serving exactly the
    # restore point it was built for. Otherwise keep tracking it to the
    # target instead of spawning a competing replica on the same port.
    target = lsn.parse(rec["target_lsn"])
    try:
        with db.replica() as conn:
            row = conn.execute(
                "SELECT pg_last_wal_replay_lsn()::text AS replay"
            ).fetchone()
            replay = row["replay"] if row else None
            if replay and lsn.parse(replay) >= target:
                conn.execute(
                    f"SELECT count(*) FROM {config.TRAFFIC_TABLE}"
                ).fetchone()
    except Exception:
        replay = None

    if replay and lsn.parse(replay) >= target:
        state.exec(
            "UPDATE recoveries SET status='success', replay_lsn=?, "
            "finished_at=COALESCE(finished_at, datetime('now')) WHERE id=?",
            (replay, rec["id"]),
        )
        state.kv_set("current_recovery_id", str(rec["id"]))
        log.info(
            "adopted surviving replica of recovery %s at restore point %r "
            "(replay %s >= target %s)",
            rec["id"], rec["restore_point"], replay, rec["target_lsn"],
        )
        return

    log.info(
        "resuming wait for surviving recovery %s (replay=%s target=%s)",
        rec["id"], replay, rec["target_lsn"],
    )
    _owned_jobs.add(rec["id"])

    def _watch():
        try:
            deadline = time.time() + config.RECOVERY_TIMEOUT_SEC
            while time.time() < deadline:
                if not _replica_running(work_dir):
                    state.exec(
                        "UPDATE recoveries SET status='failed', "
                        "error='replica postgres exited after api restart', "
                        "finished_at=datetime('now') WHERE id=?",
                        (rec["id"],),
                    )
                    try:
                        _stop_replica(work_dir)
                    except ReplicaStillRunning:
                        pass
                    return
                try:
                    with db.replica() as conn:
                        row = conn.execute(
                            "SELECT pg_last_wal_replay_lsn()::text AS replay"
                        ).fetchone()
                        cur = row["replay"] if row else None
                        if cur and lsn.parse(cur) >= target:
                            conn.execute(
                                f"SELECT count(*) FROM {config.TRAFFIC_TABLE}"
                            ).fetchone()
                            state.exec(
                                "UPDATE recoveries SET status='success', replay_lsn=?, "
                                "finished_at=datetime('now') WHERE id=?",
                                (cur, rec["id"]),
                            )
                            state.kv_set("current_recovery_id", str(rec["id"]))
                            return
                except Exception:
                    pass
                time.sleep(WATCHER_POLL_INTERVAL)
            state.exec(
                "UPDATE recoveries SET status='failed', "
                "error=COALESCE(error, 'timed out after api restart waiting for target'), "
                "finished_at=datetime('now') WHERE id=?",
                (rec["id"],),
            )
        finally:
            _owned_jobs.discard(rec["id"])

    threading.Thread(
        target=_watch, daemon=True, name=f"recovery-resume-{rec['id']}"
    ).start()


# --------------------------------------------------------------------------
# restore point readiness (WAL archive completeness)
# --------------------------------------------------------------------------

def refresh_restore_points():
    backup_end = state.kv_get("backup_end_lsn")
    if not backup_end:
        return
    start = lsn.parse(backup_end)
    backup_start = state.kv_get("backup_start_lsn")
    for point in state.q("SELECT * FROM restore_points WHERE status = 'pending'"):
        # a point at/before the backup's own redo point can never be
        # recovered from this backup (it already contains newer state)
        if backup_start and lsn.parse(point["lsn"]) <= lsn.parse(backup_start):
            state.exec(
                "UPDATE restore_points SET status='expired' WHERE id=?", (point["id"],)
            )
            log.info("restore point %s predates the base backup -> expired", point["name"])
            continue
        try:
            needed = lsn.wal_files_between(start, lsn.parse(point["lsn"]))
        except ValueError:
            continue
        missing = [
            f for f in needed
            if not os.path.exists(os.path.join(config.WAL_ARCHIVE_DIR, f))
        ]
        if missing:
            log.info(
                "restore point %s: waiting for %d WAL file(s), e.g. %s",
                point["name"], len(missing), missing[0],
            )
        else:
            state.exec("UPDATE restore_points SET status='ready' WHERE id=?", (point["id"],))
            log.info("restore point %s is ready (WAL archive complete)", point["name"])


# --------------------------------------------------------------------------
# recovery lifecycle
# --------------------------------------------------------------------------

def _known_work_dirs():
    """Every replica work directory referenced by state or present on disk."""
    dirs = set()
    for rec in state.q(
        "SELECT DISTINCT work_dir FROM recoveries "
        "WHERE work_dir IS NOT NULL AND work_dir != ''"
    ):
        dirs.add(rec["work_dir"])
    if os.path.isdir(config.RECOVERY_ROOT):
        for entry in os.listdir(config.RECOVERY_ROOT):
            full = os.path.join(config.RECOVERY_ROOT, entry)
            if os.path.isdir(full):
                dirs.add(full)
    return dirs


def _retire_all_replicas():
    """Strictly retire every replica before a new recovery may start.

    Ordering matters:
      * a healthy served replica keeps serving its original restore point
        until retirement is proven successful -- the service pointer and the
        job's success status are only changed AFTER every postmaster is
        confirmed dead and every directory removed;
      * if ANY postmaster survives, ReplicaStillRunning propagates out with
        zero bookkeeping changes: no new recovery is accepted, and the live
        (still queryable) old replica keeps serving its own restore point.
    """
    survivors = []
    stopped_dirs = []
    for work_dir in sorted(_known_work_dirs()):
        try:
            _stop_replica(work_dir)
        except ReplicaStillRunning as exc:
            survivors.append(str(exc))
            continue
        stopped_dirs.append(work_dir)

    if survivors:
        # Touch nothing: the healthy old replica must remain fully served
        # (pointer + success status intact) while a stuck one blocks switching.
        raise ReplicaStillRunning(
            "cannot accept a new recovery while old replica postmaster(s) "
            "are still running: " + " | ".join(survivors)
        )

    # All postmasters confirmed dead, all directories gone: retire the
    # bookkeeping of every previously served replica in one final step.
    for work_dir in stopped_dirs:
        rec = state.one(
            "SELECT id FROM recoveries WHERE work_dir=? AND status='success'",
            (work_dir,),
        )
        if rec:
            state.exec(
                "UPDATE recoveries SET status='superseded', "
                "finished_at=COALESCE(finished_at, datetime('now')) WHERE id=?",
                (rec["id"],),
            )
    state.exec("DELETE FROM kv WHERE key='current_recovery_id'")


def start_recovery(point_name):
    with _start_lock:
        if state.one("SELECT id FROM recoveries WHERE status = 'running'"):
            raise Conflict("another recovery is already in flight")
        point = state.one("SELECT * FROM restore_points WHERE name=?", (point_name,))
        if not point:
            raise NotFound(f"restore point {point_name!r} does not exist")
        refresh_restore_points()
        point = state.one("SELECT * FROM restore_points WHERE name=?", (point_name,))
        if point["status"] != "ready":
            raise Conflict(
                f"restore point {point_name!r} is not selectable yet: "
                f"its WAL archive is incomplete (status={point['status']})"
            )
        if state.kv_get("backup_status") != "ready":
            raise Conflict("the startup base backup is not ready yet")

        # Strict retirement gate: only proceed once every old postmaster is
        # confirmed dead and every old directory is gone. A live old replica
        # rejects this request and keeps serving its own restore point.
        try:
            _retire_all_replicas()
        except ReplicaStillRunning as exc:
            log.error("new recovery rejected: %s", exc)
            raise Conflict(str(exc)) from exc
        # Defence in depth: the fixed port must be free for the new replica.
        if _port_in_use(config.REPLICA_HOST, config.REPLICA_PORT):
            raise Conflict(
                f"replica port {config.REPLICA_HOST}:{config.REPLICA_PORT} is "
                f"still occupied; refusing to start a recovery that could not "
                f"bind. Free the port and retry."
            )

        rec_id = state.exec(
            "INSERT INTO recoveries (restore_point, target_lsn) VALUES (?, ?)",
            (point_name, point["lsn"]),
        )
        work_dir = os.path.join(config.RECOVERY_ROOT, f"r{rec_id}")
        state.exec("UPDATE recoveries SET work_dir=? WHERE id=?", (work_dir, rec_id))
        _owned_jobs.add(rec_id)
        threading.Thread(
            target=_run_recovery, args=(rec_id,), daemon=True, name=f"recovery-{rec_id}"
        ).start()
        return state.one("SELECT * FROM recoveries WHERE id=?", (rec_id,))


def _port_in_use(host, port):
    import socket

    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.settimeout(2)
    try:
        return probe.connect_ex((host, port)) == 0
    except OSError:
        return False
    finally:
        probe.close()


def _run_recovery(rec_id):
    rec = state.one("SELECT * FROM recoveries WHERE id=?", (rec_id,))
    work_dir = rec["work_dir"]
    data = _data_dir(work_dir)
    log_file = os.path.join(work_dir, "replica.log")
    try:
        # 1. fresh isolated copy of the physical base backup
        os.makedirs(data)
        result = _run(["cp", "-a", config.BACKUP_DIR + "/.", data], timeout=900)
        if result.returncode != 0:
            raise RuntimeError(f"copying base backup failed: {result.stderr[-1000:]}")
        os.chmod(data, 0o700)

        # 2. PITR configuration: replay archived WAL up to the named point
        with open(os.path.join(data, "postgresql.auto.conf"), "a") as fh:
            fh.write(
                "\n# ---- recovery-api ----\n"
                f"port = {config.REPLICA_PORT}\n"
                f"listen_addresses = '{config.REPLICA_HOST}'\n"
                "unix_socket_directories = '/tmp'\n"
                "archive_mode = off\n"
                "hot_standby = on\n"
                f"restore_command = 'cp {config.WAL_ARCHIVE_DIR}/%f %p'\n"
                f"recovery_target_name = '{rec['restore_point']}'\n"
                "recovery_target_action = 'pause'\n"
            )
        open(os.path.join(data, "recovery.signal"), "w").close()
        if config.REPLICA_RUN_AS:
            user = config.REPLICA_RUN_AS
            _run(["chown", "-R", f"{user}:{user}", work_dir])

        # Demo-only fault injection: emulate a postmaster start failure
        # (one-shot). The job fails, its directory is cleaned up by the
        # except branch, and retrying after clearing the switch succeeds
        # without any api restart.
        if state.kv_get("fault_start_failure"):
            state.kv_set("fault_start_failure", "")
            log.warning("DEMO FAULT: simulating replica start failure for %s", work_dir)
            raise RuntimeError(
                "[demo fault] simulated replica postgres start failure "
                "(e.g. missing WAL / bind error). Clear the fault with "
                "PUT /demo/faults and POST /recoveries again to retry."
            )

        # 3. start the isolated replica
        result = _pg_ctl(["-D", data, "-l", log_file, "-w", "-t", "90", "start"], timeout=120)
        if result.returncode != 0:
            raise RuntimeError(
                "replica postgres failed to start (missing WAL?): "
                + (_tail(log_file, 1500) or (result.stdout + result.stderr)[-500:])
            )

        # 4. wait until the target is really replayed and the data queryable
        target = lsn.parse(rec["target_lsn"])
        deadline = time.time() + config.RECOVERY_TIMEOUT_SEC
        while time.time() < deadline:
            if not _replica_running(work_dir):
                raise RuntimeError(
                    "replica postgres exited before reaching the recovery target: "
                    + _tail(log_file, 1500)
                )
            replay = None
            try:
                with db.replica() as conn:
                    row = conn.execute(
                        "SELECT pg_last_wal_replay_lsn()::text AS replay"
                    ).fetchone()
                    replay = row["replay"] if row else None
                    if replay and lsn.parse(replay) >= target:
                        # prove the replica is actually queryable before success
                        conn.execute(
                            f"SELECT count(*) FROM {config.TRAFFIC_TABLE}"
                        ).fetchone()
            except Exception:
                replay = None
            if replay and lsn.parse(replay) >= target:
                state.exec(
                    "UPDATE recoveries SET status='success', replay_lsn=?, "
                    "finished_at=datetime('now') WHERE id=?",
                    (replay, rec_id),
                )
                state.kv_set("current_recovery_id", str(rec_id))
                log.info(
                    "recovery %s reached target %s (replay %s)",
                    rec_id, rec["target_lsn"], replay,
                )
                return
            time.sleep(WATCHER_POLL_INTERVAL)
        raise RuntimeError(
            f"timed out after {config.RECOVERY_TIMEOUT_SEC}s waiting to reach "
            f"recovery target {rec['target_lsn']}: " + _tail(log_file, 1500)
        )
    except Exception as exc:  # noqa: BLE001 - recorded on the job
        message = str(exc)[:3000]
        log.error("recovery %s failed: %s", rec_id, message)
        state.exec(
            "UPDATE recoveries SET status='failed', error=?, finished_at=datetime('now') "
            "WHERE id=?",
            (message, rec_id),
        )
        try:
            _stop_replica(work_dir)
        except ReplicaStillRunning:
            # Extremely unlikely for a brand-new failed replica, but never
            # delete a live postmaster's directory.
            log.error(
                "failed recovery %s postmaster still alive; directory preserved",
                rec_id,
            )
    finally:
        _owned_jobs.discard(rec_id)


def cleanup_on_startup():
    """Reconcile leftover state without ever deleting a live replica's files.

    * healthy replica (live + already at its target): re-adopted, keeps
      serving the original restore point -- nothing is deleted;
    * live replica still short of its target: tracked to completion so the
      operator can still switch restore points afterwards;
    * dead/absent replica: job marked failed, service pointer cleared, stale
      directory removed; recovery point switching remains available.
    """
    state.exec("DELETE FROM kv WHERE key = 'current_recovery_id'")
    leftovers = state.q(
        "SELECT * FROM recoveries WHERE status IN ('running','success')"
    )
    live_jobs = []
    for rec in leftovers:
        work_dir = rec["work_dir"] or ""
        if work_dir and _replica_running(work_dir):
            live_jobs.append(rec)
            continue
        state.exec(
            "UPDATE recoveries SET status='failed', "
            "error=COALESCE(error, 'interrupted by api restart'), "
            "finished_at=datetime('now') WHERE id=?",
            (rec["id"],),
        )
        if work_dir:
            try:
                _stop_replica(work_dir)
            except ReplicaStillRunning:
                live_jobs.append(rec)

    # Also sweep directories with no state row (crashes before INSERT/update).
    if os.path.isdir(config.RECOVERY_ROOT):
        known = {r["work_dir"] for r in leftovers if r["work_dir"]}
        for entry in os.listdir(config.RECOVERY_ROOT):
            full = os.path.join(config.RECOVERY_ROOT, entry)
            if full in known or not os.path.isdir(full):
                continue
            try:
                _stop_replica(full)
            except ReplicaStillRunning:
                log.error(
                    "orphan live replica at %s survives restart; "
                    "directory preserved", full,
                )

    # At most one live replica can exist (single fixed port). Adopt/resume
    # each survivor; adoption restores the service pointer for healthy ones.
    for rec in live_jobs:
        _adopt_or_resume(rec)
