"""HTTP-level tests: 409 mapping, fault switches, status queryability."""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from conftest import force_shutdown, wait_for


@pytest.fixture()
def client(env, monkeypatch):
    # prevent lifespan background threads from doing real work
    from app import backup, recovery

    monkeypatch.setattr(backup, "run_base_backup", lambda *a, **k: None)
    monkeypatch.setattr(recovery, "refresh_restore_points", lambda *a, **k: None)
    monkeypatch.setattr(recovery, "monitor_current", lambda *a, **k: None)
    monkeypatch.setattr(recovery, "cleanup_on_startup", lambda *a, **k: None)

    from app.main import app

    with TestClient(app) as c:
        yield c


def test_health_and_docs(client):
    assert client.get("/health").json()["status"] in ("ok", "degraded")
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").status_code == 200
    redir = client.get("/", follow_redirects=False)
    assert redir.status_code in (302, 307)
    assert redir.headers["location"] == "/docs"


def test_fault_switch_roundtrip(client):
    r = client.put(
        "/demo/faults",
        json={"simulate_stop_failure": True, "simulate_start_failure": False},
    )
    assert r.status_code == 200
    assert r.json()["simulate_stop_failure"] is True
    r = client.get("/demo/faults")
    assert r.json()["simulate_stop_failure"] is True
    assert r.json()["simulate_start_failure"] is False
    client.put("/demo/faults", json={})
    assert client.get("/demo/faults").json()["simulate_stop_failure"] is False


def test_recovery_rejected_with_409_when_old_postmaster_alive(
    client, env, replica_factory, ready_restore_point
):
    recovery, state = env.recovery, env.state
    work_dir, proc = replica_factory("r1", accept_stop=False)
    rec_id = state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status, work_dir) "
        "VALUES ('old_point', '0/1800000', 'success', ?)",
        (work_dir,),
    )
    state.kv_set("current_recovery_id", str(rec_id))

    r = client.post("/recoveries", json={"restore_point": "before_delete"})
    assert r.status_code == 409
    assert "still alive" in r.json()["detail"]

    # old replica remains served and its task stays queryable
    assert client.get(f"/recoveries/{rec_id}").json()["status"] == "success"
    assert os.path.isdir(work_dir)
    assert proc.poll() is None

    # replica query path still resolves to the old replica (health check only;
    # no real postgres wire protocol exists in the unit test)
    cur = client.get("/recoveries/current")
    assert cur.status_code == 200
    assert cur.json()["id"] == rec_id
    assert cur.json()["restore_point"] == "old_point"


def test_recovery_404_unknown_point(client, env):
    env.state.kv_set("backup_status", "ready")
    r = client.post("/recoveries", json={"restore_point": "ghost"})
    assert r.status_code == 404


def test_unready_point_409(client, env):
    env.state.kv_set("backup_status", "ready")
    env.state.exec(
        "INSERT INTO restore_points (name, lsn, wal_file, status) "
        "VALUES ('p', '0/2000000', 'f', 'pending')"
    )
    r = client.post("/recoveries", json={"restore_point": "p"})
    assert r.status_code == 409
    assert "not selectable" in r.json()["detail"]


def test_replica_traffic_409_when_none_served(client):
    r = client.get("/replica/traffic")
    assert r.status_code == 409


def test_retry_flow_over_http_after_failure(
    client, env, replica_factory, ready_restore_point, monkeypatch
):
    recovery, state, config = env.recovery, env.state, env.config
    work_dir, proc = replica_factory("r1", accept_stop=False)
    rec_id = state.exec(
        "INSERT INTO recoveries (restore_point, target_lsn, status, work_dir) "
        "VALUES ('old_point', '0/1800000', 'success', ?)",
        (work_dir,),
    )
    state.kv_set("current_recovery_id", str(rec_id))

    # 1) blocked while the old postmaster is stuck
    blocked = client.post("/recoveries", json={"restore_point": "before_delete"})
    assert blocked.status_code == 409

    # 2) operator clears the stuck process (no api restart)
    force_shutdown(env.port, proc)

    class _FakeResult:
        returncode = 0
        stderr = ""
        stdout = ""

    monkeypatch.setattr(recovery, "_run", lambda *a, **k: _FakeResult())
    # note: the real (fake) pg_ctl on PATH stays in place so liveness/status
    # semantics match production; only cp/chown subprocess calls are stubbed.

    def fake_run_recovery(new_id):
        recovery._owned_jobs.discard(new_id)
        state.exec(
            "UPDATE recoveries SET status='success' WHERE id=?", (new_id,)
        )

    monkeypatch.setattr(recovery, "_run_recovery", fake_run_recovery)

    # 3) retry accepted, new job created, old one superseded
    r = client.post("/recoveries", json={"restore_point": "before_delete"})
    assert r.status_code == 202
    new_id = r.json()["id"]
    assert new_id != rec_id
    assert wait_for(
        lambda: client.get(f"/recoveries/{new_id}").json()["status"] == "success"
    )
    assert client.get(f"/recoveries/{rec_id}").json()["status"] == "superseded"
    assert not os.path.exists(work_dir)

    # task history remains queryable
    ids = [x["id"] for x in client.get("/recoveries").json()["items"]]
    assert set(ids) >= {rec_id, new_id}


def test_start_failure_injection_marks_job_failed(
    client, env, ready_restore_point, monkeypatch
):
    recovery, state = env.recovery, env.state
    env.state.kv_set("fault_start_failure", "1")

    class _FakeResult:
        returncode = 0
        stderr = ""
        stdout = ""

    monkeypatch.setattr(recovery, "_run", lambda *a, **k: _FakeResult())
    # keep the real fake-pg_ctl (correct status exit codes) instead of an
    # always-success stub, so post-failure cleanup behaves like production.

    r = client.post("/recoveries", json={"restore_point": "before_delete"})
    assert r.status_code == 202
    rec_id = r.json()["id"]
    assert wait_for(
        lambda: client.get(f"/recoveries/{rec_id}").json()["status"] == "failed"
    )
    detail = client.get(f"/recoveries/{rec_id}").json()
    assert "demo fault" in (detail["error"] or "")
    # one-shot switch consumed
    assert state.kv_get("fault_start_failure") in (None, "")
    # failed job's work directory is cleaned up
    wd = detail["work_dir"]
    assert not os.path.exists(wd)
    # no replica is served
    assert client.get("/replica/traffic").status_code == 409
