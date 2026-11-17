"""Traffic data-warehouse PITR recovery API.

Scenario: an operator accidentally deleted intersection minute counts from
the source database. The primary keeps collecting; this service rebuilds an
isolated read-only replica from the startup physical base backup + archived
WAL, up to a user-created named restore point, so the deleted rows can be
queried and back-filled.

Demo entry point: run the stack, then open /docs (Swagger UI).
"""

from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from . import backup, config, db, lsn, recovery, state

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("api")

NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"

# Background loops are stoppable so lifespan shutdown (and test teardown)
# cannot leave daemon threads acting on state that outlives the app instance.
_bg_stop = threading.Event()
_bg_threads: list[threading.Thread] = []


def _readiness_loop():
    while not _bg_stop.is_set():
        try:
            recovery.refresh_restore_points()
        except Exception:
            log.exception("readiness refresh failed")
        _bg_stop.wait(2)


def _monitor_loop():
    while not _bg_stop.is_set():
        try:
            recovery.monitor_current()
        except Exception:
            log.exception("replica monitor failed")
        _bg_stop.wait(3)


@asynccontextmanager
async def lifespan(_app):
    os.makedirs(config.RECOVERY_ROOT, exist_ok=True)
    os.makedirs(os.path.dirname(config.STATE_DB), exist_ok=True)
    state.init()
    recovery.cleanup_on_startup()
    _bg_stop.clear()
    _bg_threads.clear()
    # physical base backup is established at startup, in the background
    targets = [
        (backup.run_base_backup, "base-backup"),
        (_readiness_loop, "readiness"),
        (_monitor_loop, "monitor"),
    ]
    for target, name in targets:
        thread = threading.Thread(target=target, daemon=True, name=name)
        thread.start()
        _bg_threads.append(thread)
    yield
    _bg_stop.set()
    for thread in _bg_threads:
        thread.join(timeout=5)
    _bg_threads.clear()


app = FastAPI(
    title="Traffic PITR Recovery API",
    version="1.1.0",
    description=(
        "Physical point-in-time recovery for the traffic_minute_counts table. "
        "Flow: 1) POST /demo/generate to simulate collection, "
        "2) POST /restore-points to create a named restore point, "
        "3) POST /demo/delete to simulate the accidental deletion, "
        "4) POST /recoveries to rebuild an isolated read-only replica at the "
        "restore point, 5) GET /replica/traffic to fetch the lost rows. "
        "The primary keeps collecting the whole time.\n\n"
        "Switching restore points strictly retires the old replica first: its "
        "postmaster must be confirmed dead before its directory is removed or a "
        "new recovery is accepted. If the old process cannot be stopped, the new "
        "request is rejected with 409 and the healthy old replica keeps serving "
        "its original restore point. Use POST /demo/faults to exercise the "
        "stop-failure protection and the fail-then-retry flow from this UI."
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


# --------------------------------------------------------------------------
# system
# --------------------------------------------------------------------------

@app.get("/health", tags=["system"])
def health():
    try:
        with db.primary() as conn:
            conn.execute("SELECT 1")
        primary_ok = True
    except Exception:
        primary_ok = False
    return {"status": "ok" if primary_ok else "degraded", "primary_reachable": primary_ok}


@app.get("/backup", tags=["system"])
def backup_status():
    """Status of the physical base backup taken at startup."""
    return {
        "status": state.kv_get("backup_status", "unknown"),
        "end_lsn": state.kv_get("backup_end_lsn"),
        "completed_at": state.kv_get("backup_completed_at"),
        "error": state.kv_get("backup_error") or None,
        "backup_dir": config.BACKUP_DIR,
    }


# --------------------------------------------------------------------------
# restore points
# --------------------------------------------------------------------------

@app.post("/restore-points", status_code=201, tags=["restore-points"])
def create_restore_point(name: str = Query(..., pattern=NAME_PATTERN)):
    """Create a named restore point on the primary (pg_create_restore_point).

    The point becomes selectable for recovery only after every WAL segment
    between the base backup and the point has shown up in the archive.
    """
    if state.kv_get("backup_status") != "ready":
        raise HTTPException(
            409, "base backup not ready yet; restore points must be created after it"
        )
    if state.one("SELECT id FROM restore_points WHERE name=?", (name,)):
        raise HTTPException(409, f"restore point {name!r} already exists")
    with db.primary() as conn:
        point_lsn = conn.execute(
            "SELECT pg_create_restore_point(%s)::text", (name,)
        ).fetchone()[0]
        # force the segment holding the point to be archived promptly
        conn.execute("SELECT pg_switch_wal()")
    wal_file = lsn.wal_file(lsn.parse(point_lsn))
    point_id = state.exec(
        "INSERT INTO restore_points (name, lsn, wal_file) VALUES (?, ?, ?)",
        (name, point_lsn, wal_file),
    )
    recovery.refresh_restore_points()
    return state.one("SELECT * FROM restore_points WHERE id=?", (point_id,))


@app.get("/restore-points", tags=["restore-points"])
def list_restore_points():
    recovery.refresh_restore_points()
    return {"items": state.q("SELECT * FROM restore_points ORDER BY id")}


# --------------------------------------------------------------------------
# recoveries
# --------------------------------------------------------------------------

class RecoveryRequest(BaseModel):
    restore_point: str = Field(..., pattern=NAME_PATTERN)


@app.post("/recoveries", status_code=202, tags=["recoveries"])
def start_recovery(req: RecoveryRequest):
    """Start a recovery to a named restore point (only one may be in flight).

    Rebuilds a fresh isolated replica from the physical base backup. Any
    previously served replica is retired *strictly* first: the old
    postmaster is confirmed dead and its directory removed before the new
    recovery is accepted. If the old postmaster cannot be stopped (still
    alive after fast/immediate shutdown), this returns 409, the old
    directory is preserved, and the healthy old replica keeps serving its
    original restore point. Retry the same request after the stuck process
    is fixed -- no service restart needed.
    """
    try:
        return recovery.start_recovery(req.restore_point)
    except recovery.NotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except recovery.Conflict as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/recoveries", tags=["recoveries"])
def list_recoveries():
    return {"items": state.q("SELECT * FROM recoveries ORDER BY id DESC")}


@app.get("/recoveries/current", tags=["recoveries"])
def current_recovery():
    try:
        return recovery.require_current()
    except recovery.Conflict as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/recoveries/{rec_id}", tags=["recoveries"])
def get_recovery(rec_id: int):
    rec = state.one("SELECT * FROM recoveries WHERE id=?", (rec_id,))
    if not rec:
        raise HTTPException(404, f"recovery {rec_id} not found")
    return rec


# --------------------------------------------------------------------------
# querying the recovered replica
# --------------------------------------------------------------------------

@app.get("/replica/traffic", tags=["replica"])
def replica_traffic(
    intersection_id: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
):
    """Query the recovered read-only replica (409 while none is servable)."""
    try:
        rec = recovery.require_current()
    except recovery.Conflict as exc:
        raise HTTPException(409, str(exc)) from exc
    try:
        with db.replica() as conn:
            where = "WHERE intersection_id = %(id)s" if intersection_id else ""
            rows = conn.execute(
                f"SELECT intersection_id, minute_utc, vehicle_count "
                f"FROM {config.TRAFFIC_TABLE} {where} "
                f"ORDER BY minute_utc DESC, intersection_id LIMIT %(lim)s",
                {"id": intersection_id, "lim": limit},
            ).fetchall()
            total = conn.execute(
                f"SELECT count(*) AS n FROM {config.TRAFFIC_TABLE}"
            ).fetchone()["n"]
            meta = conn.execute(
                "SELECT pg_is_in_recovery() AS in_recovery, "
                "pg_last_wal_replay_lsn()::text AS replay_lsn"
            ).fetchone()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(503, f"replica query failed: {exc}") from exc
    return {
        "recovery": {
            "id": rec["id"],
            "restore_point": rec["restore_point"],
            "target_lsn": rec["target_lsn"],
            "replay_lsn": meta["replay_lsn"],
            "in_recovery_read_only": meta["in_recovery"],
        },
        "total_rows": total,
        "rows": rows,
    }


# --------------------------------------------------------------------------
# demo: fault injection (exercises retirement protection and retry)
# --------------------------------------------------------------------------

class FaultRequest(BaseModel):
    simulate_stop_failure: bool = Field(
        default=False,
        description=(
            "Make the NEXT replica retirement pretend pg_ctl stop failed while "
            "the postmaster is still alive. The next POST /recoveries must then "
            "be rejected with 409, the old replica directory preserved, and the "
            "old replica keep serving its original restore point."
        ),
    )
    simulate_start_failure: bool = Field(
        default=False,
        description=(
            "Make the NEXT replica postmaster start fail (one-shot), producing "
            "a failed recovery whose directory is cleaned up. Clearing the "
            "flag and posting /recoveries again demonstrates retry without any "
            "service restart."
        ),
    )


@app.put("/demo/faults", tags=["demo"])
def set_faults(req: FaultRequest):
    """Set/clear demo-only fault injection switches (see /demo/faults)."""
    state.kv_set("fault_stop_failure", "1" if req.simulate_stop_failure else "")
    state.kv_set("fault_start_failure", "1" if req.simulate_start_failure else "")
    return get_faults()


@app.get("/demo/faults", tags=["demo"])
def get_faults():
    """Show the current demo fault injection switches."""
    return {
        "simulate_stop_failure": bool(state.kv_get("fault_stop_failure")),
        "simulate_start_failure": bool(state.kv_get("fault_start_failure")),
        "note": (
            "demo-only switches. stop failure: old postmaster kept alive and "
            "new recovery rejected (409); start failure: next recovery fails "
            "and can be retried after clearing the switch, no restart needed."
        ),
    }


# --------------------------------------------------------------------------
# demo: simulated collection + accidental deletion on the primary
# --------------------------------------------------------------------------

@app.post("/demo/generate", tags=["demo"])
def demo_generate(
    intersections: int = Query(3, ge=1, le=100),
    minutes: int = Query(20, ge=1, le=10000),
    prefix: str = Query("IX", pattern=r"^[A-Za-z0-9_-]{1,16}$"),
):
    """Simulate collection: upsert intersection/minute counts on the primary."""
    sql = f"""
        INSERT INTO {config.TRAFFIC_TABLE} (intersection_id, minute_utc, vehicle_count)
        SELECT %(prefix)s || '-' || i,
               date_trunc('minute', now()) - (m || ' minutes')::interval,
               (random() * 90 + 10)::int
        FROM generate_series(1, %(n)s) AS i
        CROSS JOIN generate_series(1, %(m)s) AS m
        ON CONFLICT (intersection_id, minute_utc)
        DO UPDATE SET vehicle_count = EXCLUDED.vehicle_count
        RETURNING intersection_id
    """
    with db.primary() as conn:
        inserted = len(
            conn.execute(
                sql, {"prefix": prefix, "n": intersections, "m": minutes}
            ).fetchall()
        )
    return {
        "upserted_rows": inserted,
        "intersections": [f"{prefix}-{i}" for i in range(1, intersections + 1)],
        "minutes_covered": minutes,
    }


@app.post("/demo/delete", tags=["demo"])
def demo_delete(minutes: int = Query(10, ge=1, le=100000)):
    """Simulate the accident: delete the latest `minutes` minutes of data."""
    sql = f"""
        DELETE FROM {config.TRAFFIC_TABLE}
        WHERE minute_utc > COALESCE(
                  (SELECT max(minute_utc) FROM {config.TRAFFIC_TABLE}),
                  date_trunc('minute', now())
              ) - (%(m)s || ' minutes')::interval
        RETURNING intersection_id
    """
    with db.primary() as conn:
        deleted = len(conn.execute(sql, {"m": minutes}).fetchall())
    return {"deleted_rows": deleted, "note": "simulated accidental deletion on the primary"}


@app.get("/primary/traffic", tags=["demo"])
def primary_traffic(
    intersection_id: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
):
    """Current state of the primary (which keeps collecting during recovery)."""
    where = "WHERE intersection_id = %(id)s" if intersection_id else ""
    with db.primary(row_factory=dict_row) as conn:
        rows = conn.execute(
            f"SELECT intersection_id, minute_utc, vehicle_count "
            f"FROM {config.TRAFFIC_TABLE} {where} "
            f"ORDER BY minute_utc DESC, intersection_id LIMIT %(lim)s",
            {"id": intersection_id, "lim": limit},
        ).fetchall()
        total = conn.execute(
            f"SELECT count(*) AS n FROM {config.TRAFFIC_TABLE}"
        ).fetchone()["n"]
    return {"total_rows": total, "rows": rows}


@app.get("/demo/overview", tags=["demo"])
def demo_overview():
    """One-shot picture: primary data, backup, restore points, recoveries."""
    with db.primary(row_factory=dict_row) as conn:
        total = conn.execute(
            f"SELECT count(*) AS n FROM {config.TRAFFIC_TABLE}"
        ).fetchone()["n"]
        per_ix = conn.execute(
            f"SELECT intersection_id, count(*) AS rows, "
            f"min(minute_utc) AS first_minute, max(minute_utc) AS last_minute "
            f"FROM {config.TRAFFIC_TABLE} GROUP BY 1 ORDER BY 1"
        ).fetchall()
    return {
        "primary": {"total_rows": total, "per_intersection": per_ix},
        "backup": backup_status(),
        "restore_points": state.q("SELECT * FROM restore_points ORDER BY id"),
        "recoveries": state.q("SELECT * FROM recoveries ORDER BY id DESC"),
        "current_recovery_id": state.kv_get("current_recovery_id"),
    }
