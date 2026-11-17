"""Physical base backup taken once at service startup.

`pg_basebackup -X stream` produces a self-contained physical copy of the
cluster (all WAL needed for backup consistency is streamed into the backup
itself). Everything after the backup is covered by the continuously archived
WAL in WAL_ARCHIVE_DIR, which is what makes point-in-time recovery possible.
"""

import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone

from . import config, db, state

log = logging.getLogger("backup")


def _wait_for_primary(attempts: int = 60) -> bool:
    for _ in range(attempts):
        try:
            with db.primary() as conn:
                conn.execute("SELECT 1")
            return True
        except Exception:
            time.sleep(2)
    return False


def run_base_backup(attempts: int = 10, delay_sec: int = 15):
    if config.BACKUP_ON_STARTUP != "1":
        log.info("startup base backup disabled (BACKUP_ON_STARTUP=%s)",
                 config.BACKUP_ON_STARTUP)
        return
    if not _wait_for_primary():
        state.kv_set("backup_status", "failed")
        state.kv_set("backup_error", "primary database is not reachable")
        return
    for attempt in range(1, attempts + 1):
        state.kv_set("backup_status", "running")
        state.kv_set("backup_error", "")
        try:
            _take_base_backup_once()
            state.kv_set("backup_status", "ready")
            return
        except Exception as exc:  # noqa: BLE001 - surfaced via /backup
            log.exception("base backup attempt %d/%d failed", attempt, attempts)
            state.kv_set("backup_status", "failed")
            state.kv_set("backup_error", str(exc)[:2000])
            if attempt < attempts:
                time.sleep(delay_sec)


def _take_base_backup_once():
    shutil.rmtree(config.BACKUP_DIR, ignore_errors=True)
    os.makedirs(os.path.dirname(config.BACKUP_DIR), exist_ok=True)
    log.info("taking physical base backup into %s", config.BACKUP_DIR)
    result = subprocess.run(
        [
            "pg_basebackup",
            "-d", config.BACKUP_DSN,
            "-D", config.BACKUP_DIR,
            "-X", "stream",   # include WAL required for consistency
            "-c", "fast",     # fast checkpoint
            "-Fp",            # plain format (directory)
        ],
        capture_output=True,
        text=True,
        timeout=900,
    )
    if result.returncode != 0:
        raise RuntimeError(f"pg_basebackup failed: {result.stderr[-2000:]}")

    # WAL generated after this LSN must come from the archive; restore
    # point readiness is checked as continuity from here to the point.
    with db.primary() as conn:
        end_lsn = conn.execute("SELECT pg_current_wal_insert_lsn()::text").fetchone()[0]
    state.kv_set("backup_end_lsn", end_lsn)
    # The backup's own redo point: restore points at/before it predate this
    # backup and can never be recovered from it.
    start_lsn = _read_backup_label_start_lsn()
    if start_lsn:
        state.kv_set("backup_start_lsn", start_lsn)
    state.kv_set("backup_completed_at", datetime.now(timezone.utc).isoformat())
    log.info("base backup ready, start LSN %s, end LSN %s", start_lsn, end_lsn)


def _read_backup_label_start_lsn() -> str | None:
    try:
        with open(os.path.join(config.BACKUP_DIR, "backup_label")) as fh:
            for line in fh:
                if line.startswith("START WAL LOCATION:"):
                    # START WAL LOCATION: 0/2000028 (file 000000010000000000000002)
                    return line.split(":", 1)[1].strip().split()[0]
    except OSError:
        pass
    return None
