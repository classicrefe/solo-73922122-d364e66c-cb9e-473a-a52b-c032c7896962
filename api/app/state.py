"""Tiny SQLite-backed state store for restore points and recovery jobs.

Kept outside the source database on purpose: the source DB holds exactly one
business table, and API bookkeeping must not pollute it (or the replicas,
which are physical copies of the source cluster).
"""

import sqlite3
import threading

from . import config

_lock = threading.Lock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS restore_points (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL UNIQUE,
    lsn        TEXT NOT NULL,
    wal_file   TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'pending',   -- pending | ready
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS recoveries (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    restore_point TEXT NOT NULL,
    target_lsn    TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'running', -- running | success | failed | superseded
    error         TEXT,
    work_dir      TEXT,
    replay_lsn    TEXT,
    started_at    TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at   TEXT
);
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def _conn():
    conn = sqlite3.connect(config.STATE_DB, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init():
    with _lock, _conn() as conn:
        conn.executescript(_SCHEMA)


def q(sql, args=()):
    with _lock, _conn() as conn:
        return [dict(r) for r in conn.execute(sql, args)]


def one(sql, args=()):
    rows = q(sql, args)
    return rows[0] if rows else None


def exec(sql, args=()):
    with _lock, _conn() as conn:
        return conn.execute(sql, args).lastrowid


def kv_get(key, default=None):
    row = one("SELECT value FROM kv WHERE key = ?", (key,))
    return row["value"] if row else default


def kv_set(key, value):
    exec(
        "INSERT INTO kv (key, value) VALUES (?, ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (key, None if value is None else str(value)),
    )
