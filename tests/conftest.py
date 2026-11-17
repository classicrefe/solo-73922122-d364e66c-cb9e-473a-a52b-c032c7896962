"""Pytest fixtures for the retirement/recovery safety tests.

These tests run without a real PostgreSQL: a tiny Python "postmaster" stands
in for postgres (it writes a postmaster.pid with the same format and holds a
fixed TCP port), and a fake ``pg_ctl`` script implements start/stop/status.
The sandbox forbids cross-process signals, which makes it a perfect place to
prove the "stop failed but process still alive" protection deterministically.
Shutdown is therefore cooperative (a TCP SHUTDOWN command), with SIGTERM
still attempted as a best effort -- mimicking pg_ctl on a normal host.
"""

from __future__ import annotations

import os
import socket
import sys
import textwrap
import time
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


# --------------------------------------------------------------------------
# psycopg stub: the api imports psycopg at module load; tests never open a DB.
# --------------------------------------------------------------------------

def _install_psycopg_stub():
    try:
        import psycopg  # noqa: F401
        return
    except ImportError:
        pass
    psycopg = types.ModuleType("psycopg")
    rows = types.ModuleType("psycopg.rows")

    def dict_row(cursor):  # signature mirrors the real one
        return cursor

    rows.dict_row = dict_row

    class _ConnectError(RuntimeError):
        pass

    def connect(*a, **kw):  # pragma: no cover - tests must not hit this
        raise _ConnectError("no real database in unit tests")

    psycopg.connect = connect
    psycopg.rows = rows
    sys.modules["psycopg"] = psycopg
    sys.modules["psycopg.rows"] = rows


_install_psycopg_stub()


# --------------------------------------------------------------------------
# fake postmaster: long-lived process writing a postgres-style postmaster.pid
# --------------------------------------------------------------------------

POSTMASTER_SRC = textwrap.dedent(
    r"""
    import os, signal, socket, sys, time

    data_dir = sys.argv[1]
    port = int(sys.argv[2])
    accept_stop = sys.argv[3] == "1"          # "0" => shutdown requests ignored
    pid_file = os.path.join(data_dir, "postmaster.pid")

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(8)

    with open(pid_file, "w") as fh:
        # same first-line format as real postgres postmaster.pid
        fh.write(str(os.getpid()) + "\n")

    stopping = {"v": False}

    def term(signum, frame):
        if accept_stop:
            stopping["v"] = True
        # otherwise ignore: emulates "stop failed, still alive"

    signal.signal(signal.SIGTERM, term)
    signal.signal(signal.SIGINT, term)
    srv.settimeout(1)
    while not stopping["v"]:
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        try:
            cmd = conn.recv(64)
            if cmd == b"SHUTDOWN" and accept_stop:
                stopping["v"] = True
            # admin channel: emulates an operator forcefully killing a stuck
            # postmaster (the sandbox forbids real cross-process signals)
            if cmd == b"FORCE_SHUTDOWN":
                stopping["v"] = True
        except OSError:
            pass
        finally:
            conn.close()
    try:
        os.remove(pid_file)
    except OSError:
        pass
    srv.close()
    """
)

FAKE_PGCTL_SRC = '''#!/usr/bin/env python3
"""Minimal pg_ctl look-alike: pg_ctl -D DATA {start|stop|status}.

Reads DATA/fake_postmaster.conf: python_bin|postmaster_py|port|accept_stop
"""
import os, signal, socket, subprocess, sys, time

args = sys.argv[1:]
data = None
action = None
i = 0
while i < len(args):
    a = args[i]
    if a == "-D":
        data = args[i + 1]; i += 2; continue
    if a in ("start", "stop", "status"):
        action = a
    i += 1

conf = os.path.join(data, "fake_postmaster.conf")
python_bin, postmaster_py, port, accept_stop = open(conf).read().strip().split("|")
port = int(port)

def read_pid():
    try:
        return int(open(os.path.join(data, "postmaster.pid")).readline().strip())
    except OSError:
        return None

def alive(pid):
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True

if action == "status":
    sys.exit(0 if alive(read_pid()) else 3)

if action == "start":
    if alive(read_pid()):
        sys.exit(1)
    log = open(os.path.join(os.path.dirname(data), "replica.log"), "a")
    subprocess.Popen(
        [python_bin, postmaster_py, data, str(port), accept_stop],
        stdout=log, stderr=log, start_new_session=True,
    )
    deadline = time.time() + 15
    while time.time() < deadline and not alive(read_pid()):
        time.sleep(0.1)
    sys.exit(0 if alive(read_pid()) else 1)

if action == "stop":
    pid = read_pid()
    if not alive(pid):
        sys.exit(0)
    # 1) cooperative shutdown over the replica port
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2) as c:
            c.sendall(b"SHUTDOWN")
    except OSError:
        pass
    # 2) best-effort real signal (works on normal Linux / in docker)
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass
    deadline = time.time() + 2
    while time.time() < deadline and alive(pid):
        time.sleep(0.1)
    sys.exit(0 if not alive(pid) else 1)

sys.exit(2)
'''


def _free_port():
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """Configured app environment with isolated state + work directories."""
    from app import config, recovery, state

    port = _free_port()
    monkeypatch.setattr(config, "RECOVERY_ROOT", str(tmp_path / "recovery_work"))
    monkeypatch.setattr(config, "BACKUP_DIR", str(tmp_path / "backups" / "base"))
    monkeypatch.setattr(config, "WAL_ARCHIVE_DIR", str(tmp_path / "wal_archive"))
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "state" / "api.db"))
    monkeypatch.setattr(config, "REPLICA_HOST", "127.0.0.1")
    monkeypatch.setattr(config, "REPLICA_PORT", port)
    # run replica tools as the current user (no gosu in tests)
    monkeypatch.setattr(config, "REPLICA_RUN_AS", "")
    monkeypatch.setattr(config, "RECOVERY_TIMEOUT_SEC", 20)

    # keep the stop-failure path fast while exercising the full retry chain
    monkeypatch.setattr(recovery, "PG_CTL_STOP_TIMEOUT", 1)
    monkeypatch.setattr(recovery, "SIGNAL_WAIT_SEC", 1)
    monkeypatch.setattr(recovery, "STOP_RECHECK_SLEEP", 0.1)
    # fast replay polling so resume-watcher tests do not depend on wall-clock
    monkeypatch.setattr(recovery, "WATCHER_POLL_INTERVAL", 0.02)

    os.makedirs(config.RECOVERY_ROOT, exist_ok=True)
    os.makedirs(os.path.dirname(config.STATE_DB), exist_ok=True)
    os.makedirs(config.BACKUP_DIR, exist_ok=True)
    os.makedirs(config.WAL_ARCHIVE_DIR, exist_ok=True)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    postmaster_py = bin_dir / "fake_postmaster.py"
    postmaster_py.write_text(POSTMASTER_SRC)
    fake_pgctl = bin_dir / "pg_ctl"
    fake_pgctl.write_text(FAKE_PGCTL_SRC)
    fake_pgctl.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    state.init()
    recovery._owned_jobs.clear()

    return types.SimpleNamespace(
        config=config,
        state=state,
        recovery=recovery,
        port=port,
        tmp=tmp_path,
        postmaster_py=str(postmaster_py),
    )


@pytest.fixture()
def replica_factory(env):
    """Spawn a fake postmaster in a work dir, mimicking a real replica."""
    import subprocess

    from app import config

    created = []

    def _make(name, accept_stop=True):
        work_dir = os.path.join(config.RECOVERY_ROOT, name)
        data = os.path.join(work_dir, "data")
        os.makedirs(data, exist_ok=True)
        with open(os.path.join(data, "fake_postmaster.conf"), "w") as fh:
            fh.write(
                "|".join(
                    [sys.executable, env.postmaster_py, str(config.REPLICA_PORT),
                     "1" if accept_stop else "0"]
                )
            )
        # a sentinel file proving the directory must survive failed retirement
        with open(os.path.join(work_dir, "KEEP_ME.txt"), "w") as fh:
            fh.write("live replica data")
        log = open(os.path.join(work_dir, "replica.log"), "a")
        proc = subprocess.Popen(
            [sys.executable, env.postmaster_py, data,
             str(config.REPLICA_PORT), "1" if accept_stop else "0"],
            stdout=log, stderr=log, start_new_session=True,
        )
        deadline = time.time() + 10
        while time.time() < deadline and not os.path.exists(
            os.path.join(data, "postmaster.pid")
        ):
            time.sleep(0.05)
        created.append((work_dir, proc))
        return work_dir, proc

    yield _make

    # best-effort cleanup of anything the test left running
    for work_dir, proc in created:
        if proc.poll() is None:
            try:
                force_shutdown(config.REPLICA_PORT, proc)
            except Exception:
                pass


@pytest.fixture()
def ready_restore_point(env):
    """Insert a ready restore point + ready backup state."""
    from app import state

    state.kv_set("backup_status", "ready")
    state.kv_set("backup_end_lsn", "0/1000000")
    state.kv_set("backup_start_lsn", "0/0F00000")
    state.exec(
        "INSERT INTO restore_points (name, lsn, wal_file, status) "
        "VALUES (?, ?, ?, 'ready')",
        ("before_delete", "0/2000000", "000000010000000000000002"),
    )


def wait_for(predicate, timeout=10, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def force_shutdown(port, proc=None):
    """Emulate an operator killing a stuck postmaster (sandbox blocks signals).

    Uses the always-accepted FORCE_SHUTDOWN admin channel, then waits for the
    process to actually exit -- the same post-condition a real kill -9 gives.
    """
    probe = socket.socket()
    probe.settimeout(3)
    try:
        probe.connect(("127.0.0.1", port))
        probe.sendall(b"FORCE_SHUTDOWN")
    except OSError:
        pass
    finally:
        probe.close()
    if proc is not None:
        proc.wait(timeout=10)

