"""SQLite persistence layer.

Every transaction here is short and never spans an ``await``: the streaming
upload pipeline touches the database only at well-defined checkpoints, so a
slow upload or a long validation never holds a database write transaction.
"""
from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import datetime, timezone

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id          TEXT PRIMARY KEY,
    status      TEXT NOT NULL CHECK (status IN ('PENDING','READY','FAILED')),
    read_count  INTEGER,
    error       TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS files (
    batch_id   TEXT NOT NULL REFERENCES batches(id),
    end        TEXT NOT NULL CHECK (end IN ('R1','R2')),
    status     TEXT NOT NULL CHECK (status IN ('RECEIVING','VALIDATED','FAILED')),
    path       TEXT,
    size_bytes INTEGER,
    sha256     TEXT,
    records    INTEGER,
    names_hash TEXT,
    error      TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (batch_id, end)
);
"""

TERMINAL = ("READY", "FAILED")

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def init() -> None:
    global _conn
    conn = sqlite3.connect(config.DB_PATH, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")       # readers never block on the writer
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    _conn = conn


def close() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None


# ------------------------------------------------------------------ batches
def create_batch() -> dict:
    batch_id = uuid.uuid4().hex
    now = _now()
    with _lock:
        _conn.execute(
            "INSERT INTO batches(id, status, created_at, updated_at) VALUES(?, 'PENDING', ?, ?)",
            (batch_id, now, now),
        )
        return get_batch(batch_id)


def get_batch(batch_id: str) -> dict | None:
    with _lock:
        b = _conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if b is None:
            return None
        files = _conn.execute(
            "SELECT * FROM files WHERE batch_id=? ORDER BY end", (batch_id,)
        ).fetchall()
        return _to_dict(b, files)


def list_batches(status: str | None = None, limit: int = 200) -> list[dict]:
    with _lock:
        if status:
            rows = _conn.execute(
                "SELECT * FROM batches WHERE status=? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = _conn.execute(
                "SELECT * FROM batches ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        out = []
        for b in rows:
            files = _conn.execute(
                "SELECT * FROM files WHERE batch_id=? ORDER BY end", (b["id"],)
            ).fetchall()
            out.append(_to_dict(b, files))
        return out


def batch_ids_by_status(status: str) -> set[str]:
    with _lock:
        return {r["id"] for r in _conn.execute("SELECT id FROM batches WHERE status=?", (status,))}


# ------------------------------------------------------------------ files
def try_start_file(batch_id: str, end: str) -> str:
    """Atomically claim (batch, end). One upload per end is ever accepted."""
    with _lock:
        row = _conn.execute("SELECT status FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            return "no_batch"
        if row["status"] != "PENDING":
            return "not_pending"
        try:
            _conn.execute(
                "INSERT INTO files(batch_id, end, status, updated_at) VALUES(?, ?, 'RECEIVING', ?)",
                (batch_id, end, _now()),
            )
        except sqlite3.IntegrityError:
            return "duplicate"
        return "ok"


def mark_file_failed(batch_id: str, end: str, error: str) -> None:
    with _lock:
        _conn.execute(
            "UPDATE files SET status='FAILED', error=?, updated_at=? "
            "WHERE batch_id=? AND end=? AND status='RECEIVING'",
            (error, _now(), batch_id, end),
        )


def finalize_file_success(batch_id: str, end: str, meta: dict) -> tuple[str, object]:
    """Commit one validated end; when both ends are VALIDATED, decide the pair.

    Returns (outcome, info) with outcome in
    'validated' | 'ready' | 'mismatch' | 'batch_not_pending' | 'conflict'.
    Runs as one short write transaction; a late finisher whose batch already
    left PENDING is rejected and must not publish anything.
    """
    other_end = "R2" if end == "R1" else "R1"
    with _lock:
        c = _conn.cursor()
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute("SELECT status FROM batches WHERE id=?", (batch_id,)).fetchone()
            if row is None or row["status"] != "PENDING":
                c.execute("ROLLBACK")
                return ("batch_not_pending", None)
            now = _now()
            cur = c.execute(
                "UPDATE files SET status='VALIDATED', path=?, size_bytes=?, sha256=?, "
                "records=?, names_hash=?, error=NULL, updated_at=? "
                "WHERE batch_id=? AND end=? AND status='RECEIVING'",
                (
                    meta["path"], meta["size_bytes"], meta["sha256"],
                    meta["records"], meta["names_hash"], now, batch_id, end,
                ),
            )
            if cur.rowcount != 1:
                c.execute("ROLLBACK")
                return ("conflict", None)
            other = c.execute(
                "SELECT status, records, names_hash FROM files WHERE batch_id=? AND end=?",
                (batch_id, other_end),
            ).fetchone()
            if other is not None and other["status"] == "VALIDATED":
                if other["records"] != meta["records"] or other["names_hash"] != meta["names_hash"]:
                    reason = (
                        f"pair mismatch: read counts or per-record read-name sequences differ "
                        f"between ends ({other_end} records={other['records']} vs "
                        f"{end} records={meta['records']})"
                    )
                    c.execute(
                        "UPDATE batches SET status='FAILED', error=?, updated_at=? WHERE id=?",
                        (reason, now, batch_id),
                    )
                    c.execute(
                        "UPDATE files SET status='FAILED', error=?, updated_at=? "
                        "WHERE batch_id=? AND status='VALIDATED'",
                        ("pair mismatch between ends", now, batch_id),
                    )
                    c.execute("COMMIT")
                    return ("mismatch", reason)
                c.execute(
                    "UPDATE batches SET status='READY', read_count=?, updated_at=? "
                    "WHERE id=? AND status='PENDING'",
                    (meta["records"], now, batch_id),
                )
                c.execute("COMMIT")
                return ("ready", meta["records"])
            c.execute("COMMIT")
            return ("validated", None)
        except Exception:
            c.execute("ROLLBACK")
            raise


def finalize_failure(batch_id: str, reason: str) -> None:
    """Terminal batch failure; first recorded reason wins."""
    with _lock:
        c = _conn.cursor()
        c.execute("BEGIN IMMEDIATE")
        try:
            now = _now()
            c.execute(
                "UPDATE batches SET status='FAILED', error=COALESCE(error, ?), updated_at=? "
                "WHERE id=? AND status='PENDING'",
                (reason, now, batch_id),
            )
            c.execute(
                "UPDATE files SET status='FAILED', error=COALESCE(error, ?), updated_at=? "
                "WHERE batch_id=? AND status IN ('RECEIVING','VALIDATED')",
                (reason, now, batch_id),
            )
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise


# ------------------------------------------------------------- recovery
def fail_incomplete_on_startup() -> list[str]:
    """Mark every non-terminal batch FAILED after a restart; returns their ids."""
    with _lock:
        ids = [r["id"] for r in _conn.execute("SELECT id FROM batches WHERE status='PENDING'")]
        if not ids:
            return []
        c = _conn.cursor()
        c.execute("BEGIN IMMEDIATE")
        try:
            now = _now()
            for batch_id in ids:
                c.execute(
                    "UPDATE batches SET status='FAILED', error=?, updated_at=? WHERE id=?",
                    ("service restarted before the pair completed", now, batch_id),
                )
                c.execute(
                    "UPDATE files SET status='FAILED', error=COALESCE(error, ?), updated_at=? "
                    "WHERE batch_id=? AND status IN ('RECEIVING','VALIDATED')",
                    ("service restarted", now, batch_id),
                )
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return ids


# ------------------------------------------------------------------ views
def _to_dict(b: sqlite3.Row, files: list[sqlite3.Row]) -> dict:
    ends: dict[str, dict | None] = {"R1": None, "R2": None}
    for f in files:
        ends[f["end"]] = {
            "status": f["status"],
            "path": f["path"],
            "size_bytes": f["size_bytes"],
            "sha256": f["sha256"],
            "records": f["records"],
            "error": f["error"],
        }
    return {
        "batch_id": b["id"],
        "status": b["status"],
        "read_count": b["read_count"],
        "error": b["error"],
        "created_at": b["created_at"],
        "updated_at": b["updated_at"],
        "ends": ends,
    }
