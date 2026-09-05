"""
event_store.py — Durable, cross-process event queue + result store.
======================================================================
This replaces the old "poll NewsAPI every 120 seconds and process inline"
loop with a real ingestion/processing split:

  PUSH  — enqueue_event() is called the instant an article arrives (a
          webhook POST, or the fallback NewsAPI poller) and returns
          immediately. This is the event-driven front door.

  CLAIM — workers (worker.py, or an in-process worker thread started by
          app.py / streamlit_app.py) claim pending events with a fast local
          poll (~1s, configurable) on this table and hand them to
          pipeline.process_article(). "Polling a local queue every second"
          is a legitimate consumption pattern (the outbox/polling-publisher
          pattern); what actually matters for the SLA is that ingestion is
          push-based and decoupled from processing, which it now is.

SQLite instead of Redis/Kafka so the whole project still runs with zero
extra infrastructure to install. claim_next_event() uses BEGIN IMMEDIATE so
multiple workers — in-process threads OR entirely separate OS processes —
can safely compete for the same queue without ever double-processing an
event. That's the horizontal-scaling property a real broker gives you,
without requiring the student to stand one up.

Swap-in point for production: replace enqueue_event()/claim_next_event()
with a Redis Streams XADD/XREADGROUP pair and nothing else in this project
has to change — pipeline.py and every UI page reads through this module's
functions, never the raw table.
"""

import json
import os
import sqlite3
import threading
import time

DB_PATH = os.environ.get("CRISISPULSE_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "crisispulse.db"))

_local = threading.local()
_init_lock = threading.Lock()
_initialized = False


def get_conn():
    """One SQLite connection per thread — sqlite3 connections aren't safe to
    share across threads, and WAL mode makes many-connections-one-file cheap."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        _local.conn = conn
    return conn


def init_db():
    global _initialized
    with _init_lock:
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client TEXT NOT NULL,
                headline TEXT NOT NULL,
                body TEXT,
                source TEXT,
                status TEXT NOT NULL DEFAULT 'pending',   -- pending | processing | done | error
                received_at REAL NOT NULL,
                claimed_at REAL,
                done_at REAL,
                result_json TEXT,
                error TEXT,
                UNIQUE(client, headline)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_status ON events(status)")
        _initialized = True


def enqueue_event(client, headline, body, source):
    """PUSH side. Instant accept — no processing happens here. Duplicate
    (client, headline) pairs are silently ignored, so re-polling NewsAPI (or
    a retried webhook) can't double-enqueue the same article."""
    if not headline:
        return None
    conn = get_conn()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO events (client, headline, body, source, status, received_at) "
            "VALUES (?, ?, ?, ?, 'pending', ?)",
            (client, headline, body or "", source or "Unknown", time.time()),
        )
        row = conn.execute(
            "SELECT id FROM events WHERE client=? AND headline=?", (client, headline)
        ).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None


def claim_next_event():
    """CLAIM side. Atomically claims the oldest pending event. Safe under
    concurrent callers (threads or separate processes) — BEGIN IMMEDIATE
    takes the write lock before reading, so two workers can never claim the
    same row."""
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT id, client, headline, body, source, received_at FROM events "
            "WHERE status='pending' ORDER BY id LIMIT 1"
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return None
        conn.execute("UPDATE events SET status='processing', claimed_at=? WHERE id=?", (time.time(), row[0]))
        conn.execute("COMMIT")
        return {"id": row[0], "client": row[1], "headline": row[2], "body": row[3],
                "source": row[4], "received_at": row[5]}
    except sqlite3.OperationalError:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        return None


def mark_done(event_id, result):
    conn = get_conn()
    conn.execute(
        "UPDATE events SET status='done', done_at=?, result_json=? WHERE id=?",
        (time.time(), json.dumps(result, default=str), event_id),
    )


def mark_error(event_id, err):
    conn = get_conn()
    conn.execute(
        "UPDATE events SET status='error', done_at=?, error=? WHERE id=?",
        (time.time(), str(err), event_id),
    )


def queue_depth():
    conn = get_conn()
    row = conn.execute("SELECT COUNT(*) FROM events WHERE status IN ('pending','processing')").fetchone()
    return row[0] if row else 0


def get_recent_results(limit=60):
    """Newest first. Each row is exactly what pipeline.process_article()
    returned, round-tripped through JSON — a frozen snapshot at completion
    time, not a live reference into pipeline's in-memory incident dicts."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT result_json FROM events WHERE status='done' ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    out = []
    for (rj,) in rows:
        if rj:
            try:
                out.append(json.loads(rj))
            except json.JSONDecodeError:
                continue
    return out


def get_latest_incidents(limit=200):
    """One entry per incident id — the most recently updated snapshot of
    each, derived purely from persisted results. Works correctly even if the
    worker that processed an event ran in a different OS process than
    whatever is reading this (e.g. worker.py separate from streamlit_app.py)."""
    seen = {}
    for r in get_recent_results(limit=limit):
        inc = r.get("incident")
        if inc and inc.get("id") not in seen:
            seen[inc["id"]] = inc
    return sorted(seen.values(), key=lambda i: i["last_updated"], reverse=True)


def recent_errors(limit=10):
    conn = get_conn()
    rows = conn.execute(
        "SELECT headline, error, done_at FROM events WHERE status='error' ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [{"headline": h, "error": e, "at": t} for h, e, t in rows]


def queue_counts():
    conn = get_conn()
    rows = conn.execute("SELECT status, COUNT(*) FROM events GROUP BY status").fetchall()
    counts = {"pending": 0, "processing": 0, "done": 0, "error": 0}
    counts.update(dict(rows))
    return counts
