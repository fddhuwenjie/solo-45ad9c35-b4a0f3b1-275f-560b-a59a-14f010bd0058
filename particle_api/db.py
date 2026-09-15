"""SQLite persistence and append-only audit support."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from typing import Any

GENESIS_HASH = hashlib.sha256(b"particle-api/genesis/v1").hexdigest()

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS segment_versions (
    id INTEGER PRIMARY KEY,
    segment_no TEXT NOT NULL,
    version INTEGER NOT NULL,
    version_hash TEXT NOT NULL UNIQUE,
    config_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(segment_no, version)
);

CREATE TABLE IF NOT EXISTS plates (
    id INTEGER PRIMARY KEY,
    plate_code TEXT NOT NULL UNIQUE,
    grid_rows INTEGER NOT NULL CHECK(grid_rows > 0),
    grid_cols INTEGER NOT NULL CHECK(grid_cols > 0),
    cell_area_m2 REAL NOT NULL CHECK(cell_area_m2 > 0),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS exposures (
    id INTEGER PRIMARY KEY,
    segment_version_id INTEGER NOT NULL REFERENCES segment_versions(id),
    plate_id INTEGER NOT NULL REFERENCES plates(id),
    mount_position TEXT,
    installed_at TEXT NOT NULL,
    removed_at TEXT NOT NULL,
    photo_at TEXT NOT NULL,
    unexposed_json TEXT NOT NULL DEFAULT '[]',
    upload_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    CHECK(removed_at >= installed_at),
    UNIQUE(plate_id, installed_at, removed_at)
);

CREATE TABLE IF NOT EXISTS exposure_cell_counts (
    id INTEGER PRIMARY KEY,
    exposure_id INTEGER NOT NULL REFERENCES exposures(id),
    cell_index INTEGER NOT NULL CHECK(cell_index >= 0),
    bin_index INTEGER NOT NULL CHECK(bin_index >= 0),
    bin_lo_um REAL NOT NULL,
    bin_hi_um REAL,
    particle_count INTEGER NOT NULL CHECK(particle_count >= 0),
    UNIQUE(exposure_id, cell_index, bin_index)
);

CREATE TABLE IF NOT EXISTS exposure_flags (
    id INTEGER PRIMARY KEY,
    exposure_id INTEGER NOT NULL REFERENCES exposures(id),
    kind TEXT NOT NULL CHECK(kind IN (
        'OVERLAP', 'CLOCK_REGRESSION', 'PHOTO_MISALIGNED',
        'LOCAL_CONTAMINATION', 'MANUAL'
    )),
    cell_index INTEGER,
    note TEXT,
    actor TEXT,
    marked_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_flags_global
ON exposure_flags(exposure_id, kind)
WHERE cell_index IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_flags_cell
ON exposure_flags(exposure_id, kind, cell_index)
WHERE cell_index IS NOT NULL;

CREATE TABLE IF NOT EXISTS pressure_samples (
    id INTEGER PRIMARY KEY,
    segment_version_id INTEGER NOT NULL REFERENCES segment_versions(id),
    sampled_at TEXT NOT NULL,
    pressure_pa REAL NOT NULL,
    received_at TEXT NOT NULL,
    UNIQUE(segment_version_id, sampled_at)
);

CREATE TABLE IF NOT EXISTS clock_events (
    id INTEGER PRIMARY KEY,
    segment_version_id INTEGER NOT NULL REFERENCES segment_versions(id),
    kind TEXT NOT NULL CHECK(kind IN ('REGRESSION', 'RESYNC')),
    event_at TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    note TEXT
);

CREATE TABLE IF NOT EXISTS decontaminations (
    id INTEGER PRIMARY KEY,
    plate_id INTEGER NOT NULL REFERENCES plates(id),
    decontaminated_at TEXT NOT NULL,
    actor TEXT,
    note TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(plate_id, decontaminated_at)
);

CREATE TABLE IF NOT EXISTS evaluation_events (
    id INTEGER PRIMARY KEY,
    exposure_id INTEGER NOT NULL REFERENCES exposures(id),
    result TEXT NOT NULL CHECK(result IN ('PASS', 'FAIL', 'INVALID')),
    grade TEXT NOT NULL,
    decision_json TEXT NOT NULL,
    evaluated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS segment_streaks (
    segment_version_id INTEGER PRIMARY KEY REFERENCES segment_versions(id),
    consecutive_passes INTEGER NOT NULL DEFAULT 0,
    last_exposure_id INTEGER REFERENCES exposures(id),
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    seq INTEGER PRIMARY KEY,
    event_type TEXT NOT NULL,
    entity_table TEXT NOT NULL,
    entity_id TEXT,
    payload_json TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    entry_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

APPEND_ONLY_TABLES = [
    "segment_versions",
    "plates",
    "exposures",
    "exposure_cell_counts",
    "exposure_flags",
    "pressure_samples",
    "clock_events",
    "decontaminations",
    "evaluation_events",
    "audit_log",
]


def connect(db_path: str) -> sqlite3.Connection:
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def initialize(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA_SQL)
    for table in APPEND_ONLY_TABLES:
        conn.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS trg_{table}_no_update
            BEFORE UPDATE ON {table}
            BEGIN
                SELECT RAISE(ABORT, '{table} is append-only');
            END
            """
        )
        conn.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS trg_{table}_no_delete
            BEFORE DELETE ON {table}
            BEGIN
                SELECT RAISE(ABORT, '{table} is append-only');
            END
            """
        )
    conn.commit()


def canonical_json(data: Any) -> str:
    return json.dumps(
        data, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def append_audit(
    conn: sqlite3.Connection,
    event_type: str,
    entity_table: str,
    entity_id: Any,
    payload: dict[str, Any],
    created_at: str,
) -> sqlite3.Row:
    row = conn.execute(
        "SELECT entry_hash FROM audit_log ORDER BY seq DESC LIMIT 1"
    ).fetchone()
    prev_hash = row["entry_hash"] if row else GENESIS_HASH
    next_row = conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM audit_log")
    seq = next_row.fetchone()["seq"]
    entry_without_hash = {
        "seq": seq,
        "event_type": event_type,
        "entity_table": entity_table,
        "entity_id": str(entity_id) if entity_id is not None else None,
        "payload": payload,
        "prev_hash": prev_hash,
        "created_at": created_at,
    }
    entry_hash = sha256_text(canonical_json(entry_without_hash))
    conn.execute(
        """
        INSERT INTO audit_log(
            seq, event_type, entity_table, entity_id, payload_json,
            prev_hash, entry_hash, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            seq,
            event_type,
            entity_table,
            str(entity_id) if entity_id is not None else None,
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
            prev_hash,
            entry_hash,
            created_at,
        ),
    )
    return conn.execute("SELECT * FROM audit_log WHERE seq=?", (seq,)).fetchone()


def verify_audit_chain(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute("SELECT * FROM audit_log ORDER BY seq").fetchall()
    prev_hash = GENESIS_HASH
    for expected_seq, row in enumerate(rows, start=1):
        if row["seq"] != expected_seq:
            return {
                "ok": False,
                "broken_seq": row["seq"],
                "reason": "SEQUENCE_GAP",
                "entries": len(rows),
            }
        if row["prev_hash"] != prev_hash:
            return {
                "ok": False,
                "broken_seq": row["seq"],
                "reason": "PREVIOUS_HASH_MISMATCH",
                "entries": len(rows),
            }
        payload = json.loads(row["payload_json"])
        rebuilt = {
            "seq": row["seq"],
            "event_type": row["event_type"],
            "entity_table": row["entity_table"],
            "entity_id": row["entity_id"],
            "payload": payload,
            "prev_hash": row["prev_hash"],
            "created_at": row["created_at"],
        }
        if sha256_text(canonical_json(rebuilt)) != row["entry_hash"]:
            return {
                "ok": False,
                "broken_seq": row["seq"],
                "reason": "ENTRY_HASH_MISMATCH",
                "entries": len(rows),
            }
        prev_hash = row["entry_hash"]
    return {"ok": True, "entries": len(rows), "head_hash": prev_hash}
