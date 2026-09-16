"""Particle-on-target grading domain service.

The module intentionally uses only the Python standard library.  All write
operations run in SQLite transactions, append domain/audit events, and replace
(never delete) a segment's current streak when evidence is revoked.
"""

from __future__ import annotations

import json
import math
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from . import db

# A timestamp far in the future used only to represent an unclosed clock event.
FAR_FUTURE = "9999-12-31T23:59:59+00:00"

DEFAULT_CONFIG = {
    "min_exposure_seconds": 60.0,
    "photo_tolerance_seconds": 60.0,
    "target_exposure_seconds": 60.0,
    "pressure_min_samples": 2,
    "pressure_required_coverage": 0.95,
    "pressure_max_gap_seconds": 60.0,
    "segment_cooldown_seconds": 0.0,
    "plate_cooldown_seconds": 0.0,
}

HARD_FLAG_KINDS = {
    "OVERLAP",
    "CLOCK_REGRESSION",
    "PHOTO_MISALIGNED",
    "LOCAL_CONTAMINATION",
    "MANUAL",
}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        result = {"error": {"code": self.code, "message": self.message}}
        if self.details is not None:
            result["error"]["details"] = self.details
        return result


write_lock = threading.RLock()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_datetime(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ApiError(400, "INVALID_TIME", f"{field} must be ISO-8601 text")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ApiError(400, "INVALID_TIME", f"{field} is not valid ISO-8601: {exc}")
    if parsed.tzinfo is None:
        raise ApiError(400, "TIMEZONE_REQUIRED", f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def finite_number(value: Any, field: str, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ApiError(400, "INVALID_NUMBER", f"{field} must be a number")
    number = float(value)
    if not math.isfinite(number):
        raise ApiError(400, "INVALID_NUMBER", f"{field} must be finite")
    if minimum is not None and number < minimum:
        raise ApiError(400, "NUMBER_TOO_SMALL", f"{field} must be >= {minimum}")
    return number


def normalize_config(input_config: dict[str, Any] | None) -> dict[str, Any]:
    supplied = input_config or {}
    if not isinstance(supplied, dict):
        raise ApiError(400, "INVALID_CONFIG", "config must be an object")

    size_bins = supplied.get("size_bins")
    limits = supplied.get("area_limits_particles_per_m2")
    if not isinstance(size_bins, list) or not size_bins:
        raise ApiError(400, "SIZE_BINS_REQUIRED", "config.size_bins is required")
    if not isinstance(limits, list) or len(limits) != len(size_bins):
        raise ApiError(
            400,
            "LIMITS_REQUIRED",
            "area_limits_particles_per_m2 must match size_bins length",
        )

    bins: list[dict[str, float | None]] = []
    for index, raw_bin in enumerate(size_bins):
        if not isinstance(raw_bin, dict):
            raise ApiError(400, "INVALID_BIN", f"size bin {index} must be an object")
        lo = finite_number(raw_bin.get("lo_um"), f"size_bins[{index}].lo_um", 0)
        hi_raw = raw_bin.get("hi_um")
        hi = None if hi_raw is None else finite_number(
            hi_raw, f"size_bins[{index}].hi_um"
        )
        if hi is not None and hi <= lo:
            raise ApiError(400, "INVALID_BIN", f"size bin {index} hi must exceed lo")
        bins.append({"lo_um": lo, "hi_um": hi})

    area_limits = [
        finite_number(value, f"area_limits_particles_per_m2[{i}]", 0)
        for i, value in enumerate(limits)
    ]

    grid_raw = supplied.get("grid_limits_particles_per_m2")
    if grid_raw is None:
        # A unit area of target must satisfy the same density limit as the
        # complete effective area unless a stricter/local rule is supplied.
        grid_limits = area_limits.copy()
    else:
        if not isinstance(grid_raw, list) or len(grid_raw) != len(bins):
            raise ApiError(
                400,
                "INVALID_GRID_LIMITS",
                "grid_limits_particles_per_m2 must match size_bins length",
            )
        grid_limits = []
        for i, value in enumerate(grid_raw):
            if value is None:
                grid_limits.append(None)
            else:
                grid_limits.append(
                    finite_number(value, f"grid_limits_particles_per_m2[{i}]", 0)
                )

    config = dict(DEFAULT_CONFIG)
    numeric_defaults = {
        "min_exposure_seconds": 0.0,
        "photo_tolerance_seconds": 0.0,
        "target_exposure_seconds": 0.0,
        "pressure_min_samples": 1,
        "pressure_required_coverage": 0.0,
        "pressure_max_gap_seconds": 0.0,
        "segment_cooldown_seconds": 0.0,
        "plate_cooldown_seconds": 0.0,
    }
    for key, minimum in numeric_defaults.items():
        if key in supplied:
            config[key] = finite_number(supplied[key], key, minimum)
        else:
            config[key] = float(config[key])
    if config["pressure_required_coverage"] > 1:
        raise ApiError(400, "INVALID_COVERAGE", "coverage must be between 0 and 1")
    if config["target_exposure_seconds"] < config["min_exposure_seconds"]:
        raise ApiError(
            400,
            "INVALID_TARGET_EXPOSURE",
            "target_exposure_seconds must be >= min_exposure_seconds",
        )

    config["size_bins"] = bins
    config["area_limits_particles_per_m2"] = area_limits
    config["grid_limits_particles_per_m2"] = grid_limits
    return config


def begin(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN IMMEDIATE")


def commit(conn: sqlite3.Connection) -> None:
    conn.commit()


def rollback(conn: sqlite3.Connection) -> None:
    conn.rollback()


def audit(
    conn: sqlite3.Connection,
    event_type: str,
    table: str,
    entity_id: Any,
    payload: dict[str, Any],
) -> dict[str, Any]:
    row = db.append_audit(conn, event_type, table, entity_id, payload, now_iso())
    return {"seq": row["seq"], "entry_hash": row["entry_hash"]}


def get_version_by_hash(conn: sqlite3.Connection, version_hash: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM segment_versions WHERE version_hash=?",
        (version_hash,),
    ).fetchone()
    if row is None:
        raise ApiError(404, "VERSION_NOT_FOUND", f"unknown version {version_hash}")
    return row


def get_version_config(row: sqlite3.Row) -> dict[str, Any]:
    return json.loads(row["config_json"])


def register_segment(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    segment_no = payload.get("segment_no")
    if not isinstance(segment_no, str) or not segment_no.strip():
        raise ApiError(400, "SEGMENT_NO_REQUIRED", "segment_no is required")
    config = normalize_config(payload.get("config"))
    version_hash = db.sha256_text(
        db.canonical_json({"segment_no": segment_no, "config": config})
    )
    with write_lock:
        begin(conn)
        try:
            existing = conn.execute(
                "SELECT * FROM segment_versions WHERE version_hash=?", (version_hash,)
            ).fetchone()
            if existing:
                result = _version_response(existing, duplicate=True)
                commit(conn)
                return result
            next_version = conn.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 AS v "
                "FROM segment_versions WHERE segment_no=?",
                (segment_no,),
            ).fetchone()["v"]
            created = now_iso()
            cur = conn.execute(
                """
                INSERT INTO segment_versions(
                    segment_no, version, version_hash, config_json, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (segment_no, next_version, version_hash, json.dumps(config), created),
            )
            version_id = cur.lastrowid
            conn.execute(
                """
                INSERT INTO segment_streaks(
                    segment_version_id, consecutive_passes, updated_at
                ) VALUES (?, 0, ?)
                """,
                (version_id, created),
            )
            audit_row = audit(
                conn,
                "SEGMENT_VERSION_CREATED",
                "segment_versions",
                version_id,
                {"segment_no": segment_no, "version": next_version, "hash": version_hash},
            )
            row = conn.execute(
                "SELECT * FROM segment_versions WHERE id=?", (version_id,)
            ).fetchone()
            result = _version_response(row, duplicate=False, audit=audit_row)
            commit(conn)
            return result
        except Exception:
            rollback(conn)
            raise


def _version_response(
    row: sqlite3.Row, duplicate: bool, audit: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {
        "segment_no": row["segment_no"],
        "version": row["version"],
        "version_hash": row["version_hash"],
        "config": json.loads(row["config_json"]),
        "immutable": True,
        "duplicate": duplicate,
        "consecutive_passes": 0,
        "audit": audit,
    }


def register_plate(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    code = payload.get("plate_code")
    if not isinstance(code, str) or not code.strip():
        raise ApiError(400, "PLATE_CODE_REQUIRED", "plate_code is required")
    rows = int(finite_number(payload.get("grid_rows"), "grid_rows", 1))
    cols = int(finite_number(payload.get("grid_cols"), "grid_cols", 1))
    cell_area = finite_number(payload.get("cell_area_m2"), "cell_area_m2", 0)
    if rows <= 0 or cols <= 0:
        raise ApiError(400, "INVALID_GRID", "grid dimensions must be positive")
    created = now_iso()
    with write_lock:
        begin(conn)
        try:
            old = conn.execute(
                "SELECT * FROM plates WHERE plate_code=?", (code,)
            ).fetchone()
            if old:
                same = (
                    old["grid_rows"] == rows
                    and old["grid_cols"] == cols
                    and abs(old["cell_area_m2"] - cell_area) < 1e-15
                )
                if not same:
                    raise ApiError(
                        409,
                        "PLATE_CONFLICT",
                        "plate_code already exists with immutable different geometry",
                    )
                result = _plate_response(old, duplicate=True)
                commit(conn)
                return result
            cur = conn.execute(
                """
                INSERT INTO plates(
                    plate_code, grid_rows, grid_cols, cell_area_m2, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (code, rows, cols, cell_area, created),
            )
            plate_id = cur.lastrowid
            audit_row = audit(
                conn,
                "PLATE_REGISTERED",
                "plates",
                plate_id,
                {"plate_code": code, "grid_rows": rows, "grid_cols": cols,
                 "cell_area_m2": cell_area},
            )
            row = conn.execute("SELECT * FROM plates WHERE id=?", (plate_id,)).fetchone()
            result = _plate_response(row, duplicate=False, audit_row=audit_row)
            commit(conn)
            return result
        except Exception:
            rollback(conn)
            raise


def _plate_response(
    row: sqlite3.Row, duplicate: bool, audit_row: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {
        "plate_id": row["id"],
        "plate_code": row["plate_code"],
        "grid_rows": row["grid_rows"],
        "grid_cols": row["grid_cols"],
        "cell_area_m2": row["cell_area_m2"],
        "duplicate": duplicate,
        "audit": audit_row,
    }


def _get_plate(conn: sqlite3.Connection, code: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM plates WHERE plate_code=?", (code,)).fetchone()
    if row is None:
        raise ApiError(404, "PLATE_NOT_FOUND", f"unknown plate {code}")
    return row


def clock_ranges(conn: sqlite3.Connection, version_id: int) -> list[tuple[datetime, datetime]]:
    events = conn.execute(
        """
        SELECT kind, event_at FROM clock_events
        WHERE segment_version_id=?
        ORDER BY event_at, id
        """,
        (version_id,),
    ).fetchall()
    ranges: list[tuple[datetime, datetime]] = []
    regression_start: datetime | None = None
    for event in events:
        at = parse_iso(event["event_at"])
        if event["kind"] == "REGRESSION":
            if regression_start is None:
                regression_start = at
        elif regression_start is not None:
            ranges.append((regression_start, at))
            regression_start = None
    if regression_start is not None:
        ranges.append((regression_start, parse_iso(FAR_FUTURE)))
    return ranges


def intersects_clock_ranges(
    start: datetime, end: datetime, ranges: Iterable[tuple[datetime, datetime]]
) -> bool:
    for reg_start, reg_end in ranges:
        if start < reg_end and end > reg_start:
            return True
    return False


def active_clock_regression(
    conn: sqlite3.Connection, version_id: int, at: datetime
) -> dict[str, Any] | None:
    for start, end in clock_ranges(conn, version_id):
        if start <= at < end:
            return {
                "started_at": iso(start),
                "resync_at": iso(end) if end < parse_iso(FAR_FUTURE) else None,
            }
    return None


def _insert_clock_regression(
    conn: sqlite3.Connection,
    version: sqlite3.Row,
    at: datetime,
    detected_at: datetime,
    note: str | None,
    source: str,
) -> int | None:
    ranges = clock_ranges(conn, version["id"])
    if any(start <= at < end for start, end in ranges):
        return None
    cur = conn.execute(
        """
        INSERT INTO clock_events(
            segment_version_id, kind, event_at, detected_at, note
        ) VALUES (?, 'REGRESSION', ?, ?, ?)
        """,
        (version["id"], iso(at), iso(detected_at), note),
    )
    event_id = cur.lastrowid
    audit(
        conn,
        "CLOCK_REGRESSION_RECORDED",
        "clock_events",
        event_id,
        {"segment_version_hash": version["version_hash"], "at": iso(at),
         "source": source, "note": note},
    )
    _mark_regressed_exposures(conn, version, at, detected_at)
    return event_id


def _mark_regressed_exposures(
    conn: sqlite3.Connection,
    version: sqlite3.Row,
    regression_at: datetime,
    end_at: datetime,
) -> None:
    exposures = conn.execute(
        """
        SELECT * FROM exposures
        WHERE segment_version_id=?
          AND removed_at > ? AND installed_at < ?
        """,
        (version["id"], iso(regression_at), iso(end_at)),
    ).fetchall()
    for exposure in exposures:
        inserted = _ensure_flag(
            conn,
            exposure["id"],
            "CLOCK_REGRESSION",
            None,
            f"samples intersect untrusted clock interval starting {iso(regression_at)}",
            "system",
            audit_it=True,
        )
        if inserted:
            evaluate_exposure(conn, exposure["id"], actor="system")


def declare_clock_regression(
    conn: sqlite3.Connection, payload: dict[str, Any]
) -> dict[str, Any]:
    version = get_version_by_hash(conn, payload["version_hash"])
    at = utc_datetime(payload.get("at"), "at")
    note = payload.get("note")
    detected = utc_datetime(payload.get("detected_at"), "detected_at") if payload.get(
        "detected_at"
    ) else datetime.now(timezone.utc)
    with write_lock:
        begin(conn)
        try:
            event_id = _insert_clock_regression(
                conn, version, at, detected, note, "manual"
            )
            if event_id is None:
                result = {"duplicate": True, "active_regression": True}
            else:
                result = {
                    "duplicate": False,
                    "clock_event_id": event_id,
                    "active_regression": True,
                }
            commit(conn)
            return result
        except Exception:
            rollback(conn)
            raise


def resync_clock(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    version = get_version_by_hash(conn, payload["version_hash"])
    at = utc_datetime(payload.get("at"), "at")
    note = payload.get("note")
    with write_lock:
        begin(conn)
        try:
            ranges = clock_ranges(conn, version["id"])
            open_ranges = [r for r in ranges if r[1] >= parse_iso(FAR_FUTURE)]
            if not open_ranges:
                raise ApiError(409, "NO_ACTIVE_REGRESSION", "clock is already trusted")
            start = open_ranges[-1][0]
            if at < start:
                raise ApiError(400, "INVALID_RESYNC", "resync must follow regression")
            cur = conn.execute(
                """
                INSERT INTO clock_events(
                    segment_version_id, kind, event_at, detected_at, note
                ) VALUES (?, 'RESYNC', ?, ?, ?)
                """,
                (version["id"], iso(at), now_iso(), note),
            )
            event_id = cur.lastrowid
            # Backfilled exposures reported late but lying inside the now closed
            # interval get a permanent hard flag.
            exposures = conn.execute(
                """
                SELECT id FROM exposures
                WHERE segment_version_id=?
                  AND removed_at > ? AND installed_at < ?
                """,
                (version["id"], iso(start), iso(at)),
            ).fetchall()
            affected = []
            for row in exposures:
                inserted = _ensure_flag(
                    conn, row["id"], "CLOCK_REGRESSION", None,
                    f"exposure intersects clock regression {iso(start)}..{iso(at)}",
                    "system", audit_it=True,
                )
                if inserted:
                    affected.append(row["id"])
            for exposure_id in affected:
                evaluate_exposure(conn, exposure_id, actor="system")
            audit(
                conn,
                "CLOCK_RESYNC_RECORDED",
                "clock_events",
                event_id,
                {"version_hash": version["version_hash"], "at": iso(at),
                 "regression_started_at": iso(start), "newly_flagged": affected},
            )
            commit(conn)
            return {
                "clock_event_id": event_id,
                "regression_started_at": iso(start),
                "resync_at": iso(at),
                "newly_flagged_exposure_ids": affected,
            }
        except Exception:
            rollback(conn)
            raise


def add_pressure_samples(
    conn: sqlite3.Connection, payload: dict[str, Any]
) -> dict[str, Any]:
    version = get_version_by_hash(conn, payload["version_hash"])
    raw_samples = payload.get("samples")
    if not isinstance(raw_samples, list) or not raw_samples:
        raise ApiError(400, "SAMPLES_REQUIRED", "samples must be a non-empty list")
    samples: list[tuple[datetime, float]] = []
    seen: set[str] = set()
    for i, raw in enumerate(raw_samples):
        if not isinstance(raw, dict):
            raise ApiError(400, "INVALID_SAMPLE", f"samples[{i}] must be an object")
        at = utc_datetime(raw.get("sampled_at"), f"samples[{i}].sampled_at")
        pressure = finite_number(raw.get("pressure_pa"), f"samples[{i}].pressure_pa")
        key = iso(at)
        if key in seen:
            raise ApiError(400, "DUPLICATE_IN_BATCH", f"duplicate sample at {key}")
        seen.add(key)
        samples.append((at, pressure))
    samples.sort(key=lambda item: item[0])

    with write_lock:
        begin(conn)
        try:
            inserted = 0
            duplicate = 0
            regression_detected = False
            regressed_timestamps = []
            latest_before = conn.execute(
                """
                SELECT MAX(sampled_at) AS latest FROM pressure_samples
                WHERE segment_version_id=?
                """,
                (version["id"],),
            ).fetchone()["latest"]
            high_water = parse_iso(latest_before) if latest_before else None
            for at, pressure in samples:
                if high_water is not None and at < high_water:
                    regressed_timestamps.append(at)
                high_water = at if high_water is None else max(high_water, at)
            for at in regressed_timestamps:
                event_id = _insert_clock_regression(
                    conn, version, at, datetime.now(timezone.utc),
                    "pressure sample timestamp moved backwards", "pressure"
                )
                regression_detected = regression_detected or event_id is not None
            for at, pressure in samples:
                try:
                    conn.execute(
                        """
                        INSERT INTO pressure_samples(
                            segment_version_id, sampled_at, pressure_pa, received_at
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (version["id"], iso(at), pressure, now_iso()),
                    )
                    inserted += 1
                except sqlite3.IntegrityError as exc:
                    if "UNIQUE" not in str(exc).upper():
                        raise
                    duplicate += 1

            candidate_rows = conn.execute(
                """
                SELECT e.id AS exposure_id, ev.decision_json AS decision_json
                FROM exposures e
                JOIN evaluation_events ev ON ev.exposure_id=e.id
                JOIN (
                    SELECT exposure_id, MAX(id) AS max_id
                    FROM evaluation_events GROUP BY exposure_id
                ) latest ON latest.exposure_id=ev.exposure_id AND latest.max_id=ev.id
                WHERE e.segment_version_id=? AND ev.result='INVALID'
                """,
                (version["id"],),
            ).fetchall()
            candidate_ids = []
            for row in candidate_rows:
                decision = json.loads(row["decision_json"])
                if not any(reason.startswith("PRESSURE_") for reason in decision.get("invalid_reasons", [])):
                    continue
                # A hard flag remains valid independently of pressure evidence.
                hard = conn.execute(
                    "SELECT COUNT(*) AS c FROM exposure_flags WHERE exposure_id=?",
                    (row["exposure_id"],),
                ).fetchone()["c"]
                if hard:
                    continue
                candidate_ids.append(row["exposure_id"])

            reevaluated = []
            for exposure_id in candidate_ids:
                evaluate_exposure(
                    conn,
                    exposure_id,
                    actor="system",
                    recompute_streak_after=False,
                )
                reevaluated.append(exposure_id)
            if reevaluated:
                _recompute_streak(conn, version["id"])
            result = {
                "inserted": inserted,
                "duplicate_ignored": duplicate,
                "clock_regression_detected": regression_detected,
                "reevaluated_exposure_ids": reevaluated,
            }
            commit(conn)
            return result
        except Exception:
            rollback(conn)
            raise


def decontaminate_plate(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    plate_code = payload.get("plate_code")
    if not isinstance(plate_code, str):
        raise ApiError(400, "PLATE_CODE_REQUIRED", "plate_code is required")
    at = utc_datetime(payload.get("decontaminated_at"), "decontaminated_at")
    actor = payload.get("actor")
    note = payload.get("note")
    with write_lock:
        begin(conn)
        try:
            plate = _get_plate(conn, plate_code)
            try:
                cur = conn.execute(
                    """
                    INSERT INTO decontaminations(
                        plate_id, decontaminated_at, actor, note, created_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (plate["id"], iso(at), actor, note, now_iso()),
                )
            except sqlite3.IntegrityError as exc:
                if "UNIQUE" in str(exc).upper():
                    raise ApiError(409, "DUPLICATE_DECONTAMINATION",
                                   "this decontamination event already exists")
                raise
            event_id = cur.lastrowid
            audit_row = audit(
                conn,
                "PLATE_DECONTAMINATED",
                "decontaminations",
                event_id,
                {"plate_code": plate_code, "at": iso(at), "actor": actor,
                 "note": note},
            )
            commit(conn)
            return {
                "decontamination_id": event_id,
                "plate_code": plate_code,
                "decontaminated_at": iso(at),
                "audit": audit_row,
            }
        except Exception:
            rollback(conn)
            raise


def grade_exposure(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    with write_lock:
        begin(conn)
        try:
            result = _grade_exposure_tx(conn, payload)
            commit(conn)
            return result
        except Exception:
            rollback(conn)
            raise


def _is_interval_unique_conflict(message: str) -> bool:
    normalized = message.lower().replace(" ", "")
    return (
        "uniqueconstraintfailed" in normalized
        and "exposures.plate_id" in normalized
        and "exposures.installed_at" in normalized
        and "exposures.removed_at" in normalized
    )


def _grade_exposure_tx(
    conn: sqlite3.Connection, payload: dict[str, Any]
) -> dict[str, Any]:
    version = get_version_by_hash(conn, payload["version_hash"])
    config = get_version_config(version)
    plate = _get_plate(conn, payload.get("plate_code"))
    installed = utc_datetime(payload.get("installed_at"), "installed_at")
    removed = utc_datetime(payload.get("removed_at"), "removed_at")
    photo = utc_datetime(payload.get("photo_at"), "photo_at")
    if removed < installed:
        raise ApiError(400, "INVALID_INTERVAL", "removed_at precedes installed_at")
    mount_position = payload.get("mount_position")
    unexposed = _validate_cell_index_set(
        payload.get("unexposed_cells", []), plate, "unexposed_cells"
    )
    contaminated = _validate_cell_index_set(
        payload.get("contaminated_cells", []), plate, "contaminated_cells"
    )
    cell_counts = _validate_cell_counts(
        payload.get("cell_counts", []), plate, len(config["size_bins"]), unexposed
    )
    actor = payload.get("actor")

    upload_payload = {
        "version_hash": version["version_hash"],
        "plate_code": plate["plate_code"],
        "mount_position": mount_position,
        "installed_at": iso(installed),
        "removed_at": iso(removed),
        "photo_at": iso(photo),
        "unexposed_cells": sorted(unexposed),
        "cell_counts": cell_counts,
    }
    upload_hash = db.sha256_text(db.canonical_json(upload_payload))
    duplicate = False
    inserted_new = False
    created_at = now_iso()

    exposure = conn.execute(
        "SELECT * FROM exposures WHERE upload_hash=?", (upload_hash,)
    ).fetchone()
    if exposure is None:
        try:
            cur = conn.execute(
                """
                INSERT INTO exposures(
                    segment_version_id, plate_id, mount_position,
                    installed_at, removed_at, photo_at, unexposed_json,
                    upload_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    version["id"], plate["id"], mount_position,
                    iso(installed), iso(removed), iso(photo),
                    json.dumps(sorted(unexposed)), upload_hash, created_at,
                ),
            )
        except sqlite3.IntegrityError as exc:
            message = str(exc)
            duplicate_exposure = conn.execute(
                "SELECT * FROM exposures WHERE upload_hash=?", (upload_hash,)
            ).fetchone()
            if duplicate_exposure is not None:
                exposure = duplicate_exposure
                duplicate = True
            elif _is_interval_unique_conflict(message):
                raise ApiError(
                    409,
                    "EXPOSURE_INTERVAL_CONFLICT",
                    "same plate interval already exists for this immutable segment version",
                    {
                        "version_hash": version["version_hash"],
                        "plate_code": plate["plate_code"],
                        "installed_at": iso(installed),
                        "removed_at": iso(removed),
                    },
                )
            raise
        else:
            inserted_new = True
            exposure = conn.execute(
                "SELECT * FROM exposures WHERE id=?", (cur.lastrowid,)
            ).fetchone()
            audit(
                conn,
                "EXPOSURE_RECORDED",
                "exposures",
                exposure["id"],
                {"upload_hash": upload_hash, "version_hash": version["version_hash"],
                 "plate_code": plate["plate_code"], "installed_at": iso(installed),
                 "removed_at": iso(removed)},
            )
            for cell_index, values in cell_counts.items():
                for bin_index, count in enumerate(values):
                    bin_spec = config["size_bins"][bin_index]
                    conn.execute(
                        """
                        INSERT INTO exposure_cell_counts(
                            exposure_id, cell_index, bin_index, bin_lo_um,
                            bin_hi_um, particle_count
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            exposure["id"], cell_index, bin_index,
                            bin_spec["lo_um"], bin_spec["hi_um"], count,
                        ),
                    )

    if not inserted_new:
        duplicate = True

    # Re-marking is idempotent. It covers both a new physical reuse and a
    # duplicate request arriving after another overlapping exposure.
    target_overlap_added = _mark_overlaps(conn, exposure, actor or "system")
    current_flag_added = target_overlap_added

    tolerance = config["photo_tolerance_seconds"]
    if abs((photo - removed).total_seconds()) > tolerance:
        current_flag_added = (
            _ensure_flag(
                conn, exposure["id"], "PHOTO_MISALIGNED", None,
                f"photo time differs from removal by more than {tolerance:g}s",
                actor or "system", audit_it=True,
            )
            or current_flag_added
        )

    for cell_index in sorted(contaminated):
        current_flag_added = (
            _ensure_flag(
                conn, exposure["id"], "LOCAL_CONTAMINATION", cell_index,
                "local contamination marked during upload",
                actor or "system", audit_it=True,
            )
            or current_flag_added
        )

    if duplicate and not current_flag_added:
        latest = conn.execute(
            "SELECT * FROM evaluation_events WHERE exposure_id=? ORDER BY id DESC LIMIT 1",
            (exposure["id"],),
        ).fetchone()
        if latest is not None:
            decision = json.loads(latest["decision_json"])
            decision["duplicate_upload"] = True
            decision["upload_hash"] = upload_hash
            return decision
    return evaluate_exposure(conn, exposure["id"], actor=actor, duplicate=duplicate,
                             upload_hash=upload_hash)


def _validate_cell_index_set(
    value: Any, plate: sqlite3.Row, field: str
) -> set[int]:
    if value is None:
        return set()
    if not isinstance(value, list):
        raise ApiError(400, "INVALID_CELLS", f"{field} must be a list")
    total = plate["grid_rows"] * plate["grid_cols"]
    result: set[int] = set()
    for raw in value:
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise ApiError(400, "INVALID_CELL", f"{field} entries must be integers")
        if raw < 0 or raw >= total:
            raise ApiError(400, "CELL_OUTSIDE_GRID", f"{field} contains {raw}")
        if raw in result:
            raise ApiError(400, "DUPLICATE_CELL", f"{field} contains duplicate {raw}")
        result.add(raw)
    return result


def _validate_cell_counts(
    value: Any, plate: sqlite3.Row, bin_count: int, unexposed: set[int]
) -> dict[int, list[int]]:
    if not isinstance(value, list):
        raise ApiError(400, "INVALID_COUNTS", "cell_counts must be a list")
    total = plate["grid_rows"] * plate["grid_cols"]
    result: dict[int, list[int]] = {}
    for i, raw in enumerate(value):
        if not isinstance(raw, dict):
            raise ApiError(400, "INVALID_COUNTS", f"cell_counts[{i}] must be an object")
        cell_index = raw.get("cell_index")
        if isinstance(cell_index, bool) or not isinstance(cell_index, int):
            raise ApiError(400, "INVALID_CELL", f"cell_counts[{i}].cell_index required")
        if cell_index < 0 or cell_index >= total:
            raise ApiError(400, "CELL_OUTSIDE_GRID", f"cell index {cell_index}")
        if cell_index in unexposed:
            raise ApiError(400, "UNEXPOSED_COUNTS",
                           f"cell {cell_index} is marked unexposed but has counts")
        by_bin = raw.get("by_bin")
        if not isinstance(by_bin, list) or len(by_bin) != bin_count:
            raise ApiError(400, "INVALID_BIN_COUNTS",
                           f"cell_counts[{i}].by_bin must contain {bin_count} values")
        values: list[int] = []
        for j, count in enumerate(by_bin):
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise ApiError(400, "INVALID_COUNT",
                               f"cell_counts[{i}].by_bin[{j}] must be nonnegative int")
            values.append(count)
        if cell_index in result:
            raise ApiError(400, "DUPLICATE_CELL_COUNTS", f"cell {cell_index} repeated")
        result[cell_index] = values
    return result


def _mark_overlaps(
    conn: sqlite3.Connection, exposure: sqlite3.Row, actor: str
) -> bool:
    others = conn.execute(
        """
        SELECT * FROM exposures
        WHERE plate_id=? AND id<>?
          AND installed_at < ? AND removed_at > ?
        """,
        (
            exposure["plate_id"], exposure["id"],
            exposure["removed_at"], exposure["installed_at"],
        ),
    ).fetchall()
    target_flag_added = False
    affected: list[int] = []
    for other in others:
        for exposure_id in (exposure["id"], other["id"]):
            inserted = _ensure_flag(
                conn, exposure_id, "OVERLAP", None,
                "same physical target was used during an overlapping interval",
                actor, audit_it=True,
            )
            if exposure_id == exposure["id"]:
                target_flag_added = target_flag_added or inserted
            elif inserted:
                affected.append(exposure_id)
    for exposure_id in affected:
        evaluate_exposure(conn, exposure_id, actor="system")
    return target_flag_added


def _ensure_flag(
    conn: sqlite3.Connection,
    exposure_id: int,
    kind: str,
    cell_index: int | None,
    note: str,
    actor: str | None,
    audit_it: bool,
) -> bool:
    try:
        cur = conn.execute(
            """
            INSERT INTO exposure_flags(
                exposure_id, kind, cell_index, note, actor, marked_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (exposure_id, kind, cell_index, note, actor, now_iso()),
        )
    except sqlite3.IntegrityError as exc:
        if "UNIQUE" in str(exc).upper():
            return False
        raise
    if audit_it:
        audit(
            conn,
            "EXPOSURE_FLAG_APPENDED",
            "exposure_flags",
            cur.lastrowid,
            {"exposure_id": exposure_id, "kind": kind, "cell_index": cell_index,
             "note": note, "actor": actor},
        )
    return True


def add_flag(conn: sqlite3.Connection, exposure_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    kind = payload.get("kind")
    allowed = HARD_FLAG_KINDS
    if kind not in allowed:
        raise ApiError(400, "INVALID_FLAG", f"kind must be one of {sorted(allowed)}")
    cell = payload.get("cell_index")
    with write_lock:
        begin(conn)
        try:
            exposure = conn.execute(
                "SELECT * FROM exposures WHERE id=?", (exposure_id,)
            ).fetchone()
            if exposure is None:
                raise ApiError(404, "EXPOSURE_NOT_FOUND", "unknown exposure")
            plate = conn.execute(
                "SELECT * FROM plates WHERE id=?", (exposure["plate_id"],)
            ).fetchone()
            total = plate["grid_rows"] * plate["grid_cols"]
            if kind == "LOCAL_CONTAMINATION":
                if isinstance(cell, bool) or not isinstance(cell, int):
                    raise ApiError(400, "CELL_REQUIRED",
                                   "LOCAL_CONTAMINATION requires cell_index")
                if cell < 0 or cell >= total:
                    raise ApiError(400, "CELL_OUTSIDE_GRID", f"cell index {cell}")
            elif cell is not None:
                raise ApiError(400, "CELL_NOT_ALLOWED",
                               f"{kind} flag cannot target a cell")
            inserted = _ensure_flag(
                conn, exposure_id, kind, cell,
                payload.get("note") or "manually marked",
                payload.get("actor"), audit_it=True,
            )
            decision = evaluate_exposure(conn, exposure_id,
                                         actor=payload.get("actor"))
            decision["flag_duplicate"] = not inserted
            commit(conn)
            return decision
        except Exception:
            rollback(conn)
            raise


def evaluate_exposure(
    conn: sqlite3.Connection,
    exposure_id: int,
    actor: str | None = None,
    duplicate: bool = False,
    upload_hash: str | None = None,
    recompute_streak_after: bool = True,
) -> dict[str, Any]:
    exposure = conn.execute(
        "SELECT * FROM exposures WHERE id=?", (exposure_id,)
    ).fetchone()
    if exposure is None:
        raise ApiError(404, "EXPOSURE_NOT_FOUND", "unknown exposure")
    version = conn.execute(
        "SELECT * FROM segment_versions WHERE id=?",
        (exposure["segment_version_id"],),
    ).fetchone()
    plate = conn.execute(
        "SELECT * FROM plates WHERE id=?", (exposure["plate_id"],)
    ).fetchone()
    config = get_version_config(version)

    installed = parse_iso(exposure["installed_at"])
    removed = parse_iso(exposure["removed_at"])
    photo = parse_iso(exposure["photo_at"])
    unexposed = set(json.loads(exposure["unexposed_json"]))
    total_cells = plate["grid_rows"] * plate["grid_cols"]
    effective_cell_count = total_cells - len(unexposed)
    effective_area = effective_cell_count * plate["cell_area_m2"]

    flag_rows = conn.execute(
        "SELECT * FROM exposure_flags WHERE exposure_id=? ORDER BY id",
        (exposure_id,),
    ).fetchall()
    flags = [
        {
            "kind": row["kind"],
            "cell_index": row["cell_index"],
            "note": row["note"],
            "actor": row["actor"],
            "marked_at": row["marked_at"],
        }
        for row in flag_rows
    ]
    ranges = clock_ranges(conn, exposure["segment_version_id"])
    clock_invalid = intersects_clock_ranges(installed, removed, ranges)

    count_rows = conn.execute(
        """
        SELECT cell_index, bin_index, particle_count
        FROM exposure_cell_counts WHERE exposure_id=?
        """,
        (exposure_id,),
    ).fetchall()
    matrix: dict[int, list[int]] = {
        i: [0] * len(config["size_bins"]) for i in range(total_cells)
        if i not in unexposed
    }
    for row in count_rows:
        matrix.setdefault(row["cell_index"], [0] * len(config["size_bins"]))
        matrix[row["cell_index"]][row["bin_index"]] = row["particle_count"]

    totals = [0] * len(config["size_bins"])
    for values in matrix.values():
        for i, value in enumerate(values):
            totals[i] += value
    area_limits = config["area_limits_particles_per_m2"]
    grid_limits = config["grid_limits_particles_per_m2"]
    densities = [total / effective_area for total in totals]
    area_failures = [
        {
            "bin_index": i,
            "observed_particles_per_m2": densities[i],
            "limit_particles_per_m2": area_limits[i],
            "excess_particles_per_m2": max(0.0, densities[i] - area_limits[i]),
        }
        for i in range(len(totals))
        if densities[i] > area_limits[i]
    ]

    min_failing_grid = None
    nearest_grid = None
    for cell_index in sorted(matrix):
        values = matrix[cell_index]
        cell_densities = [value / plate["cell_area_m2"] for value in values]
        failed_bins = []
        worst_ratio = 0.0
        for i, density in enumerate(cell_densities):
            limit = grid_limits[i]
            if limit is not None and density > limit:
                failed_bins.append({
                    "bin_index": i,
                    "observed_particles_per_m2": density,
                    "limit_particles_per_m2": limit,
                    "excess_particles_per_m2": density - limit,
                })
            if limit is not None and limit > 0:
                worst_ratio = max(worst_ratio, density / limit)
        candidate = {
            "cell_index": cell_index,
            "row": cell_index // plate["grid_cols"],
            "col": cell_index % plate["grid_cols"],
            "counts_by_bin": values,
            "particles_per_m2_by_bin": cell_densities,
            "failed_bins": failed_bins,
        }
        if worst_ratio > 0 and (
            nearest_grid is None
            or worst_ratio > nearest_grid["limit_ratio"]
        ):
            candidate["limit_ratio"] = worst_ratio
            nearest_grid = candidate
        if failed_bins and min_failing_grid is None:
            # Row-major order gives the deterministic "minimum" failed grid.
            candidate["limit_ratio"] = worst_ratio
            min_failing_grid = candidate

    pressure = _pressure_status(conn, exposure, config)
    invalid_reasons: list[str] = []
    invalid_details: list[dict[str, Any]] = []
    for flag in flags:
        invalid_reasons.append(f"HARD_FLAG:{flag['kind']}")
        invalid_details.append({"type": "flag", **flag})
    if clock_invalid and not any(
        flag["kind"] == "CLOCK_REGRESSION" for flag in flags
    ):
        invalid_reasons.append("HARD_FLAG:CLOCK_REGRESSION")
        invalid_details.append({"type": "clock_regression", "ranges": [
            [iso(a), iso(b) if b < parse_iso(FAR_FUTURE) else None]
            for a, b in ranges
            if installed < b and removed > a
        ]})
    duration = (removed - installed).total_seconds()
    if duration < config["min_exposure_seconds"]:
        invalid_reasons.append("EXPOSURE_TOO_SHORT")
        invalid_details.append({
            "type": "exposure_duration",
            "observed_seconds": duration,
            "minimum_seconds": config["min_exposure_seconds"],
        })
    photo_delta = abs((photo - removed).total_seconds())
    if photo_delta > config["photo_tolerance_seconds"] and not any(
        flag["kind"] == "PHOTO_MISALIGNED" for flag in flags
    ):
        invalid_reasons.append("PHOTO_MISALIGNED")
        invalid_details.append({
            "type": "photo",
            "photo_removal_delta_seconds": photo_delta,
            "tolerance_seconds": config["photo_tolerance_seconds"],
        })
    if not pressure["valid"]:
        invalid_reasons.extend(pressure["failure_reasons"])
        invalid_details.append({"type": "pressure", **pressure})

    if invalid_reasons:
        result = "INVALID"
    elif area_failures or min_failing_grid is not None:
        result = "FAIL"
    else:
        result = "PASS"

    streak_before = conn.execute(
        "SELECT consecutive_passes FROM segment_streaks WHERE segment_version_id=?",
        (version["id"],),
    ).fetchone()["consecutive_passes"]
    evaluation_id = conn.execute(
        "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM evaluation_events"
    ).fetchone()["next_id"]
    streak_after = _recompute_streak(
        conn,
        version["id"],
        exposure_id,
        result,
        persist=recompute_streak_after,
    )
    current_streak = streak_after[version["id"]]

    decision = {
        "result": result,
        "grade": result,
        "evaluation_event_id": evaluation_id,
        "duplicate_upload": duplicate,
        "upload_hash": upload_hash or exposure["upload_hash"],
        "exposure_id": exposure["id"],
        "segment": {
            "segment_no": version["segment_no"],
            "version": version["version"],
            "version_hash": version["version_hash"],
            "immutable_version": True,
        },
        "plate": {
            "plate_id": plate["id"],
            "plate_code": plate["plate_code"],
            "grid_rows": plate["grid_rows"],
            "grid_cols": plate["grid_cols"],
            "cell_area_m2": plate["cell_area_m2"],
            "mount_position": exposure["mount_position"],
        },
        "interval": {
            "installed_at": exposure["installed_at"],
            "removed_at": exposure["removed_at"],
            "photo_at": exposure["photo_at"],
            "exposure_seconds": duration,
        },
        "effective_exposure": {
            "total_grid_cells": total_cells,
            "unexposed_cells": sorted(unexposed),
            "effective_cell_count": effective_cell_count,
            "effective_area_m2": effective_area,
            "formula": "effective_area_m2 = effective_cell_count * cell_area_m2",
        },
        "particle_bins": [
            {
                "bin_index": i,
                "lo_um": config["size_bins"][i]["lo_um"],
                "hi_um": config["size_bins"][i]["hi_um"],
                "particles": totals[i],
                "particles_per_m2": densities[i],
                "area_limit_particles_per_m2": area_limits[i],
                "grid_limit_particles_per_m2": grid_limits[i],
                "passes_area_limit": densities[i] <= area_limits[i],
            }
            for i in range(len(totals))
        ],
        "area_failures": area_failures,
        "minimum_failing_grid": min_failing_grid,
        "nearest_grid_to_limit": nearest_grid,
        "pressure": pressure,
        "flags": flags,
        "invalid_reasons": invalid_reasons,
        "invalid_details": invalid_details,
        "consecutive_passes_before": streak_before,
        "consecutive_passes": current_streak,
        "streak_bound_to_version_hash": version["version_hash"],
        "judgement_basis": (
            f"有效面积={effective_area:g}m²；"
            "各粒径区间单位面积颗粒量与面积限值比较；"
            "网格按同一面积密度（或更严格网格限值）逐格比较；"
            "硬污染/重叠/时钟/照片或压力证据失效时判 INVALID，连续合格清零。"
        ),
        "next_valid_sampling_window": next_sampling_window(
            conn,
            version["version_hash"],
            plate_code=plate["plate_code"],
            at=removed,
            _transaction=False,
        ),
    }

    conn.execute(
        """
        INSERT INTO evaluation_events(
            id, exposure_id, result, grade, decision_json, evaluated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            evaluation_id, exposure["id"], result, result,
            json.dumps(decision, ensure_ascii=False), now_iso(),
        ),
    )
    audit_row = audit(
        conn,
        "EXPOSURE_GRADED",
        "evaluation_events",
        evaluation_id,
        {
            "exposure_id": exposure_id,
            "result": result,
            "version_hash": version["version_hash"],
            "consecutive_passes": current_streak,
            "actor": actor,
            "invalid_reasons": invalid_reasons,
            "area_failures": area_failures,
            "minimum_failing_grid": min_failing_grid,
        },
    )
    decision["audit"] = audit_row
    return decision


def _pressure_status(
    conn: sqlite3.Connection,
    exposure: sqlite3.Row,
    config: dict[str, Any],
) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT sampled_at, pressure_pa FROM pressure_samples
        WHERE segment_version_id=? AND sampled_at>=? AND sampled_at<=?
        ORDER BY sampled_at
        """,
        (
            exposure["segment_version_id"],
            exposure["installed_at"],
            exposure["removed_at"],
        ),
    ).fetchall()
    installed = parse_iso(exposure["installed_at"])
    removed = parse_iso(exposure["removed_at"])
    duration = max((removed - installed).total_seconds(), 0.0)
    min_samples = int(config["pressure_min_samples"])
    required_coverage = float(config["pressure_required_coverage"])
    max_gap = float(config["pressure_max_gap_seconds"])
    failures: list[str] = []

    if len(rows) < min_samples:
        failures.append("PRESSURE_TOO_FEW_SAMPLES")
    gaps = []
    coverage_ratio = 0.0
    first = last = None
    if rows:
        times = [parse_iso(row["sampled_at"]) for row in rows]
        first, last = times[0], times[-1]
        gap_values = [
            (b - a).total_seconds() for a, b in zip(times, times[1:])
        ]
        gaps = gap_values
        if any(gap > max_gap for gap in gap_values):
            failures.append("PRESSURE_GAP_TOO_LARGE")
        before = max(0.0, (first - installed).total_seconds())
        after = max(0.0, (removed - last).total_seconds())
        coverage_ratio = max(0.0, (duration - before - after) / duration) if duration else 1.0
        if coverage_ratio < required_coverage:
            failures.append("PRESSURE_COVERAGE_TOO_LOW")

    return {
        "valid": not failures,
        "sample_count": len(rows),
        "minimum_samples": min_samples,
        "first_sample_at": iso(first) if first else None,
        "last_sample_at": iso(last) if last else None,
        "gaps_seconds": gaps,
        "maximum_gap_seconds": max_gap,
        "coverage_ratio": coverage_ratio,
        "required_coverage": required_coverage,
        "failure_reasons": failures,
        "values_pa": [row["pressure_pa"] for row in rows],
    }


def _recompute_streak(
    conn: sqlite3.Connection,
    version_id: int,
    pending_exposure_id: int | None = None,
    pending_result: str | None = None,
    persist: bool = True,
) -> dict[int, int]:
    rows = conn.execute(
        """
        SELECT e.id AS exposure_id, e.installed_at AS installed_at,
               e.removed_at AS removed_at, ev.result AS result
        FROM evaluation_events ev
        JOIN exposures e ON e.id=ev.exposure_id
        JOIN (
            SELECT exposure_id, MAX(id) AS max_id
            FROM evaluation_events GROUP BY exposure_id
        ) latest ON latest.exposure_id=ev.exposure_id AND latest.max_id=ev.id
        WHERE e.segment_version_id=?
        ORDER BY e.installed_at, e.removed_at, e.id
        """,
        (version_id,),
    ).fetchall()
    ordered: list[tuple[datetime, datetime, int, str]] = [
        (
            parse_iso(r["installed_at"]),
            parse_iso(r["removed_at"]),
            r["exposure_id"],
            r["result"],
        )
        for r in rows
    ]
    if pending_exposure_id is not None:
        pending = conn.execute(
            "SELECT * FROM exposures WHERE id=?", (pending_exposure_id,)
        ).fetchone()
        ordered = [
            item for item in ordered if item[2] != pending_exposure_id
        ]
        ordered.append((
            parse_iso(pending["installed_at"]),
            parse_iso(pending["removed_at"]),
            pending_exposure_id,
            pending_result or "INVALID",
        ))
        ordered.sort(key=lambda item: (item[0], item[1], item[2]))

    streak = 0
    last_exposure: int | None = None
    for _, _, exposure_id, result in ordered:
        if result == "PASS":
            streak += 1
            last_exposure = exposure_id
        else:
            streak = 0
            last_exposure = exposure_id
    if persist:
        conn.execute(
            """
            INSERT INTO segment_streaks(
                segment_version_id, consecutive_passes, last_exposure_id, updated_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(segment_version_id) DO UPDATE SET
                consecutive_passes=excluded.consecutive_passes,
                last_exposure_id=excluded.last_exposure_id,
                updated_at=excluded.updated_at
            """,
            (version_id, streak, last_exposure, now_iso()),
        )
    return {version_id: streak}


def get_streak(conn: sqlite3.Connection, version_hash: str) -> dict[str, Any]:
    with write_lock:
        begin(conn)
        try:
            version = get_version_by_hash(conn, version_hash)
            _recompute_streak(conn, version["id"])
            row = conn.execute(
                "SELECT * FROM segment_streaks WHERE segment_version_id=?",
                (version["id"],),
            ).fetchone()
            result = {
                "segment_no": version["segment_no"],
                "version": version["version"],
                "version_hash": version["version_hash"],
                "consecutive_passes": row["consecutive_passes"],
                "last_exposure_id": row["last_exposure_id"],
                "updated_at": row["updated_at"],
                "immutable_version": True,
            }
            commit(conn)
            return result
        except Exception:
            rollback(conn)
            raise


def get_exposure(conn: sqlite3.Connection, exposure_id: int) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM exposures WHERE id=?", (exposure_id,)).fetchone()
    if row is None:
        raise ApiError(404, "EXPOSURE_NOT_FOUND", "unknown exposure")
    latest = conn.execute(
        "SELECT * FROM evaluation_events WHERE exposure_id=? ORDER BY id DESC LIMIT 1",
        (exposure_id,),
    ).fetchone()
    return {
        "exposure": dict(row),
        "latest_decision": json.loads(latest["decision_json"]) if latest else None,
    }


def next_sampling_window(
    conn: sqlite3.Connection,
    version_hash: str,
    plate_code: str | None = None,
    at: datetime | str | None = None,
    _transaction: bool = True,
) -> dict[str, Any]:
    def query() -> dict[str, Any]:
        version = get_version_by_hash(conn, version_hash)
        config = get_version_config(version)
        if at is None:
            at_dt = datetime.now(timezone.utc)
        elif isinstance(at, datetime):
            at_dt = at.astimezone(timezone.utc)
        else:
            at_dt = utc_datetime(at, "at")

        active = active_clock_regression(conn, version["id"], at_dt)
        latest_segment = conn.execute(
            """
            SELECT MAX(removed_at) AS latest FROM exposures
            WHERE segment_version_id=?
            """,
            (version["id"],),
        ).fetchone()["latest"]
        segment_ready = at_dt
        if latest_segment:
            segment_ready = max(
                segment_ready,
                parse_iso(latest_segment)
                + timedelta(seconds=config["segment_cooldown_seconds"]),
            )

        pressure_requirements = {
            "minimum_samples": int(config["pressure_min_samples"]),
            "required_coverage": config["pressure_required_coverage"],
            "maximum_gap_seconds": config["pressure_max_gap_seconds"],
            "first_sample_at_or_before_start": True,
        }
        prerequisites: list[str] = []
        plate_window: dict[str, Any] | None = None

        if active:
            prerequisites.append("CLOCK_RESYNC_REQUIRED")
            segment_window = {
                "available": False,
                "start_at": None,
                "recommended_remove_at": None,
                "reason": "CLOCK_REGRESSION",
            }
        else:
            start = segment_ready
            end = start + timedelta(seconds=config["target_exposure_seconds"])
            segment_window = {
                "available": True,
                "start_at": iso(start),
                "recommended_remove_at": iso(end),
                "target_exposure_seconds": config["target_exposure_seconds"],
                "reason": "OK",
            }

        if plate_code is not None:
            plate = _get_plate(conn, plate_code)
            latest_plate = conn.execute(
                "SELECT MAX(removed_at) AS latest FROM exposures WHERE plate_id=?",
                (plate["id"],),
            ).fetchone()["latest"]
            latest_decon = conn.execute(
                "SELECT MAX(decontaminated_at) AS latest FROM decontaminations WHERE plate_id=?",
                (plate["id"],),
            ).fetchone()["latest"]
            needs_decon = bool(
                latest_plate
                and (latest_decon is None or parse_iso(latest_decon) <= parse_iso(latest_plate))
            )
            if needs_decon:
                prerequisites.append("PLATE_DECONTAMINATION_REQUIRED")
            plate_ready = segment_ready
            if latest_plate:
                plate_ready = max(
                    plate_ready,
                    parse_iso(latest_plate)
                    + timedelta(seconds=config["plate_cooldown_seconds"]),
                )
            if latest_decon:
                plate_ready = max(plate_ready, parse_iso(latest_decon))
            if active or needs_decon:
                plate_window = {
                    "available": False,
                    "start_at": None,
                    "recommended_remove_at": None,
                    "reason": "CLOCK_REGRESSION" if active else "DECONTAMINATION_REQUIRED",
                }
            else:
                plate_window = {
                    "available": True,
                    "start_at": iso(plate_ready),
                    "recommended_remove_at": iso(
                        plate_ready
                        + timedelta(seconds=config["target_exposure_seconds"])
                    ),
                    "reason": "OK",
                }
            plate_window.update({
                "plate_code": plate_code,
                "latest_use_removed_at": latest_plate,
                "latest_decontamination_at": latest_decon,
                "decontamination_required": needs_decon,
            })

        return {
            "segment_no": version["segment_no"],
            "version": version["version"],
            "version_hash": version["version_hash"],
            "evaluated_at": iso(at_dt),
            "active_clock_regression": active,
            "prerequisites": prerequisites,
            "segment_window": segment_window,
            "plate_window": plate_window,
            "pressure_requirements": pressure_requirements,
        }

    if _transaction:
        with write_lock:
            begin(conn)
            try:
                result = query()
                commit(conn)
                return result
            except Exception:
                rollback(conn)
                raise
    return query()


def audit_tail(conn: sqlite3.Connection, limit: int = 50) -> dict[str, Any]:
    limit = max(1, min(int(limit), 500))
    rows = conn.execute(
        "SELECT * FROM audit_log ORDER BY seq DESC LIMIT ?", (limit,)
    ).fetchall()
    entries = [
        {
            "seq": row["seq"],
            "event_type": row["event_type"],
            "entity_table": row["entity_table"],
            "entity_id": row["entity_id"],
            "payload": json.loads(row["payload_json"]),
            "prev_hash": row["prev_hash"],
            "entry_hash": row["entry_hash"],
            "created_at": row["created_at"],
        }
        for row in reversed(rows)
    ]
    return {"entries": entries, "count": len(entries)}
