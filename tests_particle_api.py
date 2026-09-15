from __future__ import annotations

import os
import tempfile
import threading
import unittest
import urllib.request
import json
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError

from particle_api import db, server
from particle_api.service import (
    ApiError,
    add_flag,
    add_pressure_samples,
    decontaminate_plate,
    grade_exposure,
    next_sampling_window,
    register_plate,
    register_segment,
    resync_clock,
    declare_clock_regression,
)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


class ServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        self.conn = db.connect(self.path)
        db.initialize(self.conn)
        self.base = datetime(2026, 9, 15, 10, 0, tzinfo=timezone.utc)
        self.segment = self._register_segment()
        register_plate(self.conn, {
            "plate_code": "T-01",
            "grid_rows": 2,
            "grid_cols": 2,
            "cell_area_m2": 0.01,
        })

    def tearDown(self) -> None:
        self.conn.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.path + suffix)
            except FileNotFoundError:
                pass

    def _register_segment(self):
        return register_segment(self.conn, {
            "segment_no": "SP-100",
            "config": {
                "size_bins": [
                    {"lo_um": 0.0, "hi_um": 100.0},
                    {"lo_um": 100.0, "hi_um": None},
                ],
                "area_limits_particles_per_m2": [1000.0, 100.0],
                "grid_limits_particles_per_m2": [None, 100.0],
                "min_exposure_seconds": 50.0,
                "photo_tolerance_seconds": 5.0,
                "target_exposure_seconds": 60.0,
                "pressure_min_samples": 3,
                "pressure_required_coverage": 0.9,
                "pressure_max_gap_seconds": 25.0,
                "plate_cooldown_seconds": 10.0,
            },
        })

    def _pressure(self, start: datetime, end: datetime, step: float = 20.0):
        samples = []
        t = start
        while t <= end:
            samples.append({"sampled_at": iso(t), "pressure_pa": 120.0})
            t += timedelta(seconds=step)
        if samples[-1]["sampled_at"] != iso(end):
            samples.append({"sampled_at": iso(end), "pressure_pa": 120.0})
        add_pressure_samples(self.conn, {
            "version_hash": self.segment["version_hash"],
            "samples": samples,
        })

    def _grade(self, cell_counts, start=None, minutes=1, contaminated=None, unexposed=None):
        start = start or self.base
        end = start + timedelta(minutes=minutes)
        self._pressure(start, end)
        return grade_exposure(self.conn, {
            "version_hash": self.segment["version_hash"],
            "plate_code": "T-01",
            "mount_position": "A",
            "installed_at": iso(start),
            "removed_at": iso(end),
            "photo_at": iso(end),
            "unexposed_cells": unexposed or [],
            "contaminated_cells": contaminated or [],
            "cell_counts": cell_counts,
            "actor": "tester",
        })

    def test_density_effective_area_pass_and_streak(self):
        counts = [{"cell_index": 0, "by_bin": [0, 0]}]
        first = self._grade(counts, start=self.base)
        self.assertEqual(first["result"], "PASS")
        self.assertEqual(first["consecutive_passes"], 1)
        self.assertAlmostEqual(first["effective_exposure"]["effective_area_m2"], 0.04)

        second = self._grade(
            [{"cell_index": 0, "by_bin": [0, 0]}],
            start=self.base + timedelta(hours=1),
        )
        self.assertEqual(second["result"], "PASS")
        self.assertEqual(second["consecutive_passes"], 2)
        self.assertIn("有效面积", second["judgement_basis"])

    def test_effective_area_excludes_unexposed_and_fails_minimum_grid(self):
        # Effective area 3*0.01=0.03 m². 3 large particles => 100/m² = limit;
        # one cell has two large particles => 200/m² and is minimum failed grid.
        decision = self._grade([
            {"cell_index": 0, "by_bin": [0, 2]},
            {"cell_index": 1, "by_bin": [0, 1]},
        ], unexposed=[3])
        self.assertEqual(decision["result"], "FAIL")
        self.assertEqual(decision["minimum_failing_grid"]["cell_index"], 0)
        self.assertEqual(
            decision["minimum_failing_grid"]["failed_bins"][0]["bin_index"], 1
        )
        self.assertEqual(decision["consecutive_passes"], 0)

    def test_overlap_revokes_both_but_retains_audit_chain(self):
        first = self._grade(
            [{"cell_index": 0, "by_bin": [0, 0]}],
            start=self.base,
        )
        self.assertEqual(first["consecutive_passes"], 1)

        # Overlapping second use of same physical plate.
        overlap_start = self.base + timedelta(seconds=30)
        overlap_end = overlap_start + timedelta(minutes=1)
        self._pressure(overlap_start, overlap_end)
        second = grade_exposure(self.conn, {
            "version_hash": self.segment["version_hash"],
            "plate_code": "T-01",
            "installed_at": iso(overlap_start),
            "removed_at": iso(overlap_end),
            "photo_at": iso(overlap_end),
            "cell_counts": [{"cell_index": 1, "by_bin": [0, 0]}],
        })
        self.assertEqual(second["result"], "INVALID")
        self.assertIn("HARD_FLAG:OVERLAP", second["invalid_reasons"])
        self.assertEqual(second["consecutive_passes"], 0)

        # Both exposures still have immutable histories and current statuses.
        first_latest = self.conn.execute(
            "SELECT COUNT(*) c FROM evaluation_events WHERE exposure_id=1 AND result='INVALID'"
        ).fetchone()["c"]
        self.assertGreaterEqual(first_latest, 1)
        audit_status = db.verify_audit_chain(self.conn)
        self.assertTrue(audit_status["ok"])

    def test_local_contamination_revokes_after_pass(self):
        start = self.base + timedelta(hours=2)
        decision = self._grade([{"cell_index": 0, "by_bin": [0, 0]}], start=start)
        self.assertEqual(decision["consecutive_passes"], 1)
        updated = add_flag(self.conn, decision["exposure_id"], {
            "kind": "LOCAL_CONTAMINATION",
            "cell_index": 2,
            "note": "oil spot",
            "actor": "inspector",
        })
        self.assertEqual(updated["result"], "INVALID")
        self.assertEqual(updated["consecutive_passes"], 0)
        flags = [f["kind"] for f in updated["flags"]]
        self.assertIn("LOCAL_CONTAMINATION", flags)

    def test_clock_backwards_marks_interval_and_resync_calculates_window(self):
        start = self.base + timedelta(hours=3)
        end = start + timedelta(minutes=1)
        good_samples = [
            {"sampled_at": iso(start), "pressure_pa": 1.0},
            {"sampled_at": iso(start + timedelta(seconds=20)), "pressure_pa": 1.0},
            {"sampled_at": iso(start + timedelta(seconds=40)), "pressure_pa": 1.0},
            {"sampled_at": iso(end), "pressure_pa": 1.0},
        ]
        add_pressure_samples(self.conn, {
            "version_hash": self.segment["version_hash"],
            "samples": good_samples,
        })
        decision = grade_exposure(self.conn, {
            "version_hash": self.segment["version_hash"],
            "plate_code": "T-01",
            "installed_at": iso(start),
            "removed_at": iso(end),
            "photo_at": iso(end),
            "cell_counts": [{"cell_index": 0, "by_bin": [0, 0]}],
        })
        self.assertEqual(decision["result"], "PASS")

        # A delayed sample moves clock backwards and intersects the interval.
        add_pressure_samples(self.conn, {
            "version_hash": self.segment["version_hash"],
            "samples": [{"sampled_at": iso(start + timedelta(seconds=10)), "pressure_pa": 1.0}],
        })
        # Re-grading same evidence returns latest duplicate decision, now INVALID.
        duplicate = grade_exposure(self.conn, {
            "version_hash": self.segment["version_hash"],
            "plate_code": "T-01",
            "installed_at": iso(start),
            "removed_at": iso(end),
            "photo_at": iso(end),
            "cell_counts": [{"cell_index": 0, "by_bin": [0, 0]}],
        })
        self.assertTrue(duplicate["duplicate_upload"])
        self.assertEqual(duplicate["result"], "INVALID")
        self.assertEqual(duplicate["consecutive_passes"], 0)

        blocked = next_sampling_window(
            self.conn, self.segment["version_hash"], "T-01", end
        )
        self.assertFalse(blocked["segment_window"]["available"])
        self.assertIn("CLOCK_RESYNC_REQUIRED", blocked["prerequisites"])

        resync_clock(self.conn, {
            "version_hash": self.segment["version_hash"],
            "at": iso(end + timedelta(minutes=5)),
        })
        future = end + timedelta(hours=1)
        window = next_sampling_window(
            self.conn, self.segment["version_hash"], "T-01", future
        )
        # Existing plate needs decontamination before reuse.
        self.assertTrue(window["segment_window"]["available"])
        self.assertFalse(window["plate_window"]["available"])
        self.assertIn("PLATE_DECONTAMINATION_REQUIRED", window["prerequisites"])

        decontaminate_plate(self.conn, {
            "plate_code": "T-01",
            "decontaminated_at": iso(future),
        })
        window2 = next_sampling_window(
            self.conn, self.segment["version_hash"], "T-01", future
        )
        self.assertTrue(window2["plate_window"]["available"])

    def test_concurrent_same_plate_upload_is_not_double_counted(self):
        results = []
        errors = []
        start = self.base + timedelta(hours=4)
        end = start + timedelta(minutes=1)
        self._pressure(start, end)
        payload = {
            "version_hash": self.segment["version_hash"],
            "plate_code": "T-01",
            "installed_at": iso(start),
            "removed_at": iso(end),
            "photo_at": iso(end),
            "cell_counts": [{"cell_index": 0, "by_bin": [0, 0]}],
        }

        def worker():
            try:
                results.append(grade_exposure(self.conn, payload))
            except ApiError as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse(errors)
        self.assertEqual(len(results), 8)
        self.assertEqual(sum(not r["duplicate_upload"] for r in results), 1)
        self.assertEqual(sum(r["duplicate_upload"] for r in results), 7)
        exposure_count = self.conn.execute("SELECT COUNT(*) c FROM exposures").fetchone()["c"]
        self.assertEqual(exposure_count, 1)
        row_count = self.conn.execute(
            "SELECT COUNT(*) c FROM exposure_cell_counts WHERE bin_index=0"
        ).fetchone()["c"]
        self.assertEqual(row_count, 1)

    def test_late_pressure_samples_can_upgrade_invalid_to_pass(self):
        start = self.base + timedelta(hours=5)
        end = start + timedelta(minutes=1)
        initial = grade_exposure(self.conn, {
            "version_hash": self.segment["version_hash"],
            "plate_code": "T-01",
            "installed_at": iso(start),
            "removed_at": iso(end),
            "photo_at": iso(end),
            "cell_counts": [{"cell_index": 0, "by_bin": [0, 0]}],
        })
        self.assertEqual(initial["result"], "INVALID")
        self.assertIn("PRESSURE_TOO_FEW_SAMPLES", initial["invalid_reasons"])
        self.assertEqual(initial["consecutive_passes"], 0)

        add_pressure_samples(self.conn, {
            "version_hash": self.segment["version_hash"],
            "samples": [
                {"sampled_at": iso(start), "pressure_pa": 100},
                {"sampled_at": iso(start + timedelta(seconds=20)), "pressure_pa": 100},
                {"sampled_at": iso(start + timedelta(seconds=40)), "pressure_pa": 100},
                {"sampled_at": iso(end), "pressure_pa": 100},
            ],
        })
        latest = self.conn.execute(
            """
            SELECT decision_json FROM evaluation_events
            WHERE exposure_id=? ORDER BY id DESC LIMIT 1
            """,
            (initial["exposure_id"],),
        ).fetchone()
        decision = json.loads(latest["decision_json"])
        self.assertEqual(decision["result"], "PASS")
        self.assertEqual(decision["consecutive_passes"], 1)
        self.assertTrue(db.verify_audit_chain(self.conn)["ok"])

    def test_immutable_version_hashes_separate_streak(self):
        first_version = self.segment["version_hash"]
        # Changed limit produces a new immutable version and independent streak.
        new = register_segment(self.conn, {
            "segment_no": "SP-100",
            "config": {
                "size_bins": [{"lo_um": 0.0, "hi_um": 100.0}],
                "area_limits_particles_per_m2": [900.0],
                "pressure_min_samples": 1,
                "pressure_required_coverage": 0.0,
                "pressure_max_gap_seconds": 60.0,
            },
        })
        self.assertNotEqual(new["version_hash"], first_version)
        self.assertEqual(new["version"], 2)
        self.assertEqual(new["consecutive_passes"], 0)


class HttpSmokeTest(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        self.httpd = server.create_server("127.0.0.1", 0, self.path)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.httpd.server_address[1]

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join()
        self.httpd.conn.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.path + suffix)
            except FileNotFoundError:
                pass

    def post(self, path, payload):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req) as response:
                return response.status, json.loads(response.read())
        except HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_register_and_verify_audit(self):
        status, body = self.post("/api/segments", {
            "segment_no": "HTTP",
            "config": {
                "size_bins": [{"lo_um": 0, "hi_um": 100}],
                "area_limits_particles_per_m2": [1],
                "pressure_min_samples": 1,
            },
        })
        self.assertEqual(status, 200)
        self.assertTrue(body["immutable"])
        with urllib.request.urlopen(
            f"http://127.0.0.1:{self.port}/api/audit/verify"
        ) as response:
            verification = json.loads(response.read())
        self.assertTrue(verification["ok"])
        self.assertGreaterEqual(verification["entries"], 1)


if __name__ == "__main__":
    unittest.main()
