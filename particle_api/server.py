"""Zero-dependency HTTP wrapper for the particle grading service."""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import db, service

DEFAULT_DB = os.environ.get("PARTICLE_API_DB", "particle_api.sqlite3")


def _json_default(value):
    return str(value)


class Handler(BaseHTTPRequestHandler):
    server_version = "ParticleGrading/1.0"

    def _send_json(self, status: int, body: dict) -> None:
        data = json.dumps(body, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise service.ApiError(400, "INVALID_JSON", "request body must be JSON")
        if not isinstance(data, dict):
            raise service.ApiError(400, "INVALID_JSON", "request body must be an object")
        return data

    def _handle_service(self, fn):
        try:
            result = fn()
        except service.ApiError as exc:
            self._send_json(exc.status, exc.to_dict())
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:  # Keep deterministic API errors for clients.
            self._send_json(
                500,
                {"error": {"code": "INTERNAL_ERROR", "message": str(exc)}},
            )
        else:
            self._send_json(200, result)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)

        def action():
            conn = self.server.conn
            if path.startswith("/api/exposures/"):
                try:
                    exposure_id = int(path.rsplit("/", 1)[1])
                except ValueError:
                    raise service.ApiError(
                        400, "INVALID_EXPOSURE_ID", "exposure id must be an integer"
                    )
                return service.get_exposure(conn, exposure_id)
            if path == "/api/streak":
                version_hash = query.get("version_hash", [""])[0]
                return service.get_streak(conn, version_hash)
            if path == "/api/next-window":
                return service.next_sampling_window(
                    conn,
                    query.get("version_hash", [""])[0],
                    query.get("plate_code", [None])[0],
                    query.get("at", [None])[0],
                )
            if path == "/api/audit":
                limit = int(query.get("limit", ["50"])[0])
                return service.audit_tail(conn, limit)
            if path == "/api/audit/verify":
                return db.verify_audit_chain(conn)
            raise service.ApiError(404, "NOT_FOUND", f"unknown path {path}")

        self._handle_service(action)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        payload = self._read_json()
        conn = self.server.conn

        def action():
            if path == "/api/segments":
                return service.register_segment(conn, payload)
            if path == "/api/plates":
                return service.register_plate(conn, payload)
            if path == "/api/pressure-samples":
                return service.add_pressure_samples(conn, payload)
            if path == "/api/exposures/grade":
                return service.grade_exposure(conn, payload)
            if path == "/api/clock/regressions":
                return service.declare_clock_regression(conn, payload)
            if path == "/api/clock/resync":
                return service.resync_clock(conn, payload)
            if path == "/api/plates/decontaminate":
                return service.decontaminate_plate(conn, payload)
            if path.startswith("/api/exposures/") and path.endswith("/flags"):
                try:
                    exposure_id = int(path.split("/")[3])
                except ValueError:
                    raise service.ApiError(
                        400, "INVALID_EXPOSURE_ID", "exposure id must be an integer"
                    )
                return service.add_flag(conn, exposure_id, payload)
            raise service.ApiError(404, "NOT_FOUND", f"unknown path {path}")

        self._handle_service(action)

    def log_message(self, fmt: str, *args) -> None:
        if os.environ.get("PARTICLE_API_QUIET") != "1":
            super().log_message(fmt, *args)


def create_server(host: str = "127.0.0.1", port: int = 8000, db_path: str | None = None) -> ThreadingHTTPServer:
    conn = db.connect(db_path or DEFAULT_DB)
    db.initialize(conn)
    server = ThreadingHTTPServer((host, port), Handler)
    server.conn = conn  # type: ignore[attr-defined]
    return server


def main() -> None:
    host = os.environ.get("PARTICLE_API_HOST", "127.0.0.1")
    port = int(os.environ.get("PARTICLE_API_PORT", "8000"))
    server = create_server(host, port)
    print(f"particle grading API listening on http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
