#!/usr/bin/env python3
"""Local dev server that refreshes data.json on request."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data.json"
UPDATE_SCRIPT = PROJECT_ROOT / "scripts" / "update_data.py"
REFRESH_MIN_SECONDS = int(os.getenv("PACERS_REFRESH_MIN_SECONDS", "60"))
PORT = int(os.getenv("PORT", "8000"))

_refresh_lock = threading.Lock()
_last_refresh_started = 0.0


def _refresh_data_if_needed() -> None:
    global _last_refresh_started
    now = time.time()
    with _refresh_lock:
        if now - _last_refresh_started < REFRESH_MIN_SECONDS:
            return
        _last_refresh_started = now
        cmd = [sys.executable, str(UPDATE_SCRIPT), "--safe", "--fast"]
        print(f"[dev_server] refreshing data via: {' '.join(cmd)}", flush=True)
        subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=False)


class Handler(SimpleHTTPRequestHandler):
    def translate_path(self, path: str) -> str:
        rel = urlsplit(path).path
        rel = rel.lstrip("/")
        return str(PROJECT_ROOT / rel)

    def end_headers(self) -> None:
        # Ensure browser doesn't keep stale html/js/json.
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def do_GET(self) -> None:  # noqa: N802
        req_path = urlsplit(self.path).path
        if req_path in {"/", "/index.html", "/data.json"}:
            _refresh_data_if_needed()
        if req_path == "/" and not DATA_PATH.exists():
            _refresh_data_if_needed()
        return super().do_GET()


def main() -> int:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[dev_server] serving {PROJECT_ROOT} on http://localhost:{PORT}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
