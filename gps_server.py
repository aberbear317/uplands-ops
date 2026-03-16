#!/usr/bin/env python3
"""Lightweight local server for GPS and voice helper pages."""

from __future__ import annotations

from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent


class _HelperPageRequestHandler(SimpleHTTPRequestHandler):
    """Serve helper pages with browser-friendly headers for mobile devices."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(PROJECT_ROOT), **kwargs)

    def end_headers(self) -> None:
        if self.path.startswith("/gps/"):
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.send_header("Permissions-Policy", "geolocation=(self), microphone=(self)")
            self.send_header("X-Content-Type-Options", "nosniff")
        super().end_headers()

    def guess_type(self, path: str) -> str:
        if path.endswith(".html"):
            return "text/html; charset=utf-8"
        if path.endswith(".png"):
            return "image/png"
        if path.endswith(".jpg") or path.endswith(".jpeg"):
            return "image/jpeg"
        if path.endswith(".js"):
            return "application/javascript; charset=utf-8"
        if path.endswith(".css"):
            return "text/css; charset=utf-8"
        return super().guess_type(path)


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 8502), _HelperPageRequestHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
