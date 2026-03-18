#!/usr/bin/env python3
"""Lightweight local server for GPS and voice helper pages."""

from __future__ import annotations

import html
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import mimetypes
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, quote, urlparse

from uplands_site_command_centre import DATABASE_PATH
from uplands_site_command_centre.permits.models import ToolboxTalkDocument
from uplands_site_command_centre.permits.repository import (
    DocumentNotFoundError,
    DocumentRepository,
)


PROJECT_ROOT = Path(__file__).resolve().parent


class _HelperPageRequestHandler(SimpleHTTPRequestHandler):
    """Serve helper pages with browser-friendly headers for mobile devices."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(PROJECT_ROOT), **kwargs)

    def do_GET(self) -> None:
        parsed_path = urlparse(self.path)
        if parsed_path.path == "/gps/tbt-preview":
            self._handle_toolbox_talk_preview_request(parsed_path, send_body=True)
            return
        if parsed_path.path == "/gps/tbt-document":
            self._handle_toolbox_talk_document_request(parsed_path, send_body=True)
            return
        super().do_GET()

    def do_HEAD(self) -> None:
        parsed_path = urlparse(self.path)
        if parsed_path.path == "/gps/tbt-preview":
            self._handle_toolbox_talk_preview_request(parsed_path, send_body=False)
            return
        if parsed_path.path == "/gps/tbt-document":
            self._handle_toolbox_talk_document_request(parsed_path, send_body=False)
            return
        super().do_HEAD()

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

    def _handle_toolbox_talk_document_request(
        self,
        parsed_path,
        *,
        send_body: bool,
    ) -> None:
        document = self._get_toolbox_talk_document(parsed_path)
        if document is None:
            return

        stored_path = Path(document.stored_file_path).expanduser()
        if not stored_path.exists():
            self._send_html_error(404, "The toolbox talk source file is missing from disk.")
            return

        file_bytes = stored_path.read_bytes()
        mime_type = (
            mimetypes.guess_type(document.original_file_name)[0]
            or "application/octet-stream"
        )
        file_name = Path(document.original_file_name).name or stored_path.name

        self.send_response(200)
        self.send_header("Content-Type", mime_type)
        self.send_header("Content-Length", str(len(file_bytes)))
        self.send_header(
            "Content-Disposition",
            f'inline; filename="{file_name.replace(chr(34), "")}"',
        )
        self.end_headers()
        if send_body:
            self.wfile.write(file_bytes)

    def _handle_toolbox_talk_preview_request(
        self,
        parsed_path,
        *,
        send_body: bool,
    ) -> None:
        document = self._get_toolbox_talk_document(parsed_path)
        if document is None:
            return

        raw_document_url = (
            f"{self.headers.get('X-Forwarded-Proto', 'https')}://"
            f"{self.headers.get('Host', 'uplands-site-induction.omegaleague.win')}"
            f"/gps/tbt-document?doc_id={document.doc_id}"
        )
        mime_type = (
            mimetypes.guess_type(document.original_file_name)[0]
            or "application/octet-stream"
        )
        if mime_type.startswith("image/"):
            iframe_url = raw_document_url
        else:
            iframe_url = (
                "https://docs.google.com/gview?embedded=1&url="
                f"{quote(raw_document_url, safe='')}"
            )
        escaped_file_name = html.escape(document.original_file_name)
        escaped_iframe_url = html.escape(iframe_url, quote=True)
        escaped_raw_url = html.escape(raw_document_url, quote=True)
        body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <title>Toolbox Talk Preview</title>
  <style>
    :root {{
      --ink: #121826;
      --muted: #667085;
      --accent: #d1228e;
      --accent-2: #5b8def;
      --border: #e5e7eb;
      --surface: rgba(255, 255, 255, 0.94);
      --bg: #f8fafc;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(209,34,142,0.12), transparent 34%),
        radial-gradient(circle at top right, rgba(91,141,239,0.10), transparent 30%),
        var(--bg);
      color: var(--ink);
      padding: 18px;
    }}
    .shell {{
      max-width: 1080px;
      margin: 0 auto;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 24px;
      box-shadow: 0 24px 60px rgba(15, 23, 42, 0.10);
      overflow: hidden;
    }}
    .header {{
      padding: 20px 22px 18px;
      border-bottom: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.92);
    }}
    .eyebrow {{
      color: var(--accent);
      font-size: 0.76rem;
      font-weight: 800;
      letter-spacing: 0.16em;
      text-transform: uppercase;
      margin-bottom: 0.45rem;
    }}
    h1 {{
      margin: 0 0 0.45rem;
      font-size: clamp(1.35rem, 2.5vw, 2rem);
      line-height: 1.1;
    }}
    p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.5;
    }}
    .actions {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 1rem;
    }}
    .button {{
      appearance: none;
      text-decoration: none;
      border-radius: 999px;
      padding: 13px 18px;
      font-size: 0.95rem;
      font-weight: 800;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 170px;
    }}
    .button.primary {{
      background: linear-gradient(135deg, var(--accent), var(--accent-2));
      color: #fff;
    }}
    .button.secondary {{
      background: #fff;
      color: var(--ink);
      border: 1px solid var(--border);
    }}
    .viewer-wrap {{
      padding: 16px;
      background: #eef2f7;
    }}
    iframe {{
      width: 100%;
      height: min(78vh, 980px);
      border: 0;
      border-radius: 18px;
      background: #fff;
    }}
    .hint {{
      padding: 0 22px 22px;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.5;
    }}
    @media (max-width: 640px) {{
      body {{ padding: 10px; }}
      .header {{ padding: 18px 16px 16px; }}
      .viewer-wrap {{ padding: 10px; }}
      iframe {{ height: 72vh; }}
      .hint {{ padding: 0 16px 18px; }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="header">
      <div class="eyebrow">Toolbox Talk Preview</div>
      <h1>{escaped_file_name}</h1>
      <p>Read the document in your browser, then return to the signing page once you are ready to confirm and sign.</p>
      <div class="actions">
        <a class="button primary" href="{escaped_raw_url}" target="_blank" rel="noopener noreferrer">📄 Open Raw Document</a>
        <a class="button secondary" href="javascript:window.history.back();">← Back to Sign Register</a>
      </div>
    </section>
    <section class="viewer-wrap">
      <iframe src="{escaped_iframe_url}" title="Toolbox Talk Preview"></iframe>
    </section>
    <p class="hint">Android phones generally preview better through this page than through a direct file link. If the embedded viewer still struggles, the raw document button stays available as fallback.</p>
  </main>
</body>
</html>""".encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def _get_toolbox_talk_document(
        self,
        parsed_path,
    ) -> Optional[ToolboxTalkDocument]:
        doc_id = (
            parse_qs(parsed_path.query).get("doc_id", [""])[0].strip()
        )
        if not doc_id:
            self._send_html_error(400, "Missing toolbox talk document reference.")
            return None

        repository = DocumentRepository(DATABASE_PATH)
        repository.create_schema()
        try:
            document = repository.get(doc_id)
        except DocumentNotFoundError:
            self._send_html_error(404, "Toolbox talk document not found.")
            return None

        if not isinstance(document, ToolboxTalkDocument):
            self._send_html_error(404, "The requested file is not a toolbox talk document.")
            return None
        return document

    def _send_html_error(self, status_code: int, message: str) -> None:
        body = (
            "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width, initial-scale=1'>"
            "<title>Toolbox Talk Document</title></head><body "
            "style=\"font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;"
            "padding:24px;background:#f8fafc;color:#121826;\">"
            f"<h1 style='margin:0 0 12px;'>Unable to open document</h1>"
            f"<p style='margin:0;color:#475467;'>{html.escape(message)}</p>"
            "</body></html>"
        ).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 8502), _HelperPageRequestHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
