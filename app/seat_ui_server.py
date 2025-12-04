from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from loguru import logger

from app.seat_ui import SEATING_JSON_PATH, SEATING_HTML_PATH, save_seating_chart


class SeatChartRequestHandler(BaseHTTPRequestHandler):
    """シンプルな HTML 配信 + 保存API 用ハンドラー。"""

    def log_message(self, format: str, *args: object) -> None:  # pragma: no cover
        logger.debug("SeatChartRequestHandler: " + format, *args)

    def _serve_html(self) -> None:
        try:
            file_size = os.path.getsize(SEATING_HTML_PATH)
            with open(SEATING_HTML_PATH, "rb") as handle:
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(file_size))
                self.end_headers()
                self.wfile.write(handle.read())
        except OSError as exc:
            logger.warning("Seat chart HTML の配信に失敗: %s", exc)
            self.send_error(404)

    def do_GET(self) -> None:  # pragma: no cover
        requested = self.path.split("?", 1)[0]
        html_name = os.path.basename(SEATING_HTML_PATH)
        if requested in ("/", f"/{html_name}"):
            self._serve_html()
            return
        self.send_error(404)

    def do_POST(self) -> None:  # pragma: no cover
        if self.path != "/save-seating-chart":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            logger.warning("受信したJSONのパースに失敗: %s", exc)
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"invalid json")
            return
        try:
            save_seating_chart(SEATING_JSON_PATH, payload)
            self.send_response(204)
            self.end_headers()
        except Exception as exc:  # pragma: no cover
            logger.exception("Seat chart の保存に失敗しました")
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(exc).encode("utf-8"))


class _ThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class SeatChartServer:
    """ローカルHTTPサーバーとして座席一覧と保存APIを提供します。"""

    def __init__(self) -> None:
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        base_dir = os.path.dirname(SEATING_HTML_PATH) or os.getcwd()
        self._directory = base_dir
        self._port: int | None = None

    def start(self) -> None:
        """サーバーが起動していない場合は起動します。"""
        with self._lock:
            if self._server is not None:
                return
            server = _ThreadingHTTPServer(("127.0.0.1", 0), SeatChartRequestHandler)
            self._port = server.server_port
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self._server = server
            self._thread = thread
            logger.debug("Seat chart server started on port %s", self._port)

    def stop(self) -> None:
        """サーバーを停止します（起動していなければ無視）。"""
        with self._lock:
            if self._server is None:
                return
            try:
                self._server.shutdown()
                self._server.server_close()
            except Exception as exc:
                logger.debug("Seat chart server shutdown error: %s", exc)
            finally:
                self._server = None
                self._thread = None
                self._port = None

    @property
    def is_running(self) -> bool:
        return self._server is not None

    def get_html_url(self, html_path: str) -> str | None:
        """生成済みHTMLのURLを返します（サーバーが稼働していない場合はNone）。"""
        if self._port is None or not self.is_running:
            return None
        rel_path = os.path.relpath(html_path, self._directory).replace(os.sep, "/")
        rel_path = rel_path.lstrip("./")
        return f"http://127.0.0.1:{self._port}/{rel_path}"
