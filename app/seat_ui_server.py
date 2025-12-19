from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from loguru import logger

# ログ分類（app_.logの視認性向上）
logger = logger.bind(channel="UI:SEAT")

from app.seat_ui import SEATING_JSON_PATH, SEATING_HTML_PATH, save_seating_chart

SEAT_CHART_PORT_FILE_NAME = "seat_chart_server_port.txt"


def _get_port_store_path() -> str:
    directory = os.path.dirname(SEATING_HTML_PATH) or os.getcwd()
    return os.path.join(directory, SEAT_CHART_PORT_FILE_NAME)


def _load_persisted_seat_chart_port() -> int | None:
    try:
        path = _get_port_store_path()
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as handle:
            raw = handle.read().strip()
        port = int(raw)
        if 0 < port < 65536:
            return port
    except (ValueError, OSError) as exc:
        logger.debug("Seat chart port persistence read failed: %s", exc)
    return None


def _persist_seat_chart_port(port: int | None) -> None:
    try:
        path = _get_port_store_path()
        if port is None:
            if os.path.exists(path):
                os.remove(path)
            return
        directory = os.path.dirname(path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(str(port))
    except OSError as exc:
        logger.debug("Seat chart port persistence write failed: %s", exc)


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
        self._preferred_port: int | None = _load_persisted_seat_chart_port()

    def start(self) -> None:
        """サーバーが起動していない場合は起動します。"""
        with self._lock:
            if self._server is not None:
                return
            server: ThreadingHTTPServer | None = None
            last_error: Exception | None = None
            ports = []
            if self._preferred_port is not None:
                ports.append(self._preferred_port)
            ports.append(0)
            for port_candidate in ports:
                try:
                    server = _ThreadingHTTPServer(("127.0.0.1", port_candidate), SeatChartRequestHandler)
                    break
                except OSError as exc:
                    last_error = exc
                    logger.debug("Seat chart server port %s is unavailable: %s", port_candidate, exc)
            if server is None:
                error_msg = "Failed to bind seat chart server socket"
                logger.error(error_msg)
                if last_error:
                    logger.error(last_error)
                raise RuntimeError(error_msg)
            self._port = server.server_port
            self._preferred_port = self._port
            _persist_seat_chart_port(self._port)
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
