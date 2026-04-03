from __future__ import annotations

import json
import logging
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .app import SalesBotRuntime, create_runtime
from .operator_api import OperatorInboxAPI
from .services import ConversationOwnershipError


logger = logging.getLogger("lesdal.ai_sales.dashboard")
STATIC_DIR = Path(__file__).resolve().parent / "static"


def _read_dashboard_html() -> bytes:
    return (STATIC_DIR / "dashboard.html").read_bytes()


def build_dashboard_handler(api: OperatorInboxAPI):
    class DashboardHandler(BaseHTTPRequestHandler):
        server_version = "LesDalDashboard/0.1"

        def do_GET(self) -> None:
            if not self._authorize():
                return

            parsed = urlparse(self.path)
            if parsed.path in {"/", "/index.html"}:
                self._send_html(_read_dashboard_html())
                return
            if parsed.path == "/api/health":
                self._send_json({"ok": True})
                return
            if parsed.path == "/api/conversations":
                limit = self._query_int(parsed.query, "limit", default=50)
                self._send_json(
                    {
                        "items": api.list_conversations(
                            limit=limit,
                            channel=self._query_str(parsed.query, "channel"),
                            mode=self._query_str(parsed.query, "mode"),
                            status=self._query_str(parsed.query, "status"),
                            owner=self._query_str(parsed.query, "owner"),
                            q=self._query_str(parsed.query, "q"),
                            needs_attention=self._query_bool(parsed.query, "needs_attention"),
                        )
                    }
                )
                return
            if parsed.path.startswith("/api/conversations/"):
                conversation_id, action = self._conversation_route(parsed.path)
                if conversation_id is None or action is not None:
                    self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
                    return
                self._send_json(api.get_conversation(conversation_id))
                return

            self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:
            if not self._authorize():
                return

            parsed = urlparse(self.path)
            if not parsed.path.startswith("/api/conversations/"):
                self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return

            conversation_id, action = self._conversation_route(parsed.path)
            if conversation_id is None or action is None:
                self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return

            try:
                if action == "pause":
                    result = api.pause_conversation(conversation_id)
                    self._send_json(
                        {
                            "ok": True,
                            "snapshot": api.get_conversation(conversation_id)["snapshot"],
                            "outbound_sent": result.outbound_sent,
                        }
                    )
                    return

                if action == "claim":
                    payload = self._read_json()
                    operator_name = str(payload.get("operator_name") or api.config.manager_name).strip()
                    result = api.claim_conversation(
                        conversation_id,
                        operator_name=operator_name or api.config.manager_name,
                    )
                    self._send_json(
                        {
                            "ok": True,
                            "snapshot": api.get_conversation(conversation_id)["snapshot"],
                            "outbound_sent": result.outbound_sent,
                        }
                    )
                    return

                if action == "release":
                    payload = self._read_json()
                    operator_name = str(payload.get("operator_name") or api.config.manager_name).strip()
                    result = api.release_conversation(
                        conversation_id,
                        operator_name=operator_name or api.config.manager_name,
                    )
                    self._send_json(
                        {
                            "ok": True,
                            "snapshot": api.get_conversation(conversation_id)["snapshot"],
                            "outbound_sent": result.outbound_sent,
                        }
                    )
                    return

                if action == "resume":
                    result = api.resume_conversation(conversation_id)
                    self._send_json(
                        {
                            "ok": True,
                            "snapshot": api.get_conversation(conversation_id)["snapshot"],
                            "outbound_sent": result.outbound_sent,
                        }
                    )
                    return

                if action == "reply":
                    payload = self._read_json()
                    text = str(payload.get("text") or "").strip()
                    pause_ai = bool(payload.get("pause_ai", True))
                    operator_name = str(payload.get("operator_name") or api.config.manager_name).strip()
                    if not text:
                        self._send_json({"error": "Text is required"}, status=HTTPStatus.BAD_REQUEST)
                        return
                    result = api.reply_to_conversation(
                        conversation_id,
                        text=text,
                        pause_ai=pause_ai,
                        operator_name=operator_name or api.config.manager_name,
                    )
                    self._send_json(
                        {
                            "ok": True,
                            "snapshot": api.get_conversation(conversation_id)["snapshot"],
                            "outbound_sent": result.outbound_sent,
                        }
                    )
                    return

                if action == "status":
                    payload = self._read_json()
                    status = str(payload.get("status") or "").strip()
                    operator_name = str(payload.get("operator_name") or "").strip()
                    if not status:
                        self._send_json({"error": "Status is required"}, status=HTTPStatus.BAD_REQUEST)
                        return
                    result = api.set_status(
                        conversation_id,
                        status=status,
                        operator_name=operator_name,
                    )
                    self._send_json(
                        {
                            "ok": True,
                            "snapshot": api.get_conversation(conversation_id)["snapshot"],
                            "outbound_sent": result.outbound_sent,
                        }
                    )
                    return

                if action == "notes":
                    payload = self._read_json()
                    notes = str(payload.get("notes") or "")
                    operator_name = str(payload.get("operator_name") or "").strip()
                    result = api.update_manager_notes(
                        conversation_id,
                        notes=notes,
                        operator_name=operator_name,
                    )
                    self._send_json(
                        {
                            "ok": True,
                            "snapshot": api.get_conversation(conversation_id)["snapshot"],
                            "outbound_sent": result.outbound_sent,
                        }
                    )
                    return
            except ConversationOwnershipError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
                return
            except Exception as exc:
                logger.exception("Dashboard action failed")
                self._send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
                return

            self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

        def log_message(self, format: str, *args) -> None:
            logger.info("%s - %s", self.address_string(), format % args)

        def _authorize(self) -> bool:
            token = api.config.dashboard_token
            if not token:
                return True

            header = self.headers.get("Authorization", "").strip()
            if header == f"Bearer {token}":
                return True

            parsed = urlparse(self.path)
            query_token = parse_qs(parsed.query).get("token", [""])[0]
            if query_token == token:
                return True

            self._send_json({"error": "Unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
            return False

        def _send_json(self, payload: dict, *, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status.value)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, body: bytes) -> None:
            self.send_response(HTTPStatus.OK.value)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict:
            raw_length = self.headers.get("Content-Length", "0").strip() or "0"
            length = max(0, int(raw_length))
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            return json.loads(raw.decode("utf-8"))

        def _conversation_route(self, path: str) -> tuple[int | None, str | None]:
            parts = [part for part in path.split("/") if part]
            if len(parts) not in {3, 4}:
                return None, None
            if parts[0] != "api" or parts[1] != "conversations":
                return None, None
            try:
                conversation_id = int(parts[2])
            except ValueError:
                return None, None
            action = parts[3] if len(parts) == 4 else None
            return conversation_id, action

        def _query_int(self, query: str, key: str, *, default: int) -> int:
            raw = parse_qs(query).get(key, [""])[0]
            try:
                return int(raw)
            except (TypeError, ValueError):
                return default

        def _query_str(self, query: str, key: str) -> str:
            return str(parse_qs(query).get(key, [""])[0] or "").strip()

        def _query_bool(self, query: str, key: str) -> bool | None:
            raw = self._query_str(query, key).lower()
            if raw in {"1", "true", "yes"}:
                return True
            if raw in {"0", "false", "no"}:
                return False
            return None

    return DashboardHandler


def run_dashboard_server(runtime: SalesBotRuntime | None = None) -> None:
    rt = runtime or create_runtime()
    api = OperatorInboxAPI(runtime=rt)
    server = ThreadingHTTPServer(
        (rt.config.dashboard_host, rt.config.dashboard_port),
        build_dashboard_handler(api),
    )
    logger.info(
        "Dashboard listening on http://%s:%s",
        rt.config.dashboard_host,
        rt.config.dashboard_port,
    )
    server.serve_forever()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    run_dashboard_server()
