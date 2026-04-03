from __future__ import annotations

import json
import logging
import secrets
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .app import SalesBotRuntime, create_runtime
from .operator_api import OperatorInboxAPI, serialize_outbound_result
from .services import ConversationOwnershipError, LeadProfileValidationError


logger = logging.getLogger("lesdal.ai_sales.dashboard")
STATIC_DIR = Path(__file__).resolve().parent / "static"


def _read_dashboard_html() -> bytes:
    return (STATIC_DIR / "dashboard.html").read_bytes()


class DashboardPermissionError(RuntimeError):
    def __init__(self, message: str, *, reason: str = "forbidden") -> None:
        super().__init__(message)
        self.reason = reason


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _operator_payload(operator) -> dict[str, object]:
    return {
        "operator_id": operator.operator_id,
        "display_name": operator.display_name,
        "role": operator.role,
        "can_force_takeover": operator.can_force_takeover,
    }


def _can_force_takeover(operator: dict[str, object] | None) -> bool:
    if not operator:
        return False
    if bool(operator.get("can_force_takeover")):
        return True
    return str(operator.get("role") or "").strip().lower() == "supervisor"


def _is_foreign_owner(snapshot, operator: dict[str, object] | None) -> bool:
    if snapshot is None or operator is None:
        return False
    operator_id = str(operator.get("operator_id") or "").strip()
    operator_name = str(operator.get("display_name") or "").strip()
    owner_id = str(getattr(snapshot, "owner_id", "") or "").strip()
    owner_name = str(getattr(snapshot, "owner_name", "") or "").strip()
    if owner_id and operator_id:
        return owner_id != operator_id
    if owner_name:
        return owner_name != operator_name
    return False


def _resolve_force_claim(snapshot, operator: dict[str, object] | None, requested_force: bool) -> bool:
    if not requested_force:
        return False
    if not _is_foreign_owner(snapshot, operator):
        return False
    if not _can_force_takeover(operator):
        raise DashboardPermissionError(
            "Only supervisors can force takeover of another operator's dialog",
            reason="forbidden_force_takeover",
        )
    return True


def _create_operator_session(operator, *, now: datetime | None = None) -> dict[str, object]:
    issued_at = (now or _utcnow()).isoformat()
    return {
        "operator": _operator_payload(operator),
        "created_at": issued_at,
        "last_seen_at": issued_at,
    }


def _operator_action_payload(api: OperatorInboxAPI, conversation_id: int, result) -> dict[str, object]:
    return {
        "ok": True,
        "snapshot": api.get_conversation(conversation_id)["snapshot"],
        "outbound_sent": result.outbound_sent,
        "outbound": serialize_outbound_result(result.outbound_result),
        "retry": {
            "available": bool(result.retry_available),
            "delivery_key": str(result.reply_delivery_key or ""),
        },
    }


def _error_payload(error: str, *, reason: str, **extra: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "error": error,
        "reason": reason,
    }
    payload.update(extra)
    return payload


def _session_is_expired(session: dict[str, object], *, ttl_minutes: int, now: datetime | None = None) -> bool:
    raw_last_seen = session.get("last_seen_at") or session.get("created_at")
    if not raw_last_seen:
        return True
    try:
        last_seen_at = datetime.fromisoformat(str(raw_last_seen))
    except ValueError:
        return True
    deadline = last_seen_at + timedelta(minutes=max(1, ttl_minutes))
    return deadline <= (now or _utcnow())


def _consume_operator_session(
    operator_sessions: dict[str, dict[str, object]],
    session_token: str,
    *,
    ttl_minutes: int,
    now: datetime | None = None,
    refresh: bool = True,
) -> tuple[dict[str, object] | None, bool]:
    token = str(session_token or "").strip()
    if not token:
        return None, False
    session = operator_sessions.get(token)
    if session is None:
        return None, False
    current_time = now or _utcnow()
    if _session_is_expired(session, ttl_minutes=ttl_minutes, now=current_time):
        operator_sessions.pop(token, None)
        return None, True
    if refresh:
        session["last_seen_at"] = current_time.isoformat()
    operator = session.get("operator")
    if isinstance(operator, dict):
        return dict(operator), False
    return None, False


def build_dashboard_handler(api: OperatorInboxAPI):
    operator_registry = {
        item.operator_id: item
        for item in api.config.dashboard_operators
    }
    operator_sessions: dict[str, dict[str, object]] = {}
    session_ttl_minutes = max(1, int(api.config.dashboard_session_ttl_minutes))

    class DashboardHandler(BaseHTTPRequestHandler):
        server_version = "LesDalDashboard/0.1"

        def _current_operator(self) -> dict[str, object] | None:
            if hasattr(self, "_cached_operator"):
                return self._cached_operator
            operator, expired = _consume_operator_session(
                operator_sessions,
                self.headers.get("X-Operator-Session", "").strip(),
                ttl_minutes=session_ttl_minutes,
            )
            self._cached_operator = operator
            self._operator_session_expired = expired
            return operator

        def _require_operator(self) -> dict[str, object] | None:
            operator = self._current_operator()
            if operator is not None:
                return operator
            expired = bool(getattr(self, "_operator_session_expired", False))
            self._send_json(
                _error_payload(
                    "Session expired" if expired else "Operator session required",
                    reason="session_expired" if expired else "operator_session_required",
                ),
                status=HTTPStatus.UNAUTHORIZED,
            )
            return None

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
            if parsed.path == "/api/auth/operators":
                self._send_json(
                    {
                        "items": [
                            {
                                **_operator_payload(item),
                                "pin_required": bool(item.pin),
                            }
                            for item in operator_registry.values()
                        ]
                    }
                )
                return
            if parsed.path == "/api/auth/session":
                operator = self._current_operator()
                if operator is None:
                    self._send_json(
                        _error_payload(
                            "Session expired" if getattr(self, "_operator_session_expired", False) else "Operator session required",
                            reason="session_expired" if getattr(self, "_operator_session_expired", False) else "operator_session_required",
                        ),
                        status=HTTPStatus.UNAUTHORIZED,
                    )
                    return
                self._send_json({"operator": operator})
                return
            if parsed.path == "/api/audit/forced-takeovers":
                if self._require_operator() is None:
                    return
                self._send_json(
                    api.get_forced_takeover_summary(
                        period=self._query_str(parsed.query, "period") or "30d"
                    )
                )
                return
            if parsed.path == "/api/audit/resolution-drilldown":
                if self._require_operator() is None:
                    return
                self._send_json(
                    api.get_resolution_speed_drilldown(
                        metric=self._query_str(parsed.query, "metric"),
                        operator_key=self._query_str(parsed.query, "operator"),
                        period=self._query_str(parsed.query, "period") or "30d",
                        limit=self._query_int(parsed.query, "limit", default=50),
                    )
                )
                return
            if parsed.path == "/api/conversations":
                if self._require_operator() is None:
                    return
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
                            forced_only=self._query_bool(parsed.query, "forced_only"),
                        )
                    }
                )
                return
            if parsed.path.startswith("/api/conversations/"):
                if self._require_operator() is None:
                    return
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
            if parsed.path == "/api/auth/login":
                payload = self._read_json()
                operator_id = str(payload.get("operator_id") or "").strip()
                pin = str(payload.get("pin") or "").strip()
                operator = operator_registry.get(operator_id)
                if operator is None:
                    self._send_json(
                        _error_payload("Unknown operator", reason="invalid_operator"),
                        status=HTTPStatus.UNAUTHORIZED,
                    )
                    return
                if operator.pin and operator.pin != pin:
                    self._send_json(
                        _error_payload("Invalid PIN", reason="invalid_pin"),
                        status=HTTPStatus.UNAUTHORIZED,
                    )
                    return
                session_token = secrets.token_urlsafe(24)
                operator_payload = _operator_payload(operator)
                operator_sessions[session_token] = _create_operator_session(operator)
                self._send_json(
                    {
                        "ok": True,
                        "session_token": session_token,
                        "operator": operator_payload,
                    }
                )
                return
            if parsed.path == "/api/auth/logout":
                session_token = self.headers.get("X-Operator-Session", "").strip()
                if session_token:
                    operator_sessions.pop(session_token, None)
                self._send_json({"ok": True})
                return
            if not parsed.path.startswith("/api/conversations/"):
                self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return

            operator = self._require_operator()
            if operator is None:
                return

            conversation_id, action = self._conversation_route(parsed.path)
            if conversation_id is None or action is None:
                self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return

            try:
                if action == "pause":
                    result = api.claim_conversation(
                        conversation_id,
                        operator_name=operator["display_name"],
                        operator_id=operator["operator_id"],
                    )
                    self._send_json(_operator_action_payload(api, conversation_id, result))
                    return

                if action == "claim":
                    payload = self._read_json()
                    requested_force = bool(payload.get("force"))
                    snapshot = api.service.get_snapshot(conversation_id)
                    force = _resolve_force_claim(snapshot, operator, requested_force)
                    result = api.claim_conversation(
                        conversation_id,
                        operator_name=operator["display_name"],
                        operator_id=operator["operator_id"],
                        force=force,
                    )
                    self._send_json(_operator_action_payload(api, conversation_id, result))
                    return

                if action == "release":
                    result = api.release_conversation(
                        conversation_id,
                        operator_name=operator["display_name"],
                        operator_id=operator["operator_id"],
                    )
                    self._send_json(_operator_action_payload(api, conversation_id, result))
                    return

                if action == "resume":
                    result = api.resume_conversation(conversation_id)
                    self._send_json(_operator_action_payload(api, conversation_id, result))
                    return

                if action == "reply":
                    payload = self._read_json()
                    text = str(payload.get("text") or "").strip()
                    pause_ai = bool(payload.get("pause_ai", True))
                    if not text:
                        self._send_json(
                            _error_payload("Text is required", reason="validation_error"),
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return
                    result = api.reply_to_conversation(
                        conversation_id,
                        text=text,
                        pause_ai=pause_ai,
                        operator_name=operator["display_name"],
                        operator_id=operator["operator_id"],
                    )
                    self._send_json(_operator_action_payload(api, conversation_id, result))
                    return

                if action == "retry-reply":
                    payload = self._read_json()
                    delivery_key = str(payload.get("delivery_key") or "").strip()
                    if not delivery_key:
                        self._send_json(
                            _error_payload("delivery_key is required", reason="validation_error"),
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return
                    result = api.retry_reply_delivery(
                        conversation_id,
                        delivery_key=delivery_key,
                        operator_name=operator["display_name"],
                        operator_id=operator["operator_id"],
                    )
                    self._send_json(_operator_action_payload(api, conversation_id, result))
                    return

                if action == "status":
                    payload = self._read_json()
                    status = str(payload.get("status") or "").strip()
                    if not status:
                        self._send_json(
                            _error_payload("Status is required", reason="validation_error"),
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return
                    result = api.set_status(
                        conversation_id,
                        status=status,
                        operator_name=operator["display_name"],
                        operator_id=operator["operator_id"],
                    )
                    self._send_json(_operator_action_payload(api, conversation_id, result))
                    return

                if action == "notes":
                    payload = self._read_json()
                    notes = str(payload.get("notes") or "")
                    result = api.update_manager_notes(
                        conversation_id,
                        notes=notes,
                        operator_name=operator["display_name"],
                        operator_id=operator["operator_id"],
                    )
                    self._send_json(_operator_action_payload(api, conversation_id, result))
                    return

                if action == "profile":
                    payload = self._read_json()
                    stage = str(payload.get("stage") or "").strip()
                    summary = str(payload.get("summary") or "").strip()
                    priority = str(payload.get("priority") or "").strip()
                    follow_up_date = str(payload.get("follow_up_date") or "").strip()
                    next_action = str(payload.get("next_action") or "").strip()
                    raw_tags = payload.get("tags") or []
                    if not stage:
                        self._send_json(
                            _error_payload("Stage is required", reason="validation_error"),
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return
                    if not priority:
                        self._send_json(
                            _error_payload("Priority is required", reason="validation_error"),
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return
                    tags = [
                        str(item).strip()
                        for item in raw_tags
                        if str(item).strip()
                    ]
                    result = api.update_lead_profile(
                        conversation_id,
                        stage=stage,
                        summary=summary,
                        tags=tags,
                        priority=priority,
                        follow_up_date=follow_up_date,
                        next_action=next_action,
                        operator_name=operator["display_name"],
                        operator_id=operator["operator_id"],
                    )
                    self._send_json(_operator_action_payload(api, conversation_id, result))
                    return
            except ConversationOwnershipError as exc:
                self._send_json(
                    _error_payload(str(exc), reason=getattr(exc, "reason", "owned_by_other")),
                    status=HTTPStatus.CONFLICT,
                )
                return
            except DashboardPermissionError as exc:
                self._send_json(
                    _error_payload(str(exc), reason=getattr(exc, "reason", "forbidden")),
                    status=HTTPStatus.FORBIDDEN,
                )
                return
            except (LeadProfileValidationError, ValueError) as exc:
                self._send_json(
                    _error_payload(str(exc), reason="validation_error"),
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            except Exception as exc:
                logger.exception("Dashboard action failed")
                self._send_json(
                    _error_payload(str(exc), reason="server_error"),
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
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

            self._send_json(
                _error_payload("Unauthorized", reason="dashboard_token_invalid"),
                status=HTTPStatus.UNAUTHORIZED,
            )
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
