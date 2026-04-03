from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Protocol
import json
import secrets

from .domain import ConversationMode, ConversationSnapshot, ConversationStatus, InboundMessage, LeadPriority, LeadStage, SenderRole
from .outbound import OutboundSendResult


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _owner_label(*, owner_id: str = "", owner_name: str = "") -> str:
    label = str(owner_name or owner_id or "").strip()
    return label or "another operator"


class ConversationOwnershipError(RuntimeError):
    def __init__(self, message: str, *, reason: str = "owned_by_other") -> None:
        super().__init__(message)
        self.reason = reason


class LeadProfileValidationError(ValueError):
    pass


@dataclass(slots=True)
class ReplyRetryContext:
    text: str
    already_delivered: bool = False
    previous_result: OutboundSendResult | None = None


class RepositoryProtocol(Protocol):
    def ingest_customer_message(self, message: InboundMessage) -> ConversationSnapshot: ...
    def add_message(self, *, conversation_id: int, sender_role: SenderRole, text: str, sender_name: str = "", raw_payload: dict | None = None) -> int: ...
    def add_conversation_event(self, *, conversation_id: int, event_type: str, actor: str = "", payload: dict | None = None) -> int: ...
    def get_snapshot(self, conversation_id: int) -> ConversationSnapshot: ...
    def get_conversation_target(self, conversation_id: int) -> dict: ...
    def list_recent_conversations(self, *, limit: int = 20) -> list[dict]: ...
    def list_conversation_events(self, conversation_id: int, *, limit: int = 50) -> list[dict]: ...
    def list_conversation_events_by_type(self, *, event_types: list[str] | tuple[str, ...], limit: int = 2000) -> list[dict]: ...
    def list_forced_takeover_events(self, *, limit: int = 200) -> list[dict]: ...
    def set_conversation_mode(self, *, conversation_id: int, mode: ConversationMode) -> None: ...
    def update_conversation_state(self, *, conversation_id: int, mode: ConversationMode | None = None, status: ConversationStatus | None = None, owner_id: str | None = None, owner_name: str | None = None, owner_claimed_at: datetime | None = None, clear_owner: bool = False, needs_attention: bool | None = None) -> None: ...
    def update_lead(self, *, lead_id: int, stage: LeadStage | None = None, mode: ConversationMode | None = None, summary: str | None = None, city: str | None = None, interested_products: list[str] | None = None, tags: list[str] | None = None, manager_notes: str | None = None, priority: LeadPriority | None = None, follow_up_date: str | None = None, next_action: str | None = None, amocrm_lead_id: str | None = None) -> None: ...
    def build_transcript(self, conversation_id: int, *, limit: int = 30) -> list[dict]: ...


def _parse_event_time(raw_value: object) -> datetime | None:
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(str(raw_value))
    except ValueError:
        return None


def _coerce_event_payload(raw_payload: object) -> dict:
    if isinstance(raw_payload, dict):
        return raw_payload
    if isinstance(raw_payload, str):
        try:
            decoded = json.loads(raw_payload)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _payload_delivery_key(row: dict) -> str:
    payload = _coerce_event_payload(row.get("payload"))
    return str(payload.get("delivery_key") or "").strip()


def _resolve_audit_period(period: str, *, now: datetime) -> tuple[str, str, datetime | None]:
    normalized = str(period or "30d").strip().lower()
    if normalized == "today":
        start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        return "today", "Сегодня", start
    if normalized == "7d":
        return "7d", "7 дней", now.replace(microsecond=0) - timedelta(days=7)
    if normalized == "30d":
        return "30d", "30 дней", now.replace(microsecond=0) - timedelta(days=30)
    return "30d", "30 дней", now.replace(microsecond=0) - timedelta(days=30)


def _median_minutes(samples: list[int]) -> int | float | None:
    if not samples:
        return None
    value = median(samples)
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return round(float(value), 1)


def _event_operator_identity(row: dict) -> tuple[str, str]:
    payload = _coerce_event_payload(row.get("payload"))
    operator_id = str(payload.get("operator_id") or "").strip()
    actor = str(row.get("actor") or "").strip()
    key = operator_id or actor.lower() or "unknown"
    label = actor or operator_id or "unknown"
    return key, label


def _resolution_label(row: dict) -> str:
    event_type = str(row.get("event_type") or "").strip()
    if event_type == "manager_reply":
        return "manager_reply"
    if event_type == "returned_to_ai":
        return "returned_to_ai"
    if event_type == "status_changed":
        payload = _coerce_event_payload(row.get("payload"))
        status = str(payload.get("status") or "").strip()
        if status:
            return f"status:{status}"
    return event_type or "resolved"


def _collect_resolution_minutes(
    events: list[dict],
    *,
    start_types: set[str],
    since: datetime | None = None,
    finish_types: set[str] | None = None,
    finish_matcher=None,
) -> list[int]:
    durations: list[int] = []
    pending_started_at: dict[int, datetime] = {}
    ordered = sorted(
        events,
        key=lambda row: (
            int(row.get("conversation_id") or 0),
            int(row.get("id") or 0),
        ),
    )
    for row in ordered:
        conversation_id = int(row.get("conversation_id") or 0)
        event_type = str(row.get("event_type") or "").strip()
        created_at = _parse_event_time(row.get("created_at"))
        if not conversation_id or not event_type or created_at is None:
            continue
        if event_type in start_types:
            if since is not None and created_at < since:
                pending_started_at.pop(conversation_id, None)
                continue
            pending_started_at[conversation_id] = created_at
            continue
        started_at = pending_started_at.get(conversation_id)
        if started_at is None:
            continue
        is_finish = False
        if finish_types and event_type in finish_types:
            is_finish = True
        elif callable(finish_matcher):
            is_finish = bool(finish_matcher(row))
        if not is_finish:
            continue
        delta_minutes = int((created_at - started_at).total_seconds() // 60)
        if delta_minutes >= 0:
            durations.append(delta_minutes)
        pending_started_at.pop(conversation_id, None)
    return durations


def _collect_resolution_minutes_by_operator(
    events: list[dict],
    *,
    start_types: set[str],
    operator_from: str,
    since: datetime | None = None,
    finish_types: set[str] | None = None,
    finish_matcher=None,
    descending: bool = False,
) -> list[dict[str, object]]:
    pending_started_at: dict[int, datetime] = {}
    pending_operator: dict[int, tuple[str, str]] = {}
    samples_by_operator: dict[str, list[int]] = {}
    labels_by_operator: dict[str, str] = {}
    ordered = sorted(
        events,
        key=lambda row: (
            int(row.get("conversation_id") or 0),
            int(row.get("id") or 0),
        ),
    )
    for row in ordered:
        conversation_id = int(row.get("conversation_id") or 0)
        event_type = str(row.get("event_type") or "").strip()
        created_at = _parse_event_time(row.get("created_at"))
        if not conversation_id or not event_type or created_at is None:
            continue
        if event_type in start_types:
            if since is not None and created_at < since:
                pending_started_at.pop(conversation_id, None)
                pending_operator.pop(conversation_id, None)
                continue
            pending_started_at[conversation_id] = created_at
            if operator_from == "start":
                pending_operator[conversation_id] = _event_operator_identity(row)
            continue
        started_at = pending_started_at.get(conversation_id)
        if started_at is None:
            continue
        is_finish = False
        if finish_types and event_type in finish_types:
            is_finish = True
        elif callable(finish_matcher):
            is_finish = bool(finish_matcher(row))
        if not is_finish:
            continue
        if operator_from == "finish":
            operator_key, operator_label = _event_operator_identity(row)
        else:
            operator_key, operator_label = pending_operator.get(conversation_id, ("unknown", "unknown"))
        delta_minutes = int((created_at - started_at).total_seconds() // 60)
        if delta_minutes >= 0:
            samples_by_operator.setdefault(operator_key, []).append(delta_minutes)
            labels_by_operator[operator_key] = operator_label
        pending_started_at.pop(conversation_id, None)
        pending_operator.pop(conversation_id, None)
    rows = [
        {
            "operator_key": key,
            "operator": labels_by_operator.get(key, key),
            "median_minutes": _median_minutes(samples),
            "samples": len(samples),
        }
        for key, samples in samples_by_operator.items()
        if samples
    ]
    rows.sort(
        key=lambda item: (
            -(item.get("median_minutes") or 0) if descending else (item.get("median_minutes") or 0),
            -int(item.get("samples") or 0),
            str(item.get("operator") or ""),
        )
    )
    return rows


def _collect_resolution_episodes(
    events: list[dict],
    *,
    start_types: set[str],
    operator_from: str,
    since: datetime | None = None,
    finish_types: set[str] | None = None,
    finish_matcher=None,
) -> list[dict[str, object]]:
    pending_rows: dict[int, dict] = {}
    pending_started_at: dict[int, datetime] = {}
    pending_operator: dict[int, tuple[str, str]] = {}
    ordered = sorted(
        events,
        key=lambda row: (
            int(row.get("conversation_id") or 0),
            int(row.get("id") or 0),
        ),
    )
    episodes: list[dict[str, object]] = []
    for row in ordered:
        conversation_id = int(row.get("conversation_id") or 0)
        event_type = str(row.get("event_type") or "").strip()
        created_at = _parse_event_time(row.get("created_at"))
        if not conversation_id or not event_type or created_at is None:
            continue
        if event_type in start_types:
            if since is not None and created_at < since:
                pending_rows.pop(conversation_id, None)
                pending_started_at.pop(conversation_id, None)
                pending_operator.pop(conversation_id, None)
                continue
            pending_rows[conversation_id] = row
            pending_started_at[conversation_id] = created_at
            if operator_from == "start":
                pending_operator[conversation_id] = _event_operator_identity(row)
            continue
        started_at = pending_started_at.get(conversation_id)
        start_row = pending_rows.get(conversation_id)
        if started_at is None or start_row is None:
            continue
        is_finish = False
        if finish_types and event_type in finish_types:
            is_finish = True
        elif callable(finish_matcher):
            is_finish = bool(finish_matcher(row))
        if not is_finish:
            continue
        if operator_from == "finish":
            operator_key, operator_label = _event_operator_identity(row)
        else:
            operator_key, operator_label = pending_operator.get(conversation_id, ("unknown", "unknown"))
        delta_minutes = int((created_at - started_at).total_seconds() // 60)
        if delta_minutes >= 0:
            episodes.append(
                {
                    "operator_key": operator_key,
                    "operator": operator_label,
                    "conversation_id": conversation_id,
                    "display_name": str(row.get("display_name") or start_row.get("display_name") or f"conv:{conversation_id}"),
                    "channel": str(row.get("channel") or start_row.get("channel") or ""),
                    "external_chat_id": str(row.get("external_chat_id") or start_row.get("external_chat_id") or ""),
                    "started_at": start_row.get("created_at"),
                    "resolved_at": row.get("created_at"),
                    "duration_minutes": delta_minutes,
                    "resolution": _resolution_label(row),
                }
            )
        pending_rows.pop(conversation_id, None)
        pending_started_at.pop(conversation_id, None)
        pending_operator.pop(conversation_id, None)
    return episodes


class SalesBotService:
    def __init__(self, repository: RepositoryProtocol, *, owner_ttl_minutes: int = 120) -> None:
        self.repository = repository
        self.owner_ttl_minutes = max(1, int(owner_ttl_minutes))

    def ownership_is_expired(self, snapshot: ConversationSnapshot, *, now: datetime | None = None) -> bool:
        owner_claimed_at = snapshot.owner_claimed_at
        if owner_claimed_at is None:
            return False
        has_owner = bool(snapshot.owner_id.strip() or snapshot.owner_name.strip())
        if not has_owner:
            return False
        current_time = now or _utcnow()
        return owner_claimed_at + timedelta(minutes=self.owner_ttl_minutes) <= current_time

    def _record_expired_ownership(self, snapshot: ConversationSnapshot, *, actor: str, operator_id: str = "") -> None:
        if not self.ownership_is_expired(snapshot):
            return
        self.repository.add_conversation_event(
            conversation_id=snapshot.conversation_id,
            event_type="lock_expired",
            actor=actor,
            payload={
                "operator_id": operator_id,
                "previous_owner_id": snapshot.owner_id,
                "previous_owner_name": snapshot.owner_name,
                "owner_claimed_at": snapshot.owner_claimed_at.isoformat() if snapshot.owner_claimed_at else "",
                "ttl_minutes": self.owner_ttl_minutes,
            },
        )

    def ingest_inbound_message(self, message: InboundMessage) -> ConversationSnapshot:
        snapshot = self.repository.ingest_customer_message(message)
        if snapshot.mode == ConversationMode.MANAGER:
            self.repository.update_conversation_state(
                conversation_id=snapshot.conversation_id,
                status=ConversationStatus.WAITING_MANAGER,
                needs_attention=True,
            )
            self.repository.add_conversation_event(
                conversation_id=snapshot.conversation_id,
                event_type="customer_waiting_manager",
                payload={"status": ConversationStatus.WAITING_MANAGER.value},
            )
            return self.repository.get_snapshot(snapshot.conversation_id)
        if snapshot.status == ConversationStatus.CLOSED:
            self.repository.update_conversation_state(
                conversation_id=snapshot.conversation_id,
                status=ConversationStatus.NEW,
            )
            return self.repository.get_snapshot(snapshot.conversation_id)
        return snapshot

    def get_snapshot(self, conversation_id: int) -> ConversationSnapshot:
        return self.repository.get_snapshot(conversation_id)

    def record_ai_reply(self, *, conversation_id: int, text: str) -> ConversationSnapshot:
        self.repository.add_message(
            conversation_id=conversation_id,
            sender_role=SenderRole.AI,
            sender_name="LesDal AI",
            text=text,
        )
        self.repository.add_conversation_event(
            conversation_id=conversation_id,
            event_type="ai_reply",
            actor="LesDal AI",
            payload={"text": text},
        )
        return self.repository.get_snapshot(conversation_id)

    def record_manager_reply(
        self,
        *,
        conversation_id: int,
        manager_name: str,
        operator_id: str = "",
        text: str,
        pause_ai: bool = True,
        delivery_key: str = "",
    ) -> ConversationSnapshot:
        snapshot = self.repository.get_snapshot(conversation_id)
        owner_id = snapshot.owner_id.strip()
        owner_name = snapshot.owner_name.strip()
        ownership_expired = self.ownership_is_expired(snapshot)
        resolved_delivery_key = delivery_key.strip() or secrets.token_urlsafe(12)
        if pause_ai and owner_id and operator_id and owner_id != operator_id and not ownership_expired:
            raise ConversationOwnershipError(
                f"Conversation is already owned by {_owner_label(owner_id=owner_id, owner_name=owner_name)}",
                reason="owned_by_other",
            )
        if pause_ai and not owner_id and owner_name and owner_name != manager_name and not ownership_expired:
            raise ConversationOwnershipError(
                f"Conversation is already owned by {_owner_label(owner_name=owner_name)}",
                reason="owned_by_other",
            )
        if pause_ai:
            if ownership_expired:
                self._record_expired_ownership(
                    snapshot,
                    actor=manager_name,
                    operator_id=operator_id,
                )
            self.repository.update_conversation_state(
                conversation_id=conversation_id,
                mode=ConversationMode.MANAGER,
                status=ConversationStatus.IN_PROGRESS,
                owner_id=operator_id or ("" if ownership_expired else owner_id) or manager_name,
                owner_name=manager_name,
                owner_claimed_at=_utcnow(),
                needs_attention=False,
            )
        self.repository.add_message(
            conversation_id=conversation_id,
            sender_role=SenderRole.MANAGER,
            sender_name=manager_name,
            text=text,
        )
        self.repository.add_conversation_event(
            conversation_id=conversation_id,
            event_type="manager_reply",
            actor=manager_name,
            payload={
                "text": text,
                "pause_ai": pause_ai,
                "operator_id": operator_id,
                "delivery_key": resolved_delivery_key,
            },
        )
        return self.repository.get_snapshot(conversation_id)

    def record_reply_send_outcome(
        self,
        *,
        conversation_id: int,
        delivery_key: str,
        actor: str = "",
        operator_id: str = "",
        outbound_result: OutboundSendResult,
        retry: bool = False,
    ) -> None:
        resolved_delivery_key = str(delivery_key or "").strip()
        if not resolved_delivery_key:
            return
        payload = {
            "delivery_key": resolved_delivery_key,
            "operator_id": operator_id,
            "channel": outbound_result.channel.value,
            "ok": outbound_result.ok,
            "error": outbound_result.error,
            "retryable": outbound_result.retryable,
            "message_id": outbound_result.message_id,
            "retry": retry,
        }
        if retry:
            self.repository.add_conversation_event(
                conversation_id=conversation_id,
                event_type="reply_send_retried",
                actor=actor,
                payload=payload,
            )
        self.repository.add_conversation_event(
            conversation_id=conversation_id,
            event_type="reply_send_succeeded" if outbound_result.ok else "reply_send_failed",
            actor=actor,
            payload=payload,
        )

    def prepare_reply_retry(
        self,
        *,
        conversation_id: int,
        delivery_key: str,
    ) -> ReplyRetryContext:
        resolved_delivery_key = str(delivery_key or "").strip()
        if not resolved_delivery_key:
            raise ValueError("delivery_key is required")
        events = [
            dict(row)
            for row in self.repository.list_conversation_events(conversation_id, limit=200)
        ]
        manager_reply_event = next(
            (
                row for row in events
                if str(row.get("event_type") or "") == "manager_reply"
                and _payload_delivery_key(row) == resolved_delivery_key
            ),
            None,
        )
        if manager_reply_event is None:
            raise LookupError(f"Reply delivery {resolved_delivery_key} not found")
        manager_reply_payload = _coerce_event_payload(manager_reply_event.get("payload"))
        text = str(manager_reply_payload.get("text") or "")
        if not text:
            raise LookupError(f"Reply delivery {resolved_delivery_key} has no text")
        success_event = next(
            (
                row for row in events
                if str(row.get("event_type") or "") == "reply_send_succeeded"
                and _payload_delivery_key(row) == resolved_delivery_key
            ),
            None,
        )
        if success_event is not None:
            success_payload = _coerce_event_payload(success_event.get("payload"))
            snapshot = self.repository.get_snapshot(conversation_id)
            channel_raw = str(success_payload.get("channel") or snapshot.channel.value)
            return ReplyRetryContext(
                text=text,
                already_delivered=True,
                previous_result=OutboundSendResult(
                    ok=True,
                    channel=snapshot.channel if channel_raw == snapshot.channel.value else type(snapshot.channel)(channel_raw),
                    error="",
                    retryable=False,
                    message_id=str(success_payload.get("message_id") or ""),
                ),
            )
        return ReplyRetryContext(text=text)

    def resume_ai(self, *, conversation_id: int) -> ConversationSnapshot:
        snapshot = self.repository.get_snapshot(conversation_id)
        self.repository.update_conversation_state(
            conversation_id=conversation_id,
            mode=ConversationMode.AI,
            status=ConversationStatus.IN_PROGRESS,
            clear_owner=True,
            needs_attention=False,
        )
        self.repository.add_message(
            conversation_id=conversation_id,
            sender_role=SenderRole.SYSTEM,
            sender_name="system",
            text="Conversation returned to AI mode.",
        )
        self.repository.add_conversation_event(
            conversation_id=conversation_id,
            event_type="returned_to_ai",
            actor=snapshot.owner_name or "system",
            payload={"mode": ConversationMode.AI.value},
        )
        return self.repository.get_snapshot(conversation_id)

    def update_lead_profile(
        self,
        *,
        conversation_id: int,
        stage: LeadStage | None = None,
        summary: str | None = None,
        city: str | None = None,
        interested_products: list[str] | None = None,
        tags: list[str] | None = None,
        manager_notes: str | None = None,
        priority: LeadPriority | None = None,
        follow_up_date: str | None = None,
        next_action: str | None = None,
        actor: str = "",
        actor_id: str = "",
        amocrm_lead_id: str | None = None,
    ) -> ConversationSnapshot:
        if follow_up_date:
            try:
                datetime.fromisoformat(follow_up_date)
            except ValueError as exc:
                raise LeadProfileValidationError("follow_up_date must be in YYYY-MM-DD format") from exc
        if priority in {LeadPriority.HIGH, LeadPriority.URGENT} and not (next_action or "").strip():
            raise LeadProfileValidationError("next_action is required for high or urgent priority")
        snapshot = self.repository.get_snapshot(conversation_id)
        self.repository.update_lead(
            lead_id=snapshot.lead_id,
            stage=stage,
            summary=summary,
            city=city,
            interested_products=interested_products,
            tags=tags,
            manager_notes=manager_notes,
            priority=priority,
            follow_up_date=follow_up_date,
            next_action=next_action,
            amocrm_lead_id=amocrm_lead_id,
        )
        payload: dict[str, object] = {}
        if stage is not None:
            payload["stage"] = stage.value
        if summary is not None:
            payload["summary"] = summary
        if tags is not None:
            payload["tags"] = tags
        if city is not None:
            payload["city"] = city
        if interested_products is not None:
            payload["interested_products"] = interested_products
        if manager_notes is not None:
            payload["manager_notes"] = manager_notes
        if priority is not None:
            payload["priority"] = priority.value
        if follow_up_date is not None:
            payload["follow_up_date"] = follow_up_date
        if next_action is not None:
            payload["next_action"] = next_action
        if amocrm_lead_id is not None:
            payload["amocrm_lead_id"] = amocrm_lead_id
        if payload:
            self.repository.add_conversation_event(
                conversation_id=conversation_id,
                event_type="lead_profile_updated",
                actor=actor,
                payload={**payload, "operator_id": actor_id},
            )
        return self.repository.get_snapshot(conversation_id)

    def update_manager_notes(
        self,
        *,
        conversation_id: int,
        notes: str,
        actor: str = "",
        actor_id: str = "",
    ) -> ConversationSnapshot:
        snapshot = self.repository.get_snapshot(conversation_id)
        self.repository.update_lead(
            lead_id=snapshot.lead_id,
            manager_notes=notes,
        )
        self.repository.add_conversation_event(
            conversation_id=conversation_id,
            event_type="manager_notes_updated",
            actor=actor,
            payload={"notes": notes, "operator_id": actor_id},
        )
        return self.repository.get_snapshot(conversation_id)

    def set_conversation_mode(
        self,
        *,
        conversation_id: int,
        mode: ConversationMode,
    ) -> ConversationSnapshot:
        self.repository.update_conversation_state(
            conversation_id=conversation_id,
            mode=mode,
        )
        return self.repository.get_snapshot(conversation_id)

    def claim_conversation(
        self,
        *,
        conversation_id: int,
        operator_name: str,
        operator_id: str = "",
        force: bool = False,
    ) -> ConversationSnapshot:
        snapshot = self.repository.get_snapshot(conversation_id)
        existing_owner_id = snapshot.owner_id.strip()
        owner_name = snapshot.owner_name.strip()
        ownership_expired = self.ownership_is_expired(snapshot)
        forced_reassign = False
        if existing_owner_id and operator_id and existing_owner_id != operator_id and not force and not ownership_expired:
            raise ConversationOwnershipError(
                f"Conversation is already owned by {_owner_label(owner_id=existing_owner_id, owner_name=owner_name)}",
                reason="owned_by_other",
            )
        if not existing_owner_id and owner_name and owner_name != operator_name and not force and not ownership_expired:
            raise ConversationOwnershipError(
                f"Conversation is already owned by {_owner_label(owner_name=owner_name)}",
                reason="owned_by_other",
            )
        if force and not ownership_expired and (
            (existing_owner_id and operator_id and existing_owner_id != operator_id)
            or (not existing_owner_id and owner_name and owner_name != operator_name)
        ):
            forced_reassign = True
        if ownership_expired:
            self._record_expired_ownership(
                snapshot,
                actor=operator_name,
                operator_id=operator_id,
            )
        self.repository.update_conversation_state(
            conversation_id=conversation_id,
            mode=ConversationMode.MANAGER,
            status=ConversationStatus.IN_PROGRESS,
            owner_id=operator_id or ("" if ownership_expired else existing_owner_id) or operator_name,
            owner_name=operator_name,
            owner_claimed_at=_utcnow(),
            needs_attention=False,
        )
        self.repository.add_conversation_event(
            conversation_id=conversation_id,
            event_type="force_claimed_by_supervisor" if forced_reassign else "claimed_by_manager",
            actor=operator_name,
            payload={
                "status": ConversationStatus.IN_PROGRESS.value,
                "operator_id": operator_id,
                "forced": forced_reassign,
                "previous_owner_id": existing_owner_id,
                "previous_owner_name": owner_name,
            },
        )
        return self.repository.get_snapshot(conversation_id)

    def set_conversation_status(
        self,
        *,
        conversation_id: int,
        status: ConversationStatus,
        actor: str = "",
        actor_id: str = "",
    ) -> ConversationSnapshot:
        self.repository.update_conversation_state(
            conversation_id=conversation_id,
            status=status,
        )
        self.repository.add_conversation_event(
            conversation_id=conversation_id,
            event_type="status_changed",
            actor=actor,
            payload={"status": status.value, "operator_id": actor_id},
        )
        return self.repository.get_snapshot(conversation_id)

    def release_conversation(
        self,
        *,
        conversation_id: int,
        operator_name: str,
        operator_id: str = "",
    ) -> ConversationSnapshot:
        snapshot = self.repository.get_snapshot(conversation_id)
        existing_owner_id = snapshot.owner_id.strip()
        owner_name = snapshot.owner_name.strip()
        ownership_expired = self.ownership_is_expired(snapshot)
        if ownership_expired and (
            (existing_owner_id and operator_id and existing_owner_id != operator_id)
            or (not existing_owner_id and owner_name and owner_name != operator_name)
        ):
            raise ConversationOwnershipError(
                "Conversation ownership has expired; claim it again before releasing",
                reason="ownership_expired",
            )
        if existing_owner_id and operator_id and existing_owner_id != operator_id:
            raise ConversationOwnershipError(
                f"Conversation is already owned by {_owner_label(owner_id=existing_owner_id, owner_name=owner_name)}",
                reason="owned_by_other",
            )
        if not existing_owner_id and owner_name and owner_name != operator_name:
            raise ConversationOwnershipError(
                f"Conversation is already owned by {_owner_label(owner_name=owner_name)}",
                reason="owned_by_other",
            )
        next_status = (
            ConversationStatus.WAITING_MANAGER
            if snapshot.needs_attention
            else ConversationStatus.NEW
        )
        self.repository.update_conversation_state(
            conversation_id=conversation_id,
            status=next_status,
            clear_owner=True,
        )
        self.repository.add_conversation_event(
            conversation_id=conversation_id,
            event_type="released_by_manager",
            actor=operator_name,
            payload={"status": next_status.value, "operator_id": operator_id},
        )
        return self.repository.get_snapshot(conversation_id)

    def build_manager_summary(self, *, conversation_id: int, limit: int = 12) -> str:
        snapshot = self.repository.get_snapshot(conversation_id)
        transcript = self.repository.build_transcript(conversation_id, limit=limit)
        parts = [
            f"Channel: {snapshot.channel.value}",
            f"Stage: {snapshot.stage.value}",
            f"Mode: {snapshot.mode.value}",
        ]
        if snapshot.summary:
            parts.append(f"Summary: {snapshot.summary}")
        if snapshot.interested_products:
            parts.append("Interested products: " + ", ".join(snapshot.interested_products))
        if snapshot.tags:
            parts.append("Tags: " + ", ".join(snapshot.tags))

        if transcript:
            parts.append("Recent messages:")
            for row in reversed(transcript):
                sender_name = row["sender_name"] or row["sender_role"]
                parts.append(f"- {sender_name}: {row['text']}")

        return "\n".join(parts)

    def get_conversation_target(self, conversation_id: int) -> dict:
        return self.repository.get_conversation_target(conversation_id)

    def list_recent_conversations(
        self,
        *,
        limit: int = 20,
        channel: str = "",
        mode: str = "",
        status: str = "",
        owner: str = "",
        q: str = "",
        needs_attention: bool | None = None,
        forced_only: bool | None = None,
    ) -> list[dict]:
        rows = self.repository.list_recent_conversations(limit=limit)
        items = [dict(row) for row in rows]
        if channel:
            items = [row for row in items if str(row.get("channel", "")) == channel]
        if mode:
            items = [row for row in items if str(row.get("mode", "")) == mode]
        if status:
            items = [row for row in items if str(row.get("status", "")) == status]
        if owner:
            owner_lower = owner.lower()
            items = [
                row
                for row in items
                if owner_lower in str(row.get("owner_name", "")).lower()
                or owner_lower in str(row.get("owner_id", "")).lower()
            ]
        if q:
            q_lower = q.lower()
            items = [
                row
                for row in items
                if q_lower in " ".join(
                    [
                        str(row.get("display_name", "")),
                        str(row.get("username", "")),
                        str(row.get("summary", "")),
                        str(row.get("external_chat_id", "")),
                    ]
                ).lower()
            ]
        if needs_attention is not None:
            items = [
                row
                for row in items
                if bool(row.get("needs_attention", False)) == needs_attention
            ]
        if forced_only is not None:
            items = [
                row
                for row in items
                if bool(row.get("has_forced_takeover", False)) == forced_only
            ]
        return items[:limit]

    def get_transcript(self, *, conversation_id: int, limit: int = 30) -> list[dict]:
        rows = self.repository.build_transcript(conversation_id, limit=limit)
        return [dict(row) for row in rows]

    def get_conversation_events(self, *, conversation_id: int, limit: int = 50) -> list[dict]:
        rows = self.repository.list_conversation_events(conversation_id, limit=limit)
        return [dict(row) for row in rows]

    def get_forced_takeover_summary(self, *, limit: int = 200, period: str = "30d") -> dict:
        rows = self.repository.list_forced_takeover_events(limit=limit)
        events = [dict(row) for row in rows]
        current_rows = [dict(row) for row in self.repository.list_recent_conversations(limit=1000)]
        transition_rows = [
            dict(row)
            for row in self.repository.list_conversation_events_by_type(
                event_types=(
                    "customer_waiting_manager",
                    "manager_reply",
                    "force_claimed_by_supervisor",
                    "returned_to_ai",
                    "status_changed",
                ),
                limit=5000,
            )
        ]
        now = _utcnow()
        normalized_period, period_label, period_start = _resolve_audit_period(period, now=now)
        today = now.date()
        week_start = now.date().fromordinal(today.toordinal() - today.weekday())
        filtered_events = [
            row for row in events
            if period_start is None
            or (
                (created_at := _parse_event_time(row.get("created_at"))) is not None
                and created_at >= period_start
            )
        ]
        today_count = 0
        week_count = 0
        by_operator: dict[str, int] = {}
        recent: list[dict] = []
        for row in filtered_events:
            created_at_raw = row.get("created_at")
            created_at = None
            if created_at_raw:
                try:
                    created_at = datetime.fromisoformat(str(created_at_raw))
                except ValueError:
                    created_at = None
            actor = str(row.get("actor") or "unknown")
            payload = row.get("payload") or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = {}
            by_operator[actor] = by_operator.get(actor, 0) + 1
            if created_at is not None:
                event_date = created_at.astimezone(timezone.utc).date()
                if event_date == today:
                    today_count += 1
                if event_date >= week_start:
                    week_count += 1
            recent.append(
                {
                    "conversation_id": row.get("conversation_id"),
                    "actor": actor,
                    "created_at": created_at_raw,
                    "display_name": row.get("display_name", ""),
                    "channel": row.get("channel", ""),
                    "previous_owner_name": payload.get("previous_owner_name", row.get("previous_owner_name", "")),
                    "previous_owner_id": payload.get("previous_owner_id", row.get("previous_owner_id", "")),
                }
            )
        by_operator_items = [
            {"operator": operator, "count": count}
            for operator, count in sorted(by_operator.items(), key=lambda item: (-item[1], item[0]))
        ]
        ownership_quality = {
            "waiting_manager_count": sum(
                1 for row in current_rows
                if str(row.get("status") or "") == ConversationStatus.WAITING_MANAGER.value
            ),
            "manager_without_reply_count": sum(
                1 for row in current_rows
                if str(row.get("mode") or "") == ConversationMode.MANAGER.value
                and str(row.get("status") or "") == ConversationStatus.IN_PROGRESS.value
                and (str(row.get("owner_id") or "").strip() or str(row.get("owner_name") or "").strip())
                and not row.get("last_manager_message_at")
            ),
            "forced_closed_count": sum(
                1 for row in current_rows
                if bool(row.get("has_forced_takeover", False))
                and str(row.get("status") or "") == ConversationStatus.CLOSED.value
            ),
            "forced_returned_ai_count": sum(
                1 for row in current_rows
                if bool(row.get("has_forced_takeover", False))
                and str(row.get("mode") or "") == ConversationMode.AI.value
            ),
        }
        waiting_to_reply_minutes = _collect_resolution_minutes(
            transition_rows,
            start_types={"customer_waiting_manager"},
            since=period_start,
            finish_types={"manager_reply"},
        )
        forced_to_resolution_minutes = _collect_resolution_minutes(
            transition_rows,
            start_types={"force_claimed_by_supervisor"},
            since=period_start,
            finish_types={"returned_to_ai"},
            finish_matcher=lambda row: (
                str(row.get("event_type") or "") == "status_changed"
                and _coerce_event_payload(row.get("payload")).get("status") == ConversationStatus.CLOSED.value
            ),
        )
        waiting_to_reply_by_operator = _collect_resolution_minutes_by_operator(
            transition_rows,
            start_types={"customer_waiting_manager"},
            operator_from="finish",
            since=period_start,
            finish_types={"manager_reply"},
        )
        forced_to_resolution_by_operator = _collect_resolution_minutes_by_operator(
            transition_rows,
            start_types={"force_claimed_by_supervisor"},
            operator_from="start",
            since=period_start,
            finish_types={"returned_to_ai"},
            finish_matcher=lambda row: (
                str(row.get("event_type") or "") == "status_changed"
                and _coerce_event_payload(row.get("payload")).get("status") == ConversationStatus.CLOSED.value
            ),
            descending=True,
        )
        resolution_speed = {
            "waiting_to_first_reply_median_minutes": _median_minutes(waiting_to_reply_minutes),
            "waiting_to_first_reply_samples": len(waiting_to_reply_minutes),
            "forced_to_resolution_median_minutes": _median_minutes(forced_to_resolution_minutes),
            "forced_to_resolution_samples": len(forced_to_resolution_minutes),
            "waiting_to_first_reply_by_operator": waiting_to_reply_by_operator[:8],
            "forced_to_resolution_by_operator": forced_to_resolution_by_operator[:8],
        }
        return {
            "period": normalized_period,
            "period_label": period_label,
            "today_count": today_count,
            "week_count": week_count,
            "total_count": len(filtered_events),
            "by_operator": by_operator_items,
            "recent": recent[:8],
            "ownership_quality": ownership_quality,
            "resolution_speed": resolution_speed,
        }

    def get_resolution_speed_drilldown(
        self,
        *,
        metric: str,
        operator_key: str,
        period: str = "30d",
        limit: int = 50,
    ) -> dict:
        normalized_metric = str(metric or "").strip().lower()
        target_operator = str(operator_key or "").strip().lower()
        if normalized_metric not in {"waiting_reply", "forced_resolution"}:
            raise ValueError("Unsupported drilldown metric")
        if not target_operator:
            raise ValueError("operator_key is required")
        now = _utcnow()
        normalized_period, period_label, period_start = _resolve_audit_period(period, now=now)
        transition_rows = [
            dict(row)
            for row in self.repository.list_conversation_events_by_type(
                event_types=(
                    "customer_waiting_manager",
                    "manager_reply",
                    "force_claimed_by_supervisor",
                    "returned_to_ai",
                    "status_changed",
                ),
                limit=5000,
            )
        ]
        if normalized_metric == "waiting_reply":
            episodes = _collect_resolution_episodes(
                transition_rows,
                start_types={"customer_waiting_manager"},
                operator_from="finish",
                since=period_start,
                finish_types={"manager_reply"},
            )
            metric_label = "Waiting -> first manager reply"
        else:
            episodes = _collect_resolution_episodes(
                transition_rows,
                start_types={"force_claimed_by_supervisor"},
                operator_from="start",
                since=period_start,
                finish_types={"returned_to_ai"},
                finish_matcher=lambda row: (
                    str(row.get("event_type") or "") == "status_changed"
                    and _coerce_event_payload(row.get("payload")).get("status") == ConversationStatus.CLOSED.value
                ),
            )
            metric_label = "Forced takeover -> resolution"
        filtered = [
            row for row in episodes
            if str(row.get("operator_key") or "").strip().lower() == target_operator
        ]
        filtered.sort(
            key=lambda row: (
                -int(row.get("duration_minutes") or 0),
                str(row.get("resolved_at") or ""),
            )
        )
        operator_label = filtered[0]["operator"] if filtered else target_operator
        sample_minutes = [int(row.get("duration_minutes") or 0) for row in filtered]
        return {
            "metric": normalized_metric,
            "metric_label": metric_label,
            "operator_key": target_operator,
            "operator": operator_label,
            "period": normalized_period,
            "period_label": period_label,
            "median_minutes": _median_minutes(sample_minutes),
            "samples": len(filtered),
            "episodes": filtered[:limit],
        }
