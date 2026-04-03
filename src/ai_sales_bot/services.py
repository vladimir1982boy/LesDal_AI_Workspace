from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol

from .domain import ConversationMode, ConversationSnapshot, ConversationStatus, InboundMessage, LeadPriority, LeadStage, SenderRole


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ConversationOwnershipError(RuntimeError):
    pass


class LeadProfileValidationError(ValueError):
    pass


class RepositoryProtocol(Protocol):
    def ingest_customer_message(self, message: InboundMessage) -> ConversationSnapshot: ...
    def add_message(self, *, conversation_id: int, sender_role: SenderRole, text: str, sender_name: str = "", raw_payload: dict | None = None) -> int: ...
    def add_conversation_event(self, *, conversation_id: int, event_type: str, actor: str = "", payload: dict | None = None) -> int: ...
    def get_snapshot(self, conversation_id: int) -> ConversationSnapshot: ...
    def get_conversation_target(self, conversation_id: int) -> dict: ...
    def list_recent_conversations(self, *, limit: int = 20) -> list[dict]: ...
    def list_conversation_events(self, conversation_id: int, *, limit: int = 50) -> list[dict]: ...
    def set_conversation_mode(self, *, conversation_id: int, mode: ConversationMode) -> None: ...
    def update_conversation_state(self, *, conversation_id: int, mode: ConversationMode | None = None, status: ConversationStatus | None = None, owner_name: str | None = None, owner_claimed_at: datetime | None = None, clear_owner: bool = False, needs_attention: bool | None = None) -> None: ...
    def update_lead(self, *, lead_id: int, stage: LeadStage | None = None, mode: ConversationMode | None = None, summary: str | None = None, city: str | None = None, interested_products: list[str] | None = None, tags: list[str] | None = None, manager_notes: str | None = None, priority: LeadPriority | None = None, follow_up_date: str | None = None, next_action: str | None = None, amocrm_lead_id: str | None = None) -> None: ...
    def build_transcript(self, conversation_id: int, *, limit: int = 30) -> list[dict]: ...


class SalesBotService:
    def __init__(self, repository: RepositoryProtocol) -> None:
        self.repository = repository

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
        text: str,
        pause_ai: bool = True,
    ) -> ConversationSnapshot:
        snapshot = self.repository.get_snapshot(conversation_id)
        owner_name = snapshot.owner_name.strip()
        if pause_ai and owner_name and owner_name != manager_name:
            raise ConversationOwnershipError(
                f"Conversation is already owned by {owner_name}"
            )
        if pause_ai:
            self.repository.update_conversation_state(
                conversation_id=conversation_id,
                mode=ConversationMode.MANAGER,
                status=ConversationStatus.IN_PROGRESS,
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
            payload={"text": text, "pause_ai": pause_ai},
        )
        return self.repository.get_snapshot(conversation_id)

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
                payload=payload,
            )
        return self.repository.get_snapshot(conversation_id)

    def update_manager_notes(
        self,
        *,
        conversation_id: int,
        notes: str,
        actor: str = "",
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
            payload={"notes": notes},
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
    ) -> ConversationSnapshot:
        snapshot = self.repository.get_snapshot(conversation_id)
        owner_name = snapshot.owner_name.strip()
        if owner_name and owner_name != operator_name:
            raise ConversationOwnershipError(
                f"Conversation is already owned by {owner_name}"
            )
        self.repository.update_conversation_state(
            conversation_id=conversation_id,
            mode=ConversationMode.MANAGER,
            status=ConversationStatus.IN_PROGRESS,
            owner_name=operator_name,
            owner_claimed_at=_utcnow(),
            needs_attention=False,
        )
        self.repository.add_conversation_event(
            conversation_id=conversation_id,
            event_type="claimed_by_manager",
            actor=operator_name,
            payload={"status": ConversationStatus.IN_PROGRESS.value},
        )
        return self.repository.get_snapshot(conversation_id)

    def set_conversation_status(
        self,
        *,
        conversation_id: int,
        status: ConversationStatus,
        actor: str = "",
    ) -> ConversationSnapshot:
        self.repository.update_conversation_state(
            conversation_id=conversation_id,
            status=status,
        )
        self.repository.add_conversation_event(
            conversation_id=conversation_id,
            event_type="status_changed",
            actor=actor,
            payload={"status": status.value},
        )
        return self.repository.get_snapshot(conversation_id)

    def release_conversation(
        self,
        *,
        conversation_id: int,
        operator_name: str,
    ) -> ConversationSnapshot:
        snapshot = self.repository.get_snapshot(conversation_id)
        owner_name = snapshot.owner_name.strip()
        if owner_name and owner_name != operator_name:
            raise ConversationOwnershipError(
                f"Conversation is already owned by {owner_name}"
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
            payload={"status": next_status.value},
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
        return items[:limit]

    def get_transcript(self, *, conversation_id: int, limit: int = 30) -> list[dict]:
        rows = self.repository.build_transcript(conversation_id, limit=limit)
        return [dict(row) for row in rows]

    def get_conversation_events(self, *, conversation_id: int, limit: int = 50) -> list[dict]:
        rows = self.repository.list_conversation_events(conversation_id, limit=limit)
        return [dict(row) for row in rows]
