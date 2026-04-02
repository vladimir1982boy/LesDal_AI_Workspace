from __future__ import annotations

from typing import Protocol

from .domain import ConversationMode, ConversationSnapshot, InboundMessage, LeadStage, SenderRole


class RepositoryProtocol(Protocol):
    def ingest_customer_message(self, message: InboundMessage) -> ConversationSnapshot: ...
    def add_message(self, *, conversation_id: int, sender_role: SenderRole, text: str, sender_name: str = "", raw_payload: dict | None = None) -> int: ...
    def add_conversation_event(self, *, conversation_id: int, event_type: str, actor: str = "", payload: dict | None = None) -> int: ...
    def get_snapshot(self, conversation_id: int) -> ConversationSnapshot: ...
    def get_conversation_target(self, conversation_id: int) -> dict: ...
    def list_recent_conversations(self, *, limit: int = 20) -> list[dict]: ...
    def list_conversation_events(self, conversation_id: int, *, limit: int = 50) -> list[dict]: ...
    def set_conversation_mode(self, *, conversation_id: int, mode: ConversationMode) -> None: ...
    def update_lead(self, *, lead_id: int, stage: LeadStage | None = None, mode: ConversationMode | None = None, summary: str | None = None, city: str | None = None, interested_products: list[str] | None = None, tags: list[str] | None = None, amocrm_lead_id: str | None = None) -> None: ...
    def build_transcript(self, conversation_id: int, *, limit: int = 30) -> list[dict]: ...


class SalesBotService:
    def __init__(self, repository: RepositoryProtocol) -> None:
        self.repository = repository

    def ingest_inbound_message(self, message: InboundMessage) -> ConversationSnapshot:
        return self.repository.ingest_customer_message(message)

    def get_snapshot(self, conversation_id: int) -> ConversationSnapshot:
        return self.repository.get_snapshot(conversation_id)

    def record_ai_reply(self, *, conversation_id: int, text: str) -> ConversationSnapshot:
        self.repository.add_message(
            conversation_id=conversation_id,
            sender_role=SenderRole.AI,
            sender_name="LesDal AI",
            text=text,
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
        if pause_ai:
            self.repository.set_conversation_mode(
                conversation_id=conversation_id,
                mode=ConversationMode.MANAGER,
            )
        self.repository.add_message(
            conversation_id=conversation_id,
            sender_role=SenderRole.MANAGER,
            sender_name=manager_name,
            text=text,
        )
        return self.repository.get_snapshot(conversation_id)

    def resume_ai(self, *, conversation_id: int) -> ConversationSnapshot:
        self.repository.set_conversation_mode(
            conversation_id=conversation_id,
            mode=ConversationMode.AI,
        )
        self.repository.add_message(
            conversation_id=conversation_id,
            sender_role=SenderRole.SYSTEM,
            sender_name="system",
            text="Conversation returned to AI mode.",
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
        amocrm_lead_id: str | None = None,
    ) -> ConversationSnapshot:
        snapshot = self.repository.get_snapshot(conversation_id)
        self.repository.update_lead(
            lead_id=snapshot.lead_id,
            stage=stage,
            summary=summary,
            city=city,
            interested_products=interested_products,
            tags=tags,
            amocrm_lead_id=amocrm_lead_id,
        )
        return self.repository.get_snapshot(conversation_id)

    def set_conversation_mode(
        self,
        *,
        conversation_id: int,
        mode: ConversationMode,
    ) -> ConversationSnapshot:
        self.repository.set_conversation_mode(
            conversation_id=conversation_id,
            mode=mode,
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

    def list_recent_conversations(self, *, limit: int = 20) -> list[dict]:
        rows = self.repository.list_recent_conversations(limit=limit)
        return [dict(row) for row in rows]

    def get_transcript(self, *, conversation_id: int, limit: int = 30) -> list[dict]:
        rows = self.repository.build_transcript(conversation_id, limit=limit)
        return [dict(row) for row in rows]

    def get_conversation_events(self, *, conversation_id: int, limit: int = 50) -> list[dict]:
        rows = self.repository.list_conversation_events(conversation_id, limit=limit)
        return [dict(row) for row in rows]
