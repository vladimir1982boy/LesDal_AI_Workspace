from __future__ import annotations

from dataclasses import dataclass

from .ai_engine import GeminiSalesAssistant, LeadHints, infer_lead_hints
from .app import SalesBotRuntime, create_runtime
from .domain import ConversationMode, ConversationSnapshot, InboundMessage
from .lead_sync import LeadSyncCoordinator


@dataclass(slots=True)
class CustomerTurnResult:
    snapshot: ConversationSnapshot
    hints: LeadHints
    admin_notification: str
    reply_text: str | None = None


class SalesConversationManager:
    def __init__(self, runtime: SalesBotRuntime | None = None) -> None:
        self.runtime = runtime or create_runtime()
        self.config = self.runtime.config
        self.service = self.runtime.service
        self.assistant = (
            GeminiSalesAssistant(self.config, self.runtime.catalog)
            if self.config.gemini_api_key
            else None
        )
        self.lead_sync = LeadSyncCoordinator.from_config(
            config=self.config,
            service=self.service,
        )

    def handle_inbound_customer_message(self, message: InboundMessage) -> CustomerTurnResult:
        snapshot = self.service.ingest_inbound_message(message)

        hints = infer_lead_hints(message.text, self.runtime.catalog, snapshot)
        if any(
            value is not None and value != []
            for value in (hints.stage, hints.tags, hints.interested_products, hints.city)
        ):
            snapshot = self.service.update_lead_profile(
                conversation_id=snapshot.conversation_id,
                stage=hints.stage,
                tags=hints.tags,
                interested_products=hints.interested_products,
                city=hints.city,
            )

        self.lead_sync.sync_snapshot(snapshot)
        admin_notification = self.build_admin_notification(snapshot, message.text)

        if snapshot.mode == ConversationMode.MANAGER:
            return CustomerTurnResult(
                snapshot=snapshot,
                hints=hints,
                admin_notification=admin_notification,
            )

        return CustomerTurnResult(
            snapshot=snapshot,
            hints=hints,
            admin_notification=admin_notification,
            reply_text=self.generate_ai_reply(snapshot, message.text),
        )

    def record_outbound_reply(
        self,
        snapshot: ConversationSnapshot,
        text: str,
    ) -> ConversationSnapshot:
        updated_snapshot = self.service.record_ai_reply(
            conversation_id=snapshot.conversation_id,
            text=text,
        )
        self.lead_sync.sync_snapshot(updated_snapshot)
        return updated_snapshot

    def generate_ai_reply(self, snapshot: ConversationSnapshot, user_text: str) -> str:
        if self.assistant is None:
            return (
                "Сообщение получил. Сейчас могу передать ваш запрос менеджеру или помочь с базовой консультацией позже."
            )

        try:
            transcript = self.service.get_transcript(
                conversation_id=snapshot.conversation_id,
                limit=16,
            )
            reply_text = self.assistant.generate_reply(
                snapshot=snapshot,
                transcript=transcript,
                user_message=user_text,
            )
        except Exception:
            return (
                "Я зафиксировал ваш запрос. Если хотите, могу продолжить чуть позже или подключить менеджера для точного ответа."
            )

        return reply_text or (
            "Я вас понял. Уточните, пожалуйста, какая у вас основная задача: сон, энергия, фокус, иммунитет или что-то другое?"
        )

    def build_admin_notification(self, snapshot: ConversationSnapshot, user_text: str) -> str:
        lines = [
            f"[conv:{snapshot.conversation_id}] Incoming message",
            f"Channel: {snapshot.channel.value}",
            f"Client: {snapshot.display_name or 'No name'}",
            f"Username: @{snapshot.username}" if snapshot.username else "Username: -",
            f"Stage: {snapshot.stage.value}",
            f"Mode: {snapshot.mode.value}",
            f"Products: {', '.join(snapshot.interested_products) if snapshot.interested_products else '-'}",
            "",
            user_text,
        ]
        return "\n".join(lines)
