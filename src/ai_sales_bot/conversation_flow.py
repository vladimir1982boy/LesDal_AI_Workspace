from __future__ import annotations

from dataclasses import dataclass

from .ai_engine import GeminiSalesAssistant, LeadHints, infer_lead_hints
from .app import SalesBotRuntime, create_runtime
from .domain import ConversationMode, ConversationSnapshot, InboundMessage
from .lead_sync import LeadSyncCoordinator


MANAGER_REQUEST_MARKERS = (
    "менеджер",
    "оператор",
    "человек",
    "перезвон",
    "свяж",
    "позвон",
    "human",
    "manager",
    "operator",
    "call me",
)

MANAGER_HANDOFF_REPLY = (
    "Передаю ваш запрос менеджеру. Он подключится вручную и продолжит диалог."
)

AI_FALLBACK_REPLY = (
    "Сообщение получил. Сейчас могу передать ваш запрос менеджеру или помочь с базовой консультацией позже."
)

AI_CLARIFY_REPLY = (
    "Я вас понял. Уточните, пожалуйста, какая у вас основная задача: сон, энергия, фокус, иммунитет или что-то другое?"
)


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

        if self.customer_requests_manager(message.text):
            snapshot = self.service.escalate_to_manager(
                conversation_id=snapshot.conversation_id,
                actor="LesDal AI",
                reason="customer_requested_manager",
                customer_message=message.text,
            )
            self.lead_sync.sync_snapshot(snapshot)
            return CustomerTurnResult(
                snapshot=snapshot,
                hints=hints,
                admin_notification=admin_notification,
                reply_text=MANAGER_HANDOFF_REPLY,
            )

        try:
            reply_text = self.generate_ai_reply(snapshot, message.text)
        except Exception:
            snapshot = self.service.escalate_to_manager(
                conversation_id=snapshot.conversation_id,
                actor="LesDal AI",
                reason="ai_needs_manager",
                customer_message=message.text,
            )
            self.lead_sync.sync_snapshot(snapshot)
            return CustomerTurnResult(
                snapshot=snapshot,
                hints=hints,
                admin_notification=admin_notification,
                reply_text=MANAGER_HANDOFF_REPLY,
            )

        if self.reply_needs_manager_handoff(reply_text):
            snapshot = self.service.escalate_to_manager(
                conversation_id=snapshot.conversation_id,
                actor="LesDal AI",
                reason="ai_requested_manager",
                customer_message=message.text,
            )
            self.lead_sync.sync_snapshot(snapshot)
            return CustomerTurnResult(
                snapshot=snapshot,
                hints=hints,
                admin_notification=admin_notification,
                reply_text=reply_text,
            )

        return CustomerTurnResult(
            snapshot=snapshot,
            hints=hints,
            admin_notification=admin_notification,
            reply_text=reply_text,
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
            return AI_FALLBACK_REPLY

        transcript = self.service.get_transcript(
            conversation_id=snapshot.conversation_id,
            limit=16,
        )
        reply_text = self.assistant.generate_reply(
            snapshot=snapshot,
            transcript=transcript,
            user_message=user_text,
        )
        return reply_text or AI_CLARIFY_REPLY

    def customer_requests_manager(self, text: str) -> bool:
        lowered = str(text or "").casefold()
        return any(marker in lowered for marker in MANAGER_REQUEST_MARKERS)

    def reply_needs_manager_handoff(self, text: str) -> bool:
        lowered = str(text or "").casefold()
        return (
            "подключить менеджера" in lowered
            or "передаю ваш запрос менеджеру" in lowered
            or "подключится вручную" in lowered
            or ("manager" in lowered and "connect" in lowered)
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
