from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from .app import SalesBotRuntime, create_runtime
from .domain import Channel, ConversationMode, ConversationSnapshot, ConversationStatus, LeadPriority, LeadStage
from .lead_sync import LeadSyncCoordinator
from .outbound import OutboundDispatcher
from .services import ConversationOwnershipError


DEFAULT_REPLY_TEMPLATES: tuple[dict[str, str], ...] = (
    {
        "id": "clarify_need",
        "title": "Уточнить запрос",
        "text": "Спасибо, что написали. Подскажите, пожалуйста, какой именно результат вы хотите получить и в каком формате вам удобнее продолжить общение?",
    },
    {
        "id": "send_catalog",
        "title": "Предложить варианты",
        "text": "Подобрал для вас несколько подходящих вариантов. Если хотите, я коротко сравню их по цене, составу и срокам поставки.",
    },
    {
        "id": "booking_followup",
        "title": "Договориться о контакте",
        "text": "Могу помочь вам дальше вручную. Напишите, пожалуйста, удобный номер телефона или время для связи, и я всё подготовлю.",
    },
    {
        "id": "payment_followup",
        "title": "Довести до оплаты",
        "text": "Если решение уже выбрано, я могу сейчас помочь с финальными деталями по оплате и отправке. Подтвердите, пожалуйста, какой вариант оставляем.",
    },
)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return getattr(value, "value")
    return value


def serialize_snapshot(snapshot: ConversationSnapshot) -> dict[str, Any]:
    return {
        key: _serialize_value(value)
        for key, value in asdict(snapshot).items()
    }


@dataclass(slots=True)
class OperatorActionResult:
    snapshot: ConversationSnapshot
    outbound_sent: bool = False


class OperatorInboxAPI:
    def __init__(
        self,
        runtime: SalesBotRuntime | None = None,
        dispatcher: OutboundDispatcher | None = None,
    ) -> None:
        self.runtime = runtime or create_runtime()
        self.config = self.runtime.config
        self.service = self.runtime.service
        self.dispatcher = dispatcher or OutboundDispatcher(self.config)
        self.lead_sync = LeadSyncCoordinator.from_config(
            config=self.config,
            service=self.service,
        )

    def list_conversations(
        self,
        *,
        limit: int = 50,
        channel: str = "",
        mode: str = "",
        status: str = "",
        owner: str = "",
        q: str = "",
        needs_attention: bool | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.service.list_recent_conversations(
            limit=limit,
            channel=channel,
            mode=mode,
            status=status,
            owner=owner,
            q=q,
            needs_attention=needs_attention,
        )
        return [
            {
                key: _serialize_value(value)
                for key, value in row.items()
            }
            for row in rows
        ]

    def get_conversation(self, conversation_id: int) -> dict[str, Any]:
        snapshot = self.service.get_snapshot(conversation_id)
        transcript = self.service.get_transcript(conversation_id=conversation_id, limit=100)
        events = self.service.get_conversation_events(conversation_id=conversation_id, limit=50)
        summary = self.service.build_manager_summary(conversation_id=conversation_id)
        target = self.service.get_conversation_target(conversation_id)
        return {
            "snapshot": serialize_snapshot(snapshot),
            "target": {
                key: _serialize_value(value)
                for key, value in target.items()
            },
            "transcript": [
                {
                    key: _serialize_value(value)
                    for key, value in row.items()
                }
                for row in transcript
            ],
            "events": [
                {
                    key: _serialize_value(value)
                    for key, value in row.items()
                }
                for row in events
            ],
            "summary": summary,
            "reply_templates": [dict(item) for item in DEFAULT_REPLY_TEMPLATES],
        }

    def pause_conversation(self, conversation_id: int) -> OperatorActionResult:
        claim_method = getattr(self.service, "claim_conversation", None)
        if callable(claim_method):
            snapshot = claim_method(
                conversation_id=conversation_id,
                operator_name=self.config.manager_name,
            )
        else:
            snapshot = self.service.set_conversation_mode(
                conversation_id=conversation_id,
                mode=ConversationMode.MANAGER,
            )
        self.lead_sync.sync_snapshot(snapshot)
        return OperatorActionResult(snapshot=snapshot)

    def claim_conversation(
        self,
        conversation_id: int,
        *,
        operator_name: str,
        operator_id: str = "",
    ) -> OperatorActionResult:
        snapshot = self.service.claim_conversation(
            conversation_id=conversation_id,
            operator_name=operator_name,
            operator_id=operator_id,
        )
        self.lead_sync.sync_snapshot(snapshot)
        return OperatorActionResult(snapshot=snapshot)

    def resume_conversation(self, conversation_id: int, *, notify_customer: bool = True) -> OperatorActionResult:
        snapshot = self.service.resume_ai(conversation_id=conversation_id)
        outbound_sent = False
        if notify_customer:
            target = self.service.get_conversation_target(conversation_id)
            outbound_sent = self.dispatcher.send_text(
                channel=Channel(target["channel"]),
                external_chat_id=str(target["external_chat_id"]),
                external_user_id=str(target["external_user_id"]),
                text=(
                    f"Снова с вами {self.config.manager_name}. "
                    "Я ознакомился с перепиской и могу продолжить консультацию."
                ),
            )
        self.lead_sync.sync_snapshot(snapshot)
        return OperatorActionResult(snapshot=snapshot, outbound_sent=outbound_sent)

    def release_conversation(
        self,
        conversation_id: int,
        *,
        operator_name: str,
        operator_id: str = "",
    ) -> OperatorActionResult:
        snapshot = self.service.release_conversation(
            conversation_id=conversation_id,
            operator_name=operator_name,
            operator_id=operator_id,
        )
        self.lead_sync.sync_snapshot(snapshot)
        return OperatorActionResult(snapshot=snapshot)

    def reply_to_conversation(
        self,
        conversation_id: int,
        *,
        text: str,
        pause_ai: bool = True,
        operator_name: str | None = None,
        operator_id: str = "",
    ) -> OperatorActionResult:
        target = self.service.get_conversation_target(conversation_id)
        outbound_sent = self.dispatcher.send_text(
            channel=Channel(target["channel"]),
            external_chat_id=str(target["external_chat_id"]),
            external_user_id=str(target["external_user_id"]),
            text=text,
        )
        snapshot = self.service.record_manager_reply(
            conversation_id=conversation_id,
            manager_name=operator_name or self.config.manager_name,
            operator_id=operator_id,
            text=text,
            pause_ai=pause_ai,
        )
        self.lead_sync.sync_snapshot(snapshot)
        return OperatorActionResult(snapshot=snapshot, outbound_sent=outbound_sent)

    def set_status(
        self,
        conversation_id: int,
        *,
        status: str,
        operator_name: str = "",
        operator_id: str = "",
    ) -> OperatorActionResult:
        snapshot = self.service.set_conversation_status(
            conversation_id=conversation_id,
            status=ConversationStatus(status),
            actor=operator_name,
            actor_id=operator_id,
        )
        self.lead_sync.sync_snapshot(snapshot)
        return OperatorActionResult(snapshot=snapshot)

    def update_manager_notes(
        self,
        conversation_id: int,
        *,
        notes: str,
        operator_name: str = "",
        operator_id: str = "",
    ) -> OperatorActionResult:
        snapshot = self.service.update_manager_notes(
            conversation_id=conversation_id,
            notes=notes,
            actor=operator_name,
            actor_id=operator_id,
        )
        self.lead_sync.sync_snapshot(snapshot)
        return OperatorActionResult(snapshot=snapshot)

    def update_lead_profile(
        self,
        conversation_id: int,
        *,
        stage: str,
        summary: str,
        tags: list[str],
        priority: str,
        follow_up_date: str,
        next_action: str,
        operator_name: str = "",
        operator_id: str = "",
    ) -> OperatorActionResult:
        snapshot = self.service.update_lead_profile(
            conversation_id=conversation_id,
            stage=LeadStage(stage),
            summary=summary,
            tags=tags,
            priority=LeadPriority(priority),
            follow_up_date=follow_up_date,
            next_action=next_action,
            actor=operator_name,
            actor_id=operator_id,
        )
        self.lead_sync.sync_snapshot(snapshot)
        return OperatorActionResult(snapshot=snapshot)
