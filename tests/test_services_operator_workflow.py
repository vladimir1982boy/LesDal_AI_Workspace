from __future__ import annotations

import unittest

from src.ai_sales_bot.domain import Channel, ConversationMode, ConversationSnapshot, ConversationStatus, InboundMessage, LeadStage, SenderRole
from src.ai_sales_bot.services import ConversationOwnershipError, SalesBotService


class SalesBotServiceOperatorWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.snapshot = ConversationSnapshot(
            contact_id=1,
            lead_id=2,
            conversation_id=3,
            channel=Channel.VK,
            external_user_id="42",
            external_chat_id="42",
            stage=LeadStage.NEW,
            mode=ConversationMode.MANAGER,
            status=ConversationStatus.IN_PROGRESS,
            summary="",
            display_name="Test User",
            username="tester",
            owner_name="Alice",
        )
        self.repo = _FakeRepository(self.snapshot)
        self.service = SalesBotService(self.repo)

    def test_ingest_marks_waiting_manager_for_manager_owned_conversation(self) -> None:
        snapshot = self.service.ingest_inbound_message(
            InboundMessage(
                channel=Channel.VK,
                external_user_id="42",
                external_chat_id="42",
                text="Need a human reply",
            )
        )

        self.assertEqual(snapshot.status, ConversationStatus.WAITING_MANAGER)
        self.assertTrue(snapshot.needs_attention)
        self.assertEqual(self.repo.events[-1]["event_type"], "customer_waiting_manager")

    def test_claim_conversation_assigns_owner(self) -> None:
        self.snapshot.owner_name = ""
        self.snapshot.mode = ConversationMode.AI
        self.snapshot.status = ConversationStatus.NEW

        snapshot = self.service.claim_conversation(
            conversation_id=3,
            operator_name="Bob",
        )

        self.assertEqual(snapshot.mode, ConversationMode.MANAGER)
        self.assertEqual(snapshot.status, ConversationStatus.IN_PROGRESS)
        self.assertEqual(snapshot.owner_name, "Bob")
        self.assertEqual(self.repo.events[-1]["event_type"], "claimed_by_manager")

    def test_record_manager_reply_rejects_foreign_owner(self) -> None:
        with self.assertRaises(ConversationOwnershipError):
            self.service.record_manager_reply(
                conversation_id=3,
                manager_name="Bob",
                text="Reply from wrong owner",
                pause_ai=True,
            )


class _FakeRepository:
    def __init__(self, snapshot: ConversationSnapshot) -> None:
        self.snapshot = snapshot
        self.events: list[dict] = []

    def ingest_customer_message(self, message: InboundMessage) -> ConversationSnapshot:
        return self.snapshot

    def add_message(
        self,
        *,
        conversation_id: int,
        sender_role: SenderRole,
        text: str,
        sender_name: str = "",
        raw_payload: dict | None = None,
    ) -> int:
        return 1

    def add_conversation_event(
        self,
        *,
        conversation_id: int,
        event_type: str,
        actor: str = "",
        payload: dict | None = None,
    ) -> int:
        self.events.append(
            {
                "conversation_id": conversation_id,
                "event_type": event_type,
                "actor": actor,
                "payload": payload or {},
            }
        )
        return len(self.events)

    def get_snapshot(self, conversation_id: int) -> ConversationSnapshot:
        return self.snapshot

    def get_conversation_target(self, conversation_id: int) -> dict:
        return {}

    def list_recent_conversations(self, *, limit: int = 20) -> list[dict]:
        return []

    def list_conversation_events(self, conversation_id: int, *, limit: int = 50) -> list[dict]:
        return self.events[:limit]

    def set_conversation_mode(self, *, conversation_id: int, mode: ConversationMode) -> None:
        self.snapshot.mode = mode

    def update_conversation_state(
        self,
        *,
        conversation_id: int,
        mode: ConversationMode | None = None,
        status: ConversationStatus | None = None,
        owner_name: str | None = None,
        owner_claimed_at=None,
        clear_owner: bool = False,
        needs_attention: bool | None = None,
    ) -> None:
        if mode is not None:
            self.snapshot.mode = mode
        if status is not None:
            self.snapshot.status = status
        if clear_owner:
            self.snapshot.owner_name = ""
            self.snapshot.owner_claimed_at = None
        else:
            if owner_name is not None:
                self.snapshot.owner_name = owner_name
            if owner_claimed_at is not None:
                self.snapshot.owner_claimed_at = owner_claimed_at
        if needs_attention is not None:
            self.snapshot.needs_attention = needs_attention

    def update_lead(self, **kwargs) -> None:
        return None

    def build_transcript(self, conversation_id: int, *, limit: int = 30) -> list[dict]:
        return []


if __name__ == "__main__":
    unittest.main()
