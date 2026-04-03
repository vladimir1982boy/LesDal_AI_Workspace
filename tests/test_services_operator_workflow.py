from __future__ import annotations

import unittest

from src.ai_sales_bot.domain import Channel, ConversationMode, ConversationSnapshot, ConversationStatus, InboundMessage, LeadPriority, LeadStage, SenderRole
from src.ai_sales_bot.services import ConversationOwnershipError, LeadProfileValidationError, SalesBotService


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

    def test_release_conversation_clears_owner(self) -> None:
        snapshot = self.service.release_conversation(
            conversation_id=3,
            operator_name="Alice",
        )

        self.assertEqual(snapshot.owner_name, "")
        self.assertEqual(snapshot.status, ConversationStatus.NEW)
        self.assertEqual(self.repo.events[-1]["event_type"], "released_by_manager")

    def test_release_rejects_foreign_owner(self) -> None:
        with self.assertRaises(ConversationOwnershipError):
            self.service.release_conversation(
                conversation_id=3,
                operator_name="Bob",
            )

    def test_update_manager_notes_persists_and_logs_event(self) -> None:
        snapshot = self.service.update_manager_notes(
            conversation_id=3,
            notes="Priority client, prefers WhatsApp-style pacing.",
            actor="Alice",
        )

        self.assertEqual(snapshot.manager_notes, "Priority client, prefers WhatsApp-style pacing.")
        self.assertEqual(self.repo.events[-1]["event_type"], "manager_notes_updated")

    def test_update_lead_profile_persists_and_logs_event(self) -> None:
        snapshot = self.service.update_lead_profile(
            conversation_id=3,
            stage=LeadStage.QUALIFIED,
            summary="Interested in medium price tier",
            tags=["warm", "callback"],
            priority=LeadPriority.HIGH,
            follow_up_date="2026-04-04",
            next_action="Call after proposal review",
            actor="Alice",
        )

        self.assertEqual(snapshot.stage, LeadStage.QUALIFIED)
        self.assertEqual(snapshot.summary, "Interested in medium price tier")
        self.assertEqual(snapshot.tags, ["warm", "callback"])
        self.assertEqual(snapshot.priority, LeadPriority.HIGH)
        self.assertEqual(snapshot.follow_up_date, "2026-04-04")
        self.assertEqual(snapshot.next_action, "Call after proposal review")
        self.assertEqual(self.repo.events[-1]["event_type"], "lead_profile_updated")

    def test_update_lead_profile_requires_next_action_for_high_priority(self) -> None:
        with self.assertRaises(LeadProfileValidationError):
            self.service.update_lead_profile(
                conversation_id=3,
                priority=LeadPriority.HIGH,
                next_action="",
                actor="Alice",
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
        if "stage" in kwargs and kwargs["stage"] is not None:
            self.snapshot.stage = kwargs["stage"]
        if "summary" in kwargs and kwargs["summary"] is not None:
            self.snapshot.summary = kwargs["summary"]
        if "tags" in kwargs and kwargs["tags"] is not None:
            self.snapshot.tags = kwargs["tags"]
        if "manager_notes" in kwargs and kwargs["manager_notes"] is not None:
            self.snapshot.manager_notes = kwargs["manager_notes"]
        if "priority" in kwargs and kwargs["priority"] is not None:
            self.snapshot.priority = kwargs["priority"]
        if "follow_up_date" in kwargs and kwargs["follow_up_date"] is not None:
            self.snapshot.follow_up_date = kwargs["follow_up_date"]
        if "next_action" in kwargs and kwargs["next_action"] is not None:
            self.snapshot.next_action = kwargs["next_action"]
        return None

    def build_transcript(self, conversation_id: int, *, limit: int = 30) -> list[dict]:
        return []


if __name__ == "__main__":
    unittest.main()
