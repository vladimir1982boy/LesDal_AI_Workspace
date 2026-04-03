from __future__ import annotations

import unittest
from types import SimpleNamespace

from src.ai_sales_bot.domain import Channel, ConversationMode, ConversationSnapshot, ConversationStatus, LeadStage
from src.ai_sales_bot.operator_api import OperatorInboxAPI


class OperatorInboxAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.snapshot = ConversationSnapshot(
            contact_id=1,
            lead_id=2,
            conversation_id=3,
            channel=Channel.VK,
            external_user_id="42",
            external_chat_id="42",
            stage=LeadStage.NEW,
            mode=ConversationMode.AI,
            summary="",
            display_name="Test User",
            username="tester",
        )
        self.service = _FakeService(self.snapshot)
        self.dispatcher = _FakeDispatcher()
        self.runtime = SimpleNamespace(
            config=_FakeConfig(),
            service=self.service,
        )
        self.api = OperatorInboxAPI(runtime=self.runtime, dispatcher=self.dispatcher)
        self.api.lead_sync = _FakeLeadSync()

    def test_reply_routes_message_through_channel_dispatcher(self) -> None:
        result = self.api.reply_to_conversation(
            3,
            text="Manager reply",
            pause_ai=True,
        )

        self.assertTrue(result.outbound_sent)
        self.assertEqual(self.dispatcher.sent[0]["channel"], Channel.VK)
        self.assertEqual(self.dispatcher.sent[0]["external_chat_id"], "42")
        self.assertEqual(self.service.recorded_manager_reply["text"], "Manager reply")

    def test_pause_conversation_switches_mode(self) -> None:
        result = self.api.pause_conversation(3)

        self.assertEqual(result.snapshot.mode, ConversationMode.MANAGER)
        self.assertEqual(self.service.last_mode, ConversationMode.MANAGER)

    def test_claim_conversation_assigns_owner(self) -> None:
        result = self.api.claim_conversation(3, operator_name="Alice")

        self.assertEqual(result.snapshot.owner_name, "Alice")
        self.assertEqual(result.snapshot.status, ConversationStatus.IN_PROGRESS)
        self.assertEqual(self.service.claimed_by, "Alice")

    def test_release_conversation_clears_owner(self) -> None:
        self.snapshot.mode = ConversationMode.MANAGER
        self.snapshot.status = ConversationStatus.IN_PROGRESS
        self.snapshot.owner_name = "Alice"

        result = self.api.release_conversation(3, operator_name="Alice")

        self.assertEqual(result.snapshot.owner_name, "")
        self.assertEqual(result.snapshot.status, ConversationStatus.NEW)
        self.assertEqual(self.service.released_by, "Alice")

    def test_set_status_updates_snapshot(self) -> None:
        result = self.api.set_status(3, status=ConversationStatus.CLOSED.value, operator_name="Alice")

        self.assertEqual(result.snapshot.status, ConversationStatus.CLOSED)
        self.assertEqual(self.service.last_status, ConversationStatus.CLOSED)

    def test_update_manager_notes_updates_snapshot(self) -> None:
        result = self.api.update_manager_notes(3, notes="Client prefers evening call", operator_name="Alice")

        self.assertEqual(result.snapshot.manager_notes, "Client prefers evening call")
        self.assertEqual(self.service.last_notes, "Client prefers evening call")

    def test_get_conversation_includes_reply_templates(self) -> None:
        payload = self.api.get_conversation(3)

        self.assertTrue(payload["reply_templates"])
        self.assertIn("title", payload["reply_templates"][0])

    def test_resume_conversation_notifies_customer(self) -> None:
        result = self.api.resume_conversation(3, notify_customer=True)

        self.assertTrue(result.outbound_sent)
        self.assertEqual(self.service.resume_called_with, 3)
        self.assertIn("Снова с вами", self.dispatcher.sent[-1]["text"])


class _FakeConfig:
    manager_name = "Владимир"
    has_google_sheets = False


class _FakeDispatcher:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    def send_text(self, **kwargs) -> bool:
        self.sent.append(kwargs)
        return True


class _FakeLeadSync:
    def __init__(self) -> None:
        self.snapshots = []

    def sync_snapshot(self, snapshot: ConversationSnapshot) -> bool:
        self.snapshots.append(snapshot)
        return True


class _FakeService:
    def __init__(self, snapshot: ConversationSnapshot) -> None:
        self.snapshot = snapshot
        self.recorded_manager_reply: dict = {}
        self.resume_called_with = 0
        self.last_mode = snapshot.mode
        self.claimed_by = ""
        self.released_by = ""
        self.last_status = snapshot.status
        self.last_notes = snapshot.manager_notes

    def list_recent_conversations(self, *, limit: int = 20) -> list[dict]:
        return [
            {
                "id": self.snapshot.conversation_id,
                "channel": self.snapshot.channel.value,
                "external_chat_id": self.snapshot.external_chat_id,
                "mode": self.snapshot.mode.value,
                "stage": self.snapshot.stage.value,
                "summary": self.snapshot.summary,
                "display_name": self.snapshot.display_name,
                "username": self.snapshot.username,
                "last_message_at": self.snapshot.updated_at.isoformat(),
            }
        ]

    def get_snapshot(self, conversation_id: int) -> ConversationSnapshot:
        return self.snapshot

    def get_transcript(self, *, conversation_id: int, limit: int = 30) -> list[dict]:
        return []

    def get_conversation_events(self, *, conversation_id: int, limit: int = 50) -> list[dict]:
        return []

    def build_manager_summary(self, *, conversation_id: int, limit: int = 12) -> str:
        return "Summary"

    def get_conversation_target(self, conversation_id: int) -> dict:
        return {
            "id": conversation_id,
            "channel": self.snapshot.channel.value,
            "external_chat_id": self.snapshot.external_chat_id,
            "external_user_id": self.snapshot.external_user_id,
            "mode": self.snapshot.mode.value,
            "display_name": self.snapshot.display_name,
            "username": self.snapshot.username,
        }

    def set_conversation_mode(self, *, conversation_id: int, mode: ConversationMode) -> ConversationSnapshot:
        self.snapshot.mode = mode
        self.last_mode = mode
        return self.snapshot

    def claim_conversation(self, *, conversation_id: int, operator_name: str) -> ConversationSnapshot:
        self.snapshot.mode = ConversationMode.MANAGER
        self.snapshot.status = ConversationStatus.IN_PROGRESS
        self.snapshot.owner_name = operator_name
        self.claimed_by = operator_name
        self.last_mode = ConversationMode.MANAGER
        self.last_status = ConversationStatus.IN_PROGRESS
        return self.snapshot

    def release_conversation(self, *, conversation_id: int, operator_name: str) -> ConversationSnapshot:
        self.snapshot.owner_name = ""
        self.snapshot.status = ConversationStatus.NEW
        self.released_by = operator_name
        self.last_status = ConversationStatus.NEW
        return self.snapshot

    def set_conversation_status(
        self,
        *,
        conversation_id: int,
        status: ConversationStatus,
        actor: str = "",
    ) -> ConversationSnapshot:
        self.snapshot.status = status
        self.last_status = status
        return self.snapshot

    def update_manager_notes(
        self,
        *,
        conversation_id: int,
        notes: str,
        actor: str = "",
    ) -> ConversationSnapshot:
        self.snapshot.manager_notes = notes
        self.last_notes = notes
        return self.snapshot

    def record_manager_reply(
        self,
        *,
        conversation_id: int,
        manager_name: str,
        text: str,
        pause_ai: bool = True,
    ) -> ConversationSnapshot:
        if pause_ai:
            self.snapshot.mode = ConversationMode.MANAGER
        self.recorded_manager_reply = {
            "conversation_id": conversation_id,
            "manager_name": manager_name,
            "text": text,
            "pause_ai": pause_ai,
        }
        return self.snapshot

    def resume_ai(self, *, conversation_id: int) -> ConversationSnapshot:
        self.resume_called_with = conversation_id
        self.snapshot.mode = ConversationMode.AI
        return self.snapshot


if __name__ == "__main__":
    unittest.main()
