from __future__ import annotations

import unittest
from types import SimpleNamespace

from src.ai_sales_bot.domain import Channel, ConversationMode, ConversationSnapshot, LeadStage
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
