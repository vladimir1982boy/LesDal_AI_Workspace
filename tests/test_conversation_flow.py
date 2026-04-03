from __future__ import annotations

import unittest
from types import SimpleNamespace

from src.ai_sales_bot.conversation_flow import SalesConversationManager
from src.ai_sales_bot.domain import Channel, ConversationMode, ConversationSnapshot, ConversationStatus, InboundMessage, LeadStage


class SalesConversationManagerTests(unittest.TestCase):
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
        self.runtime = SimpleNamespace(
            config=_FakeConfig(),
            service=self.service,
            catalog=_FakeCatalog(),
        )

    def test_handle_inbound_customer_message_returns_fallback_reply(self) -> None:
        manager = SalesConversationManager(self.runtime)

        result = manager.handle_inbound_customer_message(
            InboundMessage(
                channel=Channel.VK,
                external_user_id="42",
                external_chat_id="42",
                text="Need help with product selection",
                username="tester",
                display_name="Test User",
            )
        )

        self.assertIs(result.snapshot, self.snapshot)
        self.assertIsNotNone(result.reply_text)
        self.assertIn("[conv:3]", result.admin_notification)
        self.assertIn("Need help with product selection", result.admin_notification)

    def test_record_outbound_reply_updates_snapshot(self) -> None:
        manager = SalesConversationManager(self.runtime)

        updated_snapshot = manager.record_outbound_reply(self.snapshot, "Reply text")

        self.assertIs(updated_snapshot, self.snapshot)
        self.assertEqual(self.service.recorded_reply, "Reply text")

    def test_handle_inbound_customer_message_skips_ai_reply_for_waiting_manager_dialog(self) -> None:
        self.service.force_manager_takeover = True
        manager = SalesConversationManager(self.runtime)

        result = manager.handle_inbound_customer_message(
            InboundMessage(
                channel=Channel.VK,
                external_user_id="42",
                external_chat_id="42",
                text="Нужен менеджер",
                username="tester",
                display_name="Test User",
            )
        )

        self.assertEqual(result.snapshot.mode, ConversationMode.MANAGER)
        self.assertEqual(result.snapshot.status, ConversationStatus.WAITING_MANAGER)
        self.assertIsNone(result.reply_text)
        self.assertIn("[conv:3]", result.admin_notification)


class _FakeConfig:
    gemini_api_key = ""
    has_google_sheets = False


class _FakeCatalog:
    products: list[object] = []

    def search(self, text: str, limit: int = 5) -> list[object]:
        return []


class _FakeService:
    def __init__(self, snapshot: ConversationSnapshot) -> None:
        self.snapshot = snapshot
        self.recorded_reply = ""
        self.force_manager_takeover = False

    def ingest_inbound_message(self, message: InboundMessage) -> ConversationSnapshot:
        if self.force_manager_takeover:
            self.snapshot.mode = ConversationMode.MANAGER
            self.snapshot.status = ConversationStatus.WAITING_MANAGER
        return self.snapshot

    def update_lead_profile(self, **kwargs) -> ConversationSnapshot:
        return self.snapshot

    def get_transcript(self, *, conversation_id: int, limit: int = 30) -> list[dict]:
        return []

    def build_manager_summary(self, *, conversation_id: int, limit: int = 12) -> str:
        return ""

    def record_ai_reply(self, *, conversation_id: int, text: str) -> ConversationSnapshot:
        self.recorded_reply = text
        return self.snapshot


if __name__ == "__main__":
    unittest.main()
