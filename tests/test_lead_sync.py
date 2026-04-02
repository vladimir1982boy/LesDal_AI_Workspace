from __future__ import annotations

import unittest
from datetime import datetime, timezone

from src.ai_sales_bot.domain import Channel, ConversationMode, ConversationSnapshot, LeadStage
from src.ai_sales_bot.lead_sync import LeadSyncCoordinator


class LeadSyncCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.snapshot = ConversationSnapshot(
            contact_id=1,
            lead_id=2,
            conversation_id=3,
            channel=Channel.TELEGRAM,
            external_user_id="user-1",
            external_chat_id="chat-1",
            stage=LeadStage.QUALIFIED,
            mode=ConversationMode.AI,
            summary="Попросил подобрать продукт для сна.",
            display_name="Ирина",
            username="irina",
            city="Москва",
            tags=["sleep"],
            interested_products=["LesDal Night"],
            created_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 1, 1, tzinfo=timezone.utc),
        )

    def test_sync_snapshot_builds_google_sheets_payload(self) -> None:
        google_sheets = _FakeGoogleSheets()
        coordinator = LeadSyncCoordinator(
            service=_FakeService(self.snapshot),
            google_sheets=google_sheets,
        )

        result = coordinator.sync_snapshot(self.snapshot)

        self.assertTrue(result)
        self.assertEqual(len(google_sheets.calls), 1)
        payload = google_sheets.calls[0]
        self.assertIs(payload["snapshot"], self.snapshot)
        self.assertEqual(payload["last_sender"], "Ирина")
        self.assertEqual(payload["last_message"], "Мне важно наладить сон")
        self.assertEqual(payload["manager_summary"], "summary for manager")

    def test_sync_snapshot_is_noop_without_destinations(self) -> None:
        coordinator = LeadSyncCoordinator(
            service=_FakeService(self.snapshot),
            google_sheets=None,
        )

        self.assertFalse(coordinator.sync_snapshot(self.snapshot))


class _FakeService:
    def __init__(self, snapshot: ConversationSnapshot) -> None:
        self.snapshot = snapshot

    def get_snapshot(self, conversation_id: int) -> ConversationSnapshot:
        return self.snapshot

    def get_transcript(self, *, conversation_id: int, limit: int = 30) -> list[dict]:
        return [
            {
                "sender_name": "Ирина",
                "sender_role": "customer",
                "text": "Мне важно наладить сон",
            }
        ]

    def build_manager_summary(self, *, conversation_id: int, limit: int = 12) -> str:
        return "summary for manager"


class _FakeGoogleSheets:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def sync_lead(
        self,
        *,
        snapshot: ConversationSnapshot,
        last_sender: str,
        last_message: str,
        manager_summary: str,
    ) -> bool:
        self.calls.append(
            {
                "snapshot": snapshot,
                "last_sender": last_sender,
                "last_message": last_message,
                "manager_summary": manager_summary,
            }
        )
        return True


if __name__ == "__main__":
    unittest.main()
