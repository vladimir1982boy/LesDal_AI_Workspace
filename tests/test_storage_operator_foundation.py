from __future__ import annotations

import unittest
from pathlib import Path
from uuid import uuid4

from src.ai_sales_bot.domain import Channel, InboundMessage
from src.ai_sales_bot.storage import JSONLeadRepository, SQLiteLeadRepository


class StorageOperatorFoundationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime_dir = Path("tests/_runtime")
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        suffix = uuid4().hex
        self.sqlite_path = self.runtime_dir / f"operator_foundation_{suffix}.sqlite3"
        self.json_path = self.runtime_dir / f"operator_foundation_{suffix}.json"

    def test_sqlite_snapshot_contains_operator_fields(self) -> None:
        repo = SQLiteLeadRepository(self.sqlite_path)
        snapshot = self._ingest(repo)

        self.assertEqual(snapshot.status.value, "new")
        self.assertEqual(snapshot.owner_name, "")
        self.assertIsNone(snapshot.owner_claimed_at)
        self.assertIsNotNone(snapshot.last_customer_message_at)
        self.assertIsNone(snapshot.last_manager_message_at)
        self.assertFalse(snapshot.needs_attention)

    def test_json_snapshot_contains_operator_fields(self) -> None:
        repo = JSONLeadRepository(self.json_path)
        snapshot = self._ingest(repo)

        self.assertEqual(snapshot.status.value, "new")
        self.assertEqual(snapshot.owner_name, "")
        self.assertIsNone(snapshot.owner_claimed_at)
        self.assertIsNotNone(snapshot.last_customer_message_at)
        self.assertIsNone(snapshot.last_manager_message_at)
        self.assertFalse(snapshot.needs_attention)

    def test_sqlite_get_conversation_target_includes_external_user_id(self) -> None:
        repo = SQLiteLeadRepository(self.sqlite_path)
        snapshot = self._ingest(repo)

        target = repo.get_conversation_target(snapshot.conversation_id)

        self.assertEqual(target["external_user_id"], "user-1")

    def test_json_get_conversation_target_includes_external_user_id(self) -> None:
        repo = JSONLeadRepository(self.json_path)
        snapshot = self._ingest(repo)

        target = repo.get_conversation_target(snapshot.conversation_id)

        self.assertEqual(target["external_user_id"], "user-1")

    def test_sqlite_can_store_and_list_conversation_events(self) -> None:
        repo = SQLiteLeadRepository(self.sqlite_path)
        snapshot = self._ingest(repo)
        repo.add_conversation_event(
            conversation_id=snapshot.conversation_id,
            event_type="claimed",
            actor="manager",
            payload={"operator": "Vladimir"},
        )

        rows = repo.list_conversation_events(snapshot.conversation_id, limit=10)

        self.assertEqual(len(rows), 1)
        event = dict(rows[0])
        self.assertEqual(event["event_type"], "claimed")
        self.assertEqual(event["actor"], "manager")

    def test_json_can_store_and_list_conversation_events(self) -> None:
        repo = JSONLeadRepository(self.json_path)
        snapshot = self._ingest(repo)
        repo.add_conversation_event(
            conversation_id=snapshot.conversation_id,
            event_type="claimed",
            actor="manager",
            payload={"operator": "Vladimir"},
        )

        rows = repo.list_conversation_events(snapshot.conversation_id, limit=10)

        self.assertEqual(len(rows), 1)
        event = rows[0]
        self.assertEqual(event["event_type"], "claimed")
        self.assertEqual(event["actor"], "manager")

    def _ingest(self, repo: SQLiteLeadRepository | JSONLeadRepository):
        return repo.ingest_customer_message(
            InboundMessage(
                channel=Channel.VK,
                external_user_id="user-1",
                external_chat_id="chat-1",
                text="Need help with sleep",
                username="tester",
                display_name="Test User",
            )
        )


if __name__ == "__main__":
    unittest.main()
