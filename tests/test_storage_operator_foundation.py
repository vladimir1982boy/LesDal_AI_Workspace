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
        self.assertEqual(snapshot.owner_id, "")
        self.assertEqual(snapshot.owner_name, "")
        self.assertIsNone(snapshot.owner_claimed_at)
        self.assertIsNotNone(snapshot.last_customer_message_at)
        self.assertIsNone(snapshot.last_manager_message_at)
        self.assertFalse(snapshot.needs_attention)
        self.assertEqual(snapshot.manager_notes, "")
        self.assertEqual(snapshot.priority.value, "normal")
        self.assertEqual(snapshot.follow_up_date, "")
        self.assertEqual(snapshot.next_action, "")

    def test_json_snapshot_contains_operator_fields(self) -> None:
        repo = JSONLeadRepository(self.json_path)
        snapshot = self._ingest(repo)

        self.assertEqual(snapshot.status.value, "new")
        self.assertEqual(snapshot.owner_id, "")
        self.assertEqual(snapshot.owner_name, "")
        self.assertIsNone(snapshot.owner_claimed_at)
        self.assertIsNotNone(snapshot.last_customer_message_at)
        self.assertIsNone(snapshot.last_manager_message_at)
        self.assertFalse(snapshot.needs_attention)
        self.assertEqual(snapshot.manager_notes, "")
        self.assertEqual(snapshot.priority.value, "normal")
        self.assertEqual(snapshot.follow_up_date, "")
        self.assertEqual(snapshot.next_action, "")

    def test_sqlite_can_persist_manager_notes(self) -> None:
        repo = SQLiteLeadRepository(self.sqlite_path)
        snapshot = self._ingest(repo)

        repo.update_lead(lead_id=snapshot.lead_id, manager_notes="Needs callback after 18:00")
        updated = repo.get_snapshot(snapshot.conversation_id)

        self.assertEqual(updated.manager_notes, "Needs callback after 18:00")
        self.assertEqual(updated.priority.value, "normal")

    def test_json_can_persist_manager_notes(self) -> None:
        repo = JSONLeadRepository(self.json_path)
        snapshot = self._ingest(repo)

        repo.update_lead(lead_id=snapshot.lead_id, manager_notes="Needs callback after 18:00")
        updated = repo.get_snapshot(snapshot.conversation_id)

        self.assertEqual(updated.manager_notes, "Needs callback after 18:00")

    def test_sqlite_can_persist_kpi_fields(self) -> None:
        repo = SQLiteLeadRepository(self.sqlite_path)
        snapshot = self._ingest(repo)

        from src.ai_sales_bot.domain import LeadPriority

        repo.update_lead(
            lead_id=snapshot.lead_id,
            priority=LeadPriority.URGENT,
            follow_up_date="2026-04-04",
            next_action="Call client after pricing approval",
        )
        updated = repo.get_snapshot(snapshot.conversation_id)

        self.assertEqual(updated.priority.value, "urgent")
        self.assertEqual(updated.follow_up_date, "2026-04-04")
        self.assertEqual(updated.next_action, "Call client after pricing approval")

    def test_json_can_persist_kpi_fields(self) -> None:
        repo = JSONLeadRepository(self.json_path)
        snapshot = self._ingest(repo)

        from src.ai_sales_bot.domain import LeadPriority

        repo.update_lead(
            lead_id=snapshot.lead_id,
            priority=LeadPriority.HIGH,
            follow_up_date="2026-04-05",
            next_action="Send follow-up with offer comparison",
        )
        updated = repo.get_snapshot(snapshot.conversation_id)

        self.assertEqual(updated.priority.value, "high")
        self.assertEqual(updated.follow_up_date, "2026-04-05")
        self.assertEqual(updated.next_action, "Send follow-up with offer comparison")

    def test_sqlite_get_conversation_target_includes_external_user_id(self) -> None:
        repo = SQLiteLeadRepository(self.sqlite_path)
        snapshot = self._ingest(repo)

        target = repo.get_conversation_target(snapshot.conversation_id)

        self.assertEqual(target["external_user_id"], "user-1")
        self.assertEqual(target["owner_id"], "")

    def test_json_get_conversation_target_includes_external_user_id(self) -> None:
        repo = JSONLeadRepository(self.json_path)
        snapshot = self._ingest(repo)

        target = repo.get_conversation_target(snapshot.conversation_id)

        self.assertEqual(target["external_user_id"], "user-1")
        self.assertEqual(target["owner_id"], "")

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

    def test_sqlite_recent_conversations_include_forced_takeover_audit(self) -> None:
        repo = SQLiteLeadRepository(self.sqlite_path)
        snapshot = self._ingest(repo)
        repo.add_conversation_event(
            conversation_id=snapshot.conversation_id,
            event_type="force_claimed_by_supervisor",
            actor="Lead",
            payload={"previous_owner_id": "alice"},
        )

        row = dict(repo.list_recent_conversations(limit=1)[0])
        refreshed = repo.get_snapshot(snapshot.conversation_id)

        self.assertTrue(row["has_forced_takeover"])
        self.assertEqual(row["last_forced_takeover_by"], "Lead")
        self.assertTrue(refreshed.has_forced_takeover)
        self.assertEqual(refreshed.last_forced_takeover_by, "Lead")

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

    def test_json_recent_conversations_include_forced_takeover_audit(self) -> None:
        repo = JSONLeadRepository(self.json_path)
        snapshot = self._ingest(repo)
        repo.add_conversation_event(
            conversation_id=snapshot.conversation_id,
            event_type="force_claimed_by_supervisor",
            actor="Lead",
            payload={"previous_owner_id": "alice"},
        )

        row = repo.list_recent_conversations(limit=1)[0]
        refreshed = repo.get_snapshot(snapshot.conversation_id)

        self.assertTrue(row["has_forced_takeover"])
        self.assertEqual(row["last_forced_takeover_by"], "Lead")
        self.assertTrue(refreshed.has_forced_takeover)
        self.assertEqual(refreshed.last_forced_takeover_by, "Lead")

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
