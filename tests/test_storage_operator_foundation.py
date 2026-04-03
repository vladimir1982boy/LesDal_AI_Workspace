from __future__ import annotations

import json
import sqlite3
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

    def test_sqlite_can_filter_conversation_events_by_type(self) -> None:
        repo = SQLiteLeadRepository(self.sqlite_path)
        snapshot = self._ingest(repo)
        repo.add_conversation_event(
            conversation_id=snapshot.conversation_id,
            event_type="customer_waiting_manager",
            actor="system",
            payload={"status": "waiting_manager"},
        )
        repo.add_conversation_event(
            conversation_id=snapshot.conversation_id,
            event_type="manager_reply",
            actor="manager",
            payload={"text": "Handled"},
        )

        rows = repo.list_conversation_events_by_type(
            event_types=("customer_waiting_manager", "manager_reply"),
            limit=10,
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(dict(rows[0])["event_type"], "customer_waiting_manager")
        self.assertEqual(dict(rows[1])["event_type"], "manager_reply")

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

    def test_sqlite_migrates_legacy_conversation_schema_idempotently(self) -> None:
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute("PRAGMA journal_mode=MEMORY;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.executescript(
                """
                CREATE TABLE contacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT NOT NULL,
                    external_user_id TEXT NOT NULL,
                    username TEXT NOT NULL DEFAULT '',
                    display_name TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contact_id INTEGER NOT NULL UNIQUE,
                    source_channel TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    city TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    interested_products TEXT NOT NULL DEFAULT '[]',
                    tags TEXT NOT NULL DEFAULT '[]',
                    amocrm_lead_id TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_message_at TEXT NOT NULL
                );
                CREATE TABLE conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contact_id INTEGER NOT NULL,
                    channel TEXT NOT NULL,
                    external_chat_id TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_message_at TEXT NOT NULL
                );
                """
            )

        SQLiteLeadRepository(self.sqlite_path)
        SQLiteLeadRepository(self.sqlite_path)

        with sqlite3.connect(self.sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(conversations)").fetchall()
            }
            self.assertTrue(
                {
                    "owner_id",
                    "owner_name",
                    "owner_claimed_at",
                    "last_customer_message_at",
                    "last_manager_message_at",
                    "needs_attention",
                }.issubset(columns)
            )
            tables = {
                row["name"]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            self.assertIn("conversation_events", tables)

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

    def test_json_can_filter_conversation_events_by_type(self) -> None:
        repo = JSONLeadRepository(self.json_path)
        snapshot = self._ingest(repo)
        repo.add_conversation_event(
            conversation_id=snapshot.conversation_id,
            event_type="customer_waiting_manager",
            actor="system",
            payload={"status": "waiting_manager"},
        )
        repo.add_conversation_event(
            conversation_id=snapshot.conversation_id,
            event_type="manager_reply",
            actor="manager",
            payload={"text": "Handled"},
        )

        rows = repo.list_conversation_events_by_type(
            event_types=("customer_waiting_manager", "manager_reply"),
            limit=10,
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["event_type"], "customer_waiting_manager")
        self.assertEqual(rows[1]["event_type"], "manager_reply")

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

    def test_json_load_backfills_legacy_conversation_structure(self) -> None:
        legacy = {
            "counters": {
                "contacts": 1,
                "leads": 1,
                "conversations": 1,
                "messages": 1,
                "lead_events": 0,
            },
            "contacts": [
                {
                    "id": 1,
                    "channel": "vk",
                    "external_user_id": "user-1",
                    "username": "tester",
                    "display_name": "Test User",
                    "phone": "",
                    "created_at": "2026-04-01T10:00:00+00:00",
                    "updated_at": "2026-04-01T10:00:00+00:00",
                }
            ],
            "leads": [
                {
                    "id": 1,
                    "contact_id": 1,
                    "source_channel": "vk",
                    "stage": "new",
                    "mode": "ai",
                    "city": "",
                    "summary": "",
                    "interested_products": [],
                    "tags": [],
                    "created_at": "2026-04-01T10:00:00+00:00",
                    "updated_at": "2026-04-01T10:00:00+00:00",
                    "last_message_at": "2026-04-01T10:00:00+00:00",
                }
            ],
            "conversations": [
                {
                    "id": 1,
                    "contact_id": 1,
                    "channel": "vk",
                    "external_chat_id": "chat-1",
                    "mode": "ai",
                    "created_at": "2026-04-01T10:00:00+00:00",
                    "updated_at": "2026-04-01T10:00:00+00:00",
                    "last_message_at": "2026-04-01T10:00:00+00:00",
                }
            ],
            "messages": [
                {
                    "id": 1,
                    "conversation_id": 1,
                    "sender_role": "customer",
                    "sender_name": "Test User",
                    "text": "Need help",
                    "raw_payload": {},
                    "created_at": "2026-04-01T10:00:00+00:00",
                }
            ],
            "lead_events": [],
            "inbound_events": [],
        }
        self.json_path.write_text(json.dumps(legacy, ensure_ascii=False), encoding="utf-8")

        repo = JSONLeadRepository(self.json_path)
        snapshot = repo.get_snapshot(1)
        target = repo.get_conversation_target(1)

        self.assertEqual(snapshot.status.value, "new")
        self.assertEqual(snapshot.owner_name, "")
        self.assertFalse(snapshot.needs_attention)
        self.assertEqual(target["external_user_id"], "user-1")
        rows = repo.list_conversation_events(1, limit=10)
        self.assertEqual(rows, [])

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
