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
            owner_id="alice",
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
        self.snapshot.owner_id = ""
        self.snapshot.owner_name = ""
        self.snapshot.mode = ConversationMode.AI
        self.snapshot.status = ConversationStatus.NEW

        snapshot = self.service.claim_conversation(
            conversation_id=3,
            operator_name="Bob",
            operator_id="bob",
        )

        self.assertEqual(snapshot.mode, ConversationMode.MANAGER)
        self.assertEqual(snapshot.status, ConversationStatus.IN_PROGRESS)
        self.assertEqual(snapshot.owner_id, "bob")
        self.assertEqual(snapshot.owner_name, "Bob")
        self.assertEqual(self.repo.events[-1]["event_type"], "claimed_by_manager")

    def test_record_manager_reply_rejects_foreign_owner(self) -> None:
        with self.assertRaises(ConversationOwnershipError):
            self.service.record_manager_reply(
                conversation_id=3,
                manager_name="Bob",
                operator_id="bob",
                text="Reply from wrong owner",
                pause_ai=True,
            )

    def test_force_claim_reassigns_foreign_owner(self) -> None:
        snapshot = self.service.claim_conversation(
            conversation_id=3,
            operator_name="Supervisor",
            operator_id="supervisor",
            force=True,
        )

        self.assertEqual(snapshot.owner_id, "supervisor")
        self.assertEqual(snapshot.owner_name, "Supervisor")
        self.assertEqual(self.repo.events[-1]["event_type"], "force_claimed_by_supervisor")
        self.assertTrue(self.repo.events[-1]["payload"]["forced"])

    def test_release_conversation_clears_owner(self) -> None:
        snapshot = self.service.release_conversation(
            conversation_id=3,
            operator_name="Alice",
            operator_id="alice",
        )

        self.assertEqual(snapshot.owner_id, "")
        self.assertEqual(snapshot.owner_name, "")
        self.assertEqual(snapshot.status, ConversationStatus.NEW)
        self.assertEqual(self.repo.events[-1]["event_type"], "released_by_manager")

    def test_release_rejects_foreign_owner(self) -> None:
        with self.assertRaises(ConversationOwnershipError):
            self.service.release_conversation(
                conversation_id=3,
                operator_name="Bob",
                operator_id="bob",
            )

    def test_update_manager_notes_persists_and_logs_event(self) -> None:
        snapshot = self.service.update_manager_notes(
            conversation_id=3,
            notes="Priority client, prefers WhatsApp-style pacing.",
            actor="Alice",
            actor_id="alice",
        )

        self.assertEqual(snapshot.manager_notes, "Priority client, prefers WhatsApp-style pacing.")
        self.assertEqual(self.repo.events[-1]["event_type"], "manager_notes_updated")
        self.assertEqual(self.repo.events[-1]["payload"]["operator_id"], "alice")

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
            actor_id="alice",
        )

        self.assertEqual(snapshot.stage, LeadStage.QUALIFIED)
        self.assertEqual(snapshot.summary, "Interested in medium price tier")
        self.assertEqual(snapshot.tags, ["warm", "callback"])
        self.assertEqual(snapshot.priority, LeadPriority.HIGH)
        self.assertEqual(snapshot.follow_up_date, "2026-04-04")
        self.assertEqual(snapshot.next_action, "Call after proposal review")
        self.assertEqual(self.repo.events[-1]["event_type"], "lead_profile_updated")
        self.assertEqual(self.repo.events[-1]["payload"]["operator_id"], "alice")

    def test_update_lead_profile_requires_next_action_for_high_priority(self) -> None:
        with self.assertRaises(LeadProfileValidationError):
            self.service.update_lead_profile(
                conversation_id=3,
                priority=LeadPriority.HIGH,
                next_action="",
                actor="Alice",
            )

    def test_forced_takeover_summary_aggregates_by_day_week_and_operator(self) -> None:
        self.repo.forced_events = [
            {
                "conversation_id": 3,
                "actor": "Lead",
                "created_at": "2026-04-03T10:00:00+00:00",
                "payload": {"previous_owner_id": "alice"},
                "display_name": "Test User",
                "channel": "vk",
            },
            {
                "conversation_id": 4,
                "actor": "Lead",
                "created_at": "2026-04-01T10:00:00+00:00",
                "payload": {"previous_owner_id": "bob"},
                "display_name": "Second User",
                "channel": "telegram",
            },
            {
                "conversation_id": 5,
                "actor": "Supervisor 2",
                "created_at": "2026-03-25T10:00:00+00:00",
                "payload": {"previous_owner_id": "carol"},
                "display_name": "Old User",
                "channel": "max",
            },
        ]
        self.repo.transition_events = [
            {
                "id": 1,
                "conversation_id": 3,
                "event_type": "customer_waiting_manager",
                "channel": "vk",
                "display_name": "Test User",
                "external_chat_id": "42",
                "created_at": "2026-04-03T10:00:00+00:00",
                "payload": {"status": "waiting_manager"},
            },
            {
                "id": 2,
                "conversation_id": 3,
                "event_type": "manager_reply",
                "actor": "Alice",
                "channel": "vk",
                "display_name": "Test User",
                "external_chat_id": "42",
                "created_at": "2026-04-03T10:10:00+00:00",
                "payload": {"text": "Reply 1", "operator_id": "alice"},
            },
            {
                "id": 3,
                "conversation_id": 4,
                "event_type": "customer_waiting_manager",
                "channel": "telegram",
                "display_name": "Second User",
                "external_chat_id": "tg-4",
                "created_at": "2026-04-03T11:00:00+00:00",
                "payload": {"status": "waiting_manager"},
            },
            {
                "id": 4,
                "conversation_id": 4,
                "event_type": "manager_reply",
                "actor": "Bob",
                "channel": "telegram",
                "display_name": "Second User",
                "external_chat_id": "tg-4",
                "created_at": "2026-04-03T11:30:00+00:00",
                "payload": {"text": "Reply 2", "operator_id": "bob"},
            },
            {
                "id": 5,
                "conversation_id": 5,
                "event_type": "force_claimed_by_supervisor",
                "actor": "Supervisor 1",
                "channel": "max",
                "display_name": "Forced User",
                "external_chat_id": "max-5",
                "created_at": "2026-04-03T09:00:00+00:00",
                "payload": {"forced": True, "operator_id": "supervisor_1"},
            },
            {
                "id": 6,
                "conversation_id": 5,
                "event_type": "status_changed",
                "channel": "max",
                "display_name": "Forced User",
                "external_chat_id": "max-5",
                "created_at": "2026-04-03T09:40:00+00:00",
                "payload": {"status": "closed"},
            },
            {
                "id": 7,
                "conversation_id": 6,
                "event_type": "force_claimed_by_supervisor",
                "actor": "Supervisor 2",
                "channel": "vk",
                "display_name": "Returned User",
                "external_chat_id": "vk-6",
                "created_at": "2026-04-03T08:00:00+00:00",
                "payload": {"forced": True, "operator_id": "supervisor_2"},
            },
            {
                "id": 8,
                "conversation_id": 6,
                "event_type": "returned_to_ai",
                "channel": "vk",
                "display_name": "Returned User",
                "external_chat_id": "vk-6",
                "created_at": "2026-04-03T08:20:00+00:00",
                "payload": {"mode": "ai"},
            },
        ]

        from unittest import mock
        from datetime import datetime, timezone

        with mock.patch("src.ai_sales_bot.services._utcnow", return_value=datetime(2026, 4, 3, 12, 0, tzinfo=timezone.utc)):
            summary = self.service.get_forced_takeover_summary(limit=10)

        self.assertEqual(summary["today_count"], 1)
        self.assertEqual(summary["week_count"], 2)
        self.assertEqual(summary["total_count"], 3)
        self.assertEqual(summary["by_operator"][0]["operator"], "Lead")
        self.assertEqual(summary["by_operator"][0]["count"], 2)
        self.assertEqual(summary["recent"][0]["actor"], "Lead")
        self.assertEqual(summary["ownership_quality"]["waiting_manager_count"], 1)
        self.assertEqual(summary["ownership_quality"]["manager_without_reply_count"], 1)
        self.assertEqual(summary["ownership_quality"]["forced_closed_count"], 1)
        self.assertEqual(summary["ownership_quality"]["forced_returned_ai_count"], 1)
        self.assertEqual(summary["resolution_speed"]["waiting_to_first_reply_median_minutes"], 20)
        self.assertEqual(summary["resolution_speed"]["waiting_to_first_reply_samples"], 2)
        self.assertEqual(summary["resolution_speed"]["forced_to_resolution_median_minutes"], 30)
        self.assertEqual(summary["resolution_speed"]["forced_to_resolution_samples"], 2)
        self.assertEqual(summary["resolution_speed"]["waiting_to_first_reply_by_operator"][0]["operator"], "Alice")
        self.assertEqual(summary["resolution_speed"]["waiting_to_first_reply_by_operator"][0]["median_minutes"], 10)
        self.assertEqual(summary["resolution_speed"]["forced_to_resolution_by_operator"][0]["operator"], "Supervisor 1")
        self.assertEqual(summary["resolution_speed"]["forced_to_resolution_by_operator"][0]["median_minutes"], 40)

    def test_resolution_speed_drilldown_returns_operator_episodes(self) -> None:
        self.repo.transition_events = [
            {
                "id": 1,
                "conversation_id": 3,
                "event_type": "customer_waiting_manager",
                "channel": "vk",
                "display_name": "Test User",
                "external_chat_id": "42",
                "created_at": "2026-04-03T10:00:00+00:00",
                "payload": {"status": "waiting_manager"},
            },
            {
                "id": 2,
                "conversation_id": 3,
                "event_type": "manager_reply",
                "actor": "Alice",
                "channel": "vk",
                "display_name": "Test User",
                "external_chat_id": "42",
                "created_at": "2026-04-03T10:10:00+00:00",
                "payload": {"text": "Reply 1", "operator_id": "alice"},
            },
            {
                "id": 3,
                "conversation_id": 4,
                "event_type": "customer_waiting_manager",
                "channel": "telegram",
                "display_name": "Second User",
                "external_chat_id": "tg-4",
                "created_at": "2026-04-03T11:00:00+00:00",
                "payload": {"status": "waiting_manager"},
            },
            {
                "id": 4,
                "conversation_id": 4,
                "event_type": "manager_reply",
                "actor": "Bob",
                "channel": "telegram",
                "display_name": "Second User",
                "external_chat_id": "tg-4",
                "created_at": "2026-04-03T11:30:00+00:00",
                "payload": {"text": "Reply 2", "operator_id": "bob"},
            },
        ]

        from unittest import mock
        from datetime import datetime, timezone

        with mock.patch("src.ai_sales_bot.services._utcnow", return_value=datetime(2026, 4, 3, 12, 0, tzinfo=timezone.utc)):
            payload = self.service.get_resolution_speed_drilldown(
                metric="waiting_reply",
                operator_key="alice",
                period="today",
                limit=10,
            )

        self.assertEqual(payload["operator"], "Alice")
        self.assertEqual(payload["median_minutes"], 10)
        self.assertEqual(payload["samples"], 1)
        self.assertEqual(payload["episodes"][0]["conversation_id"], 3)
        self.assertEqual(payload["episodes"][0]["display_name"], "Test User")
        self.assertEqual(payload["episodes"][0]["duration_minutes"], 10)

    def test_forced_takeover_summary_applies_today_period_to_event_metrics(self) -> None:
        self.repo.forced_events = [
            {
                "conversation_id": 3,
                "actor": "Lead",
                "created_at": "2026-04-03T10:00:00+00:00",
                "payload": {"previous_owner_id": "alice"},
                "display_name": "Today User",
                "channel": "vk",
            },
            {
                "conversation_id": 4,
                "actor": "Lead",
                "created_at": "2026-04-01T10:00:00+00:00",
                "payload": {"previous_owner_id": "bob"},
                "display_name": "Older User",
                "channel": "telegram",
            },
        ]
        self.repo.transition_events = [
            {
                "id": 1,
                "conversation_id": 3,
                "event_type": "customer_waiting_manager",
                "channel": "vk",
                "display_name": "Today User",
                "external_chat_id": "42",
                "created_at": "2026-04-03T10:00:00+00:00",
                "payload": {"status": "waiting_manager"},
            },
            {
                "id": 2,
                "conversation_id": 3,
                "event_type": "manager_reply",
                "actor": "Alice",
                "channel": "vk",
                "display_name": "Today User",
                "external_chat_id": "42",
                "created_at": "2026-04-03T10:10:00+00:00",
                "payload": {"text": "Reply 1", "operator_id": "alice"},
            },
            {
                "id": 3,
                "conversation_id": 4,
                "event_type": "customer_waiting_manager",
                "channel": "telegram",
                "display_name": "Older User",
                "external_chat_id": "tg-4",
                "created_at": "2026-04-01T11:00:00+00:00",
                "payload": {"status": "waiting_manager"},
            },
            {
                "id": 4,
                "conversation_id": 4,
                "event_type": "manager_reply",
                "actor": "Bob",
                "channel": "telegram",
                "display_name": "Older User",
                "external_chat_id": "tg-4",
                "created_at": "2026-04-01T11:30:00+00:00",
                "payload": {"text": "Reply 2", "operator_id": "bob"},
            },
        ]

        from unittest import mock
        from datetime import datetime, timezone

        with mock.patch("src.ai_sales_bot.services._utcnow", return_value=datetime(2026, 4, 3, 12, 0, tzinfo=timezone.utc)):
            summary = self.service.get_forced_takeover_summary(limit=10, period="today")

        self.assertEqual(summary["period"], "today")
        self.assertEqual(summary["period_label"], "Сегодня")
        self.assertEqual(summary["total_count"], 1)
        self.assertEqual(summary["today_count"], 1)
        self.assertEqual(summary["week_count"], 1)
        self.assertEqual(summary["resolution_speed"]["waiting_to_first_reply_samples"], 1)
        self.assertEqual(summary["resolution_speed"]["waiting_to_first_reply_median_minutes"], 10)


class _FakeRepository:
    def __init__(self, snapshot: ConversationSnapshot) -> None:
        self.snapshot = snapshot
        self.events: list[dict] = []
        self.forced_events: list[dict] = []
        self.transition_events: list[dict] = []

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
        return [
            {
                "id": 3,
                "mode": "manager",
                "status": "waiting_manager",
                "owner_id": "alice",
                "owner_name": "Alice",
                "last_manager_message_at": None,
                "has_forced_takeover": False,
            },
            {
                "id": 4,
                "mode": "manager",
                "status": "in_progress",
                "owner_id": "bob",
                "owner_name": "Bob",
                "last_manager_message_at": None,
                "has_forced_takeover": False,
            },
            {
                "id": 5,
                "mode": "manager",
                "status": "closed",
                "owner_id": "lead",
                "owner_name": "Lead",
                "last_manager_message_at": "2026-04-03T09:00:00+00:00",
                "has_forced_takeover": True,
            },
            {
                "id": 6,
                "mode": "ai",
                "status": "new",
                "owner_id": "",
                "owner_name": "",
                "last_manager_message_at": "2026-04-03T08:00:00+00:00",
                "has_forced_takeover": True,
            },
        ][:limit]

    def list_conversation_events(self, conversation_id: int, *, limit: int = 50) -> list[dict]:
        return self.events[:limit]

    def list_conversation_events_by_type(self, *, event_types: list[str] | tuple[str, ...], limit: int = 2000) -> list[dict]:
        normalized = {str(item).strip() for item in event_types if str(item).strip()}
        return [
            row for row in self.transition_events
            if str(row.get("event_type", "")).strip() in normalized
        ][:limit]

    def list_forced_takeover_events(self, *, limit: int = 200) -> list[dict]:
        return self.forced_events[:limit]

    def set_conversation_mode(self, *, conversation_id: int, mode: ConversationMode) -> None:
        self.snapshot.mode = mode

    def update_conversation_state(
        self,
        *,
        conversation_id: int,
        mode: ConversationMode | None = None,
        status: ConversationStatus | None = None,
        owner_id: str | None = None,
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
            self.snapshot.owner_id = ""
            self.snapshot.owner_name = ""
            self.snapshot.owner_claimed_at = None
        else:
            if owner_id is not None:
                self.snapshot.owner_id = owner_id
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
