from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from src.ai_sales_bot.dashboard_server import (
    DashboardPermissionError,
    _can_force_takeover,
    _consume_operator_session,
    _create_operator_session,
    _is_foreign_owner,
    _resolve_force_claim,
    _session_is_expired,
)
from src.ai_sales_bot.config import DashboardOperator


class DashboardServerPermissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.foreign_snapshot = SimpleNamespace(owner_id="alice", owner_name="Alice")
        self.own_snapshot = SimpleNamespace(owner_id="bob", owner_name="Bob")
        self.unowned_snapshot = SimpleNamespace(owner_id="", owner_name="")
        self.manager = {
            "operator_id": "bob",
            "display_name": "Bob",
            "role": "manager",
            "can_force_takeover": False,
        }
        self.supervisor = {
            "operator_id": "lead",
            "display_name": "Lead",
            "role": "supervisor",
            "can_force_takeover": True,
        }

    def test_can_force_takeover_detects_supervisor(self) -> None:
        self.assertFalse(_can_force_takeover(self.manager))
        self.assertTrue(_can_force_takeover(self.supervisor))

    def test_is_foreign_owner_detects_another_operator(self) -> None:
        self.assertTrue(_is_foreign_owner(self.foreign_snapshot, self.manager))
        self.assertFalse(_is_foreign_owner(self.own_snapshot, self.manager))
        self.assertFalse(_is_foreign_owner(self.unowned_snapshot, self.manager))

    def test_resolve_force_claim_rejects_manager_for_foreign_dialog(self) -> None:
        with self.assertRaises(DashboardPermissionError):
            _resolve_force_claim(self.foreign_snapshot, self.manager, requested_force=True)

    def test_resolve_force_claim_allows_supervisor_for_foreign_dialog(self) -> None:
        self.assertTrue(_resolve_force_claim(self.foreign_snapshot, self.supervisor, requested_force=True))

    def test_resolve_force_claim_ignores_force_for_unowned_dialog(self) -> None:
        self.assertFalse(_resolve_force_claim(self.unowned_snapshot, self.supervisor, requested_force=True))


class DashboardServerSessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.operator = DashboardOperator(
            operator_id="alice",
            display_name="Alice",
            pin="1234",
            role="manager",
        )
        self.now = datetime(2026, 4, 3, 12, 0, tzinfo=timezone.utc)

    def test_create_operator_session_sets_created_and_last_seen(self) -> None:
        session = _create_operator_session(self.operator, now=self.now)

        self.assertEqual(session["operator"]["operator_id"], "alice")
        self.assertEqual(session["created_at"], self.now.isoformat())
        self.assertEqual(session["last_seen_at"], self.now.isoformat())

    def test_session_is_expired_after_ttl(self) -> None:
        session = _create_operator_session(self.operator, now=self.now)

        expired = _session_is_expired(
            session,
            ttl_minutes=30,
            now=self.now + timedelta(minutes=31),
        )

        self.assertTrue(expired)

    def test_consume_operator_session_refreshes_last_seen(self) -> None:
        session = _create_operator_session(self.operator, now=self.now)
        sessions = {"token-1": session}

        payload, expired = _consume_operator_session(
            sessions,
            "token-1",
            ttl_minutes=30,
            now=self.now + timedelta(minutes=10),
            refresh=True,
        )

        self.assertFalse(expired)
        assert payload is not None
        self.assertEqual(payload["operator_id"], "alice")
        self.assertEqual(
            sessions["token-1"]["last_seen_at"],
            (self.now + timedelta(minutes=10)).isoformat(),
        )

    def test_consume_operator_session_removes_expired_session(self) -> None:
        session = _create_operator_session(self.operator, now=self.now)
        sessions = {"token-1": session}

        payload, expired = _consume_operator_session(
            sessions,
            "token-1",
            ttl_minutes=30,
            now=self.now + timedelta(minutes=31),
        )

        self.assertIsNone(payload)
        self.assertTrue(expired)
        self.assertEqual(sessions, {})


if __name__ == "__main__":
    unittest.main()
