from __future__ import annotations

import unittest
from types import SimpleNamespace

from src.ai_sales_bot.dashboard_server import (
    DashboardPermissionError,
    _can_force_takeover,
    _is_foreign_owner,
    _resolve_force_claim,
)


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


if __name__ == "__main__":
    unittest.main()
