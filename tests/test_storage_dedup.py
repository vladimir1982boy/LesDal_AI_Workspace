from __future__ import annotations

import unittest
from pathlib import Path
from uuid import uuid4

from src.ai_sales_bot.domain import Channel
from src.ai_sales_bot.storage import JSONLeadRepository, SQLiteLeadRepository


class InboundEventDedupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime_dir = Path("tests/_runtime")
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        suffix = uuid4().hex
        self.json_path = self.runtime_dir / f"dedup_{self._testMethodName}_{suffix}.json"
        self.sqlite_path = self.runtime_dir / f"dedup_{self._testMethodName}_{suffix}.sqlite3"

    def test_json_repository_rejects_duplicate_inbound_event(self) -> None:
        repo = JSONLeadRepository(self.json_path)

        self.assertTrue(repo.register_inbound_event(channel=Channel.VK, event_key="evt-1"))
        self.assertFalse(repo.register_inbound_event(channel=Channel.VK, event_key="evt-1"))

    def test_sqlite_repository_rejects_duplicate_inbound_event(self) -> None:
        repo = SQLiteLeadRepository(self.sqlite_path)

        self.assertTrue(repo.register_inbound_event(channel=Channel.VK, event_key="evt-1"))
        self.assertFalse(repo.register_inbound_event(channel=Channel.VK, event_key="evt-1"))


if __name__ == "__main__":
    unittest.main()
