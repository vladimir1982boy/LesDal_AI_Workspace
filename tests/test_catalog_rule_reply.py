from __future__ import annotations

import unittest
from pathlib import Path

from src.ai_sales_bot.ai_engine import build_catalog_rule_reply
from src.ai_sales_bot.catalog import ProductCatalog
from src.ai_sales_bot.domain import Channel, ConversationMode, ConversationSnapshot, LeadStage


class CatalogRuleReplyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = ProductCatalog.from_json(Path("AI_BOT/catalog_2026.json"))
        self.snapshot = ConversationSnapshot(
            contact_id=1,
            lead_id=1,
            conversation_id=1,
            channel=Channel.VK,
            external_user_id="42",
            external_chat_id="42",
            stage=LeadStage.NEW,
            mode=ConversationMode.AI,
            summary="",
            display_name="Василиса",
            username="vasilisa",
            city="",
            tags=[],
            interested_products=[],
        )

    def test_panther_availability_lists_multiple_panther_forms(self) -> None:
        reply = build_catalog_rule_reply(
            catalog=self.catalog,
            snapshot=self.snapshot,
            transcript=[],
            user_message="Так пантеры все таки есть?",
        )

        self.assertIsNotNone(reply)
        assert reply is not None
        self.assertIn("пантерный мухомор есть в наличии", reply.lower())
        self.assertIn("Пантерный мухомор в капсулах 0.45 г 2025", reply)
        self.assertIn("Шляпки пантерного мухомора порошок 2025", reply)
        self.assertIn("Шляпки пантерного мухомора целые 2025", reply)
        self.assertNotIn("Красный мухомор", reply)

    def test_contextual_form_request_keeps_panther_family(self) -> None:
        transcript = [
            {"sender_role": "customer", "text": "Так пантеры все таки есть?"},
            {"sender_role": "ai", "text": "Да, пантерный мухомор есть."},
        ]

        reply = build_catalog_rule_reply(
            catalog=self.catalog,
            snapshot=self.snapshot,
            transcript=transcript,
            user_message="Я вообще то шляпки хочу. Почему не предлагаете?",
        )

        self.assertIsNotNone(reply)
        assert reply is not None
        self.assertIn("пантерный мухомор", reply.lower())
        self.assertIn("Шляпки пантерного мухомора целые 2025", reply)
        self.assertNotIn("Красный мухомор", reply)

    def test_typo_and_complaint_about_red_still_keep_panther_family(self) -> None:
        transcript = [
            {"sender_role": "customer", "text": "Пантерный..."},
        ]

        reply = build_catalog_rule_reply(
            catalog=self.catalog,
            snapshot=self.snapshot,
            transcript=transcript,
            user_message="Я же про партерный, зачем опять про красный?",
        )

        self.assertIsNotNone(reply)
        assert reply is not None
        self.assertIn("пантерный мухомор", reply.lower())
        self.assertIn("Пантерный мухомор в капсулах 0.45 г 2025", reply)
        self.assertIn("Шляпки пантерного мухомора целые 2025", reply)
        self.assertNotIn("Красный мухомор", reply)
        self.assertNotIn("Да Хун Пао", reply)

    def test_price_objection_gets_budget_step_instead_of_repeating_full_list(self) -> None:
        transcript = [
            {"sender_role": "customer", "text": "Пантерный"},
        ]

        reply = build_catalog_rule_reply(
            catalog=self.catalog,
            snapshot=self.snapshot,
            transcript=transcript,
            user_message="Пантера дорога у вас",
        )

        self.assertIsNotNone(reply)
        assert reply is not None
        self.assertIn("Понимаю вас", reply)
        self.assertIn("32 шт за 1200 ₽", reply)
        self.assertIn("от 2000 ₽", reply)
        self.assertIn("самый бюджетный вариант", reply)
        self.assertNotIn("Да Хун Пао", reply)


if __name__ == "__main__":
    unittest.main()
