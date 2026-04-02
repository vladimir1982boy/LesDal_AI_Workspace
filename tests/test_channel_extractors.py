from __future__ import annotations

import unittest

from src.ai_sales_bot.max_app import _extract_max_inbound
from src.ai_sales_bot.vk_app import _extract_vk_inbound


class ChannelExtractorTests(unittest.TestCase):
    def test_extract_vk_inbound_message(self) -> None:
        extracted = _extract_vk_inbound(
            {
                "type": "message_new",
                "object": {
                    "message": {
                        "out": 0,
                        "text": "hello from vk",
                        "from_id": 101,
                        "peer_id": 101,
                    }
                },
            }
        )

        self.assertIsNotNone(extracted)
        inbound, user_id, event_key = extracted or (None, None, None)
        self.assertEqual(user_id, 101)
        self.assertEqual(event_key, "101:0")
        self.assertEqual(inbound.external_user_id, "101")
        self.assertEqual(inbound.external_chat_id, "101")
        self.assertEqual(inbound.text, "hello from vk")

    def test_extract_max_inbound_message(self) -> None:
        extracted = _extract_max_inbound(
            {
                "update_type": "message_created",
                "message": {
                    "sender": {
                        "user_id": 202,
                        "first_name": "Max",
                        "last_name": "User",
                        "username": "maxuser",
                        "is_bot": False,
                    },
                    "recipient": {
                        "user_id": 999,
                    },
                    "body": {
                        "text": "hello from max",
                    },
                },
            }
        )

        self.assertIsNotNone(extracted)
        inbound, target = extracted or (None, None)
        self.assertEqual(inbound.external_user_id, "202")
        self.assertEqual(inbound.external_chat_id, "999")
        self.assertEqual(inbound.display_name, "Max User")
        self.assertEqual(target["user_id"], 202)
        self.assertIsNone(target["chat_id"])


if __name__ == "__main__":
    unittest.main()
