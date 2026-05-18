from __future__ import annotations

import io
import logging
import unittest

from src.ai_sales_bot.logging_setup import configure_logging


class LoggingSetupTests(unittest.TestCase):
    def test_configure_logging_redacts_telegram_token_in_urls(self) -> None:
        root = logging.getLogger()
        old_handlers = list(root.handlers)
        old_level = root.level
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        root.handlers = [handler]
        root.setLevel(logging.INFO)

        try:
            configure_logging()
            root.warning("POST https://api.telegram.org/bot123456:SECRET/getMe")
        finally:
            root.handlers = old_handlers
            root.setLevel(old_level)

        output = stream.getvalue()
        self.assertIn("https://api.telegram.org/bot<redacted>/getMe", output)
        self.assertNotIn("123456:SECRET", output)

    def test_configure_logging_quiets_noisy_network_loggers(self) -> None:
        configure_logging()

        self.assertEqual(logging.getLogger("httpx").level, logging.WARNING)
        self.assertEqual(logging.getLogger("telegram").level, logging.WARNING)
        self.assertEqual(logging.getLogger("telegram.ext").level, logging.WARNING)


if __name__ == "__main__":
    unittest.main()
