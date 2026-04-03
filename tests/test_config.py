from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from src.ai_sales_bot.config import PROJECT_ROOT, SalesBotConfig


TEST_RUNTIME_DIR = PROJECT_ROOT / "tests" / "_runtime"
TEST_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


class SalesBotConfigTests(unittest.TestCase):
    def test_google_sheets_requires_existing_credentials_file(self) -> None:
        env_path = TEST_RUNTIME_DIR / "config_missing.env"
        env_path.write_text(
            "\n".join(
                [
                    "GOOGLE_SHEETS_SPREADSHEET_ID=test-sheet",
                    "GOOGLE_SHEETS_CREDENTIALS_PATH=missing.json",
                ]
            ),
            encoding="utf-8",
        )

        with mock.patch.dict("os.environ", {}, clear=True):
            config = SalesBotConfig.from_env(env_path)

        self.assertFalse(config.has_google_sheets)
        self.assertEqual(
            config.google_sheets_credentials_path,
            PROJECT_ROOT / "missing.json",
        )

    def test_google_sheets_accepts_existing_credentials_file(self) -> None:
        env_path = TEST_RUNTIME_DIR / "config_existing.env"
        credentials_path = TEST_RUNTIME_DIR / "service_account.json"
        credentials_path.write_text("{}", encoding="utf-8")
        env_path.write_text(
            "\n".join(
                [
                    "GOOGLE_SHEETS_SPREADSHEET_ID=test-sheet",
                    "GOOGLE_SHEETS_CREDENTIALS_PATH=tests/_runtime/service_account.json",
                ]
            ),
            encoding="utf-8",
        )

        with mock.patch.dict("os.environ", {}, clear=True):
            config = SalesBotConfig.from_env(env_path)

        self.assertTrue(config.has_google_sheets)
        self.assertEqual(config.google_sheets_credentials_path, credentials_path)

    def test_manager_name_repairs_utf8_mojibake(self) -> None:
        env_path = TEST_RUNTIME_DIR / "config_manager.env"
        env_path.write_text(
            "AI_SALES_MANAGER_NAME=Р’Р»Р°РґРёРјРёСЂ",
            encoding="utf-8",
        )

        with mock.patch.dict("os.environ", {}, clear=True):
            config = SalesBotConfig.from_env(env_path)

        self.assertNotEqual(config.manager_name, "Р’Р»Р°РґРёРјРёСЂ")


    def test_vk_longpoll_token_prefers_dedicated_env_key(self) -> None:
        env_path = TEST_RUNTIME_DIR / "config_vk.env"
        env_path.write_text(
            "\n".join(
                [
                    "VK_API_KEY=wall-token",
                    "VK_LONGPOLL_TOKEN=bot-token",
                    "VK_GROUP_ID=123",
                ]
            ),
            encoding="utf-8",
        )

        with mock.patch.dict("os.environ", {}, clear=True):
            config = SalesBotConfig.from_env(env_path)

        self.assertEqual(config.vk_access_token, "wall-token")
        self.assertEqual(config.vk_longpoll_token, "bot-token")
        self.assertTrue(config.has_vk)

    def test_admin_chat_target_prefers_explicit_chat_id(self) -> None:
        env_path = TEST_RUNTIME_DIR / "config_admin.env"
        env_path.write_text(
            "\n".join(
                [
                    "AI_SALES_ADMIN_ID=123",
                    "AI_SALES_ADMIN_CHAT_ID=-100555",
                ]
            ),
            encoding="utf-8",
        )

        with mock.patch.dict("os.environ", {}, clear=True):
            config = SalesBotConfig.from_env(env_path)

        self.assertEqual(config.admin_chat_target, "-100555")
        self.assertFalse(config.has_admin_channel)

    def test_dashboard_defaults_are_applied(self) -> None:
        env_path = TEST_RUNTIME_DIR / "config_dashboard.env"
        env_path.write_text("", encoding="utf-8")

        with mock.patch.dict("os.environ", {}, clear=True):
            config = SalesBotConfig.from_env(env_path)

        self.assertEqual(config.dashboard_host, "127.0.0.1")
        self.assertEqual(config.dashboard_port, 8787)
        self.assertEqual(len(config.dashboard_operators), 1)
        self.assertEqual(config.dashboard_operators[0].operator_id, "manager")

    def test_dashboard_operators_parse_from_env(self) -> None:
        env_path = TEST_RUNTIME_DIR / "config_dashboard_operators.env"
        env_path.write_text(
            "AI_SALES_DASHBOARD_OPERATORS=alice|Alice|1234|manager,bob|Bob|5678|supervisor",
            encoding="utf-8",
        )

        with mock.patch.dict("os.environ", {}, clear=True):
            config = SalesBotConfig.from_env(env_path)

        self.assertEqual(len(config.dashboard_operators), 2)
        self.assertEqual(config.dashboard_operators[0].operator_id, "alice")
        self.assertEqual(config.dashboard_operators[0].display_name, "Alice")
        self.assertEqual(config.dashboard_operators[0].pin, "1234")
        self.assertEqual(config.dashboard_operators[0].role, "manager")
        self.assertEqual(config.dashboard_operators[1].operator_id, "bob")
        self.assertEqual(config.dashboard_operators[1].pin, "5678")
        self.assertEqual(config.dashboard_operators[1].role, "supervisor")
        self.assertTrue(config.dashboard_operators[1].can_force_takeover)

    def test_default_env_files_support_secrets_directory(self) -> None:
        env_path = TEST_RUNTIME_DIR / "config_secrets.env"
        env_path.write_text("AI_SALES_ADMIN_ID=777", encoding="utf-8")

        with mock.patch("src.ai_sales_bot.config.DEFAULT_ENV_FILES", (env_path,)):
            with mock.patch.dict("os.environ", {}, clear=True):
                config = SalesBotConfig.from_env()

        self.assertEqual(config.admin_user_id, 777)


if __name__ == "__main__":
    unittest.main()
