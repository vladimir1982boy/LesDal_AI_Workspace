from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "AI_BOT" / "lesdal_ai_sales.sqlite3"
DEFAULT_CATALOG_PATH = PROJECT_ROOT / "AI_BOT" / "catalog_2026.json"
DEFAULT_LEAD_MAGNET_PATH = PROJECT_ROOT / "AI_BOT" / "lead_magnet.pdf"
DEFAULT_ENV_FILES = (
    PROJECT_ROOT / "secrets" / ".env.local",
    PROJECT_ROOT / ".env.local",
    PROJECT_ROOT / ".env",
)


def _resolve_path(raw_path: str, *, default: Path | None = None) -> Path:
    if not raw_path:
        return default or Path()

    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _repair_utf8_mojibake(value: str) -> str:
    if not value:
        return value

    if value == "Р’Р»Р°РґРёРјРёСЂ":
        return "Владимир"

    try:
        repaired = value.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value

    broken_score = value.count("Р") + value.count("Ð")
    repaired_score = repaired.count("Р") + repaired.count("Ð")
    return repaired if repaired_score < broken_score else value

def load_project_env(env_path: str | Path | None = None) -> None:
    if env_path is not None:
        load_dotenv(env_path)
        return

    for candidate in DEFAULT_ENV_FILES:
        if candidate.is_file():
            load_dotenv(candidate)


@dataclass(slots=True)
class SalesBotConfig:
    admin_user_id: int
    admin_chat_id: str
    manager_name: str
    telegram_bot_token: str
    telegram_channel_id: str
    gemini_api_key: str
    gemini_model: str
    max_bot_token: str
    max_channel_id: str
    max_longpoll_timeout: int
    max_longpoll_limit: int
    vk_access_token: str
    vk_longpoll_token: str
    vk_group_id: str
    vk_api_version: str
    vk_longpoll_wait: int
    amocrm_base_url: str
    amocrm_access_token: str
    amocrm_pipeline_id: str
    google_sheets_spreadsheet_id: str
    google_sheets_credentials_path: Path
    google_sheets_credentials_json: str
    google_sheets_leads_sheet: str
    dashboard_host: str
    dashboard_port: int
    dashboard_token: str
    db_path: Path
    catalog_path: Path
    lead_magnet_path: Path

    @classmethod
    def from_env(cls, env_path: str | Path | None = None) -> "SalesBotConfig":
        load_project_env(env_path)

        db_path_raw = os.getenv("AI_SALES_DB_PATH", "").strip()
        catalog_path_raw = os.getenv("AI_SALES_CATALOG_PATH", "").strip()
        lead_magnet_raw = os.getenv("AI_SALES_LEAD_MAGNET_PATH", "").strip()
        google_creds_path_raw = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH", "").strip()

        config = cls(
            admin_user_id=int(os.getenv("AI_SALES_ADMIN_ID", "0").strip() or "0"),
            admin_chat_id=os.getenv("AI_SALES_ADMIN_CHAT_ID", "").strip(),
            manager_name=os.getenv("AI_SALES_MANAGER_NAME", "Владимир").strip() or "Владимир",
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_channel_id=os.getenv("TELEGRAM_CHANNEL_ID", "").strip(),
            gemini_api_key=(
                os.getenv("GEMINI_API_KEY", "").strip()
                or os.getenv("GEMINI_KEY", "").strip()
            ),
            gemini_model=os.getenv("AI_SALES_GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash",
            max_bot_token=os.getenv("MAX_BOT_TOKEN", "").strip(),
            max_channel_id=os.getenv("MAX_CHANNEL_ID", "").strip(),
            max_longpoll_timeout=max(0, int(os.getenv("MAX_LONGPOLL_TIMEOUT", "30").strip() or "30")),
            max_longpoll_limit=max(1, int(os.getenv("MAX_LONGPOLL_LIMIT", "100").strip() or "100")),
            vk_access_token=(
                os.getenv("VK_ACCESS_TOKEN", "").strip()
                or os.getenv("VK_API_KEY", "").strip()
            ),
            vk_longpoll_token=(
                os.getenv("VK_LONGPOLL_TOKEN", "").strip()
                or os.getenv("VK_BOT_LONG_POLL_TOKEN", "").strip()
                or os.getenv("VK_ACCESS_TOKEN", "").strip()
                or os.getenv("VK_API_KEY", "").strip()
            ),
            vk_group_id=os.getenv("VK_GROUP_ID", "").strip(),
            vk_api_version=os.getenv("VK_API_VERSION", "5.199").strip() or "5.199",
            vk_longpoll_wait=max(1, int(os.getenv("VK_LONGPOLL_WAIT", "25").strip() or "25")),
            amocrm_base_url=os.getenv("AMOCRM_BASE_URL", "").strip(),
            amocrm_access_token=os.getenv("AMOCRM_ACCESS_TOKEN", "").strip(),
            amocrm_pipeline_id=os.getenv("AMOCRM_PIPELINE_ID", "").strip(),
            google_sheets_spreadsheet_id=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip(),
            google_sheets_credentials_path=_resolve_path(google_creds_path_raw),
            google_sheets_credentials_json=os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "").strip(),
            google_sheets_leads_sheet=os.getenv("GOOGLE_SHEETS_LEADS_SHEET", "Leads").strip() or "Leads",
            dashboard_host=os.getenv("AI_SALES_DASHBOARD_HOST", "127.0.0.1").strip() or "127.0.0.1",
            dashboard_port=max(1, int(os.getenv("AI_SALES_DASHBOARD_PORT", "8787").strip() or "8787")),
            dashboard_token=os.getenv("AI_SALES_DASHBOARD_TOKEN", "").strip(),
            db_path=_resolve_path(db_path_raw, default=DEFAULT_DB_PATH),
            catalog_path=_resolve_path(catalog_path_raw, default=DEFAULT_CATALOG_PATH),
            lead_magnet_path=_resolve_path(lead_magnet_raw, default=DEFAULT_LEAD_MAGNET_PATH),
        )
        config.manager_name = _repair_utf8_mojibake(config.manager_name) or "Владимир"
        return config

    @property
    def has_telegram(self) -> bool:
        return bool(self.telegram_bot_token)

    @property
    def has_admin(self) -> bool:
        return self.admin_user_id > 0

    @property
    def admin_chat_target(self) -> str:
        if self.admin_chat_id:
            return self.admin_chat_id
        if self.admin_user_id > 0:
            return str(self.admin_user_id)
        return ""

    @property
    def has_admin_channel(self) -> bool:
        return bool(self.telegram_bot_token and self.admin_chat_target)

    @property
    def has_max(self) -> bool:
        return bool(self.max_bot_token and self.max_channel_id)

    @property
    def has_max_inbox(self) -> bool:
        return bool(self.max_bot_token)

    @property
    def has_vk(self) -> bool:
        return bool(self.vk_longpoll_token and self.vk_group_id)

    @property
    def has_amocrm(self) -> bool:
        return bool(self.amocrm_base_url and self.amocrm_access_token)

    @property
    def has_google_sheets(self) -> bool:
        return bool(
            self.google_sheets_spreadsheet_id
            and (
                self.google_sheets_credentials_json
                or self.google_sheets_credentials_path.is_file()
            )
        )
