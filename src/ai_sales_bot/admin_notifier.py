from __future__ import annotations

import logging

import requests

from .config import SalesBotConfig


logger = logging.getLogger("lesdal.ai_sales.admin_notifier")


def notify_admin_via_telegram(config: SalesBotConfig, text: str) -> bool:
    if not (config.has_admin_channel and text):
        return False

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage",
            data={
                "chat_id": config.admin_chat_target,
                "text": text,
            },
            timeout=20,
        )
        if response.status_code >= 400:
            logger.warning(
                "Failed to notify admin via Telegram: %s %s",
                response.status_code,
                response.text,
            )
            return False
    except requests.RequestException as exc:
        logger.warning("Failed to notify admin via Telegram: %s", exc)
        return False

    return True
