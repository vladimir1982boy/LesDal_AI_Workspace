from __future__ import annotations

import logging
import time

import requests

from .config import SalesBotConfig
from .domain import Channel


logger = logging.getLogger("lesdal.ai_sales.outbound")
VK_API_URL = "https://api.vk.com/method"
MAX_API_URL = "https://platform-api.max.ru"


class OutboundDispatchError(RuntimeError):
    pass


class OutboundDispatcher:
    def __init__(self, config: SalesBotConfig) -> None:
        self.config = config

    def send_text(
        self,
        *,
        channel: Channel,
        external_chat_id: str,
        external_user_id: str,
        text: str,
    ) -> bool:
        if channel == Channel.TELEGRAM:
            return self._send_telegram_text(chat_id=external_chat_id, text=text)
        if channel == Channel.VK:
            return self._send_vk_text(peer_id=external_chat_id, text=text)
        if channel == Channel.MAX:
            return self._send_max_text(
                external_chat_id=external_chat_id,
                external_user_id=external_user_id,
                text=text,
            )
        raise OutboundDispatchError(f"Unsupported outbound channel: {channel.value}")

    def _send_telegram_text(self, *, chat_id: str, text: str) -> bool:
        if not self.config.telegram_bot_token:
            raise OutboundDispatchError("TELEGRAM_BOT_TOKEN is missing")

        response = requests.post(
            f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": text,
            },
            timeout=30,
        )
        if response.status_code >= 400:
            raise OutboundDispatchError(
                f"Telegram sendMessage failed: {response.status_code} {response.text}"
            )
        return True

    def _send_vk_text(self, *, peer_id: str, text: str) -> bool:
        if not (self.config.vk_longpoll_token and self.config.vk_group_id):
            raise OutboundDispatchError("VK_LONGPOLL_TOKEN/VK_GROUP_ID are missing")

        response = requests.post(
            f"{VK_API_URL}/messages.send",
            data={
                "access_token": self.config.vk_longpoll_token,
                "v": self.config.vk_api_version,
                "peer_id": peer_id,
                "random_id": int(time.time() * 1000),
                "message": text,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            error = payload["error"] or {}
            raise OutboundDispatchError(
                f"VK messages.send failed: {error.get('error_msg', 'Unknown VK error')}"
            )
        return True

    def _send_max_text(
        self,
        *,
        external_chat_id: str,
        external_user_id: str,
        text: str,
    ) -> bool:
        if not self.config.max_bot_token:
            raise OutboundDispatchError("MAX_BOT_TOKEN is missing")

        params: dict[str, int] = {}
        chat_id = external_chat_id.strip()
        user_id = external_user_id.strip()
        if chat_id and chat_id.isdigit() and chat_id != user_id:
            params["chat_id"] = int(chat_id)
        elif user_id and user_id.isdigit():
            params["user_id"] = int(user_id)
        else:
            raise OutboundDispatchError("MAX outbound target is missing")

        response = requests.post(
            f"{MAX_API_URL}/messages",
            headers={
                "Authorization": self.config.max_bot_token,
                "Content-Type": "application/json",
            },
            params=params,
            json={"text": text},
            timeout=30,
        )
        response.raise_for_status()
        return True
