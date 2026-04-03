from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

from .config import SalesBotConfig
from .domain import Channel


logger = logging.getLogger("lesdal.ai_sales.outbound")
VK_API_URL = "https://api.vk.com/method"
MAX_API_URL = "https://platform-api.max.ru"


class OutboundDispatchError(RuntimeError):
    pass


@dataclass(slots=True)
class OutboundSendResult:
    ok: bool
    channel: Channel
    error: str = ""
    retryable: bool = False
    message_id: str = ""


def _success(channel: Channel, *, message_id: str = "") -> OutboundSendResult:
    return OutboundSendResult(
        ok=True,
        channel=channel,
        message_id=message_id,
    )


def _failure(channel: Channel, *, error: str, retryable: bool) -> OutboundSendResult:
    return OutboundSendResult(
        ok=False,
        channel=channel,
        error=error,
        retryable=retryable,
    )


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
    ) -> OutboundSendResult:
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

    def _send_telegram_text(self, *, chat_id: str, text: str) -> OutboundSendResult:
        if not self.config.telegram_bot_token:
            return _failure(
                Channel.TELEGRAM,
                error="TELEGRAM_BOT_TOKEN is missing",
                retryable=False,
            )
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage",
                data={
                    "chat_id": chat_id,
                    "text": text,
                },
                timeout=30,
            )
        except requests.RequestException as exc:
            return _failure(Channel.TELEGRAM, error=str(exc), retryable=True)
        if response.status_code >= 400:
            return _failure(
                Channel.TELEGRAM,
                error=f"Telegram sendMessage failed: {response.status_code} {response.text}",
                retryable=response.status_code >= 500 or response.status_code == 429,
            )
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        message_id = ""
        result = payload.get("result") if isinstance(payload, dict) else {}
        if isinstance(result, dict):
            message_id = str(result.get("message_id") or "")
        return _success(Channel.TELEGRAM, message_id=message_id)

    def _send_vk_text(self, *, peer_id: str, text: str) -> OutboundSendResult:
        if not (self.config.vk_longpoll_token and self.config.vk_group_id):
            return _failure(
                Channel.VK,
                error="VK_LONGPOLL_TOKEN/VK_GROUP_ID are missing",
                retryable=False,
            )
        try:
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
        except requests.RequestException as exc:
            return _failure(Channel.VK, error=str(exc), retryable=True)
        if response.status_code >= 400:
            return _failure(
                Channel.VK,
                error=f"VK messages.send failed: {response.status_code} {response.text}",
                retryable=response.status_code >= 500 or response.status_code == 429,
            )
        payload = response.json()
        if "error" in payload:
            error = payload["error"] or {}
            error_code = int(error.get("error_code") or 0)
            return _failure(
                Channel.VK,
                error=f"VK messages.send failed: {error.get('error_msg', 'Unknown VK error')}",
                retryable=error_code in {6, 9, 10, 29},
            )
        return _success(Channel.VK, message_id=str(payload.get("response") or ""))

    def _send_max_text(
        self,
        *,
        external_chat_id: str,
        external_user_id: str,
        text: str,
    ) -> OutboundSendResult:
        if not self.config.max_bot_token:
            return _failure(
                Channel.MAX,
                error="MAX_BOT_TOKEN is missing",
                retryable=False,
            )

        params: dict[str, int] = {}
        chat_id = external_chat_id.strip()
        user_id = external_user_id.strip()
        if chat_id and chat_id.isdigit() and chat_id != user_id:
            params["chat_id"] = int(chat_id)
        elif user_id and user_id.isdigit():
            params["user_id"] = int(user_id)
        else:
            return _failure(
                Channel.MAX,
                error="MAX outbound target is missing",
                retryable=False,
            )
        try:
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
        except requests.RequestException as exc:
            return _failure(Channel.MAX, error=str(exc), retryable=True)
        if response.status_code >= 400:
            return _failure(
                Channel.MAX,
                error=f"MAX send failed: {response.status_code} {response.text}",
                retryable=response.status_code >= 500 or response.status_code == 429,
            )
        message_id = ""
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if isinstance(payload, dict):
            message_id = str(payload.get("message_id") or payload.get("id") or "")
        return _success(Channel.MAX, message_id=message_id)
