from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

from .admin_notifier import notify_admin_via_telegram
from .app import SalesBotRuntime, create_runtime
from .config import SalesBotConfig
from .conversation_flow import SalesConversationManager
from .domain import Channel, InboundMessage


logger = logging.getLogger("lesdal.ai_sales.vk")
VK_API_URL = "https://api.vk.com/method"
VK_LONGPOLL_TIMEOUT = 40


class VKAPIError(RuntimeError):
    pass


@dataclass(slots=True)
class VKLongPollState:
    server: str
    key: str
    ts: str


def _vk_api_call(config: SalesBotConfig, method: str, **params) -> dict:
    response = requests.post(
        f"{VK_API_URL}/{method}",
        data={
            "access_token": config.vk_longpoll_token,
            "v": config.vk_api_version,
            **params,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        error = payload["error"] or {}
        raise VKAPIError(f"{method} failed: {error.get('error_msg', 'Unknown VK error')}")
    return payload.get("response") or {}


def _extract_vk_inbound(update: dict) -> tuple[InboundMessage, int, str] | None:
    if update.get("type") != "message_new":
        return None

    message = ((update.get("object") or {}).get("message") or {})
    if not isinstance(message, dict):
        return None
    if int(message.get("out") or 0) == 1:
        return None

    text = str(message.get("text") or "").strip()
    from_id = int(message.get("from_id") or 0)
    peer_id = int(message.get("peer_id") or 0)
    if not text or from_id <= 0 or peer_id <= 0:
        return None

    message_id = int(message.get("id") or 0)
    conversation_message_id = int(message.get("conversation_message_id") or 0)
    event_key = str(update.get("event_id") or f"{peer_id}:{message_id or conversation_message_id}")

    return (
        InboundMessage(
            channel=Channel.VK,
            external_user_id=str(from_id),
            external_chat_id=str(peer_id),
            text=text,
            raw_payload=update,
        ),
        from_id,
        event_key,
    )


class VKSalesBot:
    def __init__(self, runtime: SalesBotRuntime | None = None) -> None:
        self.runtime = runtime or create_runtime()
        self.config = self.runtime.config
        self.flow = SalesConversationManager(self.runtime)
        self._user_cache: dict[int, dict[str, str]] = {}

    def run(self) -> None:
        if not self.config.has_vk:
            raise RuntimeError("VK_LONGPOLL_TOKEN/VK_GROUP_ID are missing in .env")

        state = self._get_longpoll_state()
        logger.info("VK long poll started for group %s", self.config.vk_group_id)
        while True:
            try:
                state = self._poll_once(state)
            except requests.RequestException as exc:
                logger.warning("VK long poll request failed: %s", exc)
                time.sleep(3)
            except VKAPIError as exc:
                logger.warning("VK API error: %s", exc)
                time.sleep(3)

    def _get_longpoll_state(self) -> VKLongPollState:
        response = _vk_api_call(
            self.config,
            "groups.getLongPollServer",
            group_id=self.config.vk_group_id,
        )
        return VKLongPollState(
            server=str(response.get("server") or ""),
            key=str(response.get("key") or ""),
            ts=str(response.get("ts") or ""),
        )

    def _poll_once(self, state: VKLongPollState) -> VKLongPollState:
        response = requests.get(
            state.server,
            params={
                "act": "a_check",
                "key": state.key,
                "ts": state.ts,
                "wait": self.config.vk_longpoll_wait,
            },
            timeout=VK_LONGPOLL_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()

        failed = int(payload.get("failed") or 0)
        if failed == 1:
            return VKLongPollState(server=state.server, key=state.key, ts=str(payload.get("ts") or state.ts))
        if failed in {2, 3}:
            return self._get_longpoll_state()

        for update in payload.get("updates") or []:
            self._handle_update(update)

        return VKLongPollState(
            server=state.server,
            key=state.key,
            ts=str(payload.get("ts") or state.ts),
        )

    def _handle_update(self, update: dict) -> None:
        extracted = _extract_vk_inbound(update)
        if extracted is None:
            return

        inbound, user_id, event_key = extracted
        if not self.runtime.repository.register_inbound_event(
            channel=Channel.VK,
            event_key=event_key,
        ):
            logger.info("Skipping duplicate VK event %s", event_key)
            return
        logger.info("Received VK inbound message from user %s in peer %s", inbound.external_user_id, inbound.external_chat_id)
        try:
            profile = self._get_user_profile(user_id)
            inbound.username = profile.get("username", "")
            inbound.display_name = profile.get("display_name", "") or inbound.external_user_id

            result = self.flow.handle_inbound_customer_message(inbound)
            if result.admin_notification:
                notify_admin_via_telegram(self.config, result.admin_notification)

            if not result.reply_text:
                return

            self._send_text(peer_id=int(inbound.external_chat_id), text=result.reply_text)
            self.flow.record_outbound_reply(result.snapshot, result.reply_text)
        except (requests.RequestException, VKAPIError) as exc:
            logger.warning("VK update handling failed: %s", exc)

    def _get_user_profile(self, user_id: int) -> dict[str, str]:
        cached = self._user_cache.get(user_id)
        if cached is not None:
            return cached

        response = _vk_api_call(
            self.config,
            "users.get",
            user_ids=str(user_id),
            fields="screen_name",
        )
        profile: dict[str, str] = {}
        if isinstance(response, list) and response:
            row = response[0] or {}
            full_name = " ".join(
                part for part in [str(row.get("first_name") or "").strip(), str(row.get("last_name") or "").strip()] if part
            ).strip()
            profile = {
                "display_name": full_name,
                "username": str(row.get("screen_name") or "").strip(),
            }
        self._user_cache[user_id] = profile
        return profile

    def _send_text(self, *, peer_id: int, text: str) -> None:
        _vk_api_call(
            self.config,
            "messages.send",
            peer_id=peer_id,
            random_id=int(time.time() * 1000),
            message=text,
        )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    VKSalesBot().run()
