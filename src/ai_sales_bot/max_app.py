from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

from .admin_notifier import notify_admin_via_telegram
from .app import SalesBotRuntime, create_runtime
from .conversation_flow import SalesConversationManager
from .domain import Channel, InboundMessage


logger = logging.getLogger("lesdal.ai_sales.max")
MAX_API_URL = "https://platform-api.max.ru"


@dataclass(slots=True)
class MaxUpdateBatch:
    updates: list[dict]
    marker: int | None


def _extract_max_inbound(update: dict) -> tuple[InboundMessage, dict[str, int | None]] | None:
    if update.get("update_type") != "message_created":
        return None

    message = update.get("message") or {}
    if not isinstance(message, dict):
        return None

    sender = message.get("sender") or {}
    if bool(sender.get("is_bot")):
        return None

    body = message.get("body") or {}
    text = str(body.get("text") or "").strip()
    user_id = int(sender.get("user_id") or 0)
    if not text or user_id <= 0:
        return None

    recipient = message.get("recipient") or {}
    chat_id = recipient.get("chat_id")
    if chat_id is None and isinstance(recipient.get("chat"), dict):
        chat_id = recipient["chat"].get("chat_id")
    recipient_user_id = recipient.get("user_id")
    if recipient_user_id is None and isinstance(recipient.get("user"), dict):
        recipient_user_id = recipient["user"].get("user_id")

    full_name = " ".join(
        part for part in [str(sender.get("first_name") or "").strip(), str(sender.get("last_name") or "").strip()] if part
    ).strip()
    outbound_user_id = user_id if chat_id in (None, 0, "") else None
    outbound_chat_id = int(chat_id) if chat_id not in (None, "", 0) else None
    external_chat_id = str(outbound_chat_id or recipient_user_id or user_id)

    return (
        InboundMessage(
            channel=Channel.MAX,
            external_user_id=str(user_id),
            external_chat_id=external_chat_id,
            text=text,
            username=str(sender.get("username") or "").strip(),
            display_name=full_name or str(sender.get("username") or user_id),
            raw_payload=update,
        ),
        {
            "user_id": outbound_user_id,
            "chat_id": outbound_chat_id,
        },
    )


class MaxSalesBot:
    def __init__(self, runtime: SalesBotRuntime | None = None) -> None:
        self.runtime = runtime or create_runtime()
        self.config = self.runtime.config
        self.flow = SalesConversationManager(self.runtime)

    def run(self) -> None:
        if not self.config.has_max_inbox:
            raise RuntimeError("MAX_BOT_TOKEN is missing in .env")

        marker: int | None = None
        while True:
            try:
                batch = self._get_updates(marker)
                marker = batch.marker
                for update in batch.updates:
                    self._handle_update(update)
            except requests.RequestException as exc:
                logger.warning("MAX long poll request failed: %s", exc)
                time.sleep(3)

    def _get_updates(self, marker: int | None) -> MaxUpdateBatch:
        response = requests.get(
            f"{MAX_API_URL}/updates",
            headers={"Authorization": self.config.max_bot_token},
            params={
                "limit": self.config.max_longpoll_limit,
                "timeout": self.config.max_longpoll_timeout,
                "marker": marker,
                "types": "message_created",
            },
            timeout=self.config.max_longpoll_timeout + 10,
        )
        response.raise_for_status()
        payload = response.json()
        return MaxUpdateBatch(
            updates=list(payload.get("updates") or []),
            marker=payload.get("marker"),
        )

    def _handle_update(self, update: dict) -> None:
        extracted = _extract_max_inbound(update)
        if extracted is None:
            return

        inbound, outbound_target = extracted
        try:
            result = self.flow.handle_inbound_customer_message(inbound)
            if result.admin_notification:
                notify_admin_via_telegram(self.config, result.admin_notification)

            if not result.reply_text:
                return

            self._send_text(
                text=result.reply_text,
                user_id=outbound_target.get("user_id"),
                chat_id=outbound_target.get("chat_id"),
            )
            self.flow.record_outbound_reply(result.snapshot, result.reply_text)
        except requests.RequestException as exc:
            logger.warning("MAX update handling failed: %s", exc)

    def _send_text(
        self,
        *,
        text: str,
        user_id: int | None,
        chat_id: int | None,
    ) -> None:
        params: dict[str, int] = {}
        if chat_id:
            params["chat_id"] = chat_id
        elif user_id:
            params["user_id"] = user_id
        else:
            raise RuntimeError("MAX outbound target is missing")

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


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    MaxSalesBot().run()
