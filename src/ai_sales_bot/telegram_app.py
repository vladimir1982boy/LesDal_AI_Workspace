from __future__ import annotations

import asyncio
import logging
import re

from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from .app import SalesBotRuntime, create_runtime
from .conversation_flow import SalesConversationManager
from .domain import Channel, ConversationMode, ConversationSnapshot, InboundMessage
from .lead_sync import LeadSyncCoordinator
from .operator_api import OperatorInboxAPI


logger = logging.getLogger("lesdal.ai_sales.telegram")
CONVERSATION_RE = re.compile(r"\[conv:(\d+)\]")


class TelegramSalesBot:
    def __init__(self, runtime: SalesBotRuntime | None = None) -> None:
        self.runtime = runtime or create_runtime()
        self.config = self.runtime.config
        self.service = self.runtime.service
        self.flow = SalesConversationManager(self.runtime)
        self.lead_sync = LeadSyncCoordinator.from_config(
            config=self.config,
            service=self.service,
        )
        self.operator_api = OperatorInboxAPI(runtime=self.runtime)

    def build_application(self) -> Application:
        if not self.config.telegram_bot_token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is missing in .env")

        application = ApplicationBuilder().token(self.config.telegram_bot_token).build()
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("help", self.help_command))
        application.add_handler(CommandHandler("inbox", self.inbox))
        application.add_handler(CommandHandler("lead", self.lead))
        application.add_handler(CommandHandler("manager", self.manager_mode))
        application.add_handler(CommandHandler("ai", self.ai_mode))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.text_message))
        return application

    def run(self) -> None:
        application = self.build_application()
        application.run_polling(close_loop=False)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_message or not update.effective_user:
            return

        if self._is_admin(update):
            await update.effective_message.reply_text(self._operator_help())
            return

        text = (
            f"Здравствуйте. Меня зовут {self.config.manager_name}, я консультант LesDal.\n\n"
            "Помогу подобрать продукт под вашу задачу, расскажу по формату, цене и доставке.\n"
            "Можно просто написать, что вас интересует: сон, фокус, энергия, иммунитет, ЖКТ или другой запрос."
        )
        await update.effective_message.reply_text(text)

        if self.config.has_admin_channel:
            try:
                await context.bot.send_message(
                    chat_id=self.config.admin_chat_target,
                    text=(
                        "Новый пользователь запустил бота.\n"
                        f"Имя: {self._display_name(update)}\n"
                        f"Telegram ID: {update.effective_user.id}"
                    ),
                )
            except Exception as exc:
                logger.warning("Failed to notify admin on /start: %s", exc)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_message:
            return
        if self._is_admin(update):
            await update.effective_message.reply_text(self._operator_help())
            return
        await update.effective_message.reply_text(
            "Опишите задачу или напишите название продукта."
        )

    async def inbox(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update) or not update.effective_message:
            return
        rows = self.service.list_recent_conversations(limit=15)
        if not rows:
            await update.effective_message.reply_text("Inbox пока пуст.")
            return

        lines = ["Последние диалоги:"]
        for row in rows:
            label = row.get("display_name") or row.get("username") or "Без имени"
            lines.append(
                f"[conv:{row['id']}] {label} | {row['channel']} | {row['stage']} | {row['mode']}"
            )
        await update.effective_message.reply_text("\n".join(lines))

    async def lead(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update) or not update.effective_message:
            return
        conversation_id = self._command_conversation_id(context.args if context else [])
        if conversation_id is None:
            await update.effective_message.reply_text("Использование: /lead <conversation_id>")
            return

        try:
            summary = self.service.build_manager_summary(conversation_id=conversation_id)
        except Exception as exc:
            await update.effective_message.reply_text(f"Не удалось собрать summary: {exc}")
            return
        await update.effective_message.reply_text(summary)

    async def manager_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update) or not update.effective_message:
            return
        conversation_id = self._command_conversation_id(context.args if context else [])
        if conversation_id is None:
            await update.effective_message.reply_text("Использование: /manager <conversation_id>")
            return
        try:
            result = await asyncio.to_thread(
                self.operator_api.pause_conversation,
                conversation_id,
            )
            await update.effective_message.reply_text(
                f"Диалог [conv:{result.snapshot.conversation_id}] переведен в manager mode."
            )
        except Exception as exc:
            await update.effective_message.reply_text(f"Не удалось перевести диалог: {exc}")

    async def ai_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update) or not update.effective_message:
            return
        conversation_id = self._command_conversation_id(context.args if context else [])
        if conversation_id is None:
            await update.effective_message.reply_text("Использование: /ai <conversation_id>")
            return
        try:
            result = await asyncio.to_thread(
                self.operator_api.resume_conversation,
                conversation_id,
            )
            await update.effective_message.reply_text(
                f"Диалог [conv:{result.snapshot.conversation_id}] возвращен в AI mode."
            )
        except Exception as exc:
            await update.effective_message.reply_text(f"Не удалось вернуть AI: {exc}")

    async def text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_message or not update.effective_user or not update.effective_chat:
            return

        if self._is_admin(update):
            await self._handle_admin_text(update, context)
            return

        user_text = (update.effective_message.text or "").strip()
        if not user_text:
            return

        result = await asyncio.to_thread(
            self.flow.handle_inbound_customer_message,
            InboundMessage(
                channel=Channel.TELEGRAM,
                external_user_id=str(update.effective_user.id),
                external_chat_id=str(update.effective_chat.id),
                text=user_text,
                username=update.effective_user.username or "",
                display_name=self._display_name(update),
            )
        )
        snapshot = result.snapshot

        await self._notify_admin_text(context, result.admin_notification)

        if snapshot.mode == ConversationMode.MANAGER:
            return

        reply_text = result.reply_text
        if not reply_text:
            return
        await update.effective_message.reply_text(reply_text)
        await asyncio.to_thread(
            self.flow.record_outbound_reply,
            snapshot,
            reply_text,
        )

    async def _handle_admin_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_message:
            return

        reply_to = update.effective_message.reply_to_message
        if reply_to is None:
            await update.effective_message.reply_text(
                "Для ответа клиенту используйте Reply на уведомление бота или команды /inbox, /lead, /manager, /ai."
            )
            return

        conversation_id = self._extract_conversation_id(reply_to.text or reply_to.caption or "")
        if conversation_id is None:
            await update.effective_message.reply_text("Не нашел conversation id в сообщении, на которое вы отвечаете.")
            return

        customer_text = (update.effective_message.text or "").strip()
        if not customer_text:
            return

        try:
            await asyncio.to_thread(
                self.operator_api.reply_to_conversation,
                conversation_id,
                text=customer_text,
                pause_ai=True,
            )
        except Exception as exc:
            await update.effective_message.reply_text(f"Не удалось отправить сообщение: {exc}")
            return

        await update.effective_message.reply_text(
            f"Сообщение отправлено в [conv:{conversation_id}]. AI поставлен на паузу."
        )

    async def _notify_admin_text(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        text: str,
    ) -> None:
        if not (self.config.has_admin_channel and text):
            return
        try:
            await context.bot.send_message(chat_id=self.config.admin_chat_target, text=text)
        except Exception as exc:
            logger.warning("Failed to notify admin: %s", exc)

    async def _sync_snapshot(self, snapshot: ConversationSnapshot) -> None:
        await asyncio.to_thread(self.lead_sync.sync_snapshot, snapshot)

    def _display_name(self, update: Update) -> str:
        user = update.effective_user
        if user is None:
            return ""
        full_name = " ".join(part for part in [user.first_name, user.last_name] if part).strip()
        return full_name or user.username or str(user.id)

    def _is_admin(self, update: Update) -> bool:
        return bool(
            self.config.has_admin
            and update.effective_user
            and update.effective_user.id == self.config.admin_user_id
        )

    def _extract_conversation_id(self, text: str) -> int | None:
        match = CONVERSATION_RE.search(text or "")
        if not match:
            return None
        return int(match.group(1))

    def _command_conversation_id(self, args: list[str]) -> int | None:
        if not args:
            return None
        try:
            return int(args[0])
        except ValueError:
            return None

    def _operator_help(self) -> str:
        return (
            "Режим оператора LesDal AI Sales.\n\n"
            "Команды:\n"
            "/inbox - последние диалоги\n"
            "/lead <id> - summary по диалогу\n"
            "/manager <id> - перевести диалог в ручной режим\n"
            "/ai <id> - вернуть AI в диалог\n\n"
            "Чтобы ответить клиенту вручную, используйте Reply на уведомление с [conv:id]."
        )

    def _target_chat_id(self, raw_chat_id: str) -> int | str:
        return int(raw_chat_id) if raw_chat_id.lstrip("-").isdigit() else raw_chat_id


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    TelegramSalesBot().run()
