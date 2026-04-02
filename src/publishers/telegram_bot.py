# -*- coding: utf-8 -*-
"""
telegram_bot.py — публикатор поста в Telegram-канал LesDal.

Отправляет фото + caption (или только текст) в канал.
Токен бота и ID канала берёт из .env в корне проекта.

Использование в пайплайне (async):
    from src.publishers.telegram_bot import send_post
    import asyncio
    asyncio.run(send_post(image_path="out/generated/img.png", caption="Текст поста"))

Тест из командной строки:
    # с картинкой:
    python src/publishers/telegram_bot.py --image "out/generated/img.png" --caption "Текст"
    # только текст:
    python src/publishers/telegram_bot.py --text-only --caption "Текст без картинки"

Зависимости (см. requirements.txt):
    python-telegram-bot>=20.7
    python-dotenv>=1.0.0
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
from pathlib import Path

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import (
    BadRequest,
    ChatMigrated,
    Forbidden,
    NetworkError,
    TelegramError,
    TimedOut,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

from src.ai_sales_bot.config import load_project_env

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("lesdal.telegram")

TELEGRAM_CAPTION_LIMIT = 1024   # жёсткий лимит Telegram на подпись к фото
LONG_TEXT_THRESHOLD    = 1000   # если текст длиннее — отправляем фото и текст двумя сообщениями
TELEGRAM_CONNECT_TIMEOUT = 20
TELEGRAM_WRITE_TIMEOUT = 60
TELEGRAM_READ_TIMEOUT = 60
TELEGRAM_POOL_TIMEOUT = 20
TELEGRAM_RETRIES = 2


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def _resolve_channel_id(raw: str) -> str:
    """
    Парсит TELEGRAM_CHANNEL_ID из .env.
    Возможные форматы в .env:
        @lesdal_ru
        -1001234567890
        3703117021 @lesdal_ru      ← оба значения в одной строке
        3703117021

    Приоритет: @username > числовой ID.
    Для числового ID без минуса добавляет префикс -100 (supergroup/channel).
    """
    raw = raw.strip()
    # Ищем @username
    m = re.search(r"(@[\w]+)", raw)
    if m:
        return m.group(1)

    # Ищем числовой ID (берём первую группу цифр)
    m_num = re.search(r"-?\d{7,}", raw)
    if m_num:
        num = m_num.group(0)
        if num.startswith("-"):
            return num
        # Telegram каналы: полный chat_id = -100 + id
        return f"-100{num}"

    raise ValueError(
        f"Не удалось распознать TELEGRAM_CHANNEL_ID: {raw!r}\n"
        "Ожидается @username или числовой ID."
    )


def _truncate_caption(text: str) -> str:
    """Обрезает подпись до лимита API. Используется только как страховка."""
    text = text.strip()
    if len(text) <= TELEGRAM_CAPTION_LIMIT:
        return text
    return text[: TELEGRAM_CAPTION_LIMIT - 1].rstrip() + "…"


async def _call_with_retry(api_call, action: str, /, **kwargs):
    """
    Выполняет вызов Telegram API с увеличенными таймаутами и повторной попыткой
    при сетевом таймауте.
    """
    last_exc = None
    for attempt in range(1, TELEGRAM_RETRIES + 1):
        try:
            return await api_call(
                **kwargs,
                read_timeout=TELEGRAM_READ_TIMEOUT,
                write_timeout=TELEGRAM_WRITE_TIMEOUT,
                connect_timeout=TELEGRAM_CONNECT_TIMEOUT,
                pool_timeout=TELEGRAM_POOL_TIMEOUT,
            )
        except (TimedOut, NetworkError) as exc:
            last_exc = exc
            if attempt >= TELEGRAM_RETRIES:
                break
            logger.warning(
                "%s: попытка %d/%d не удалась (%s). Повторяю ...",
                action,
                attempt,
                TELEGRAM_RETRIES,
                exc,
            )
            await asyncio.sleep(2 * attempt)

    raise last_exc


# ---------------------------------------------------------------------------
# Основная функция отправки
# ---------------------------------------------------------------------------

async def send_post(
    caption: str,
    *,
    image_path: str | Path | None = None,
    token: str | None = None,
    channel_id: str | None = None,
    parse_mode: str = ParseMode.HTML,
    disable_notification: bool = False,
) -> bool:
    """
    Отправляет пост в Telegram-канал.

    Args:
        caption:              Текст поста (HTML или обычный текст).
        image_path:           Путь к PNG/JPG. Если None — отправится только текст.
        token:                Bot API token. По умолчанию из .env.
        channel_id:           ID канала. По умолчанию из .env.
        parse_mode:           ParseMode.HTML (по умолчанию) или ParseMode.MARKDOWN_V2.
        disable_notification: Тихая отправка без звука.

    Returns:
        True при успехе, False при обработанной ошибке.

    Raises:
        RuntimeError: Если токен или channel_id не заданы ни здесь, ни в .env.
    """
    load_project_env()

    bot_token = token or os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    raw_channel = channel_id or os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()

    if not bot_token:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN не найден. "
            "Добавьте его в .env или передайте аргументом token=."
        )
    if not raw_channel:
        raise RuntimeError(
            "TELEGRAM_CHANNEL_ID не найден. "
            "Добавьте его в .env или передайте аргументом channel_id=."
        )

    chat_id = _resolve_channel_id(raw_channel)
    text = caption.strip()  # текст отправляется целиком, без обрезки

    logger.info("Отправка в канал %s (%d символов) ...", chat_id, len(text))

    async with Bot(token=bot_token) as bot:
        try:
            if image_path:
                img = Path(image_path)
                if not img.is_file():
                    logger.error("Файл изображения не найден: %s", img)
                    return False

                img_bytes = img.read_bytes()

                if len(text) > LONG_TEXT_THRESHOLD:
                    # Текст длиннее 1000 символов — Telegram не разрешает такую подпись.
                    # Отправляем картинку без подписи, затем текст отдельным сообщением.
                    await _call_with_retry(
                        bot.send_photo,
                        "send_photo",
                        chat_id=chat_id,
                        photo=img_bytes,
                        disable_notification=disable_notification,
                    )
                    await _call_with_retry(
                        bot.send_message,
                        "send_message",
                        chat_id=chat_id,
                        text=text,
                        parse_mode=parse_mode,
                        disable_notification=disable_notification,
                        disable_web_page_preview=False,
                    )
                    logger.info(
                        "Длинный пост: фото + текст отправлены двумя сообщениями (%d символов).",
                        len(text),
                    )
                else:
                    # Короткий текст — обычная подпись к фото.
                    await _call_with_retry(
                        bot.send_photo,
                        "send_photo",
                        chat_id=chat_id,
                        photo=img_bytes,
                        caption=text,
                        parse_mode=parse_mode,
                        disable_notification=disable_notification,
                    )
                    logger.info("Фото + caption отправлены (%d символов).", len(text))
            else:
                await _call_with_retry(
                    bot.send_message,
                    "send_message",
                    chat_id=chat_id,
                    text=text,
                    parse_mode=parse_mode,
                    disable_notification=disable_notification,
                    disable_web_page_preview=False,
                )
                logger.info("Текст отправлен (%d символов).", len(text))

            return True

        # --- обработка конкретных ошибок ---

        except Forbidden as exc:
            logger.error(
                "Нет доступа к каналу %s. "
                "Убедитесь, что бот добавлен как администратор с правом «Публикация сообщений». "
                "Детали: %s",
                chat_id, exc,
            )

        except BadRequest as exc:
            msg = str(exc).lower()
            if "chat not found" in msg:
                logger.error(
                    "Канал %s не найден. "
                    "Проверьте TELEGRAM_CHANNEL_ID в .env. "
                    "Для приватных каналов нужен числовой ID (-100...).",
                    chat_id,
                )
            elif "caption is too long" in msg:
                logger.error(
                    "Caption слишком длинный (%d символов) — неожиданно, логика разделения должна была сработать.",
                    len(text),
                )
            elif "photo_invalid_dimensions" in msg or "wrong file" in msg.replace("_", " "):
                logger.error("Изображение повреждено или неподдерживаемый формат: %s", image_path)
            else:
                logger.error("Ошибка запроса к Telegram API: %s", exc)

        except ChatMigrated as exc:
            # Группа была преобразована в супергруппу — ID поменялся
            logger.error(
                "Чат мигрировал. Новый chat_id: %s. "
                "Обновите TELEGRAM_CHANNEL_ID в .env.",
                exc.new_chat_id,
            )

        except TimedOut:
            logger.error(
                "Превышено время ожидания ответа Telegram. "
                "Проверьте соединение и попробуйте снова."
            )

        except NetworkError as exc:
            logger.error("Сетевая ошибка: %s. Проверьте интернет-соединение.", exc)

        except TelegramError as exc:
            # Ловим все прочие ошибки Telegram
            logger.error("Неожиданная ошибка Telegram: %s", exc)

    return False


# ---------------------------------------------------------------------------
# CLI для ручного теста
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Отправить пост в Telegram-канал LesDal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Примеры:\n"
            "  python src/publishers/telegram_bot.py "
            '--image "out/generated/img.png" --caption "Текст"\n'
            "  python src/publishers/telegram_bot.py "
            '--text-only --caption "Только текст"'
        ),
    )
    p.add_argument("--image", type=Path, default=None, help="Путь к изображению (PNG/JPG)")
    p.add_argument("--caption", required=True, help="Текст поста (до 1024 символов)")
    p.add_argument(
        "--text-only",
        action="store_true",
        help="Отправить только текст (без изображения)",
    )
    p.add_argument(
        "--silent",
        action="store_true",
        help="Тихая отправка (без звука уведомления)",
    )
    p.add_argument(
        "--channel-id",
        default=None,
        help="Переопределить канал (по умолчанию из .env)",
    )
    return p


async def _async_main(args: argparse.Namespace) -> int:
    img = None if args.text_only else args.image
    ok = await send_post(
        caption=args.caption,
        image_path=img,
        channel_id=args.channel_id,
        disable_notification=args.silent,
    )
    return 0 if ok else 1


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if not args.text_only and args.image is None:
        parser.error("Укажите --image <путь> или флаг --text-only.")

    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
