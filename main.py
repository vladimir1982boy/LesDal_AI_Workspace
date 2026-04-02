# -*- coding: utf-8 -*-
"""
main.py — точка входа пайплайна LesDal Content Pipeline.

Что происходит при запуске:
  1. FETCH   — google_alerts.py читает RSS Google Alerts, возвращает список новых статей.
  2. PROCESS — content_manager.py пишет экспертный пост для Telegram (Gemini).
  3. PUBLISH — telegram_bot.py отправляет текст в канал @lesdal_ru.

Генерация изображений: включена. По умолчанию используется Gemini Image Generation,
при необходимости можно переключить провайдер на Imagen или Pollinations.

Запуск:
  python main.py                         # до 3 новых постов
  python main.py --max-posts 1           # ровно 1 пост
  python main.py --no-image              # флаг сохранён для совместимости (картинки уже выключены)
  python main.py --dry-run               # всё посчитать, но НЕ отправлять в канал
  python main.py --rss "https://..."     # передать RSS напрямую (можно несколько раз)
  python main.py --delay 30              # пауза 30 сек между постами при batch-режиме

Зависимости: requirements.txt в корне проекта.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from pathlib import Path

from google import genai

# ── внутренние модули ──────────────────────────────────────────────────────
from src.fetchers.google_alerts import (
    DEFAULT_CACHE_FILE,
    DEFAULT_URLS_FILE,
    entry_uid,
    load_cache,
    load_urls,
    parse_feed,
    save_cache,
    sha1,
)
from src.ai_sales_bot.config import load_project_env
from src.processors.content_manager import (
    DEFAULT_BUSINESS_RULES,
    DEFAULT_IMAGE_MODEL,
    DEFAULT_IMAGE_PROVIDER,
    DEFAULT_OUT_DIR,
    DEFAULT_TEXT_MODEL,
    IrrelevantNewsError,
    build_system_prompt,
    generate_and_save_image,
    generate_image_prompt,
    generate_telegram_post,
    load_business_context,
)
from src.publishers.max_bot import publish_to_max
from src.publishers.telegram_bot import send_post
from src.publishers.vk_bot import publish_to_vk

# ── конфигурация логов ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lesdal.main")

PROJECT_ROOT = Path(__file__).resolve().parent

# Шаг-теги для визуального разделения в консоли
S_FETCH   = "🔍 [FETCH]"
S_AI      = "🤖 [AI]"
S_IMAGE   = "🎨 [IMAGE]"
S_PUBLISH = "📤 [PUBLISH]"
S_MAX     = "💬 [MAX]"
S_OK      = "✅"
S_SKIP    = "⏭  [SKIP]"
S_FAIL    = "❌ [ERROR]"
S_DONE    = "🏁 [DONE]"


# ──────────────────────────────────────────────────────────────────────────
# Сбор новых статей из RSS (обёртка над google_alerts)
# ──────────────────────────────────────────────────────────────────────────

def fetch_new_articles(
    urls: list[str],
    cache_file: Path,
    limit_entries: int = 80,
) -> tuple[list[dict], dict[str, list[str]]]:
    """
    Читает все RSS-ленты и возвращает:
      - articles: список {'title': str, 'url': str, 'feed_url': str, 'uid': str}
      - feeds_seen: текущий кэш (dict url_hash → list[uid])
    Кэш пока НЕ сохраняется на диск — main.py сохранит его после успешной отправки.
    """
    feeds_seen = load_cache(cache_file)
    articles: list[dict] = []

    for feed_url in urls:
        url_hash = sha1(feed_url)
        seen_set = set(feeds_seen.get(url_hash, []))

        log.info("%s %s", S_FETCH, feed_url[:80])
        try:
            entries, diag = parse_feed(feed_url, limit_entries=limit_entries)
        except Exception as exc:
            log.warning("%s Не удалось прочитать ленту: %s — %s", S_FAIL, feed_url[:60], exc)
            continue

        new_in_feed = 0
        for entry in entries:
            uid = entry_uid(entry)
            if not uid or uid in seen_set:
                continue

            title = entry.get("title") or "(без заголовка)"
            link  = entry.get("link")  or uid
            if not isinstance(title, str):
                title = str(title)
            if not isinstance(link, str):
                link = str(link)

            articles.append({
                "title":    title.strip(),
                "url":      link.strip(),
                "feed_url": feed_url,
                "uid":      uid,
                "url_hash": url_hash,
            })
            seen_set.add(uid)
            new_in_feed += 1

        if new_in_feed:
            feeds_seen[url_hash] = list(seen_set)
            log.info("%s  ↳ найдено новых: %d", S_FETCH, new_in_feed)
        else:
            log.info("%s  ↳ новых нет", S_FETCH)

    return articles, feeds_seen


# ──────────────────────────────────────────────────────────────────────────
# Пайплайн одной статьи
# ──────────────────────────────────────────────────────────────────────────

async def process_article(
    article: dict,
    *,
    gemini_client: genai.Client,
    system_prompt: str,
    text_model: str,
    image_model: str,
    image_provider: str,
    out_dir: Path,
    no_image: bool,
    dry_run: bool,
) -> str:
    """
    Обрабатывает одну статью: генерирует пост + картинку, отправляет в Telegram.
    Возвращает одно из значений: "published", "skipped", "failed".
    """
    title = article["title"]
    url   = article["url"]

    sep = "─" * 60
    log.info("\n%s", sep)
    log.info("%s  %s", S_AI, title[:80])

    # ── шаг 1: текст поста ─────────────────────────────────────────────
    try:
        log.info("%s  Генерирую текст поста (Gemini) ...", S_AI)
        post_text = generate_telegram_post(
            gemini_client, text_model, system_prompt, title, url
        )
    except IrrelevantNewsError:
        log.info("%s  Новость нерелевантна для канала — пропускаю.", S_SKIP)
        return "skipped"
    except Exception as exc:
        log.error("%s  Не удалось сгенерировать пост: %s", S_FAIL, exc)
        return "failed"

    log.info("%s  Пост готов (%d символов).", S_OK, len(post_text))
    if dry_run:
        log.info("\n--- Пост (dry-run preview) ---\n%s\n---", post_text)

    # ── шаг 2: изображение ───────────────────────────────────────────────
    image_path: Path | None = None

    if no_image:
        log.info("%s  Пропущено (--no-image).", S_IMAGE)
    else:
        try:
            log.info("%s  Составляю промпт для генерации картинки ...", S_IMAGE)
            img_prompt = generate_image_prompt(
                gemini_client, text_model, title, post_text
            )
            log.info("%s  Промпт: %s", S_IMAGE, img_prompt[:120])

            if not dry_run:
                log.info(
                    "%s  Генерирую изображение через %s (%s) ...",
                    S_IMAGE,
                    image_provider,
                    image_model,
                )
                image_path = generate_and_save_image(
                    gemini_client,
                    image_model,
                    img_prompt,
                    out_dir,
                    stem=title,
                    image_provider=image_provider,
                )
                log.info("%s  Изображение сохранено: %s", S_OK, image_path)
            else:
                log.info("%s  (dry-run — генерация картинки пропущена)", S_IMAGE)

        except Exception as exc:
            log.warning(
                "%s  Не удалось сгенерировать картинку: %s\n"
                "     Публикую пост без изображения.",
                S_FAIL, exc,
            )
            image_path = None  # продолжаем без картинки

    # ── шаг 3: публикация в Telegram ───────────────────────────────────
    if dry_run:
        log.info("%s  (dry-run) Публикация пропущена. Всё выглядит хорошо!", S_SKIP)
        return "published"

    log.info("%s  Отправляю в Telegram ...", S_PUBLISH)
    try:
        ok = await send_post(caption=post_text, image_path=image_path)
    except RuntimeError as exc:
        log.error("%s  Ошибка конфигурации Telegram: %s", S_FAIL, exc)
        return "failed"

    if ok:
        log.info("%s  Пост опубликован в канале.", S_OK)
    else:
        log.error("%s  Telegram вернул ошибку — пост НЕ опубликован.", S_FAIL)

    # Step 4: publish to MAX messenger (best-effort, does not affect Telegram result)
    log.info("%s  Отправляю в мессенджер Макс ...", S_MAX)
    max_ok = publish_to_max(text=post_text, image_path=image_path)
    if max_ok:
        log.info("%s  Пост опубликован в Максе.", S_OK)
    else:
        log.warning("%s  Макс: публикация не удалась (пост в Telegram уже отправлен).", S_FAIL)

    log.info("[VK] Sending post to VK ...")
    vk_ok = publish_to_vk(text=post_text, image_path=image_path)
    if vk_ok:
        log.info("%s  VK post published.", S_OK)
    else:
        log.warning("%s  VK publish failed (Telegram/MAX already processed).", S_FAIL)

    return "published" if ok else "failed"


# ──────────────────────────────────────────────────────────────────────────
# Основная async-функция
# ──────────────────────────────────────────────────────────────────────────

async def run_pipeline(args: argparse.Namespace) -> int:
    # Загружаем secrets/.env.local или локальные fallback-файлы
    load_project_env()

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        log.error(
            "%s GEMINI_API_KEY не найден. "
            "Добавьте ключ в secrets/.env.local.",
            S_FAIL,
        )
        return 2

    gemini_client = genai.Client(api_key=api_key)

    # Контекст бренда ───────────────────────────────────────────────────
    business_md = load_business_context(DEFAULT_BUSINESS_RULES)
    if business_md:
        log.info("📄  business.mdc загружен (%d символов).", len(business_md))
    else:
        log.warning("⚠️  business.mdc не найден — генерация без контекста бренда.")

    system_prompt = build_system_prompt(business_md)

    # RSS-ссылки ────────────────────────────────────────────────────────
    urls = load_urls(DEFAULT_URLS_FILE, args.rss)
    if not urls:
        log.error(
            "%s RSS-ссылки не найдены.\n"
            "  Добавьте их в: %s\n"
            "  Или передайте через: --rss https://...",
            S_FAIL,
            DEFAULT_URLS_FILE,
        )
        return 2

    log.info("%s  Лент для проверки: %d", S_FETCH, len(urls))

    # Шаг 1: FETCH ──────────────────────────────────────────────────────
    articles, feeds_seen = fetch_new_articles(
        urls,
        cache_file=DEFAULT_CACHE_FILE,
        limit_entries=args.limit_entries,
    )

    if not articles:
        log.info("%s  Новых статей не найдено. Канал LesDal актуален. 👍", S_DONE)
        return 0

    log.info("\n%s  Всего новых статей: %d | Обработаю: %d",
             S_FETCH, len(articles), min(len(articles), args.max_posts))

    if args.dry_run:
        log.info("🧪  Режим DRY-RUN: посты не будут отправлены в канал.\n")

    # Шаг 2-3: PROCESS + PUBLISH каждой статьи ─────────────────────────
    published   = 0
    failed      = 0
    skipped     = 0
    uids_to_save: list[tuple[str, str]] = []   # (url_hash, uid) успешных

    for article in articles[: args.max_posts]:
        status = await process_article(
            article,
            gemini_client=gemini_client,
            system_prompt=system_prompt,
            text_model=args.text_model,
            image_model=args.image_model,
            image_provider=args.image_provider,
            out_dir=DEFAULT_OUT_DIR,
            no_image=args.no_image,
            dry_run=args.dry_run,
        )

        if status == "published":
            published += 1
            if not args.dry_run:
                uids_to_save.append((article["url_hash"], article["uid"]))
        elif status == "skipped":
            skipped += 1
            if not args.dry_run:
                uids_to_save.append((article["url_hash"], article["uid"]))
        else:
            failed += 1

        # Задержка между постами (только если ещё есть следующие)
        remaining = min(len(articles), args.max_posts) - published - failed - skipped
        if remaining > 0 and args.delay > 0:
            log.info("⏳  Пауза %d сек перед следующим постом ...", args.delay)
            await asyncio.sleep(args.delay)

    # Сохраняем кэш только для реально опубликованных ───────────────────
    if uids_to_save:
        updated_cache = load_cache(DEFAULT_CACHE_FILE)
        for url_hash, uid in uids_to_save:
            bucket = set(updated_cache.get(url_hash, []))
            bucket.add(uid)
            updated_cache[url_hash] = list(bucket)
        save_cache(DEFAULT_CACHE_FILE, updated_cache)
        log.info("💾  Кэш обновлён (%d записей).", len(uids_to_save))

    # Итоги ─────────────────────────────────────────────────────────────
    log.info("\n%s  Пайплайн завершён.", S_DONE)
    log.info("    Опубликовано: %d  |  Ошибок: %d  |  Пропущено: %d",
             published, failed, skipped + (len(articles) - min(len(articles), args.max_posts)))

    return 0 if failed == 0 else 1


# ──────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="main.py",
        description="LesDal Content Pipeline: RSS → Gemini → Telegram + MAX + VK",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Примеры:\n"
            "  python main.py                     # стандартный запуск\n"
            "  python main.py --max-posts 1       # один пост за запуск\n"
            "  python main.py --dry-run           # тест без отправки в Telegram\n"
            '  python main.py --rss "https://..."  # использовать конкретный RSS\n'
            "  python main.py --delay 60           # 60 сек между постами"
        ),
    )
    p.add_argument(
        "--max-posts", type=int, default=3, metavar="N",
        help="Максимум постов за один запуск (по умолчанию 3).",
    )
    p.add_argument(
        "--rss", action="append", default=[], metavar="URL",
        help="RSS URL (можно указать несколько раз). По умолчанию из rss_urls_google_alerts.txt.",
    )
    p.add_argument(
        "--no-image", action="store_true",
        help="Не генерировать картинку — публиковать только текст.",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Сгенерировать посты, но НЕ отправлять в Telegram и НЕ обновлять кэш.",
    )
    p.add_argument(
        "--delay", type=int, default=10, metavar="SEC",
        help="Пауза (сек) между публикациями при нескольких постах (по умолчанию 10).",
    )
    p.add_argument(
        "--text-model", default=DEFAULT_TEXT_MODEL,
        help=f"Модель Gemini для текста (по умолчанию {DEFAULT_TEXT_MODEL}).",
    )
    p.add_argument(
        "--image-model", default=DEFAULT_IMAGE_MODEL,
        help=f"Модель генерации картинки (по умолчанию {DEFAULT_IMAGE_MODEL}).",
    )
    p.add_argument(
        "--image-provider", default=DEFAULT_IMAGE_PROVIDER,
        choices=("gemini", "imagen", "pollinations"),
        help=f"Провайдер картинки (по умолчанию {DEFAULT_IMAGE_PROVIDER}).",
    )
    p.add_argument(
        "--limit-entries", type=int, default=80,
        help="Сколько записей загружать из каждой RSS-ленты (по умолчанию 80).",
    )
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    raise SystemExit(main())
