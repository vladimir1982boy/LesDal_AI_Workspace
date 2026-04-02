# -*- coding: utf-8 -*-
"""
Генерация короткого поста для Telegram по новости + иллюстрация.

Текст:     Google Gemini (gemini-2.5-flash по умолчанию) через google-genai SDK.
Картинки:  По умолчанию Gemini Image Generation (gemini-2.5-flash-image).
           Опционально можно использовать Imagen 3 через тот же SDK или
           резервный Pollinations.ai.

Контекст бренда: читает `.cursor/rules/business.mdc` в корне проекта.

Пример:
  python src/processors/content_manager.py --title "..." --url "https://..."
  python src/processors/content_manager.py --title "..." --url "..." --no-image
  python src/processors/content_manager.py --title "..." --url "..." --text-model gemini-1.5-pro

Зависимости: см. requirements.txt в корне проекта.
"""

from __future__ import annotations

import argparse
import base64
import os
import re
import sys
import urllib.parse

import requests
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.genai import types

PROJECT_ROOT = Path(__file__).resolve().parents[2]
from src.ai_sales_bot.config import load_project_env
DEFAULT_BUSINESS_RULES = PROJECT_ROOT / ".cursor" / "rules" / "business.mdc"
DEFAULT_OUT_DIR = PROJECT_ROOT / "out" / "generated"
TELEGRAM_MAX_CHARS = 1000
IRRELEVANT_NEWS_MARKER = "__SKIP_IRRELEVANT_NEWS__"

# Модель для генерации текста
DEFAULT_TEXT_MODEL = "gemini-2.5-flash"

# Провайдер и модель по умолчанию для генерации изображений.
DEFAULT_IMAGE_PROVIDER = "gemini"
DEFAULT_IMAGE_MODEL = "gemini-2.5-flash-image"


class IrrelevantNewsError(RuntimeError):
    """Raised when a news item is outside the channel topic and must be skipped."""


# ──────────────────────────────────────────────────────────────────────────
# Вспомогательные функции
# ──────────────────────────────────────────────────────────────────────────

def load_business_context(rules_path: Path, max_chars: int = 12000) -> str:
    if not rules_path.is_file():
        return ""
    raw = rules_path.read_text(encoding="utf-8", errors="replace").strip()
    if len(raw) > max_chars:
        return raw[:max_chars] + "\n\n[…фрагмент обрезан по длине…]"
    return raw


def build_system_prompt(business_md: str) -> str:
    return (
        "Напиши экспертный пост для Telegram-канала «ЛесДал» (биохакинг, здоровье от природы, ноотропы).\n\n"
        "ТРЕБОВАНИЯ К ТЕКСТУ:\n"
        "• Длина текста — СТРОГО от 500 до 1000 символов с пробелами.\n"
        "• В этом объёме полностью, логично и глубоко раскрой суть новости.\n"
        "• Структура поста (строго в таком порядке):\n"
        "  1) Цепляющий заголовок с эмодзи;\n"
        "  2) Основная суть исследования / новости — простым языком;\n"
        "  3) Практическая польза для здоровья, фокуса или энергии;\n"
        "  4) Короткий вовлекающий вопрос к подписчикам в конце.\n"
        "• Язык — русский, тон — экспертный и тёплый, без паники.\n"
        "• Никогда не обрывай текст на полуслове или в середине предложения.\n"
        "• Без медицинских обещаний («вылечит», «гарантия», «100%»), без хэштегов.\n"
        "• Последняя строка после вопроса — ссылка на источник.\n\n"
        "--- Контекст бренда (business.mdc) ---\n"
        f"{business_md or '(файл business.mdc не найден — опирайся на нишу ЛесДал)'}\n"
        "--- конец контекста ---"
    )


def build_user_prompt(title: str, url: str) -> str:
    return (
        f"Заголовок новости: {title}\n"
        f"Ссылка на источник: {url}\n\n"
        "Напиши пост строго по инструкции выше. "
        "Объём — от 500 до 1000 символов, текст целиком, без обрыва."
    )


def truncate_telegram_post(text: str, max_chars: int = TELEGRAM_MAX_CHARS) -> str:
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


# ──────────────────────────────────────────────────────────────────────────
# Генерация текста поста
# ──────────────────────────────────────────────────────────────────────────

def generate_telegram_post(
    client: genai.Client,
    model_name: str,
    system_prompt: str,
    title: str,
    url: str,
) -> str:
    response = client.models.generate_content(
        model=model_name,
        contents=build_user_prompt(title, url),
        config=types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=0.7,
        ),
    )
    return (response.text or "").strip()


# ──────────────────────────────────────────────────────────────────────────
# Генерация изображений — Imagen 3 (Nano Banana 2 / Gemini Image Generation)
# Использует тот же client и GEMINI_API_KEY, что и текстовая генерация.
# ──────────────────────────────────────────────────────────────────────────

def _build_image_prompt_request(title: str, post_text: str) -> str:
    """Запрос к текстовой модели: составить англоязычный промпт для Imagen."""
    return (
        "You are an expert at crafting image generation prompts.\n"
        "Based on the news below about biohacking or nootropics, write ONE short, "
        "vivid English prompt for a photorealistic illustration.\n\n"
        f"News title: {title}\n"
        f"Post preview: {post_text[:400]}\n\n"
        "Style requirements:\n"
        "- photorealistic, high quality, cinematic lighting\n"
        "- biohacking aesthetics: nature, plants, mushrooms, brain, energy, focus\n"
        "- clean and calm atmosphere, no clutter\n"
        "- NO text, NO logos, NO real human faces, NO gore\n\n"
        "Reply with ONLY the image prompt as a single line, no explanations."
    )


def generate_image_prompt(
    client: genai.Client,
    text_model: str,
    title: str,
    post_text: str,
) -> str:
    """Просит Gemini составить короткий английский промпт для Imagen."""
    response = client.models.generate_content(
        model=text_model,
        contents=_build_image_prompt_request(title, post_text),
        config=types.GenerateContentConfig(
            temperature=0.8,
        ),
    )
    prompt = (response.text or "").strip()
    # Убираем возможные кавычки, которые модель иногда добавляет
    prompt = prompt.strip('"').strip("'").strip()
    if not prompt:
        raise RuntimeError("Gemini вернул пустой промпт для изображения")
    return prompt


def _build_out_path(out_dir: Path, stem: str, ext: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_stem = re.sub(r"[^\w\-]+", "_", stem, flags=re.UNICODE)[:80] or "image"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return out_dir / f"{safe_stem}_{ts}.{ext}"


def _save_gemini_content_image(
    client: genai.Client,
    image_model: str,
    prompt: str,
    out_dir: Path,
    stem: str,
) -> Path:
    response = client.models.generate_content(
        model=image_model,
        contents=[prompt],
    )

    parts = getattr(response, "parts", None) or []
    for part in parts:
        inline_data = getattr(part, "inline_data", None)
        if not inline_data:
            continue

        mime_type = (getattr(inline_data, "mime_type", "") or "").lower()
        data = getattr(inline_data, "data", None)
        if not data:
            continue

        if isinstance(data, str):
            image_bytes = base64.b64decode(data)
        else:
            image_bytes = data

        ext = "png" if "png" in mime_type else "jpg"
        out_path = _build_out_path(out_dir, stem, ext)
        out_path.write_bytes(image_bytes)
        return out_path

    raise RuntimeError(
        "Gemini не вернул изображение. Проверьте модель image generation "
        "или попробуйте резервный провайдер pollinations."
    )


def _save_imagen_image(
    client: genai.Client,
    image_model: str,
    prompt: str,
    out_dir: Path,
    stem: str,
) -> Path:
    response = client.models.generate_images(
        model=image_model,
        prompt=prompt,
        config=types.GenerateImagesConfig(
            number_of_images=1,
            output_mime_type="image/jpeg",
        ),
    )

    generated_images = getattr(response, "generated_images", None) or []
    if not generated_images:
        raise RuntimeError("Imagen не вернул изображение")

    image = getattr(generated_images[0], "image", None)
    if image is None:
        raise RuntimeError("Imagen вернул ответ без image payload")

    image_bytes = None
    for attr in ("image_bytes", "data"):
        candidate = getattr(image, attr, None)
        if candidate:
            image_bytes = candidate
            break

    if image_bytes is None and hasattr(image, "_pil_image"):
        out_path = _build_out_path(out_dir, stem, "jpg")
        image._pil_image.save(out_path, format="JPEG")
        return out_path

    if image_bytes is None:
        raise RuntimeError("Не удалось извлечь байты изображения из ответа Imagen")

    if isinstance(image_bytes, str):
        image_bytes = base64.b64decode(image_bytes)

    out_path = _build_out_path(out_dir, stem, "jpg")
    out_path.write_bytes(image_bytes)
    return out_path


def _save_pollinations_image(
    prompt: str,
    out_dir: Path,
    stem: str,
) -> Path:
    out_path = _build_out_path(out_dir, stem, "jpg")

    encoded_prompt = urllib.parse.quote(prompt)
    url = (
        f"https://image.pollinations.ai/prompt/{encoded_prompt}"
        "?width=1024&height=1024&nologo=true"
    )

    img_response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        timeout=30,
    )
    img_response.raise_for_status()

    if not img_response.content:
        raise RuntimeError("Pollinations.ai вернул пустой ответ")

    out_path.write_bytes(img_response.content)
    return out_path


def generate_and_save_image(
    client: genai.Client,
    image_model: str,
    prompt: str,
    out_dir: Path,
    stem: str,
    image_provider: str = DEFAULT_IMAGE_PROVIDER,
) -> Path:
    """
    Генерирует изображение и сохраняет его в out_dir.

    Поддерживаемые провайдеры:
    - gemini: text-to-image через generate_content и image-capable Gemini model
    - imagen: Google Imagen через generate_images
    - pollinations: публичный резервный сервис без ключа
    """
    provider = (image_provider or DEFAULT_IMAGE_PROVIDER).strip().lower()

    if provider == "gemini":
        return _save_gemini_content_image(client, image_model, prompt, out_dir, stem)
    if provider == "imagen":
        return _save_imagen_image(client, image_model, prompt, out_dir, stem)
    if provider == "pollinations":
        return _save_pollinations_image(prompt, out_dir, stem)

    raise ValueError(
        f"Неизвестный image provider: {image_provider!r}. "
        "Используйте gemini, imagen или pollinations."
    )


# ──────────────────────────────────────────────────────────────────────────
# CLI — точка входа для ручного запуска
# ──────────────────────────────────────────────────────────────────────────

def _looks_like_irrelevant_news_reply(text: str) -> bool:
    lowered = text.strip().lower()
    refusal_markers = (
        "не соответствует тематике",
        "не соответствует теме канала",
        "не могу выполнить этот запрос",
        "предоставьте новость",
        "благодарю за запрос",
        "создание поста на основе данной новости",
        "противоречит",
    )
    return any(marker in lowered for marker in refusal_markers)


def build_system_prompt(business_md: str) -> str:
    return (
        "Напиши экспертный пост для Telegram-канала «ЛесДал» "
        "(биохакинг, здоровье от природы, ноотропы).\n\n"
        "Сначала оцени релевантность новости тематике канала.\n"
        "Если новость нерелевантна и не связана с биохакингом, нейробиологией, "
        "когнитивными функциями, натуральным восстановлением, сном, энергией, "
        "фокусом, стрессом, полезными исследованиями о здоровье или близкими темами, "
        "то не пиши объяснений, не пиши письмо пользователю и не извиняйся. "
        f"Верни только одну строку: {IRRELEVANT_NEWS_MARKER}\n\n"
        "Требования к тексту:\n"
        "• Длина текста — строго от 500 до 1000 символов с пробелами.\n"
        "• Полностью и логично раскрой суть новости.\n"
        "• Структура: заголовок с эмодзи, суть новости, практическая польза, короткий вопрос в конце.\n"
        "• Язык — русский, тон — экспертный и тёплый, без паники.\n"
        "• Никогда не обрывай текст на полуслове или в середине предложения.\n"
        "• Без медицинских обещаний и без хэштегов.\n"
        "• Последняя строка после вопроса — ссылка на источник.\n\n"
        "--- Контекст бренда (business.mdc) ---\n"
        f"{business_md or '(файл business.mdc не найден — опирайся на нишу ЛесДал)'}\n"
        "--- конец контекста ---"
    )


def generate_telegram_post(
    client: genai.Client,
    model_name: str,
    system_prompt: str,
    title: str,
    url: str,
) -> str:
    response = client.models.generate_content(
        model=model_name,
        contents=build_user_prompt(title, url),
        config=types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=0.7,
        ),
    )
    text = (response.text or "").strip()
    if not text:
        raise RuntimeError("Gemini returned an empty response for post generation")
    if text == IRRELEVANT_NEWS_MARKER or _looks_like_irrelevant_news_reply(text):
        raise IrrelevantNewsError("News item is not relevant to the LesDal channel topic")
    return text


def main() -> int:
    load_project_env()

    parser = argparse.ArgumentParser(
        description="LesDal: пост в Telegram + картинка через Google Gemini / Imagen 3"
    )
    parser.add_argument("--title", required=True, help="Заголовок новости")
    parser.add_argument("--url", required=True, help="URL материала")
    parser.add_argument(
        "--business-rules",
        type=Path,
        default=DEFAULT_BUSINESS_RULES,
        help="Путь к business.mdc",
    )
    parser.add_argument(
        "--text-model",
        default=DEFAULT_TEXT_MODEL,
        help=f"Модель Gemini для текста (по умолчанию {DEFAULT_TEXT_MODEL})",
    )
    parser.add_argument(
        "--image-model",
        default=DEFAULT_IMAGE_MODEL,
        help=f"Модель для генерации картинки (по умолчанию {DEFAULT_IMAGE_MODEL})",
    )
    parser.add_argument(
        "--image-provider",
        default=DEFAULT_IMAGE_PROVIDER,
        choices=("gemini", "imagen", "pollinations"),
        help=f"Провайдер картинки (по умолчанию {DEFAULT_IMAGE_PROVIDER})",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help="Папка для сохранения PNG",
    )
    parser.add_argument(
        "--no-image",
        action="store_true",
        help="Не генерировать изображение, только текст",
    )
    args = parser.parse_args()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key or not api_key.strip():
        print(
            "Нет GEMINI_API_KEY в окружении. Добавьте ключ в secrets/.env.local.",
            file=sys.stderr,
        )
        return 2

    client = genai.Client(api_key=api_key)
    business_md = load_business_context(args.business_rules)
    system_prompt = build_system_prompt(business_md)

    # ── Шаг 1: текст поста ──────────────────────────────────────────────
    print("--- Пост для Telegram ---")
    post = generate_telegram_post(client, args.text_model, system_prompt, args.title, args.url)
    print(post)
    print(f"\n(длина: {len(post)} / {TELEGRAM_MAX_CHARS} символов)\n")

    if args.no_image:
        return 0

    # ── Шаг 2: промпт для картинки ──────────────────────────────────────
    print("--- Промпт для Imagen (англ.) ---")
    try:
        img_prompt = generate_image_prompt(client, args.text_model, args.title, post)
        print(img_prompt)
    except Exception as exc:
        print(f"Не удалось составить промпт для картинки: {exc}", file=sys.stderr)
        return 1

    # ── Шаг 3: генерация изображения ────────────────────────────────────
    print("\n--- Генерация изображения (Imagen 3) ---")
    try:
        path = generate_and_save_image(
            client,
            args.image_model,
            img_prompt,
            args.out_dir,
            stem=args.title,
            image_provider=args.image_provider,
        )
        print(f"Сохранено: {path}")
    except Exception as exc:
        print(f"Не удалось сгенерировать картинку: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
