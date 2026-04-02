"""
google_alerts.py

Читает RSS-ленты Google Alerts и печатает ТОЛЬКО новые статьи
(заголовок + ссылка), которых ещё не было в локальном кэше.

Зависимость:
  pip install feedparser

Запуск (варианты):
  1) С файла со ссылками (по одной на строку):
     python src/fetchers/google_alerts.py

     По умолчанию файл: rss_urls_google_alerts.txt (в корне проекта)

  2) Явно передать ссылки:
     python src/fetchers/google_alerts.py --url "https://..." --url "https://..."
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import feedparser


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_DIR = PROJECT_ROOT / ".cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_URLS_FILE = PROJECT_ROOT / "rss_urls_google_alerts.txt"
DEFAULT_CACHE_FILE = CACHE_DIR / "google_alerts_seen.json"


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def load_urls(urls_file: Path, cli_urls: list[str]) -> list[str]:
    urls: list[str] = []

    # CLI override
    if cli_urls:
        return [u.strip() for u in cli_urls if u.strip()]

    # File-based
    if urls_file.exists():
        for line in urls_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            urls.append(s)

    # Remove duplicates, preserve order
    seen = set()
    uniq: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def load_cache(cache_file: Path) -> dict[str, list[str]]:
    if not cache_file.exists():
        return {}
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            feeds = data.get("feeds", {})
            if isinstance(feeds, dict):
                return feeds
    except Exception:
        pass
    return {}


def save_cache(cache_file: Path, feeds_seen_by_hash: dict[str, list[str]]) -> None:
    payload = {
        "feeds": feeds_seen_by_hash,
        "updated_at": int(time.time()),
    }
    cache_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def entry_uid(entry: dict) -> str | None:
    """
    Уникальный идентификатор записи, чтобы надёжно не повторять вывод.
    В RSS/Atom поля могут отличаться.
    """
    for key in ("id", "guid", "link"):
        v = entry.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def parse_feed(url: str, limit_entries: int = 50) -> tuple[list[dict], str]:
    """
    Возвращает:
      - список entry-объектов (как feedparser их отдаёт)
      - diagnostic строку для логов
    """
    feed = feedparser.parse(url)
    bozo = getattr(feed, "bozo", 0)
    bozo_exception = getattr(feed, "bozo_exception", None)
    diag = f"bozo={bozo}"
    if bozo_exception:
        diag += f", error={bozo_exception}"

    entries = list(getattr(feed, "entries", []) or [])[: max(0, limit_entries)]
    return entries, diag


def main() -> int:
    parser = argparse.ArgumentParser(description="Google Alerts RSS fetcher")
    parser.add_argument(
        "--urls-file",
        type=Path,
        default=DEFAULT_URLS_FILE,
        help="Файл с RSS URL (по одной ссылке на строку)",
    )
    parser.add_argument(
        "--url",
        action="append",
        default=[],
        help="URL RSS (можно повторять --url ... --url ...)",
    )
    parser.add_argument(
        "--cache-file",
        type=Path,
        default=DEFAULT_CACHE_FILE,
        help="Файл кэша увиденных статей",
    )
    parser.add_argument(
        "--limit-entries",
        type=int,
        default=80,
        help="Сколько последних записей учитывать из каждой ленты",
    )
    args = parser.parse_args()

    urls = load_urls(args.urls_file, args.url)
    if not urls:
        print(
            "Не найдено ни одной RSS-ссылки.\n"
            f"Проверьте файл: {args.urls_file}\n"
            "Либо передайте ссылки через --url.",
            file=sys.stderr,
        )
        return 2

    feeds_seen = load_cache(args.cache_file)  # url_hash -> list[uids]

    total_new = 0
    for url in urls:
        url_hash = sha1(url)
        seen_set = set(feeds_seen.get(url_hash, []))

        try:
            entries, diag = parse_feed(url, limit_entries=args.limit_entries)
        except Exception as e:
            print(f"[Ошибка] Не удалось прочитать RSS: {url}\n  {e}", file=sys.stderr)
            continue

        new_items: list[tuple[str, str]] = []
        for entry in entries:
            uid = entry_uid(entry)
            if not uid:
                continue
            if uid in seen_set:
                continue

            title = entry.get("title") or "(без заголовка)"
            link = entry.get("link") or uid
            if not isinstance(title, str):
                title = str(title)
            if not isinstance(link, str):
                link = str(link)

            new_items.append((title.strip(), link.strip()))

            # mark in-memory to avoid duplicates within same run
            seen_set.add(uid)

        # Update cache only if something new found
        if new_items:
            feeds_seen[url_hash] = list(seen_set)
            print(f"\n=== Новые статьи из ленты ===\n{url}")
            for title, link in new_items:
                print(f"- {title}\n  {link}")
            total_new += len(new_items)

        # If nothing new - keep silent to keep output clean.
        # If you want diagnostics, add print(f"[{diag}] ...")

    if total_new == 0:
        # Soft output so the user sees the run was successful.
        print("Новых статей за текущий запуск не найдено.")
    else:
        save_cache(args.cache_file, feeds_seen)

    # Always save cache if it changed (new items) - already done above.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

