from __future__ import annotations

import logging
import re


NOISY_NETWORK_LOGGERS = (
    "httpx",
    "httpcore",
    "telegram",
    "telegram.ext",
    "urllib3",
)

TELEGRAM_TOKEN_RE = re.compile(r"(api\.telegram\.org/bot)[^/\s\"']+")


def _redact_telegram_token(value: object) -> object:
    if isinstance(value, str):
        return TELEGRAM_TOKEN_RE.sub(r"\1<redacted>", value)
    return value


class TelegramTokenRedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = _redact_telegram_token(record.msg)
        if isinstance(record.args, tuple):
            record.args = tuple(_redact_telegram_token(arg) for arg in record.args)
        elif isinstance(record.args, dict):
            record.args = {key: _redact_telegram_token(value) for key, value in record.args.items()}
        return True


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    redaction_filter = TelegramTokenRedactionFilter()
    for handler in logging.getLogger().handlers:
        handler.addFilter(redaction_filter)
    for logger_name in NOISY_NETWORK_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
