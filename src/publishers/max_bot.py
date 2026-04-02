# -*- coding: utf-8 -*-
"""
max_bot.py - publisher for MAX messenger.

Sends direct POST requests to platform-api.max.ru without external SDK.
Token and channel id are loaded from .env in project root.

Required .env keys:
    MAX_BOT_TOKEN=<max bot token>
    MAX_CHANNEL_ID=<numeric channel id, for example -72708340848674>
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import requests

logger = logging.getLogger("lesdal.max")

BASE_URL = "https://platform-api.max.ru/messages"
UPLOADS_URL = "https://platform-api.max.ru/uploads"
ATTACHMENT_RETRIES = 3


def _log_http_error(exc: requests.exceptions.RequestException, prefix: str) -> None:
    resp_text = exc.response.text if getattr(exc, "response", None) is not None else ""
    logger.error(
        "%s: %s%s",
        prefix,
        exc,
        f" | server response: {resp_text}" if resp_text else "",
    )


def _upload_image_and_get_payload(
    *,
    token: str,
    image_path: Path,
) -> dict | None:
    """
    Upload image to MAX and return payload for message attachment.

    Flow by MAX API docs:
    1) POST /uploads?type=image -> receives upload URL (and sometimes token)
    2) POST <upload_url> multipart field 'data' with file bytes
    3) Upload response JSON becomes attachment payload
    """
    auth_headers = {"Authorization": token}

    # Step 1: get upload URL.
    try:
        init_resp = requests.post(
            UPLOADS_URL,
            params={"type": "image"},
            headers=auth_headers,
            timeout=30,
        )
        init_resp.raise_for_status()
        init_json = init_resp.json()
    except requests.exceptions.RequestException as exc:
        _log_http_error(exc, "MAX image upload init failed")
        return None
    except ValueError:
        logger.error("MAX image upload init returned invalid JSON.")
        return None

    upload_url = str(init_json.get("url", "")).strip()
    if not upload_url:
        logger.error("MAX image upload init did not return upload URL. Response: %s", init_json)
        return None

    # Step 2: upload file bytes.
    content_type = "image/png" if image_path.suffix.lower() == ".png" else "image/jpeg"
    try:
        with image_path.open("rb") as file_obj:
            upload_resp = requests.post(
                upload_url,
                headers=auth_headers,
                files={"data": (image_path.name, file_obj, content_type)},
                timeout=60,
            )
        upload_resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        _log_http_error(exc, "MAX image file upload failed")
        return None

    # Step 3: derive attachment payload.
    upload_payload: dict = {}
    try:
        body = upload_resp.json()
        if isinstance(body, dict):
            upload_payload = body
    except ValueError:
        upload_payload = {}

    # Sometimes token can come from init response, sometimes from upload response.
    if "token" not in upload_payload and "token" in init_json:
        upload_payload["token"] = init_json["token"]

    if not upload_payload:
        logger.error(
            "MAX upload succeeded but payload is empty. "
            "Cannot build image attachment."
        )
        return None

    return upload_payload


def _send_message(
    *,
    token: str,
    chat_id_int: int,
    text: str | None = None,
    attachments: list[dict] | None = None,
) -> requests.Response:
    auth_headers = {"Authorization": token, "Content-Type": "application/json"}
    body: dict = {}
    if text is not None:
        body["text"] = text
    if attachments is not None:
        body["attachments"] = attachments

    return requests.post(
        BASE_URL,
        params={"chat_id": chat_id_int},
        headers=auth_headers,
        json=body,
        timeout=30,
    )


def _send_image_message_with_retry(
    *,
    token: str,
    chat_id_int: int,
    image_payload: dict,
) -> bool:
    """
    Send image attachment and retry when MAX returns attachment.not.ready.
    """
    attachments = [{"type": "image", "payload": image_payload}]
    delay_sec = 1.5

    for attempt in range(1, ATTACHMENT_RETRIES + 1):
        try:
            response = _send_message(
                token=token,
                chat_id_int=chat_id_int,
                text="",
                attachments=attachments,
            )
        except requests.exceptions.RequestException as exc:
            _log_http_error(exc, f"MAX image message send failed (attempt {attempt}/{ATTACHMENT_RETRIES})")
            if attempt < ATTACHMENT_RETRIES:
                time.sleep(delay_sec)
                delay_sec *= 2
            continue

        if response.ok:
            return True

        # Parse structured API error when available.
        code = ""
        message = response.text
        try:
            json_body = response.json()
            if isinstance(json_body, dict):
                code = str(json_body.get("code", "")).strip()
                if json_body.get("message"):
                    message = str(json_body["message"])
        except ValueError:
            pass

        if code == "attachment.not.ready" and attempt < ATTACHMENT_RETRIES:
            logger.warning(
                "MAX attachment is not ready yet (attempt %d/%d). Retrying in %.1fs ...",
                attempt,
                ATTACHMENT_RETRIES,
                delay_sec,
            )
            time.sleep(delay_sec)
            delay_sec *= 2
            continue

        logger.warning(
            "MAX image message rejected (%s): %s",
            response.status_code,
            message,
        )
        return False

    return False


def publish_to_max(text: str, image_path: str | Path | None = None) -> bool:
    """
    Publish post to MAX channel.

    chat_id is passed as query parameter (?chat_id=...), while body keeps message
    payload fields (text/files).
    """
    token = os.getenv("MAX_BOT_TOKEN", "").strip()
    chat_id = os.getenv("MAX_CHANNEL_ID", "").strip()

    if not token or not chat_id:
        logger.info("[SKIP] MAX publish skipped: MAX_BOT_TOKEN or MAX_CHANNEL_ID missing in .env")
        return False

    try:
        chat_id_int = int(chat_id)
    except ValueError:
        logger.error("MAX_CHANNEL_ID='%s' is not a number. Check .env.", chat_id)
        return False

    # Step 1: optional image upload + image message.
    if image_path:
        img = Path(image_path)
        if img.is_file():
            image_payload = _upload_image_and_get_payload(token=token, image_path=img)
            if image_payload:
                if _send_image_message_with_retry(
                    token=token,
                    chat_id_int=chat_id_int,
                    image_payload=image_payload,
                ):
                    logger.info("MAX image sent successfully.")
                else:
                    logger.warning("MAX image was uploaded, but message with attachment was not sent.")
            else:
                logger.warning("MAX image upload stage failed. Sending text only.")
        else:
            logger.warning("MAX image file not found: %s. Sending text only.", image_path)

    # Step 2: text message.
    try:
        response = _send_message(
            token=token,
            chat_id_int=chat_id_int,
            text=text,
        )
        response.raise_for_status()
        logger.info("MAX text sent successfully (%d chars).", len(text))
        return True
    except requests.exceptions.RequestException as exc:
        _log_http_error(exc, "MAX text send failed")
        return False
