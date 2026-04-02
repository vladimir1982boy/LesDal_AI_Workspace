# -*- coding: utf-8 -*-
"""
vk_bot.py - publisher for VK community wall posts.

Publishes text posts and optional images to a VK community wall using direct
HTTP requests to the VK API.

Supported .env keys:
    VK_API_KEY=<VK user access token for wall/photos methods>
    VK_ACCESS_TOKEN=<alias for VK_API_KEY>
    VK_GROUP_ID=<numeric community id, e.g. 123456789>
    VK_GROUP_DOMAIN=<community short name, e.g. lesdal_ru>
    VK_SCREEN_NAME=<alias for VK_GROUP_DOMAIN>
    VK_API_VERSION=<optional, defaults to 5.199>
"""

from __future__ import annotations

import html
import logging
import os
import re
from pathlib import Path

import requests

logger = logging.getLogger("lesdal.vk")

API_URL = "https://api.vk.com/method"
DEFAULT_API_VERSION = "5.199"
REQUEST_TIMEOUT = 30
VK_MESSAGE_SOFT_LIMIT = 4000
URL_RE = re.compile(r"https?://\S+")
HTML_TAG_RE = re.compile(r"<[^>]+>")


class VKAPIError(RuntimeError):
    """Raised when VK API returns a structured error."""


def _mask_token(token: str) -> str:
    token = token.strip()
    if len(token) <= 10:
        return "***"
    return f"{token[:6]}...{token[-4:]}"


def _describe_vk_error(exc: Exception) -> str:
    text = str(exc)
    if "Group authorization failed" in text:
        return (
            f"{text}. Hint: current token looks like a community token. "
            "For wall posting and wall photo upload, use a user access token "
            "with wall/photos permissions and admin rights in the target community."
        )
    return text


def _strip_telegram_html(text: str) -> str:
    text = text.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    text = HTML_TAG_RE.sub("", text)
    return html.unescape(text)


def _smart_trim(text: str, max_len: int) -> str:
    text = text.strip()
    if len(text) <= max_len:
        return text

    trimmed = text[:max_len].rstrip()
    for sep in ("\n\n", "\n", ". ", "! ", "? ", "; ", ", "):
        pos = trimmed.rfind(sep)
        if pos >= max_len // 2:
            trimmed = trimmed[:pos].rstrip()
            break

    return trimmed.rstrip(" ,;:-") + "…"


def _prepare_vk_text(text: str) -> tuple[str, str | None]:
    plain = _strip_telegram_html(text)
    plain = plain.replace("\r\n", "\n").replace("\r", "\n")
    plain = re.sub(r"\n{3,}", "\n\n", plain).strip()

    urls = URL_RE.findall(plain)
    source_url = urls[-1] if urls else None

    lines = [line.strip() for line in plain.splitlines()]
    body_lines: list[str] = []
    for line in lines:
        if not line:
            body_lines.append("")
            continue
        if source_url and line == source_url:
            continue
        body_lines.append(line)

    body = "\n".join(body_lines)
    body = re.sub(r"\n{3,}", "\n\n", body).strip()

    if source_url:
        paragraphs = [part.strip() for part in re.split(r"\n{2,}", body) if part.strip()]
        if paragraphs:
            vk_text = "\n\n".join([paragraphs[0], source_url, *paragraphs[1:]])
        else:
            vk_text = source_url

        if len(vk_text) > VK_MESSAGE_SOFT_LIMIT:
            suffix = f"\n\n{source_url}"
            available = max(80, VK_MESSAGE_SOFT_LIMIT - len(suffix) - 1)
            vk_text = _smart_trim(body, available) + suffix
    else:
        vk_text = body
        if len(vk_text) > VK_MESSAGE_SOFT_LIMIT:
            vk_text = _smart_trim(vk_text, VK_MESSAGE_SOFT_LIMIT)

    return vk_text.strip(), source_url


def _api_call(method: str, *, token: str, api_version: str, **params) -> dict:
    payload = {
        "access_token": token,
        "v": api_version,
        **params,
    }

    response = requests.post(
        f"{API_URL}/{method}",
        data=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    data = response.json()
    if "error" in data:
        error = data["error"] or {}
        code = error.get("error_code", "unknown")
        message = error.get("error_msg", "Unknown VK API error")
        raise VKAPIError(f"{method} failed ({code}): {message}")

    result = data.get("response")
    if result is None:
        raise VKAPIError(f"{method} returned no response payload")

    return result


def _resolve_group_id(*, token: str, api_version: str) -> int | None:
    raw_group_id = os.getenv("VK_GROUP_ID", "").strip()
    if raw_group_id:
        try:
            return abs(int(raw_group_id))
        except ValueError:
            logger.error("VK_GROUP_ID='%s' is not a valid number. Check .env.", raw_group_id)
            return None

    screen_name = (
        os.getenv("VK_GROUP_DOMAIN", "").strip()
        or os.getenv("VK_SCREEN_NAME", "").strip()
    )
    if not screen_name:
        logger.info(
            "[SKIP] VK publish skipped: add VK_GROUP_ID or VK_GROUP_DOMAIN to .env "
            "(token detected: %s).",
            _mask_token(token),
        )
        return None

    try:
        result = _api_call(
            "groups.getById",
            token=token,
            api_version=api_version,
            group_id=screen_name,
        )
    except (requests.RequestException, ValueError, VKAPIError) as exc:
        logger.error(
            "Failed to resolve VK group by domain '%s': %s",
            screen_name,
            _describe_vk_error(exc),
        )
        return None

    groups: list[dict] = []
    if isinstance(result, list):
        groups = result
    elif isinstance(result, dict):
        nested_groups = result.get("groups")
        if isinstance(nested_groups, list):
            groups = nested_groups

    if groups:
        group = groups[0]
        group_id = group.get("id")
        if isinstance(group_id, int):
            return abs(group_id)

    logger.error("VK groups.getById returned unexpected payload for '%s': %s", screen_name, result)
    return None


def _build_photo_attachment(*, token: str, api_version: str, group_id: int, image_path: Path) -> str | None:
    upload_info = _api_call(
        "photos.getWallUploadServer",
        token=token,
        api_version=api_version,
        group_id=group_id,
    )
    upload_url = str(upload_info.get("upload_url", "")).strip()
    if not upload_url:
        logger.error("VK did not return upload_url for wall photo upload.")
        return None

    content_type = "image/png" if image_path.suffix.lower() == ".png" else "image/jpeg"

    with image_path.open("rb") as file_obj:
        upload_response = requests.post(
            upload_url,
            files={"photo": (image_path.name, file_obj, content_type)},
            timeout=60,
        )
    upload_response.raise_for_status()
    upload_data = upload_response.json()

    photo = upload_data.get("photo")
    server = upload_data.get("server")
    photo_hash = upload_data.get("hash")
    if not photo or server is None or not photo_hash:
        logger.error("VK image upload returned incomplete payload: %s", upload_data)
        return None

    saved = _api_call(
        "photos.saveWallPhoto",
        token=token,
        api_version=api_version,
        group_id=group_id,
        photo=photo,
        server=server,
        hash=photo_hash,
    )
    if not isinstance(saved, list) or not saved:
        logger.error("VK photos.saveWallPhoto returned unexpected payload: %s", saved)
        return None

    photo_info = saved[0]
    owner_id = photo_info.get("owner_id")
    photo_id = photo_info.get("id")
    access_key = str(photo_info.get("access_key", "")).strip()
    if owner_id is None or photo_id is None:
        logger.error("VK saved photo payload misses owner_id/id: %s", photo_info)
        return None

    attachment = f"photo{owner_id}_{photo_id}"
    if access_key:
        attachment = f"{attachment}_{access_key}"
    return attachment


def publish_to_vk(text: str, image_path: str | Path | None = None) -> bool:
    """
    Publish post to a VK community wall.

    The function is best-effort by design: if VK is not configured correctly,
    it logs the reason and returns False without affecting the main pipeline.
    """
    token = (
        os.getenv("VK_ACCESS_TOKEN", "").strip()
        or os.getenv("VK_API_KEY", "").strip()
    )
    api_version = os.getenv("VK_API_VERSION", DEFAULT_API_VERSION).strip() or DEFAULT_API_VERSION

    if not token:
        logger.info("[SKIP] VK publish skipped: VK_API_KEY or VK_ACCESS_TOKEN missing in .env")
        return False

    group_id = _resolve_group_id(token=token, api_version=api_version)
    if not group_id:
        return False

    owner_id = -group_id
    attachments: str | None = None
    vk_text, source_url = _prepare_vk_text(text)

    if image_path:
        img = Path(image_path)
        if img.is_file():
            try:
                attachments = _build_photo_attachment(
                    token=token,
                    api_version=api_version,
                    group_id=group_id,
                    image_path=img,
                )
                if attachments:
                    logger.info("VK image uploaded successfully.")
                else:
                    logger.warning("VK image upload failed. Sending text-only post.")
            except (requests.RequestException, ValueError, VKAPIError) as exc:
                logger.warning(
                    "VK image upload failed: %s. Sending text-only post.",
                    _describe_vk_error(exc),
                )
        else:
            logger.warning("VK image file not found: %s. Sending text-only post.", image_path)

    post_params = {
        "owner_id": owner_id,
        "from_group": 1,
        "message": vk_text,
    }
    if attachments:
        post_params["attachments"] = attachments
    if source_url:
        post_params["copyright"] = source_url

    try:
        result = _api_call(
            "wall.post",
            token=token,
            api_version=api_version,
            **post_params,
        )
    except (requests.RequestException, ValueError, VKAPIError) as exc:
        logger.error("VK wall post failed: %s", _describe_vk_error(exc))
        return False

    post_id = result.get("post_id") if isinstance(result, dict) else None
    logger.info("VK wall post sent successfully%s.", f" (post_id={post_id})" if post_id else "")
    return True
