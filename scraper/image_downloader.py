#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Image downloader and update checker."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Optional, Tuple

import requests

logger = logging.getLogger(__name__)


def download_image(url: str, save_path: Path, headers: Optional[dict] = None) -> bool:
    logger.info(f"Downloading image: {url} -> {save_path}")
    try:
        default_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        if headers:
            default_headers.update(headers)

        response = requests.get(url, headers=default_headers, timeout=30)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "").lower()
        if "image" not in content_type and not response.content.startswith((b"\xff\xd8", b"\x89PNG")):
            logger.warning(f"Content-Type is not image: {content_type}")
            return False

        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(response.content)
        logger.info(f"Image downloaded: {save_path} ({len(response.content)} bytes)")
        return True
    except Exception as e:
        logger.error(f"Image download error ({url}): {e}", exc_info=True)
        return False


def get_file_hash(file_path: Path) -> Optional[str]:
    if not file_path.exists():
        return None
    try:
        with open(file_path, "rb") as f:
            content = f.read()
        return hashlib.sha256(content).hexdigest()
    except Exception as e:
        logger.warning(f"Hash calculation error: {e}")
        return None


def check_image_updated(
    url: str,
    local_path: Path,
    last_url: Optional[str] = None,
    last_hash: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Check if the image has been updated and keep local_path refreshed.

    Returns (is_updated, new_hash).
    """
    if last_hash is not None and last_hash == get_file_hash(local_path):
        return False, last_hash
    url_changed = bool(last_url and last_url != url)

    if not local_path.exists() or url_changed:
        if download_image(url, local_path):
            return True, get_file_hash(local_path)
        return True, None

    temp_path = local_path.with_suffix(".tmp")
    if download_image(url, temp_path):
        new_hash = get_file_hash(temp_path)
        old_hash = get_file_hash(local_path)
        if new_hash and old_hash and new_hash != old_hash:
            temp_path.replace(local_path)
            return True, new_hash
        temp_path.unlink(missing_ok=True)
        return False, new_hash or old_hash

    return False, last_hash or get_file_hash(local_path)
