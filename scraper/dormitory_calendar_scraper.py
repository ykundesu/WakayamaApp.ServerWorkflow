#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Utilities for scraping dormitory calendar images."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DORMITORY_CALENDAR_URL = "https://www.wakayama-nct.ac.jp/campuslife/dormitory/calendar/"

CALENDAR_KEYWORDS = ("行事", "学寮", "寮", "calendar", "schedule")


def _get_image_src(img) -> Optional[str]:
    for key in ("data-src", "data-lazy-src", "src"):
        value = img.get(key)
        if value:
            return value
    return None


def _nearest_heading(img) -> str:
    heading_tag = img.find_previous(["h1", "h2", "h3"])
    if heading_tag:
        return heading_tag.get_text(strip=True)
    return ""


def extract_calendar_images(html: str, base_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    pagebody = soup.find(class_="pagebody")
    search_root = pagebody or soup

    images: List[Dict[str, Any]] = []
    seen = set()
    for img in search_root.find_all("img"):
        src = _get_image_src(img)
        if not src:
            continue
        full_url = urljoin(base_url, src)
        if full_url in seen:
            continue
        seen.add(full_url)
        alt = (img.get("alt") or "").strip()
        heading = _nearest_heading(img)
        images.append({
            "url": full_url,
            "alt": alt,
            "heading": heading,
        })

    return images


def _score_image(info: Dict[str, Any]) -> int:
    score = 0
    url = info.get("url", "")
    text = f"{info.get('alt', '')} {info.get('heading', '')}".lower()
    for keyword in CALENDAR_KEYWORDS:
        if keyword.lower() in text:
            score += 5
    if "calendar" in url or "schedule" in url:
        score += 2
    if url.lower().endswith((".png", ".jpg", ".jpeg")):
        score += 1
    return score


def find_calendar_image(html: str, base_url: str) -> Optional[Dict[str, Any]]:
    images = extract_calendar_images(html, base_url)
    if not images:
        return None
    ranked = sorted(images, key=_score_image, reverse=True)
    return ranked[0]


def scrape_dormitory_calendar_page() -> Optional[Dict[str, Any]]:
    logger.info(f"Scraping dormitory calendar page: {DORMITORY_CALENDAR_URL}")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.get(DORMITORY_CALENDAR_URL, headers=headers, timeout=30)
        response.raise_for_status()
        result = find_calendar_image(response.text, DORMITORY_CALENDAR_URL)
        if result:
            logger.info(f"Calendar image found: {result.get('url')}")
        else:
            logger.warning("Calendar image not found on page.")
        return result
    except requests.RequestException as exc:
        logger.error(f"[dormitory_calendar] Request failed: {exc}", exc_info=True)
    except Exception as exc:
        logger.error(f"[dormitory_calendar] Unexpected error: {exc}", exc_info=True)
    return None
