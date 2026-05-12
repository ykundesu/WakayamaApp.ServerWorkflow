#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Annual events page scraper."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

from common.certificates import configure_wakayama_ca_bundle
from common.scrape_errors import ScrapeError

logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))

ANNUAL_EVENTS_URL = "https://www.wakayama-nct.ac.jp/about/profile/calendar/"

FULLWIDTH_DIGIT_TRANSLATION = str.maketrans({
    "０": "0",
    "１": "1",
    "２": "2",
    "３": "3",
    "４": "4",
    "５": "5",
    "６": "6",
    "７": "7",
    "８": "8",
    "９": "9",
})

ERA_OFFSETS = {
    "令和": 2018,
    "平成": 1988,
    "昭和": 1925,
}


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.translate(FULLWIDTH_DIGIT_TRANSLATION)).strip()


def extract_academic_year(text: str) -> Optional[int]:
    normalized = normalize_text(text)

    for era, offset in ERA_OFFSETS.items():
        match = re.search(rf"{era}\s*(\d+|元)\s*年\s*度?", normalized)
        if match:
            value = match.group(1)
            era_year = 1 if value == "元" else int(value)
            return offset + era_year

    match = re.search(r"((?:19|20)\d{2})\s*年?\s*度", normalized)
    if match:
        return int(match.group(1))
    return None


def extract_pdf_links(html: str, base_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[Dict[str, Any]] = []

    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        if ".pdf" not in href.lower():
            continue

        text = normalize_text(anchor.get_text(" ", strip=True))
        url = urljoin(base_url, href)
        haystack = f"{text} {href}"
        year = extract_academic_year(haystack)

        score = 0
        if "年間行事" in haystack:
            score += 20
        if "行事計画" in haystack or "行事計画表" in haystack:
            score += 20
        if "本科" in haystack:
            score += 5
        if "専攻科" in haystack:
            score += 5

        links.append({
            "url": url,
            "text": text,
            "academic_year": year,
            "score": score,
        })

    return links


def _month_from_heading(text: str) -> Optional[int]:
    normalized = normalize_text(text)
    match = re.search(r"(?<!\d)(1[0-2]|[1-9])\s*月", normalized)
    if not match:
        return None
    month = int(match.group(1))
    if 1 <= month <= 12:
        return month
    return None


def _is_stop_heading(text: str) -> bool:
    normalized = normalize_text(text)
    return bool(normalized) and "年間行事" not in normalized and _month_from_heading(normalized) is None


def extract_monthly_events(html: str) -> Dict[int, List[str]]:
    """Extract the month-by-month event list below the annual events PDF link."""
    soup = BeautifulSoup(html, "html.parser")
    heading: Optional[Tag] = None
    for candidate in soup.find_all(re.compile(r"^h[1-6]$")):
        text = normalize_text(candidate.get_text(" ", strip=True))
        if text == "年間行事":
            heading = candidate
            break

    if heading is None:
        return {}

    monthly: Dict[int, List[str]] = {}
    current_month: Optional[int] = None
    for node in heading.find_all_next():
        if not isinstance(node, Tag):
            continue
        if node.name and re.match(r"^h[1-6]$", node.name):
            text = normalize_text(node.get_text(" ", strip=True))
            month = _month_from_heading(text)
            if month is not None:
                current_month = month
                monthly.setdefault(month, [])
                continue
            if current_month is not None and _is_stop_heading(text):
                break
        if current_month is None:
            continue
        if node.name == "li":
            item = normalize_text(node.get_text(" ", strip=True))
            if item and item not in monthly.setdefault(current_month, []):
                monthly[current_month].append(item)

    return monthly


def find_latest_pdf_info(html: str, base_url: str) -> Optional[Dict[str, Any]]:
    links = [link for link in extract_pdf_links(html, base_url) if link.get("score", 0) > 0]
    if not links:
        return None

    now = datetime.now(JST)
    current_academic_year = now.year if now.month >= 4 else now.year - 1

    def sort_key(link: Dict[str, Any]) -> tuple[int, int, int]:
        year = link.get("academic_year")
        year_value = int(year) if isinstance(year, int) else 0
        future_penalty = 0 if year_value <= current_academic_year + 1 else -1
        return (int(link.get("score") or 0), future_penalty, year_value)

    return sorted(links, key=sort_key, reverse=True)[0]


def scrape_annual_events_page(url: str = ANNUAL_EVENTS_URL) -> Optional[Dict[str, Any]]:
    logger.info("年間行事ページをスクレイピング中: %s", url)
    try:
        configure_wakayama_ca_bundle()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        pdf_info = find_latest_pdf_info(response.text, url)
        if not pdf_info:
            logger.warning("年間行事PDFリンクが見つかりませんでした")
            return None

        monthly_events = extract_monthly_events(response.text)
        result = {
            "page_url": url,
            "url": pdf_info["url"],
            "text": pdf_info.get("text") or "",
            "academic_year": pdf_info.get("academic_year"),
            "monthly_events": monthly_events,
        }
        logger.info(
            "年間行事PDFリンクを発見: %s (academic_year=%s)",
            result["url"],
            result.get("academic_year"),
        )
        return result
    except requests.RequestException as e:
        logger.error("年間行事ページスクレイピングエラー: %s", e, exc_info=True)
        raise ScrapeError(f"年間行事ページの取得に失敗しました: {e}") from e
    except Exception as e:
        logger.error("年間行事ページ処理中の予期しないエラー: %s", e, exc_info=True)
        raise ScrapeError(f"年間行事ページの処理中に予期しないエラーが発生しました: {e}") from e
