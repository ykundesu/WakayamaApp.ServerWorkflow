#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Utilities for scraping dormitory meal PDFs."""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

DORMITORY_URL = "https://www.wakayama-nct.ac.jp/campuslife/dormitory/restaurant/"

ERA_OFFSETS = {
    "\u4ee4\u548c": 2018,  # Reiwa -> 2018 + era year
    "\u5e73\u6210": 1988,  # Heisei -> 1988 + era year
    "\u662d\u548c": 1925,  # Showa -> 1925 + era year
}

ERA_PATTERN = re.compile(
    r"(?P<era>\u4ee4\u548c|\u5e73\u6210|\u662d\u548c)\s*"
    r"(?P<era_year>\d{1,2})\s*(?:\u5e74|\u5e74\u5ea6)\s*"
    r"(?P<month>\d{1,2})\s*\u6708?",
    re.UNICODE,
)

ROMAN_REIWA_PATTERN = re.compile(
    r"R(?P<era_year>\d{1,2})[_-]?(?P<month>\d{2})", re.IGNORECASE
)

YEAR_MONTH_PATTERNS = [
    re.compile(
        r"(?P<year>\d{4})\s*(?:\u5e74|\u5e74\u5ea6)\s*(?P<month>\d{1,2})\s*\u6708?",
        re.UNICODE,
    ),
    re.compile(r"(?P<year>\d{4})\s*[-_/\.]\s*(?P<month>\d{1,2})", re.UNICODE),
    re.compile(r"(?P<year>\d{4})\s+(?P<month>\d{1,2})\b", re.UNICODE),
    re.compile(r"(?P<year>\d{4})(?P<month>\d{2})"),
]


def _normalize_text(text: str) -> str:
    """Apply unicode normalization so date parsing becomes easier."""
    normalized = unicodedata.normalize("NFKC", text or "")
    return normalized.replace("\u3000", " ").strip()


def _safe_year_month(year: int, month: int) -> Optional[str]:
    """Return YYYY-MM if the values are within the calendar range."""
    if 1 <= month <= 12:
        return f"{year}-{month:02d}"
    return None


def extract_pdf_links(html: str, base_url: str) -> List[Dict[str, Any]]:
    """
    Collect candidate PDF links from the dormitory menu page.

    Returns a list containing the resolved URL and any detected date metadata.
    """
    soup = BeautifulSoup(html, "html.parser")
    pdf_links: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if not href or ".pdf" not in href.lower():
            continue

        full_url = urljoin(base_url, href)
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        text = link.get_text(strip=True)
        combined_text = f"{text} {href}"

        date_info = extract_date_from_text(combined_text)
        year: Optional[int] = None
        month: Optional[int] = None
        if date_info:
            parsed = _parse_year_month(date_info)
            if parsed:
                year, month = parsed

        pdf_links.append(
            {
                "url": full_url,
                "text": text,
                "href": href,
                "date": date_info,
                "year": year,
                "month": month,
            }
        )

    return pdf_links


def extract_date_from_text(text: str) -> Optional[str]:
    """
    Try to detect a YYYY-MM string from link text or filenames.

    Supports both Gregorian years and simple Japanese era notations
    such as 令和7年4月 or R07_04.
    """
    normalized = _normalize_text(text)
    if not normalized:
        return None

    era_match = ERA_PATTERN.search(normalized)
    if era_match:
        era = era_match.group("era")
        base_year = ERA_OFFSETS.get(era)
        era_year = int(era_match.group("era_year"))
        month = int(era_match.group("month"))
        if base_year is not None:
            result = _safe_year_month(base_year + era_year, month)
            if result:
                return result

    roman_match = ROMAN_REIWA_PATTERN.search(normalized)
    if roman_match:
        base_year = ERA_OFFSETS.get("\u4ee4\u548c")  # Reiwa
        era_year = int(roman_match.group("era_year"))
        month = int(roman_match.group("month"))
        if base_year is not None:
            result = _safe_year_month(base_year + era_year, month)
            if result:
                return result

    for pattern in YEAR_MONTH_PATTERNS:
        match = pattern.search(normalized)
        if not match:
            continue
        year = int(match.group("year"))
        month = int(match.group("month"))
        formatted = _safe_year_month(year, month)
        if formatted:
            return formatted

    return None


def _parse_year_month(date_str: Optional[str]) -> Optional[Tuple[int, int]]:
    """Parse 'YYYY-MM' into integers."""
    if not date_str:
        return None
    try:
        year_str, month_str = date_str.split("-")
        return int(year_str), int(month_str)
    except ValueError:
        return None


def find_current_and_next_pdf_links(html: str, base_url: str) -> List[Dict[str, Any]]:
    """
    Return the PDF link for the current and next month in that order.

    If explicit month information is unavailable, fall back to the two most
    relevant PDFs found on the page.
    """
    pdf_links = extract_pdf_links(html, base_url)
    if not pdf_links:
        return []

    now = datetime.now()
    current = (now.year, now.month)
    next_month = (now.year + 1, 1) if now.month == 12 else (now.year, now.month + 1)

    by_month: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for link in pdf_links:
        year = link.get("year")
        month = link.get("month")
        if year and month and (year, month) not in by_month:
            by_month[(year, month)] = link

    selected: List[Dict[str, Any]] = []
    for target_key, label in ((current, "current"), (next_month, "next")):
        chosen = by_month.get(target_key)
        if chosen:
            item = dict(chosen)
            item["target"] = label
            selected.append(item)

    if selected:
        return selected

    def sort_key(link: Dict[str, Any]) -> Tuple[int, int, str]:
        year = link.get("year")
        month = link.get("month")
        if year and month:
            delta = (year - current[0]) * 12 + (month - current[1])
            group = 0 if delta >= 0 else 1
            return (group, abs(delta), link.get("url", ""))
        return (2, 0, link.get("url", ""))

    fallback_links = sorted(pdf_links, key=sort_key)
    results: List[Dict[str, Any]] = []
    for link in fallback_links:
        item = dict(link)
        item.setdefault("target", "fallback")
        results.append(item)
        if len(results) >= 2:
            break

    return results


def scrape_dormitory_page() -> List[Dict[str, Any]]:
    """Fetch the dormitory page and return target PDF metadata."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.get(DORMITORY_URL, headers=headers, timeout=30)
        response.raise_for_status()
        return find_current_and_next_pdf_links(response.text, DORMITORY_URL)
    except requests.RequestException as exc:
        print(f"[dormitory] failed to fetch dormitory page: {exc}")
    except Exception as exc:  # pragma: no cover - safeguard
        print(f"[dormitory] unexpected error: {exc}")
    return []

