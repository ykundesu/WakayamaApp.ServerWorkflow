#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""School rules page scraper."""

from __future__ import annotations

import logging
import re
import time
from typing import Dict, Iterable, Iterator, List, Optional, cast
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

RULES_URL = "https://www.wakayama-nct.ac.jp/about/profile/rules/"


def normalize_text(text: str) -> str:
    """Normalize spaces and whitespace."""
    if not text:
        return ""
    normalized = text.replace("\u3000", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def fetch_html(url: str, timeout: float = 15.0, retries: int = 3, backoff: float = 1.5) -> str:
    """Fetch HTML with retries."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; WakayamaRulesScraper/1.0)"
    }
    last_exc: Optional[Exception] = None
    with requests.Session() as session:
        session.headers.update(headers)
        for attempt in range(retries):
            try:
                response = session.get(url, timeout=timeout)
                response.raise_for_status()
                response.encoding = response.encoding or response.apparent_encoding or "utf-8"
                return response.text
            except Exception as exc:  # pragma: no cover - defensive
                last_exc = exc
                if attempt < retries - 1:
                    time.sleep(backoff ** attempt)
                else:
                    break
    raise RuntimeError(f"failed to fetch: {url} ({last_exc})")


def _normalize_href(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "".join(str(item) for item in value).strip()
    return str(value).strip()


def _iter_anchors(nodes: Iterable[Tag]) -> Iterator[Tag]:
    for node in nodes:
        if not isinstance(node, Tag):
            continue
        if node.name == "a" and node.has_attr("href"):
            yield node
        for anchor in node.find_all("a", href=True):
            if isinstance(anchor, Tag):
                yield anchor


def extract_links(nodes: List[Tag], base_url: Optional[str], pdf_only: bool = False) -> List[Dict[str, str]]:
    """Extract anchor links from a list of nodes."""
    items: List[Dict[str, str]] = []
    seen = set()
    for anchor in _iter_anchors(nodes):
        name = normalize_text(anchor.get_text(strip=True))
        href = _normalize_href(anchor.get("href"))
        if not name or not href:
            continue
        if pdf_only and not href.lower().endswith(".pdf"):
            continue
        url = urljoin(base_url or "", href)
        key = (name, url)
        if key in seen:
            continue
        seen.add(key)
        items.append({"name": name, "url": url})
    return items


def extract_links_until_next_heading(
    heading: Tag,
    container: Tag,
    base_url: Optional[str],
    pdf_only: bool = False,
) -> List[Dict[str, str]]:
    """Extract links from nodes following the heading until the next heading."""
    items: List[Dict[str, str]] = []
    seen = set()
    for element in heading.next_elements:
        if isinstance(element, Tag):
            if container not in element.parents:
                break
            if element.name in {"h1", "h2", "h3"}:
                break
            if element.name != "a" or not element.has_attr("href"):
                continue
            name = normalize_text(element.get_text(strip=True))
            href = _normalize_href(element.get("href"))
            if not name or not href:
                continue
            if pdf_only and not href.lower().endswith(".pdf"):
                continue
            url = urljoin(base_url or "", href)
            key = (name, url)
            if key in seen:
                continue
            seen.add(key)
            items.append({"name": name, "url": url})
    return items


def parse_rules(html: str, base_url: Optional[str] = None, pdf_only: bool = False) -> List[Dict[str, object]]:
    """Parse school rules page into chapter entries."""
    soup = None
    for parser in ("lxml", "html.parser"):
        try:
            soup = BeautifulSoup(html, parser)
            break
        except Exception:  # pragma: no cover - fallback
            continue
    if soup is None:
        return []

    container = soup.select_one("div.pagebody") or soup
    headings: List[Tag] = [cast(Tag, h) for h in container.find_all(["h2", "h3"]) if isinstance(h, Tag)]
    result: List[Dict[str, object]] = []

    for heading in headings:
        title = normalize_text(heading.get_text(" ", strip=True))
        if not title:
            continue

        items = extract_links_until_next_heading(heading, container, base_url, pdf_only=pdf_only)

        if items:
            result.append({"name": title, "contents": items})

    return result


def scrape_rules_page(url: str = RULES_URL, pdf_only: bool = True) -> List[Dict[str, object]]:
    """Fetch and parse the rules page."""
    logger.info("Scraping rules page: %s", url)
    html = fetch_html(url)
    base = url
    return parse_rules(html, base_url=base, pdf_only=pdf_only)
