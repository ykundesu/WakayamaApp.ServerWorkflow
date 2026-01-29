#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Dormitory events image processing."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

from PIL import Image

from common.pdf_processor import PDFProcessor

logger = logging.getLogger(__name__)

ERA_OFFSETS = {
    "令和": 2018,
    "平成": 1988,
    "昭和": 1925,
}

ACADEMIC_YEAR_RE = re.compile(r"(?P<year>\d{4})\s*年度")
ERA_YEAR_RE = re.compile(r"(?P<era>令和|平成|昭和)\s*(?P<era_year>\d{1,2})\s*年度")

DATE_RE = re.compile(r"(?P<month>\d{1,2})\s*/\s*(?P<day>\d{1,2})")
DATE_JP_RE = re.compile(r"(?P<month>\d{1,2})\s*月\s*(?P<day>\d{1,2})\s*日")
DATE_RANGE_RE = re.compile(
    r"(?P<sm>\d{1,2})\s*/\s*(?P<sd>\d{1,2})\s*[-〜~～]\s*(?P<em>\d{1,2})\s*/\s*(?P<ed>\d{1,2})"
)
DATE_RANGE_JP_RE = re.compile(
    r"(?P<sm>\d{1,2})\s*月\s*(?P<sd>\d{1,2})\s*日\s*[-〜~～]\s*(?P<em>\d{1,2})\s*月\s*(?P<ed>\d{1,2})\s*日"
)


EVENTS_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "academic_year": {"type": "integer"},
        "events": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "date": {"type": "string"},
                    "grade": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
                    "name": {"type": "string"},
                },
                "required": ["date", "grade", "name"],
            },
        },
    },
    "required": ["events"],
}


DORMITORY_EVENTS_PROMPT = """この画像は学生寮の行事予定表です。表の各行から行事を抽出してください。

ルール:
- date は "MM/DD" 形式
- grade は対象学年を 1〜5 の整数で返す。全学年/全寮生/対象なし/不明は null
- name は行事名
- academic_year は西暦の年度 (例: 2025)

出力は次の JSON スキーマに従ってください。
```json
{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "academic_year": { "type": "integer" },
    "events": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "date": { "type": "string" },
          "grade": { "anyOf": [{"type": "integer"}, {"type": "null"}] },
          "name": { "type": "string" }
        },
        "required": ["date", "grade", "name"]
      }
    }
  },
  "required": ["events"]
}
```
"""


def current_academic_year(now: Optional[datetime] = None) -> int:
    now = now or datetime.now()
    return now.year if now.month >= 4 else now.year - 1


def extract_academic_year(text: str) -> Optional[int]:
    if not text:
        return None
    match = ERA_YEAR_RE.search(text)
    if match:
        era = match.group("era")
        era_year = int(match.group("era_year"))
        base = ERA_OFFSETS.get(era)
        if base:
            return base + era_year
    match = ACADEMIC_YEAR_RE.search(text)
    if match:
        return int(match.group("year"))
    return None


def resolve_year_for_month(academic_year: int, month: int) -> int:
    return academic_year if month >= 4 else academic_year + 1


def normalize_date(month: int, day: int) -> str:
    return f"{month:02d}/{day:02d}"


def expand_date_range(start_month: int, start_day: int, end_month: int, end_day: int, academic_year: int) -> List[str]:
    start_year = resolve_year_for_month(academic_year, start_month)
    end_year = resolve_year_for_month(academic_year, end_month)
    start = datetime(start_year, start_month, start_day)
    end = datetime(end_year, end_month, end_day)
    if end < start:
        end = datetime(end_year + 1, end_month, end_day)
    dates = []
    current = start
    while current <= end:
        dates.append(normalize_date(current.month, current.day))
        current += timedelta(days=1)
    return dates


def parse_dates_from_text(text: str, academic_year: int) -> List[str]:
    if not text:
        return []
    range_match = DATE_RANGE_RE.search(text) or DATE_RANGE_JP_RE.search(text)
    if range_match:
        sm = int(range_match.group("sm"))
        sd = int(range_match.group("sd"))
        em = int(range_match.group("em"))
        ed = int(range_match.group("ed"))
        return expand_date_range(sm, sd, em, ed, academic_year)

    match = DATE_RE.search(text) or DATE_JP_RE.search(text)
    if match:
        month = int(match.group("month"))
        day = int(match.group("day"))
        return [normalize_date(month, day)]
    return []


def parse_grade_values(value: Any, name_hint: str = "") -> List[Optional[int]]:
    if value is None:
        return [None]
    if isinstance(value, int):
        return [value]
    if isinstance(value, str):
        text = value
    else:
        text = str(value)

    combined = f"{text} {name_hint}"
    if any(keyword in combined for keyword in ("全学年", "全寮生", "全員", "全体", "全て")):
        return [None]

    range_match = re.search(r"(?<!\d)([1-5])\s*[-〜~～]\s*([1-5])\s*年", combined)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        if start <= end:
            return list(range(start, end + 1))

    list_match = re.search(r"(?<!\d)([1-5](?:[\s,、・/]+[1-5])+)\s*年", combined)
    if list_match:
        digits = re.findall(r"[1-5]", list_match.group(1))
        grades = sorted({int(d) for d in digits})
        return [cast(Optional[int], grade) for grade in grades]

    single_matches = re.findall(r"(?<!\d)([1-5])\s*年", combined)
    if single_matches:
        grades = sorted({int(d) for d in single_matches})
        return [cast(Optional[int], grade) for grade in grades]

    return [None]


def normalize_events(raw_events: Any, academic_year: int) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    seen = set()
    if not isinstance(raw_events, list):
        return normalized

    for event in raw_events:
        if not isinstance(event, dict):
            continue
        name = str(event.get("name") or "").strip()
        date_text = str(event.get("date") or "")
        dates = parse_dates_from_text(date_text, academic_year)
        if not dates and name:
            dates = parse_dates_from_text(name, academic_year)
        if not dates:
            continue
        grade_values = parse_grade_values(event.get("grade"), name_hint=name)
        for date in dates:
            for grade in grade_values:
                key = (date, grade, name)
                if key in seen:
                    continue
                seen.add(key)
                normalized.append({"date": date, "grade": grade, "name": name})
    return normalized


def process_dormitory_events_image(
    image_path: str,
    out_dir: Path,
    model: str = "gemini-2.5-pro",
    api_key: Optional[str] = None,
    dpi: int = 288,
    temperature: float = 0.2,
    max_tokens: int = 2000,
    use_yomitoku: bool = False,
    yomitoku_device: str = "cpu",
    yomitoku_config: Optional[Path] = None,
    title_hint: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    logger.info(f"Processing dormitory events image: {image_path}")
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        json_dir = out_dir / "json"
        json_dir.mkdir(parents=True, exist_ok=True)

        processor = PDFProcessor(
            model=model,
            api_key=api_key,
            schema=EVENTS_SCHEMA,
            dpi=dpi,
            temperature=temperature,
            max_tokens=max_tokens,
            use_yomitoku=use_yomitoku,
            yomitoku_device=yomitoku_device,
            yomitoku_config=yomitoku_config,
        )

        prompt_text = DORMITORY_EVENTS_PROMPT
        academic_year_hint = extract_academic_year(title_hint or "")
        if title_hint:
            prompt_text = f"タイトル情報: {title_hint}\n" + prompt_text
        if academic_year_hint:
            prompt_text = f"academic_year_hint: {academic_year_hint}\n" + prompt_text

        image = Image.open(image_path).convert("RGB")
        result_json = processor.process_page(
            page_num=1,
            page_image=image,
            prompt=prompt_text,
            out_dir=out_dir,
            call_mode="none",
            merge_strategy="deep",
        )

        page_json_path = json_dir / "page_0001.json"
        with open(page_json_path, "w", encoding="utf-8") as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2)

        data = result_json
        if isinstance(result_json, dict) and "result" in result_json:
            data = result_json["result"]

        academic_year = None
        if isinstance(data, dict):
            raw_year = data.get("academic_year")
            if isinstance(raw_year, int):
                academic_year = raw_year
            elif isinstance(raw_year, str):
                academic_year = extract_academic_year(raw_year)

        if academic_year is None:
            academic_year = academic_year_hint or current_academic_year()

        raw_events = data.get("events") if isinstance(data, dict) else []
        events = normalize_events(raw_events, academic_year)
        logger.info(f"Extracted events: {len(events)}")
        return {"academic_year": academic_year, "events": events}
    except Exception as e:
        logger.error(f"Dormitory events processing error: {e}", exc_info=True)
        return None
