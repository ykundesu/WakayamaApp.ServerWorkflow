#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Annual events PDF processing."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from common.api_client import OpenAICaller, model_uses_openai
from common.image_utils import render_pdf_pages, save_image
from common.json_extractor import extract_json_from_text

logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))

COURSE_KEYS = ("regular", "advanced")
WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
SOURCE_TYPES = ("monthly_note", "day_cell")


ANNUAL_EVENTS_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.com/schemas/annual-events.schema.json",
    "title": "AnnualEventsExtraction",
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "academicYear": {"type": "integer"},
        "regular": {"$ref": "#/$defs/CourseExtraction"},
        "advanced": {"$ref": "#/$defs/CourseExtraction"},
    },
    "required": ["academicYear", "regular", "advanced"],
    "$defs": {
        "CourseExtraction": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "monthlyEvents": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/MonthlyEvents"},
                },
                "longVacations": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/LongVacation"},
                },
                "exams": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/Exam"},
                },
                "specialClassDays": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/SpecialClassDay"},
                },
            },
            "required": ["monthlyEvents", "longVacations", "exams", "specialClassDays"],
        },
        "MonthlyEvents": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "month": {"type": "integer"},
                "events": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/MonthlyEvent"},
                },
            },
            "required": ["month", "events"],
        },
        "DateOccurrence": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "date": {"type": ["string", "null"]},
                "endDate": {"type": ["string", "null"]},
                "raw": {"type": "string"},
            },
            "required": ["date", "endDate", "raw"],
        },
        "MonthlyEvent": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {"type": "string"},
                "category": {
                    "type": "string",
                    "enum": [
                        "ceremony",
                        "class",
                        "exam",
                        "sports",
                        "dormitory",
                        "contest",
                        "meeting",
                        "holiday",
                        "international",
                        "admission",
                        "other",
                    ],
                },
                "dates": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/DateOccurrence"},
                },
                "sourceTypes": {
                    "type": "array",
                    "items": {"type": "string", "enum": list(SOURCE_TYPES)},
                },
            },
            "required": ["name", "category", "dates", "sourceTypes"],
        },
        "LongVacation": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {"type": "string"},
                "startDate": {"type": ["string", "null"]},
                "endDate": {"type": ["string", "null"]},
                "closeDormDate": {"type": ["string", "null"]},
                "openDormDate": {"type": ["string", "null"]},
                "status": {"type": "string", "enum": ["complete", "incomplete"]},
                "sourceEventNames": {"type": "array", "items": {"type": "string"}},
            },
            "required": [
                "name",
                "startDate",
                "endDate",
                "closeDormDate",
                "openDormDate",
                "status",
                "sourceEventNames",
            ],
        },
        "Exam": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {"type": "string"},
                "startDate": {"type": ["string", "null"]},
                "endDate": {"type": ["string", "null"]},
                "category": {
                    "type": "string",
                    "enum": ["general", "term_end", "makeup", "grade_decision", "other"],
                },
                "sourceTypes": {
                    "type": "array",
                    "items": {"type": "string", "enum": list(SOURCE_TYPES)},
                },
            },
            "required": ["name", "startDate", "endDate", "category", "sourceTypes"],
        },
        "SpecialClassDay": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "date": {"type": "string"},
                "actualWeekday": {"type": "string", "enum": list(WEEKDAYS)},
                "classWeekday": {"type": "string", "enum": list(WEEKDAYS)},
                "label": {"type": "string"},
                "sourceTypes": {
                    "type": "array",
                    "items": {"type": "string", "enum": list(SOURCE_TYPES)},
                },
            },
            "required": ["date", "actualWeekday", "classWeekday", "label", "sourceTypes"],
        },
    },
}


ANNUAL_EVENTS_PROMPT = """和歌山高専の本科生および専攻科生の年間行事計画表を抽出してください。

入力:
- PDFをページ画像として添付します。抽出は添付画像の内容のみを根拠にしてください。

抽出ルール:
- academicYear は対象年度の西暦です。
- regular は本科、advanced は専攻科です。1回の応答で両方を必ず出してください。
- monthlyEvents は月ごとに、その月に存在する行事をカテゴリ付きで出してください。
- PDF下部の月別メモ由来は sourceTypes に monthly_note を入れてください。
- 日付セルやカレンダー本体由来は sourceTypes に day_cell を入れてください。
- 同じ行事が月別メモと日付セルの両方にある場合は1件にまとめ、sourceTypes は両方を入れてください。
- 日付が読める行事は dates に YYYY-MM-DD 形式で入れてください。範囲の場合は date と endDate を使ってください。
- 日付が読めない、または月別メモだけの汎用行事は dates を空配列にしてください。
- 体育大会、高専祭、授業参観、学生総会、寮生総会、ロボコン、英語プレコン、留学、入試、開寮、閉寮など、月別メモに無くても日付セルやPDF下部にあれば対象です。
- longVacations は閉寮から次の開寮までを長期休みとして出してください。次の開寮が同一PDFに無い場合は endDate/openDormDate を null、status を incomplete にしてください。
- exams は一般科目試験、期末試験、追試、再試、成績判定会などを抽出してください。
- specialClassDays は「月曜授業」「水曜授業」など、実際の日付の曜日と授業上の曜日が違う日を抽出してください。
- weekday は mon/tue/wed/thu/fri/sat/sun のいずれかです。
- JSON Schemaに厳密に従い、JSONオブジェクトのみを返してください。
"""


def _build_prompt() -> str:
    return (
        ANNUAL_EVENTS_PROMPT
        + "\n\n[JSON Schema]\n"
        + json.dumps(ANNUAL_EVENTS_SCHEMA, ensure_ascii=False, indent=2)
    )


def _unwrap_result(result_json: Any) -> Dict[str, Any]:
    if isinstance(result_json, dict):
        if "result" in result_json and isinstance(result_json["result"], dict):
            return result_json["result"]
        return result_json
    raise ValueError("年間行事の抽出結果がJSONオブジェクトではありません")


def _content_to_text(content: Any) -> str:
    if isinstance(content, list):
        return "".join(item.get("text", "") if isinstance(item, dict) else str(item) for item in content)
    return str(content)


def _parse_date(value: Any, academic_year: int) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    iso_match = re.search(r"((?:19|20)\d{2})[-/](\d{1,2})[-/](\d{1,2})", text)
    if iso_match:
        year = int(iso_match.group(1))
        month = int(iso_match.group(2))
        day = int(iso_match.group(3))
    else:
        md_match = re.search(r"(?<!\d)(1[0-2]|0?[1-9])\s*[/月]\s*([0-3]?\d)", text)
        if not md_match:
            return None
        month = int(md_match.group(1))
        day = int(md_match.group(2))
        year = academic_year if month >= 4 else academic_year + 1
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None


def _month_from_date(date_text: Optional[str]) -> Optional[int]:
    if not date_text:
        return None
    try:
        return int(date_text[5:7])
    except (TypeError, ValueError):
        return None


def _normalize_source_types(values: Any, default: str = "day_cell") -> List[str]:
    if not isinstance(values, list):
        values = [values] if values else [default]
    normalized = []
    for value in values:
        text = str(value).strip()
        if text in SOURCE_TYPES and text not in normalized:
            normalized.append(text)
    if not normalized:
        normalized.append(default)
    return normalized


def _normalize_dates(values: Any, academic_year: int) -> List[Dict[str, Any]]:
    if not isinstance(values, list):
        return []
    normalized: List[Dict[str, Any]] = []
    seen = set()
    for value in values:
        if not isinstance(value, dict):
            continue
        date = _parse_date(value.get("date"), academic_year)
        end_date = _parse_date(value.get("endDate"), academic_year)
        raw = str(value.get("raw") or value.get("date") or "").strip()
        if not date and not end_date:
            continue
        key = (date, end_date, raw)
        if key in seen:
            continue
        seen.add(key)
        normalized.append({"date": date, "endDate": end_date, "raw": raw})
    normalized.sort(key=lambda item: (item.get("date") or "", item.get("endDate") or "", item.get("raw") or ""))
    return normalized


def _normalize_monthly_events(values: Any, academic_year: int) -> List[Dict[str, Any]]:
    buckets: Dict[int, Dict[tuple[str, str], Dict[str, Any]]] = {}
    if not isinstance(values, list):
        values = []

    for bucket in values:
        if not isinstance(bucket, dict):
            continue
        try:
            month = int(bucket.get("month"))
        except (TypeError, ValueError):
            continue
        if not 1 <= month <= 12:
            continue
        for event in bucket.get("events") or []:
            if not isinstance(event, dict):
                continue
            name = str(event.get("name") or "").strip()
            if not name:
                continue
            category = str(event.get("category") or "other")
            dates = _normalize_dates(event.get("dates"), academic_year)
            event_month = month
            if dates:
                event_month = _month_from_date(dates[0].get("date")) or month
            sources = _normalize_source_types(event.get("sourceTypes"))
            key = (name, category)
            month_events = buckets.setdefault(event_month, {})
            existing = month_events.get(key)
            if existing:
                existing_sources = set(existing["sourceTypes"])
                existing["sourceTypes"] = [item for item in SOURCE_TYPES if item in existing_sources | set(sources)]
                existing_dates = existing["dates"]
                for date in dates:
                    if date not in existing_dates:
                        existing_dates.append(date)
                existing_dates.sort(key=lambda item: (item.get("date") or "", item.get("endDate") or ""))
            else:
                month_events[key] = {
                    "name": name,
                    "category": category,
                    "dates": dates,
                    "sourceTypes": sources,
                }

    normalized = []
    for month in range(1, 13):
        events_map = buckets.get(month, {})
        events = sorted(
            events_map.values(),
            key=lambda item: (
                item["dates"][0]["date"] if item["dates"] else "",
                item["category"],
                item["name"],
            ),
        )
        normalized.append({"month": month, "events": events})
    return normalized


def _vacation_name_for_close_date(close_date: str) -> str:
    month = _month_from_date(close_date)
    if month in {7, 8, 9}:
        return "夏季休業"
    if month in {12, 1}:
        return "冬季休業"
    if month in {2, 3}:
        return "学年末休業"
    return "長期休業"


def _normalize_long_vacations(values: Any, academic_year: int) -> List[Dict[str, Any]]:
    if not isinstance(values, list):
        return []
    normalized = []
    for item in values:
        if not isinstance(item, dict):
            continue
        start_date = _parse_date(item.get("startDate"), academic_year)
        end_date = _parse_date(item.get("endDate"), academic_year)
        close_date = _parse_date(item.get("closeDormDate"), academic_year) or start_date
        open_date = _parse_date(item.get("openDormDate"), academic_year) or end_date
        status = "complete" if open_date else "incomplete"
        names = item.get("sourceEventNames")
        if not isinstance(names, list):
            names = []
        normalized.append({
            "name": str(item.get("name") or _vacation_name_for_close_date(close_date or "")).strip() or "長期休業",
            "startDate": start_date,
            "endDate": end_date,
            "closeDormDate": close_date,
            "openDormDate": open_date,
            "status": status,
            "sourceEventNames": [str(name) for name in names if str(name).strip()],
        })
    normalized.sort(key=lambda item: item.get("startDate") or "")
    return normalized


def _normalize_exams(values: Any, academic_year: int) -> List[Dict[str, Any]]:
    if not isinstance(values, list):
        return []
    normalized = []
    seen = set()
    for item in values:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        start_date = _parse_date(item.get("startDate"), academic_year)
        end_date = _parse_date(item.get("endDate"), academic_year) or start_date
        category = str(item.get("category") or "other")
        sources = _normalize_source_types(item.get("sourceTypes"))
        key = (name, start_date, end_date, category)
        if key in seen:
            continue
        seen.add(key)
        normalized.append({
            "name": name,
            "startDate": start_date,
            "endDate": end_date,
            "category": category,
            "sourceTypes": sources,
        })
    normalized.sort(key=lambda item: (item.get("startDate") or "", item["name"]))
    return normalized


def _weekday_for_date(date_text: str) -> str:
    weekday = datetime.strptime(date_text, "%Y-%m-%d").weekday()
    return WEEKDAYS[weekday]


def _normalize_special_class_days(values: Any, academic_year: int) -> List[Dict[str, Any]]:
    if not isinstance(values, list):
        return []
    normalized = []
    seen = set()
    for item in values:
        if not isinstance(item, dict):
            continue
        date = _parse_date(item.get("date"), academic_year)
        if not date:
            continue
        actual = _weekday_for_date(date)
        class_weekday = str(item.get("classWeekday") or "").strip()
        label = str(item.get("label") or "").strip()
        if class_weekday not in WEEKDAYS:
            for jp, key in {"月曜": "mon", "火曜": "tue", "水曜": "wed", "木曜": "thu", "金曜": "fri", "土曜": "sat", "日曜": "sun"}.items():
                if jp in label:
                    class_weekday = key
                    break
        if actual not in WEEKDAYS or class_weekday not in WEEKDAYS:
            continue
        sources = _normalize_source_types(item.get("sourceTypes"))
        key = (date, class_weekday, label)
        if key in seen:
            continue
        seen.add(key)
        normalized.append({
            "date": date,
            "actualWeekday": actual,
            "classWeekday": class_weekday,
            "label": label or f"{class_weekday}授業",
            "sourceTypes": sources,
        })
    normalized.sort(key=lambda item: (item["date"], item["classWeekday"]))
    return normalized


def _normalize_course(course: Any, academic_year: int) -> Dict[str, Any]:
    if not isinstance(course, dict):
        course = {}
    monthly_events = _normalize_monthly_events(course.get("monthlyEvents"), academic_year)
    return {
        "monthlyEvents": monthly_events,
        "longVacations": _normalize_long_vacations(course.get("longVacations"), academic_year),
        "exams": _normalize_exams(course.get("exams"), academic_year),
        "specialClassDays": _normalize_special_class_days(course.get("specialClassDays"), academic_year),
    }


def _normalize_payload(
    extracted: Dict[str, Any],
    *,
    academic_year_hint: Optional[int],
    page_url: str,
    pdf_url: str,
    pdf_hash: Optional[str],
) -> Dict[str, Dict[str, Any]]:
    raw_year = extracted.get("academicYear")
    try:
        academic_year = int(raw_year)
    except (TypeError, ValueError):
        if academic_year_hint is None:
            raise ValueError("年間行事の年度を抽出できませんでした")
        academic_year = academic_year_hint

    generated_at = datetime.now(JST).isoformat(timespec="seconds")
    result: Dict[str, Dict[str, Any]] = {}
    for course in COURSE_KEYS:
        course_payload = _normalize_course(extracted.get(course), academic_year)
        course_payload.update({
            "academicYear": academic_year,
            "course": course,
            "source": {
                "pageUrl": page_url,
                "pdfUrl": pdf_url,
                "pdfHash": pdf_hash,
                "generatedAt": generated_at,
            },
        })
        # Keep metadata at the top of the JSON file.
        result[course] = {
            "academicYear": course_payload.pop("academicYear"),
            "course": course_payload.pop("course"),
            "source": course_payload.pop("source"),
            **course_payload,
        }
    return result


def process_annual_events_pdf(
    pdf_path: str,
    out_dir: Path,
    *,
    page_url: str,
    pdf_url: str,
    pdf_hash: Optional[str] = None,
    academic_year_hint: Optional[int] = None,
    model: str = "gpt-5.5",
    api_key: Optional[str] = None,
    dpi: int = 220,
    temperature: float = 0.2,
    reasoning_effort: str = "medium",
) -> Optional[Dict[str, Dict[str, Any]]]:
    logger.info("年間行事PDF処理を開始: %s", pdf_path)
    if reasoning_effort not in {"medium", "high"}:
        raise ValueError("年間行事処理の reasoning_effort は medium または high を指定してください。")
    if not model_uses_openai(model):
        raise ValueError("年間行事処理はOpenAI公式APIモデルを指定してください。")

    try:
        pdf_file = Path(pdf_path)
        out_dir.mkdir(parents=True, exist_ok=True)
        images_dir = out_dir / "images"
        json_dir = out_dir / "json"
        final_dir = out_dir / "annual-events"
        for directory in (images_dir, json_dir, final_dir):
            directory.mkdir(parents=True, exist_ok=True)

        logger.info("年間行事PDFを画像化中...")
        pages = render_pdf_pages(str(pdf_file), dpi=dpi)
        images: Dict[str, Any] = {}
        for index, image in enumerate(pages, start=1):
            key = f"page_{index:04d}"
            images[key] = image
            save_image(image, images_dir / f"{key}.png")

        prompt = _build_prompt()
        caller = OpenAICaller(
            model=model,
            api_key=api_key,
            temperature=temperature,
            schema=ANNUAL_EVENTS_SCHEMA,
            reasoning_effort=reasoning_effort,
        )
        response = caller.call_multimodal(prompt, images)
        message = response["choices"][0]["message"]
        refusal = message.get("refusal") if isinstance(message, dict) else None
        if refusal:
            raise ValueError(f"年間行事抽出が拒否されました: {refusal}")
        text = _content_to_text(message.get("content", "") if isinstance(message, dict) else "")
        result_json = extract_json_from_text(text)
        if result_json is None:
            raise ValueError("年間行事抽出結果からJSONを取得できませんでした")

        extracted = _unwrap_result(result_json)
        (json_dir / "extraction.json").write_text(
            json.dumps(extracted, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        normalized = _normalize_payload(
            extracted,
            academic_year_hint=academic_year_hint,
            page_url=page_url,
            pdf_url=pdf_url,
            pdf_hash=pdf_hash,
        )

        for course, payload in normalized.items():
            course_dir = final_dir / course
            course_dir.mkdir(parents=True, exist_ok=True)
            year = payload["academicYear"]
            output_path = course_dir / f"{year}.json"
            output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("年間行事API JSONを保存しました: %s", output_path)

        return normalized
    except Exception as e:
        logger.error("年間行事PDF処理エラー: %s", e, exc_info=True)
        return None
