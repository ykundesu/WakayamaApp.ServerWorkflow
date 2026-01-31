#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""School rules processing pipeline."""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from common.api_client import OpenRouterCaller, call_gemini_multimodal
from common.image_utils import render_pdf_pages
from common.ocr_utils import YomitokuOCR
from scraper.pdf_downloader import download_pdf, get_file_hash
from scraper.school_rules_scraper import RULES_URL, scrape_rules_page

logger = logging.getLogger(__name__)

ARTICLE_LABEL_RE = re.compile(r"^(第[〇零一二三四五六七八九十百千万0-9]+条)")
JSON_BLOCK_RE = re.compile(
    r"(?:```json\s*(?P<json1>\{.*?\})\s*```)|(?P<json2>\{.*\})",
    re.DOTALL,
)
CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

RULES_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "summary": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "sections": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "title": {"type": "string"},
                    "articles": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "label": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                                "content": {"type": "string"},
                            },
                            "required": ["label", "content"],
                        },
                    },
                },
                "required": ["title", "articles"],
            },
        },
        "other_texts": {"anyOf": [{"type": "string"}, {"type": "null"}]},
    },
    "required": ["summary", "sections", "other_texts"],
}


@dataclass
class RuleItem:
    chapter_id: str
    chapter_title: str
    chapter_order: int
    rule_id: str
    rule_title: str
    rule_order: int
    pdf_url: str


def _strip_code_fences(text: str) -> str:
    return re.sub(r"```(?:json)?\s*|```", "", text, flags=re.IGNORECASE)


def _balanced_brace_extract(text: str) -> Optional[str]:
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def _soft_json_fixes(text: str) -> str:
    fixed = text.replace("\r\n", "\n")
    fixed = fixed.replace("\u201c", '"').replace("\u201d", '"').replace("“", '"').replace("”", '"')
    fixed = fixed.replace(" True", " true").replace(" False", " false").replace(" None", " null")
    fixed = re.sub(r",\s*([}\]])", r"\1", fixed)
    return fixed


def _normalize_payload(parsed: Any) -> Optional[Dict[str, Any]]:
    expected_keys = {"summary", "sections", "other_texts"}
    if isinstance(parsed, dict):
        if expected_keys & set(parsed.keys()):
            return parsed
        return None
    if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
        return _normalize_payload(parsed[0])
    return None


def _try_yaml_parse(text: str) -> Optional[Dict[str, Any]]:
    try:
        import yaml
    except Exception:
        return None
    try:
        parsed = yaml.safe_load(text)
    except Exception:
        return None
    return _normalize_payload(parsed)


def extract_json_payload(text: str) -> Optional[Dict[str, Any]]:
    candidates: List[str] = []
    match = JSON_BLOCK_RE.search(text)
    if match:
        candidate = match.group("json1") or match.group("json2") or None
        if candidate:
            candidates.append(candidate)

    stripped = _strip_code_fences(text)
    balanced = _balanced_brace_extract(stripped)
    if balanced:
        candidates.append(balanced)
    if stripped:
        candidates.append(stripped)

    for candidate in candidates:
        if not candidate:
            continue
        for variant in (candidate, _soft_json_fixes(candidate)):
            cleaned = variant.strip()
            if not cleaned:
                continue
            try:
                parsed = json.loads(cleaned)
            except json.JSONDecodeError:
                parsed = None
            normalized = _normalize_payload(parsed) if parsed is not None else None
            if normalized is not None:
                return normalized
            yaml_parsed = _try_yaml_parse(cleaned)
            if yaml_parsed is not None:
                return yaml_parsed

    return None


def sanitize_minimal_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("minimal payload must be an object")

    summary = data.get("summary")
    if summary is None or str(summary).strip() == "":
        summary = None
    else:
        summary = str(summary)

    raw_sections = data.get("sections") or []
    if not isinstance(raw_sections, list):
        raise ValueError("sections must be a list")

    sections: List[Dict[str, Any]] = []
    for raw in raw_sections:
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title") or "")
        raw_articles = raw.get("articles") or []
        if not isinstance(raw_articles, list):
            raise ValueError("articles must be a list")
        articles: List[Dict[str, Any]] = []
        for article in raw_articles:
            if article is None:
                continue
            if isinstance(article, dict):
                label = article.get("label")
                content = article.get("content") or article.get("body") or ""
                articles.append({"label": label, "content": str(content)})
            else:
                text = str(article)
                label, body = split_article_label(text)
                articles.append({"label": label, "content": body})
        sections.append({"title": title, "articles": articles})

    other_texts = data.get("other_texts")
    if other_texts is None:
        other_texts = ""
    other_texts = str(other_texts)

    return {"summary": summary, "sections": sections, "other_texts": other_texts}


def sanitize_rules_markdown(markdown: str, rule_id: str) -> str:
    if not isinstance(markdown, str):
        markdown = str(markdown or "")
    cleaned = CONTROL_CHAR_RE.sub("", markdown)
    if cleaned != markdown:
        logger.warning(
            "Rules markdown sanitized for %s: %s -> %s chars",
            rule_id,
            len(markdown),
            len(cleaned),
        )
    return cleaned


def split_article_label(text: str) -> Tuple[Optional[str], str]:
    stripped = text.strip()
    match = ARTICLE_LABEL_RE.match(stripped)
    if not match:
        return None, stripped
    label = match.group(1)
    remainder = stripped[len(label) :].lstrip(" :：\u3000")
    if remainder:
        return label, remainder
    return label, stripped


def compose_rule_detail(
    rule: RuleItem,
    minimal: Dict[str, Any],
    summary_override: Optional[str] = None,
    last_updated: Optional[str] = None,
) -> Dict[str, Any]:
    article_counter = 1
    sections_output: List[Dict[str, Any]] = []
    fallback_articles: List[Dict[str, Any]] = []

    for raw_section in minimal.get("sections", []):
        title = (raw_section.get("title") or "").strip()
        raw_articles = raw_section.get("articles") or []
        section_articles: List[Dict[str, Any]] = []
        for raw_article in raw_articles:
            if raw_article is None:
                continue
            label = None
            body = ""
            if isinstance(raw_article, dict):
                label = raw_article.get("label")
                body = raw_article.get("content") or raw_article.get("body") or ""
            else:
                text = str(raw_article)
                label, body = split_article_label(text)

            if not body:
                continue

            if isinstance(label, str) and label.strip() == "":
                label = None

            article_id = f"{rule.rule_id}-article-{article_counter:04d}"
            article_counter += 1
            entry = {
                "id": article_id,
                "label": label,
                "body": str(body),
                "notes": None,
                "relatedIds": None,
            }
            if title:
                section_articles.append(entry)
            else:
                fallback_articles.append(entry)

        if title and section_articles:
            sections_output.append({
                "id": f"{rule.rule_id}-section-{len(sections_output) + 1:02d}",
                "title": title,
                "order": len(sections_output) + 1,
                "articles": section_articles,
            })

    other_texts = minimal.get("other_texts")
    if other_texts:
        article_id = f"{rule.rule_id}-article-{article_counter:04d}"
        article_counter += 1
        fallback_articles.append({
            "id": article_id,
            "label": None,
            "body": str(other_texts).strip(),
            "notes": None,
            "relatedIds": None,
        })

    summary = minimal.get("summary")
    if summary is None and summary_override:
        summary = summary_override

    detail = {
        "id": rule.rule_id,
        "chapterId": rule.chapter_id,
        "title": rule.rule_title,
        "summary": summary,
        "order": rule.rule_order,
        "pdfUrl": rule.pdf_url,
        "sourcePage": None,
        "lastUpdated": last_updated,
        "sections": sections_output,
        "articles": fallback_articles,
        "relatedIds": None,
    }
    return detail


class RulesTextCaller:
    def __init__(
        self,
        provider: str,
        model: str,
        gemini_api_key: Optional[str] = None,
        openrouter_api_key: Optional[str] = None,
        temperature: float = 0.2,
    ):
        self.provider = provider
        self.model = model
        self.gemini_api_key = gemini_api_key
        self.temperature = temperature
        self._openrouter: Optional[OpenRouterCaller] = None
        if provider == "openrouter":
            self._openrouter = OpenRouterCaller(
                model=model,
                api_key=openrouter_api_key,
                temperature=temperature,
                schema=RULES_SCHEMA,
            )

    def call(self, prompt: str) -> str:
        if self.provider == "openrouter":
            assert self._openrouter is not None
            response = self._openrouter.call_multimodal(prompt, {})
            content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
            if isinstance(content, list):
                parts: List[str] = []
                for item in content:
                    if isinstance(item, dict) and "text" in item:
                        parts.append(str(item.get("text")))
                    elif item is not None:
                        parts.append(str(item))
                return "".join(parts)
            return str(content or "")

        response = call_gemini_multimodal(
            api_key=self.gemini_api_key or "",
            model=self.model,
            prompt_text=prompt,
            images={},
            temperature=self.temperature,
        )
        candidates = response.get("candidates") or []
        if not candidates:
            return ""
        content = candidates[0].get("content") or {}
        parts = content.get("parts") or []
        texts: List[str] = []
        for part in parts:
            if isinstance(part, dict) and "text" in part:
                texts.append(str(part.get("text")))
        return "".join(texts)


def build_prompt(markdown: str) -> str:
    return (
        "You are an assistant that converts Japanese school regulations written in Markdown into JSON.\n"
        "Return ONLY a single JSON object with this exact schema (no extra text):\n"
        "{\"summary\": string|null, \"sections\": [{\"title\": string, \"articles\": [{\"label\": string|null, \"content\": string}]}], \"other_texts\": string}\n\n"
        "Rules:\n"
        "- summary: one concise Japanese sentence. Use null if unclear.\n"
        "- Preserve Markdown and line breaks in content, including <img> tags.\n"
        "- If a section has no title, use an empty string for title.\n"
        "- If you cannot reliably split an article label, set label to null.\n"
        "- Put introductory or trailing text outside sections into other_texts (empty string if none).\n"
        "- Do NOT include any commentary or extra keys.\n\n"
        "Markdown:\n" + markdown
    )


def _parse_numeric_id(text: str, prefix: str) -> Optional[int]:
    if not text.startswith(prefix):
        return None
    try:
        return int(text.replace(prefix, ""))
    except ValueError:
        return None


def _next_rule_id(used_ids: Set[str], start: int) -> Tuple[str, int]:
    current = start
    while True:
        candidate = f"rule-{current:04d}"
        if candidate not in used_ids:
            used_ids.add(candidate)
            return candidate, current + 1
        current += 1


def load_existing_index(server_repo_path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not server_repo_path:
        return None
    index_path = server_repo_path / "v1" / "school-rules" / "index.json"
    if not index_path.exists():
        return None
    try:
        return json.loads(index_path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to read existing index: %s", exc)
        return None


def build_existing_maps(
    index_data: Optional[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str], Dict[Tuple[str, str], str], Dict[str, str]]:
    rules_by_id: Dict[str, Dict[str, Any]] = {}
    rules_by_url: Dict[str, str] = {}
    rules_by_key: Dict[Tuple[str, str], str] = {}
    chapters_by_title: Dict[str, str] = {}

    if not index_data:
        return rules_by_id, rules_by_url, rules_by_key, chapters_by_title

    for chapter in index_data.get("chapters") or []:
        title = chapter.get("title")
        chapter_id = chapter.get("id")
        if isinstance(title, str) and isinstance(chapter_id, str):
            chapters_by_title.setdefault(title, chapter_id)

    rule_list = index_data.get("rules") or []
    for rule in rule_list:
        rule_id = rule.get("id")
        pdf_url = rule.get("pdfUrl")
        if not isinstance(rule_id, str):
            continue
        rules_by_id[rule_id] = rule
        if isinstance(pdf_url, str):
            rules_by_url[pdf_url] = rule_id

    key_counts: Dict[Tuple[str, str], int] = {}
    for rule in rule_list:
        chapter_id = rule.get("chapterId")
        title = rule.get("title")
        if not isinstance(chapter_id, str) or not isinstance(title, str):
            continue
        chapter_title = None
        for chapter in index_data.get("chapters") or []:
            if chapter.get("id") == chapter_id:
                chapter_title = chapter.get("title")
                break
        if not isinstance(chapter_title, str):
            continue
        key = (chapter_title, title)
        key_counts[key] = key_counts.get(key, 0) + 1

    for rule in rule_list:
        chapter_id = rule.get("chapterId")
        title = rule.get("title")
        rule_id = rule.get("id")
        if not isinstance(chapter_id, str) or not isinstance(title, str) or not isinstance(rule_id, str):
            continue
        chapter_title = None
        for chapter in index_data.get("chapters") or []:
            if chapter.get("id") == chapter_id:
                chapter_title = chapter.get("title")
                break
        if not isinstance(chapter_title, str):
            continue
        key = (chapter_title, title)
        if key_counts.get(key) == 1:
            rules_by_key[key] = rule_id

    return rules_by_id, rules_by_url, rules_by_key, chapters_by_title


def load_existing_rule_detail(server_repo_path: Optional[Path], rule_id: str) -> Optional[Dict[str, Any]]:
    if not server_repo_path:
        return None
    rule_path = server_repo_path / "v1" / "school-rules" / "rules" / f"{rule_id}.json"
    if not rule_path.exists():
        return None
    try:
        return json.loads(rule_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_rule_items(
    structure: Sequence[Dict[str, Any]],
    existing_rules_by_id: Dict[str, Dict[str, Any]],
    existing_rules_by_url: Dict[str, str],
    existing_rules_by_key: Dict[Tuple[str, str], str],
    chapters_by_title: Dict[str, str],
) -> Tuple[List[RuleItem], List[Dict[str, Any]], Set[str]]:
    used_rule_ids = set(existing_rules_by_id.keys())
    max_rule_num = 0
    for rule_id in used_rule_ids:
        num = _parse_numeric_id(rule_id, "rule-")
        if num is not None:
            max_rule_num = max(max_rule_num, num)
    next_rule_num = max_rule_num + 1

    used_chapter_ids = set(chapters_by_title.values())
    max_chapter_num = 0
    for chapter_id in used_chapter_ids:
        num = _parse_numeric_id(chapter_id, "chapter-")
        if num is not None:
            max_chapter_num = max(max_chapter_num, num)
    next_chapter_num = max_chapter_num + 1

    rule_items: List[RuleItem] = []
    chapters_acc: List[Dict[str, Any]] = []
    all_rule_ids: Set[str] = set()

    for chapter_order, chapter in enumerate(structure, start=1):
        chapter_title = str(chapter.get("name") or "")
        chapter_id = chapters_by_title.get(chapter_title)
        if not chapter_id:
            chapter_id = f"chapter-{next_chapter_num:03d}"
            next_chapter_num += 1
            chapters_by_title[chapter_title] = chapter_id
        chapter_entry = {
            "id": chapter_id,
            "title": chapter_title,
            "order": chapter_order,
            "ruleIds": [],
        }

        contents = chapter.get("contents") or []
        for rule_order, rule in enumerate(contents, start=1):
            rule_title = str(rule.get("name") or "")
            pdf_url = str(rule.get("url") or "")

            rule_id = existing_rules_by_url.get(pdf_url)
            if not rule_id:
                key = (chapter_title, rule_title)
                rule_id = existing_rules_by_key.get(key)

            if not rule_id:
                rule_id, next_rule_num = _next_rule_id(used_rule_ids, next_rule_num)

            all_rule_ids.add(rule_id)
            chapter_entry["ruleIds"].append(rule_id)
            rule_items.append(
                RuleItem(
                    chapter_id=chapter_id,
                    chapter_title=chapter_title,
                    chapter_order=chapter_order,
                    rule_id=rule_id,
                    rule_title=rule_title,
                    rule_order=rule_order,
                    pdf_url=pdf_url,
                )
            )

        chapters_acc.append(chapter_entry)

    return rule_items, chapters_acc, all_rule_ids


def render_pdf_to_markdown(
    pdf_path: Path,
    markdown_dir: Path,
    rule_id: str,
    dpi: int,
    ocr: YomitokuOCR,
) -> str:
    markdown_dir.mkdir(parents=True, exist_ok=True)
    pages = render_pdf_pages(str(pdf_path), dpi=dpi)
    parts: List[str] = []
    for index, page in enumerate(pages, start=1):
        page_md_path = markdown_dir / f"{rule_id}_page{index:04d}.md"
        text = ocr.ocr_page_markdown(page, md_save_path=page_md_path)
        if text:
            parts.append(text.strip())
    joined = "\n\n".join(parts).strip()
    combined_path = markdown_dir / f"{rule_id}.md"
    combined_path.write_text(joined, encoding="utf-8")
    return joined


def normalize_rules_models(models: Optional[Sequence[str] | str], fallback_model: str) -> List[str]:
    if not models:
        return [fallback_model]
    if isinstance(models, str):
        raw_items = [models]
    else:
        raw_items = list(models)

    normalized: List[str] = []
    for item in raw_items:
        if not item:
            continue
        for part in str(item).split(","):
            model = part.strip()
            if model:
                normalized.append(model)

    return normalized or [fallback_model]


def request_minimal_payload(
    callers: Sequence[RulesTextCaller],
    markdown: str,
    rule_id: str,
    max_retries: int = 3,
    throttle_sec: float = 1.0,
) -> Optional[Dict[str, Any]]:
    if not markdown or not markdown.strip():
        logger.warning("Rules markdown empty for %s; skipping LLM call", rule_id)
        return None
    cleaned_markdown = sanitize_rules_markdown(markdown, rule_id)
    if not cleaned_markdown.strip():
        logger.warning("Rules markdown empty after sanitize for %s; skipping LLM call", rule_id)
        return None
    base_prompt = build_prompt(cleaned_markdown)
    logger.debug(
        "Rules prompt size: %s chars (markdown: %s chars, sanitized: %s chars)",
        len(base_prompt),
        len(markdown),
        len(cleaned_markdown),
    )
    if not callers:
        return None

    for caller_index, caller in enumerate(callers, start=1):
        for attempt in range(1, max_retries + 1):
            try:
                response_text = caller.call(base_prompt)
            except Exception as exc:  # pragma: no cover - external API
                logger.warning(
                    "LLM call failed for %s model %s attempt %s: %s",
                    rule_id,
                    caller.model,
                    attempt,
                    exc,
                )
                response_text = ""

            parsed = extract_json_payload(response_text)
            if isinstance(parsed, dict):
                try:
                    sanitized = sanitize_minimal_payload(parsed)
                except Exception as exc:
                    logger.warning(
                        "Failed to sanitize payload for %s model %s attempt %s: %s",
                        rule_id,
                        caller.model,
                        attempt,
                        exc,
                    )
                else:
                    if not sanitized.get("sections") and not sanitized.get("other_texts"):
                        logger.warning(
                            "LLM returned empty payload for %s model %s attempt %s",
                            rule_id,
                            caller.model,
                            attempt,
                        )
                    else:
                        return sanitized

            if attempt < max_retries:
                time.sleep(max(throttle_sec, 0.0))

        if caller_index < len(callers):
            logger.warning(
                "Model %s failed after %s attempts for %s. Trying next model.",
                caller.model,
                max_retries,
                rule_id,
            )

    return None


def process_school_rules(
    output_dir: Path,
    api_key: Optional[str],
    model: str = "gemini-2.5-pro",
    models: Optional[Sequence[str] | str] = None,
    dpi: int = 220,
    use_yomitoku: bool = False,
    rules_url: str = RULES_URL,
    processed_hashes: Optional[Set[str]] = None,
    server_repo_path: Optional[Path] = None,
    provider: str = "gemini",
    openrouter_api_key: Optional[str] = None,
) -> Tuple[bool, List[str], bool]:
    """Process school rules PDFs that have been updated."""
    logger.info("School rules processing started")
    if processed_hashes is None:
        processed_hashes = set()

    if provider == "gemini" and not api_key:
        logger.error("Gemini APIキーが指定されていません。")
        return False, [], False

    if provider == "openrouter" and not openrouter_api_key:
        logger.error("OpenRouter APIキーが指定されていません。")
        return False, [], False

    if not use_yomitoku:
        logger.error("School rules processing requires Yomitoku OCR (--use-yomitoku).")
        return False, [], False

    structure = scrape_rules_page(rules_url, pdf_only=True)
    if not structure:
        logger.warning("No rules found on the rules page")
        return True, [], False

    rules_output = output_dir / "rules_output"
    downloads_dir = rules_output / "downloads"
    markdown_dir = rules_output / "markdown"
    rules_dir = rules_output / "rules"
    rules_output.mkdir(parents=True, exist_ok=True)
    downloads_dir.mkdir(parents=True, exist_ok=True)
    markdown_dir.mkdir(parents=True, exist_ok=True)
    rules_dir.mkdir(parents=True, exist_ok=True)

    existing_index = load_existing_index(server_repo_path)
    existing_rules_by_id, existing_rules_by_url, existing_rules_by_key, chapters_by_title = build_existing_maps(existing_index)

    rule_items, chapters_list, all_rule_ids = build_rule_items(
        structure,
        existing_rules_by_id,
        existing_rules_by_url,
        existing_rules_by_key,
        chapters_by_title,
    )

    removed_rule_ids = sorted(set(existing_rules_by_id.keys()) - all_rule_ids)

    model_list = normalize_rules_models(models or model, model)
    logger.info("Rules model candidates: %s", ", ".join(model_list))
    callers = [
        RulesTextCaller(
            provider=provider,
            model=selected_model,
            gemini_api_key=api_key,
            openrouter_api_key=openrouter_api_key,
        )
        for selected_model in model_list
    ]
    ocr = YomitokuOCR(device="cpu")
    generated_at = datetime.now(timezone.utc)
    version = generated_at.strftime("%Y%m%dT%H%M%SZ")

    collected_hashes: List[str] = []
    updated_rule_ids: List[str] = []
    regenerated_rule_ids: List[str] = []
    failed_rule_ids: List[str] = []

    rules_meta: List[Dict[str, Any]] = []

    for rule in rule_items:
        existing_meta = existing_rules_by_id.get(rule.rule_id)
        existing_detail = load_existing_rule_detail(server_repo_path, rule.rule_id)
        existing_pdf_url = existing_meta.get("pdfUrl") if existing_meta else None
        existing_summary = None
        existing_last_updated = None
        if isinstance(existing_detail, dict):
            existing_summary = existing_detail.get("summary")
            existing_last_updated = existing_detail.get("lastUpdated")
        elif existing_meta:
            existing_summary = existing_meta.get("summary")

        pdf_url_changed = existing_pdf_url is None or existing_pdf_url != rule.pdf_url
        metadata_changed = False
        if existing_detail:
            metadata_changed = (
                existing_detail.get("title") != rule.rule_title
                or existing_detail.get("chapterId") != rule.chapter_id
                or existing_detail.get("order") != rule.rule_order
                or existing_detail.get("pdfUrl") != rule.pdf_url
            )

        needs_content_update = pdf_url_changed and bool(rule.pdf_url)
        needs_metadata_update = metadata_changed and existing_detail is not None

        updated_detail: Optional[Dict[str, Any]] = None

        if needs_content_update:
            pdf_path = downloads_dir / f"{rule.rule_id}.pdf"
            temp_path = pdf_path.with_suffix(".tmp")
            if not download_pdf(rule.pdf_url, temp_path):
                logger.error("Failed to download PDF: %s", rule.pdf_url)
                if existing_detail is None:
                    failed_rule_ids.append(rule.rule_id)
                continue
            pdf_hash = get_file_hash(temp_path)

            if pdf_hash and pdf_hash in processed_hashes and existing_detail is not None:
                logger.info("PDF already processed (hash match), skipping content regeneration: %s", rule.rule_id)
                temp_path.replace(pdf_path)
                needs_content_update = False
                if pdf_hash:
                    collected_hashes.append(pdf_hash)
                needs_metadata_update = True
            else:
                temp_path.replace(pdf_path)
                logger.info("Regenerating rule: %s", rule.rule_id)
                markdown_text = render_pdf_to_markdown(pdf_path, markdown_dir, rule.rule_id, dpi, ocr)
                minimal_payload = request_minimal_payload(callers, markdown_text, rule.rule_id)
                if minimal_payload is None:
                    logger.error("Failed to extract JSON for rule %s", rule.rule_id)
                    failed_rule_ids.append(rule.rule_id)
                    continue
                if not minimal_payload.get("sections") and not minimal_payload.get("other_texts"):
                    logger.warning(
                        "LLM payload empty for %s (%s)",
                        rule.rule_id,
                        rule.pdf_url,
                    )
                updated_detail = compose_rule_detail(
                    rule,
                    minimal_payload,
                    summary_override=existing_summary if isinstance(existing_summary, str) else None,
                    last_updated=generated_at.isoformat(),
                )
                regenerated_rule_ids.append(rule.rule_id)
                updated_rule_ids.append(rule.rule_id)
                if pdf_hash:
                    collected_hashes.append(pdf_hash)

        if not needs_content_update and needs_metadata_update and existing_detail:
            updated_detail = dict(existing_detail)
            updated_detail["chapterId"] = rule.chapter_id
            updated_detail["title"] = rule.rule_title
            updated_detail["order"] = rule.rule_order
            updated_detail["pdfUrl"] = rule.pdf_url
            updated_detail.setdefault("summary", existing_summary)
            updated_detail.setdefault("sourcePage", None)
            updated_detail.setdefault("lastUpdated", existing_last_updated)
            updated_rule_ids.append(rule.rule_id)

        if updated_detail:
            if not updated_detail.get("sections") and not updated_detail.get("articles"):
                logger.warning(
                    "Rule content empty after compose: %s (%s)",
                    rule.rule_id,
                    rule.pdf_url,
                )
            rule_json_path = rules_dir / f"{rule.rule_id}.json"
            rule_json_path.write_text(json.dumps(updated_detail, ensure_ascii=False, indent=2), encoding="utf-8")

        rule_summary = None
        if updated_detail:
            rule_summary = updated_detail.get("summary")
        elif isinstance(existing_summary, str):
            rule_summary = existing_summary

        rules_meta.append(
            {
                "id": rule.rule_id,
                "chapterId": rule.chapter_id,
                "title": rule.rule_title,
                "summary": rule_summary,
                "order": rule.rule_order,
                "pdfUrl": rule.pdf_url,
                "sourcePage": None,
                "lastUpdated": updated_detail.get("lastUpdated") if updated_detail else existing_last_updated,
            }
        )

    chapters_payload = {
        "version": version,
        "generatedAt": generated_at.isoformat(),
        "chapters": chapters_list,
    }
    chapter_order_map = {chapter["id"]: chapter["order"] for chapter in chapters_list}
    index_payload = {
        "version": version,
        "generatedAt": generated_at.isoformat(),
        "chapters": chapters_list,
        "rules": sorted(
            rules_meta,
            key=lambda x: (chapter_order_map.get(x["chapterId"], 9999), x["order"]),
        ),
    }

    (rules_output / "structure.json").write_text(
        json.dumps(structure, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (rules_output / "index.json").write_text(
        json.dumps(index_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (rules_output / "chapters.json").write_text(
        json.dumps(chapters_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    manifest = {
        "version": version,
        "generatedAt": generated_at.isoformat(),
        "rulesTotal": len(rule_items),
        "rulesUpdated": len(updated_rule_ids),
        "rulesRegenerated": len(regenerated_rule_ids),
        "rulesFailed": len(failed_rule_ids),
        "updatedRuleIds": updated_rule_ids,
        "regeneratedRuleIds": regenerated_rule_ids,
        "failedRuleIds": failed_rule_ids,
        "removedRuleIds": removed_rule_ids,
        "rulesUrl": rules_url,
        "provider": provider,
        "model": model_list[0],
        "models": model_list,
    }
    (rules_output / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Rules processing done. Updated=%s, regenerated=%s, removed=%s",
        len(updated_rule_ids),
        len(regenerated_rule_ids),
        len(removed_rule_ids),
    )

    logger.info("School rules processing finished")
    for for_rule in failed_rule_ids:
        logger.error("Failed to process rule: %s", for_rule)

    had_error = len(failed_rule_ids) != 0
    return not had_error, collected_hashes, bool(updated_rule_ids or regenerated_rule_ids or removed_rule_ids)
