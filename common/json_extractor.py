#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSON抽出ユーティリティ
APIレスポンスからJSONを抽出する共通処理
"""

import json
import re
import logging
from typing import Any, Optional, Union, Dict, List

logger = logging.getLogger(__name__)


JsonType = Union[Dict[str, Any], List[Any], str, int, float, bool, None]


JSON_BLOCK_RE = re.compile(
    r"(?:```json\s*(?P<json1>\{.*?\})\s*```)|(?P<json2>\{.*\})",
    re.DOTALL,
)


def extract_json_from_text(text: str) -> Optional[Any]:
    """
    回答テキストから最初のJSONブロックを丁寧に抽出。
    1) ```json ... ``` の中身
    2) それがなければ最初の { ... } をナイーブに
    """
    logger.debug(f"JSON抽出を開始: テキスト長={len(text)}文字")
    m = JSON_BLOCK_RE.search(text)
    if not m:
        logger.warning("JSONブロックが見つかりませんでした")
        return None
    blob = m.group("json1") or m.group("json2")
    # 余計なトレーリングカンマなどに優しく:
    try:
        result = json.loads(blob)
        logger.debug("JSON抽出成功")
        return result
    except json.JSONDecodeError as e:
        logger.warning(f"JSONパースエラー、修復を試行中: {e}")
        # よくある壊れ: True/False/None を JSONに寄せる
        fixed = (
            blob.replace("'", '"')
                .replace(" True", " true")
                .replace(" False", " false")
                .replace(" None", " null")
        )
        try:
            result = json.loads(fixed)
            logger.debug("JSON修復成功")
            return result
        except Exception as e2:
            logger.error(f"JSON修復失敗: {e2}")
            return None


def try_json_loads(text: str) -> JsonType:
    """厳密JSONパース。失敗時はJSONらしき部分を抽出して再トライ。"""
    logger.debug(f"JSONパースを開始: テキスト長={len(text)}文字")
    try:
        result = json.loads(text)
        logger.debug("JSONパース成功")
        return result
    except Exception as e:
        logger.debug(f"JSONパース失敗、抽出を試行中: {e}")

    # 最後の { ... } または [ ... ] を強引に抽出
    obj_match = re.findall(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
    logger.debug(f"JSONらしきブロックを{len(obj_match)}件発見")
    for s in reversed(obj_match):
        try:
            result = json.loads(s)
            logger.debug("JSON抽出・パース成功")
            return result
        except Exception as e:
            logger.debug(f"抽出ブロックのパース失敗: {e}")
            continue
    logger.error(f"JSONの抽出/パースに失敗しました。応答:\n{text[:1000]}")
    raise ValueError("JSONの抽出/パースに失敗しました。応答:\n" + text[:1000])


def deep_merge(a: JsonType, b: JsonType) -> JsonType:
    """dictは再帰マージ、listは連結（重複除去）、他はbで上書き。"""
    if isinstance(a, dict) and isinstance(b, dict):
        out = dict(a)
        for k, v in b.items():
            if k in out:
                out[k] = deep_merge(out[k], v)
            else:
                out[k] = v
        return out
    if isinstance(a, list) and isinstance(b, list):
        seen = set()
        merged = []
        for item in a + b:
            key = json.dumps(item, sort_keys=True, ensure_ascii=False) if isinstance(item, (dict, list)) else str(item)
            if key not in seen:
                seen.add(key)
                merged.append(item)
        return merged
    return b

