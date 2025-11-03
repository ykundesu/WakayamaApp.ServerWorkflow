#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSON抽出ユーティリティ
APIレスポンスからJSONを抽出する共通処理
"""

import json
import re
from typing import Any, Optional, Union, Dict, List


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
    m = JSON_BLOCK_RE.search(text)
    if not m:
        return None
    blob = m.group("json1") or m.group("json2")
    # 余計なトレーリングカンマなどに優しく:
    try:
        return json.loads(blob)
    except json.JSONDecodeError:
        # よくある壊れ: True/False/None を JSONに寄せる
        fixed = (
            blob.replace("'", '"')
                .replace(" True", " true")
                .replace(" False", " false")
                .replace(" None", " null")
        )
        try:
            return json.loads(fixed)
        except Exception:
            return None


def try_json_loads(text: str) -> JsonType:
    """厳密JSONパース。失敗時はJSONらしき部分を抽出して再トライ。"""
    try:
        return json.loads(text)
    except Exception:
        pass

    # 最後の { ... } または [ ... ] を強引に抽出
    obj_match = re.findall(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
    for s in reversed(obj_match):
        try:
            return json.loads(s)
        except Exception:
            continue
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

