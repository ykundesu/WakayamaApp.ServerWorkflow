#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
メニューデータの形式変換
旧形式（Daily Menus）から新形式（All Menus）への変換を行う
"""

import datetime as _dt
from typing import Any, Dict, List, Optional

_ALLOWED_TYPES = {"A", "B", "カレー"}


def _coerce_type(src_type: Optional[str], main: str, subs: List[str]) -> str:
    """
    新スキーマの enum に寄せる。優先度:
    1) すでに A/B/カレー → そのまま
    2) 料理名に『カレー』を含む → 'カレー'
    3) 'a'/'b' のようなケース → 大文字化 ('A'/'B')
    4) 最後の手段として元値 or 'A'
    """
    if src_type in _ALLOWED_TYPES:
        return src_type  # type: ignore[return-value]

    text = (main or "") + " " + " ".join(subs or [])
    if "カレー" in text:
        return "カレー"

    up = (src_type or "").upper()
    if up in {"A", "B"}:
        return up

    return src_type or "A"


def _guess_main_type(item: Dict[str, Any]) -> str:
    """
    新スキーマの mainType を推定。
    ルール:
      - isCurry が True またはテキストに『カレー』 → 'カレー'
      - isRice が True または『ライス』 → 'ライス'
      - 『うどん』→ 'うどん'
      - 『パン』 → 'パン'
      - 上記以外 → 'その他'
    """
    main = item.get("main") or ""
    subs_list: List[str] = item.get("subs") or []
    text = f"{main} {' '.join(subs_list)}"

    # isCarry と isCurry の両方に対応（誤字の可能性を考慮）
    if item.get("isCurry") or item.get("isCarry") or "カレー" in text:
        return "カレー"
    if item.get("isRice") or "ライス" in text:
        return "ライス"
    if "うどん" in text:
        return "うどん"
    if "パン" in text:
        return "パン"
    return "その他"


def _convert_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    旧: MenuItem -> 新: MenuItem
    """
    main: str = item.get("main") or ""
    subs: List[str] = item.get("subs") or []
    nut: Dict[str, Any] = item.get("nutritional") or {}

    return {
        "type": _coerce_type(item.get("type"), main, subs),
        "mainType": _guess_main_type(item),
        "main": main,
        "subs": subs,
        "nutrition": {
            "energyKcal": nut.get("E"),
            "proteinG": nut.get("P"),
            "fatG": nut.get("F"),
            "calciumMg": nut.get("Ca"),
            "saltG": nut.get("S"),
        },
    }


def _convert_meal(meal: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    旧スキーマでは nullable、 新スキーマでは空配列OK なので None → [] に。
    """
    if meal is None:
        return []
    return [_convert_item(it) for it in meal]


def convert_daily_to_all(
    src: Dict[str, Any],
    *,
    base_year: Optional[int] = None,
    convert_to_new_format: bool = True,
) -> Dict[str, Any]:
    """
    旧『Daily Menus』形式の dict を、新『All Menus』形式の dict に変換。

    Parameters
    ----------
    src : dict
        {"menus": [ ... ]} を想定。
    base_year : int | None
        "MM/DD" に年を補完するための年。未指定なら実行年。
    convert_to_new_format : bool
        True の場合、旧形式のMenuItemを新形式に変換する。
        False の場合、形式変換を行わずにそのまま返す（後方互換性のため）。

    Returns
    -------
    dict
        {"allMenus": [ ... ]} 形式。
    """
    if type(src) == list:
        result = {"menus": []}
        for item in src:
            for day in item["menus"]:
                result["menus"].append(day)
        src = result
    menus = src.get("menus")
    if not isinstance(menus, list):
        raise ValueError("src['menus'] は配列である必要があるよ。")

    out_days: List[Dict[str, Any]] = []
    for day in menus:
        if not isinstance(day, dict):
            continue
        mmdd = day.get("day")
        if not isinstance(mmdd, str):
            raise ValueError("各メニューの 'day' は 'MM/DD' 文字列である必要があるよ。")

        # MM/DD を YYYY-MM-DD に変換
        if base_year is None:
            base_year = _dt.date.today().year
        month_str, day_str = mmdd.split("/")
        dt = _dt.date(base_year, int(month_str), int(day_str))
        date_str = dt.strftime("%Y-%m-%d")

        if convert_to_new_format:
            # 新形式に変換
            out_days.append(
                {
                    "date": date_str,
                    "breakfast": _convert_meal(day.get("breakfast")),
                    "lunch": _convert_meal(day.get("lunch")),
                    "dinner": _convert_meal(day.get("dinner")),
                }
            )
        else:
            # 旧形式のまま（後方互換性）
            out_days.append(
                {
                    "date": date_str,
                    "breakfast": day.get("breakfast") or [],
                    "lunch": day.get("lunch") or [],
                    "dinner": day.get("dinner") or [],
                }
            )

    return {"allMenus": out_days}

