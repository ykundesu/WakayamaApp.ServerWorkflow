#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
寮食PDF処理
献立PDFを処理して週ごとのJSONに分割してmeals/ディレクトリに出力
"""

import os
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict

from common.pdf_processor import PDFProcessor
from common.image_utils import render_pdf_pages
from common.menu_converter import convert_daily_to_all

logger = logging.getLogger(__name__)


MEALS_PROMPT = """献立の画像を添付してあります。以下のスキーマの形で画像に含まれている一週間の献立を抜き出してください。
上から順に、朝昼晩の食事です。
もし空欄の場合は、そのメニュー(例えばB)は存在しないということです。(休日や祝日の場合に一部メニューが存在しない場合があります。)
また、「共通」に含まれているものは対象のメニューの全てのsubsに含めてください。例えば、共通に味噌汁とライスが指定されている場合、AとBのどちらものsubsに味噌汁,ライスと出力する必要があります。
ただし、朝の場合は朝の中で一番上に記載されているメニューをA/B共にmainとしてください。ライス/パンなどは、それぞれAとBに振り分けて。
```{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.com/menu.schema.json",
  "title": "Daily Menus",
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "menus": {
      "type": "array",
      "minItems": 1,
      "items": { "$ref": "#/$defs/MenuDay" }
    }
  },
  "required": ["menus"],
  "$defs": {
    "Nutritional": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "E": { "type": "number", "minimum": 0, "description": "kcal など" },
        "P": { "type": "number", "minimum": 0 },
        "F": { "type": "number", "minimum": 0 },
        "Ca": { "type": "number", "minimum": 0 },
        "S": { "type": "number", "minimum": 0 }
      },
      "required": ["E", "P", "F", "Ca", "S"]
    },
    "MenuItem": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "type": { "type": "string" },
        "main": { "type": "string" },
        "subs": {
          "type": "array",
          "items": { "type": "string" }
        },
        "isRice": { "type": "boolean" },
        "isCurry": { "type": "boolean" },
        "nutritional": { "$ref": "#/$defs/Nutritional" }
      },
      "required": ["type", "main", "subs", "isRice", "isCurry", "nutritional"]
    },
    "Meal": {
      "type": "array",
      "items": { "$ref": "#/$defs/MenuItem" }
    },
    "MenuDay": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "day": {
          "type": "string",
          "pattern": "^(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])$",
          "description": "MM/DD"
        },
        "breakfast": {
          "type": ["array", "null"],
          "items": { "$ref": "#/$defs/MenuItem" },
          "description": "nullable"
        },
        "lunch": {
          "type": ["array", "null"],
          "items": { "$ref": "#/$defs/MenuItem" },
          "description": "nullable"
        },
        "dinner": {
          "type": ["array", "null"],
          "items": { "$ref": "#/$defs/MenuItem" },
          "description": "nullable"
        }
      },
      "required": ["day"]
    }
  }
}```
提供されている全ての日時のbreakfast, lunch, dinnerのデータを抽出してください。"""


def get_monday_date(date_str: str) -> str:
    """与えられた日付の週の月曜日を取得する"""
    date = datetime.strptime(date_str, '%Y-%m-%d')
    monday = date - timedelta(days=date.weekday())
    return monday.strftime('%Y-%m-%d')


def group_by_week(menus: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """メニューを週ごとにグループ化する"""
    weekly_menus = defaultdict(list)
    
    for menu in menus:
        date_str = menu['date']
        monday_date = get_monday_date(date_str)
        weekly_menus[monday_date].append(menu)
    
    return weekly_menus




def process_meals_pdf(
    pdf_path: str,
    out_dir: Path,
    model: str = "gemini-2.5-pro",
    api_key: Optional[str] = None,
    dpi: int = 288,  # gemini_pdf_pipeline.pyのデフォルト値
    temperature: float = 0.6,
    use_yomitoku: bool = False,
    yomitoku_device: str = "cpu",
    yomitoku_config: Optional[Path] = None,
    prompt_file: Optional[Path] = None,  # プロンプトファイル（オプション）
) -> bool:
    """
    寮食PDFを処理する
    
    Args:
        pdf_path: PDFファイルパス
        out_dir: 出力ディレクトリ
        model: 使用するモデル名
        api_key: APIキー
        dpi: レンダリングDPI
        temperature: 温度パラメータ
        use_yomitoku: Yomitoku OCRを使用するか
        yomitoku_device: Yomitokuデバイス
        yomitoku_config: Yomitoku設定ファイルパス
    
    Returns:
        処理成功したかどうか
    """
    logger.info(f"寮食PDF処理を開始: {pdf_path}")
    logger.debug(f"パラメータ: model={model}, dpi={dpi}, use_yomitoku={use_yomitoku}, out_dir={out_dir}")
    try:
        # 出力ディレクトリ作成
        out_dir.mkdir(parents=True, exist_ok=True)
        json_dir = out_dir / "json"
        json_dir.mkdir(parents=True, exist_ok=True)
        meals_dir = out_dir / "meals"
        meals_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"出力ディレクトリを作成: json_dir={json_dir}, meals_dir={meals_dir}")
        
        # PDFProcessor初期化
        logger.info("PDFProcessorを初期化中...")
        processor = PDFProcessor(
            model=model,
            api_key=api_key,
            schema=None,  # スキーマはプロンプトに含める
            dpi=dpi,
            temperature=temperature,
            use_yomitoku=use_yomitoku,
            yomitoku_device=yomitoku_device,
            yomitoku_config=yomitoku_config,
        )
        
        # プロンプトテキストを決定（ファイルから読み込むか、デフォルトを使用）
        if prompt_file and prompt_file.exists():
            logger.info(f"プロンプトファイルを読み込み中: {prompt_file}")
            prompt_text = prompt_file.read_text(encoding="utf-8")
            logger.debug(f"プロンプト長: {len(prompt_text)}文字")
        else:
            logger.debug("デフォルトプロンプトを使用")
            prompt_text = MEALS_PROMPT
        
        # PDFをレンダリング
        logger.info("PDFをレンダリング中...")
        pages = render_pdf_pages(pdf_path, dpi=dpi)
        logger.info(f"{len(pages)} ページをレンダリングしました。")
        
        # 各ページを処理
        all_menus = []
        logger.info(f"各ページの処理を開始: 総ページ数={len(pages)}")
        for idx, im in enumerate(pages, start=1):
            label = f"page_{idx:04d}"
            logger.info(f"Processing: {label} ({idx}/{len(pages)})...")
            
            try:
                result_json = processor.process_page(
                    page_num=idx,
                    page_image=im,
                    prompt=prompt_text,
                    out_dir=out_dir,
                    call_mode="none",  # RyosokuProcess-Yomitokuの方式: full画像のみ
                    merge_strategy="deep",
                )
                
                # 結果からmenusを抽出
                if isinstance(result_json, dict) and "result" in result_json:
                    menus_data = result_json["result"]
                else:
                    menus_data = result_json
                
                if isinstance(menus_data, dict) and "menus" in menus_data:
                    menu_count = len(menus_data["menus"])
                    all_menus.extend(menus_data["menus"])
                    logger.debug(f"ページ {idx} から {menu_count}件のメニューを抽出")
                
                # JSON保存
                page_json_path = json_dir / f"{label}.json"
                with open(page_json_path, "w", encoding="utf-8") as f:
                    json.dump(result_json, f, ensure_ascii=False, indent=2)
                logger.debug(f"JSON保存OK: {page_json_path}")
                
            except Exception as e:
                logger.error(f"  -> ERROR: {e}", exc_info=True)
                continue
        
        logger.info(f"全ページ処理完了: 合計{len(all_menus)}件のメニューを抽出")
        
        # 週ごとに分割
        if all_menus:
            logger.info("メニューを週ごとにグループ化中...")
            # 旧形式から新形式に変換
            converted = convert_daily_to_all(
                {"menus": all_menus},
                base_year=datetime.now().year,
                convert_to_new_format=True,
            )
            weekly_menus = group_by_week(converted["allMenus"])
            logger.info(f"週ごとのグループ化完了: {len(weekly_menus)}週分")
            
            # 週ごとのメニューを保存
            for monday_date, menus in weekly_menus.items():
                filename = f"{monday_date}.json"
                filepath = meals_dir / filename
                
                week_data = {
                    "week_start": monday_date,
                    "menus": menus
                }
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(week_data, f, ensure_ascii=False, separators=(',', ':'))
                
                logger.info(f"保存しました: {filepath} ({len(menus)}件のメニュー)")
        
        logger.info("寮食PDF処理が完了しました")
        return True
    except Exception as e:
        logger.exception(f"寮食PDF処理エラー: {e}")
        return False
