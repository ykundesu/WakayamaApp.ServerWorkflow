#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
授業PDF処理
時間割PDFを処理してfinal/ディレクトリ構造で出力
"""

import os
import json
import logging
import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import fitz  # PyMuPDF

from common.pdf_processor import PDFProcessor
from common.image_utils import render_pdf_pages

logger = logging.getLogger(__name__)


CLASSES_PROMPT = """以下のスキーマで抽出して。
```
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.com/schemas/timetable.schema.json",
  "title": "Timetable",
  "type": "object",
  "additionalProperties": {
    "type": "object",
    "description": "学年オブジェクト（例: '1'）（留学生の場合は'1r'のようにrをつける）",
    "additionalProperties": {
      "type": "array",
      "description": "クラスの時間割（例: 'B'）",
      "items": {
        "type": "object",
        "required": ["day", "classes"],
        "properties": {
          "day": {
            "type": "integer",
            "minimum": 0,
            "maximum": 6,
            "description": "0=月, 1=火, 2=水, 3=木, 4=金, 5=土, 6=日（通常は0〜4）"
          },
          "classes": {
            "type": "array",
            "minItems": 1,
            "items": {
              "type": "object",
              "required": ["start", "end", "name"],
              "properties": {
                "start": {
                  "type": "string",
                  "pattern": "^([01]\\d|2[0-3]):[0-5]\\d$",
                  "description": "開始時刻 (HH:MM)"
                },
                "end": {
                  "type": "string",
                  "pattern": "^([01]\\d|2[0-3]):[0-5]\\d$",
                  "description": "終了時刻 (HH:MM)"
                },
                "name": {
                  "type": "string",
                  "minLength": 1,
                  "description": "科目名"
                },
                "teacher": {
                  "type": ["string", "null"],
                  "description": "教員名（未定ならnull可）"
                }
              },
              "additionalProperties": false
            }
          }
        },
        "additionalProperties": false
      }
    }
  }
}
```"""


def process_classes_pdf(
    pdf_path: str,
    out_dir: Path,
    model: str = "gemini-2.5-pro",
    api_key: Optional[str] = None,
    dpi: int = 220,
    temperature: float = 0.5,
    max_tokens: int = 2000,
    use_yomitoku: bool = False,
    yomitoku_device: str = "cpu",
    yomitoku_config: Optional[Path] = None,
) -> bool:
    """
    授業PDFを処理する
    
    Args:
        pdf_path: PDFファイルパス
        out_dir: 出力ディレクトリ
        model: 使用するモデル名
        api_key: APIキー
        dpi: レンダリングDPI
        temperature: 温度パラメータ
        max_tokens: 最大トークン数
        use_yomitoku: Yomitoku OCRを使用するか
        yomitoku_device: Yomitokuデバイス
        yomitoku_config: Yomitoku設定ファイルパス
    
    Returns:
        処理成功したかどうか
    """
    logger.info(f"授業PDF処理を開始: {pdf_path}")
    logger.debug(f"パラメータ: model={model}, dpi={dpi}, use_yomitoku={use_yomitoku}, out_dir={out_dir}")
    try:
        # 出力ディレクトリ作成
        out_dir.mkdir(parents=True, exist_ok=True)
        img_dir = out_dir / "images"
        img_dir.mkdir(parents=True, exist_ok=True)
        raw_dir = out_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        json_dir = out_dir / "json"
        json_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"出力ディレクトリを作成: img_dir={img_dir}, json_dir={json_dir}, raw_dir={raw_dir}")
        
        # PDFProcessor初期化
        logger.info("PDFProcessorを初期化中...")
        processor = PDFProcessor(
            model=model,
            api_key=api_key,
            schema=None,  # スキーマはプロンプトに含める
            dpi=dpi,
            temperature=temperature,
            max_tokens=max_tokens,
            use_yomitoku=use_yomitoku,
            yomitoku_device=yomitoku_device,
            yomitoku_config=yomitoku_config,
        )
        
        # PDFをレンダリング
        logger.info("PDFをレンダリング中...")
        pages = render_pdf_pages(pdf_path, dpi=dpi)
        logger.info(f"{len(pages)} ページをレンダリングしました。")
        
        # 各ページを処理
        logger.info(f"各ページの処理を開始: 総ページ数={len(pages)}")
        for idx, im in enumerate(pages, start=1):
            # 画像バリアント作成と保存
            from common.image_utils import crop_top_bottom
            top, bottom = crop_top_bottom(im)
            variants = {
                "full": im,
                "top": top,
                "bottom": bottom,
            }
            
            # 各バリアントを保存
            logger.debug(f"ページ {idx} の画像バリアントを保存中...")
            for vname, vim in variants.items():
                save_path = img_dir / f"page{idx:04d}_{vname}.png"
                vim.save(save_path)
            
            label = f"page{idx:04d}"
            logger.info(f"Sending: {label} (full, top, bottom) ({idx}/{len(pages)})...")
            
            try:
                result_json = processor.process_page(
                    page_num=idx,
                    page_image=im,
                    prompt=CLASSES_PROMPT,
                    out_dir=out_dir,
                    call_mode="none",
                    merge_strategy="deep",
                )
                
                # JSON保存
                page_json_path = json_dir / f"{label}.json"
                with open(page_json_path, "w", encoding="utf-8") as f:
                    json.dump(result_json, f, ensure_ascii=False, indent=2)
                logger.debug(f"  -> JSON保存OK: {page_json_path}")
                
            except Exception as e:
                err_txt = str(e)
                error_file = raw_dir / f"{label}.error.txt"
                with open(error_file, "w", encoding="utf-8") as f:
                    f.write(err_txt)
                logger.error(f"  -> ERROR: {e}", exc_info=True)
                continue
        
        logger.info("全ページ処理完了")
        
        # final/ 出力
        logger.info("final/ への書き出しを開始...")
        build_final_outputs(json_dir, out_dir)
        logger.info("final/ への書き出し完了")
        
        logger.info("授業PDF処理が完了しました")
        return True
    except Exception as e:
        logger.exception(f"授業PDF処理エラー: {e}")
        return False


def build_final_outputs(json_dir: Path, out_dir: Path) -> None:
    """
    json/ 配下の page*.json をすべて読み込み、
    final/{cohort}{CLASS}/{grade}_{value}.json 形式で出力
    
    Args:
        json_dir: JSONファイルがあるディレクトリ
        out_dir: 出力ベースディレクトリ
    """
    logger.info("final/ 出力を構築中...")
    final_root = out_dir / "final"
    final_root.mkdir(parents=True, exist_ok=True)
    
    # 統合コンテナ
    merged: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    
    # すべての page*.json を読み込んでマージ
    json_files = list(json_dir.glob("page*.json"))
    logger.debug(f"JSONファイル数: {len(json_files)}")
    for fname in sorted(json_files):
        try:
            logger.debug(f"JSONファイルを読み込み中: {fname}")
            with open(fname, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception as e:
            logger.warning(f"JSONファイルの読み込みに失敗: {fname}, エラー: {e}")
            continue
        
        # obj は { "1": { "B": [ ... ] , ... }, "2": { ... } } の想定
        # または {"page": 1, "result": {...}} の形式
        if "result" in obj:
            obj = obj["result"]
        
        for grade_str, class_map in obj.items():
            if not isinstance(class_map, dict):
                continue
            merged.setdefault(grade_str, {})
            for class_key, timetable_list in class_map.items():
                if not isinstance(timetable_list, list):
                    continue
                merged[grade_str][class_key] = timetable_list
    
    logger.debug(f"マージ結果: {len(merged)}学年分のデータ")
    
    # 現在の年と月から base_year と value_suffix を決定
    now = datetime.datetime.now()
    # 4月を基準に年度を計算
    base_year = now.year if now.month >= 4 else now.year - 1
    # 前期（4-9月）= 0, 後期（10-3月）= 1
    value_suffix = 0 if 4 <= now.month <= 9 else 1
    logger.debug(f"年度情報: base_year={base_year}, value_suffix={value_suffix}")
    
    # 書き出し
    output_count = 0
    for grade_str, class_map in merged.items():
        try:
            grade_num = int(grade_str.replace("r", ""))  # 留学生対応
        except ValueError:
            logger.warning(f"不正な学年文字列をスキップ: {grade_str}")
            continue
        
        cohort_year = base_year - (grade_num - 1)
        
        for class_key, timetable_list in class_map.items():
            class_code = str(class_key).upper()
            cohort_dirname = f"{cohort_year}{class_code}"
            target_dir = final_root / cohort_dirname
            target_dir.mkdir(parents=True, exist_ok=True)
            
            target_fname = f"{grade_num}_{value_suffix}.json"
            target_path = target_dir / target_fname
            
            payload = {"data": timetable_list}
            with open(target_path, "w", encoding="utf-8") as wf:
                json.dump(payload, wf, ensure_ascii=False, indent=2)
            output_count += 1
            logger.debug(f"ファイル出力: {target_path}")
    
    logger.info(f"final/ への書き出し完了: {output_count}ファイル")
