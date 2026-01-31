#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF処理の共通ロジック
PDF→画像変換、ページ分割、API呼び出しの統合処理
"""

import os
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from PIL import Image

from .image_utils import render_pdf_pages, render_page_to_pil, crop_top_bottom, split_lr, save_image
from .api_client import GeminiCaller, OpenRouterCaller, call_gemini_multimodal
from .json_extractor import extract_json_from_text, deep_merge, JsonType
from .ocr_utils import YomitokuOCR

logger = logging.getLogger(__name__)


class PDFProcessor:
    """PDF処理の共通クラス"""
    
    def __init__(
        self,
        model: str,
        api_key: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None,
        dpi: int = 220,
        temperature: float = 0.2,
        use_yomitoku: bool = False,
        yomitoku_device: str = "cpu",
        yomitoku_config: Optional[Path] = None,
    ):
        logger.info(f"PDFProcessorを初期化中: model={model}, dpi={dpi}, temperature={temperature}, use_yomitoku={use_yomitoku}")
        self.model = model
        self.api_key = api_key
        self.schema = schema
        self.dpi = dpi
        self.temperature = temperature
        
        # モデル名に '/' が含まれる場合（例: 'google/gemini-2.5-flash'）は OpenRouter 前提とみなす
        self.use_openrouter = ("/" in model)
        logger.debug(f"API種類: {'OpenRouter' if self.use_openrouter else 'Gemini'}")
        
        if self.use_openrouter:
            logger.debug("OpenRouterCallerを作成中...")
            self.caller = OpenRouterCaller(
                model=model,
                api_key=api_key,
                temperature=temperature,
                schema=schema,
            )
        else:
            logger.debug("GeminiCallerを作成中...")
            self.caller = GeminiCaller(
                model_name=model,
                api_key=api_key,
                schema=schema,
                temperature=temperature
            )
        
        # Yomitoku OCR 準備
        self.ocr: Optional[YomitokuOCR] = None
        if use_yomitoku:
            logger.info(f"Yomitoku OCRを初期化中: device={yomitoku_device}")
            if yomitoku_config and yomitoku_config.exists():
                try:
                    import yaml
                    ocr_config = yaml.safe_load(yomitoku_config.read_text(encoding="utf-8"))
                    logger.debug(f"Yomitoku設定ファイルを読み込みました: {yomitoku_config}")
                except Exception as e:
                    logger.warning(f"[Yomitoku] 設定ファイルの読込に失敗しました: {e}")
                    ocr_config = None
            else:
                ocr_config = None
            
            try:
                self.ocr = YomitokuOCR(device=yomitoku_device, config=ocr_config)
                logger.info(f"[Yomitoku] 初期化完了 device={yomitoku_device}")
            except Exception as e:
                logger.warning(f"[Yomitoku] 初期化に失敗しました。OCRなしで続行します: {e}")
                self.ocr = None
        else:
            logger.debug("Yomitoku OCRは使用しません")
        
        logger.info("PDFProcessorの初期化が完了しました")
    
    def process_page(
        self,
        page_num: int,
        page_image: Image.Image,
        prompt: str,
        out_dir: Path,
        call_mode: str = "none",
        merge_strategy: str = "deep",
    ) -> JsonType:
        """
        1ページを処理する
        
        Args:
            page_num: ページ番号（1始まり）
            page_image: ページ画像（PIL.Image）
            prompt: プロンプトテキスト
            out_dir: 出力ディレクトリ
            call_mode: "single", "triple", "none" のいずれか
            merge_strategy: "bundle" または "deep"
        
        Returns:
            抽出されたJSONデータ
        """
        logger.info(f"ページ {page_num} を処理中 (call_mode={call_mode}, merge_strategy={merge_strategy})")
        logger.debug(f"画像サイズ: {page_image.size}, プロンプト長: {len(prompt)}文字")
        
        # 画像バリアント作成
        if call_mode in ["triple", "single"]:
            logger.debug("画像バリアントを作成中...")
            top, bottom = crop_top_bottom(page_image)
            left, right = split_lr(page_image)
            variants = {
                "full": page_image,
                "top": top,
                "bottom": bottom,
                "left": left,
                "right": right,
            }
            logger.debug(f"画像バリアント作成完了: {list(variants.keys())}")
        else:
            variants = {"full": page_image}
        
        # OCR（任意）
        ocr_md_text = ""
        if self.ocr is not None:
            logger.debug(f"ページ {page_num} のOCR処理を開始...")
            try:
                md_path = out_dir / "pages" / f"page_{page_num:04d}.md"
                ocr_md_text = self.ocr.ocr_page_markdown(page_image, md_save_path=md_path)
                logger.debug(f"OCR完了: {len(ocr_md_text)}文字のMarkdownを取得")
            except Exception as e:
                logger.warning(f"[Yomitoku] Page {page_num} OCR失敗: {e}")
                ocr_md_text = ""
        else:
            logger.debug("OCRは使用しません")
        
        # プロンプト構築
        prefix = (
            f"あなたは与えられた入力から所定のJSONのみを厳密に返すアシスタントです。\n"
            f"対象は PDF のページ画像です。分析中のページ番号: {page_num}。\n"
            f"以下の追加プロンプトに厳密に従い、JSON以外は一切出力しないでください。"
        )
        
        ocr_section = (
            "\n\n[参考資料: このページのOCR 結果 - Markdown]\n"
            "OCR結果は誤りを含む可能性があります。必要に応じて画像と突き合わせて解釈してください。\n\n"
            f"{ocr_md_text.strip()}\n\n"
            if ocr_md_text.strip() else ""
        )
        
        full_prompt = prefix + "\n\n" + prompt.strip() + ocr_section
        logger.debug(f"プロンプト構築完了: 総長={len(full_prompt)}文字")
        
        # API呼び出し
        logger.info(f"ページ {page_num} のAPI呼び出しを開始 (call_mode={call_mode})")
        if call_mode == "single":
            if self.use_openrouter:
                logger.debug("OpenRouter singleモードで呼び出し中...")
                resp = self.caller.call_multimodal(full_prompt, {"full": variants["full"], "left": variants["left"], "right": variants["right"]})
                text = resp["choices"][0]["message"]["content"]
                if isinstance(text, list):
                    text = "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in text])
                result_json = extract_json_from_text(text)
            else:
                logger.debug("Gemini singleモードで呼び出し中...")
                result_json = self.caller.generate(full_prompt, [variants["left"], variants["right"], variants["full"]])
        elif call_mode == "none":
            if self.use_openrouter:
                logger.debug("OpenRouter noneモードで呼び出し中...")
                resp = self.caller.call_multimodal(full_prompt, {"full": variants["full"]})
                text = resp["choices"][0]["message"]["content"]
                if isinstance(text, list):
                    text = "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in text])
                result_json = extract_json_from_text(text)
            else:
                logger.debug("Gemini noneモードで呼び出し中...")
                result_json = self.caller.generate(full_prompt, [variants["full"]])
        else:  # triple
            logger.debug("tripleモードで複数回呼び出し中...")
            if self.use_openrouter:
                logger.debug("元画像を処理中...")
                res_original = self.caller.call_multimodal(full_prompt + "\n\n(この入力は: 元画像)", {"full": variants["full"]})
                text_orig = res_original["choices"][0]["message"]["content"]
                if isinstance(text_orig, list):
                    text_orig = "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in text_orig])
                res_original_json = extract_json_from_text(text_orig)
                
                logger.debug("左半分を処理中...")
                res_left = self.caller.call_multimodal(full_prompt + "\n\n(この入力は: 左半分)", {"left": variants["left"]})
                text_left = res_left["choices"][0]["message"]["content"]
                if isinstance(text_left, list):
                    text_left = "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in text_left])
                res_left_json = extract_json_from_text(text_left)
                
                logger.debug("右半分を処理中...")
                res_right = self.caller.call_multimodal(full_prompt + "\n\n(この入力は: 右半分)", {"right": variants["right"]})
                text_right = res_right["choices"][0]["message"]["content"]
                if isinstance(text_right, list):
                    text_right = "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in text_right])
                res_right_json = extract_json_from_text(text_right)
                
                if merge_strategy == "bundle":
                    result_json = {"page": page_num, "original": res_original_json, "left": res_left_json, "right": res_right_json}
                else:
                    logger.debug("deepマージを実行中...")
                    merged = res_original_json
                    merged = deep_merge(merged, res_left_json)
                    merged = deep_merge(merged, res_right_json)
                    result_json = {"page": page_num, "result": merged}
            else:
                logger.debug("元画像を処理中...")
                res_original = self.caller.generate(full_prompt + "\n\n(この入力は: 元画像)", [variants["full"]])
                logger.debug("左半分を処理中...")
                res_left = self.caller.generate(full_prompt + "\n\n(この入力は: 左半分)", [variants["left"]])
                logger.debug("右半分を処理中...")
                res_right = self.caller.generate(full_prompt + "\n\n(この入力は: 右半分)", [variants["right"]])
                
                if merge_strategy == "bundle":
                    result_json = {"page": page_num, "original": res_original, "left": res_left, "right": res_right}
                else:
                    logger.debug("deepマージを実行中...")
                    merged = res_original
                    merged = deep_merge(merged, res_left)
                    merged = deep_merge(merged, res_right)
                    result_json = {"page": page_num, "result": merged}
        
        logger.info(f"ページ {page_num} の処理が完了しました")
        return result_json

