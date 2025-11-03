#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF処理の共通ロジック
PDF→画像変換、ページ分割、API呼び出しの統合処理
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from PIL import Image

from .image_utils import render_pdf_pages, render_page_to_pil, crop_top_bottom, split_lr, save_image
from .api_client import GeminiCaller, OpenRouterCaller, call_gemini_multimodal
from .json_extractor import extract_json_from_text, deep_merge, JsonType
from .ocr_utils import YomitokuOCR


class PDFProcessor:
    """PDF処理の共通クラス"""
    
    def __init__(
        self,
        model: str,
        api_key: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None,
        dpi: int = 220,
        temperature: float = 0.2,
        max_tokens: int = 2000,
        use_yomitoku: bool = False,
        yomitoku_device: str = "cpu",
        yomitoku_config: Optional[Path] = None,
    ):
        self.model = model
        self.api_key = api_key
        self.schema = schema
        self.dpi = dpi
        self.temperature = temperature
        self.max_tokens = max_tokens
        
        # モデル名に '/' が含まれる場合（例: 'google/gemini-2.5-flash'）は OpenRouter 前提とみなす
        self.use_openrouter = ("/" in model)
        
        if self.use_openrouter:
            self.caller = OpenRouterCaller(
                model=model,
                api_key=api_key,
                temperature=temperature,
                max_tokens=max_tokens
            )
        else:
            self.caller = GeminiCaller(
                model_name=model,
                api_key=api_key,
                schema=schema,
                temperature=temperature
            )
        
        # Yomitoku OCR 準備
        self.ocr: Optional[YomitokuOCR] = None
        if use_yomitoku:
            if yomitoku_config and yomitoku_config.exists():
                try:
                    import yaml
                    ocr_config = yaml.safe_load(yomitoku_config.read_text(encoding="utf-8"))
                except Exception as e:
                    print(f"[Yomitoku] 設定ファイルの読込に失敗しました: {e}")
                    ocr_config = None
            else:
                ocr_config = None
            
            try:
                self.ocr = YomitokuOCR(device=yomitoku_device, config=ocr_config)
                print(f"[Yomitoku] 初期化完了 device={yomitoku_device}")
            except Exception as e:
                print(f"[Yomitoku] 初期化に失敗しました。OCRなしで続行します: {e}")
                self.ocr = None
    
    def process_page(
        self,
        page_num: int,
        page_image: Image.Image,
        prompt: str,
        out_dir: Path,
        call_mode: str = "triple",
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
        # 画像バリアント作成
        if call_mode in ["triple", "single"]:
            top, bottom = crop_top_bottom(page_image)
            left, right = split_lr(page_image)
            variants = {
                "full": page_image,
                "top": top,
                "bottom": bottom,
                "left": left,
                "right": right,
            }
        else:
            variants = {"full": page_image}
        
        # OCR（任意）
        ocr_md_text = ""
        if self.ocr is not None:
            try:
                md_path = out_dir / "pages" / f"page_{page_num:04d}.md"
                ocr_md_text = self.ocr.ocr_page_markdown(page_image, md_save_path=md_path)
            except Exception as e:
                print(f"[Yomitoku] Page {page_num} OCR失敗: {e}")
                ocr_md_text = ""
        
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
        
        # API呼び出し
        if call_mode == "single":
            if self.use_openrouter:
                resp = self.caller.call_multimodal(full_prompt, {"full": variants["full"], "left": variants["left"], "right": variants["right"]})
                text = resp["choices"][0]["message"]["content"]
                if isinstance(text, list):
                    text = "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in text])
                result_json = extract_json_from_text(text)
            else:
                result_json = self.caller.generate(full_prompt, [variants["left"], variants["right"], variants["full"]])
        elif call_mode == "none":
            if self.use_openrouter:
                resp = self.caller.call_multimodal(full_prompt, {"full": variants["full"]})
                text = resp["choices"][0]["message"]["content"]
                if isinstance(text, list):
                    text = "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in text])
                result_json = extract_json_from_text(text)
            else:
                result_json = self.caller.generate(full_prompt, [variants["full"]])
        else:  # triple
            if self.use_openrouter:
                res_original = self.caller.call_multimodal(full_prompt + "\n\n(この入力は: 元画像)", {"full": variants["full"]})
                text_orig = res_original["choices"][0]["message"]["content"]
                if isinstance(text_orig, list):
                    text_orig = "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in text_orig])
                res_original_json = extract_json_from_text(text_orig)
                
                res_left = self.caller.call_multimodal(full_prompt + "\n\n(この入力は: 左半分)", {"left": variants["left"]})
                text_left = res_left["choices"][0]["message"]["content"]
                if isinstance(text_left, list):
                    text_left = "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in text_left])
                res_left_json = extract_json_from_text(text_left)
                
                res_right = self.caller.call_multimodal(full_prompt + "\n\n(この入力は: 右半分)", {"right": variants["right"]})
                text_right = res_right["choices"][0]["message"]["content"]
                if isinstance(text_right, list):
                    text_right = "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in text_right])
                res_right_json = extract_json_from_text(text_right)
                
                if merge_strategy == "bundle":
                    result_json = {"page": page_num, "original": res_original_json, "left": res_left_json, "right": res_right_json}
                else:
                    merged = res_original_json
                    merged = deep_merge(merged, res_left_json)
                    merged = deep_merge(merged, res_right_json)
                    result_json = {"page": page_num, "result": merged}
            else:
                res_original = self.caller.generate(full_prompt + "\n\n(この入力は: 元画像)", [variants["full"]])
                res_left = self.caller.generate(full_prompt + "\n\n(この入力は: 左半分)", [variants["left"]])
                res_right = self.caller.generate(full_prompt + "\n\n(この入力は: 右半分)", [variants["right"]])
                
                if merge_strategy == "bundle":
                    result_json = {"page": page_num, "original": res_original, "left": res_left, "right": res_right}
                else:
                    merged = res_original
                    merged = deep_merge(merged, res_left)
                    merged = deep_merge(merged, res_right)
                    result_json = {"page": page_num, "result": merged}
        
        return result_json

