#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ページ単位 LLM 呼び出しのオーケストレータ。

各 ``processors/*_processor.py`` は ``PDFProcessor`` を介して 1 ページずつ
LLM へ画像 + プロンプトを投げ、返ってきた JSON を集約する。

責務:
    - LLM クライアント (Gemini / OpenRouter) の選択と保持
    - YomitokuOCR の遅延ロード（OCR モード時）
    - ページごとの呼び出しループ + 結果のマージ（``deep_merge``）
    - リトライ / フォールバック制御

設計メモ:
    - ``call_mode`` で「Gemini 構造化出力 / OpenRouter / Gemini 関数版」の 3 系統
      を分岐している。これは現在 ``LLMCaller`` Protocol 化で整理予定。
    - ``schema`` は LLM への ``response_format`` として渡る JSON Schema。
"""

import os
import logging
import time
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
        openrouter_provider: Optional[Any] = None,
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
                provider=openrouter_provider,
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
            f"対象は PDF のページ画像です。\n"
            f"以下の追加プロンプトに厳密に従い、JSON以外は一切出力しないでください。"
        )
        
        ocr_section = (
            "\n\n[参考資料: このページのOCR 結果 - Markdown]\n"
            "OCR結果は誤りを含む可能性があります。必要に応じて画像と突き合わせて解釈してください。画像が常に正しいです。\n\n"
            f"{ocr_md_text.strip()}\n\n"
            if ocr_md_text.strip() else ""
        )
        
        full_prompt = prefix + "\n\n" + prompt.strip() + ocr_section
        logger.debug(f"プロンプト構築完了: 総長={len(full_prompt)}文字")
        
        def _content_to_text(content: Any) -> str:
            if isinstance(content, list):
                return "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in content])
            return str(content)

        def _call_openrouter(prompt_text: str, image_variants: Dict[str, Image.Image], label: str) -> JsonType:
            resp = self.caller.call_multimodal(prompt_text, image_variants)
            text = _content_to_text(resp["choices"][0]["message"]["content"])
            result = extract_json_from_text(text)
            if result is None:
                raise ValueError(f"{label} の応答からJSONを抽出できませんでした")
            return result

        def _call_once() -> JsonType:
            if call_mode == "single":
                if self.use_openrouter:
                    logger.debug("OpenRouter singleモードで呼び出し中...")
                    return _call_openrouter(
                        full_prompt,
                        {"full": variants["full"], "left": variants["left"], "right": variants["right"]},
                        "single",
                    )
                logger.debug("Gemini singleモードで呼び出し中...")
                return self.caller.generate(full_prompt, [variants["left"], variants["right"], variants["full"]])

            if call_mode == "none":
                if self.use_openrouter:
                    logger.debug("OpenRouter noneモードで呼び出し中...")
                    return _call_openrouter(full_prompt, {"full": variants["full"]}, "none")
                logger.debug("Gemini noneモードで呼び出し中...")
                return self.caller.generate(full_prompt, [variants["full"]])

            logger.debug("tripleモードで複数回呼び出し中...")
            if self.use_openrouter:
                logger.debug("元画像を処理中...")
                res_original_json = _call_openrouter(
                    full_prompt + "\n\n(この入力は: 元画像)", {"full": variants["full"]}, "元画像"
                )

                logger.debug("左半分を処理中...")
                res_left_json = _call_openrouter(
                    full_prompt + "\n\n(この入力は: 左半分)", {"left": variants["left"]}, "左半分"
                )

                logger.debug("右半分を処理中...")
                res_right_json = _call_openrouter(
                    full_prompt + "\n\n(この入力は: 右半分)", {"right": variants["right"]}, "右半分"
                )

                if merge_strategy == "bundle":
                    return {"page": page_num, "original": res_original_json, "left": res_left_json, "right": res_right_json}

                logger.debug("deepマージを実行中...")
                merged = res_original_json
                merged = deep_merge(merged, res_left_json)
                merged = deep_merge(merged, res_right_json)
                return {"page": page_num, "result": merged}

            logger.debug("元画像を処理中...")
            res_original = self.caller.generate(full_prompt + "\n\n(この入力は: 元画像)", [variants["full"]])
            logger.debug("左半分を処理中...")
            res_left = self.caller.generate(full_prompt + "\n\n(この入力は: 左半分)", [variants["left"]])
            logger.debug("右半分を処理中...")
            res_right = self.caller.generate(full_prompt + "\n\n(この入力は: 右半分)", [variants["right"]])

            if merge_strategy == "bundle":
                return {"page": page_num, "original": res_original, "left": res_left, "right": res_right}

            logger.debug("deepマージを実行中...")
            merged = res_original
            merged = deep_merge(merged, res_left)
            merged = deep_merge(merged, res_right)
            return {"page": page_num, "result": merged}

        # API呼び出し
        logger.info(f"ページ {page_num} のAPI呼び出しを開始 (call_mode={call_mode})")
        last_error: Optional[Exception] = None
        result_json: JsonType = None
        for attempt in range(1, 4):
            try:
                if attempt > 1:
                    logger.info(f"ページ {page_num} のAPI呼び出しを再試行します ({attempt}/3)")
                result_json = _call_once()
                if result_json is None:
                    raise ValueError("応答からJSONを抽出できませんでした")
                break
            except Exception as e:
                last_error = e
                if attempt >= 3:
                    logger.error(f"ページ {page_num} のAPI呼び出し/JSON抽出に3回失敗しました: {e}")
                    raise
                logger.warning(f"ページ {page_num} のAPI呼び出し/JSON抽出に失敗しました ({attempt}/3): {e}")
                time.sleep(min(2 ** attempt, 8))
        
        logger.info(f"ページ {page_num} の処理が完了しました")
        return result_json

