#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF処理の共通ロジック
PDF→画像変換、ページ分割、API呼び出しの統合処理
"""

import os
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from PIL import Image

from .image_utils import render_pdf_pages, render_page_to_pil, crop_top_bottom, split_lr, save_image
from .api_client import (
    GeminiCaller,
    OpenAICaller,
    OpenRouterCaller,
    call_gemini_multimodal,
    model_uses_openai,
    model_uses_openrouter,
)
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
        fallback_model: Optional[str] = None,
        fallback_api_key: Optional[str] = None,
        fallback_openrouter_provider: Optional[Any] = None,
    ):
        logger.info(f"PDFProcessorを初期化中: model={model}, dpi={dpi}, temperature={temperature}, use_yomitoku={use_yomitoku}")
        self.model = model
        self.api_key = api_key
        self.schema = schema
        self.dpi = dpi
        self.temperature = temperature
        
        self.api_provider, self.caller = self._create_caller(
            model=model,
            api_key=api_key,
            schema=schema,
            temperature=temperature,
            openrouter_provider=openrouter_provider,
        )
        self.use_openrouter = self.api_provider == "openrouter"
        self.fallback_model = fallback_model
        self.fallback_api_provider: Optional[str] = None
        self.fallback_caller: Optional[Any] = None
        if fallback_model:
            self.fallback_api_provider, self.fallback_caller = self._create_caller(
                model=fallback_model,
                api_key=fallback_api_key,
                schema=schema,
                temperature=temperature,
                openrouter_provider=fallback_openrouter_provider,
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

    @staticmethod
    def _create_caller(
        model: str,
        api_key: Optional[str],
        schema: Optional[Dict[str, Any]],
        temperature: float,
        openrouter_provider: Optional[Any],
    ) -> Tuple[str, Any]:
        if model_uses_openrouter(model):
            logger.debug("OpenRouterCallerを作成中...")
            return (
                "openrouter",
                OpenRouterCaller(
                    model=model,
                    api_key=api_key,
                    temperature=temperature,
                    schema=schema,
                    provider=openrouter_provider,
                ),
            )
        if model_uses_openai(model):
            logger.debug("OpenAICallerを作成中...")
            return (
                "openai",
                OpenAICaller(
                    model=model,
                    api_key=api_key,
                    temperature=temperature,
                    schema=schema,
                ),
            )
        logger.debug("GeminiCallerを作成中...")
        return (
            "gemini",
            GeminiCaller(
                model_name=model,
                api_key=api_key,
                schema=schema,
                temperature=temperature,
            ),
        )
    
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

        def _call_chat_completion(caller: Any, prompt_text: str, image_variants: Dict[str, Image.Image], label: str) -> JsonType:
            resp = caller.call_multimodal(prompt_text, image_variants)
            message = resp["choices"][0]["message"]
            refusal = message.get("refusal") if isinstance(message, dict) else None
            if refusal:
                raise ValueError(f"{label} の応答が拒否されました: {refusal}")
            text = _content_to_text(message.get("content", "") if isinstance(message, dict) else "")
            result = extract_json_from_text(text)
            if result is None:
                raise ValueError(f"{label} の応答からJSONを抽出できませんでした")
            return result

        def _call_once(caller: Any, api_provider: str) -> JsonType:
            uses_chat_completion = api_provider in {"openrouter", "openai"}
            if call_mode == "single":
                if uses_chat_completion:
                    logger.debug("%s singleモードで呼び出し中...", api_provider)
                    return _call_chat_completion(
                        caller,
                        full_prompt,
                        {"full": variants["full"], "left": variants["left"], "right": variants["right"]},
                        "single",
                    )
                logger.debug("Gemini singleモードで呼び出し中...")
                return caller.generate(full_prompt, [variants["left"], variants["right"], variants["full"]])

            if call_mode == "none":
                if uses_chat_completion:
                    logger.debug("%s noneモードで呼び出し中...", api_provider)
                    return _call_chat_completion(caller, full_prompt, {"full": variants["full"]}, "none")
                logger.debug("Gemini noneモードで呼び出し中...")
                return caller.generate(full_prompt, [variants["full"]])

            logger.debug("tripleモードで複数回呼び出し中...")
            if uses_chat_completion:
                logger.debug("元画像を処理中...")
                res_original_json = _call_chat_completion(
                    caller,
                    full_prompt + "\n\n(この入力は: 元画像)", {"full": variants["full"]}, "元画像"
                )

                logger.debug("左半分を処理中...")
                res_left_json = _call_chat_completion(
                    caller,
                    full_prompt + "\n\n(この入力は: 左半分)", {"left": variants["left"]}, "左半分"
                )

                logger.debug("右半分を処理中...")
                res_right_json = _call_chat_completion(
                    caller,
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
            res_original = caller.generate(full_prompt + "\n\n(この入力は: 元画像)", [variants["full"]])
            logger.debug("左半分を処理中...")
            res_left = caller.generate(full_prompt + "\n\n(この入力は: 左半分)", [variants["left"]])
            logger.debug("右半分を処理中...")
            res_right = caller.generate(full_prompt + "\n\n(この入力は: 右半分)", [variants["right"]])

            if merge_strategy == "bundle":
                return {"page": page_num, "original": res_original, "left": res_left, "right": res_right}

            logger.debug("deepマージを実行中...")
            merged = res_original
            merged = deep_merge(merged, res_left)
            merged = deep_merge(merged, res_right)
            return {"page": page_num, "result": merged}

        def _run_with_retries(caller: Any, api_provider: str, model_label: str) -> JsonType:
            result: JsonType = None
            for attempt in range(1, 4):
                try:
                    if attempt > 1:
                        logger.info(f"ページ {page_num} のAPI呼び出しを再試行します ({model_label}, {attempt}/3)")
                    result = _call_once(caller, api_provider)
                    if result is None:
                        raise ValueError("応答からJSONを抽出できませんでした")
                    return result
                except Exception as e:
                    if attempt >= 3:
                        logger.error(f"ページ {page_num} のAPI呼び出し/JSON抽出に3回失敗しました ({model_label}): {e}")
                        raise
                    logger.warning(f"ページ {page_num} のAPI呼び出し/JSON抽出に失敗しました ({model_label}, {attempt}/3): {e}")
                    time.sleep(min(2 ** attempt, 8))
            return result

        # API呼び出し
        logger.info(f"ページ {page_num} のAPI呼び出しを開始 (call_mode={call_mode})")
        try:
            result_json = _run_with_retries(self.caller, self.api_provider, self.model)
        except Exception as primary_error:
            if self.fallback_caller is None or self.fallback_api_provider is None or not self.fallback_model:
                raise
            logger.warning(
                "ページ %s の処理をフォールバックモデル %s で再試行します: %s",
                page_num,
                self.fallback_model,
                primary_error,
            )
            result_json = _run_with_retries(
                self.fallback_caller,
                self.fallback_api_provider,
                self.fallback_model,
            )
        
        logger.info(f"ページ {page_num} の処理が完了しました")
        return result_json

