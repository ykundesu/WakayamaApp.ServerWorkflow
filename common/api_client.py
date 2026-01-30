#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
APIクライアント
Gemini/OpenRouter API呼び出しの統一インターフェース
"""

import os
import json
import base64
import io
import logging
from typing import Any, Dict, List, Optional, Union
from PIL import Image

from .json_extractor import try_json_loads, JsonType

logger = logging.getLogger(__name__)

# Gemini API用
try:
    from google import genai
    from google.genai import types
    _gemini_available = True
except ImportError:
    _gemini_available = False

# OpenRouter API用
try:
    import requests
    _requests_available = True
except ImportError:
    _requests_available = False

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, retry_if_exception


def is_503_error(exception: Exception) -> bool:
    """503エラーを検出する関数"""
    error_str = str(exception)
    # エラーメッセージに "503" または "UNAVAILABLE" が含まれているか確認
    if "503" in error_str or "UNAVAILABLE" in error_str:
        return True
    # requests の HTTPError の場合
    if _requests_available:
        import requests
        if isinstance(exception, requests.HTTPError):
            if hasattr(exception, "response") and exception.response is not None:
                if exception.response.status_code == 503:
                    return True
    # エラーオブジェクトに error 属性がある場合（google-genai のエラー形式）
    if hasattr(exception, "error"):
        error_obj = getattr(exception, "error", {})
        if isinstance(error_obj, dict):
            code = error_obj.get("code")
            status = error_obj.get("status")
            if code == 503 or status == "UNAVAILABLE":
                return True
    return False


def pil_to_png_bytes(im: Image.Image) -> bytes:
    buf = io.BytesIO()
    im.save(buf, format="PNG")
    return buf.getvalue()


def img_to_data_url(im: Image.Image) -> str:
    png_bytes = pil_to_png_bytes(im)
    b64 = base64.b64encode(png_bytes).decode("ascii")
    return f"data:image/png;base64,{b64}"


class GeminiCaller:
    """Gemini API呼び出しクラス"""
    
    def __init__(self, model_name: str, api_key: Optional[str] = None,
                 schema: Optional[Dict[str, Any]] = None, temperature: float = 0.6):
        logger.info(f"GeminiCallerを初期化中: model={model_name}, temperature={temperature}")
        if not _gemini_available:
            raise RuntimeError("google-genai パッケージがインストールされていません。")
        
        api_key = api_key or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise RuntimeError("APIキーが見つかりません。--api-key 引数か GOOGLE_API_KEY 環境変数を設定してください。")
        
        logger.debug("Gemini APIクライアントを作成中...")
        self.client = genai.Client(api_key=api_key)
        self.model_name = model_name
        self.schema = schema
        self.temperature = temperature

        # google-genai の GenerateContentConfig を使用
        cfg_kwargs: Dict[str, Any] = {"temperature": temperature}
        if schema:
            # 構造化出力: JSON固定 & JSON Schema を送信
            cfg_kwargs["response_mime_type"] = "application/json"
            cfg_kwargs["response_json_schema"] = schema
            logger.debug("構造化出力JSONスキーマを設定しました")
        self.gen_config = types.GenerateContentConfig(**cfg_kwargs)
        logger.info("GeminiCallerの初期化が完了しました")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=4, min=1, max=120),
           retry=retry_if_exception(is_503_error))
    def generate(self, prompt: str, images: List[Image.Image]) -> JsonType:
        logger.info(f"Gemini API呼び出し中: model={self.model_name}, 画像数={len(images)}")
        logger.debug(f"プロンプト長: {len(prompt)}文字")
        # google-genai: Client 経由で呼ぶ
        parts = [prompt] + images
        try:
            resp = self.client.models.generate_content(
                model=self.model_name,
                contents=parts,
                config=types.GenerateContentConfig(
                    temperature=self.temperature,
                    thinking_config=types.ThinkingConfig(thinking_budget=24576),
                    response_mime_type=self.gen_config.response_mime_type if getattr(self.gen_config, "response_mime_type", None) else None,
                    response_json_schema=self.gen_config.response_json_schema if getattr(self.gen_config, "response_json_schema", None) else None,
                ),
            )
            # 構造化出力時は JSON 文字列になる想定
            text = getattr(resp, "text", None) or (resp.candidates[0].content.parts[0].text if resp and resp.candidates and resp.candidates[0].content.parts else "")
            if not text:
                # 念のため raw を覗く
                text = str(resp)
            logger.debug(f"APIレスポンス受信: レスポンス長={len(text)}文字")
            result = try_json_loads(text)
            logger.info("Gemini API呼び出しが成功しました")
            return result
        except Exception as e:
            if is_503_error(e):
                logger.warning(f"Gemini API呼び出しエラー (503): {e}. リトライします...")
            else:
                logger.error(f"Gemini API呼び出しエラー: {e}", exc_info=True)
            raise


class OpenRouterCaller:
    """OpenRouter API呼び出しクラス"""
    
    OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
    
    def __init__(self, model: str, api_key: Optional[str] = None,
                 temperature: float = 0.2, max_tokens: int = 2000):
        logger.info(f"OpenRouterCallerを初期化中: model={model}, temperature={temperature}, max_tokens={max_tokens}")
        if not _requests_available:
            raise RuntimeError("requests パッケージがインストールされていません。")
        
        api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY が見つからないよ。環境変数 or .env で設定して！")
        
        self.api_key = api_key
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        logger.info("OpenRouterCallerの初期化が完了しました")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10),
           retry=retry_if_exception_type(Exception))
    def call_multimodal(self, prompt_text: str, images: Dict[str, Image.Image],
                       extra_headers: Optional[Dict[str, str]] = None,
                       extra_body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        OpenAI互換のChat Completionsに data URL で画像を渡す。
        モデルは画像対応のものを使うこと（例: openai/gpt-4o-mini 等）。
        """
        logger.info(f"OpenRouter API呼び出し中: model={self.model}, 画像数={len(images)}")
        logger.debug(f"プロンプト長: {len(prompt_text)}文字, 画像キー: {list(images.keys())}")
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if extra_headers:
            headers.update(extra_headers)

        data_urls = []
        for vname, im in images.items():
            data_url = img_to_data_url(im)
            data_urls.append({"type": "image_url", "image_url": {"url": data_url, "detail": "high"}, "id": vname})

        image_refs = " ".join([f"{vname}: <image:{vname}>" for vname in images.keys()])
        full_prompt_content = [{"type": "text", "text": f"{image_refs} {prompt_text}"}] + data_urls

        body = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": full_prompt_content,
                }
            ],
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }
        if extra_body:
            body.update(extra_body)

        try:
            resp = requests.post(self.OPENROUTER_API_URL, headers=headers, data=json.dumps(body), timeout=120)
            resp.raise_for_status()
            result = resp.json()
            logger.info("OpenRouter API呼び出しが成功しました")
            logger.debug(f"レスポンス受信: {len(str(result))}文字")
            return result
        except Exception as e:
            logger.warning(f"OpenRouter API呼び出しエラー: {e}", exc_info=True)
            raise


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=2, max=30),
       retry=retry_if_exception(is_503_error))
def call_gemini_multimodal(
    api_key: str,
    model: str,
    prompt_text: str,
    images: Dict[str, Image.Image],
    temperature: float = 0.2,
    max_tokens: int = 2000,
    extra_headers: Optional[Dict[str, str]] = None,
    extra_body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Google Generative Language API (Gemini) へマルチモーダルで投げる。
    model 例: "gemini-2.5-flash" など。
    """
    logger.info(f"Gemini Multimodal API呼び出し中: model={model}, 画像数={len(images)}")
    logger.debug(f"プロンプト長: {len(prompt_text)}文字, 画像キー: {list(images.keys())}")
    
    def pil_to_b64_png(im: Image.Image) -> str:
        buf = io.BytesIO()
        im.save(buf, format="PNG")
        return base64.b64encode(buf.getvalue()).decode("ascii")

    if not _requests_available:
        raise RuntimeError("requests パッケージがインストールされていません。")

    image_parts: List[Dict[str, Any]] = []
    # full, top, bottom の順を保つ（辞書順だと順不同になりうるため）
    for key in ["full", "top", "bottom"]:
        if key in images:
            image_parts.append({
                "inline_data": {
                    "mime_type": "image/png",
                    "data": pil_to_b64_png(images[key]),
                }
            })
    # 残り（もし他のキーがある場合）
    for vname, vim in images.items():
        if vname in {"full", "top", "bottom"}:
            continue
        image_parts.append({
            "inline_data": {
                "mime_type": "image/png",
                "data": pil_to_b64_png(vim),
            }
        })

    # prompt は text パートとして先頭に置き、後に画像パートを連結
    parts: List[Dict[str, Any]] = [{"text": prompt_text}] + image_parts

    # generationConfig
    gen_cfg: Dict[str, Any] = {
        "temperature": temperature,
        "maxOutputTokens": max_tokens
    }

    if extra_body:
        # ユーザーが追加で上書きしたいフィールドがあれば反映
        gen_cfg.update(extra_body.get("generationConfig", {}))

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    params = {"key": api_key}
    headers = {"Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)

    try:
        resp = requests.post(url, headers=headers, params=params, data=json.dumps({"contents": [{"role": "user", "parts": parts}], "generationConfig": gen_cfg}), timeout=120)
        resp.raise_for_status()
        result = resp.json()
        logger.info("Gemini Multimodal API呼び出しが成功しました")
        return result
    except Exception as e:
        if is_503_error(e):
            logger.warning(f"Gemini Multimodal API呼び出しエラー (503): {e}. リトライします...")
        else:
            logger.error(f"Gemini Multimodal API呼び出しエラー: {e}", exc_info=True)
        raise

