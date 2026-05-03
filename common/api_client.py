#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""LLM API クライアント（Gemini / OpenRouter 両対応）。

このモジュールは ``processors/`` から呼び出される LLM クライアントの集約点。
モデル名の `/` 有無で OpenRouter / Gemini を切り分けて使う運用 (main.py の
``model_uses_openrouter()`` 参照) を前提に、両プロバイダ向けのクラス・関数を
ここに集めている。

公開 API:
    - ``GeminiCaller``           : google-genai 経由で構造化出力 (JSON Schema) を強制
    - ``OpenRouterCaller``       : OpenAI 互換 Chat Completions の thin client
    - ``call_gemini_multimodal`` : ``GeminiCaller`` に依存しない関数版（rules で利用）
    - ``is_503_error``           : tenacity の retry 判定で使う 503 / UNAVAILABLE 判別

設計メモ:
    - 共通インターフェイス（Protocol）はまだ存在しない。``processors/`` 側で
      ``use_openrouter`` フラグの分岐を持っており、改善ロードマップで
      ``LLMCaller`` Protocol への統一を予定（``docs/architecture.md`` 参照）。
    - リトライは ``tenacity`` を使い、503 / UNAVAILABLE のみ自動再試行する。
      JSON Schema 不整合のような恒久エラーまでリトライしないようにするため、
      ``is_503_error`` で限定している。
    - ``Retry-After`` および ``X-RateLimit-Reset[-Requests]`` ヘッダを尊重する
      （``_retry_after_from_headers`` 参照）。OpenRouter / Gemini ともに
      これらのヘッダで明示された待ち時間が来た場合、tenacity の指数バックオフ
      よりこちらを優先したい意図がある。
"""

import os
import json
import base64
import io
import logging
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Union, Mapping, cast
from PIL import Image

from .json_extractor import try_json_loads, JsonType

logger = logging.getLogger(__name__)

# 2001-09-09 (UNIX 時刻 1,000,000,000) 以後の数値はエポック秒、それ以前の値は
# 「相対秒数」として解釈する。OpenRouter / Gemini どちらの RateLimit ヘッダでも
# 「次にリクエスト可能になる時刻」をエポック秒で返す実装と「待つべき秒数」を
# 返す実装が混在しているため、この閾値で機械的に判別している。
EPOCH_THRESHOLD_SECONDS = 1_000_000_000

# Gemini API用
try:
    from google import genai
    from google.genai import types
    _gemini_available = True
except ImportError:
    genai = None
    types = None
    _gemini_available = False

# OpenRouter API用（OpenAI 互換 REST を直接叩く形なので requests のみで足りる）
try:
    import requests
    _requests_available = True
except ImportError:
    requests = None
    _requests_available = False

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception


def is_503_error(exception: BaseException) -> bool:
    """503エラーを検出する関数"""
    error_str = str(exception)
    # エラーメッセージに "503" または "UNAVAILABLE" が含まれているか確認
    if "503" in error_str or "UNAVAILABLE" in error_str:
        return True
    # requests の HTTPError の場合
    if _requests_available and requests is not None:
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


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except Exception:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())


def _parse_rate_limit_reset(value: Optional[str]) -> Optional[float]:
    """``X-RateLimit-Reset`` 系ヘッダを「待つべき秒数」に正規化する。

    値の意味がプロバイダ実装ごとに異なる:
        - エポック秒（"次に実行可能になる絶対時刻"）
        - 相対秒数（"今から何秒待てばよいか"）
    どちらかは ``EPOCH_THRESHOLD_SECONDS`` を境に機械判定する。
    返り値は常に「今から何秒待てばよいか」（>=0）。
    """
    if not value:
        return None
    try:
        reset = float(value)
    except ValueError:
        return None
    if reset > EPOCH_THRESHOLD_SECONDS:
        # 絶対時刻（エポック秒）として解釈し、現在時刻との差分を返す。
        return max(0.0, reset - time.time())
    # 相対秒数として解釈する。負値は 0 にクリップ。
    return max(0.0, reset)


def _retry_after_from_headers(headers: Mapping[str, str]) -> Optional[float]:
    retry_after = _parse_retry_after(headers.get("Retry-After"))
    if retry_after is not None:
        return retry_after
    return (
        _parse_rate_limit_reset(headers.get("X-RateLimit-Reset"))
        or _parse_rate_limit_reset(headers.get("X-RateLimit-Reset-Requests"))
    )


def _normalize_openrouter_provider(
    raw: Optional[Union[str, Mapping[str, Any]]],
    env_var: Optional[str] = "OPENROUTER_PROVIDER",
) -> Optional[Dict[str, Any]]:
    if raw is None and env_var:
        raw = os.getenv(env_var)
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str):
        value = raw.strip()
        if not value:
            return None
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            order = [part.strip() for part in value.split(",") if part.strip()]
            if order:
                return {"order": order}
            logger.warning("OpenRouter provider filter must be JSON: %s", value)
            return None
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            order = [str(item).strip() for item in parsed if str(item).strip()]
            if order:
                return {"order": order}
            return None
        logger.warning(
            "OpenRouter provider filter must be JSON object or array, got %s",
            type(parsed).__name__,
        )
        return None
    logger.warning(
        "OpenRouter provider filter must be dict or JSON string, got %s",
        type(raw).__name__,
    )
    return None


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
        assert genai is not None
        assert types is not None
        
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
        assert types is not None
        # google-genai: Client 経由で呼ぶ
        parts: List[Any] = [prompt] + images
        try:
            resp = self.client.models.generate_content(
                model=self.model_name,
                contents=cast(Any, parts),
                config=types.GenerateContentConfig(
                    temperature=self.temperature,
                    thinking_config=types.ThinkingConfig(thinking_budget=24576),
                    response_mime_type=self.gen_config.response_mime_type if getattr(self.gen_config, "response_mime_type", None) else None,
                    response_json_schema=self.gen_config.response_json_schema if getattr(self.gen_config, "response_json_schema", None) else None,
                ),
            )
            resp = cast(Any, resp)
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

    def __init__(
        self,
        model: str,
        api_key: Optional[str] = None,
        temperature: float = 0.2,
        schema: Optional[Dict[str, Any]] = None,
        provider: Optional[Union[str, Mapping[str, Any]]] = None,
        provider_env_var: Optional[str] = "OPENROUTER_PROVIDER",
    ):
        logger.info(f"OpenRouterCallerを初期化中: model={model}, temperature={temperature}")
        if not _requests_available:
            raise RuntimeError("requests パッケージがインストールされていません。")
        
        api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY が見つからないよ。環境変数 or .env で設定して！")
        
        self.api_key = api_key
        self.model = model
        self.temperature = temperature
        self.schema = schema
        self.provider = _normalize_openrouter_provider(provider, provider_env_var)
        self.response_format = self._build_response_format(schema)
        logger.info("OpenRouterCallerの初期化が完了しました")

    @staticmethod
    def _build_response_format(schema: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not schema:
            return None
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "output",
                "schema": schema,
                "strict": True,
            },
        }

    @staticmethod
    def _ensure_response_healing_plugin(body: Dict[str, Any]) -> None:
        response_format = body.get("response_format")
        if not isinstance(response_format, dict):
            return
        if response_format.get("type") not in {"json_schema", "json_object"}:
            return

        plugins = body.setdefault("plugins", [])
        if not isinstance(plugins, list):
            logger.warning("OpenRouter plugins must be a list; response-healing plugin was not added")
            return
        if any(isinstance(plugin, dict) and plugin.get("id") == "response-healing" for plugin in plugins):
            return
        plugins.append({"id": "response-healing"})

    def call_multimodal(self, prompt_text: str, images: Dict[str, Image.Image],
                       extra_headers: Optional[Dict[str, str]] = None,
                       extra_body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        OpenAI互換のChat Completionsに data URL で画像を渡す。
        モデルは画像対応のものを使うこと（例: openai/gpt-4o-mini 等）。
        """
        logger.info(f"OpenRouter API呼び出し中: model={self.model}, 画像数={len(images)}")
        logger.debug(f"プロンプト長: {len(prompt_text)}文字, 画像キー: {list(images.keys())}")
        assert requests is not None
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

        if data_urls:
            image_refs = " ".join([f"{vname}: <image:{vname}>" for vname in images.keys()])
            prompt = f"{image_refs} {prompt_text}".strip()
            full_prompt_content = [{"type": "text", "text": prompt}] + data_urls
        else:
            full_prompt_content = prompt_text

        body = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": full_prompt_content,
                }
            ],
            "temperature": self.temperature,
        }
        if self.provider is not None and "provider" not in body:
            body["provider"] = self.provider
        if extra_body:
            body.update(extra_body)
        if self.response_format and "response_format" not in body:
            body["response_format"] = self.response_format
        self._ensure_response_healing_plugin(body)

        def _redact_body_for_log(payload: Dict[str, Any]) -> Dict[str, Any]:
            try:
                redacted = json.loads(json.dumps(payload, ensure_ascii=False))
            except Exception:
                redacted = dict(payload)
            messages = redacted.get("messages")
            if isinstance(messages, list):
                for message in messages:
                    if not isinstance(message, dict):
                        continue
                    content = message.get("content")
                    if isinstance(content, list):
                        for item in content:
                            if not isinstance(item, dict):
                                continue
                            if item.get("type") == "image_url":
                                image_url = item.get("image_url")
                                if isinstance(image_url, dict):
                                    url = image_url.get("url")
                                    if isinstance(url, str):
                                        image_url["url"] = f"<omitted {len(url)} chars>"
            return redacted

        logger.info(
            "OpenRouter request body (redacted): %s",
            json.dumps(_redact_body_for_log(body), ensure_ascii=False),
        )

        max_attempts = 5
        base_delay = 2.0
        max_delay = 60.0

        for attempt in range(1, max_attempts + 1):
            try:
                resp = requests.post(
                    self.OPENROUTER_API_URL,
                    headers=headers,
                    data=json.dumps(body),
                    timeout=120,
                )
                if resp.status_code in {429, 503}:
                    retry_after = _retry_after_from_headers(resp.headers)
                    wait_seconds = retry_after if retry_after is not None else min(max_delay, base_delay * (2 ** (attempt - 1)))
                    wait_seconds = min(max_delay, wait_seconds + random.uniform(0.0, 0.5))
                    if attempt < max_attempts:
                        logger.warning(
                            "OpenRouter APIがレート制限/過負荷です (status=%s)。%.1f秒待機して再試行します (%s/%s)",
                            resp.status_code,
                            wait_seconds,
                            attempt,
                            max_attempts,
                        )
                        time.sleep(wait_seconds)
                        continue
                resp.raise_for_status()
                result = resp.json()
                logger.info("OpenRouter API呼び出しが成功しました")
                logger.debug(f"レスポンス受信: {len(str(result))}文字")
                return result
            except requests.RequestException as e:
                if attempt < max_attempts:
                    wait_seconds = min(max_delay, base_delay * (2 ** (attempt - 1)))
                    wait_seconds = min(max_delay, wait_seconds + random.uniform(0.0, 0.5))
                    logger.warning(
                        "OpenRouter API呼び出しエラー: %s。%.1f秒待機して再試行します (%s/%s)",
                        e,
                        wait_seconds,
                        attempt,
                        max_attempts,
                    )
                    time.sleep(wait_seconds)
                    continue
                logger.warning("OpenRouter API呼び出しエラー: %s", e, exc_info=True)
                raise

        raise RuntimeError("OpenRouter API呼び出しが失敗しました。")


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=2, max=30),
       retry=retry_if_exception(is_503_error))
def call_gemini_multimodal(
    api_key: str,
    model: str,
    prompt_text: str,
    images: Dict[str, Image.Image],
    temperature: float = 0.2,
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
    assert requests is not None

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
    }

    if extra_body:
        # ユーザーが追加で上書きしたいフィールドがあれば反映
        gen_cfg.update(extra_body.get("generationConfig", {}))
    gen_cfg.pop("maxOutputTokens", None)

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

