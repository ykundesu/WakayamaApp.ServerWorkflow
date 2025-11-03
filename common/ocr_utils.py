#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yomitoku OCRユーティリティ
OCR処理の共通化
"""

from typing import Optional, Dict, Any
from pathlib import Path
from PIL import Image

# Yomitoku（任意機能）のための遅延インポート用フラグ
_yomitoku_available = False
try:
    import importlib.util as _impspec
    _yomitoku_available = _impspec.find_spec("yomitoku") is not None
except Exception:
    _yomitoku_available = False

try:
    import numpy as np
    _np_available = True
except Exception:
    _np_available = False

try:
    import cv2
    _cv2_available = True
except Exception:
    _cv2_available = False


class YomitokuOCR:
    """Yomitoku によるページ単位のOCRを行い、Markdown文字列を返す補助クラス。

    初回に heavy な初期化を行い、以降のページでは再利用する。
    """

    def __init__(self, device: str = "cpu", config: Optional[Dict[str, Any]] = None):
        if not _yomitoku_available or not _cv2_available or not _np_available:
            raise RuntimeError(
                "Yomitoku OCR を使用するには 'yomitoku', 'opencv-python', 'numpy' のインストールが必要です。"
            )
        from yomitoku import DocumentAnalyzer  # 遅延 import
        self.DocumentAnalyzer = DocumentAnalyzer
        self.device = device
        self.config = config or {}
        # 可視化は不要、設定を注入
        self.analyzer = self.DocumentAnalyzer(visualize=False, device=device, configs=self.config)

    @staticmethod
    def pil_to_bgr(img: Image.Image) -> "np.ndarray":
        # PIL(RGB) -> np.ndarray(RGB) -> cv2(BGR)
        arr = np.array(img.convert("RGB"))
        bgr = cv2.cvtColor(arr, cv2.COLOR_RGB2BGR)
        return bgr

    def ocr_page_markdown(self, img: Image.Image, md_save_path: Optional[Path] = None) -> str:
        """1ページ分の画像からMarkdownを返す。必要ならmdファイルも保存。

        引数:
          - img: PIL.Image ページ画像（全体）
          - md_save_path: 保存先パス（省略可）
        戻り値:
          - 生成されたMarkdown文字列（空文字の可能性あり）
        """
        bgr = self.pil_to_bgr(img)
        try:
            results, _, _ = self.analyzer(bgr)
        except Exception as e:
            print(f"[Yomitoku] 解析に失敗: {e}")
            return ""

        md_text: Optional[str] = None
        # APIの仕様により to_markdown はファイルへの保存を前提とするため、いったん保存→読込
        if md_save_path is not None:
            try:
                md_save_path.parent.mkdir(parents=True, exist_ok=True)
                results.to_markdown(str(md_save_path), img=bgr)
                md_text = md_save_path.read_text(encoding="utf-8", errors="ignore")
            except Exception as e:
                print(f"[Yomitoku] Markdown保存/読込に失敗: {e}")

        if not md_text:
            # 念のため保存せずに取得できる場合に備えたフォールバック（多くの実装では不可）
            try:
                tmp = Path(md_save_path or "page_tmp.md")
                results.to_markdown(str(tmp), img=bgr)
                md_text = tmp.read_text(encoding="utf-8", errors="ignore")
                if not md_save_path:
                    # 一時ファイルは削除しておく
                    try:
                        tmp.unlink(missing_ok=True)
                    except Exception:
                        pass
            except Exception:
                md_text = ""

        return md_text or ""

