#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
画像処理ユーティリティ
PDFレンダリング、画像分割などの共通処理
"""

import io
from typing import List, Tuple
from pathlib import Path

import fitz  # PyMuPDF
from PIL import Image


def render_pdf_pages(pdf_path: str, dpi: int = 200) -> List[Image.Image]:
    """
    PDFを各ページPNG (Pillow) にレンダリング。
    dpiが高いほど解像度↑だが処理重くなる。200〜300くらいが現実的。
    """
    pages = []
    with fitz.open(pdf_path) as doc:
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        for pno in range(len(doc)):
            pix = doc[pno].get_pixmap(matrix=mat, alpha=False)
            im = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            pages.append(im)
    return pages


def render_page_to_pil(page: "fitz.Page", dpi: int = 288) -> Image.Image:
    """PDFページを指定dpiでレンダリングし、PIL.Image を返す。"""
    zoom = dpi / 72.0
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)  # RGB
    # pixmap → PNGバイト → PIL
    img_bytes = pix.tobytes("png")
    return Image.open(io.BytesIO(img_bytes)).convert("RGB")


def crop_top_bottom(im: Image.Image) -> Tuple[Image.Image, Image.Image]:
    """
    画像を高さ方向でちょうど半分にカットして (top, bottom) を返す。
    """
    w, h = im.size
    mid = h // 2
    top = im.crop((0, 0, w, mid))
    bottom = im.crop((0, mid, w, h))
    return top, bottom


def split_lr(img: Image.Image) -> Tuple[Image.Image, Image.Image]:
    """画像を左右2分割して (left, right) を返す。"""
    w, h = img.size
    mid = w // 2
    left = img.crop((0, 0, mid, h))
    right = img.crop((mid, 0, w, h))
    return left, right


def save_image(img: Image.Image, path: Path, quality: int = 95):
    """画像を保存する"""
    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, format="PNG", optimize=True)

