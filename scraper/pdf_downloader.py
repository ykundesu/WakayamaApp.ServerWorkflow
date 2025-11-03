#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDFダウンローダー
PDFリンクからPDFをダウンロードし、更新チェックを行う
"""

import os
import hashlib
import requests
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse


def download_pdf(url: str, save_path: Path, headers: Optional[dict] = None) -> bool:
    """
    PDFをダウンロードする
    
    Args:
        url: PDFのURL
        save_path: 保存先パス
        headers: HTTPリクエストヘッダー
    
    Returns:
        ダウンロード成功したかどうか
    """
    try:
        default_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        if headers:
            default_headers.update(headers)
        
        response = requests.get(url, headers=default_headers, timeout=30)
        response.raise_for_status()
        
        # Content-Typeチェック
        content_type = response.headers.get("Content-Type", "").lower()
        if "pdf" not in content_type:
            # 実際の内容を確認
            if not response.content.startswith(b"%PDF"):
                return False
        
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(response.content)
        
        return True
    except Exception as e:
        print(f"PDFダウンロードエラー ({url}): {e}")
        return False


def get_file_hash(file_path: Path) -> Optional[str]:
    """ファイルのSHA256ハッシュを取得"""
    if not file_path.exists():
        return None
    
    try:
        with open(file_path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except Exception:
        return None


def check_pdf_updated(url: str, local_path: Path) -> Tuple[bool, Optional[str]]:
    """
    PDFが更新されているかチェック
    
    Args:
        url: PDFのURL
        local_path: ローカルのPDFパス
    
    Returns:
        (更新されているか, 新しいハッシュ)
    """
    if not local_path.exists():
        return True, None
    
    try:
        # リモートのContent-Lengthを取得（簡易チェック）
        response = requests.head(url, timeout=10)
        remote_size = response.headers.get("Content-Length")
        
        if remote_size:
            local_size = local_path.stat().st_size
            if int(remote_size) != local_size:
                return True, None
        
        # 実際にダウンロードしてハッシュ比較
        temp_path = local_path.with_suffix(".tmp")
        if download_pdf(url, temp_path):
            new_hash = get_file_hash(temp_path)
            old_hash = get_file_hash(local_path)
            
            if new_hash != old_hash:
                temp_path.replace(local_path)
                return True, new_hash
            else:
                temp_path.unlink(missing_ok=True)
                return False, old_hash
        
        return False, get_file_hash(local_path)
    except Exception as e:
        print(f"PDF更新チェックエラー ({url}): {e}")
        # エラー時は更新ありとみなす
        return True, None


def resolve_url(base_url: str, link: str) -> str:
    """相対URLを絶対URLに変換"""
    return urljoin(base_url, link)

