#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDFダウンローダー
PDFリンクからPDFをダウンロードし、更新チェックを行う
"""

import os
import hashlib
import logging
import requests
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)


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
    logger.info(f"PDFダウンロードを開始: {url} -> {save_path}")
    try:
        default_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        if headers:
            default_headers.update(headers)
        
        logger.debug(f"リクエスト送信中...")
        response = requests.get(url, headers=default_headers, timeout=30)
        response.raise_for_status()
        logger.debug(f"レスポンス受信: ステータス={response.status_code}, サイズ={len(response.content)}バイト")
        
        # Content-Typeチェック
        content_type = response.headers.get("Content-Type", "").lower()
        if "pdf" not in content_type:
            # 実際の内容を確認
            if not response.content.startswith(b"%PDF"):
                logger.warning(f"Content-TypeがPDFではない、かつPDFマジックナンバーも不一致: {content_type}")
                return False
        
        save_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"ファイルを保存中: {save_path}")
        with open(save_path, "wb") as f:
            f.write(response.content)
        
        logger.info(f"PDFダウンロード完了: {save_path} ({len(response.content)}バイト)")
        return True
    except Exception as e:
        logger.error(f"PDFダウンロードエラー ({url}): {e}", exc_info=True)
        return False


def get_file_hash(file_path: Path) -> Optional[str]:
    """ファイルのSHA256ハッシュを取得"""
    if not file_path.exists():
        logger.debug(f"ファイルが存在しません: {file_path}")
        return None
    
    try:
        logger.debug(f"ファイルのハッシュを計算中: {file_path}")
        with open(file_path, "rb") as f:
            content = f.read()
            hash_value = hashlib.sha256(content).hexdigest()
            logger.debug(f"ハッシュ計算完了: {hash_value[:16]}...")
            return hash_value
    except Exception as e:
        logger.warning(f"ハッシュ計算エラー: {e}")
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
    logger.debug(f"PDF更新チェック: {url} vs {local_path}")
    if not local_path.exists():
        logger.info("ローカルファイルが存在しないため、更新ありと判定")
        return True, None
    
    try:
        # リモートのContent-Lengthを取得（簡易チェック）
        logger.debug("リモートファイルのサイズを確認中...")
        response = requests.head(url, timeout=10)
        remote_size = response.headers.get("Content-Length")
        
        if remote_size:
            local_size = local_path.stat().st_size
            logger.debug(f"サイズ比較: リモート={remote_size}, ローカル={local_size}")
            if int(remote_size) != local_size:
                logger.info(f"ファイルサイズが異なるため、更新ありと判定")
                return True, None
        
        # 実際にダウンロードしてハッシュ比較
        logger.debug("ハッシュ比較のため一時ファイルをダウンロード中...")
        temp_path = local_path.with_suffix(".tmp")
        if download_pdf(url, temp_path):
            new_hash = get_file_hash(temp_path)
            old_hash = get_file_hash(local_path)
            
            logger.debug(f"ハッシュ比較: 新={new_hash[:16] if new_hash else 'None'}..., 旧={old_hash[:16] if old_hash else 'None'}...")
            if new_hash != old_hash:
                logger.info("ハッシュが異なるため、更新ありと判定")
                temp_path.replace(local_path)
                return True, new_hash
            else:
                logger.info("ハッシュが同一のため、更新なしと判定")
                temp_path.unlink(missing_ok=True)
                return False, old_hash
        
        logger.warning("一時ファイルのダウンロードに失敗したため、更新なしと判定")
        return False, get_file_hash(local_path)
    except Exception as e:
        logger.error(f"PDF更新チェックエラー ({url}): {e}", exc_info=True)
        # エラー時は更新ありとみなす
        logger.warning("エラーにより更新ありと判定")
        return True, None


def resolve_url(base_url: str, link: str) -> str:
    """相対URLを絶対URLに変換"""
    return urljoin(base_url, link)

