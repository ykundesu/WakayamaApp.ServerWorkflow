#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Discord Webhook通知
処理結果をDiscordに送信
"""

import os
import json
import logging
import requests
from typing import Optional, Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)


def send_discord_notification(
    webhook_url: str,
    title: str,
    description: str,
    color: int = 0x3498db,  # デフォルトは青
    fields: Optional[List[Dict[str, Any]]] = None,
    footer: Optional[str] = None,
) -> bool:
    """
    Discord Webhookにメッセージを送信
    
    Args:
        webhook_url: Discord Webhook URL
        title: タイトル
        description: 説明文
        color: カラーコード（整数）
        fields: フィールドのリスト
        footer: フッター
    
    Returns:
        送信成功したかどうか
    """
    logger.info(f"Discord通知を送信中: title={title}")
    logger.debug(f"webhook_url={webhook_url[:30]}..., color={hex(color)}")
    try:
        embed = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        if fields:
            embed["fields"] = fields
            logger.debug(f"フィールド数: {len(fields)}")
        
        if footer:
            embed["footer"] = {"text": footer}
        
        payload = {
            "embeds": [embed]
        }
        
        logger.debug("HTTPリクエスト送信中...")
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Discord通知の送信が完了しました")
        return True
    except Exception as e:
        logger.error(f"Discord通知エラー: {e}", exc_info=True)
        return False


def notify_success(
    webhook_url: str,
    process_type: str,
    details: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    成功通知を送信
    
    Args:
        webhook_url: Discord Webhook URL
        process_type: 処理タイプ（"classes" または "meals"）
        details: 詳細情報の辞書
    
    Returns:
        送信成功したかどうか
    """
    title = f"✅ {process_type}処理成功"
    description = f"{process_type}の処理が正常に完了しました。"
    
    fields = []
    if details:
        for key, value in details.items():
            fields.append({
                "name": key,
                "value": str(value),
                "inline": True,
            })
    
    return send_discord_notification(
        webhook_url=webhook_url,
        title=title,
        description=description,
        color=0x2ecc71,  # 緑
        fields=fields,
    )


def notify_error(
    webhook_url: str,
    process_type: str,
    error_message: str,
    details: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    エラー通知を送信
    
    Args:
        webhook_url: Discord Webhook URL
        process_type: 処理タイプ（"classes" または "meals"）
        error_message: エラーメッセージ
        details: 詳細情報の辞書
    
    Returns:
        送信成功したかどうか
    """
    title = f"❌ {process_type}処理エラー"
    description = f"{process_type}の処理中にエラーが発生しました。\n\n```{error_message}```"
    
    fields = []
    if details:
        for key, value in details.items():
            fields.append({
                "name": key,
                "value": str(value),
                "inline": True,
            })
    
    return send_discord_notification(
        webhook_url=webhook_url,
        title=title,
        description=description,
        color=0xe74c3c,  # 赤
        fields=fields,
    )


def notify_no_update(
    webhook_url: str,
    process_type: str,
    reason: str = "PDFが更新されていません",
) -> bool:
    """
    更新なし通知を送信
    
    Args:
        webhook_url: Discord Webhook URL
        process_type: 処理タイプ（"classes" または "meals"）
        reason: 理由
    
    Returns:
        送信成功したかどうか
    """
    title = f"ℹ️ {process_type}更新なし"
    description = reason
    
    return send_discord_notification(
        webhook_url=webhook_url,
        title=title,
        description=description,
        color=0x95a5a6,  # グレー
    )

