#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
寮食ページスクレイパー
寮食ページから最新の献立PDFリンクを抽出
"""

import re
import unicodedata
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse


DORMITORY_URL = "https://www.wakayama-nct.ac.jp/campuslife/dormitory/restaurant/"


def extract_pdf_links(html: str, base_url: str) -> List[Dict[str, Any]]:
    """
    HTMLからPDFリンクを抽出
    
    Args:
        html: HTMLコンテンツ
        base_url: ベースURL
    
    Returns:
        PDFリンク情報のリスト（url, text, date を含む）
    """
    soup = BeautifulSoup(html, "html.parser")
    pdf_links = []
    
    # すべてのaタグを検索
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        text = link.get_text(strip=True)
        
        # PDFリンクかチェック
        if href.lower().endswith(".pdf") or ".pdf" in href.lower():
            full_url = urljoin(base_url, href)
            
            # 日付情報を抽出（年月など）
            date_info = extract_date_from_text(text + " " + href)
            
            pdf_links.append({
                "url": full_url,
                "text": text,
                "date": date_info,
            })
    
    return pdf_links


def extract_date_from_text(text: str) -> Optional[str]:
    """
    テキストから日付情報を抽出（年月など）
    
    Args:
        text: 検索対象テキスト
    
    Returns:
        抽出された日付文字列（YYYY-MM形式など）またはNone
    """
    normalized = unicodedata.normalize("NFKC", text) if text else ""
    if not normalized:
        return None

    # よくある区切り文字をまとめて検索
    patterns = [
        r"(\d{4})\s*年\s*(\d{1,2})\s*月",           # 2025年4月
        r"(\d{4})\s*[./_-]\s*(\d{1,2})\s*月?",      # 2025.04月 / 2025-04 など
        r"(\d{4})(\d{2})",                          # 202504
    ]

    for pattern in patterns:
        match = re.search(pattern, normalized)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            if 1 <= month <= 12:
                return f"{year}-{str(month).zfill(2)}"

    return None


def find_latest_pdf_url(html: str, base_url: str) -> Optional[str]:
    """
    最新のPDFリンクを見つける
    
    Args:
        html: HTMLコンテンツ
        base_url: ベースURL
    
    Returns:
        最新のPDFのURL、見つからない場合はNone
    """
    pdf_links = extract_pdf_links(html, base_url)
    
    if not pdf_links:
        return None
    
    # 日付情報でソート（来月→今月以降→過去→不明）
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    if current_month == 12:
        next_year = current_year + 1
        next_month = 1
    else:
        next_year = current_year
        next_month = current_month + 1
    
    def sort_key(link: Dict[str, Any]) -> tuple:
        date_info = link.get("date")
        if date_info:
            try:
                year, month = date_info.split("-")
                year_int = int(year)
                month_int = int(month)
                
                # 来月分を優先
                if year_int == next_year and month_int == next_month:
                    return (0, -year_int, -month_int)  # 最優先
                elif (year_int, month_int) >= (current_year, current_month):
                    return (1, -year_int, -month_int)  # 今月以降
                else:
                    return (2, -year_int, -month_int)  # 過去
            except ValueError:
                pass
        
        return (3, 0, 0)  # 日付情報がないものは最後
    
    sorted_links = sorted(pdf_links, key=sort_key)
    return sorted_links[0]["url"] if sorted_links else None


def scrape_dormitory_page() -> Optional[str]:
    """
    寮食ページをスクレイピングして最新のPDFリンクを取得
    
    Returns:
        最新のPDFのURL、見つからない場合はNone
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        response = requests.get(DORMITORY_URL, headers=headers, timeout=30)
        response.raise_for_status()
        
        return find_latest_pdf_url(response.text, DORMITORY_URL)
    except Exception as e:
        print(f"寮食ページスクレイピングエラー: {e}")
        return None

