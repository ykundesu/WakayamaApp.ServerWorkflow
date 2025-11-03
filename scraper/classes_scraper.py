#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
授業ページスクレイパー
授業ページから最新の時間割PDFリンクを抽出
"""

import re
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse


FULLWIDTH_DIGIT_TRANSLATION = str.maketrans({
    "０": "0",
    "１": "1",
    "２": "2",
    "３": "3",
    "４": "4",
    "５": "5",
    "６": "6",
    "７": "7",
    "８": "8",
    "９": "9",
})

ERA_OFFSETS = {
    "令和": 2018,
    "平成": 1988,
    "昭和": 1925,
}

TERM_KEYWORDS = {
    0: (
        "前期",
        "前学期",
        "春学期",
        "第1学期",
        "第一学期",
        "1学期",
    ),
    1: (
        "後期",
        "後学期",
        "秋学期",
        "第2学期",
        "第二学期",
        "2学期",
    ),
}


CLASSES_URL = "https://www.wakayama-nct.ac.jp/campuslife/education/program/"


def normalize_text(text: str) -> str:
    """全角数字を半角に揃える。"""
    return text.translate(FULLWIDTH_DIGIT_TRANSLATION)


def extract_pdf_links(html: str, base_url: str) -> List[Dict[str, Any]]:
    """
    HTMLからPDFリンクを抽出
    
    Args:
        html: HTMLコンテンツ
        base_url: ベースURL
    
    Returns:
        PDFリンク情報のリスト（url, text, year, term を含む）
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
            
            # 年度・学期情報を抽出
            year_term = extract_year_term_from_text(text + " " + href)
            
            pdf_links.append({
                "url": full_url,
                "text": text,
                "year": year_term.get("year"),
                "term": year_term.get("term"),
            })
    
    return pdf_links


def extract_year_term_from_text(text: str) -> Dict[str, Optional[int]]:
    """
    テキストから年度・学期情報を抽出
    
    Args:
        text: 検索対象テキスト
    
    Returns:
        {"year": 年度, "term": 学期} の辞書
    """
    normalized_text = normalize_text(text)
    result = {"year": None, "term": None}

    # 和暦（令和・平成など）を西暦に変換
    for era, offset in ERA_OFFSETS.items():
        match = re.search(rf"{era}(\d+)年[度]?", normalized_text)
        if match:
            era_year = int(match.group(1))
            if era_year > 0:
                result["year"] = offset + era_year
            break
    
    # 年度パターン（2025年度、2025前期、2025後期など）
    year_patterns = [
        r"(\d{4})年度",
        r"(\d{4})年",
    ]
    
    for pattern in year_patterns:
        match = re.search(pattern, normalized_text)
        if match:
            year = int(match.group(1))
            result["year"] = year
    
    # 学期パターン（前期=0, 後期=1）
    lower_text = normalized_text.lower()
    for term_value, keywords in TERM_KEYWORDS.items():
        for keyword in keywords:
            if keyword in normalized_text or keyword.lower() in lower_text:
                result["term"] = term_value
                break
        if result["term"] is not None:
            break
    
    return result


def find_latest_pdf_url(html: str, base_url: str) -> Optional[str]:
    """
    最新のPDFリンクを見つける（来期分を優先）
    
    Args:
        html: HTMLコンテンツ
        base_url: ベースURL
    
    Returns:
        最新のPDFのURL、見つからない場合はNone
    """
    pdf_links = extract_pdf_links(html, base_url)
    
    if not pdf_links:
        return None
    
    # 現在の年度・学期を計算
    now = datetime.now()
    current_year = now.year
    # 4月を基準に年度を計算
    academic_year = current_year if now.month >= 4 else current_year - 1
    # 前期（4-9月）= 0, 後期（10-3月）= 1
    current_term = 0 if 4 <= now.month <= 9 else 1
    
    # 来期を計算
    next_term = 1 - current_term
    next_year = academic_year if next_term == 1 else academic_year + 1
    
    def sort_key(link: Dict[str, Any]) -> tuple:
        year = link.get("year")
        term = link.get("term")
        
        if year is None or term is None:
            return (3, 0, 0)  # 情報がないものは最後
        
        # 来期分を最優先
        if year == next_year and term == next_term:
            return (0, year, term)
        # 今期以降
        elif year == academic_year and term >= current_term:
            return (1, year, term)
        # 過去
        else:
            return (2, year, term)
    
    sorted_links = sorted(pdf_links, key=sort_key)
    return sorted_links[0]["url"] if sorted_links else None


def scrape_classes_page() -> Optional[str]:
    """
    授業ページをスクレイピングして最新のPDFリンクを取得
    
    Returns:
        最新のPDFのURL、見つからない場合はNone
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        response = requests.get(CLASSES_URL, headers=headers, timeout=30)
        response.raise_for_status()
        
        return find_latest_pdf_url(response.text, CLASSES_URL)
    except Exception as e:
        print(f"授業ページスクレイピングエラー: {e}")
        return None

