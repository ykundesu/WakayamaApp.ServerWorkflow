#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ファイル配置管理
出力JSONをWakayamaServerの適切なディレクトリに配置
"""

import logging
import shutil
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger(__name__)


def copy_final_files(source_dir: Path, target_dir: Path) -> List[Tuple[Path, Path]]:
    """
    final/ディレクトリ内のファイルをWakayamaServerのv1/classes/にコピー
    
    Args:
        source_dir: ソースディレクトリ（final/を含む）
        target_dir: ターゲットディレクトリ（WakayamaServer/v1/classes/）
    
    Returns:
        コピーされたファイルのリスト（(source, target)のタプル）
    """
    logger.info(f"授業データファイルをコピー中: {source_dir} -> {target_dir}")
    copied_files = []
    final_dir = source_dir / "final"
    
    if not final_dir.exists():
        logger.warning(f"final/ディレクトリが存在しません: {final_dir}")
        return copied_files
    
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # final/内のすべてのディレクトリとファイルをコピー
    cohort_dirs = [d for d in final_dir.iterdir() if d.is_dir()]
    logger.debug(f"コピー対象のcohortディレクトリ数: {len(cohort_dirs)}")
    for cohort_dir in cohort_dirs:
        target_cohort_dir = target_dir / cohort_dir.name
        target_cohort_dir.mkdir(parents=True, exist_ok=True)
        
        json_files = list(cohort_dir.glob("*.json"))
        logger.debug(f"{cohort_dir.name}: {len(json_files)}ファイルをコピー中...")
        for json_file in json_files:
            target_file = target_cohort_dir / json_file.name
            shutil.copy2(json_file, target_file)
            copied_files.append((json_file, target_file))
    
    logger.info(f"授業データファイルコピー完了: {len(copied_files)}ファイル")
    return copied_files


def copy_meals_files(source_dir: Path, target_dir: Path) -> List[Tuple[Path, Path]]:
    """
    meals/ディレクトリ内のファイルをWakayamaServerのv1/meals/にコピー
    
    Args:
        source_dir: ソースディレクトリ（meals/を含む）
        target_dir: ターゲットディレクトリ（WakayamaServer/v1/meals/）
    
    Returns:
        コピーされたファイルのリスト（(source, target)のタプル）
    """
    logger.info(f"寮食データファイルをコピー中: {source_dir} -> {target_dir}")
    copied_files = []
    meals_dir = source_dir / "meals"
    
    if not meals_dir.exists():
        logger.warning(f"meals/ディレクトリが存在しません: {meals_dir}")
        return copied_files
    
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # meals/内のすべてのJSONファイルをコピー
    json_files = list(meals_dir.glob("*.json"))
    logger.debug(f"コピー対象のJSONファイル数: {len(json_files)}")
    for json_file in json_files:
        target_file = target_dir / json_file.name
        shutil.copy2(json_file, target_file)
        copied_files.append((json_file, target_file))
    
    logger.info(f"寮食データファイルコピー完了: {len(copied_files)}ファイル")
    return copied_files

