#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ファイル配置管理
出力JSONをWakayamaServerの適切なディレクトリに配置
"""

import shutil
from pathlib import Path
from typing import List, Tuple


def copy_final_files(source_dir: Path, target_dir: Path) -> List[Tuple[Path, Path]]:
    """
    final/ディレクトリ内のファイルをWakayamaServerのv1/classes/にコピー
    
    Args:
        source_dir: ソースディレクトリ（final/を含む）
        target_dir: ターゲットディレクトリ（WakayamaServer/v1/classes/）
    
    Returns:
        コピーされたファイルのリスト（(source, target)のタプル）
    """
    copied_files = []
    final_dir = source_dir / "final"
    
    if not final_dir.exists():
        return copied_files
    
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # final/内のすべてのディレクトリとファイルをコピー
    for cohort_dir in final_dir.iterdir():
        if cohort_dir.is_dir():
            target_cohort_dir = target_dir / cohort_dir.name
            target_cohort_dir.mkdir(parents=True, exist_ok=True)
            
            for json_file in cohort_dir.glob("*.json"):
                target_file = target_cohort_dir / json_file.name
                shutil.copy2(json_file, target_file)
                copied_files.append((json_file, target_file))
    
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
    copied_files = []
    meals_dir = source_dir / "meals"
    
    if not meals_dir.exists():
        return copied_files
    
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # meals/内のすべてのJSONファイルをコピー
    for json_file in meals_dir.glob("*.json"):
        target_file = target_dir / json_file.name
        shutil.copy2(json_file, target_file)
        copied_files.append((json_file, target_file))
    
    return copied_files

