#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ファイル配置管理
出力JSONをWakayamaServerの適切なディレクトリに配置
"""

import json
import logging
import shutil
from pathlib import Path
from typing import Dict, List, Literal, Optional, Set, Tuple

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
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # source_dir（例: meals_output）配下の各サブディレクトリにある meals/*.json を再帰的にコピー
    # 想定構造: {source_dir}/{label}/meals/*.json
    found_any = False
    for label_dir in source_dir.iterdir() if source_dir.exists() else []:
        if not label_dir.is_dir():
            continue
        meals_dir = label_dir / "meals"
        if not meals_dir.exists():
            logger.debug(f"meals/ディレクトリが見つかりません: {meals_dir}")
            continue
        json_files = list(meals_dir.glob("*.json"))
        logger.debug(f"{label_dir.name}: コピー対象のJSONファイル数: {len(json_files)}")
        for json_file in json_files:
            target_file = target_dir / json_file.name
            shutil.copy2(json_file, target_file)
            copied_files.append((json_file, target_file))
            found_any = True
    
    if not found_any:
        # 旧構造: source_dir/meals/*.json にも一応対応
        fallback_meals_dir = source_dir / "meals"
        if fallback_meals_dir.exists():
            json_files = list(fallback_meals_dir.glob("*.json"))
            logger.debug(f"fallback構造: コピー対象のJSONファイル数: {len(json_files)}")
            for json_file in json_files:
                target_file = target_dir / json_file.name
                shutil.copy2(json_file, target_file)
                copied_files.append((json_file, target_file))
        else:
            logger.warning(f"meals/ディレクトリが存在しません: {fallback_meals_dir}")
    
    logger.info(f"寮食データファイルコピー完了: {len(copied_files)}ファイル")
    return copied_files


def copy_dormitory_events_files(source_dir: Path, target_dir: Path) -> List[Tuple[Path, Path]]:
    """
    events/ディレクトリ内のファイルをWakayamaServerのv1/dormitory/events/にコピー
    """
    logger.info(f"寮行事データファイルをコピー中: {source_dir} -> {target_dir}")
    copied_files = []
    events_dir = source_dir / "events"

    if not events_dir.exists():
        logger.warning(f"events/ディレクトリが存在しません: {events_dir}")
        return copied_files

    target_dir.mkdir(parents=True, exist_ok=True)
    json_files = list(events_dir.glob("*.json"))
    logger.debug(f"コピー対象のJSONファイル数: {len(json_files)}")
    for json_file in json_files:
        target_file = target_dir / json_file.name
        shutil.copy2(json_file, target_file)
        copied_files.append((json_file, target_file))

    logger.info(f"寮行事データファイルコピー完了: {len(copied_files)}ファイル")
    return copied_files


def load_processed_hashes(server_repo_path: Path, target_name: Literal["meals", "classes", "school_rules"]) -> Set[str]:
    """
    サーバーリポジトリから処理済みハッシュを読み込む
    
    Args:
        server_repo_path: サーバーリポジトリのパス
        target_name: "meals" または "classes" または "school_rules"
    
    Returns:
        処理済みハッシュの集合
    """
    hash_file = server_repo_path / "v1" / "sources" / "list" / f"{target_name}.json"
    
    if not hash_file.exists():
        logger.debug(f"処理済みハッシュファイルが存在しません: {hash_file}")
        return set()
    
    try:
        with open(hash_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            processed = data.get("processed", [])
            if not isinstance(processed, list):
                logger.warning(f"処理済みハッシュの形式が不正です: {hash_file}")
                return set()
            logger.debug(f"処理済みハッシュを読み込みました: {target_name} ({len(processed)}件)")
            return set(processed)
    except json.JSONDecodeError as e:
        logger.warning(f"処理済みハッシュファイルのJSON解析に失敗しました: {hash_file}, エラー: {e}")
        return set()
    except Exception as e:
        logger.warning(f"処理済みハッシュファイルの読み込みに失敗しました: {hash_file}, エラー: {e}")
        return set()


def load_dormitory_events_state(server_repo_path: Path) -> Dict[str, Optional[str]]:
    state_file = server_repo_path / "v1" / "sources" / "list" / "dormitory_events.json"
    if not state_file.exists():
        logger.debug(f"寮行事状態ファイルが存在しません: {state_file}")
        return {}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        last_url = data.get("last_url")
        last_hash = data.get("last_hash")
        return {
            "last_url": last_url if isinstance(last_url, str) else None,
            "last_hash": last_hash if isinstance(last_hash, str) else None,
        }
    except json.JSONDecodeError as e:
        logger.warning(f"寮行事状態ファイルのJSON解析に失敗しました: {state_file}, エラー: {e}")
        return {}
    except Exception as e:
        logger.warning(f"寮行事状態ファイルの読み込みに失敗しました: {state_file}, エラー: {e}")
        return {}


def merge_and_write_processed_hashes(
    source_hash_file: Path,
    server_repo_path: Path,
    target_name: Literal["meals", "classes", "school_rules"],
) -> Optional[Path]:
    """
    処理済みハッシュをマージしてサーバーリポジトリに書き込む
    
    Args:
        source_hash_file: ソースハッシュファイル（output/*_hashes.json）
        server_repo_path: サーバーリポジトリのパス
        target_name: "meals" または "classes" または "school_rules"
    
    Returns:
        更新されたファイルパス（変更がなかった場合はNone）
    """
    if not source_hash_file.exists():
        logger.debug(f"ソースハッシュファイルが存在しません: {source_hash_file}")
        return None
    
    # ソースファイルを読み込む
    try:
        with open(source_hash_file, "r", encoding="utf-8") as f:
            source_data = json.load(f)
            source_hashes = source_data.get("processed", [])
            if not isinstance(source_hashes, list):
                logger.warning(f"ソースハッシュファイルの形式が不正です: {source_hash_file}")
                return None
    except json.JSONDecodeError as e:
        logger.warning(f"ソースハッシュファイルのJSON解析に失敗しました: {source_hash_file}, エラー: {e}")
        return None
    except Exception as e:
        logger.warning(f"ソースハッシュファイルの読み込みに失敗しました: {source_hash_file}, エラー: {e}")
        return None
    
    if not source_hashes:
        logger.debug(f"ソースハッシュファイルにハッシュが含まれていません: {source_hash_file}")
        return None
    
    # サーバー側ファイルを読み込む
    target_file = server_repo_path / "v1" / "sources" / "list" / f"{target_name}.json"
    existing_hashes = set()
    
    if target_file.exists():
        try:
            with open(target_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                existing_hashes = set(existing_data.get("processed", []))
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"サーバー側ハッシュファイルの読み込みに失敗しました: {target_file}, エラー: {e}")
            existing_hashes = set()
    
    # マージ（重複除去）
    merged_hashes = existing_hashes | set(source_hashes)
    
    # 変更がない場合はスキップ
    if merged_hashes == existing_hashes:
        logger.debug(f"ハッシュに変更がありません: {target_name}")
        return None
    
    # ディレクトリを作成
    target_file.parent.mkdir(parents=True, exist_ok=True)
    
    # 書き込み
    output_data = {"processed": sorted(list(merged_hashes))}
    try:
        with open(target_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        logger.info(f"処理済みハッシュを更新しました: {target_name} ({len(existing_hashes)}件 -> {len(merged_hashes)}件)")
        return target_file
    except Exception as e:
        logger.error(f"処理済みハッシュファイルの書き込みに失敗しました: {target_file}, エラー: {e}")
        return None


def merge_and_write_dormitory_events_state(
    source_state_file: Path,
    server_repo_path: Path,
) -> Optional[Path]:
    if not source_state_file.exists():
        logger.debug(f"ソース状態ファイルが存在しません: {source_state_file}")
        return None
    try:
        with open(source_state_file, "r", encoding="utf-8") as f:
            source_data = json.load(f)
    except json.JSONDecodeError as e:
        logger.warning(f"ソース状態ファイルのJSON解析に失敗しました: {source_state_file}, エラー: {e}")
        return None
    except Exception as e:
        logger.warning(f"ソース状態ファイルの読み込みに失敗しました: {source_state_file}, エラー: {e}")
        return None

    target_file = server_repo_path / "v1" / "sources" / "list" / "dormitory_events.json"
    existing_data = None
    if target_file.exists():
        try:
            with open(target_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        except Exception:
            existing_data = None

    if existing_data == source_data:
        logger.debug("寮行事状態に変更がありません")
        return None

    target_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(target_file, "w", encoding="utf-8") as f:
            json.dump(source_data, f, ensure_ascii=False, indent=2)
        logger.info("寮行事状態ファイルを更新しました")
        return target_file
    except Exception as e:
        logger.error(f"寮行事状態ファイルの書き込みに失敗しました: {target_file}, エラー: {e}")
        return None


def _extract_rule_ids_from_index(index_path: Path) -> Set[str]:
    if not index_path.exists():
        return set()
    try:
        data = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    rules = data.get("rules") or []
    rule_ids: Set[str] = set()
    for rule in rules:
        rule_id = rule.get("id")
        if isinstance(rule_id, str):
            rule_ids.add(rule_id)
    return rule_ids


def _remove_rule_files(target_rules_dir: Path, rule_ids: Set[str]) -> int:
    removed = 0
    for rule_id in rule_ids:
        rule_path = target_rules_dir / f"{rule_id}.json"
        if rule_path.exists():
            rule_path.unlink(missing_ok=True)
            removed += 1
    return removed


def _remove_rule_figures(target_figures_dir: Path, rule_ids: Set[str]) -> int:
    if not target_figures_dir.exists():
        return 0
    removed = 0
    for file_path in target_figures_dir.iterdir():
        if not file_path.is_file():
            continue
        name = file_path.name
        if not name.startswith("rule-"):
            continue
        parts = name.split("_")
        if not parts:
            continue
        rule_prefix = parts[0]
        if rule_prefix in rule_ids:
            file_path.unlink(missing_ok=True)
            removed += 1
    return removed


def copy_school_rules_files(source_dir: Path, target_dir: Path) -> Dict[str, int]:
    """Copy school rules data into the server repository."""
    logger.info(f"学校規則データをコピー中: {source_dir} -> {target_dir}")
    result = {
        "rules_copied": 0,
        "figures_copied": 0,
        "rules_removed": 0,
        "figures_removed": 0,
    }

    target_dir.mkdir(parents=True, exist_ok=True)
    target_rules_dir = target_dir / "rules"
    target_rules_dir.mkdir(parents=True, exist_ok=True)
    target_figures_dir = target_dir / "figures"
    target_figures_dir.mkdir(parents=True, exist_ok=True)

    source_index = source_dir / "index.json"
    source_chapters = source_dir / "chapters.json"
    source_rules_dir = source_dir / "rules"
    source_figures_dir = source_dir / "markdown" / "figures"
    manifest_path = source_dir / "manifest.json"

    target_index_path = target_dir / "index.json"
    target_rule_ids = _extract_rule_ids_from_index(target_index_path)
    source_rule_ids = _extract_rule_ids_from_index(source_index)

    if source_index.exists():
        shutil.copy2(source_index, target_dir / "index.json")
    if source_chapters.exists():
        shutil.copy2(source_chapters, target_dir / "chapters.json")

    removed_rule_ids: Set[str] = set()
    regenerated_rule_ids: Set[str] = set()
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            removed_rule_ids = set(manifest.get("removedRuleIds") or [])
            regenerated_rule_ids = set(manifest.get("regeneratedRuleIds") or [])
        except Exception:
            removed_rule_ids = set()
            regenerated_rule_ids = set()

    if not removed_rule_ids and target_rule_ids:
        removed_rule_ids = target_rule_ids - source_rule_ids

    if removed_rule_ids:
        result["rules_removed"] = _remove_rule_files(target_rules_dir, removed_rule_ids)
        result["figures_removed"] += _remove_rule_figures(target_figures_dir, removed_rule_ids)

    if regenerated_rule_ids:
        result["figures_removed"] += _remove_rule_figures(target_figures_dir, regenerated_rule_ids)

    if source_rules_dir.exists():
        for rule_file in source_rules_dir.glob("*.json"):
            target_file = target_rules_dir / rule_file.name
            shutil.copy2(rule_file, target_file)
            result["rules_copied"] += 1

    if source_figures_dir.exists():
        for fig_file in source_figures_dir.glob("*.*"):
            target_file = target_figures_dir / fig_file.name
            shutil.copy2(fig_file, target_file)
            result["figures_copied"] += 1

    logger.info(
        "学校規則コピー完了: rules=%s, figures=%s, removed_rules=%s, removed_figures=%s",
        result["rules_copied"],
        result["figures_copied"],
        result["rules_removed"],
        result["figures_removed"],
    )
    return result

