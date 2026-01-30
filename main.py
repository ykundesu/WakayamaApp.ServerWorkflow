#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
メインエントリーポイント
スクレイピング→処理→更新→通知の一連の流れを実行
"""

import json
import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Optional, List, Dict, Set, Union
from urllib.parse import urlparse

# パスを追加
sys.path.insert(0, str(Path(__file__).parent))

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

from scraper.dormitory_scraper import scrape_dormitory_page
from scraper.dormitory_calendar_scraper import scrape_dormitory_calendar_page
from scraper.classes_scraper import scrape_classes_page
from scraper.pdf_downloader import download_pdf, check_pdf_updated, get_file_hash
from scraper.image_downloader import check_image_updated, get_file_hash as get_image_hash
from processors.classes_processor import process_classes_pdf
from processors.meals_processor import process_meals_pdf
from processors.school_rules_processor import process_school_rules
from processors.dormitory_events_processor import process_dormitory_events_image
from server_updater.file_manager import (
    copy_final_files,
    copy_meals_files,
    copy_dormitory_events_files,
    copy_school_rules_files,
    load_processed_hashes,
    load_dormitory_events_state,
    merge_and_write_processed_hashes,
    merge_and_write_dormitory_events_state,
)
from server_updater.git_updater import init_git_repo, commit_and_push
from notifier.discord import notify_success, notify_error, notify_no_update


def process_dormitory_meals(
    output_dir: Path,
    api_key: str,
    model: str = "gemini-2.5-pro",
    dpi: int = 288,
    use_yomitoku: bool = False,
    discord_webhook: Optional[str] = None,
    prompt_file: Optional[Path] = None,
    processed_hashes: Optional[Set[str]] = None,
) -> tuple[bool, List[str], bool]:
    """寮食PDFの処理
    
    Returns:
        (エラーが無かったか, 処理済みPDFのハッシュリスト, 何かを処理したか)
    """
    logger.info("寮食PDF処理を開始します")
    logger.debug(f"パラメータ: model={model}, dpi={dpi}, use_yomitoku={use_yomitoku}, output_dir={output_dir}")
    if processed_hashes is None:
        processed_hashes = set()
    
    collected_hashes: List[str] = []
    
    try:
        logger.info("寮食ページをスクレイピング中...")
        pdf_infos = scrape_dormitory_page()
        
        if not pdf_infos:
            logger.warning("寮食PDFリンクが見つかりませんでした。")
            if discord_webhook:
                notify_no_update(discord_webhook, "meals", "PDFリンクが見つかりませんでした。")
            return True, collected_hashes, False
        
        logger.info(f"PDFリンクを{len(pdf_infos)}件見つけました")

        pdf_dir = output_dir / "pdfs"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        meals_output_root = output_dir / "meals_output"
        logger.debug(f"出力ディレクトリ: pdf_dir={pdf_dir}, meals_output_root={meals_output_root}")
        
        processed: List[Dict[str, str]] = []
        skipped_existing: List[str] = []
        skipped_not_updated: List[str] = []
        skipped_processed: List[str] = []
        had_error = False
        
        for index, pdf_info in enumerate(pdf_infos, start=1):
            pdf_url = pdf_info.get("url")
            if not pdf_url:
                logger.warning(f"URLが取得できなかったエントリをスキップします: {pdf_info}")
                continue
            
            date_label = pdf_info.get("date")
            year = pdf_info.get("year")
            month = pdf_info.get("month")
            fallback_label = f"pdf_{index:02d}"
            if year and month:
                fallback_label = f"{year}-{month:02d}"
            label = (date_label or fallback_label).replace("/", "-").replace(" ", "_")
            target_label = pdf_info.get("target", "unknown")
            
            logger.info(f"処理対象 ({target_label}): {label} -> {pdf_url}")
            
            month_output_dir = meals_output_root / label
            meals_dir = month_output_dir / "meals"
            if meals_dir.exists() and any(meals_dir.glob("*.json")):
                logger.info(f"{label} の既存データが見つかったため処理をスキップします。")
                skipped_existing.append(label)
                continue
            
            pdf_path = pdf_dir / f"meals_{label}.pdf"
            
            # 一時DLしてハッシュチェック
            logger.debug(f"{label} のPDFを一時ダウンロードしてハッシュを確認中...")
            temp_path = pdf_path.with_suffix(".tmp")
            if not download_pdf(pdf_url, temp_path):
                logger.warning(f"{label} のPDFの一時ダウンロードに失敗しました。フォールバックとして既存の更新チェックを使用します。")
                # フォールバック: 既存の更新チェックを使用
                is_updated, _ = check_pdf_updated(pdf_url, pdf_path)
                if not is_updated:
                    msg = f"{label} のPDFが更新されていません。"
                    logger.info(msg)
                    skipped_not_updated.append(label)
                    continue
                
                logger.info(f"{label} のPDFをダウンロード中...")
                if not download_pdf(pdf_url, pdf_path):
                    error_message = f"{label} のPDFのダウンロードに失敗しました。"
                    logger.error(error_message)
                    logger.info("had error is true")
                    had_error = True
                    if discord_webhook:
                        notify_error(discord_webhook, "meals", error_message, {"PDF": pdf_url})
                    continue
            else:
                # ハッシュを計算
                pdf_hash = get_file_hash(temp_path)
                if pdf_hash:
                    if pdf_hash in processed_hashes:
                        logger.info(f"{label} のPDFは既に処理済みです（ハッシュ: {pdf_hash[:16]}...）。スキップします。")
                        skipped_processed.append(label)
                        temp_path.unlink(missing_ok=True)
                        continue
                    
                    logger.info(f"{label} のPDFをダウンロードしました（ハッシュ: {pdf_hash[:16]}...）。処理を続行します。")
                    # 一時ファイルを正式ファイルに移動
                    temp_path.replace(pdf_path)
                else:
                    logger.warning(f"{label} のPDFのハッシュ計算に失敗しました。フォールバックとして既存の更新チェックを使用します。")
                    temp_path.unlink(missing_ok=True)
                    # フォールバック: 既存の更新チェックを使用
                    is_updated, _ = check_pdf_updated(pdf_url, pdf_path)
                    if not is_updated:
                        msg = f"{label} のPDFが更新されていません。"
                        logger.info(msg)
                        skipped_not_updated.append(label)
                        continue
                    
                    logger.info(f"{label} のPDFをダウンロード中...")
                    if not download_pdf(pdf_url, pdf_path):
                        error_message = f"{label} のPDFのダウンロードに失敗しました。"
                        logger.error(error_message)
                        logger.info("had error is true")  
                        had_error = True
                        if discord_webhook:
                            notify_error(discord_webhook, "meals", error_message, {"PDF": pdf_url})
                        continue
            
            logger.info(f"{label} のPDFダウンロードが完了しました")
            
            # PDF処理
            logger.info(f"{label} の寮食PDFを処理中...")
            success = process_meals_pdf(
                pdf_path=str(pdf_path),
                out_dir=month_output_dir,
                model=model,
                api_key=api_key,
                dpi=dpi,
                use_yomitoku=use_yomitoku,
                prompt_file=prompt_file,
            )
            
            if success:
                logger.info(f"{label} の寮食PDF処理が完了しました。")
                # ハッシュを収集
                pdf_hash = get_file_hash(pdf_path)
                if pdf_hash:
                    collected_hashes.append(pdf_hash)
                    logger.debug(f"{label} のハッシュを収集しました: {pdf_hash[:16]}...")
                
                processed.append(
                    {
                        "label": label,
                        "url": pdf_url,
                        "out_dir": str(month_output_dir),
                    }
                )
            else:
                error_message = f"{label} のPDF処理に失敗しました。"
                logger.error(error_message)
                had_error = True
                logger.info("had error is true")
                if discord_webhook:
                    notify_error(discord_webhook, "meals", error_message, {"PDF": pdf_url})
        
        logger.info(f"処理結果: 成功={len(processed)}, スキップ(既存)={len(skipped_existing)}, スキップ(未更新)={len(skipped_not_updated)}, スキップ(処理済み)={len(skipped_processed)}, エラー={'あり' if had_error else 'なし'}")
        
        if processed and not had_error:
            logger.info(f"{len(processed)}件のPDF処理が完了しました")
            if discord_webhook:
                details = {
                    "処理済みPDF": "\n".join(f"{p['label']}: {p['url']}" for p in processed),
                    "出力ディレクトリ": "\n".join(p["out_dir"] for p in processed),
                }
                notify_success(discord_webhook, "meals", details)
            return True, collected_hashes, True
        
        if processed and had_error:
            # 一部成功・一部失敗
            return False, collected_hashes, True
        
        if not had_error and discord_webhook:
            reasons = []
            if skipped_existing:
                reasons.append("既存データ: " + ", ".join(skipped_existing))
            if skipped_not_updated:
                reasons.append("未更新: " + ", ".join(skipped_not_updated))
            if skipped_processed:
                reasons.append("処理済み: " + ", ".join(skipped_processed))
            if not reasons:
                reasons.append("処理可能なPDFがありませんでした。")
            notify_no_update(discord_webhook, "meals", "\n".join(reasons))
        
        logger.info("寮食PDF処理を完了しました（処理対象なし）")
        return True, collected_hashes, False
    except Exception as e:
        logger.exception(f"寮食処理エラー: {e}")
        if discord_webhook:
            notify_error(discord_webhook, "meals", str(e))
        return False, collected_hashes, False


def process_classes(
    output_dir: Path,
    api_key: str,
    model: str = "gemini-2.5-pro",
    dpi: int = 220,
    use_yomitoku: bool = False,
    discord_webhook: Optional[str] = None,
    processed_hashes: Optional[Set[str]] = None,
) -> tuple[bool, Optional[str], bool]:
    """授業PDFの処理
    
    Returns:
        (エラーが無かったか, 処理済みPDFのハッシュ, 何かを処理したか)
    """
    logger.info("授業PDF処理を開始します")
    logger.debug(f"パラメータ: model={model}, dpi={dpi}, use_yomitoku={use_yomitoku}, output_dir={output_dir}")
    if processed_hashes is None:
        processed_hashes = set()
    
    try:
        logger.info("授業ページをスクレイピング中...")
        pdf_url = scrape_classes_page()
        
        if not pdf_url:
            logger.warning("授業PDFリンクが見つかりませんでした。")
            if discord_webhook:
                notify_no_update(discord_webhook, "classes", "PDFリンクが見つかりませんでした。")
            return True, None, False
        
        logger.info(f"PDF URL: {pdf_url}")
        
        # PDFダウンロード
        pdf_path = output_dir / "pdfs" / "classes.pdf"
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"PDF保存先: {pdf_path}")
        
        # 一時DLしてハッシュチェック
        logger.debug("PDFを一時ダウンロードしてハッシュを確認中...")
        temp_path = pdf_path.with_suffix(".tmp")
        if not download_pdf(pdf_url, temp_path):
            logger.warning("PDFの一時ダウンロードに失敗しました。フォールバックとして既存の更新チェックを使用します。")
            # フォールバック: 既存の更新チェックを使用
            is_updated, _ = check_pdf_updated(pdf_url, pdf_path)
            if not is_updated:
                logger.info("PDFが更新されていません。")
                if discord_webhook:
                    notify_no_update(discord_webhook, "classes", "PDFが更新されていません。")
                return True, None, False
            
            logger.info("PDFをダウンロード中...")
            if not download_pdf(pdf_url, pdf_path):
                logger.error("PDFのダウンロードに失敗しました。")
                if discord_webhook:
                    notify_error(discord_webhook, "classes", "PDFのダウンロードに失敗しました。")
                return False, None, False
        else:
            # ハッシュを計算
            pdf_hash = get_file_hash(temp_path)
            if pdf_hash:
                if pdf_hash in processed_hashes:
                    logger.info(f"PDFは既に処理済みです（ハッシュ: {pdf_hash[:16]}...）。スキップします。")
                    if discord_webhook:
                        notify_no_update(discord_webhook, "classes", "PDFは既に処理済みです。")
                    temp_path.unlink(missing_ok=True)
                    return True, None, False
                
                logger.info(f"PDFをダウンロードしました（ハッシュ: {pdf_hash[:16]}...）。処理を続行します。")
                # 一時ファイルを正式ファイルに移動
                temp_path.replace(pdf_path)
            else:
                logger.warning("PDFのハッシュ計算に失敗しました。フォールバックとして既存の更新チェックを使用します。")
                temp_path.unlink(missing_ok=True)
                # フォールバック: 既存の更新チェックを使用
                is_updated, _ = check_pdf_updated(pdf_url, pdf_path)
                if not is_updated:
                    logger.info("PDFが更新されていません。")
                    if discord_webhook:
                        notify_no_update(discord_webhook, "classes", "PDFが更新されていません。")
                    return True, None, False
                
                logger.info("PDFをダウンロード中...")
                if not download_pdf(pdf_url, pdf_path):
                    logger.error("PDFのダウンロードに失敗しました。")
                    if discord_webhook:
                        notify_error(discord_webhook, "classes", "PDFのダウンロードに失敗しました。")
                    return False, None, False
        
        logger.info("PDFダウンロードが完了しました")
        
        # PDF処理
        logger.info("授業PDFを処理中...")
        classes_output_dir = output_dir / "classes_output"
        success = process_classes_pdf(
            pdf_path=str(pdf_path),
            out_dir=classes_output_dir,
            model=model,
            api_key=api_key,
            dpi=dpi,
            use_yomitoku=use_yomitoku,
        )
        
        if success:
            logger.info("授業PDF処理が完了しました。")
            # ハッシュを収集
            pdf_hash = get_file_hash(pdf_path)
            if discord_webhook:
                notify_success(
                    discord_webhook,
                    "classes",
                    {"処理済みPDF": pdf_url, "出力ディレクトリ": str(classes_output_dir)},
                )
            return True, pdf_hash, True
        else:
            logger.error("授業PDF処理に失敗しました。")
            if discord_webhook:
                notify_error(discord_webhook, "classes", "PDF処理に失敗しました。")
            return False, None, False
    except Exception as e:
        logger.exception(f"授業処理エラー: {e}")
        if discord_webhook:
            notify_error(discord_webhook, "classes", str(e))
        return False, None, False


def process_dormitory_events(
    output_dir: Path,
    api_key: str,
    model: str = "gemini-2.5-pro",
    dpi: int = 288,
    use_yomitoku: bool = False,
    discord_webhook: Optional[str] = None,
    processed_state: Optional[Dict[str, Optional[str]]] = None,
) -> tuple[bool, Dict[str, Optional[str]], bool]:
    """寮行事予定画像の処理"""
    logger.info("寮行事予定の処理を開始します")
    if processed_state is None:
        processed_state = {}

    last_url = processed_state.get("last_url")
    last_hash = processed_state.get("last_hash")

    def save_state(state: Dict[str, Optional[str]]) -> None:
        if not state.get("last_url"):
            return
        state_file = output_dir / "dormitory_events_state.json"
        try:
            with open(state_file, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            logger.info(f"寮行事の状態ファイルを保存しました: {state_file}")
        except Exception as e:
            logger.warning(f"寮行事状態ファイルの保存に失敗しました: {e}")

    try:
        logger.info("寮行事予定ページをスクレイピング中...")
        image_info = scrape_dormitory_calendar_page()
        if not image_info or not image_info.get("url"):
            logger.warning("寮行事予定の画像リンクが見つかりませんでした。")
            if discord_webhook:
                notify_no_update(discord_webhook, "dormitory_events", "画像リンクが見つかりませんでした。")
            return True, processed_state, False

        image_url = image_info["url"]
        title_hint = image_info.get("heading") or image_info.get("alt") or ""

        events_output_root = output_dir / "dormitory_events_output"
        images_dir = events_output_root / "images"
        images_dir.mkdir(parents=True, exist_ok=True)

        suffix = Path(urlparse(image_url).path).suffix.lower()
        if suffix not in {".png", ".jpg", ".jpeg", ".webp"}:
            suffix = ".jpg"
        image_path = images_dir / f"calendar{suffix}"

        logger.info(f"寮行事画像を確認中: {image_url}")
        updated, new_hash = check_image_updated(
            image_url,
            image_path,
            last_url=last_url,
            last_hash=last_hash,
        )

        if not updated:
            logger.info("寮行事予定画像が更新されていません。")
            state = {"last_url": image_url, "last_hash": new_hash or last_hash}
            save_state(state)
            if discord_webhook:
                notify_no_update(discord_webhook, "dormitory_events", "画像が更新されていません。")
            return True, state, False

        if new_hash is None:
            new_hash = get_image_hash(image_path)
        if new_hash is None:
            error_message = "画像のダウンロードまたはハッシュ計算に失敗しました。"
            logger.error(error_message)
            if discord_webhook:
                notify_error(discord_webhook, "dormitory_events", error_message, {"画像URL": image_url})
            return False, processed_state, False

        logger.info("寮行事予定画像を処理中...")
        result = process_dormitory_events_image(
            image_path=str(image_path),
            out_dir=events_output_root,
            model=model,
            api_key=api_key,
            dpi=dpi,
            use_yomitoku=use_yomitoku,
            title_hint=title_hint,
        )

        if not result:
            error_message = "寮行事予定の解析に失敗しました。"
            logger.error(error_message)
            if discord_webhook:
                notify_error(discord_webhook, "dormitory_events", error_message, {"画像URL": image_url})
            return False, processed_state, False

        academic_year = result.get("academic_year")
        events = result.get("events", [])
        if not academic_year or not events:
            error_message = "寮行事予定の抽出結果が空でした。"
            logger.error(error_message)
            if discord_webhook:
                notify_error(discord_webhook, "dormitory_events", error_message)
            return False, processed_state, False

        events_dir = events_output_root / "events"
        events_dir.mkdir(parents=True, exist_ok=True)
        output_path = events_dir / f"{academic_year}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"academic_year": academic_year, "events": events}, f, ensure_ascii=False, indent=2)
        logger.info(f"寮行事予定を保存しました: {output_path} ({len(events)}件)")

        state = {"last_url": image_url, "last_hash": new_hash}
        save_state(state)

        if discord_webhook:
            notify_success(
                discord_webhook,
                "dormitory_events",
                {
                    "画像URL": image_url,
                    "年度": academic_year,
                    "件数": len(events),
                    "出力": str(output_path),
                },
            )
        return True, state, True
    except Exception as e:
        logger.exception(f"寮行事処理エラー: {e}")
        if discord_webhook:
            notify_error(discord_webhook, "dormitory_events", str(e))
        return False, processed_state, False


def update_server(
    output_dir: Path,
    server_repo_path: Path,
    github_token: str,
    repo_url: str,
    branch: str = "main",
) -> bool:
    """WakayamaServerを更新"""
    logger.info("WakayamaServer更新を開始します")
    logger.debug(f"パラメータ: repo_url={repo_url}, branch={branch}, server_repo_path={server_repo_path}")
    try:
        logger.info("WakayamaServerリポジトリを初期化中...")
        if not init_git_repo(server_repo_path, github_token, repo_url, branch):
            logger.error("Gitリポジトリの初期化に失敗しました")
            return False
        
        logger.info("Gitリポジトリの初期化が完了しました")
        
        # ファイルをコピー
        classes_output_dir = output_dir / "classes_output"
        meals_output_dir = output_dir / "meals_output"
        dormitory_events_output_dir = output_dir / "dormitory_events_output"
        rules_output_dir = output_dir / "rules_output"
        
        copied_files = []
        classes_copied_counter = 0
        meals_copied_counter = 0
        dormitory_events_copied_counter = 0
        rules_copied_counter = 0
        rules_removed_counter = 0
        rules_figures_copied_counter = 0
        
        if classes_output_dir.exists():
            logger.info("授業データファイルをコピー中...")
            classes_target = server_repo_path / "v1" / "classes"
            copied = copy_final_files(classes_output_dir, classes_target)
            copied_files.extend(copied)
            classes_copied_counter = len(copied)
            logger.info(f"授業データ: {len(copied)}ファイルをコピーしました。")
        else:
            logger.debug("授業データディレクトリが存在しません")
        
        if meals_output_dir.exists():
            logger.info("寮食データファイルをコピー中...")
            meals_target = server_repo_path / "v1" / "meals"
            copied = copy_meals_files(meals_output_dir, meals_target)
            copied_files.extend(copied)
            meals_copied_counter = len(copied)
            logger.info(f"寮食データ: {len(copied)}ファイルをコピーしました。")
        else:
            logger.debug("寮食データディレクトリが存在しません")

        if dormitory_events_output_dir.exists():
            logger.info("寮行事データファイルをコピー中...")
            events_target = server_repo_path / "v1" / "dormitory" / "events"
            copied = copy_dormitory_events_files(dormitory_events_output_dir, events_target)
            copied_files.extend(copied)
            dormitory_events_copied_counter = len(copied)
            logger.info(f"寮行事データ: {len(copied)}ファイルをコピーしました。")
        else:
            logger.debug("寮行事データディレクトリが存在しません")

        if rules_output_dir.exists():
            rules_manifest_path = rules_output_dir / "manifest.json"
            should_copy_rules = True
            if rules_manifest_path.exists():
                try:
                    manifest = json.loads(rules_manifest_path.read_text(encoding="utf-8"))
                    updated = int(manifest.get("rulesUpdated") or 0)
                    regenerated = int(manifest.get("rulesRegenerated") or 0)
                    removed = len(manifest.get("removedRuleIds") or [])
                    if updated == 0 and regenerated == 0 and removed == 0:
                        should_copy_rules = False
                except Exception:
                    should_copy_rules = True

            if should_copy_rules:
                logger.info("規則データファイルをコピー中...")
                rules_target = server_repo_path / "v1" / "school-rules"
                rules_result = copy_school_rules_files(rules_output_dir, rules_target)
                rules_copied_counter = rules_result.get("rules_copied", 0)
                rules_removed_counter = rules_result.get("rules_removed", 0)
                rules_figures_copied_counter = rules_result.get("figures_copied", 0)
                logger.info(
                    "規則データ: rules=%s, figures=%s, removed=%s",
                    rules_copied_counter,
                    rules_figures_copied_counter,
                    rules_removed_counter,
                )
            else:
                logger.info("規則更新がないためコピーをスキップします")
        else:
            logger.debug("規則データディレクトリが存在しません")
        
        # 処理済みハッシュをマージ
        hash_files_updated = 0
        state_files_updated = 0
        meals_hash_file = output_dir / "meals_hashes.json"
        classes_hash_file = output_dir / "classes_hashes.json"
        dormitory_state_file = output_dir / "dormitory_events_state.json"
        rules_hash_file = output_dir / "school_rules_hashes.json"
        
        if meals_hash_file.exists():
            logger.info("寮食の処理済みハッシュをマージ中...")
            updated_file = merge_and_write_processed_hashes(meals_hash_file, server_repo_path, "meals")
            if updated_file:
                hash_files_updated += 1
                logger.info(f"寮食の処理済みハッシュを更新しました: {updated_file}")
        
        if classes_hash_file.exists():
            logger.info("授業の処理済みハッシュをマージ中...")
            updated_file = merge_and_write_processed_hashes(classes_hash_file, server_repo_path, "classes")
            if updated_file:
                hash_files_updated += 1
                logger.info(f"授業の処理済みハッシュを更新しました: {updated_file}")

        if rules_hash_file.exists():
            logger.info("規則の処理済みハッシュをマージ中...")
            updated_file = merge_and_write_processed_hashes(rules_hash_file, server_repo_path, "school_rules")
            if updated_file:
                hash_files_updated += 1
                logger.info(f"規則の処理済みハッシュを更新しました: {updated_file}")

        if dormitory_state_file.exists():
            logger.info("寮行事の状態ファイルをマージ中...")
            updated_file = merge_and_write_dormitory_events_state(dormitory_state_file, server_repo_path)
            if updated_file:
                state_files_updated += 1
                logger.info(f"寮行事の状態ファイルを更新しました: {updated_file}")
        
        rules_changed = (rules_copied_counter + rules_removed_counter + rules_figures_copied_counter) > 0
        if not copied_files and hash_files_updated == 0 and state_files_updated == 0 and not rules_changed:
            logger.info("更新するファイルがありません。")
            return True
        
        # コミット＆プッシュ
        logger.info("変更をコミット・プッシュ中...")
        data_updates = []
        if classes_copied_counter:
            data_updates.append(f"{classes_copied_counter}の授業ファイル")
        if meals_copied_counter:
            data_updates.append(f"{meals_copied_counter}の寮食ファイル")
        if dormitory_events_copied_counter:
            data_updates.append(f"{dormitory_events_copied_counter}の寮行事ファイル")
        if rules_copied_counter or rules_removed_counter:
            if rules_removed_counter:
                data_updates.append(f"{rules_copied_counter}の規則ファイル/{rules_removed_counter}件削除")
            else:
                data_updates.append(f"{rules_copied_counter}の規則ファイル")
        commit_message_parts = []
        if data_updates:
            commit_message_parts.append("Actions: " + "、".join(data_updates) + "を更新")
        if hash_files_updated > 0:
            commit_message_parts.append(f"{hash_files_updated}件の処理済みハッシュを更新")
        if state_files_updated > 0:
            commit_message_parts.append(f"{state_files_updated}件の状態ファイルを更新")
        if not commit_message_parts:
            commit_message_parts.append("Actions: データを更新")
        commit_message = " ".join(commit_message_parts) + " by Github Actions"
        logger.debug(f"コミットメッセージ: {commit_message}")
        success = commit_and_push(
            repo_path=server_repo_path,
            github_token=github_token,
            repo_url=repo_url,
            branch=branch,
            commit_message=commit_message,
        )
        
        if success:
            logger.info("サーバー更新が完了しました")
        else:
            logger.error("サーバー更新に失敗しました")
        
        return success
    except Exception as e:
        logger.exception(f"サーバー更新エラー: {e}")
    return False


def parse_rules_models(raw_rules_model: Optional[Union[List[str], str]], default_model: str) -> List[str]:
    if not raw_rules_model:
        return [default_model]

    if isinstance(raw_rules_model, str):
        raw_items = [raw_rules_model]
    else:
        raw_items = raw_rules_model

    models: List[str] = []
    for item in raw_items:
        if not item:
            continue
        for part in str(item).split(","):
            model = part.strip()
            if model:
                models.append(model)

    return models or [default_model]


def main():
    logger.info("=" * 60)
    logger.info("ServerProcesser自動化ワークフローを開始します")
    logger.info("=" * 60)
    
    parser = argparse.ArgumentParser(description="ServerProcesser自動化ワークフロー")
    parser.add_argument("--process", choices=["meals", "classes", "dormitory_events", "rules", "all"], default="all", help="処理タイプ")
    parser.add_argument("--output-dir", type=Path, default=Path("output"), help="出力ディレクトリ")
    parser.add_argument("--api-key", type=str, default=None, help="APIキー（未指定なら環境変数 GOOGLE_API_KEY を使用）")
    parser.add_argument("--model", type=str, default="gemini-2.5-pro", help="使用するモデル")
    parser.add_argument(
        "--rules-model",
        type=str,
        action="append",
        default=None,
        help="学校規則処理用モデル（未指定なら --model を使用、複数指定はカンマ区切り or 複数回指定）",
    )
    parser.add_argument(
        "--rules-provider",
        choices=["gemini", "openrouter"],
        default="gemini",
        help="学校規則の抽出プロバイダ",
    )
    parser.add_argument("--openrouter-api-key", type=str, default=None, help="OpenRouter APIキー（未指定なら環境変数 OPENROUTER_API_KEY を使用）")
    parser.add_argument("--dpi", type=int, default=220, help="レンダリングDPI")
    parser.add_argument("--use-yomitoku", action="store_true", help="Yomitoku OCRを使用")
    parser.add_argument("--prompt-file", type=Path, default=None, help="プロンプトファイルパス（寮食処理用）")
    parser.add_argument("--discord-webhook", type=str, default=None, help="Discord Webhook URL（未指定なら環境変数 DISCORD_WEBHOOK_URL を使用）")
    parser.add_argument("--update-server", action="store_true", help="WakayamaServerを更新するか")
    parser.add_argument("--server-repo-path", type=Path, default=None, help="WakayamaServerリポジトリのローカルパス")
    parser.add_argument("--server-repo-url", type=str, default=None, help="WakayamaServerリポジトリURL")
    parser.add_argument("--github-token", type=str, default=None, help="GitHubトークン（未指定なら環境変数 GITHUB_TOKEN を使用）")
    parser.add_argument("--branch", type=str, default="main", help="Gitブランチ")
    
    args = parser.parse_args()
    
    logger.info(f"処理タイプ: {args.process}")
    rules_models = parse_rules_models(args.rules_model, args.model)
    logger.info(f"使用モデル: {args.model}")
    logger.info(f"規則モデル: {', '.join(rules_models)}")
    logger.info(f"規則プロバイダ: {args.rules_provider}")
    logger.info(f"DPI: {args.dpi}")
    logger.info(f"Yomitoku OCR: {'有効' if args.use_yomitoku else '無効'}")
    logger.info(f"出力ディレクトリ: {args.output_dir}")
    logger.info(f"サーバー更新: {'有効' if args.update_server else '無効'}")
    
    # 環境変数から取得
    api_key = args.api_key or os.getenv("GOOGLE_API_KEY")
    openrouter_api_key = args.openrouter_api_key or os.getenv("OPENROUTER_API_KEY")
    discord_webhook = args.discord_webhook or os.getenv("DISCORD_WEBHOOK_URL")
    github_token = args.github_token or os.getenv("GITHUB_TOKEN")
    
    logger.debug(
        "環境変数取得状況: API_KEY=%s, OPENROUTER_API_KEY=%s, DISCORD_WEBHOOK=%s, GITHUB_TOKEN=%s",
        "設定済み" if api_key else "未設定",
        "設定済み" if openrouter_api_key else "未設定",
        "設定済み" if discord_webhook else "未設定",
        "設定済み" if github_token else "未設定",
    )

    needs_gemini_key = args.process in ["meals", "classes", "dormitory_events", "all"]
    if args.process in ["rules", "all"] and args.rules_provider == "gemini":
        needs_gemini_key = True

    if needs_gemini_key and not api_key:
        logger.error("Google APIキーが設定されていません。")
        sys.exit(1)

    if args.process in ["rules", "all"] and args.rules_provider == "openrouter" and not openrouter_api_key:
        logger.error("OpenRouter APIキーが設定されていません。")
        sys.exit(1)

    google_api_key = api_key or ""
    
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"出力ディレクトリを作成しました: {output_dir}")

    if args.server_repo_path:
        server_repo_path = args.server_repo_path.resolve()
    else:
        server_repo_path = (output_dir / "server_repo").resolve()
    
    # 処理前にserver_repoを初期化してハッシュを読み込む
    meals_processed_hashes: Set[str] = set()
    classes_processed_hashes: Set[str] = set()
    rules_processed_hashes: Set[str] = set()
    dormitory_events_state: Dict[str, Optional[str]] = {}
    
    if args.update_server and (args.process in ["meals", "classes", "dormitory_events", "rules", "all"]):
        if github_token and args.server_repo_url:
            logger.info("処理前に既処理ハッシュを読み込み中...")
            if init_git_repo(server_repo_path, github_token, args.server_repo_url, args.branch):
                if args.process in ["meals", "all"]:
                    meals_processed_hashes = load_processed_hashes(server_repo_path, "meals")
                    logger.info(f"寮食の既処理ハッシュ: {len(meals_processed_hashes)}件")
                if args.process in ["classes", "all"]:
                    classes_processed_hashes = load_processed_hashes(server_repo_path, "classes")
                    logger.info(f"授業の既処理ハッシュ: {len(classes_processed_hashes)}件")
                if args.process in ["rules", "all"]:
                    rules_processed_hashes = load_processed_hashes(server_repo_path, "school_rules")
                    logger.info(f"規則の既処理ハッシュ: {len(rules_processed_hashes)}件")
                if args.process in ["dormitory_events", "all"]:
                    dormitory_events_state = load_dormitory_events_state(server_repo_path)
                    logger.info("寮行事の既状態を読み込みました")
            else:
                logger.warning("server_repoの初期化に失敗しました。ハッシュチェックをスキップします。")
        else:
            logger.debug("server_repoの情報が不足しているため、ハッシュチェックをスキップします。")
    
    # 集計フラグ
    had_any_error = False
    meals_collected_hashes: List[str] = []
    classes_collected_hash: Optional[str] = None
    rules_collected_hashes: List[str] = []
    
    # 処理実行
    if args.process in ["meals", "all"]:
        logger.info("--- 寮食処理を開始 ---")
        meals_ok, collected, meals_did_process = process_dormitory_meals(
            output_dir=output_dir,
            api_key=google_api_key,
            model=args.model,
            dpi=args.dpi,
            use_yomitoku=args.use_yomitoku,
            discord_webhook=discord_webhook,
            prompt_file=args.prompt_file,
            processed_hashes=meals_processed_hashes,
        )
        had_any_error |= (not meals_ok)
        meals_collected_hashes = collected
        logger.info("--- 寮食処理を完了 ---")

    if args.process in ["dormitory_events", "all"]:
        logger.info("--- 寮行事処理を開始 ---")
        events_ok, events_state, events_did_process = process_dormitory_events(
            output_dir=output_dir,
            api_key=google_api_key,
            model=args.model,
            dpi=args.dpi,
            use_yomitoku=args.use_yomitoku,
            discord_webhook=discord_webhook,
            processed_state=dormitory_events_state,
        )
        had_any_error |= (not events_ok)
        dormitory_events_state = events_state
        logger.info("--- 寮行事処理を完了 ---")

    if args.process in ["classes", "all"]:
        logger.info("--- 授業処理を開始 ---")
        classes_ok, collected_hash, classes_did_process = process_classes(
            output_dir=output_dir,
            api_key=google_api_key,
            model=args.model,
            dpi=args.dpi,
            use_yomitoku=args.use_yomitoku,
            discord_webhook=discord_webhook,
            processed_hashes=classes_processed_hashes,
        )
        had_any_error |= (not classes_ok)
        classes_collected_hash = collected_hash
        logger.info("--- 授業処理を完了 ---")

    if args.process in ["rules", "all"]:
        logger.info("--- 規則処理を開始 ---")
        rules_ok, collected_hashes, rules_did_process = process_school_rules(
            output_dir=output_dir,
            api_key=google_api_key if args.rules_provider == "gemini" else None,
            models=rules_models,
            dpi=args.dpi,
            use_yomitoku=args.use_yomitoku,
            processed_hashes=rules_processed_hashes,
            server_repo_path=server_repo_path if args.update_server else None,
            provider=args.rules_provider,
            openrouter_api_key=openrouter_api_key,
        )
        had_any_error |= (not rules_ok)
        rules_collected_hashes = collected_hashes

        if discord_webhook:
            if not rules_ok:
                notify_error(discord_webhook, "rules", "規則処理に失敗しました。")
            elif not rules_did_process:
                notify_no_update(discord_webhook, "rules", "規則の更新がありませんでした。")
            else:
                manifest_path = output_dir / "rules_output" / "manifest.json"
                details = {}
                if manifest_path.exists():
                    try:
                        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                        details = {
                            "更新ルール数": manifest.get("rulesUpdated"),
                            "再生成ルール数": manifest.get("rulesRegenerated"),
                            "削除ルール数": len(manifest.get("removedRuleIds") or []),
                        }
                    except Exception:
                        details = {}
                notify_success(discord_webhook, "rules", details)

        logger.info("--- 規則処理を完了 ---")
    
    # 処理済みハッシュをファイルに保存
    if meals_collected_hashes:
        meals_hash_file = output_dir / "meals_hashes.json"
        try:
            with open(meals_hash_file, "w", encoding="utf-8") as f:
                json.dump({"processed": meals_collected_hashes}, f, ensure_ascii=False, indent=2)
            logger.info(f"寮食の処理済みハッシュを保存しました: {meals_hash_file} ({len(meals_collected_hashes)}件)")
        except Exception as e:
            logger.warning(f"寮食の処理済みハッシュの保存に失敗しました: {e}")
    
    if classes_collected_hash:
        classes_hash_file = output_dir / "classes_hashes.json"
        try:
            with open(classes_hash_file, "w", encoding="utf-8") as f:
                json.dump({"processed": [classes_collected_hash]}, f, ensure_ascii=False, indent=2)
            logger.info(f"授業の処理済みハッシュを保存しました: {classes_hash_file}")
        except Exception as e:
            logger.warning(f"授業の処理済みハッシュの保存に失敗しました: {e}")

    if rules_collected_hashes:
        rules_hash_file = output_dir / "school_rules_hashes.json"
        try:
            with open(rules_hash_file, "w", encoding="utf-8") as f:
                json.dump({"processed": rules_collected_hashes}, f, ensure_ascii=False, indent=2)
            logger.info(
                f"規則の処理済みハッシュを保存しました: {rules_hash_file} ({len(rules_collected_hashes)}件)"
            )
        except Exception as e:
            logger.warning(f"規則の処理済みハッシュの保存に失敗しました: {e}")
    
    # サーバー更新
    if args.update_server and not had_any_error:
        if not github_token:
            logger.warning("GitHubトークンが設定されていないため、サーバー更新をスキップします。")
        elif not args.server_repo_url:
            logger.warning("サーバーリポジトリURLが設定されていないため、サーバー更新をスキップします。")
        else:
            logger.info("--- サーバー更新を開始 ---")
            update_server(
                output_dir=output_dir,
                server_repo_path=server_repo_path,
                github_token=github_token,
                repo_url=args.server_repo_url,
                branch=args.branch,
            )
            logger.info("--- サーバー更新を完了 ---")
    
    logger.info("=" * 60)
    success = not had_any_error
    if success:
        logger.info("すべての処理が正常に完了しました")
    else:
        logger.error("一部の処理でエラーが発生しました")
    logger.info("=" * 60)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
