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
from typing import Optional, List, Dict, Set

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
from scraper.classes_scraper import scrape_classes_page
from scraper.pdf_downloader import download_pdf, check_pdf_updated, get_file_hash
from processors.classes_processor import process_classes_pdf
from processors.meals_processor import process_meals_pdf
from server_updater.file_manager import (
    copy_final_files,
    copy_meals_files,
    load_processed_hashes,
    merge_and_write_processed_hashes,
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
) -> tuple[bool, List[str]]:
    """寮食PDFの処理
    
    Returns:
        (成功したかどうか, 処理済みPDFのハッシュリスト)
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
            return False, collected_hashes
        
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
                if discord_webhook:
                    notify_error(discord_webhook, "meals", error_message, {"PDF": pdf_url})
        
        logger.info(f"処理結果: 成功={len(processed)}, スキップ(既存)={len(skipped_existing)}, スキップ(未更新)={len(skipped_not_updated)}, スキップ(処理済み)={len(skipped_processed)}, エラー={'あり' if had_error else 'なし'}")
        
        if processed:
            logger.info(f"{len(processed)}件のPDF処理が完了しました")
            if discord_webhook:
                details = {
                    "処理済みPDF": "\n".join(f"{p['label']}: {p['url']}" for p in processed),
                    "出力ディレクトリ": "\n".join(p["out_dir"] for p in processed),
                }
                notify_success(discord_webhook, "meals", details)
            return True, collected_hashes
        
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
        return True, collected_hashes
    except Exception as e:
        logger.exception(f"寮食処理エラー: {e}")
        if discord_webhook:
            notify_error(discord_webhook, "meals", str(e))
        return False, collected_hashes


def process_classes(
    output_dir: Path,
    api_key: str,
    model: str = "gemini-2.5-pro",
    dpi: int = 220,
    use_yomitoku: bool = False,
    discord_webhook: Optional[str] = None,
    processed_hashes: Optional[Set[str]] = None,
) -> tuple[bool, Optional[str]]:
    """授業PDFの処理
    
    Returns:
        (成功したかどうか, 処理済みPDFのハッシュ)
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
            return True, None
        
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
                return True, None
            
            logger.info("PDFをダウンロード中...")
            if not download_pdf(pdf_url, pdf_path):
                logger.error("PDFのダウンロードに失敗しました。")
                if discord_webhook:
                    notify_error(discord_webhook, "classes", "PDFのダウンロードに失敗しました。")
                return False, None
        else:
            # ハッシュを計算
            pdf_hash = get_file_hash(temp_path)
            if pdf_hash:
                if pdf_hash in processed_hashes:
                    logger.info(f"PDFは既に処理済みです（ハッシュ: {pdf_hash[:16]}...）。スキップします。")
                    if discord_webhook:
                        notify_no_update(discord_webhook, "classes", "PDFは既に処理済みです。")
                    temp_path.unlink(missing_ok=True)
                    return True, None
                
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
                    return True, None
                
                logger.info("PDFをダウンロード中...")
                if not download_pdf(pdf_url, pdf_path):
                    logger.error("PDFのダウンロードに失敗しました。")
                    if discord_webhook:
                        notify_error(discord_webhook, "classes", "PDFのダウンロードに失敗しました。")
                    return False, None
        
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
            return True, pdf_hash
        else:
            logger.error("授業PDF処理に失敗しました。")
            if discord_webhook:
                notify_error(discord_webhook, "classes", "PDF処理に失敗しました。")
            return False, None
    except Exception as e:
        logger.exception(f"授業処理エラー: {e}")
        if discord_webhook:
            notify_error(discord_webhook, "classes", str(e))
        return False, None


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
        
        copied_files = []
        classes_copied_counter = 0
        meals_copied_counter = 0
        
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
        
        # 処理済みハッシュをマージ
        hash_files_updated = 0
        meals_hash_file = output_dir / "meals_hashes.json"
        classes_hash_file = output_dir / "classes_hashes.json"
        
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
        
        if not copied_files and hash_files_updated == 0:
            logger.info("更新するファイルがありません。")
            return True
        
        # コミット＆プッシュ
        logger.info("変更をコミット・プッシュ中...")
        commit_message_parts = [f"Actions: {classes_copied_counter}の授業ファイルと{meals_copied_counter}の寮食ファイルを更新"]
        if hash_files_updated > 0:
            commit_message_parts.append(f"{hash_files_updated}件の処理済みハッシュを更新")
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


def main():
    logger.info("=" * 60)
    logger.info("ServerProcesser自動化ワークフローを開始します")
    logger.info("=" * 60)
    
    parser = argparse.ArgumentParser(description="ServerProcesser自動化ワークフロー")
    parser.add_argument("--process", choices=["meals", "classes", "all"], default="all", help="処理タイプ")
    parser.add_argument("--output-dir", type=Path, default=Path("output"), help="出力ディレクトリ")
    parser.add_argument("--api-key", type=str, default=None, help="APIキー（未指定なら環境変数 GOOGLE_API_KEY を使用）")
    parser.add_argument("--model", type=str, default="gemini-2.5-pro", help="使用するモデル")
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
    logger.info(f"使用モデル: {args.model}")
    logger.info(f"DPI: {args.dpi}")
    logger.info(f"Yomitoku OCR: {'有効' if args.use_yomitoku else '無効'}")
    logger.info(f"出力ディレクトリ: {args.output_dir}")
    logger.info(f"サーバー更新: {'有効' if args.update_server else '無効'}")
    
    # 環境変数から取得
    api_key = args.api_key or os.getenv("GOOGLE_API_KEY")
    discord_webhook = args.discord_webhook or os.getenv("DISCORD_WEBHOOK_URL")
    github_token = args.github_token or os.getenv("GITHUB_TOKEN")
    
    logger.debug(f"環境変数取得状況: API_KEY={'設定済み' if api_key else '未設定'}, "
                 f"DISCORD_WEBHOOK={'設定済み' if discord_webhook else '未設定'}, "
                 f"GITHUB_TOKEN={'設定済み' if github_token else '未設定'}")
    
    if not api_key:
        logger.error("APIキーが設定されていません。")
        sys.exit(1)
    
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
    
    if args.update_server and (args.process in ["meals", "classes", "all"]):
        if github_token and args.server_repo_url:
            logger.info("処理前に既処理ハッシュを読み込み中...")
            if init_git_repo(server_repo_path, github_token, args.server_repo_url, args.branch):
                if args.process in ["meals", "all"]:
                    meals_processed_hashes = load_processed_hashes(server_repo_path, "meals")
                    logger.info(f"寮食の既処理ハッシュ: {len(meals_processed_hashes)}件")
                if args.process in ["classes", "all"]:
                    classes_processed_hashes = load_processed_hashes(server_repo_path, "classes")
                    logger.info(f"授業の既処理ハッシュ: {len(classes_processed_hashes)}件")
            else:
                logger.warning("server_repoの初期化に失敗しました。ハッシュチェックをスキップします。")
        else:
            logger.debug("server_repoの情報が不足しているため、ハッシュチェックをスキップします。")
    
    success = True
    meals_collected_hashes: List[str] = []
    classes_collected_hash: Optional[str] = None
    
    # 処理実行
    if args.process in ["meals", "all"]:
        logger.info("--- 寮食処理を開始 ---")
        meals_success, collected = process_dormitory_meals(
            output_dir=output_dir,
            api_key=api_key,
            model=args.model,
            dpi=args.dpi,
            use_yomitoku=args.use_yomitoku,
            discord_webhook=discord_webhook,
            prompt_file=args.prompt_file,
            processed_hashes=meals_processed_hashes,
        )
        success &= meals_success
        meals_collected_hashes = collected
        logger.info("--- 寮食処理を完了 ---")
    
    if args.process in ["classes", "all"]:
        logger.info("--- 授業処理を開始 ---")
        classes_success, collected_hash = process_classes(
            output_dir=output_dir,
            api_key=api_key,
            model=args.model,
            dpi=args.dpi,
            use_yomitoku=args.use_yomitoku,
            discord_webhook=discord_webhook,
            processed_hashes=classes_processed_hashes,
        )
        success &= classes_success
        classes_collected_hash = collected_hash
        logger.info("--- 授業処理を完了 ---")
    
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
    
    # サーバー更新
    if args.update_server and success:
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
    if success:
        logger.info("すべての処理が正常に完了しました")
    else:
        logger.error("一部の処理でエラーが発生しました")
    logger.info("=" * 60)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
