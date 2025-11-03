#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
メインエントリーポイント
スクレイピング→処理→更新→通知の一連の流れを実行
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Optional

# パスを追加
sys.path.insert(0, str(Path(__file__).parent))

from scraper.dormitory_scraper import scrape_dormitory_page
from scraper.classes_scraper import scrape_classes_page
from scraper.pdf_downloader import download_pdf, check_pdf_updated
from processors.classes_processor import process_classes_pdf
from processors.meals_processor import process_meals_pdf
from server_updater.file_manager import copy_final_files, copy_meals_files
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
) -> bool:
    """寮食PDFの処理"""
    try:
        print("寮食ページをスクレイピング中...")
        pdf_url = scrape_dormitory_page()
        
        if not pdf_url:
            print("寮食PDFリンクが見つかりませんでした。")
            if discord_webhook:
                notify_no_update(discord_webhook, "meals", "PDFリンクが見つかりませんでした。")
            return False
        
        print(f"PDF URL: {pdf_url}")
        
        # PDFダウンロード
        pdf_path = output_dir / "pdfs" / "meals.pdf"
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 更新チェック
        is_updated, _ = check_pdf_updated(pdf_url, pdf_path)
        if not is_updated:
            print("PDFが更新されていません。")
            if discord_webhook:
                notify_no_update(discord_webhook, "meals", "PDFが更新されていません。")
            return False
        
        if not download_pdf(pdf_url, pdf_path):
            print("PDFのダウンロードに失敗しました。")
            if discord_webhook:
                notify_error(discord_webhook, "meals", "PDFのダウンロードに失敗しました。")
            return False
        
        # PDF処理
        print("寮食PDFを処理中...")
        meals_output_dir = output_dir / "meals_output"
        success = process_meals_pdf(
            pdf_path=str(pdf_path),
            out_dir=meals_output_dir,
            model=model,
            api_key=api_key,
            dpi=dpi,
            use_yomitoku=use_yomitoku,
            prompt_file=prompt_file,
        )
        
        if success:
            print("寮食PDF処理が完了しました。")
            if discord_webhook:
                notify_success(
                    discord_webhook,
                    "meals",
                    {"処理済みPDF": pdf_url, "出力ディレクトリ": str(meals_output_dir)},
                )
        else:
            if discord_webhook:
                notify_error(discord_webhook, "meals", "PDF処理に失敗しました。")
        
        return success
    except Exception as e:
        print(f"寮食処理エラー: {e}")
        if discord_webhook:
            notify_error(discord_webhook, "meals", str(e))
        return False


def process_classes(
    output_dir: Path,
    api_key: str,
    model: str = "gemini-2.5-pro",
    dpi: int = 220,
    use_yomitoku: bool = False,
    discord_webhook: Optional[str] = None,
) -> bool:
    """授業PDFの処理"""
    try:
        print("授業ページをスクレイピング中...")
        pdf_url = scrape_classes_page()
        
        if not pdf_url:
            print("授業PDFリンクが見つかりませんでした。")
            if discord_webhook:
                notify_no_update(discord_webhook, "classes", "PDFリンクが見つかりませんでした。")
            return False
        
        print(f"PDF URL: {pdf_url}")
        
        # PDFダウンロード
        pdf_path = output_dir / "pdfs" / "classes.pdf"
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 更新チェック
        is_updated, _ = check_pdf_updated(pdf_url, pdf_path)
        if not is_updated:
            print("PDFが更新されていません。")
            if discord_webhook:
                notify_no_update(discord_webhook, "classes", "PDFが更新されていません。")
            return False
        
        if not download_pdf(pdf_url, pdf_path):
            print("PDFのダウンロードに失敗しました。")
            if discord_webhook:
                notify_error(discord_webhook, "classes", "PDFのダウンロードに失敗しました。")
            return False
        
        # PDF処理
        print("授業PDFを処理中...")
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
            print("授業PDF処理が完了しました。")
            if discord_webhook:
                notify_success(
                    discord_webhook,
                    "classes",
                    {"処理済みPDF": pdf_url, "出力ディレクトリ": str(classes_output_dir)},
                )
        else:
            if discord_webhook:
                notify_error(discord_webhook, "classes", "PDF処理に失敗しました。")
        
        return success
    except Exception as e:
        print(f"授業処理エラー: {e}")
        if discord_webhook:
            notify_error(discord_webhook, "classes", str(e))
        return False


def update_server(
    output_dir: Path,
    server_repo_path: Path,
    github_token: str,
    repo_url: str,
    branch: str = "main",
) -> bool:
    """WakayamaServerを更新"""
    try:
        print("WakayamaServerリポジトリを初期化中...")
        if not init_git_repo(server_repo_path, github_token, repo_url, branch):
            return False
        
        # ファイルをコピー
        classes_output_dir = output_dir / "classes_output"
        meals_output_dir = output_dir / "meals_output"
        
        copied_files = []
        
        if classes_output_dir.exists():
            classes_target = server_repo_path / "v1" / "classes"
            copied = copy_final_files(classes_output_dir, classes_target)
            copied_files.extend(copied)
            print(f"授業データ: {len(copied)}ファイルをコピーしました。")
        
        if meals_output_dir.exists():
            meals_target = server_repo_path / "v1" / "meals"
            copied = copy_meals_files(meals_output_dir, meals_target)
            copied_files.extend(copied)
            print(f"寮食データ: {len(copied)}ファイルをコピーしました。")
        
        if not copied_files:
            print("更新するファイルがありません。")
            return True
        
        # コミット＆プッシュ
        print("変更をコミット・プッシュ中...")
        success = commit_and_push(
            repo_path=server_repo_path,
            github_token=github_token,
            repo_url=repo_url,
            branch=branch,
            commit_message=f"自動更新: {len(copied_files)}ファイルを更新",
        )
        
        return success
    except Exception as e:
        print(f"サーバー更新エラー: {e}")
        return False


def main():
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
    
    # 環境変数から取得
    api_key = args.api_key or os.getenv("GOOGLE_API_KEY")
    discord_webhook = args.discord_webhook or os.getenv("DISCORD_WEBHOOK_URL")
    github_token = args.github_token or os.getenv("GITHUB_TOKEN")
    
    if not api_key:
        print("エラー: APIキーが設定されていません。")
        sys.exit(1)
    
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    success = True
    
    # 処理実行
    if args.process in ["meals", "all"]:
        success &= process_dormitory_meals(
            output_dir=output_dir,
            api_key=api_key,
            model=args.model,
            dpi=args.dpi,
            use_yomitoku=args.use_yomitoku,
            discord_webhook=discord_webhook,
            prompt_file=args.prompt_file,
        )
    
    if args.process in ["classes", "all"]:
        success &= process_classes(
            output_dir=output_dir,
            api_key=api_key,
            model=args.model,
            dpi=args.dpi,
            use_yomitoku=args.use_yomitoku,
            discord_webhook=discord_webhook,
        )
    
    # サーバー更新
    if args.update_server and success:
        if not github_token:
            print("警告: GitHubトークンが設定されていないため、サーバー更新をスキップします。")
        elif not args.server_repo_url:
            print("警告: サーバーリポジトリURLが設定されていないため、サーバー更新をスキップします。")
        else:
            server_repo_path = args.server_repo_path or output_dir / "server_repo"
            update_server(
                output_dir=output_dir,
                server_repo_path=server_repo_path,
                github_token=github_token,
                repo_url=args.server_repo_url,
                branch=args.branch,
            )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

