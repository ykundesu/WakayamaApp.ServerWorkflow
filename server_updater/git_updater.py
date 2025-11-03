#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Git操作
WakayamaServerリポジトリへのコミット＆プッシュ
"""

import os
import logging
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


def init_git_repo(repo_path: Path, github_token: str, repo_url: str, branch: str = "main") -> bool:
    """
    Gitリポジトリを初期化またはクローン
    
    Args:
        repo_path: リポジトリのローカルパス
        github_token: GitHubトークン
        repo_url: リポジトリURL（例: https://github.com/user/repo.git）
        branch: ブランチ名
    
    Returns:
        成功したかどうか
    """
    logger.info(f"Gitリポジトリを初期化中: repo_path={repo_path}, branch={branch}")
    try:
        # リポジトリURLにトークンを埋め込む
        if "github.com" in repo_url and "@" not in repo_url:
            # https://github.com/user/repo.git -> https://TOKEN@github.com/user/repo.git
            url_parts = repo_url.replace("https://", "").split("/")
            auth_url = f"https://{github_token}@{'/'.join(url_parts)}"
        else:
            auth_url = repo_url
        
        if not repo_path.exists() or not (repo_path / ".git").exists():
            # クローン
            logger.info(f"リポジトリをクローン中: {repo_url}")
            repo_path.parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                ["git", "clone", "-b", branch, auth_url, str(repo_path)],
                check=True,
                capture_output=True,
            )
            logger.info("リポジトリのクローンが完了しました")
        else:
            # プル
            logger.info("既存リポジトリを更新中...")
            os.chdir(repo_path)
            logger.debug("git fetchを実行中...")
            subprocess.run(["git", "fetch"], check=True, capture_output=True)
            logger.debug(f"ブランチ {branch} に切り替え中...")
            subprocess.run(["git", "checkout", branch], check=True, capture_output=True)
            logger.debug("git pullを実行中...")
            subprocess.run(["git", "pull"], check=True, capture_output=True)
            logger.info("リポジトリの更新が完了しました")
        
        return True
    except Exception as e:
        logger.error(f"Gitリポジトリ初期化エラー: {e}", exc_info=True)
        return False


def commit_and_push(
    repo_path: Path,
    github_token: str,
    repo_url: str,
    branch: str = "main",
    commit_message: str = "Update data from automated workflow",
    files: Optional[List[Path]] = None,
) -> bool:
    """
    変更をコミットしてプッシュ
    
    Args:
        repo_path: リポジトリのローカルパス
        github_token: GitHubトークン
        repo_url: リポジトリURL
        branch: ブランチ名
        commit_message: コミットメッセージ
        files: コミットするファイルのリスト（Noneの場合はすべて）
    
    Returns:
        成功したかどうか
    """
    logger.info(f"Gitコミット・プッシュを開始: branch={branch}")
    logger.debug(f"コミットメッセージ: {commit_message}")
    try:
        os.chdir(repo_path)
        
        # ユーザー設定
        logger.debug("Gitユーザー設定中...")
        subprocess.run(
            ["git", "config", "user.name", "GitHub Actions"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "actions@github.com"],
            check=True,
            capture_output=True,
        )
        
        # 変更をステージング
        logger.debug("変更をステージング中...")
        if files:
            logger.debug(f"指定ファイルのみステージング: {len(files)}ファイル")
            for file_path in files:
                if file_path.exists():
                    subprocess.run(
                        ["git", "add", str(file_path.relative_to(repo_path))],
                        check=True,
                        capture_output=True,
                    )
        else:
            logger.debug("すべての変更をステージング")
            subprocess.run(["git", "add", "."], check=True, capture_output=True)
        
        # 変更があるかチェック
        logger.debug("変更の有無を確認中...")
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            capture_output=True,
        )
        if result.returncode == 0:
            logger.info("変更がありません。コミットをスキップします。")
            return True
        
        # コミット
        logger.info("コミットを実行中...")
        subprocess.run(
            ["git", "commit", "-m", commit_message],
            check=True,
            capture_output=True,
        )
        logger.info("コミットが完了しました")
        
        # プッシュ
        logger.info("プッシュを実行中...")
        # リポジトリURLにトークンを埋め込む
        if "github.com" in repo_url and "@" not in repo_url:
            url_parts = repo_url.replace("https://", "").split("/")
            auth_url = f"https://{github_token}@{'/'.join(url_parts)}"
        else:
            auth_url = repo_url
        
        # リモートURLを設定
        subprocess.run(
            ["git", "remote", "set-url", "origin", auth_url],
            check=True,
            capture_output=True,
        )
        
        subprocess.run(
            ["git", "push", "origin", branch],
            check=True,
            capture_output=True,
        )
        logger.info("プッシュが完了しました")
        
        return True
    except Exception as e:
        logger.error(f"Gitコミット・プッシュエラー: {e}", exc_info=True)
        return False

