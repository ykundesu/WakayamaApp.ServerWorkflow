# ServerProcesser自動化ワークフロー

このディレクトリには、ServerProcesserをGitHub Actions上で自動実行するためのワークフローが含まれています。

## 概要

- 寮食ページと授業ページから最新のPDFをスクレイピング
- PDFを処理してJSONデータを生成
- WakayamaServerリポジトリを更新
- Discord Webhookで処理結果を通知

## ディレクトリ構造

```
WakayamaApp.ServerWorkflow/
├── .github/workflows/          # GitHub Actionsワークフロー
├── common/                     # 共通ライブラリ
├── scraper/                    # スクレイピング
├── processors/                 # PDF処理
├── server_updater/             # WakayamaServer更新
├── notifier/                   # Discord通知
├── main.py                     # メインエントリーポイント
└── requirements.txt            # 依存関係
```

## セットアップ

1. 依存関係をインストール:
```bash
pip install -r requirements.txt
```

2. 環境変数を設定:
- `GOOGLE_API_KEY`: Google Gemini APIキー
- `DISCORD_WEBHOOK_URL`: Discord Webhook URL（オプション）
- `GITHUB_TOKEN`: GitHubトークン（サーバー更新用）

## 使用方法

### ローカル実行

```bash
# すべての処理を実行
python main.py --process all

# 寮食のみ処理
python main.py --process meals

# 授業のみ処理
python main.py --process classes

# サーバー更新も含める
python main.py --process all --update-server --server-repo-url https://github.com/user/repo.git
```

### GitHub Actions

`.github/workflows/server_processor.yml` を設定して、cronで自動実行できます。

## 環境変数

GitHub Secretsに以下を設定してください:

- `GOOGLE_API_KEY`: Google Gemini APIキー
- `DISCORD_WEBHOOK_URL`: Discord Webhook URL
- `GITHUBACCOUNT_TOKEN`: GitHubトークン（WakayamaServer更新用）
- `SERVER_REPO_URL`: WakayamaServerリポジトリURL（例: https://github.com/user/WakayamaServer.git）

