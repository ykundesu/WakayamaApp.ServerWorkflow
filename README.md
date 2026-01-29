# WakayamaApp自動化ワークフロー

このレポジトリでは、自動で寮食・授業・学校規則情報を取得するプログラム、それをGitHub Actions上で自動実行するためのワークフローが含まれています。

## 概要

- 寮食ページ・授業ページ・学校規則ページから最新のPDFをスクレイピング
- PDFや規則文書を処理してJSONデータを生成
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
- `OPENROUTER_API_KEY`: OpenRouter APIキー（学校規則の抽出でOpenRouterを使う場合）
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

# 学校規則のみ処理
python main.py --process rules --rules-provider gemini --rules-model gemini-2.5-pro

# 学校規則モデルのフォールバック指定（先頭から順に3回ずつリトライ）
python main.py --process rules --rules-provider gemini --rules-model gemini-2.5-pro,gemini-2.0-flash

# もしくは複数回指定
python main.py --process rules --rules-provider gemini --rules-model gemini-2.5-pro --rules-model gemini-2.0-flash

# サーバー更新も含める
python main.py --process all --update-server --server-repo-url https://github.com/user/repo.git
```

### GitHub Actions

`.github/workflows/server_processor.yml` を設定して、cronで自動実行できます。

## 環境変数

GitHub Secretsに以下を設定してください:

- `GOOGLE_API_KEY`: Google Gemini APIキー
- `OPENROUTER_API_KEY`: OpenRouter APIキー（学校規則の抽出でOpenRouterを使う場合）
- `DISCORD_WEBHOOK_URL`: Discord Webhook URL
- `GITHUBACCOUNT_TOKEN`: GitHubトークン（情報リポジトリの更新用）
- `SERVER_REPO_URL`: WakayamaServerリポジトリURL（例: https://github.com/user/WakayamaServer.git）

