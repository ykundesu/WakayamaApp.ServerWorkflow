# WakayamaApp.ServerWorkflow

和歌山高専の **寮食メニュー** ・ **クラス時間割** ・ **寮行事予定** ・ **学校規則** を自動で収集・構造化し、別リポジトリ `WakayamaServer` に静的 JSON として配置する自動化ワークフローです。GitHub Actions 上で 1 日 1 回 cron 実行され、Discord に処理結果を通知します。

> **このリポジトリの位置付け**
> モバイル / Web アプリの「裏方」にあたるバッチ処理。アプリは `WakayamaServer` の静的 JSON を直接読みます。アプリから見た **API の実体は本リポジトリが書き出す JSON ファイル** で、その仕様（=静的 API）は [`schemas/v1/`](./schemas/v1/) と [`docs/output_schema.md`](./docs/output_schema.md) に定義しています。

---

## 目次

1. [全体像](#全体像)
2. [アーキテクチャ](#アーキテクチャ)
3. [出力静的 API（v1）](#出力静的-apiv1)
4. [リポジトリ構成](#リポジトリ構成)
5. [ローカル実行](#ローカル実行)
6. [GitHub Actions 運用](#github-actions-運用)
7. [環境変数 / シークレット](#環境変数--シークレット)
8. [LLM プロバイダとモデル指定](#llm-プロバイダとモデル指定)
9. [Wakayama NCT 固有の CA 証明書対応](#wakayama-nct-固有の-ca-証明書対応)
10. [開発ガイド](#開発ガイド)
11. [トラブルシューティング](#トラブルシューティング)
12. [既知の課題と今後の方針](#既知の課題と今後の方針)

---

## 全体像

```
[Wakayama NCT 公式サイト]
        │ 1. scrape  (HTML パース)
        ▼
[scraper/]                 PDF / 画像の URL を取得
        │ 2. download
        ▼
[ハッシュ判定]             既処理 SHA-256 と突き合わせ、未処理だけ続行
        │ 3. process
        ▼
[processors/]              PDF → 画像 → LLM (Gemini / OpenRouter) → JSON
        │ 4. update-server
        ▼
[server_updater/]          WakayamaServer リポにコピーして git push
        │ 5. notify
        ▼
[notifier/discord.py]      成功 / エラー / 更新無しを Discord へ
```

スクレイプ対象 4 種:

| 種類 | スクレイプ対象 | 抽出方法 | 出力 |
|------|--------------|---------|------|
| **meals**（寮食） | 寮食ページの最新献立 PDF | 画像化 → LLM (Vision) | 週ごとに分割した JSON |
| **classes**（時間割） | 授業ページの時間割 PDF | 画像化 → LLM (Vision) | 学年×クラス×期別の JSON |
| **dormitory_events**（寮行事） | 寮ページの行事予定画像 | LLM (Vision) | 年度ごとの JSON |
| **rules**（学校規則） | 学校規則ページの個別 PDF 群 | YomitokuOCR → Markdown → LLM (Text) | 章 / 条文構造の JSON 群 + index / manifest |

---

## アーキテクチャ

詳細図とレイヤごとの責務は [`docs/architecture.md`](./docs/architecture.md) を参照。要点だけ抜粋:

```
main.py
  ├─ scraper/          HTML → PDF/画像 URL の抽出 + ダウンロード
  ├─ common/           PDF→画像、OCR、LLM 呼び出し、JSON 抽出など共通処理
  │   └─ api_client.py     Gemini / OpenRouter のクライアント
  ├─ processors/       LLM プロンプト + スキーマ強制 + 出力 JSON 生成
  ├─ server_updater/   WakayamaServer リポへの配置 + git commit/push
  └─ notifier/         Discord Webhook 通知
```

依存方向は基本上から下への単方向で、循環参照はありません。`common/` のみ複数レイヤから参照されます。

---

## 出力静的 API（v1）

WakayamaServer リポジトリに配置される静的ファイル群:

```
WakayamaServer/
└── v1/
    ├── meals/
    │   └── {YYYY-MM-DD}.json        # 月曜日付。1 ファイル = 1 週間
    ├── classes/
    │   └── {cohortYear}{classCode}/  # 例: 2025B/
    │       └── {grade}_{value}.json  # 例: 2_0.json (2 年・前期)
    ├── dormitory/
    │   └── events/
    │       └── {academic_year}.json  # 1 ファイル = 1 年度
    ├── school-rules/
    │   ├── index.json                # 章 + ルールメタ一覧（アプリの一覧画面の入口）
    │   ├── chapters.json             # 章メタのみ
    │   ├── manifest.json             # バッチ実行追跡（version, counts, models）
    │   └── rules/
    │       └── {ruleId}.json         # 個別ルール本体（'rule-NNNN'）
    └── sources/
        └── list/
            ├── meals.json            # {"processed": [hash...]}
            ├── classes.json          # {"processed": [hash...]}
            ├── school_rules.json     # {"processed": [hash...]}
            └── dormitory_events.json # {"last_url": ..., "last_hash": ...}
```

各 JSON のフィールド仕様は次に分割記載:

- **JSON Schema（機械可読）**: [`schemas/v1/`](./schemas/v1/)
- **解説と例（人間向け）**: [`docs/output_schema.md`](./docs/output_schema.md)
- **既知の不整合と v2 計画**: [`docs/output_schema.md#既知の課題`](./docs/output_schema.md#既知の課題)

> ⚠ v1 の JSON は **リソース間で命名規則・日付フォーマットが揃っていません**（meals/classes/events は snake_case 寄り、rules は camelCase など）。アプリ側の都合で揃えた経緯のため、v1 の互換は維持し、整理は v2 で実施予定です。詳しくは [既知の課題](#既知の課題と今後の方針) を参照。

---

## リポジトリ構成

```
WakayamaApp.ServerWorkflow/
├── main.py                       # CLI エントリポイント
├── requirements.txt              # 依存パッケージ
├── README.md                     # このファイル
│
├── .github/
│   └── workflows/
│       └── server_processor.yml  # cron + workflow_dispatch
│
├── common/                       # レイヤ横断ユーティリティ
│   ├── api_client.py             # Gemini / OpenRouter クライアント、リトライ
│   ├── pdf_processor.py          # ページ単位の LLM 呼び出しオーケストレータ
│   ├── image_utils.py            # PDF → PIL.Image 変換（PyMuPDF）
│   ├── ocr_utils.py              # YomitokuOCR ラッパ（任意）
│   ├── menu_converter.py         # 寮食 LLM 出力の整形
│   ├── json_extractor.py         # LLM 出力からの JSON 抽出 / 修復
│   ├── certificates.py           # Wakayama NCT 用 CA バンドルの動的構築
│   └── scrape_errors.py          # スクレイプ層の例外型
│
├── scraper/                      # スクレイプ + ダウンロード
│   ├── dormitory_scraper.py
│   ├── dormitory_calendar_scraper.py
│   ├── classes_scraper.py
│   ├── school_rules_scraper.py
│   ├── pdf_downloader.py         # PDF DL + 更新チェック
│   └── image_downloader.py       # 画像 DL + 履歴チェック
│
├── processors/                   # PDF → JSON 変換
│   ├── meals_processor.py
│   ├── classes_processor.py
│   ├── dormitory_events_processor.py
│   └── school_rules_processor.py
│
├── server_updater/               # WakayamaServer 反映
│   ├── file_manager.py           # コピー / マージ / 状態ファイル管理
│   └── git_updater.py            # git clone / commit / push
│
├── notifier/
│   └── discord.py                # Discord Webhook 送信
│
├── schemas/                      # 出力静的 API のスキーマ定義
│   ├── README.md
│   └── v1/
│       └── *.schema.json
│
└── docs/                         # 補助ドキュメント
    ├── architecture.md
    └── output_schema.md
```

---

## ローカル実行

### 1. 依存関係のインストール

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`yomitoku`, `opencv-python-headless`, `numpy` は学校規則 OCR に使う任意の依存です（`--use-yomitoku` 指定時のみ実際にロード）。

### 2. 環境変数の設定

`.env` をプロジェクトルートに置くか、シェルで `export` します。最低限:

```bash
export GOOGLE_API_KEY=your_gemini_key
# 任意:
export OPENROUTER_API_KEY=your_openrouter_key
export DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
export GITHUB_TOKEN=ghp_xxx
```

### 3. CLI 実行例

```bash
# すべてのリソースを処理（サーバ更新なし、ローカル output/ にだけ書く）
python main.py --process all

# 個別リソース
python main.py --process meals
python main.py --process classes
python main.py --process dormitory_events
python main.py --process rules --rules-provider gemini --rules-model gemini-2.5-pro

# rules のフォールバックモデル指定（左から順に試行、各 3 回リトライ）
python main.py --process rules \
  --rules-provider gemini \
  --rules-model gemini-2.5-pro,gemini-2.0-flash

# 同等表現（複数回フラグ指定でも可）
python main.py --process rules \
  --rules-provider gemini \
  --rules-model gemini-2.5-pro \
  --rules-model gemini-2.0-flash

# WakayamaServer リポジトリへの反映まで含める
python main.py --process all \
  --update-server \
  --server-repo-url https://github.com/<owner>/WakayamaServer.git \
  --branch main

# Yomitoku OCR を使う（学校規則の精度向上）
python main.py --process rules --use-yomitoku --rules-provider gemini --rules-model gemini-2.5-pro
```

### 主要 CLI オプション

| オプション | 説明 |
|-----------|------|
| `--process {all,meals,classes,dormitory_events,rules}` | 対象リソース。デフォルト `all` |
| `--output-dir PATH` | 出力先（デフォルト `./output`） |
| `--model NAME` | 通常リソース用モデル。`/` を含むと OpenRouter として扱う |
| `--rules-provider {gemini,openrouter}` | 学校規則用プロバイダ |
| `--rules-model NAME[,NAME...]` | 学校規則用モデル（カンマ区切りまたは複数指定でフォールバック）|
| `--openrouter-provider JSON` | 通常リソース用 OpenRouter provider フィルタ |
| `--rules-openrouter-provider JSON` | 学校規則用 OpenRouter provider フィルタ |
| `--dpi INT` | PDF→画像化時の DPI |
| `--use-yomitoku` | 学校規則で YomitokuOCR を併用 |
| `--update-server` | WakayamaServer に反映（指定なしで dry-run 同等） |
| `--server-repo-url URL` | サーバ側リポ URL |
| `--branch NAME` | push 先ブランチ（デフォルト `main`） |
| `--discord-webhook URL` | Discord 通知先（環境変数より優先） |

完全な一覧は `python main.py --help` で確認できます。

---

## GitHub Actions 運用

`.github/workflows/server_processor.yml` が cron で 1 日 1 回（**JST 02:00 / UTC 17:00**）実行されます。

- `workflow_dispatch` による手動実行も可能
- 失敗時は `output/` ディレクトリが Artifact として 7 日間保存されるため、ログ + 中間ファイルから原因特定可能
- Discord に成功・エラー・更新なしの 3 種が通知されます

### CI 実行時のデフォルトモデル

ワークフローでは以下のモデルが指定されています（リポジトリの運用方針による）:

- 通常リソース: `moonshotai/kimi-k2.6` (OpenRouter)
- 学校規則: `openai/gpt-oss-120b:free,openai/gpt-oss-120b` (フォールバックあり)
- OpenRouter provider はコスト・品質の観点で固定

変更する場合は `.github/workflows/server_processor.yml` の `python -X utf8 main.py ...` の引数を編集してください。

---

## 環境変数 / シークレット

| 名前 | 必須 | 用途 |
|------|------|------|
| `GOOGLE_API_KEY` | Gemini を使う場合 | Google AI Studio の API キー |
| `OPENROUTER_API_KEY` | OpenRouter を使う場合 | OpenRouter の API キー |
| `OPENROUTER_PROVIDER` | 任意 | provider フィルタ JSON のデフォルト |
| `DISCORD_WEBHOOK_URL` | 任意 | 通知先 Webhook URL |
| `GITHUB_TOKEN` / `GITHUBACCOUNT_TOKEN` | サーバ更新時 | WakayamaServer への push に使用 |
| `SERVER_REPO_URL` | サーバ更新時 | WakayamaServer の URL |
| `REQUESTS_CA_BUNDLE` / `SSL_CERT_FILE` | Wakayama NCT 接続時 | 後述の CA バンドル |

GitHub Actions 上では Secrets に同名で登録してください（`GITHUB_TOKEN` の代わりに `GITHUBACCOUNT_TOKEN` を使う運用です）。

---

## LLM プロバイダとモデル指定

このプロジェクトは **2 系統の LLM** を使い分けます。

| プロバイダ | 使い方 | 主な用途 |
|-----------|-------|----------|
| **Google Gemini** (`google-genai`) | モデル名に `/` を含まない | meals / classes / events / rules すべて可 |
| **OpenRouter** (`requests` 直叩き) | モデル名に `/` を含む（`vendor/model` 形式）| 同上 |

判定は `main.py:model_uses_openrouter()` で `/` の有無のみ。`provider` フィルタを指定すると、OpenRouter 内のサブプロバイダ（Anthropic / Vertex / DeepInfra など）の優先順位を JSON で制御できます:

```bash
--openrouter-provider '{"order":["anthropic","amazon-bedrock","google-vertex"],"allow_fallbacks":true}'
```

### `--rules-model` のフォールバック

`--rules-model` はカンマ区切り or 複数回指定でフォールバック候補を並べられます。各モデルにつき内部で 3 回までリトライしてから次に進みます（`processors/school_rules_processor.py:request_minimal_payload()` 周辺）。

---

## Wakayama NCT 固有の CA 証明書対応

Wakayama NCT サイトの証明書チェーンには `nii-odca4g8rsa` / `tlsrsarootca2024` が必要で、デフォルトの `certifi` バンドルでは検証エラーになります。GitHub Actions では:

```yaml
- name: Configure Wakayama NCT certificate bundle
  run: |
    python - <<'PY'
    # certifi バンドル + 必要 CA 証明書を結合した独自バンドルを生成
    ...
    PY
```

で `wakayama-ca-bundle.pem` を生成し、`REQUESTS_CA_BUNDLE` / `SSL_CERT_FILE` に設定しています。

ローカル実行時は `common/certificates.py:configure_wakayama_ca_bundle()` が初回ネットワークアクセス時に同等のバンドルを `~/.cache/...` に生成して環境変数を上書きします。

---

## 開発ガイド

### コーディング規約

- **言語**: Python 3.11+
- **型ヒント**: 公開関数 / クラスメソッドには必須。`Dict[str, Any]` は必要悪としつつ、可能なら `TypedDict` / `dataclass` に置換していく方針
- **ログ**: `logging.getLogger(__name__)` を使用。`print()` は使わない
- **例外**: スクレイプ層では `common.scrape_errors.ScrapeError` を投げる（適用は順次拡大中）
- **コメント**: 「何をしているか」より「なぜそうしたか」を書く。プロンプトの周辺・マジックナンバー・特殊な分岐には背景を残す
- **docstring**: 公開関数には `Args` / `Returns` / `Raises` / `Side effects` を揃えて記載

### 新リソースを追加するには

1. `scraper/` に HTML パーサ（`*_scraper.py`）と必要なら専用ダウンローダを追加
2. `processors/` に PDF→JSON 変換器を追加（既存 processor 参照、特に `meals_processor.py` がシンプル）
3. `schemas/v1/` に対応する `*.schema.json` を追加し、`docs/output_schema.md` に説明を追記
4. `server_updater/file_manager.py` に配置先パス + マージ関数を追加
5. `main.py` の argparse / 分岐に新リソースを追加（将来 `ResourcePipeline` に集約予定）
6. README の出力ツリーに反映

### スキーマ変更ポリシー

- v1 のフィールド削除・リネームは **禁止**（アプリ互換のため）
- v1 への **追加** は OK ですが、`additionalProperties: false` を尊重するため `schemas/v1/*.schema.json` も同時更新
- 大きな整理（命名規則統一など）は v2 として `schemas/v2/` を別途作成し、移行計画を `docs/output_schema.md` に書く

### テスト方針

現時点では自動テストは整備されていません。回帰確認は:

1. ローカルで `python main.py --process all --output-dir ./before` を main ブランチで実行
2. ブランチ切替して `--output-dir ./after` で実行
3. `diff -r before after` で出力 JSON 差分を確認

CI 上のスモークテストとして `jsonschema` で出力検証する仕組みを今後追加予定。

---

## トラブルシューティング

| 症状 | 原因の目安 | 対応 |
|------|-----------|------|
| `SSL: CERTIFICATE_VERIFY_FAILED` | Wakayama NCT 用 CA が未設定 | `REQUESTS_CA_BUNDLE` 設定 / GitHub Actions の証明書ステップ参照 |
| LLM が JSON を返さない / 壊れた JSON | プロンプトとスキーマのミスマッチ、モデル変更 | `processors/*_processor.py` の `*_PROMPT` を確認、`json_extractor.py` の修復ロジックも参照 |
| `503` / `UNAVAILABLE` で止まる | Gemini/OpenRouter の一時的不調 | `tenacity` で 503 のみ自動リトライ。それでも失敗なら `--rules-model` のフォールバックを増やす |
| `WakayamaServer` への push 失敗 | `GITHUB_TOKEN` の権限不足、ブランチ保護 | トークンスコープを確認。`--branch` 指定の妥当性も |
| Discord 通知が届かない | Webhook URL 失効 / レート制限 | URL 再発行、`notifier/discord.py` のログ確認 |
| 出力 JSON が空 | スクレイプ対象 URL の HTML 構造変化 | `scraper/*.py` の CSS セレクタを更新 |

---

## 既知の課題と今後の方針

直近のリファクタ計画（プランニングは完了済み、実装は段階的に進行中）:

| # | 課題 | 状態 |
|---|------|------|
| 1 | スキーマを `schemas/v1/` に外部化 | ✅ 完了（このコミット） |
| 2 | README / docs の充実 | ✅ 完了（このコミット） |
| 3 | 出力 JSON 命名規則・日付フォーマットの v2 統一 | 設計中 |
| 4 | meals / classes / events 用の `manifest.json` 追加 | 設計中 |
| 5 | `LLMCaller` Protocol で Gemini / OpenRouter 抽象化 | 設計中 |
| 6 | `main.py` の 4 リソース処理関数を `ResourcePipeline` に統合 | 設計中 |
| 7 | 設定値を `WorkflowConfig` dataclass に集約 | 設計中 |
| 8 | `StateStore` クラスでハッシュ管理を集約 | 設計中 |
| 9 | スモークテスト（出力 JSON のスキーマ検証）CI 化 | 設計中 |

設計の詳細は [`docs/architecture.md#改善ロードマップ`](./docs/architecture.md#改善ロードマップ) を参照。

---

## ライセンス

このリポジトリの利用条件はリポジトリ管理者の指示に従ってください（明示的なライセンスファイルは現時点で添付されていません）。
