# アーキテクチャ

WakayamaApp.ServerWorkflow の構造と、各レイヤの設計意図を解説します。コード読解の入口として参照してください。

---

## レイヤ構成

```
┌──────────────────────────────────────────────────────────┐
│  main.py                                                  │
│   - argparse による CLI                                  │
│   - 環境変数読み取り                                     │
│   - リソース別の処理関数（meals/classes/events/rules）   │
│   - サーバ更新 + 通知のオーケストレーション             │
└────────────────────┬─────────────────────────────────────┘
                     │
       ┌─────────────┼─────────────┬──────────────┐
       ▼             ▼             ▼              ▼
┌────────────┐ ┌──────────┐ ┌───────────────┐ ┌──────────┐
│ scraper/   │ │processors│ │server_updater/│ │notifier/ │
│            │ │          │ │               │ │          │
│ HTML →     │ │ PDF →    │ │ コピー +      │ │ Discord  │
│ PDF/画像   │ │ JSON     │ │ git push      │ │ Webhook  │
│ URL + DL   │ │          │ │               │ │          │
└─────┬──────┘ └─────┬────┘ └───────┬───────┘ └──────────┘
      │              │              │
      └──────────────┴──────────────┘
                     │
                     ▼
              ┌──────────────┐
              │  common/     │
              │              │
              │ - api_client │  Gemini / OpenRouter クライアント
              │ - pdf_proc.. │  ページ単位 LLM オーケストレータ
              │ - image_utils│  PDF→画像（PyMuPDF）
              │ - ocr_utils  │  YomitokuOCR ラッパ
              │ - json_extr  │  LLM 出力修復
              │ - menu_conv  │  寮食出力整形
              │ - certif..   │  Wakayama NCT 用 CA バンドル
              │ - scrape_err │  例外型
              └──────────────┘
```

依存方向は基本上から下、`common/` のみ各レイヤから参照されます。**循環参照はありません**（2026-05 時点）。

---

## データフロー

```
[公式サイト]
    │
    │  scraper/*_scraper.py  (BeautifulSoup4)
    ▼
[PDF/画像 URL の一覧]
    │
    │  scraper/pdf_downloader.py / image_downloader.py
    ▼
[ローカル PDF/画像]──── SHA-256 ───┐
    │                              │
    │                              ▼
    │                  [v1/sources/list/*.json と突き合わせ]
    │                              │
    │             既処理ならスキップ │
    │                              │
    │  processors/*_processor.py   │
    ▼                              │
[PIL.Image (PDF→画像化)]            │
    │                              │
    │  common/api_client.py        │
    ▼  (Gemini or OpenRouter)      │
[LLM 出力 JSON]                     │
    │                              │
    │  common/json_extractor.py    │
    ▼                              │
[スキーマ準拠 JSON]                 │
    │                              │
    │  processors/* 内で書き出し    │
    ▼                              │
[output/{resource}_output/]        │
    │                              │
    │  server_updater/file_manager │
    ▼                              │
[WakayamaServer/v1/...]            │
    │                              │
    │  server_updater/git_updater  │
    ▼                              │
[git commit + push]                │
    │                              │
    │  notifier/discord.py         │
    ▼                              │
[Discord Webhook]──────────────────┘
```

---

## 各モジュールの責務

### `main.py`
- argparse による CLI 解釈
- 環境変数の読み取り（`GOOGLE_API_KEY` など）
- `--process` フラグに応じた `process_dormitory_meals()` / `process_classes()` / `process_dormitory_events()` / `process_school_rules` 呼び出しの制御
- 各処理結果（成否 + 収集ハッシュ + 何かを処理したか）を集約し、最後に `update_server()` と Discord 通知を行う
- **責務過多** で、現在 1054 行。`ResourcePipeline` への分解が改善ロードマップ上で計画されている

### `scraper/`
公式サイトの HTML を `requests` + `BeautifulSoup4` でパースし、対象 PDF / 画像の URL リストを返す。

| ファイル | 役割 |
|---------|------|
| `dormitory_scraper.py` | 寮食ページ → PDF 候補一覧 |
| `dormitory_calendar_scraper.py` | 寮ページ → 行事予定画像 1 件 |
| `classes_scraper.py` | 授業ページ → 学年別時間割 PDF |
| `school_rules_scraper.py` | 学校規則ページ → 章 / 規則 PDF の階層構造 |
| `pdf_downloader.py` | PDF DL + Content-Type/マジックナンバー検証 + 更新チェック |
| `image_downloader.py` | 画像 DL + 履歴比較 |

戻り値型はリソースによって異なる（`Optional[str]` / `List[Dict[str, Any]]` / `Optional[Dict[str, Any]]` 等）。共通 Protocol への統一は計画段階。

### `common/api_client.py`
LLM クライアント。`GeminiCaller` と `OpenRouterCaller` の 2 系統（モジュール関数 `call_gemini_multimodal()` も並存）。

- **リトライ**: `tenacity` + `is_503_error()` で 503 / `UNAVAILABLE` / google-genai 独自エラー形式に対応
- **Retry-After 解釈**: `_retry_after_from_headers()` で標準ヘッダ + RateLimit ヘッダを処理
- **構造化出力**: `response_format`（OpenRouter）/ `response_json_schema`（Gemini）でスキーマ強制
- **マルチモーダル**: 画像を data URL でエンコードして渡す

> 現状は `use_openrouter` フラグの分岐が呼び出し側に残っている。`LLMCaller(Protocol)` に統一する予定。

### `common/pdf_processor.py`
ページ単位の LLM 呼び出しオーケストレータ。OCR ありモード（YomitokuOCR で前段処理）と純粋画像モードを切り替える。

### `common/image_utils.py`
PyMuPDF を使った PDF → `PIL.Image` 変換 (`render_pdf_pages()`)。DPI 指定で解像度を制御。

### `common/ocr_utils.py`
YomitokuOCR の遅延ロード + シングルトン管理。`opencv-python-headless` / `numpy` も任意依存として扱う。

### `common/json_extractor.py`
LLM 出力に混じる ` ```json ... ``` ` ブロックの剥がし、不正 JSON の修復（`{` `}` のバランスチェック）など。

### `common/menu_converter.py`
寮食 LLM 出力を `MenuDay` / `MenuItem` 配列に整形。'共通' 欄の展開などの後処理を担当。

### `common/certificates.py`
Wakayama NCT サイトの証明書チェーンが `certifi` バンドルに含まれないため、必要 CA を動的にダウンロードして合成バンドルを作成し、`REQUESTS_CA_BUNDLE` / `SSL_CERT_FILE` に設定する。

### `processors/`
PDF → 画像 → LLM → JSON のリソース別処理。各 processor が:

1. 出力ディレクトリ作成
2. `PDFProcessor` 初期化（OCR / API キー / モデル / プロンプト保持）
3. ページごとに LLM 呼び出し
4. 結果をマージ、後処理（週ごと分割、章 / 条文ツリー組み立てなど）
5. 出力 JSON 書き出し

**スキーマ定義（`*_SCHEMA`）はソース内の dict として保持** しています。同等の JSON Schema が `schemas/v1/` にも置いてあり、ドキュメント / アプリ側参照用に使えます。Python から JSON ファイル参照に切り替える作業は、出力同等性の検証込みで段階的に進める計画です。

### `server_updater/file_manager.py`
- `output/` のリソース別 JSON を `WakayamaServer/v1/` 以下にコピー
- `processed_hashes` / `dormitory_events_state` のマージ + 書き戻し
- `has_server_target_data()` でサーバ側の存在確認（初回 push 判定に使用）

### `server_updater/git_updater.py`
- `git clone` / `git add` / `git commit` / `git push`
- トークンを含む URL の組み立て（`https://x-access-token:TOKEN@github.com/...`）

### `notifier/discord.py`
- `notify_success()` / `notify_error()` / `notify_no_update()` の 3 種
- Embed の組み立てと POST を 1 関数で行う（フォーマッタ / センダーの分離は今後）

---

## 例外戦略

### スクレイプ層
`common/scrape_errors.ScrapeError` を投げる方針ですが、現状は **未適用** の箇所が多く `Exception` を直接 raise しているところがあります（順次 `ScrapeError` への移行予定）。

### 処理層
`processors/` 系では `except Exception` で握り、`logger.exception()` の上で `False` を返すパターンが基本。LLM 失敗時のフォールバック先としては `--rules-model` の複数指定が利用できます。

### 通知層
通知の失敗は **業務継続を優先** して握りつぶしてログのみ。Discord 障害がワークフロー全体を止めないように。

---

## 冪等性 / 差分検知

- 各 PDF / 画像の **SHA-256 を `v1/sources/list/{target}.json` に蓄積**
- 既存ハッシュに含まれるものは LLM 呼び出しをスキップ
- `dormitory_events` だけは月次上書きの単一画像なので、`last_url` / `last_hash` の 1 ペアだけ保持
- `school_rules` は PDF URL の変化 + ハッシュの変化 + メタデータ（章タイトル等）の変化を **個別に判定** し、必要なルールだけ regenerate する仕組みがある（`processors/school_rules_processor.py:783-870` 付近）

これにより、LLM コストとサーバ git ログのノイズを抑えています。

---

## ロギング

- `logging.getLogger(__name__)` で統一
- `main.py` の `logging.basicConfig()` でフォーマットとレベルを設定
- `logger.info` は通常進捗、`logger.debug` は詳細、`logger.warning` は回復可能、`logger.error` は致命
- `print()` は使わない方針（過去残存箇所は順次置換）

---

## 設定値の所在

| 値 | 現在の所在 |
|----|-----------|
| LLM モデル名 | CLI `--model` / argparse default / GitHub Actions YAML |
| DPI | CLI `--dpi` / argparse default / 各 processor のデフォルト引数（不揃い） |
| 出力ディレクトリ | CLI `--output-dir` |
| サーバ branch | CLI `--branch` |
| OpenRouter provider | CLI `--openrouter-provider` / `OPENROUTER_PROVIDER` 環境変数 |
| API キー | 環境変数 |
| Discord Webhook | CLI `--discord-webhook` / 環境変数 |
| スクレイプ対象 URL | 各 scraper のモジュール定数（複数ファイルに散在） |

> 散在しているデフォルト値を `WorkflowConfig` dataclass に集約する計画があります（[改善ロードマップ](#改善ロードマップ) 参照）。

---

## 改善ロードマップ

整理された設計プランは以下のとおり。実装は段階的に進めます。

### 短期（破壊的変更なし）

1. **スキーマ外部化** ✅ — `schemas/v1/*.schema.json` を追加（このコミットで完了）
2. **README / docs 充実** ✅ — このドキュメント含む（完了）
3. **マジックナンバーの定数化** — `1_000_000_000`（epoch 判定）など名前付けと根拠コメント
4. **`print()` 残存箇所の logger 化** — `main.py:426, 467, 500`
5. **`ScrapeError` の実適用拡大** — 各 scraper を順次 `ScrapeError` raise に統一

### 中期（リファクタ、出力同等性検証込み）

6. **`LLMCaller` Protocol** — Gemini / OpenRouter / フォールバックを統一インターフェイスに
7. **`WorkflowConfig` dataclass** — 散在する設定値の集約、CLI / 環境変数 / CI YAML との対応明確化
8. **`StateStore` クラス** — `load_processed_hashes` / `merge_and_write_*` の集約
9. **`ResourcePipeline`** — `main.py` の 4 リソース処理関数の共通化（`DL → ハッシュ → 処理 → 通知` パターン）
10. **`manifest.json` の全リソース化** — `school_rules` 以外にも生成日時・モデル・カウントを記録
11. **スモークテスト** — 出力 JSON を `schemas/v1/*` で検証する CI ステップ追加

### 長期（v2 リリースとセット）

12. **v2 スキーマ定義** — `schemas/v2/*.schema.json` を追加（仕様のみ、生成は別 PR）
13. **v2 出力生成** — 命名統一、ISO8601 統一、`meta` envelope、明示 ID、エラー表現
14. **WakayamaServer / アプリ側の v2 対応**（このリポジトリの範囲外）

---

## 設計上の意思決定メモ

### なぜ Python か
LLM SDK（`google-genai`）/ PDF（`pymupdf`）/ OCR（`yomitoku`）/ HTML スクレイプ（`beautifulsoup4`）の 4 つを 1 リポジトリで扱う必要があり、これらが揃うエコシステムは Python が最も成熟。Go や Rust への移行は依存ライブラリの置換コストに見合いません。

### なぜ静的 JSON か（API サーバではなく）
- アプリ側の運用コストが低い（GitHub Pages 等のホスティングで十分）
- 履歴が git で残る（過去の出力にいつでもロールバック可）
- 1 日 1 回の更新で十分な情報の性質（リアルタイム性は要らない）

### なぜ 4 リソースを 1 リポにまとめているか
- 共通のインフラ（CA 証明書、LLM クライアント、git push、Discord 通知）を共有できる
- 1 回の cron で全部処理するシンプルな運用

### なぜハッシュベースの冪等性か
- LLM 課金を最小化したい
- WakayamaServer の git ログをノイズで埋めたくない
- ファイルベースなので状態管理が単純（DB 不要）
