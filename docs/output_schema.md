# 出力静的 API（v1）仕様書

WakayamaApp.ServerWorkflow が `WakayamaServer` リポジトリに書き出す JSON の **データ仕様書** です。アプリ側はこの形を読みます。機械可読な JSON Schema は [`schemas/v1/`](../schemas/v1/) 配下にあります。

---

## 目次

- [配置レイアウト](#配置レイアウト)
- [meals — 寮食メニュー](#meals--寮食メニュー)
- [classes — 時間割](#classes--時間割)
- [dormitory_events — 寮行事予定](#dormitory_events--寮行事予定)
- [school_rules — 学校規則](#school_rules--学校規則)
- [sources/list — 処理済み状態](#sourceslist--処理済み状態)
- [既知の課題](#既知の課題)
- [v2 ドラフト](#v2-ドラフト)

---

## 配置レイアウト

```
v1/
├── meals/{YYYY-MM-DD}.json
├── classes/{cohortYear}{classCode}/{grade}_{value}.json
├── dormitory/events/{academic_year}.json
├── school-rules/
│   ├── index.json
│   ├── chapters.json
│   ├── manifest.json
│   └── rules/{ruleId}.json
└── sources/list/{target}.json
```

すべての JSON は **UTF-8 / インデント 2 スペース / `ensure_ascii=False`** で書き出されます。

---

## meals — 寮食メニュー

- 配置先: `v1/meals/{YYYY-MM-DD}.json`（ファイル名はその週の **月曜日** の日付）
- 1 ファイル = 1 週間分
- スキーマ: [`schemas/v1/meals.schema.json`](../schemas/v1/meals.schema.json)

### 例

```json
{
  "menus": [
    {
      "day": "04/15",
      "breakfast": [
        {
          "type": "A",
          "main": "目玉焼き",
          "subs": ["味噌汁", "ライス", "サラダ"],
          "isRice": true,
          "isCurry": false,
          "nutritional": {"E": 580, "P": 22.1, "F": 18.4, "Ca": 95, "S": 2.3}
        },
        {
          "type": "B",
          "main": "目玉焼き",
          "subs": ["味噌汁", "パン", "サラダ"],
          "isRice": false,
          "isCurry": false,
          "nutritional": {"E": 560, "P": 20.0, "F": 17.0, "Ca": 92, "S": 2.4}
        }
      ],
      "lunch": [...],
      "dinner": [...]
    }
  ]
}
```

### フィールド要点

| フィールド | 型 | 意味 / 注意 |
|-----------|-----|-------------|
| `menus[].day` | string `MM/DD` | **年情報なし**。ファイル名（週の月曜日）から復元 |
| `menus[].breakfast/lunch/dinner` | `MenuItem[]` \| `null` | `null` は休日等で PDF にデータがないこと。空配列にはしない |
| `MenuItem.type` | string | `'A'` / `'B'` / `'共通'` など。PDF 上の区分に対応 |
| `MenuItem.main` | string | 主菜。朝食では PDF 最上段のメニューを A/B 双方の `main` に複製 |
| `MenuItem.subs` | string[] | 副菜・汁物・主食。PDF の '共通' 欄は全 type に展開済み |
| `MenuItem.isRice` | bool | ライス系か（**camelCase / snake_case 混在の数少ない箇所**） |
| `MenuItem.isCurry` | bool | カレーか |
| `MenuItem.nutritional.{E,P,F,Ca,S}` | number | E=kcal, P/F=g, Ca=mg, S=g（食塩相当）|

---

## classes — 時間割

- 配置先: `v1/classes/{cohortYear}{classCode}/{grade}_{value}.json`
  - 例: `2025B/2_0.json` = 2025 年度入学・B コース・2 年・前期
  - `cohortYear` は入学年度、`classCode` はコース記号
  - `grade` は学年、`value` は学期 / 期別の識別子（前期=0, 後期=1 など）
- スキーマ: [`schemas/v1/classes.schema.json`](../schemas/v1/classes.schema.json)

### 例

```json
{
  "2": {
    "B": [
      {
        "day": 0,
        "classes": [
          {"start": "09:00", "end": "10:30", "name": "数学II", "teacher": "山田 太郎"},
          {"start": "10:40", "end": "12:10", "name": "物理学(140分)", "teacher": null}
        ]
      },
      {"day": 1, "classes": [...]}
    ]
  }
}
```

### フィールド要点

| フィールド | 型 | 意味 |
|-----------|-----|------|
| トップレベルキー | string | 学年番号（`'1'`, `'2'`, ...） |
| 学年下のキー | string | クラス記号（`'B'` など） |
| `day` | int 0-6 | 0=月, 1=火, ..., 6=日（通常 0-4）|
| `classes[].start/end` | string `HH:MM` | 24h JST。**日付情報は持たない** |
| `classes[].name` | string | 科目名。140 分等の特殊コマは末尾に `(140 分)` |
| `classes[].teacher` | string \| null | 担当教員。未定は null |

---

## dormitory_events — 寮行事予定

- 配置先: `v1/dormitory/events/{academic_year}.json`
- 1 ファイル = 1 年度分（4 月始まり）
- スキーマ: [`schemas/v1/dormitory_events.schema.json`](../schemas/v1/dormitory_events.schema.json)

### 例

```json
{
  "academic_year": 2025,
  "events": [
    {"date": "04/08", "grade": null, "name": "入寮式"},
    {"date": "05/15", "grade": 1,    "name": "1年生歓迎会"},
    {"date": "06/20", "grade": 5,    "name": "卒業準備説明会"}
  ]
}
```

### フィールド要点

| フィールド | 型 | 意味 |
|-----------|-----|------|
| `academic_year` | int (任意) | 西暦年度。なければファイル名から復元 |
| `events[].date` | string `MM/DD` | 年は `academic_year` から復元。期間ものは複数行に展開済み |
| `events[].grade` | int 1-5 \| null | 対象学年。**'全学年' / '対象なし' / '不明' すべて null** |
| `events[].name` | string | 行事名 |

---

## school_rules — 学校規則

学校規則だけは複数ファイル構成で、最も重い静的 API です。

### 配置

```
v1/school-rules/
├── index.json     ← アプリ一覧画面の入口
├── chapters.json  ← 章メタのみ
├── manifest.json  ← バッチ実行の追跡情報
└── rules/
    └── rule-0001.json
    └── rule-0002.json
    ...
```

### `rules/{ruleId}.json` （個別ルール）

スキーマ: [`schemas/v1/rule_detail.schema.json`](../schemas/v1/rule_detail.schema.json)

```json
{
  "id": "rule-0001",
  "chapterId": "chapter-0001",
  "title": "一般規則",
  "order": 1,
  "pdfUrl": "https://www.wakayama-nct.ac.jp/...rules.pdf",
  "summary": "...",
  "sections": [
    {
      "title": "総則",
      "articles": [
        {"label": "第一条", "content": "..."},
        {"label": "第二条", "content": "..."}
      ]
    }
  ],
  "sourcePage": null,
  "lastUpdated": "2025-05-03T17:00:00+00:00"
}
```

`label` が null のときは LLM が条文ラベルを認識できなかったケース。`summary` も同様。

### `index.json`

スキーマ: [`schemas/v1/rules_index.schema.json`](../schemas/v1/rules_index.schema.json)

```json
{
  "version": "20250503T170000Z",
  "generatedAt": "2025-05-03T17:00:00+00:00",
  "chapters": [
    {"id": "chapter-0001", "title": "一般", "order": 1}
  ],
  "rules": [
    {
      "id": "rule-0001",
      "chapterId": "chapter-0001",
      "title": "一般規則",
      "summary": "...",
      "order": 1,
      "pdfUrl": "...",
      "sourcePage": null,
      "lastUpdated": "2025-05-03T17:00:00+00:00"
    }
  ]
}
```

`rules` は `(chapter.order, rule.order)` 昇順でソート済みです。

### `chapters.json`

`index.json` の `chapters` フィールドだけを切り出した形（同じ version / generatedAt を持つ）。アプリが章一覧だけ欲しい時の軽量 API。

### `manifest.json`

スキーマ: [`schemas/v1/rules_manifest.schema.json`](../schemas/v1/rules_manifest.schema.json)

```json
{
  "version": "20250503T170000Z",
  "generatedAt": "2025-05-03T17:00:00+00:00",
  "rulesTotal": 23,
  "rulesUpdated": 3,
  "rulesRegenerated": 1,
  "rulesFailed": 0,
  "updatedRuleIds": ["rule-0007", "rule-0011", "rule-0019"],
  "regeneratedRuleIds": ["rule-0019"],
  "failedRuleIds": [],
  "removedRuleIds": [],
  "rulesUrl": "https://www.wakayama-nct.ac.jp/.../rules/",
  "provider": "openrouter",
  "model": "openai/gpt-oss-120b:free",
  "models": ["openai/gpt-oss-120b:free", "openai/gpt-oss-120b"]
}
```

> **学校規則のみメタデータが充実している** のは、章 / 条文の動的な追加・削除・再生成を追跡する必要があるためです。他リソースもこの粒度に揃える計画があります（[既知の課題](#既知の課題) 参照）。

---

## sources/list — 処理済み状態

冪等性チェック用の状態ファイル。アプリ側からは通常参照しません（運用ツール用）。

### `meals.json` / `classes.json` / `school_rules.json`

```json
{
  "processed": [
    "0123abcd...",
    "fedc4567..."
  ]
}
```

- 値は **PDF/画像の SHA-256（小文字 hex 64 文字）**
- 処理が完了すると追記される
- 同じハッシュが既に存在する PDF は LLM 呼び出しをスキップ

### `dormitory_events.json`

```json
{
  "last_url": "https://www.wakayama-nct.ac.jp/.../events.png",
  "last_hash": "0123abcd..."
}
```

- 寮行事は **月次に上書きされる単一画像** なので、最終 URL とハッシュだけ保持
- `last_url` が変わるか `last_hash` が変わったときだけ再処理

スキーマ: [`schemas/v1/sources_list.schema.json`](../schemas/v1/sources_list.schema.json)

---

## 既知の課題

v1 の設計を運用しながら見えてきた、**アプリ側に押し付けてしまっている不整合** をリスト化しています。v1 互換のため即時の変更は行わず、v2 で整理予定です。

### 1. 命名規則の不統一

| リソース | 命名 | 例 |
|---------|-----|-----|
| meals | snake_case + camelCase 混在 | `day`, `breakfast`, **`isRice`**, **`isCurry`** |
| classes | snake_case | `start`, `end`, `teacher` |
| dormitory_events | snake_case | `academic_year`, `date`, `grade` |
| school_rules | 完全 camelCase | `chapterId`, `pdfUrl`, `lastUpdated`, `generatedAt` |

### 2. 日付フォーマットの不統一

| リソース | フォーマット | 年情報 |
|---------|------------|-------|
| meals.day | `MM/DD` | ファイル名から復元 |
| classes.start/end | `HH:MM` | 持たない（時刻のみ） |
| dormitory_events.date | `MM/DD` | `academic_year` から復元 |
| rules.lastUpdated | ISO8601 | あり |

### 3. ID の不統一

`school_rules` のみ `id` フィールドがあり、他はファイル名がキー。

### 4. メタデータの非対称

`school_rules` のみ `version` / `generatedAt` / `manifest.json` を持つ。他リソースはバッチ追跡情報なし。

### 5. `null` の意味が場所により異なる

| 箇所 | `null` の意味 |
|------|-------------|
| `meals.breakfast/lunch/dinner` | データなし（休日） |
| `events.grade` | 全学年 / 対象なし / 不明 のいずれか（区別不能） |
| `rules.summary` | LLM 生成失敗 |
| `rules.lastUpdated`（index 内） | 過去未取得 |

### 6. エラー表現が定義されていない

LLM が失敗した日のメニューや、抽出に失敗した条文を **どう表現するか** が仕様化されておらず、現状は「ファイルが出ない」「フィールドが欠ける」「null になる」が混在。

---

## v2 ドラフト

下記方針で `schemas/v2/` を別途用意する計画。実生成は WakayamaServer / アプリ側との合意が前提。

```
{
  "meta": {
    "schemaVersion": "2.0",
    "generatedAt": "2025-05-03T17:00:00Z",
    "source": {"url": "...", "hash": "...", "fetchedAt": "..."}
  },
  "data": { ... }
}
```

- **命名**: 全リソース camelCase に統一
- **日付**: ISO8601 全面採用（`"2025-04-15"` または `"2025-04-15T09:00:00+09:00"`）
- **ID**: 全レコードに `mealId` / `classId` / `eventId` / `ruleId`
- **メタ**: 全リソースに `meta` envelope
- **null**: 「不明」のみ。「対象なし」は明示フラグ（例: `applicableGrades: []` か `appliesToAll: true`）
- **manifest**: 全リソースに `manifest.json`
- **エラー表現**: `extraction.status`, `extraction.confidence` を導入

---

## アプリ側で v1 を読む際の推奨パターン

このリポジトリ側の都合で v1 が不揃いなため、アプリ側にロード/正規化レイヤを置くことを推奨します:

1. ファイル名から年（meals）/ 年度（events）を補完
2. 命名規則をアプリ内モデルに正規化（例: `is_rice` ↔ `isRice`）
3. `null` の意味分岐を明示的にハンドリング
4. `lastUpdated` / `manifest.version` の鮮度チェックは `school_rules` のみ可能
