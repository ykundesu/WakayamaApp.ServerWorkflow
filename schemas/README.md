# schemas/

WakayamaApp.ServerWorkflow が **WakayamaServer リポジトリに書き出す静的ファイルの JSON Schema 定義集** です。これらのファイルがアプリ側にとっての実質的な API になります。

## ディレクトリ構成

```
schemas/
└── v1/
    ├── meals.schema.json              v1/meals/{YYYY-MM-DD}.json の形
    ├── classes.schema.json            v1/classes/{cohort}/{grade}_{value}.json の形
    ├── dormitory_events.schema.json   v1/dormitory/events/{academic_year}.json の形
    ├── rules.schema.json              （LLM 抽出時の最小ペイロード）
    ├── rule_detail.schema.json        v1/school-rules/rules/{ruleId}.json の形
    ├── rules_index.schema.json        v1/school-rules/index.json の形
    ├── rules_manifest.schema.json     v1/school-rules/manifest.json の形
    └── sources_list.schema.json       v1/sources/list/{target}.json の形
```

## このフォルダの位置付け

- これらのスキーマは **正本（canonical）として運用するもの** ですが、当面は `processors/*.py` 内の `*_SCHEMA` Python dict が LLM 呼び出し時の実体を担っています。Python 側からの参照に切り替えるリファクタは、生成 JSON の差分検証込みで段階的に進める予定です（メモ: `processors/_schema_loader.py` を新設して JSON ローダに統一する）。
- スキーマ定義の意図やフィールドの意味、null の解釈は **各 `.schema.json` の `description` に日本語で詳述** してあります。仕様書として読めるよう、命名の不整合や曖昧さもコメントしています。

## 既知の不整合（v1 仕様）

`docs/output_schema.md` の "既知の課題" セクションも参照。

| 観点 | meals | classes | dormitory_events | school_rules |
|------|-------|---------|------------------|--------------|
| 命名 | snake/camel 混在 | snake_case | snake_case | camelCase |
| 日付 | `MM/DD` (年欠落) | 時刻のみ | `MM/DD` (年欠落) | ISO8601 |
| ID | 無し（ファイル名） | 無し | 無し | `id` あり |
| メタ | 無し | 無し | `academic_year` のみ | `version` / `manifest.json` あり |

## v2 計画（予定）

- すべて camelCase に統一
- 日付は ISO8601 全面採用
- 全リソースに `meta` envelope（`schemaVersion`, `generatedAt`, `source`）
- 全レコードに明示 ID
- 全リソースに `manifest.json`

v2 はモバイル / Web アプリ側の対応とセットでリリースするため、本リポジトリ単独でフィールドを変更しません。
