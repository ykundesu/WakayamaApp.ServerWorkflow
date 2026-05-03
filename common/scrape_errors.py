#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""スクレイプ層の例外型定義。

スクレイプ失敗（HTTP エラー / 想定 HTML 構造ではない / 必須セレクタが見つから
ない 等）を ``ScrapeError`` で統一表現することで、上位レイヤ（``processors/``
や ``main.py``）が「ネットワーク・HTML 構造起因の失敗」と「LLM / 内部処理の
失敗」を区別できるようにする。

注意: 現状この例外型は **未適用** の scraper が多く、``Exception`` を直接
raise しているケースがある。改善ロードマップ（docs/architecture.md）で
順次 ``ScrapeError`` への統一を予定。
"""


class ScrapeError(RuntimeError):
    """スクレイパが取得・パースに失敗したときに投げる例外。

    上位レイヤはこの例外型をキャッチすることで、ネットワーク / HTML 構造の
    変化による回復可能な失敗を、LLM 失敗や内部バグから区別できる。
    """
