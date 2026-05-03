#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Wakayama NCT 用 CA バンドル構築ヘルパー。

和歌山高専の公式サイトは Secom Trust 系の CA チェーン
(``nii-odca4g8rsa`` / ``tlsrsarootca2024``) で署名されており、Python の
``certifi`` 標準バンドルにはこれらの中間 / ルート証明書が含まれていない。
そのため ``requests`` で SSL 検証が ``CERTIFICATE_VERIFY_FAILED`` で失敗する。

このモジュールは初回呼び出し時に:

    1. 必要 CA を Secom Trust の公開 URL から DER でダウンロード
    2. PEM に変換して ``certifi`` バンドルと結合
    3. ``REQUESTS_CA_BUNDLE`` / ``SSL_CERT_FILE`` 環境変数に書き込む

ことで、以降の HTTPS リクエストが正しく検証されるようにする。同等処理は
GitHub Actions ワークフローでも先に実施しているので、CI 上では本関数は
事実上 no-op になる（環境変数を見て早期 return する）。
"""

from __future__ import annotations

import logging
import os
import ssl
import tempfile
import urllib.request
from pathlib import Path

import certifi

logger = logging.getLogger(__name__)

# Wakayama NCT サイトの証明書チェーンを補うため取得する CA 証明書 URL。
# DER 形式で配布されているので、取得後 PEM に変換して既存バンドルに追記する。
WAKAYAMA_CA_URLS = (
    "http://repo1.secomtrust.net/sppca/nii/odca4/nii-odca4g8rsa.cer",
    "http://repo2.secomtrust.net/root/tlsrsa/tlsrsarootca2024.cer",
)

# プロセス内で 1 回だけ構築すれば十分なので、結果をモジュール変数で記憶する。
_configured = False


def configure_wakayama_ca_bundle() -> None:
    """必要 CA を含む独自 PEM バンドルを生成し ``REQUESTS_CA_BUNDLE`` に設定する。

    冪等: 2 回目以降は no-op。``REQUESTS_CA_BUNDLE`` / ``SSL_CERT_FILE`` が
    すでに環境変数に設定されている場合（GitHub Actions 上の事前ステップ等）も
    no-op として扱う。

    例外時はバンドル生成を諦めてログのみ吐く（呼び出し側は直後に通常の
    ``requests.get`` を試みるが、その時点で SSL エラーが出る可能性がある）。
    """
    global _configured
    if _configured:
        return

    if os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE"):
        _configured = True
        return

    try:
        bundle_path = Path(tempfile.gettempdir()) / "wakayama-ca-bundle.pem"
        content = Path(certifi.where()).read_text(encoding="utf-8")
        for url in WAKAYAMA_CA_URLS:
            with urllib.request.urlopen(url, timeout=30) as response:
                content += "\n" + ssl.DER_cert_to_PEM_cert(response.read())
        bundle_path.write_text(content, encoding="utf-8")
        os.environ["REQUESTS_CA_BUNDLE"] = str(bundle_path)
        os.environ["SSL_CERT_FILE"] = str(bundle_path)
        _configured = True
        logger.info("Configured Wakayama NCT CA bundle: %s", bundle_path)
    except Exception as exc:
        logger.warning("Failed to configure Wakayama NCT CA bundle: %s", exc)
