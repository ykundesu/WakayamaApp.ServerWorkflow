#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Certificate bundle helpers for Wakayama NCT scraping."""

from __future__ import annotations

import logging
import os
import ssl
import tempfile
import urllib.request
from pathlib import Path

import certifi

logger = logging.getLogger(__name__)

WAKAYAMA_CA_URLS = (
    "http://repo1.secomtrust.net/sppca/nii/odca4/nii-odca4g8rsa.cer",
    "http://repo2.secomtrust.net/root/tlsrsa/tlsrsarootca2024.cer",
)

_configured = False


def configure_wakayama_ca_bundle() -> None:
    """Add Wakayama NCT's currently missing CA chain to requests' CA bundle."""
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
