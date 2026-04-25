#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Scraper-specific exceptions."""


class ScrapeError(RuntimeError):
    """Raised when a scraper could not fetch or parse its source page."""
