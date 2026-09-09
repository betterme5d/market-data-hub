# -*- coding: utf-8 -*-
"""兼容层：深交所份额数据源已迁移至 providers.exchanges.shares.szse"""
from providers.exchanges.shares.szse import (
    SzseShareSource,
    SzseShareProbe,
    split_date_range,
    SZSE_SHARE_REPORT_URL,
    SZSE_FUNDS_REFERER,
)

__all__ = [
    "SzseShareSource",
    "SzseShareProbe",
    "split_date_range",
    "SZSE_SHARE_REPORT_URL",
    "SZSE_FUNDS_REFERER",
]
