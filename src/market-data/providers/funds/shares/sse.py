# -*- coding: utf-8 -*-
"""兼容层：上交所份额数据源已迁移至 providers.exchanges.shares.sse"""
from providers.exchanges.shares.sse import (
    SseShareSource,
    SseShareProbe,
    SSE_COMMON_QUERY_URL,
    SSE_REFERER,
)

__all__ = ["SseShareSource", "SseShareProbe", "SSE_COMMON_QUERY_URL", "SSE_REFERER"]
