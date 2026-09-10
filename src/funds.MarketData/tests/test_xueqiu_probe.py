# -*- coding: utf-8 -*-
"""雪球 K 线探针的语义校验测试"""
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from providers.quotes.xueqiu import XueqiuKlineProbe, XueqiuProvider

TODAY = date.today()


def _bar(days_ago: int, close: float = 3900.0, high: float = 3950.0, low: float = 3850.0) -> dict:
    return {
        "date": (TODAY - timedelta(days=days_ago)).strftime("%Y-%m-%d"),
        "open": 3900.0, "high": high, "low": low, "close": close,
        "volume": 1.0e10, "amount": 1.0e12,
        "change": 1.0, "percent": 0.1, "turnover_rate": 1.0,
    }


@pytest.mark.asyncio
async def test_probe_passes_with_healthy_payload():
    with patch.object(XueqiuProvider, "_fetch_kline_slice", AsyncMock(return_value=[_bar(2), _bar(1)])):
        await XueqiuKlineProbe().probe()  # 不抛异常即视为健康


@pytest.mark.asyncio
async def test_probe_raises_on_empty_items():
    with patch.object(XueqiuProvider, "_fetch_kline_slice", AsyncMock(return_value=[])):
        with pytest.raises(RuntimeError, match="0 根 K 线"):
            await XueqiuKlineProbe().probe()


@pytest.mark.asyncio
async def test_probe_raises_on_missing_field():
    bar = _bar(1)
    bar.pop("amount")
    with patch.object(XueqiuProvider, "_fetch_kline_slice", AsyncMock(return_value=[bar])):
        with pytest.raises(RuntimeError, match="缺字段"):
            await XueqiuKlineProbe().probe()


@pytest.mark.asyncio
async def test_probe_raises_on_invalid_close():
    with patch.object(XueqiuProvider, "_fetch_kline_slice", AsyncMock(return_value=[_bar(1, close=0.0)])):
        with pytest.raises(RuntimeError, match="收盘价非法"):
            await XueqiuKlineProbe().probe()


@pytest.mark.asyncio
async def test_probe_raises_on_high_below_low():
    with patch.object(XueqiuProvider, "_fetch_kline_slice", AsyncMock(return_value=[_bar(1, high=1.0, low=2.0)])):
        with pytest.raises(RuntimeError, match="最高价低于最低价"):
            await XueqiuKlineProbe().probe()


@pytest.mark.asyncio
async def test_probe_raises_when_upstream_stale():
    """接口仍响应但已停更（最新 K 线距今过久）同样应判为不可用"""
    with patch.object(XueqiuProvider, "_fetch_kline_slice", AsyncMock(return_value=[_bar(40)])):
        with pytest.raises(RuntimeError, match="疑似接口停更"):
            await XueqiuKlineProbe().probe()
