# -*- coding: utf-8 -*-
"""Task 121: KLineBar + KLineResponse 强类型模型单元测试"""
import math
import pytest
from core.models import KLineBar, KLineResponse


def test_kline_bar_valid():
    bar = KLineBar(
        date="2026-09-09",
        open=6.12, high=6.25, low=6.08, close=6.20,
        volume=125000.0, amount=7650000.0,
        change=0.08, percent=1.31, turnover_rate=0.45,
    )
    assert bar.date == "2026-09-09"
    assert bar.close == 6.20
    assert bar.percent == 1.31


def test_kline_bar_optional_fields_default_none():
    bar = KLineBar(date="2026-01-01", open=1.0, high=1.1, low=0.9, close=1.0, volume=100.0, amount=500.0)
    assert bar.change is None
    assert bar.percent is None
    assert bar.turnover_rate is None


def test_kline_bar_sanitize_nan():
    """NaN 浮点值应被净化为 None，不抛异常"""
    bar = KLineBar(date="2026-01-01", open=1.0, high=1.1, low=0.9, close=1.0,
                   volume=100.0, amount=500.0, percent=float("nan"))
    assert bar.percent is None


def test_kline_response_defaults():
    resp = KLineResponse(code="002092.SZ", source="xueqiu", count=0)
    assert resp.period == "day"
    assert resp.adjust == "qfq"
    assert resp.items == []


def test_kline_response_with_items():
    bar = KLineBar(date="2026-09-09", open=6.0, high=6.1, low=5.9, close=6.05, volume=1000.0, amount=6000.0)
    resp = KLineResponse(code="510300.SH", source="xueqiu", period="day", adjust="qfq", count=1, items=[bar])
    assert resp.count == 1
    assert resp.items[0].date == "2026-09-09"
    assert resp.items[0].close == 6.05


def test_kline_response_source_and_adjust_echoed():
    """必须回显数据源与复权口径，确保调用方感知一致性"""
    resp = KLineResponse(code="600519.SH", source="tencent", adjust="hfq", count=0)
    assert resp.source == "tencent"
    assert resp.adjust == "hfq"
