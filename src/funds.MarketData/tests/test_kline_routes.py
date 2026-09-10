# -*- coding: utf-8 -*-
"""Task 124: K 线标准接口路由单元测试"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
from main import app
from core.models import KLineBar, KLineResponse

client = TestClient(app)

SAMPLE = KLineResponse(
    code="002092.SZ", source="xueqiu", period="day", adjust="qfq", count=1,
    items=[KLineBar(date="2026-01-02", open=6.0, high=6.1, low=5.9, close=6.05, volume=1000.0, amount=6000.0)]
)


@patch("routers.kline.kline_provider")
def test_standard_endpoint_ok(mock_p):
    mock_p.get_kline = AsyncMock(return_value=SAMPLE)
    r = client.get("/api/v1/securities/002092.SZ/kline?start_date=2026-01-01&end_date=2026-01-31")
    assert r.status_code == 200
    b = r.json()
    assert b["code"] == "002092.SZ"
    assert b["source"] == "xueqiu"
    assert b["adjust"] == "qfq"
    assert b["count"] == 1
    assert b["items"][0]["date"] == "2026-01-02"


@patch("routers.kline.kline_provider")
def test_explicit_source_and_adjust(mock_p):
    mock_p.get_kline = AsyncMock(return_value=SAMPLE)
    r = client.get(
        "/api/v1/securities/510300.SH/kline?start_date=2026-01-01&end_date=2026-01-31&source=xueqiu&adjust=hfq"
    )
    assert r.status_code == 200


def test_missing_start_date_returns_422():
    r = client.get("/api/v1/securities/002092.SZ/kline?end_date=2026-01-31")
    assert r.status_code == 422


def test_date_order_error_returns_400():
    r = client.get(
        "/api/v1/securities/002092.SZ/kline?start_date=2026-12-31&end_date=2026-01-01"
    )
    assert r.status_code == 400


@patch("routers.kline.kline_provider")
def test_invalid_source_returns_400(mock_p):
    mock_p.get_kline = AsyncMock(side_effect=ValueError("unsupported source"))
    r = client.get(
        "/api/v1/securities/002092.SZ/kline?start_date=2026-01-01&end_date=2026-01-31&source=badSrc"
    )
    assert r.status_code == 400


@patch("routers.kline.kline_provider")
def test_xueqiu_raw_format_accepted(mock_p):
    """C# 侧可能传来 SZ002092 格式，必须被接受"""
    mock_p.get_kline = AsyncMock(return_value=SAMPLE)
    r = client.get("/api/v1/securities/SZ002092/kline?start_date=2026-01-01&end_date=2026-01-31")
    assert r.status_code == 200


@patch("routers.kline.kline_provider")
def test_six_digit_code_accepted(mock_p):
    """纯6位代码 002092 也能被接受（自动归一化为 002092.SZ）"""
    mock_p.get_kline = AsyncMock(return_value=SAMPLE)
    r = client.get("/api/v1/securities/002092/kline?start_date=2026-01-01&end_date=2026-01-31")
    assert r.status_code == 200
