# -*- coding: utf-8 -*-
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
import pytest

from core.models import FundNav
from main import app

client = TestClient(app)


def test_navs_missing_date_params():
    # 缺少 start_date / end_date 报错 422
    resp = client.get("/api/v1/funds/510300/navs")
    assert resp.status_code == 422


def test_navs_invalid_date_order():
    # start_date > end_date 报错 400
    resp = client.get("/api/v1/funds/510300/navs?start_date=2024-05-31&end_date=2024-05-01")
    assert resp.status_code == 400
    assert "cannot be after" in resp.json()["detail"]


def test_navs_invalid_code():
    # 代码非 6 位数字报错 400
    resp = client.get("/api/v1/funds/ABC/navs?start_date=2024-01-01&end_date=2024-01-10")
    assert resp.status_code == 400
    assert "must be 6 digits" in resp.json()["detail"]


@patch("routers.funds.fund_nav_provider.get_fund_nav_history", new_callable=AsyncMock)
def test_navs_success(mock_get_nav):
    mock_get_nav.return_value = [
        FundNav(code="510300", nav_date="2024-01-02", unit_nav=3.5, accum_nav=3.6),
        FundNav(code="510300", nav_date="2024-01-03", unit_nav=3.55, accum_nav=3.65),
    ]

    resp = client.get("/api/v1/funds/510300/navs?start_date=2024-01-01&end_date=2024-01-10")
    assert resp.status_code == 200
    data = resp.json()
    assert data["source"] == "eastmoney"
    assert data["count"] == 2
    assert len(data["items"]) == 2
    assert data["items"][0]["nav_date"] == "2024-01-02"


def test_legacy_history_missing_dates():
    # 旧端点未传日期报错 400
    resp = client.get("/api/fund-nav/history?source=eastmoney&code=510300")
    assert resp.status_code == 400
    assert "Both start_date and end_date are required" in resp.json()["detail"]


@patch("routers.funds.fund_nav_provider.get_fund_nav_history", new_callable=AsyncMock)
def test_legacy_history_success(mock_get_nav):
    mock_get_nav.return_value = [
        FundNav(code="510300", nav_date="2024-01-02", unit_nav=3.5, accum_nav=3.6)
    ]
    resp = client.get("/api/fund-nav/history?source=eastmoney&code=510300&start_date=2024-01-01&end_date=2024-01-10")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["items"][0]["code"] == "510300"
