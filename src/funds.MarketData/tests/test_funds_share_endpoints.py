import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
from main import app
from core.models import FundShare

client = TestClient(app)

def test_get_fund_shares_validation_error():
    # 非法代码格式（非数字）
    resp = client.get("/api/v1/funds/abcdef/shares?start_date=2026-01-01&end_date=2026-06-30")
    assert resp.status_code == 400

    # 非深市/非沪市代码（如 600000 或 000001 友好报错）
    resp = client.get("/api/v1/funds/600000/shares?start_date=2026-01-01&end_date=2026-06-30")
    assert resp.status_code == 400
    assert "starting with '1' (SZSE) or '5' (SSE)" in resp.json()["detail"]

    # start_date > end_date
    resp = client.get("/api/v1/funds/159901/shares?start_date=2026-06-30&end_date=2026-01-01")
    assert resp.status_code == 400

@patch("providers.funds.shares.provider.FundShareProvider.get_fund_shares")
def test_get_fund_shares_szse_success(mock_get_shares):
    mock_get_shares.return_value = [
        FundShare(code="159901", share_date="2026-06-30", shares=245890.0, raw_shares=2458900000.0, name="深证100")
    ]
    resp = client.get("/api/v1/funds/159901/shares?start_date=2026-06-01&end_date=2026-06-30")
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == "159901"
    assert body["exchange"] == "szse"
    assert body["count"] == 1
    assert body["items"][0]["shares"] == 245890.0

@patch("providers.funds.shares.provider.FundShareProvider.get_fund_shares")
def test_get_fund_shares_sse_success(mock_get_shares):
    mock_get_shares.return_value = [
        FundShare(code="510300", share_date="2026-06-30", shares=28000.0, raw_shares=280000000.0, name="300ETF")
    ]
    resp = client.get("/api/v1/funds/510300/shares?start_date=2026-06-01&end_date=2026-06-30")
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == "510300"
    assert body["exchange"] == "sse"
    assert body["count"] == 1
    assert body["items"][0]["shares"] == 28000.0
