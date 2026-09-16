import pytest
from core.models import FundShare, FundShareResponse

def test_fund_share_model_validation():
    # 测试基础数据与类型清洗
    share = FundShare(
        code="159901",
        share_date="2026-06-30",
        shares=123.4567,
        raw_shares=1234567.0,
        name="易方达深证100ETF"
    )
    assert share.code == "159901"
    assert share.share_date == "2026-06-30"
    assert share.shares == 123.4567
    assert share.raw_shares == 1234567.0
    assert share.name == "易方达深证100ETF"

def test_fund_share_model_sanitization():
    # 测试 NaN 与空值容错
    share = FundShare(
        code=" 159901 ",
        share_date="2026-06-30",
        shares=float("nan"),
        raw_shares=None,
        name=None
    )
    assert share.code == "159901"
    assert share.shares is None
    assert share.raw_shares is None
    assert share.name is None

def test_fund_share_response():
    resp = FundShareResponse(
        code="159901",
        exchange="szse",
        count=1,
        items=[
            FundShare(code="159901", share_date="2026-06-30", shares=100.0)
        ]
    )
    assert resp.code == "159901"
    assert resp.exchange == "szse"
    assert resp.count == 1
    assert len(resp.items) == 1
