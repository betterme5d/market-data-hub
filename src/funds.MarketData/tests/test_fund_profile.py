# -*- coding: utf-8 -*-
"""基金档案取数源(成立/上市日期)解析逻辑的 fixture 单测(不触网)。"""
import os
import sys
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from providers.exchanges.sse import _normalize_listing_date
from providers.exchanges.szse import SzseFundListSource
from providers.funds.fund_profile import (
    EastmoneyExchangeRankSource,
    EastmoneyProfileSource,
    _normalize_date,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load(name: str) -> bytes:
    with open(os.path.join(FIXTURES, name), "rb") as f:
        return f.read()


def test_normalize_date():
    assert _normalize_date("2015-09-25") == "2015-09-25"
    assert _normalize_date("20150925") == "2015-09-25"
    assert _normalize_date("--") is None
    assert _normalize_date("") is None
    assert _normalize_date(None) is None


def test_parse_eastmoney_exchange_rank():
    """东财场内排行 fixture:全量解析 + 成立日期列位正确(排行表仅覆盖 ETF,LOF 走 jbgk 兜底)。"""
    text = _load("eastmoney_exchange_rank.txt").decode("utf-8")
    rows = EastmoneyExchangeRankSource()._parse_text(text)
    assert len(rows) >= 1000
    by_code = {r["fund_code"]: r for r in rows}
    # 实测锚点:科创半导体设备ETF华泰柏瑞 成立 2025-05-26
    assert by_code["588710"]["establish_date"] == "2025-05-26"
    dates = [r["establish_date"] for r in rows if r["establish_date"]]
    # 绝大多数行能解析出 ISO 日期
    assert len(dates) > len(rows) * 0.9
    assert all(len(d) == 10 and d[4] == "-" for d in dates[:50])


def test_parse_szse_fund_listing():
    """SZSE 基金产品列表 fixture(1105/1000_lf 同构):全量解析 + 上市日期实测值锚定。

    数据源已统一到 1105（www.szse.cn 主站，覆盖 ETF/LOF/REITs）。
    fixture 沿用旧 1000_lf 表（含净值列），解析取基金代码/简称/类别/上市日期。
    """
    content = _load("szse_fund_list_scale.xlsx")
    rows = SzseFundListSource()._parse_xlsx(content)
    assert len(rows) >= 800
    by_code = {r["fund_code"]: r for r in rows}
    # 实测锚点:南方积配LOF 上市 2004-12-20、500ETF联接LOF 上市 2009-11-11
    assert by_code["160105"]["list_date"] == "2004-12-20"
    assert by_code["160119"]["list_date"] == "2009-11-11"


def test_normalize_sse_listing_date():
    assert _normalize_listing_date("20150925") == "2015-09-25"
    assert _normalize_listing_date("") is None
    assert _normalize_listing_date(None) is None
    assert _normalize_listing_date("abc") is None


def test_parse_eastmoney_jbgk():
    """F10 概况页 fixture:成立日期从概况表格解析(161724 实测 2015-05-20)。"""
    html = _load("eastmoney_jbgk_161724.html").decode("utf-8")
    profile = EastmoneyProfileSource()._parse_html(html)
    assert profile["establish_date"] == "2015-05-20"


@pytest.mark.asyncio
async def test_get_fund_dates():
    from unittest.mock import AsyncMock, patch
    from providers.funds.fund_profile import get_fund_dates

    with patch(
        "providers.funds.establish_dates.EstablishDateProvider.get_one",
        new=AsyncMock(return_value={"establish_date": "2012-05-04"}),
    ), patch(
        "providers.exchanges.listing_dates.ListingDateProvider.get_one",
        new=AsyncMock(return_value={"list_date": "2012-05-28"}),
    ):
        res = await get_fund_dates("510300")
        assert res == {
            "fund_code": "510300",
            "establish_date": "2012-05-04",
            "list_date": "2012-05-28",
        }
