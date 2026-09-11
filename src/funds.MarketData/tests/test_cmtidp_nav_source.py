# -*- coding: utf-8 -*-
"""CMTIDP 全量最新净值：内部循环最近 3 个交易日，同一基金取最新日期，且仅返回 ETF/LOF。"""
from unittest.mock import AsyncMock, patch

import pytest

from providers.funds.cmtidp import CmtidpSource

DAYS = ["2026-09-11", "2026-09-10", "2026-09-09"]
WHITELIST = {"510300", "159915", "161725"}


def _row(code: str, day: str, unit: str, accum: str = "9.9999") -> dict:
    """构造一条 CMTIDP 净值行；accum="" 模拟累计净值缺失的那条（168701 实测场景）。"""
    return {
        "code": code,
        "valuationDate": day,
        "shareNetValue": unit,
        "totalNetValue": accum,
    }


def _fake_upstream(by_day: dict, page_size: int = 5000):
    """按 (date, offset) 返回分页结果，模拟 CMTIDP getPublicFundJZInfoMore.do。"""
    async def _fetch(*, fund_code, start_date, end_date, start, length):
        rows = by_day.get(start_date, [])[start:start + length]
        return {"iTotalRecords": len(by_day.get(start_date, [])), "aaData": rows}
    return _fetch


@pytest.mark.asyncio
async def test_merges_three_days_and_takes_latest_per_fund():
    """三天合并：同一只基金只保留最新日期的净值。"""
    src = CmtidpSource()
    by_day = {
        "2026-09-11": [_row("159915", "2026-09-11", "1.0000")],
        "2026-09-10": [_row("510300", "2026-09-10", "2.0000"), _row("159915", "2026-09-10", "0.9000")],
        "2026-09-09": [_row("510300", "2026-09-09", "1.8000"), _row("161725", "2026-09-09", "3.0000")],
    }
    with patch("providers.funds.cmtidp._szse_calendar.latest_trading_days", new_callable=AsyncMock, return_value=DAYS), patch(
        "providers.funds.cmtidp.get_exchange_listed_codes", new_callable=AsyncMock, return_value=WHITELIST
    ), patch.object(src, "_fetch_page", side_effect=_fake_upstream(by_day)):
        items = await src.get_latest_all_nav()

    got = {it.code: (it.nav_date, it.unit_nav) for it in items}
    assert got == {
        "159915": ("2026-09-11", 1.0),
        "510300": ("2026-09-10", 2.0),
        "161725": ("2026-09-09", 3.0),
    }


@pytest.mark.asyncio
async def test_filters_out_non_exchange_listed():
    """非 ETF/LOF（不在白名单）必须被过滤掉。"""
    src = CmtidpSource()
    by_day = {
        "2026-09-11": [_row("510300", "2026-09-11", "1.0000"), _row("000001", "2026-09-11", "9.9000")],
    }
    with patch("providers.funds.cmtidp._szse_calendar.latest_trading_days", new_callable=AsyncMock, return_value=DAYS), patch(
        "providers.funds.cmtidp.get_exchange_listed_codes", new_callable=AsyncMock, return_value=WHITELIST
    ), patch.object(src, "_fetch_page", side_effect=_fake_upstream(by_day)):
        items = await src.get_latest_all_nav()

    assert [it.code for it in items] == ["510300"]

def _recording(by_day: dict, seen: list):
    """包装 _fake_upstream，记录每天的分页调用次数。"""
    fake = _fake_upstream(by_day)

    async def _fetch(**kwargs):
        seen.append(kwargs["start_date"])
        return await fake(**kwargs)

    return _fetch


@pytest.mark.asyncio
async def test_paginates_within_a_day():
    """单日多页必须翻到底（iTotalRecords 驱动）。"""
    src = CmtidpSource()
    rows = [_row("510300", "2026-09-11", str(i)) for i in range(5)]
    by_day = {"2026-09-11": rows}
    seen: list = []
    with patch("providers.funds.cmtidp._szse_calendar.latest_trading_days", new_callable=AsyncMock, return_value=DAYS), patch(
        "providers.funds.cmtidp.get_exchange_listed_codes", new_callable=AsyncMock, return_value=WHITELIST
    ), patch("providers.funds.cmtidp._CMTIDP_PAGE_SIZE", 2), patch.object(
        src, "_fetch_page", side_effect=_recording(by_day, seen)
    ):
        items = await src.get_latest_all_nav()

    # 09-11 单日需 3 页（2 + 2 + 1）；其余两天各 1 次空页请求
    assert seen.count("2026-09-11") == 3
    assert len(items) == 1                     # 同一代码去重后只剩 1 条


@pytest.mark.asyncio
async def test_raises_on_page_failure():
    """任一分页请求失败必须抛错，不得静默返回半份数据。"""
    src = CmtidpSource()
    calls = {"n": 0}

    async def _flaky(*, fund_code, start_date, end_date, start, length):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"iTotalRecords": 1, "aaData": [_row("510300", "2026-09-11", "1.0")]}
        return None

    with patch("providers.funds.cmtidp._szse_calendar.latest_trading_days", new_callable=AsyncMock, return_value=DAYS), patch(
        "providers.funds.cmtidp.get_exchange_listed_codes", new_callable=AsyncMock, return_value=WHITELIST
    ), patch.object(src, "_fetch_page", side_effect=_flaky):
        with pytest.raises(RuntimeError, match="不完整"):
            await src.get_latest_all_nav()


@pytest.mark.asyncio
async def test_history_pagination_still_works():
    """单只基金历史净值分页回归（该路径不参与 ETF/LOF 过滤）。"""
    src = CmtidpSource()
    by_day = {"2026-09-11": [_row("510300", "2026-09-11", "1.0")]}
    with patch.object(src, "_fetch_page", side_effect=_fake_upstream(by_day)):
        items = await src.get_fund_nav_history("510300", "2026-09-11", "2026-09-11")
    assert len(items) == 1
    assert items[0].unit_nav == 1.0



@pytest.mark.asyncio
async def test_latest_all_nav_duplicate_prefers_complete_when_complete_first():
    """完整行在前：后一条缺累计净值，必须保留前一条完整行。"""
    src = CmtidpSource()
    by_day = {
        "2026-09-11": [
            _row("168701", "2026-09-11", "0.9731", "0.9731"),
            _row("168701", "2026-09-11", "0.9598", ""),
        ],
    }
    with patch(
        "providers.funds.cmtidp._szse_calendar.latest_trading_days",
        new_callable=AsyncMock,
        return_value=DAYS,
    ), patch(
        "providers.funds.cmtidp.get_exchange_listed_codes", new_callable=AsyncMock, return_value={"168701"}
    ), patch.object(src, "_fetch_page", side_effect=_fake_upstream(by_day)):
        items = await src.get_latest_all_nav()

    assert len(items) == 1
    assert (items[0].unit_nav, items[0].accum_nav) == (0.9731, 0.9731)


@pytest.mark.asyncio
async def test_latest_all_nav_duplicate_prefers_complete_when_complete_last():
    """完整行在后：必须用后一条完整行覆盖前一条缺累计净值的行。"""
    src = CmtidpSource()
    by_day = {
        "2026-09-11": [
            _row("168701", "2026-09-11", "0.9598", ""),
            _row("168701", "2026-09-11", "0.9731", "0.9731"),
        ],
    }
    with patch(
        "providers.funds.cmtidp._szse_calendar.latest_trading_days",
        new_callable=AsyncMock,
        return_value=DAYS,
    ), patch(
        "providers.funds.cmtidp.get_exchange_listed_codes", new_callable=AsyncMock, return_value={"168701"}
    ), patch.object(src, "_fetch_page", side_effect=_fake_upstream(by_day)):
        items = await src.get_latest_all_nav()

    assert len(items) == 1
    assert (items[0].unit_nav, items[0].accum_nav) == (0.9731, 0.9731)


@pytest.mark.asyncio
async def test_history_duplicate_prefers_complete_row():
    """历史净值同日同代码多条：只返回一条，且优先完整行（两种顺序都要成立）。"""
    cases = [
        [_row("168701", "2026-09-11", "0.9731", "0.9731"), _row("168701", "2026-09-11", "0.9598", "")],
        [_row("168701", "2026-09-11", "0.9598", ""), _row("168701", "2026-09-11", "0.9731", "0.9731")],
    ]
    for rows in cases:
        src = CmtidpSource()
        with patch.object(src, "_fetch_page", side_effect=_fake_upstream({"2026-09-11": rows})):
            items = await src.get_fund_nav_history("168701", "2026-09-11", "2026-09-11")
        assert len(items) == 1
        assert (items[0].unit_nav, items[0].accum_nav) == (0.9731, 0.9731)


@pytest.mark.asyncio
async def test_latest_all_nav_applies_polite_delay():
    """3 个交易日 × 多页 → 每次上游请求之间都要有礼貌延时。"""
    src = CmtidpSource()
    by_day = {
        "2026-09-11": [_row("510300", "2026-09-11", str(i)) for i in range(5)],
    }
    with patch(
        "providers.funds.cmtidp._szse_calendar.latest_trading_days",
        new_callable=AsyncMock,
        return_value=DAYS,
    ), patch(
        "providers.funds.cmtidp.get_exchange_listed_codes", new_callable=AsyncMock, return_value=WHITELIST
    ), patch("providers.funds.cmtidp._CMTIDP_PAGE_SIZE", 2), patch.object(
        src, "_fetch_page", side_effect=_fake_upstream(by_day)
    ), patch("providers.funds.cmtidp.polite_delay", new_callable=AsyncMock) as mock_delay:
        await src.get_latest_all_nav()

    # 09-11 需 3 页（第 2、3 页各一次延时）+ 后两个交易日各一次 → 至少 3 次
    assert mock_delay.call_count >= 3


@pytest.mark.asyncio
async def test_history_streams_pages_via_on_page():
    """CMTIDP 历史净值必须逐页回调 on_page（缓存层靠它即时落盘）。"""
    src = CmtidpSource()
    by_day = {
        "2026-09-11": [
            _row("510300", "2026-09-11", "1.0"),
        ],
    }
    calls = []

    async def _collect(page_items, cov_s, cov_e):
        calls.append((len(page_items), cov_s, cov_e))

    with patch("providers.funds.cmtidp._CMTIDP_PAGE_SIZE", 1), patch.object(
        src, "_fetch_page", side_effect=_fake_upstream(by_day)
    ):
        items = await src.get_fund_nav_history(
            "510300", "2026-09-11", "2026-09-11", on_page=_collect
        )

    assert len(items) == 1
    assert len(calls) == 1
    assert calls[0][0] == 1
    assert calls[0][1] == "2026-09-11" and calls[0][2] == "2026-09-11"
