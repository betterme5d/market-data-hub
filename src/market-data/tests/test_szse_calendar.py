# -*- coding: utf-8 -*-
"""深交所交易日历：纯日历口径 vs 「已发布」口径（15:30 截止）。"""
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from providers.exchanges.szse import SzseCalendarSource

# 2026-09 的真实形态：10(四)/11(五) 交易日，12/13 周末，14(一)/15(二) 交易日
SEP = [
    ("2026-09-08", True),
    ("2026-09-09", True),
    ("2026-09-10", True),
    ("2026-09-11", True),
    ("2026-09-12", False),
    ("2026-09-13", False),
    ("2026-09-14", True),
    ("2026-09-15", True),
]


async def _fake_month_days(month: str):
    return SEP if month == "2026-09" else []


@pytest.fixture
def cal():
    c = SzseCalendarSource()
    with patch.object(c, "get_month_days", new_callable=AsyncMock, side_effect=_fake_month_days):
        yield c


@pytest.mark.asyncio
async def test_calendar_only_returns_today_on_trading_day(cal):
    """纯日历口径（默认）：交易日当天就返回当天——东财跨年不变式依赖这个。"""
    assert await cal.latest_trading_day(now=datetime(2026, 9, 11, 10, 0)) == "2026-09-11"
    assert await cal.latest_trading_day(now=datetime(2026, 9, 14, 9, 0)) == "2026-09-14"


@pytest.mark.asyncio
async def test_published_only_before_cutoff_falls_back(cal):
    """已发布口径：15:30 前当日净值未披露 → 参考日回退。"""
    assert await cal.latest_trading_day(
        now=datetime(2026, 9, 11, 10, 0), published_only=True
    ) == "2026-09-10"
    assert await cal.latest_trading_day(
        now=datetime(2026, 9, 11, 15, 29), published_only=True
    ) == "2026-09-10"


@pytest.mark.asyncio
@pytest.mark.parametrize("hh,mm", [(15, 30), (15, 31), (20, 0)])
async def test_published_only_at_or_after_cutoff_returns_today(cal, hh, mm):
    """已发布口径：15:30（含）之后视为当日净值已披露。"""
    assert await cal.latest_trading_day(
        now=datetime(2026, 9, 11, hh, mm), published_only=True
    ) == "2026-09-11"


@pytest.mark.asyncio
async def test_published_only_keeps_friday_through_weekend(cal):
    """周五收盘后一直到周日，都返回 09-11。"""
    assert await cal.latest_trading_day(now=datetime(2026, 9, 11, 16, 0), published_only=True) == "2026-09-11"
    assert await cal.latest_trading_day(now=datetime(2026, 9, 12, 10, 0), published_only=True) == "2026-09-11"
    assert await cal.latest_trading_day(now=datetime(2026, 9, 13, 20, 0), published_only=True) == "2026-09-11"


@pytest.mark.asyncio
async def test_published_only_monday_before_cutoff_still_friday(cal):
    """周一 15:30 前：当日净值未披露，仍返回 09-11。"""
    assert await cal.latest_trading_day(now=datetime(2026, 9, 14, 0, 30), published_only=True) == "2026-09-11"
    assert await cal.latest_trading_day(now=datetime(2026, 9, 14, 15, 0), published_only=True) == "2026-09-11"


@pytest.mark.asyncio
async def test_published_only_monday_after_cutoff_returns_monday(cal):
    assert await cal.latest_trading_day(now=datetime(2026, 9, 14, 15, 30), published_only=True) == "2026-09-14"


@pytest.mark.asyncio
async def test_latest_trading_days_published_only_window(cal):
    """3 日窗口：周一 15:30 前为 [09-11, 09-10, 09-09]；之后前移到 [09-14, 09-11, 09-10]。"""
    assert await cal.latest_trading_days(
        3, now=datetime(2026, 9, 14, 9, 0), published_only=True
    ) == ["2026-09-11", "2026-09-10", "2026-09-09"]
    assert await cal.latest_trading_days(
        3, now=datetime(2026, 9, 14, 16, 0), published_only=True
    ) == ["2026-09-14", "2026-09-11", "2026-09-10"]


@pytest.mark.asyncio
async def test_as_of_override_ignores_cutoff(cal):
    """显式传 as_of 时保持原语义（测试/回溯用）。"""
    assert await cal.latest_trading_day(as_of="2026-09-11", published_only=True) == "2026-09-11"

