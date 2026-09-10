# -*- coding: utf-8 -*-
"""区间 → K 线根数估算（安全上界语义）单元测试"""
from datetime import date

import pytest

from core.bar_estimator import MAX_PAGE_SIZE, estimate_bar_count


def test_day_single_week():
    """2026-01-05(周一) ~ 2026-01-09(周五) -> 5 个工作日"""
    assert estimate_bar_count("2026-01-05", "2026-01-09") == 5


def test_day_inclusive_endpoints():
    """单日窗口（周日）-> 0；单日窗口（周一）-> 1"""
    assert estimate_bar_count("2026-09-06", "2026-09-06") == 0
    assert estimate_bar_count("2026-01-05", "2026-01-05") == 1


def test_day_spanning_weekends():
    """跨两个完整周的 14 天窗口 -> 10 个工作日"""
    assert estimate_bar_count("2026-01-05", "2026-01-18") == 10


def test_day_weekend_only_returns_zero():
    """纯周末窗口（周六~周日）-> 0，调用方可据此跳过网络请求"""
    assert estimate_bar_count("2026-09-05", "2026-09-06") == 0


def test_day_holiday_week_is_upper_bound():
    """国庆整周无交易日，但工作日数 6 仍是根数上界（宁多勿少）"""
    est = estimate_bar_count("2026-10-01", "2026-10-08")
    assert est == 6
    assert est >= 0  # 实际交易日为 0，上界成立


def test_day_long_window_exceeds_page_size():
    """2005-02-23 ~ 2026-09-09 的工作日数超过单页上限，调用方需分页"""
    est = estimate_bar_count("2005-02-23", "2026-09-09")
    assert est == 5621
    assert est > MAX_PAGE_SIZE


def test_week_period_uses_calendar_weeks():
    """周K：跨度 91 天 -> 91//7 + 2 = 15 个自然周上界"""
    assert estimate_bar_count("2026-01-01", "2026-04-01", "week") == 15


def test_month_period_uses_calendar_months():
    """月K：跨度 91 天 -> 91//28 + 2 = 5 个自然月上界"""
    assert estimate_bar_count("2026-01-01", "2026-04-01", "month") == 5


def test_period_case_and_blank_tolerated():
    """周期入参大小写、空白与空值均容错，空值按日K处理"""
    assert estimate_bar_count("2026-01-01", "2026-04-01", " WEEK ") == 15
    assert estimate_bar_count("2026-01-01", "2026-04-01", None) == 65


def test_accepts_date_objects():
    """date 对象与字符串等价"""
    assert estimate_bar_count(date(2026, 1, 5), date(2026, 1, 9)) == 5


def test_inverted_range_raises():
    with pytest.raises(ValueError, match="start_date"):
        estimate_bar_count("2026-01-09", "2026-01-05")
