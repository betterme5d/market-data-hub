# -*- coding: utf-8 -*-
import pytest
from core.timeseries_cache.tracker import IntervalTracker


def test_empty_intervals():
    tracker = IntervalTracker()
    slices = tracker.find_missing_slices([], "2024-01-01", "2024-05-31")
    assert slices == [("2024-01-01", "2024-05-31")]


def test_fully_contained_interval():
    tracker = IntervalTracker()
    intervals = [("2023-01-01", "2024-12-31")]
    # 完全在内部
    slices = tracker.find_missing_slices(intervals, "2024-01-01", "2024-05-31")
    assert slices == []

    # 边界完全一致
    slices_exact = tracker.find_missing_slices(intervals, "2023-01-01", "2024-12-31")
    assert slices_exact == []


def test_right_extension():
    tracker = IntervalTracker()
    intervals = [("2024-01-01", "2024-05-31")]
    slices = tracker.find_missing_slices(intervals, "2024-01-01", "2024-06-15")
    assert slices == [("2024-06-01", "2024-06-15")]


def test_left_extension():
    tracker = IntervalTracker()
    intervals = [("2024-03-01", "2024-05-31")]
    slices = tracker.find_missing_slices(intervals, "2024-01-01", "2024-05-31")
    # 2024 是闰年，2月最后一天是 29 号
    assert slices == [("2024-01-01", "2024-02-29")]


def test_both_sides_extension():
    tracker = IntervalTracker()
    intervals = [("2024-03-01", "2024-05-31")]
    slices = tracker.find_missing_slices(intervals, "2024-01-01", "2024-06-30")
    assert slices == [
        ("2024-01-01", "2024-02-29"),
        ("2024-06-01", "2024-06-30")
    ]


def test_middle_gap_between_disjoint_intervals():
    tracker = IntervalTracker()
    intervals = [
        ("2024-01-01", "2024-01-31"),
        ("2024-03-01", "2024-03-31")
    ]
    # 请求跨越了 1 月和 3 月，缺失 2 月
    slices = tracker.find_missing_slices(intervals, "2024-01-15", "2024-03-15")
    assert slices == [("2024-02-01", "2024-02-29")]


def test_disjoint_request():
    tracker = IntervalTracker()
    intervals = [("2024-01-01", "2024-01-31")]
    # 请求完全在右侧不相交
    slices = tracker.find_missing_slices(intervals, "2024-05-01", "2024-05-31")
    assert slices == [("2024-05-01", "2024-05-31")]


def test_merge_intervals():
    tracker = IntervalTracker()
    # 包含重叠
    raw = [("2024-01-01", "2024-03-31"), ("2024-02-01", "2024-05-31")]
    merged = tracker.merge_intervals(raw)
    assert merged == [("2024-01-01", "2024-05-31")]

    # 包含相邻日期 (2024-01-31 与 2024-02-01 相邻，应当合并)
    adjacent = [("2024-01-01", "2024-01-31"), ("2024-02-01", "2024-02-29")]
    merged_adj = tracker.merge_intervals(adjacent)
    assert merged_adj == [("2024-01-01", "2024-02-29")]

    # 包含新区间合并
    merged_with_new = tracker.merge_intervals([("2024-01-01", "2024-01-31")], ("2024-02-01", "2024-05-31"))
    assert merged_with_new == [("2024-01-01", "2024-05-31")]
