# -*- coding: utf-8 -*-
"""
时序区间覆盖探测器：负责区间差集计算（缺失切片）与区间合并。
"""
from datetime import date, datetime, timedelta
from typing import List, Sequence, Tuple, Union

DateLike = Union[str, date]
Interval = Tuple[str, str]


class IntervalTracker:
    """区间覆盖计算与合并器。"""

    DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def _to_date(cls, val: DateLike) -> date:
        if isinstance(val, date):
            return val
        return datetime.strptime(str(val).strip(), cls.DATE_FORMAT).date()

    @classmethod
    def _to_str(cls, d: date) -> str:
        return d.strftime(cls.DATE_FORMAT)

    def merge_intervals(
        self,
        intervals: Sequence[Union[Tuple[DateLike, DateLike], Sequence[DateLike]]],
        new_interval: Union[Tuple[DateLike, DateLike], Sequence[DateLike], None] = None,
    ) -> List[Interval]:
        """
        合并多个可能重叠或相邻的闭区间。
        例如: [("2024-01-01", "2024-01-31"), ("2024-02-01", "2024-02-29")] -> [("2024-01-01", "2024-02-29")]
        """
        parsed: List[Tuple[date, date]] = []
        for it in intervals:
            if len(it) >= 2:
                s, e = self._to_date(it[0]), self._to_date(it[1])
                if s <= e:
                    parsed.append((s, e))

        if new_interval is not None and len(new_interval) >= 2:
            s, e = self._to_date(new_interval[0]), self._to_date(new_interval[1])
            if s <= e:
                parsed.append((s, e))

        if not parsed:
            return []

        # 按起始日期排序
        parsed.sort(key=lambda x: (x[0], x[1]))

        merged: List[Tuple[date, date]] = []
        cur_start, cur_end = parsed[0]

        for s, e in parsed[1:]:
            # 如果当前区间与前一个区间相交，或者相邻（即 s <= cur_end + 1 day）
            if s <= cur_end + timedelta(days=1):
                cur_end = max(cur_end, e)
            else:
                merged.append((cur_start, cur_end))
                cur_start, cur_end = s, e

        merged.append((cur_start, cur_end))

        return [(self._to_str(s), self._to_str(e)) for s, e in merged]

    def find_missing_slices(
        self,
        intervals: Sequence[Union[Tuple[DateLike, DateLike], Sequence[DateLike]]],
        req_start: DateLike,
        req_end: DateLike,
    ) -> List[Interval]:
        """
        计算请求区间 [req_start, req_end] 中尚未被 intervals 覆盖的子区间列表。
        返回按时间先后排序的缺失切片列表，若完全覆盖则返回空列表 []。
        """
        r_start = self._to_date(req_start)
        r_end = self._to_date(req_end)

        if r_start > r_end:
            raise ValueError(f"req_start ({req_start}) cannot be after req_end ({req_end})")

        merged = self.merge_intervals(intervals)
        if not merged:
            return [(self._to_str(r_start), self._to_str(r_end))]

        missing: List[Interval] = []
        cursor = r_start

        for it in merged:
            c_start = self._to_date(it[0])
            c_end = self._to_date(it[1])

            # 整个覆盖区间在游标之前，跳过
            if c_end < cursor:
                continue

            # 覆盖区间在请求范围之后，结束循环
            if c_start > r_end:
                break

            # 游标与当前覆盖区间起点之间存在空隙（未覆盖）
            if cursor < c_start:
                slice_end = min(r_end, c_start - timedelta(days=1))
                missing.append((self._to_str(cursor), self._to_str(slice_end)))
                cursor = c_start

            # 覆盖区间将游标向右推移
            if cursor <= c_end:
                cursor = c_end + timedelta(days=1)

            if cursor > r_end:
                break

        # 如果游标仍未到达请求结尾，剩余部分也是缺失切片
        if cursor <= r_end:
            missing.append((self._to_str(cursor), self._to_str(r_end)))

        return missing
