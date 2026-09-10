# -*- coding: utf-8 -*-
"""
K 线根数估算：按日期区间与周期推算单次请求的 count（安全上界）。
对外只暴露 estimate_bar_count 与单页上限常量，所有「区间 → 根数」的换算集中在此文件，
禁止在 Provider 内散落日期推算逻辑。
"""
from datetime import date, datetime, timedelta
from typing import Union

# 单页上限：实测雪球 v5/stock/chart/kline.json 一次可返回 8441 根（SZ000001 全量历史，
# count=-20000 亦然），5000 并非上游上限，而是本项目的风控保守值——降低单次拉取量以规避风控。
MAX_PAGE_SIZE = 5000

DateLike = Union[str, date]


def _to_date(val: DateLike) -> date:
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return datetime.strptime(str(val).strip(), "%Y-%m-%d").date()


def estimate_bar_count(start_date: DateLike, end_date: DateLike, period: str = "day") -> int:
    """
    估算覆盖闭区间 [start_date, end_date] 所需的 K 线根数上界（未按 MAX_PAGE_SIZE 截断）。

    一律取「不可能少」的上界：高估只是多取几根窗口外的旧数据（读取时按日期过滤掉），
    低估则会漏数据。依据：
      - 日K：K 线只落在工作日，区间工作日数即根数上界；
      - 周K：每根周K占用一个自然周，跨度 N 天的区间最多横跨 N//7 + 2 个自然周；
      - 月K：任一自然月至少 28 天，跨度 N 天的区间最多横跨 N//28 + 2 个自然月。

    :return: 根数上界；区间内不含任何工作日（如纯周末）时返回 0。
    """
    s, e = _to_date(start_date), _to_date(end_date)
    if s > e:
        raise ValueError(f"start_date ({s}) must be <= end_date ({e})")
    span_days = (e - s).days + 1

    normalized = (period or "day").strip().lower()
    if normalized == "week":
        return span_days // 7 + 2
    if normalized == "month":
        return span_days // 28 + 2

    # 日K：整周部分固定 5 个工作日，余数天从起始日逐日判定
    full_weeks, remainder = divmod(span_days, 7)
    return full_weeks * 5 + sum(
        1 for offset in range(remainder) if (s + timedelta(days=offset)).weekday() < 5
    )
