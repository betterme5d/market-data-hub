# -*- coding: utf-8 -*-
"""N5：yfinance 行情缓存 TTL 必须按「交易所本地时间」判定。

旧实现读 `fast_info.market_state`，但 yfinance 的 FastInfo 根本没有该属性
（键列表见 yfinance/scrapers/quote.py 的 `_properties`，且未自定义 `__getattr__`），
`getattr(..., "REGULAR")` 永远返回默认值 → 收盘后与周末仍按 10 分钟打上游。
"""
from datetime import datetime, timezone

import pytest
import pytz

from providers.quotes.yfinance import get_market_ttl

NY = "America/New_York"
HKT = "Asia/Hong_Kong"


class _FastInfo:
    """最小 fast_info 替身：只有 timezone 字段（与真实 FastInfo 的键集合一致）。"""

    def __init__(self, tz):
        self.timezone = tz


def _local(y, m, d, h, mi=0, tz=NY):
    return pytz.timezone(tz).localize(datetime(y, m, d, h, mi))


# 2026-09-11 是周五，09-12 周六，09-13 周日，09-14 周一
@pytest.mark.parametrize(
    "when, expected, why",
    [
        (_local(2026, 9, 8, 10, 0), 600, "周二盘中 → 10 分钟"),
        (_local(2026, 9, 8, 3, 0), 3600, "周二盘前 03:00 → 撑到当日 04:00"),
        (_local(2026, 9, 11, 21, 0), 198000, "周五收盘后 → 撑到周一 04:00（55 小时）"),
        (_local(2026, 9, 12, 12, 0), 144000, "周六 → 撑到周一 04:00（40 小时）"),
        (_local(2026, 9, 13, 23, 0), 18000, "周日 23:00 → 撑到周一 04:00（5 小时）"),
        (_local(2026, 9, 14, 19, 59), 600, "周一盘后时段内 → 10 分钟"),
        (_local(2026, 9, 14, 20, 0), 28800, "周一 20:00 出窗口 → 撑到周二 04:00（8 小时）"),
    ],
)
def test_market_ttl_follows_exchange_local_time(when, expected, why):
    assert get_market_ttl(_FastInfo(NY), now=when) == expected, why


def test_market_ttl_uses_exchange_timezone_not_utc():
    """同一时刻在不同交易所时区下结论不同：港股 15:00 是盘中，纽约 03:00 是盘前。"""
    hk_now = _local(2026, 9, 8, 15, 0, tz=HKT)
    assert get_market_ttl(_FastInfo(HKT), now=hk_now) == 600
    # 同一绝对时刻，按纽约时区看是 03:00（盘前，窗口外）→ 短窗口内应为 3600
    ny_same_moment = hk_now.astimezone(pytz.timezone(NY))
    assert ny_same_moment.hour == 3
    assert get_market_ttl(_FastInfo(NY), now=hk_now) == 3600


def test_market_ttl_never_below_minimum():
    """任何时刻都不得返回小于下限的值（缓存抖动）。"""
    for day in range(7, 15):
        for hour in range(0, 24):
            ttl = get_market_ttl(_FastInfo(NY), now=_local(2026, 9, day, hour))
            assert 600 <= ttl <= 259200, f"2026-09-{day} {hour}:00 → {ttl}"


def test_market_ttl_tolerates_missing_or_invalid_timezone():
    """缺 timezone / 时区名非法都不得抛错（回落 UTC）。"""

    class _NoTz:
        pass

    assert get_market_ttl(_NoTz(), now=datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc)) == 600
    assert get_market_ttl(_FastInfo("Not/AZone"), now=_local(2026, 9, 8, 10, 0)) == 600
    assert get_market_ttl(_FastInfo(None), now=_local(2026, 9, 8, 10, 0)) == 600


def test_market_ttl_accepts_naive_now_as_utc():
    """naive datetime 按 UTC 处理（不抛错）。"""
    ttl = get_market_ttl(_FastInfo("UTC"), now=datetime(2026, 9, 8, 10, 0))
    assert ttl == 600


def test_market_ttl_default_now_does_not_raise():
    """不注入 now 时用当前时间，只要求返回合法值。"""
    ttl = get_market_ttl(_FastInfo(NY))
    assert isinstance(ttl, int)
    assert 600 <= ttl <= 259200


class _FrozenDatetime(datetime):
    """把模块内的 datetime.now() 冻结在「纽约 2026-09-11（周五）21:00」。"""

    _INSTANT = datetime(2026, 9, 12, 1, 0, tzinfo=timezone.utc)  # = 纽约 09-11 21:00 EDT

    @classmethod
    def now(cls, tz=None):
        return cls._INSTANT.astimezone(tz) if tz is not None else cls._INSTANT.replace(tzinfo=None)


def test_market_ttl_uses_wall_clock_when_now_not_injected(monkeypatch):
    """不注入 now 的默认路径也必须是「按交易所本地时间」的判定。

    旧实现在这里恒返回 600：它先读 `fast_info.market_state`（该属性不存在 → 永远 REGULAR），
    于是「收盘后延到下一个交易日」的分支从未被执行过。
    """
    from providers.quotes import yfinance as yf_mod

    monkeypatch.setattr(yf_mod, "datetime", _FrozenDatetime)
    assert get_market_ttl(_FastInfo(NY)) == 198000


def test_fastinfo_has_no_market_state_property():
    """证据固化（2026-09-12 实测 yfinance 1.7.0）：

    `FastInfo` 不含 `market_state`，也没有自定义 `__getattr__`，因此
    `getattr(fast_info, "market_state", "REGULAR")` 永远拿到默认值。
    若将来 yfinance 真的加了这个字段，本用例会失败，提示可以重新评估 TTL 判据。
    """
    from yfinance.scrapers.quote import FastInfo

    assert "__getattr__" not in FastInfo.__dict__, "FastInfo 新增了 __getattr__，需重新评估判据"
    assert not hasattr(FastInfo, "market_state"), "yfinance 新增了 market_state，可考虑改用它"
