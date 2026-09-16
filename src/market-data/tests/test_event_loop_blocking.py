# -*- coding: utf-8 -*-
"""事件循环阻塞回归用例（D6 yfinance / D7 akshare）。

这两个 provider 声明成 async，内部却是同步网络调用（yfinance 的 Ticker/fast_info、
akshare 的 pandas 抓取）。跑在事件循环线程上时，一次上游调用就会把整个服务的所有请求
卡住几秒；本用例用"并发心跳计数"作为可观测量：慢调用期间事件循环必须仍在推进。
"""
import asyncio
import threading
import time
from unittest.mock import patch

import pytest


class _Heartbeat:
    """每 10ms 自增一次；慢调用期间计数不动就说明事件循环被阻塞。"""

    def __init__(self) -> None:
        self.ticks = 0
        self._task = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(0.01)
            self.ticks += 1

    async def __aenter__(self):
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, *exc):
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        return False


class _SlowFastInfo:
    """模拟 yfinance fast_info：每次属性访问都是一次阻塞网络往返。"""

    _VALUES = {
        "last_price": 100.0,
        "regular_market_previous_close": 99.0,
        "open": 99.5,
        "day_high": 101.0,
        "day_low": 98.0,
        "last_volume": 1000,
        "currency": "USD",
        "exchange": "NYSE",
        "timestamp": 1700000000,
        "market_state": "CLOSED",
        "timezone": "America/New_York",
    }

    def __init__(self, delay: float) -> None:
        self._delay = delay

    def __getattr__(self, item):
        time.sleep(self._delay)
        return self._VALUES.get(item)


class _FakeTicker:
    """yf.Ticker 替身：记录调用线程，网络访问用 sleep 模拟。"""

    def __init__(self, delay: float, hist_thread: list) -> None:
        self._delay = delay
        self._hist_thread = hist_thread
        self.fast_info = _SlowFastInfo(delay)

    def history(self, **kwargs):
        self._hist_thread.append(threading.current_thread())
        time.sleep(self._delay)
        import pandas as pd

        return pd.DataFrame(
            {"Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0], "Adj Close": [1.0], "Volume": [1]},
            index=[pd.Timestamp("2024-01-02")],
        )


@pytest.mark.asyncio
async def test_yfinance_get_quote_does_not_block_event_loop():
    """D6：yfinance 报价取数期间，事件循环必须仍能调度其它协程。"""
    from providers.quotes import yfinance as yf_mod

    delay = 0.02
    with patch.object(
        yf_mod.yf, "Ticker", side_effect=lambda symbol, session=None: _FakeTicker(delay, [])
    ):
        async with _Heartbeat() as hb:
            quote, ttl = await yf_mod.YFinanceProvider().get_quote("AAPL")
            ticks_during_call = hb.ticks

    assert quote.symbol == "AAPL"
    assert quote.price == 100.0
    assert ttl > 0
    assert ticks_during_call >= 3, (
        f"慢调用期间事件循环只推进了 {ticks_during_call} 次心跳：yfinance 仍在阻塞事件循环"
    )


@pytest.mark.asyncio
async def test_yfinance_get_history_runs_network_call_off_loop_thread():
    """D6：ticker.history 的网络调用必须搬到工作线程（不能在事件循环线程里跑）。"""
    from providers.quotes import yfinance as yf_mod

    hist_threads: list = []
    with patch.object(
        yf_mod.yf, "Ticker", side_effect=lambda symbol, session=None: _FakeTicker(0.01, hist_threads)
    ):
        with patch("core.cache.get_cached_quote", return_value=None):
            with patch("core.cache.get_cached_anchor", return_value={"adj_close": 1.0, "close": 1.0}):
                await yf_mod.YFinanceProvider().get_history(
                    "AAPL", period="1mo", interval="1d", start=None, end=None, adj="none"
                )

    assert hist_threads, "ticker.history 未被调用"
    assert all(t is not threading.main_thread() for t in hist_threads), (
        "ticker.history 跑在事件循环线程上（阻塞事件循环）"
    )


@pytest.mark.asyncio
async def test_yfinance_get_info_runs_off_loop_thread():
    """D6：.info 同样是同步网络访问，必须搬到工作线程。"""
    from providers.quotes import yfinance as yf_mod

    threads: list = []

    class _InfoTicker:
        @property
        def info(self):
            threads.append(threading.current_thread())
            return {"shortName": "Apple"}

    with patch.object(yf_mod.yf, "Ticker", side_effect=lambda symbol, session=None: _InfoTicker()):
        info = await yf_mod.YFinanceProvider().get_info("AAPL")

    assert info == {"shortName": "Apple"}
    assert threads and all(t is not threading.main_thread() for t in threads)


@pytest.mark.asyncio
async def test_router_fund_info_does_not_block_event_loop(monkeypatch):
    """D7：基金基本信息走 akshare+pandas，必须在线程里执行，不能卡住事件循环。"""
    from routers import funds as funds_router

    def slow_get_fund_info(symbol: str):
        time.sleep(0.15)
        return {"fund_code": symbol, "fund_name": "测试基金"}

    monkeypatch.setattr(funds_router.eastmoney_provider, "get_fund_info", slow_get_fund_info)

    async with _Heartbeat() as hb:
        result = await funds_router.get_fund_info(symbol="510300")
        ticks_during_call = hb.ticks

    assert result["fund_code"] == "510300"
    assert ticks_during_call >= 5, (
        f"慢调用期间事件循环只推进了 {ticks_during_call} 次心跳：akshare 调用仍在阻塞事件循环"
    )
