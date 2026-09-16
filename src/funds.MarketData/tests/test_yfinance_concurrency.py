# -*- coding: utf-8 -*-
"""yfinance 共享 `requests.Session` 的并发安全性（默认跳过，需 `-m integration`）。

背景：台账 D6 的「未验证项」——`providers/quotes/yfinance.py` 用的是**模块级单例**
`_yf_session`，所有 `Ticker` 共享它；D6 之后取数又改成 `asyncio.to_thread`，
于是同一个 session 会被多个工作线程同时使用。而 `requests.Session` 本身不是线程安全的
（会话级 headers / cookies 的读写没有锁）。

本用例做两件事：
1. 多个不同标的并发取数：全部成功且字段合理（无互相污染）；
2. 同一标的 N 路并发：结果必须完全一致（出现多个取值即说明共享 session 串了）。

注意：这是**弱证据**——一次跑通不能证明没有竞态，只能证明当前代码路径在这个并发度下
没有可观测的串扰。真要保险就给 yfinance 加并发闸门（台账里的建议，另行讨论）。
"""

import asyncio

import pytest

pytestmark = pytest.mark.integration  # 需要真实外部网络，默认跳过

SYMBOLS = ["AAPL", "MSFT", "NVDA", "GOOGL"]


@pytest.mark.asyncio
async def test_concurrent_distinct_symbols_do_not_corrupt_each_other():
    """多个不同标的在 to_thread 下并发取数，结果必须各自正确。"""
    from providers.quotes.yfinance import YFinanceProvider

    provider = YFinanceProvider()
    results = await asyncio.gather(
        *[provider.get_quote(s) for s in SYMBOLS], return_exceptions=True
    )

    errors = []
    for sym, res in zip(SYMBOLS, results):
        if isinstance(res, Exception):
            errors.append(f"{sym}: {type(res).__name__}: {res}")
            continue
        quote, ttl = res
        if not quote.price or quote.price <= 0:
            errors.append(f"{sym}: 价格异常 {quote.price}")
        if quote.name != sym:
            errors.append(f"{sym}: 返回体被串成 {quote.name}（共享 session 污染）")

    assert not errors, "并发取数出现异常/串扰：\n  " + "\n  ".join(errors)


@pytest.mark.asyncio
async def test_concurrent_same_symbol_returns_identical_results():
    """同一标的多路并发：结果取值必须唯一（不唯一即共享 session 出现竞态）。"""
    from providers.quotes.yfinance import YFinanceProvider

    provider = YFinanceProvider()
    results = await asyncio.gather(
        *[provider.get_quote("AAPL") for _ in range(4)], return_exceptions=True
    )

    errs = [r for r in results if isinstance(r, Exception)]
    assert not errs, f"并发取数抛异常：{type(errs[0]).__name__}: {errs[0]}"

    values = {(round(q.price, 4), q.currency, q.date) for q, _ in results}
    assert len(values) == 1, f"同标的并发返回了 {len(values)} 种取值，疑似共享 session 竞态：{values}"
