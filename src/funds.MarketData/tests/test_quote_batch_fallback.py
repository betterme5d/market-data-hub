# -*- coding: utf-8 -*-
"""N4：批量行情的跨源降级与缺码可观测性。

旧实现每轮都从头取「第一个可用源」，首源拿不到数据时只会反复重试同一个源，
永远不降级到备用源；且 provider 静默漏掉某个代码时既不报错也不计失败，
表现为「响应里少几条、无告警、无降级」。
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.dispatcher import QuoteDispatcher, get_quote_cache_key
from core.models import UnifiedQuote


def _q(symbol: str, name: str, source: str, price: float = 1.0) -> UnifiedQuote:
    return UnifiedQuote(symbol=symbol, name=name, price=price, date="2026-09-11", source=source)


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.setex_args = []

    def get(self, key):
        return self.store.get(key)

    def setex(self, key, ttl, value):
        self.setex_args.append((key, ttl))
        self.store[key] = value

    def exists(self, key):
        return 1 if key in self.store else 0

    def incr(self, key):
        self.store[key] = str(int(self.store.get(key, 0)) + 1)
        return int(self.store[key])

    def expire(self, key, ttl):
        return True

    def delete(self, key):
        self.store.pop(key, None)
        return 1

    def mget(self, keys):
        return [self.store.get(k) for k in keys]

    def ttl(self, key):
        return 60


@pytest.mark.asyncio
async def test_batch_falls_back_to_secondary_source():
    """首源只回了部分代码 → 剩下的必须由备用源补上（而不是重试首源到轮次耗尽）。"""
    calls = []

    primary = MagicMock()
    async def _primary(symbols, with_depth=False):
        calls.append(("sina", tuple(symbols)))
        return {"sh510300": _q("sh510300", "首源", "sina")}
    primary.get_quotes = AsyncMock(side_effect=_primary)

    secondary = MagicMock()
    async def _secondary(symbols, with_depth=False):
        calls.append(("tencent", tuple(symbols)))
        return {"sh159915": _q("sh159915", "备用源", "tencent")}
    secondary.get_quotes = AsyncMock(side_effect=_secondary)

    items = [
        {"symbol": "510300", "allowed_sources": {"sina": "sh510300", "tencent": "sh510300"}},
        {"symbol": "159915", "allowed_sources": {"sina": "sh159915", "tencent": "sh159915"}},
    ]

    with patch("core.dispatcher.redis_client", None), patch.dict(
        "core.dispatcher.PROVIDERS", {"sina": primary, "tencent": secondary}, clear=False
    ):
        res = await QuoteDispatcher.get_quotes_batch(items)

    assert {q.symbol: q.name for q in res} == {"510300": "首源", "159915": "备用源"}
    assert [c[0] for c in calls] == ["sina", "tencent"], "必须降级到备用源"
    # 第二轮只请求仍未拿到的那个代码（已成功的标的不会重复回源）
    assert calls[1][1] == ("sh159915",)


@pytest.mark.asyncio
async def test_batch_falls_back_when_primary_raises():
    """首源整体失败（抛异常）同样要降级到备用源。"""
    calls = []

    primary = MagicMock()
    async def _primary(symbols, with_depth=False):
        calls.append("sina")
        raise RuntimeError("upstream 503")
    primary.get_quotes = AsyncMock(side_effect=_primary)

    secondary = MagicMock()
    async def _secondary(symbols, with_depth=False):
        calls.append("tencent")
        return {"sh510300": _q("sh510300", "备用源", "tencent")}
    secondary.get_quotes = AsyncMock(side_effect=_secondary)

    with patch("core.dispatcher.redis_client", None), patch.dict(
        "core.dispatcher.PROVIDERS", {"sina": primary, "tencent": secondary}, clear=False
    ):
        res = await QuoteDispatcher.get_quotes_batch(
            [{"symbol": "510300", "allowed_sources": {"sina": "sh510300", "tencent": "sh510300"}}]
        )

    assert [q.name for q in res] == ["备用源"]
    assert calls == ["sina", "tencent"]


@pytest.mark.asyncio
async def test_batch_rounds_are_bounded_when_all_sources_fail():
    """所有源都没数据时轮次有上限：不得无限翻轮，也不得抛错（保持原契约）。"""
    calls = []
    providers = {}
    for name in ("sina", "tencent"):
        p = MagicMock()

        async def _empty(symbols, with_depth=False, _name=name):
            calls.append(_name)
            return {}

        p.get_quotes = AsyncMock(side_effect=_empty)
        providers[name] = p

    with patch("core.dispatcher.redis_client", None), patch.dict(
        "core.dispatcher.PROVIDERS", providers, clear=False
    ):
        res = await QuoteDispatcher.get_quotes_batch(
            [{"symbol": "510300", "allowed_sources": {"sina": "sh510300", "tencent": "sh510300"}}]
        )

    assert res == []
    # 3 轮上限：首源 → 备用源 → 回到首源做一次抖动重试
    assert calls == ["sina", "tencent", "sina"]


@pytest.mark.asyncio
async def test_batch_cache_uses_provider_reported_ttl():
    """N5 配套：provider 自报的 TTL（yfinance 收盘后是小时级）必须用于批量写缓存。"""
    fake = _FakeRedis()
    provider = MagicMock(spec=["get_quote"])
    provider.get_quote = AsyncMock(return_value=(_q("AAPL", "上游值", "yfinance", 200.0), 198000))

    with patch("core.dispatcher.redis_client", fake), patch.dict(
        "core.dispatcher.PROVIDERS", {"yfinance": provider}, clear=False
    ):
        res = await QuoteDispatcher.get_quotes_batch(
            [{"symbol": "AAPL", "allowed_sources": {"yfinance": "AAPL"}}]
        )

    assert [q.symbol for q in res] == ["AAPL"]
    assert fake.setex_args, "必须写入缓存"
    assert fake.setex_args[-1][0] == get_quote_cache_key("AAPL")
    assert fake.setex_args[-1][1] == 198000, "批量路径必须沿用 provider 自报 TTL，不能写死 10 秒"
