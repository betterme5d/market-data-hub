# -*- coding: utf-8 -*-
"""行情接口 refresh=true（方案A）：跳过缓存读取，但仍把上游结果回写缓存。"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.dispatcher import QuoteDispatcher, get_quote_cache_key
from core.models import UnifiedQuote


class _FakeRedis:
    """最小 Redis 替身：只实现 dispatcher 用得到的方法，并记录读写次数。"""

    def __init__(self, initial=None):
        self.store = dict(initial or {})
        self.get_calls = 0
        self.setex_calls = 0
        self.setex_args = []  # [(key, ttl), ...]

    def get(self, key):
        self.get_calls += 1
        return self.store.get(key)

    def setex(self, key, ttl, value):
        self.setex_calls += 1
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
async def test_quote_refresh_bypasses_cache_read_but_still_writes():
    """refresh=True：不读缓存直接打上游；refresh=False：命中缓存不调上游。"""
    cached = UnifiedQuote(symbol="510300", name="缓存值", price=1.0, date="2026-09-11", source="sina")
    fresh = UnifiedQuote(symbol="510300", name="上游值", price=2.0, date="2026-09-11", source="sina")
    fake = _FakeRedis({get_quote_cache_key("510300"): cached.model_dump_json()})

    provider = MagicMock()
    provider.get_quote = AsyncMock(return_value=(fresh, 10))

    with patch("core.dispatcher.redis_client", fake), patch.dict(
        "core.dispatcher.PROVIDERS", {"sina": provider}, clear=False
    ), patch.dict("core.dispatcher.SEMAPHORES", {"sina": asyncio.Semaphore(1)}, clear=False):
        hit = await QuoteDispatcher.get_quote_with_fallback("510300", {"sina": "sh510300"})
        assert hit.name == "缓存值"
        assert provider.get_quote.call_count == 0

        got = await QuoteDispatcher.get_quote_with_fallback(
            "510300", {"sina": "sh510300"}, refresh=True
        )
        assert got.name == "上游值"
        assert provider.get_quote.call_count == 1

    assert fake.get_calls == 1, "refresh=True 不应再读缓存"
    assert fake.setex_calls >= 1, "方案A：刷新结果要回写缓存"


@pytest.mark.asyncio
async def test_single_cache_hit_rejected_when_source_not_allowed():
    """显式指定 source 时，不得被其它源的缓存命中绕过。"""
    cached = UnifiedQuote(
        symbol="510300", name="sina缓存", price=1.0, date="2026-09-11", source="sina"
    )
    fresh = UnifiedQuote(
        symbol="510300", name="tencent上游", price=2.0, date="2026-09-11", source="tencent"
    )
    fake = _FakeRedis({get_quote_cache_key("510300"): cached.model_dump_json()})
    provider = MagicMock()
    provider.get_quote = AsyncMock(return_value=(fresh, 10))

    with patch("core.dispatcher.redis_client", fake), patch.dict(
        "core.dispatcher.PROVIDERS", {"tencent": provider}, clear=False
    ), patch.dict("core.dispatcher.SEMAPHORES", {"tencent": asyncio.Semaphore(1)}, clear=False):
        got = await QuoteDispatcher.get_quote_with_fallback("510300", {"tencent": "sh510300"})

    assert got.name == "tencent上游"
    assert provider.get_quote.call_count == 1


@pytest.mark.asyncio
async def test_batch_cache_write_uses_source_ttl():
    """批量写缓存必须按源取 TTL：yfinance 不能沿用写死的 10 秒。"""
    fake = _FakeRedis()
    quote = UnifiedQuote(
        symbol="AAPL", name="上游值", price=200.0, date="2026-09-11", source="yfinance"
    )
    provider = MagicMock()
    provider.get_quotes = AsyncMock(return_value={"AAPL": quote})

    with patch("core.dispatcher.redis_client", fake), patch.dict(
        "core.dispatcher.PROVIDERS", {"yfinance": provider}, clear=False
    ), patch.dict("core.dispatcher.SEMAPHORES", {"yfinance": asyncio.Semaphore(1)}, clear=False):
        await QuoteDispatcher.get_quotes_batch([{"symbol": "AAPL", "allowed_sources": {"yfinance": "AAPL"}}])

    assert fake.setex_args, "必须写入缓存"
    assert fake.setex_args[-1][1] >= 600, fake.setex_args
