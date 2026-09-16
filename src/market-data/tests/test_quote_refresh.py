# -*- coding: utf-8 -*-
"""行情接口 refresh=true（方案A）：跳过缓存读取，但仍把上游结果回写缓存。

缓存已从 Valkey 改为进程内实现（core/state_store.py），这里直接用真实存储断言，
不再用 FakeRedis 替身——断言的 TTL/读写次数就是生产路径的行为。
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.cache import quote_cache
from core.dispatcher import QuoteDispatcher, get_quote_cache_key
from core.models import UnifiedQuote


@pytest.mark.asyncio
async def test_quote_refresh_bypasses_cache_read_but_still_writes():
    """refresh=True：不读缓存直接打上游；refresh=False：命中缓存不调上游。"""
    cached = UnifiedQuote(symbol="510300", name="缓存值", price=1.0, date="2026-09-11", source="sina")
    fresh = UnifiedQuote(symbol="510300", name="上游值", price=2.0, date="2026-09-11", source="sina")
    quote_cache.setex(get_quote_cache_key("510300"), 10, cached.model_dump_json())
    reads_before = quote_cache.stats()["reads"]

    provider = MagicMock()
    provider.get_quote = AsyncMock(return_value=(fresh, 10))

    with patch.dict("core.dispatcher.PROVIDERS", {"sina": provider}, clear=False), \
         patch.dict("core.dispatcher.SEMAPHORES", {"sina": asyncio.Semaphore(1)}, clear=False):
        hit = await QuoteDispatcher.get_quote_with_fallback("510300", {"sina": "sh510300"})
        assert hit.name == "缓存值"
        assert provider.get_quote.call_count == 0

        got = await QuoteDispatcher.get_quote_with_fallback(
            "510300", {"sina": "sh510300"}, refresh=True
        )
        assert got.name == "上游值"
        assert provider.get_quote.call_count == 1

    assert quote_cache.stats()["reads"] - reads_before == 1, "refresh=True 不应再读缓存"
    written = UnifiedQuote.model_validate_json(quote_cache.get(get_quote_cache_key("510300")))
    assert written.name == "上游值", "方案A：刷新结果要回写缓存"


@pytest.mark.asyncio
async def test_single_cache_hit_rejected_when_source_not_allowed():
    """显式指定 source 时，不得被其它源的缓存命中绕过。"""
    cached = UnifiedQuote(
        symbol="510300", name="sina缓存", price=1.0, date="2026-09-11", source="sina"
    )
    fresh = UnifiedQuote(
        symbol="510300", name="tencent上游", price=2.0, date="2026-09-11", source="tencent"
    )
    quote_cache.setex(get_quote_cache_key("510300"), 10, cached.model_dump_json())
    provider = MagicMock()
    provider.get_quote = AsyncMock(return_value=(fresh, 10))

    with patch.dict("core.dispatcher.PROVIDERS", {"tencent": provider}, clear=False), \
         patch.dict("core.dispatcher.SEMAPHORES", {"tencent": asyncio.Semaphore(1)}, clear=False):
        got = await QuoteDispatcher.get_quote_with_fallback("510300", {"tencent": "sh510300"})

    assert got.name == "tencent上游"
    assert provider.get_quote.call_count == 1


@pytest.mark.asyncio
async def test_batch_cache_write_uses_source_ttl():
    """批量写缓存必须按源取 TTL：yfinance 不能沿用写死的 10 秒。"""
    quote = UnifiedQuote(
        symbol="AAPL", name="上游值", price=200.0, date="2026-09-11", source="yfinance"
    )
    provider = MagicMock()
    provider.get_quotes = AsyncMock(return_value={"AAPL": quote})

    with patch.dict("core.dispatcher.PROVIDERS", {"yfinance": provider}, clear=False), \
         patch.dict("core.dispatcher.SEMAPHORES", {"yfinance": asyncio.Semaphore(1)}, clear=False):
        await QuoteDispatcher.get_quotes_batch(
            [{"symbol": "AAPL", "allowed_sources": {"yfinance": "AAPL"}}]
        )

    ttl = quote_cache.ttl(get_quote_cache_key("AAPL"))
    assert ttl >= 590, f"批量路径必须沿用源级默认 TTL（yfinance 600s），实际 {ttl}"


@pytest.mark.asyncio
async def test_quote_cache_is_memory_only_and_expires():
    """报价短路缓存是纯内存的：不落文件，且 TTL 到期即失效。"""
    import time as _time

    quote_cache.setex(get_quote_cache_key("SH600000"), 1, "{}")
    assert quote_cache.ttl(get_quote_cache_key("SH600000")) == 1
    _time.sleep(1.1)
    assert quote_cache.get(get_quote_cache_key("SH600000")) is None, "过期后必须读不到"
    assert quote_cache.persist is False, "10 秒级报价数据不应落盘"
    assert quote_cache.path.exists() is False, "内存存储不应产生状态文件"