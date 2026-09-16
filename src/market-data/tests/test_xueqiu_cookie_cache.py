# -*- coding: utf-8 -*-
"""雪球 Cookie 共享缓存（Valkey）行为测试"""
from unittest.mock import AsyncMock, patch

import pytest

from providers.quotes.xueqiu import XueqiuProvider


@pytest.mark.asyncio
async def test_reuses_shared_cookie_without_touching_gateway():
    """共享缓存命中时直接复用，不再经网关取 Cookie"""
    provider = XueqiuProvider()
    with patch("core.cache.get_xueqiu_auth",
               return_value={"cookie": "xq_a_token=shared", "userAgent": "UA-shared"}), \
         patch("httpx.AsyncClient.get", side_effect=AssertionError("共享缓存命中时不应发起网络请求")):
        cookie, ua = await provider._ensure_cookie()

    assert cookie == "xq_a_token=shared"
    assert ua == "UA-shared"


@pytest.mark.asyncio
async def test_gateway_cookie_is_written_to_shared_cache():
    """网关取到的 Cookie 应写入共享缓存，供其它实例/进程复用"""
    provider = XueqiuProvider()
    gateway_resp = AsyncMock(status_code=200)
    gateway_resp.json = lambda: {"success": True, "cookie": "xq_a_token=fresh", "userAgent": "UA-fresh"}
    written = {}

    with patch("core.cache.get_xueqiu_auth", return_value=None), \
         patch("core.cache.set_xueqiu_auth",
               side_effect=lambda cookie, user_agent, ttl=3600: written.update(cookie=cookie, ua=user_agent)), \
         patch("httpx.AsyncClient.get", AsyncMock(return_value=gateway_resp)):
        cookie, ua = await provider._ensure_cookie()

    assert cookie == "xq_a_token=fresh"
    assert written == {"cookie": "xq_a_token=fresh", "ua": "UA-fresh"}


@pytest.mark.asyncio
async def test_shared_cookie_failure_falls_back_to_gateway():
    """共享缓存不可用（返回 None）时回落网关取数，不影响取数链路"""
    provider = XueqiuProvider()
    gateway_resp = AsyncMock(status_code=200)
    gateway_resp.json = lambda: {"success": True, "cookie": "xq_a_token=fallback", "userAgent": "UA-fb"}

    with patch("core.cache.get_xueqiu_auth", return_value=None), \
         patch("core.cache.set_xueqiu_auth"), \
         patch("httpx.AsyncClient.get", AsyncMock(return_value=gateway_resp)):
        cookie, _ = await provider._ensure_cookie()

    assert cookie == "xq_a_token=fallback"
