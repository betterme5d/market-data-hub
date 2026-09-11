# -*- coding: utf-8 -*-
"""Valkey 客户端创建：不得在导入期 ping（否则启动抖动会让缓存永久降级）。"""
from unittest.mock import MagicMock, patch

from core import cache as cache_mod
from core import config


def test_create_client_does_not_ping():
    """创建客户端不得 ping：连接问题应在调用时按次降级，而不是让整个进程永久禁用缓存。"""
    fake = MagicMock()
    with patch("redis.Redis", return_value=fake):
        client = cache_mod._create_client()
    assert client is fake
    assert fake.ping.call_count == 0


def test_create_client_uses_central_config():
    """连接参数统一取 core.config（不再散落 os.getenv）。"""
    with patch("redis.Redis", return_value=MagicMock()) as mock_redis:
        cache_mod._create_client()
    kwargs = mock_redis.call_args.kwargs
    assert kwargs["host"] == config.VALKEY_HOST
    assert kwargs["port"] == config.VALKEY_PORT
    assert kwargs["password"] == config.VALKEY_PASSWORD
    assert kwargs["socket_connect_timeout"] == 2


def test_create_client_returns_none_on_failure():
    """构造失败（极少数情况）才降级为 None，不抛异常。"""
    with patch("redis.Redis", side_effect=RuntimeError("boom")):
        assert cache_mod._create_client() is None

