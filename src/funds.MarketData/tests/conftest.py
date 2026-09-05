# -*- coding: utf-8 -*-
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core import health as health_svc  # noqa: E402


@pytest.fixture(autouse=True)
def isolated_health_store(monkeypatch):
    """每个用例都隔离健康指标：强制内存存储并清空历史。"""
    monkeypatch.setattr(health_svc, "redis_client", None)
    health_svc._memory_calls.clear()
    health_svc._last_status.clear()
    yield


@pytest.fixture
def fixture_loader():
    """加载 tests/fixtures 下的录制响应。"""
    import json
    base = os.path.join(os.path.dirname(__file__), "fixtures")

    def load(name: str):
        with open(os.path.join(base, name), encoding="utf-8") as f:
            return json.load(f)

    return load
