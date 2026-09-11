# -*- coding: utf-8 -*-
"""主动探测闸门：避免「每轮无条件全量探测」造成的持续上游压力。"""
from unittest.mock import AsyncMock, patch

import pytest

from core import config
from core import health as health_svc


@pytest.fixture
def reg():
    """注册临时探针并在测试结束后清理，避免污染其它用例。"""
    names = []

    def _register(name: str):
        health_svc.register_probe(name, "test", AsyncMock())
        names.append(name)
        return name

    yield _register
    for n in names:
        health_svc._probes.pop(n, None)
        health_svc._memory_calls.pop(n, None)
        health_svc._last_status.pop(n, None)


@pytest.mark.asyncio
async def test_skips_source_with_recent_business_traffic(reg):
    """有近期业务流量的源不该被主动探测（idle 判定必须生效）。"""
    name = reg("t-busy")
    health_svc.record_call(name, True, 1.0, via="business")

    with patch.object(health_svc, "run_probe", new_callable=AsyncMock) as mock_probe:
        await health_svc.probe_idle_sources()

    probed = [c.args[0] for c in mock_probe.call_args_list]
    assert name not in probed


@pytest.mark.asyncio
async def test_skips_recently_probed_source(reg):
    """同一源在最小探测间隔内不得重复探测。"""
    name = reg("t-just-probed")
    health_svc.record_call(name, True, 1.0, via="probe")

    with patch.object(health_svc, "run_probe", new_callable=AsyncMock) as mock_probe:
        await health_svc.probe_idle_sources()

    probed = [c.args[0] for c in mock_probe.call_args_list]
    assert name not in probed


@pytest.mark.asyncio
async def test_limits_probes_per_round(reg):
    """每轮最多探测 HEALTH_PROBE_MAX_PER_ROUND 个源（错峰，不一拥而上）。"""
    for i in range(5):
        reg(f"t-many-{i}")

    with patch.object(health_svc, "run_probe", new_callable=AsyncMock) as mock_probe:
        await health_svc.probe_idle_sources()

    assert mock_probe.call_count <= config.HEALTH_PROBE_MAX_PER_ROUND
    assert config.HEALTH_PROBE_MAX_PER_ROUND >= 1

