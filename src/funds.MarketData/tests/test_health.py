# -*- coding: utf-8 -*-
"""
core.health 状态机与探针行为测试（全部离线）。
"""
import asyncio
import time

import pytest

from core import config
from core import health


class TestStateMachine:
    def test_unknown_when_no_calls(self):
        assert health.get_source_health("never-called")["status"] == health.STATUS_UNKNOWN

    def test_healthy_after_success(self):
        health.record_call("src-a", True, 10)
        snap = health.get_source_health("src-a")
        assert snap["status"] == health.STATUS_HEALTHY
        assert snap["window"]["success"] == 1
        assert snap["window"]["success_rate"] == 1.0

    def test_degraded_when_success_rate_below_threshold(self):
        for _ in range(6):
            health.record_call("src-b", True, 10)
        for _ in range(4):
            health.record_call("src-b", False, 10, error="boom")
        # 成功率 0.6 ≥ 0.5 → healthy
        assert health.get_source_health("src-b")["status"] == health.STATUS_HEALTHY

        for _ in range(3):
            health.record_call("src-b", False, 5)
        # 6/13 ≈ 0.46 < 0.5 → degraded
        assert health.get_source_health("src-b")["status"] == health.STATUS_DEGRADED

    def test_down_when_all_recent_calls_failed(self):
        for _ in range(config.HEALTH_DOWN_MIN_SAMPLES):
            health.record_call("src-c", False, 10, error="timeout")
        snap = health.get_source_health("src-c")
        assert snap["status"] == health.STATUS_DOWN
        assert snap["last_error"] == "timeout"

    def test_recover_from_down_by_success(self):
        for _ in range(config.HEALTH_DOWN_MIN_SAMPLES):
            health.record_call("src-d", False, 10)
        assert health.get_source_health("src-d")["status"] == health.STATUS_DOWN
        health.record_call("src-d", True, 10)
        # 3 失败 1 成功 → 成功率 0.25 → degraded（而不是直接回 healthy）
        assert health.get_source_health("src-d")["status"] == health.STATUS_DEGRADED

    def test_window_is_bounded(self):
        for i in range(config.HEALTH_WINDOW_SIZE * 2):
            health.record_call("src-e", i % 2 == 0, 1)
        snap = health.get_source_health("src-e")
        assert snap["window"]["size"] == config.HEALTH_WINDOW_SIZE

    def test_idle_flag(self, monkeypatch):
        health.record_call("src-f", True, 10)
        snap = health.get_source_health("src-f")
        assert snap["idle"] is False

        # 把时间推过 idle 阈值
        past = time.time() - config.HEALTH_IDLE_SECONDS - 1
        health._memory_calls["src-f"][0]["ts"] = past
        assert health.get_source_health("src-f")["idle"] is True


class TestProbe:
    async def test_run_probe_success_records_call(self):
        calls = []

        async def ok_probe():
            calls.append(1)

        health.register_probe("probe-ok", "test", ok_probe)
        try:
            result = await health.run_probe("probe-ok")
            assert result["status"] == "healthy"
            snap = health.get_source_health("probe-ok")
            assert snap["status"] == health.STATUS_HEALTHY
            assert len(calls) == 1
        finally:
            health._probes.pop("probe-ok", None)

    async def test_run_probe_failure_records_error(self):
        async def bad_probe():
            raise RuntimeError("upstream refused")

        health.register_probe("probe-bad", "test", bad_probe)
        try:
            # 单次失败只降级为 degraded（少样本不误判 down）
            result = await health.run_probe("probe-bad")
            assert result["status"] == "down"
            assert "refused" in result["error"]
            assert health.get_source_health("probe-bad")["status"] == health.STATUS_DEGRADED

            # 连续失败达到样本门槛后置为 down
            for _ in range(config.HEALTH_DOWN_MIN_SAMPLES - 1):
                await health.run_probe("probe-bad")
            assert health.get_source_health("probe-bad")["status"] == health.STATUS_DOWN
        finally:
            health._probes.pop("probe-bad", None)

    async def test_run_probe_timeout(self, monkeypatch):
        monkeypatch.setattr(config, "HEALTH_PROBE_TIMEOUT", 0.2)

        async def slow_probe():
            await asyncio.sleep(5)

        health.register_probe("probe-slow", "test", slow_probe)
        try:
            result = await health.run_probe("probe-slow")
            assert result["status"] == "down"
        finally:
            health._probes.pop("probe-slow", None)

    async def test_run_probe_unknown_source(self):
        with pytest.raises(KeyError):
            await health.run_probe("no-such-source")

    async def test_probe_idle_sources_only_probes_idle(self):
        probed = []

        async def mk(name):
            async def p():
                probed.append(name)
            return p

        health.register_probe("idle-src", "test", await mk("idle-src"))
        health.register_probe("busy-src", "test", await mk("busy-src"))
        try:
            # busy-src 刚有业务流量，不应被探测
            health.record_call("busy-src", True, 5)
            results = await health.probe_idle_sources()
            assert probed == ["idle-src"]
            assert results[0]["status"] == "healthy"
        finally:
            health._probes.pop("idle-src", None)
            health._probes.pop("busy-src", None)
