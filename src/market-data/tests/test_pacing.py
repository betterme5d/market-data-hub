# -*- coding: utf-8 -*-
"""礼貌延时：默认 0.2~0.5s 随机抖动，max=0 时不休眠。"""
from unittest.mock import AsyncMock, patch

import pytest

import core.pacing as pacing


@pytest.mark.asyncio
async def test_polite_delay_sleeps_within_range():
    """休眠时长必须落在 [min, max] 内。"""
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await pacing.polite_delay()
    assert mock_sleep.call_count == 1
    slept = mock_sleep.call_args.args[0]
    assert pacing.MIN_DELAY <= slept <= pacing.MAX_DELAY


@pytest.mark.asyncio
async def test_polite_delay_defaults_are_0_2_to_0_5():
    assert (pacing.MIN_DELAY, pacing.MAX_DELAY) == (0.2, 0.5)


@pytest.mark.asyncio
async def test_polite_delay_noop_when_disabled():
    """max=0 → 不休眠（便于测试与压测关闭）。"""
    with patch.object(pacing, "MAX_DELAY", 0.0), patch(
        "asyncio.sleep", new_callable=AsyncMock
    ) as mock_sleep:
        await pacing.polite_delay()
    assert mock_sleep.call_count == 0
