# -*- coding: utf-8 -*-
"""优雅退出：必须等待后台份额扇出落盘完成，且超时不能阻塞退出。"""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

import main
from providers.funds.shares.provider import fund_share_provider


@pytest.mark.asyncio
async def test_drain_waits_for_pending_fanouts():
    """退出时必须调用 wait_pending_fanouts 等待扇出落盘。"""
    with patch.object(
        fund_share_provider, "wait_pending_fanouts", new_callable=AsyncMock
    ) as mock_wait:
        await main._drain_share_fanouts()
    assert mock_wait.call_count == 1


@pytest.mark.asyncio
async def test_drain_times_out_without_raising():
    """超时只告警，不能让退出流程抛异常。"""

    async def _never_finish():
        await asyncio.sleep(3600)

    with patch.object(
        fund_share_provider, "wait_pending_fanouts", side_effect=_never_finish
    ):
        await main._drain_share_fanouts(timeout=0.01)  # 不抛异常即通过

