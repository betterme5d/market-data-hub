# -*- coding: utf-8 -*-
import os
import shutil
from unittest.mock import AsyncMock
import pytest

from core.models import FundNav
from core.timeseries_cache.manager import TimeSeriesCacheManager
from providers.funds.fund_nav import FundNavProvider

TEST_CACHE_DIR = "data/test_fund_nav_provider_cache"


@pytest.fixture(autouse=True)
def clean_cache():
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)
    yield
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)


@pytest.mark.asyncio
async def test_get_fund_nav_history_cached():
    cache_manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    mock_em = AsyncMock()
    mock_em.get_fund_nav_history.return_value = [
        FundNav(code="510300", nav_date="2024-01-02", unit_nav=3.5, accum_nav=3.6),
        FundNav(code="510300", nav_date="2024-01-03", unit_nav=3.55, accum_nav=3.65),
    ]

    provider = FundNavProvider(
        cache_manager=cache_manager,
        eastmoney_source=mock_em
    )

    # 首次查询：拉取并缓存
    res1 = await provider.get_fund_nav_history("510300", "2024-01-01", "2024-01-10", source="eastmoney")
    assert len(res1) == 2
    assert isinstance(res1[0], FundNav)
    assert res1[0].nav_date == "2024-01-02"
    assert mock_em.get_fund_nav_history.call_count == 1

    # 第二次查询：完全命中缓存
    mock_em.get_fund_nav_history.reset_mock()
    res2 = await provider.get_fund_nav_history("510300", "2024-01-01", "2024-01-10", source="eastmoney")
    assert len(res2) == 2
    assert mock_em.get_fund_nav_history.call_count == 0


@pytest.mark.asyncio
async def test_source_selection():
    cache_manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    mock_em = AsyncMock()
    mock_cm = AsyncMock()
    mock_cm.get_fund_nav_history.return_value = [
        FundNav(code="510300", nav_date="2024-01-02", unit_nav=3.5)
    ]

    provider = FundNavProvider(
        cache_manager=cache_manager,
        eastmoney_source=mock_em,
        cmtidp_source=mock_cm
    )

    res = await provider.get_fund_nav_history("510300", "2024-01-01", "2024-01-10", source="cmtidp")
    assert len(res) == 1
    assert mock_cm.get_fund_nav_history.call_count == 1
    assert mock_em.get_fund_nav_history.call_count == 0

    # 验证未知数据源抛出 ValueError
    with pytest.raises(ValueError, match="Unknown source"):
        await provider.get_fund_nav_history("510300", "2024-01-01", "2024-01-10", source="invalid_source")
