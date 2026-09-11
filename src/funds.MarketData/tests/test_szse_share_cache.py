import pytest
from unittest.mock import AsyncMock, patch
from pathlib import Path
from core.timeseries_cache.storage import ParquetStorageEngine
from core.timeseries_cache.tracker import IntervalTracker
from providers.funds.shares.provider import FundShareProvider
from providers.exchanges.shares.szse import SzseShareSource

@pytest.mark.asyncio
async def test_fund_share_provider_fanout(tmp_path: Path):
    storage = ParquetStorageEngine(base_dir=tmp_path)
    tracker = IntervalTracker()
    mock_source = AsyncMock(spec=SzseShareSource)
    
    # 模拟上游返回全市场 2 只基金的数据
    mock_source.fetch_market_shares.return_value = {
        "159901": [
            {"code": "159901", "share_date": "2026-06-30", "shares": 245890.0, "raw_shares": 2458900000.0, "name": "深证100"}
        ],
        "159915": [
            {"code": "159915", "share_date": "2026-06-30", "shares": 150000.0, "raw_shares": 1500000000.0, "name": "创业板"}
        ]
    }

    provider = FundShareProvider(storage=storage, tracker=tracker, szse_source=mock_source)

    # 1. 首次查询 159901：触发上游拉取并扇出持久化 159901 和 159915
    res1 = await provider.get_fund_shares("159901", "2026-06-01", "2026-06-30")
    assert len(res1) == 1
    assert res1[0].code == "159901"
    assert res1[0].shares == 245890.0
    assert mock_source.fetch_market_shares.call_count == 1

    # 2. 查询 159915（同时间段）：应直接 100% 命中缓存，不再请求上游
    res2 = await provider.get_fund_shares("159915", "2026-06-01", "2026-06-30")
    assert len(res2) == 1
    assert res2[0].code == "159915"
    assert res2[0].shares == 150000.0
    # 上游调用次数依然保持为 1！说明全市场扇出预热成功
    assert mock_source.fetch_market_shares.call_count == 1

    # 3. 验证元数据覆盖区间与物理文件
    meta1 = storage.read_metadata("fund_share", "159901", dimensions={"exchange": "szse"})
    assert [tuple(x) for x in meta1["intervals"]] == [("2026-06-01", "2026-06-30")]

    meta2 = storage.read_metadata("fund_share", "159915", dimensions={"exchange": "szse"})
    assert [tuple(x) for x in meta2["intervals"]] == [("2026-06-01", "2026-06-30")]


@pytest.mark.asyncio
async def test_share_provider_force_refetch_even_when_covered(tmp_path: Path):
    """force=True 时即使区间已覆盖也必须重取；force=False 命中覆盖则不重取。"""
    storage = ParquetStorageEngine(base_dir=tmp_path)
    dims = {"exchange": "szse"}
    storage.write_metadata(
        "fund_share",
        "159901",
        {
            "key": "159901",
            "namespace": "fund_share",
            "dimensions": dims,
            "intervals": [["2026-06-01", "2026-06-30"]],
            "date_column": "share_date",
        },
        dimensions=dims,
    )
    mock_source = AsyncMock(spec=SzseShareSource)
    mock_source.fetch_market_shares.return_value = {}
    provider = FundShareProvider(storage=storage, szse_source=mock_source)

    await provider.get_fund_shares("159901", "2026-06-01", "2026-06-30", exchange="szse")
    assert mock_source.fetch_market_shares.call_count == 0, "已覆盖且未 force → 不应重取"

    await provider.get_fund_shares(
        "159901", "2026-06-01", "2026-06-30", exchange="szse", force=True
    )
    assert mock_source.fetch_market_shares.call_count == 1, "force=True → 必须重取"


@pytest.mark.asyncio
async def test_szse_share_chunks_apply_polite_delay(tmp_path: Path):
    """>60 天区间会被切成多片，片间必须有礼貌延时（片数-1 次）。"""
    from providers.exchanges.shares.szse import split_date_range
    from providers.funds.shares import provider as share_provider_mod

    storage = ParquetStorageEngine(base_dir=tmp_path)
    mock_source = AsyncMock(spec=SzseShareSource)
    mock_source.fetch_market_shares.return_value = {}
    provider = FundShareProvider(storage=storage, szse_source=mock_source)

    with patch.object(share_provider_mod, "polite_delay", new_callable=AsyncMock) as mock_delay:
        await provider.get_fund_shares("159901", "2026-01-01", "2026-06-30", exchange="szse")

    expected_chunks = len(split_date_range("2026-01-01", "2026-06-30", max_days=60))
    assert expected_chunks > 1
    assert mock_source.fetch_market_shares.call_count == expected_chunks
    assert mock_delay.call_count == expected_chunks - 1
