import pytest
from unittest.mock import AsyncMock
from pathlib import Path
from core.timeseries_cache.storage import ParquetStorageEngine
from core.timeseries_cache.tracker import IntervalTracker
from providers.funds.shares.provider import FundShareProvider
from providers.exchanges.shares.sse import SseShareSource

@pytest.mark.asyncio
async def test_fund_share_provider_sse_fanout(tmp_path: Path):
    storage = ParquetStorageEngine(base_dir=tmp_path)
    tracker = IntervalTracker()
    mock_sse = AsyncMock(spec=SseShareSource)
    
    # 模拟上游返回全市场 2 只沪市基金的数据
    mock_sse.fetch_market_shares_range.return_value = {
        "510050": [
            {"code": "510050", "share_date": "2026-06-25", "shares": 15000.5, "raw_shares": 150005000.0, "name": "50ETF"}
        ],
        "510300": [
            {"code": "510300", "share_date": "2026-06-25", "shares": 28000.0, "raw_shares": 280000000.0, "name": "300ETF"}
        ]
    }

    provider = FundShareProvider(storage=storage, tracker=tracker, sse_source=mock_sse)

    # 1. 首次查询 510050：触发上交所上游拉取并扇出持久化 510050 和 510300
    res1 = await provider.get_fund_shares("510050", "2026-06-01", "2026-06-30")
    assert len(res1) == 1
    assert res1[0].code == "510050"
    assert res1[0].shares == 15000.5
    assert mock_sse.fetch_market_shares_range.call_count == 1

    # 2. 查询 510300（同时间段）：应直接 100% 命中 exchange=sse 缓存，不再调用上游
    res2 = await provider.get_fund_shares("510300", "2026-06-01", "2026-06-30")
    assert len(res2) == 1
    assert res2[0].code == "510300"
    assert res2[0].shares == 28000.0
    assert mock_sse.fetch_market_shares_range.call_count == 1  # 依然保持 1

    # 3. 验证元数据覆盖区间
    meta = storage.read_metadata("fund_share", "510300", dimensions={"exchange": "sse"})
    assert [tuple(x) for x in meta["intervals"]] == [("2026-06-01", "2026-06-30")]


@pytest.mark.asyncio
async def test_provider_marks_pre_2012_sse_range_covered_without_fetching(tmp_path: Path):
    """沪市 2012-01-04 之前上游没有份额数据：直接按已覆盖记入，不请求上游。"""
    storage = ParquetStorageEngine(base_dir=tmp_path)
    mock_sse = AsyncMock(spec=SseShareSource)
    mock_sse.fetch_market_shares_range.return_value = {}
    provider = FundShareProvider(storage=storage, tracker=IntervalTracker(), sse_source=mock_sse)

    await provider.get_fund_shares("510050", "2005-02-22", "2005-09-29")

    mock_sse.fetch_market_shares_range.assert_not_called()
    meta = storage.read_metadata("fund_share", "510050", dimensions={"exchange": "sse"})
    assert [tuple(x) for x in meta["intervals"]] == [("2005-02-22", "2012-01-03")]


@pytest.mark.asyncio
async def test_provider_only_fetches_after_2012_boundary(tmp_path: Path):
    """跨 2012 边界：只把 2012-01-04 之后的部分发给上游。"""
    storage = ParquetStorageEngine(base_dir=tmp_path)
    mock_sse = AsyncMock(spec=SseShareSource)
    mock_sse.fetch_market_shares_range.return_value = {}
    provider = FundShareProvider(storage=storage, tracker=IntervalTracker(), sse_source=mock_sse)

    await provider.get_fund_shares("510050", "2011-12-01", "2012-01-31")

    args = mock_sse.fetch_market_shares_range.call_args.args
    assert args[0] == "2012-01-04"
    assert args[1] == "2012-01-31"


@pytest.mark.asyncio
async def test_provider_surfaces_incomplete_days(tmp_path: Path):
    """沪市某个分类接口失败的日子要透给调用方（C#），否则会被误标成"上游无数据"。"""
    storage = ParquetStorageEngine(base_dir=tmp_path)
    mock_sse = AsyncMock(spec=SseShareSource)

    async def fake_range(s, e, on_window=None, **kwargs):
        out = kwargs.get("incomplete_days_out")
        if out is not None:
            out.append("2026-06-24")
        return {}

    mock_sse.fetch_market_shares_range.side_effect = fake_range
    provider = FundShareProvider(storage=storage, tracker=IntervalTracker(), sse_source=mock_sse)

    incomplete: list[str] = []
    await provider.get_fund_shares("510050", "2026-06-01", "2026-06-30", incomplete_days=incomplete)

    assert incomplete == ["2026-06-24"]
