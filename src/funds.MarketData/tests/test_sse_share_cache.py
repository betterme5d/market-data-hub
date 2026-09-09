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
