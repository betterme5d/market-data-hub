import pytest
from unittest.mock import AsyncMock
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
