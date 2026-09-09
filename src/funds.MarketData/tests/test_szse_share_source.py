import io
import pytest
from unittest.mock import AsyncMock, patch
import openpyxl
from providers.exchanges.shares.szse import SzseShareSource, split_date_range

def test_split_date_range_under_limit():
    # 小于等于 60 天不切分
    slices = split_date_range("2026-01-01", "2026-02-15", max_days=60)
    assert slices == [("2026-01-01", "2026-02-15")]

def test_split_date_range_multi_chunks():
    # 超过 60 天均匀分片，边界严丝合缝
    slices = split_date_range("2026-01-01", "2026-04-10", max_days=60)
    assert len(slices) == 2
    assert slices[0] == ("2026-01-01", "2026-03-01")
    assert slices[1] == ("2026-03-02", "2026-04-10")

def _create_mock_szse_xlsx():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["日期", "基金代码", "基金简称", "基金规模(份)"])
    ws.append(["2026-06-30", "159901", "易方达深证100ETF", 2458900000])
    ws.append(["2026-06-30", "159915", "易方达创业板ETF", "1,500,000,000"])
    ws.append(["2026-06-29", "159901", "易方达深证100ETF", 2450000000])
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()

@pytest.mark.asyncio
async def test_szse_share_source_parse():
    source = SzseShareSource()
    xlsx_bytes = _create_mock_szse_xlsx()
    
    with patch("httpx.AsyncClient.get") as mock_get:
        mock_resp = AsyncMock()
        mock_resp.status_code = 200
        mock_resp.content = xlsx_bytes
        mock_resp.raise_for_status = lambda: None
        mock_get.return_value = mock_resp

        data = await source.fetch_market_shares("2026-06-29", "2026-06-30")
        assert "159901" in data
        assert "159915" in data
        
        # 验证 159901 解析结果及单位换算 (份 -> 万份)
        f100 = data["159901"]
        assert len(f100) == 2
        # 按日期倒序或正序，验证数据项
        dates = [x["share_date"] for x in f100]
        assert "2026-06-30" in dates
        assert "2026-06-29" in dates
        d30 = [x for x in f100 if x["share_date"] == "2026-06-30"][0]
        assert d30["raw_shares"] == 2458900000.0
        assert d30["shares"] == 245890.0
        assert d30["name"] == "易方达深证100ETF"

        # 验证带逗号字符串解析
        f_cyb = data["159915"]
        assert len(f_cyb) == 1
        assert f_cyb[0]["shares"] == 150000.0
