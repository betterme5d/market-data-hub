# -*- coding: utf-8 -*-
import asyncio
import os
import shutil
import tempfile
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from providers.quotes.xueqiu import XueqiuProvider
from core.timeseries_cache.manager import TimeSeriesCacheManager
from core.timeseries_cache.storage import ParquetStorageEngine
from core.timeseries_cache.tracker import IntervalTracker


@pytest.fixture
def temp_cache_dir():
    d = tempfile.mkdtemp(prefix="kline_cache_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.mark.asyncio
async def test_xueqiu_pagination_multi_page(monkeypatch):
    """测试当数据量超过单页 5000 条时，能够向前倒序翻页并在触底时正确终止。"""
    provider = XueqiuProvider()
    monkeypatch.setattr(provider, "_ensure_cookie", AsyncMock(return_value=("fake_cookie", "fake_ua")))

    # 模拟两页数据：
    # Page 1: 5000 根 (从 2026-09-09 到 2006-05-10) -> 满页
    # Page 2: 239 根 (从 2006-05-09 到 2005-02-23) -> < 5000 根，触发触底终止
    ts_2026_09_09 = 1788883200000
    ts_2006_05_10 = 1147219200000
    ts_2006_05_09 = 1147132800000
    ts_2005_02_23 = 1109088000000

    col = ["timestamp", "volume", "open", "high", "low", "close", "chg", "percent", "turnoverrate", "amount"]
    
    # 构造 Page 1 (5000条)
    page1_items = []
    # 仅为了构造测试数据，首项 2006-05-10，末项 2026-09-09
    for i in range(5000):
        # 简单模拟时间戳递增
        ts = ts_2006_05_10 + i * (ts_2026_09_09 - ts_2006_05_10) // 5000
        page1_items.append([ts, 100, 1.0, 1.1, 0.9, 1.05, 0.05, 5.0, 1.0, 1000.0])

    # 构造 Page 2 (239条)
    page2_items = []
    for i in range(239):
        ts = ts_2005_02_23 + i * (ts_2006_05_09 - ts_2005_02_23) // 239
        page2_items.append([ts, 50, 0.8, 0.9, 0.7, 0.85, 0.05, 6.0, 0.5, 500.0])

    call_count = 0
    requested_begins = []

    async def mock_get(url, headers, timeout=None):
        nonlocal call_count
        call_count += 1
        # 从 URL 中解析 begin 参数
        import urllib.parse
        parsed = urllib.parse.urlparse(str(url))
        qs = urllib.parse.parse_qs(parsed.query)
        begin_val = int(qs["begin"][0])
        requested_begins.append(begin_val)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        if call_count == 1:
            mock_resp.json.return_value = {
                "error_code": 0,
                "data": {"column": col, "item": page1_items}
            }
        else:
            mock_resp.json.return_value = {
                "error_code": 0,
                "data": {"column": col, "item": page2_items}
            }
        return mock_resp

    # 将 sleep 模拟为极短时间
    slept = []
    async def mock_sleep(sec):
        slept.append(sec)

    monkeypatch.setattr(asyncio, "sleep", mock_sleep)

    with patch("httpx.AsyncClient.get", side_effect=mock_get):
        chunk_calls = []
        async def on_chunk(records, s_date, e_date):
            chunk_calls.append((len(records), s_date, e_date))

        records = await provider._fetch_kline_slice(
            symbol="SH510050",
            slice_start="2005-02-23",
            slice_end="2026-09-09",
            period_str="day",
            adjust_type="normal",
            on_chunk=on_chunk
        )

    assert call_count == 2
    assert len(requested_begins) == 2
    # 第二页的 begin 必须等于第一页最早时间戳 - 1
    assert requested_begins[1] == page1_items[0][0] - 1
    # on_chunk 应该被即时调用了 2 次
    assert len(chunk_calls) == 2
    assert chunk_calls[0][0] == 5000
    assert chunk_calls[1][0] == 239
    # 总记录数 5239
    assert len(records) == 5239
    # 检验延时是否在 2-5 秒区间
    assert len(slept) == 1
    assert 2.0 <= slept[0] <= 5.0


@pytest.mark.asyncio
async def test_xueqiu_kline_parquet_cache_integration(temp_cache_dir, monkeypatch):
    """测试 get_history 接入 Parquet 缓存：首次落盘，二次 0 网络请求命中。"""
    storage = ParquetStorageEngine(base_dir=temp_cache_dir)
    tracker = IntervalTracker()
    cache_mgr = TimeSeriesCacheManager(base_dir=temp_cache_dir, storage=storage, tracker=tracker)

    provider = XueqiuProvider(cache_manager=cache_mgr)
    
    mock_slice_calls = 0
    fake_records = [
        {"date": "2026-01-02", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05, "volume": 100.0, "amount": 1000.0, "change": 0.05, "percent": 5.0, "turnover_rate": 1.0},
        {"date": "2026-01-05", "open": 1.05, "high": 1.15, "low": 1.0, "close": 1.1, "volume": 120.0, "amount": 1200.0, "change": 0.05, "percent": 4.76, "turnover_rate": 1.2},
    ]

    async def mock_fetch_slice(symbol, slice_start, slice_end, period_str, adjust_type, on_chunk=None):
        nonlocal mock_slice_calls
        mock_slice_calls += 1
        if on_chunk:
            await on_chunk(fake_records, slice_start, slice_end)
        return fake_records

    monkeypatch.setattr(provider, "_fetch_kline_slice", mock_slice_calls_wrapper := AsyncMock(side_effect=mock_fetch_slice))

    # 1. 首次查询：触发底层网络抓取与 Parquet 落盘
    res1 = await provider.get_history("SH510050", period="1mo", interval="1d", start="2026-01-01", end="2026-01-10", adj="none")
    assert mock_slice_calls == 1
    assert len(res1["data"]) == 2

    # 验证 Parquet 文件已生成
    p_path, m_path = storage.get_paths("kline", "SH510050", dimensions={"adj": "none", "interval": "1d", "source": "xueqiu"})
    assert os.path.exists(p_path)
    assert os.path.exists(m_path)

    # 2. 二次查询完全相同区间：直接命中缓存，0 网络请求！
    res2 = await provider.get_history("SH510050", period="1mo", interval="1d", start="2026-01-01", end="2026-01-10", adj="none")
    assert mock_slice_calls == 1  # 依然是 1，未新增调用
    assert len(res2["data"]) == 2
    assert res2["data"][0]["date"] == "2026-01-02"
    assert res2["data"][1]["date"] == "2026-01-05"
