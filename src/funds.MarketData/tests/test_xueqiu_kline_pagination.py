# -*- coding: utf-8 -*-
import asyncio
import os
import shutil
import tempfile
from datetime import date, datetime, timedelta
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from providers.quotes.xueqiu import XueqiuProvider
from core.timeseries_cache.manager import TimeSeriesCacheManager
from core.timeseries_cache.storage import ParquetStorageEngine
from core.timeseries_cache.tracker import IntervalTracker


def _ts(y, m, d):
    """构造当日 12:00 的毫秒时间戳，避免解析回本地日期时跨日。"""
    return int(datetime(y, m, d, 12, 0, 0).timestamp() * 1000)


KLINE_COLUMNS = ["timestamp", "volume", "open", "high", "low", "close", "chg", "percent", "turnoverrate", "amount"]


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
    # 缓存维度已归一化（D26）：adj=normal/before/after、interval=day/week/month
    p_path, m_path = storage.get_paths(
        "kline", "SH510050", dimensions={"adj": "normal", "interval": "day", "source": "xueqiu"}
    )
    assert os.path.exists(p_path)
    assert os.path.exists(m_path)

    # 2. 二次查询完全相同区间：直接命中缓存，0 网络请求！
    res2 = await provider.get_history("SH510050", period="1mo", interval="1d", start="2026-01-01", end="2026-01-10", adj="none")
    assert mock_slice_calls == 1  # 依然是 1，未新增调用
    assert len(res2["data"]) == 2
    assert res2["data"][0]["date"] == "2026-01-02"
    assert res2["data"][1]["date"] == "2026-01-05"


@pytest.mark.asyncio
async def test_small_window_uses_weekday_count_single_page(monkeypatch):
    """小窗口：count 按区间工作日数下发（不再固定 5000），且窗口起始日落在周末时也能单页收敛。

    2026-01-04 是周日：最早 K 线日期（01-05）必然晚于 slice_start，
    只能靠「估算未被上限截断」这条终止依据收敛，否则会多发一次翻页请求。
    """
    provider = XueqiuProvider()
    monkeypatch.setattr(provider, "_ensure_cookie", AsyncMock(return_value=("fake_cookie", "fake_ua")))

    # 2026-01-05(周一) ~ 2026-01-16(周五) 共 10 个交易日
    items = [
        [_ts(2026, 1, d), 100, 1.0, 1.1, 0.9, 1.05, 0.05, 5.0, 1.0, 1000.0]
        for d in [5, 6, 7, 8, 9, 12, 13, 14, 15, 16]
    ]

    requested_counts = []

    async def mock_get(url, headers, timeout=None):
        import urllib.parse
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(str(url)).query)
        requested_counts.append(int(qs["count"][0]))
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"error_code": 0, "data": {"column": KLINE_COLUMNS, "item": items}}
        return mock_resp

    slept = []

    async def mock_sleep(sec):
        slept.append(sec)

    monkeypatch.setattr(asyncio, "sleep", mock_sleep)

    with patch("httpx.AsyncClient.get", side_effect=mock_get):
        chunk_calls = []

        async def on_chunk(records, s_date, e_date):
            chunk_calls.append((len(records), s_date, e_date))

        records = await provider._fetch_kline_slice(
            symbol="SH510300",
            slice_start="2026-01-04",
            slice_end="2026-01-16",
            period_str="day",
            adjust_type="before",
            on_chunk=on_chunk,
        )

    assert requested_counts == [-10]  # 10 个工作日，而非固定 5000
    assert len(records) == 10
    assert len(chunk_calls) == 1
    assert chunk_calls[0] == (10, "2026-01-04", "2026-01-16")  # 覆盖区间对齐整个窗口
    assert slept == []  # 单页收敛，无翻页延时


@pytest.mark.asyncio
async def test_weekend_only_window_skips_network(monkeypatch):
    """纯周末窗口必然无 K 线：不发任何网络请求，也不触发 on_chunk（由缓存管理器标记区间覆盖）。"""
    provider = XueqiuProvider()
    monkeypatch.setattr(provider, "_ensure_cookie", AsyncMock(return_value=("fake_cookie", "fake_ua")))

    async def forbidden_get(url, headers, timeout=None):
        raise AssertionError(f"纯周末窗口不应发起网络请求，但请求了: {url}")

    with patch("httpx.AsyncClient.get", side_effect=forbidden_get):
        chunk_calls = []

        async def on_chunk(records, s_date, e_date):
            chunk_calls.append((len(records), s_date, e_date))

        records = await provider._fetch_kline_slice(
            symbol="SH510300",
            slice_start="2026-09-05",  # 周六
            slice_end="2026-09-06",    # 周日
            period_str="day",
            adjust_type="before",
            on_chunk=on_chunk,
        )

    assert records == []
    assert chunk_calls == []


def _mock_get_returning(days: list, columns: list):
    async def mock_get(url, headers, timeout=None):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "error_code": 0,
            "data": {"column": columns, "item": [[_ts(*d), 100, 1.0, 1.1, 0.9, 1.05, 0.05, 5.0, 1.0, 1000.0] for d in days]},
        }
        return mock_resp
    return mock_get


@pytest.mark.asyncio
async def test_today_bar_is_not_frozen_in_coverage(monkeypatch):
    """覆盖到今天的切片：覆盖声明只到昨天，避免盘中半日 K 线被固化成「今天已就绪」。"""
    provider = XueqiuProvider()
    monkeypatch.setattr(provider, "_ensure_cookie", AsyncMock(return_value=("fake_cookie", "fake_ua")))

    today = date.today()
    yesterday = today - timedelta(days=1)
    days = [(yesterday.year, yesterday.month, yesterday.day), (today.year, today.month, today.day)]

    with patch("httpx.AsyncClient.get", side_effect=_mock_get_returning(days, KLINE_COLUMNS)):
        chunk_calls = []

        async def on_chunk(records, s_date, e_date):
            chunk_calls.append((len(records), s_date, e_date))

        await provider._fetch_kline_slice(
            symbol="SH510300",
            slice_start=(today - timedelta(days=5)).strftime("%Y-%m-%d"),
            slice_end=today.strftime("%Y-%m-%d"),
            period_str="day",
            adjust_type="normal",
            on_chunk=on_chunk,
        )

    assert len(chunk_calls) == 1
    assert chunk_calls[0][2] == yesterday.strftime("%Y-%m-%d")  # 声明到昨天，不含今天


@pytest.mark.asyncio
async def test_today_only_slice_does_not_claim_today(monkeypatch):
    """切片仅含今天一天时，不得把今天声明为已覆盖（否则回退分支会固化它）。"""
    today = date.today()
    if today.weekday() >= 5:
        pytest.skip("今天不是工作日，单日窗口无 K 线")
    provider = XueqiuProvider()
    monkeypatch.setattr(provider, "_ensure_cookie", AsyncMock(return_value=("fake_cookie", "fake_ua")))

    today_str = today.strftime("%Y-%m-%d")
    yesterday_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    with patch("httpx.AsyncClient.get", side_effect=_mock_get_returning([(today.year, today.month, today.day)], KLINE_COLUMNS)):
        chunk_calls = []

        async def on_chunk(records, s_date, e_date):
            chunk_calls.append((len(records), s_date, e_date))

        await provider._fetch_kline_slice(
            symbol="SH510300",
            slice_start=today_str,
            slice_end=today_str,
            period_str="day",
            adjust_type="normal",
            on_chunk=on_chunk,
        )

    assert len(chunk_calls) == 1
    assert today_str not in chunk_calls[0][1:]
    assert chunk_calls[0][1:] == (yesterday_str, yesterday_str)


@pytest.mark.asyncio
async def test_empty_first_page_raises_instead_of_returning_empty(monkeypatch):
    """首屏返回空必须抛异常：返回空会被缓存记为「已覆盖」、被 C# 标成"上游无数据"并永久固化。

    实证：160324/161038/511260/512570 雪球各有 1500~2200 根 K 线，却因一次静默空被整段误标。
    """
    from core.exceptions import BusinessException

    provider = XueqiuProvider()
    monkeypatch.setattr(provider, "_ensure_cookie", AsyncMock(return_value=("fake_cookie", "fake_ua")))

    async def mock_get(url, headers, timeout=None):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"error_code": 0, "data": {"column": KLINE_COLUMNS, "item": []}}
        return resp

    with patch("httpx.AsyncClient.get", side_effect=mock_get):
        with pytest.raises(BusinessException):
            await provider._fetch_kline_slice(
                symbol="SZ160324",
                slice_start="2017-08-08",
                slice_end="2026-09-10",
                period_str="day",
                adjust_type="normal",
            )


@pytest.mark.asyncio
async def test_empty_later_page_still_ends_normally(monkeypatch):
    """翻到后面的页才空 = 已触底回到历史起点，属正常结束，不能抛异常。"""
    provider = XueqiuProvider()
    monkeypatch.setattr(provider, "_ensure_cookie", AsyncMock(return_value=("fake_cookie", "fake_ua")))

    page1_items = [[_ts(2026, 9, 8), 100, 1.0, 1.1, 0.9, 1.05, 0.05, 5.0, 1.0, 1000.0]]
    call_count = 0

    async def mock_get(url, headers, timeout=None):
        nonlocal call_count
        call_count += 1
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = (
            {"error_code": 0, "data": {"column": KLINE_COLUMNS, "item": page1_items}}
            if call_count == 1
            else {"error_code": 0, "data": {"column": KLINE_COLUMNS, "item": []}}
        )
        return resp

    async def mock_sleep(sec):
        return None

    monkeypatch.setattr(asyncio, "sleep", mock_sleep)
    with patch("httpx.AsyncClient.get", side_effect=mock_get):
        records = await provider._fetch_kline_slice(
            symbol="SZ160324",
            slice_start="2005-01-01",
            slice_end="2026-09-10",
            period_str="day",
            adjust_type="normal",
        )

    assert [r["date"] for r in records] == ["2026-09-08"]


class _CapturingCacheManager:
    """只记录 get_or_fetch 的入参，用于断言缓存维度是否被净化。"""

    def __init__(self):
        self.kwargs = None

    async def get_or_fetch(self, **kwargs):
        self.kwargs = kwargs
        return []


@pytest.mark.asyncio
async def test_legacy_get_history_normalizes_cache_dimensions():
    """legacy get_history（/history/{symbol}）不得把查询参数原样当缓存维度——否则可目录穿越。"""
    from providers.quotes.xueqiu import XueqiuProvider

    cap = _CapturingCacheManager()
    provider = XueqiuProvider(cache_manager=cap)

    await provider.get_history(
        "510300.SH", "1mo", "../../evil", None, None, "../../evil"
    )

    dims = cap.kwargs["dimensions"]
    assert dims["adj"] in {"normal", "before", "after"}, dims
    assert dims["interval"] in {"day", "week", "month"}, dims
