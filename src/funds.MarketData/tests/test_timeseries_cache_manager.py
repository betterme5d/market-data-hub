# -*- coding: utf-8 -*-
import asyncio
import os
import shutil
from datetime import date, timedelta
from unittest.mock import AsyncMock
import pytest

from core.timeseries_cache.manager import TimeSeriesCacheManager
from core.timeseries_cache.storage import ParquetStorageEngine
from core.timeseries_cache.tracker import IntervalTracker

TEST_CACHE_DIR = "data/test_manager_cache"


@pytest.fixture(autouse=True)
def clean_test_cache():
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)
    yield
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)


@pytest.mark.asyncio
async def test_first_fetch_and_cache_hit():
    manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)

    fetch_mock = AsyncMock()
    fetch_mock.return_value = [
        {"nav_date": "2024-01-02", "unit_nav": 3.1},
        {"nav_date": "2024-01-03", "unit_nav": 3.2},
    ]

    # 首次调用，应触发 fetch
    res1 = await manager.get_or_fetch(
        namespace="fund_nav",
        key="510300",
        start_date="2024-01-01",
        end_date="2024-01-10",
        fetch_fn=fetch_mock,
        date_column="nav_date",
        dimensions={"source": "eastmoney"}
    )
    assert len(res1) == 2
    assert fetch_mock.call_count == 1
    fetch_mock.assert_called_with("2024-01-01", "2024-01-10")

    # 第二次相同请求，完全命中缓存，0 次 fetch
    fetch_mock.reset_mock()
    res2 = await manager.get_or_fetch(
        namespace="fund_nav",
        key="510300",
        start_date="2024-01-01",
        end_date="2024-01-10",
        fetch_fn=fetch_mock,
        date_column="nav_date",
        dimensions={"source": "eastmoney"}
    )
    assert len(res2) == 2
    assert fetch_mock.call_count == 0


@pytest.mark.asyncio
async def test_incremental_extension():
    manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)

    # 预置 1 月数据
    fetch_mock = AsyncMock()
    fetch_mock.return_value = [
        {"nav_date": "2024-01-15", "unit_nav": 3.0}
    ]
    await manager.get_or_fetch(
        namespace="fund_nav",
        key="510300",
        start_date="2024-01-01",
        end_date="2024-01-31",
        fetch_fn=fetch_mock,
        date_column="nav_date"
    )
    assert fetch_mock.call_count == 1

    # 查询扩展至 2 月 15 日
    fetch_mock.reset_mock()
    fetch_mock.return_value = [
        {"nav_date": "2024-02-05", "unit_nav": 3.1}
    ]
    res = await manager.get_or_fetch(
        namespace="fund_nav",
        key="510300",
        start_date="2024-01-01",
        end_date="2024-02-15",
        fetch_fn=fetch_mock,
        date_column="nav_date"
    )
    # 仅向 fetch_fn 请求缺失的 2024-02-01 ~ 2024-02-15
    assert fetch_mock.call_count == 1
    fetch_mock.assert_called_with("2024-02-01", "2024-02-15")
    assert len(res) == 2
    assert [r["nav_date"] for r in res] == ["2024-01-15", "2024-02-05"]


@pytest.mark.asyncio
async def test_concurrency_lock():
    manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)

    call_counter = 0

    async def slow_fetch(s, e):
        nonlocal call_counter
        call_counter += 1
        await asyncio.sleep(0.05)
        return [{"nav_date": "2024-01-02", "unit_nav": 3.0}]

    # 并发 5 个协程同时请求
    tasks = [
        manager.get_or_fetch(
            namespace="fund_nav",
            key="510300",
            start_date="2024-01-01",
            end_date="2024-01-10",
            fetch_fn=slow_fetch,
            date_column="nav_date"
        )
        for _ in range(5)
    ]
    results = await asyncio.gather(*tasks)

    # 确认所有协程拿到正确结果
    for r in results:
        assert len(r) == 1
    # 协程锁生效，fetch 仅执行 1 次
    assert call_counter == 1


@pytest.mark.asyncio
async def test_t_day_protection_when_not_published():
    manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    today = date.today()
    yesterday = today - timedelta(days=1)
    today_str = today.strftime("%Y-%m-%d")
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    fetch_mock = AsyncMock()
    # 上游只返回到昨天，没有今天的净值
    fetch_mock.return_value = [
        {"nav_date": yesterday_str, "unit_nav": 3.0}
    ]

    res = await manager.get_or_fetch(
        namespace="fund_nav",
        key="510300",
        start_date=yesterday_str,
        end_date=today_str,
        fetch_fn=fetch_mock,
        date_column="nav_date"
    )
    assert len(res) == 1
    assert fetch_mock.call_count == 1

    # 验证元数据中的 intervals 右边界没有被推到 today
    meta = manager.storage.read_metadata("fund_nav", "510300")
    assert meta is not None
    intervals = meta.get("intervals", [])
    assert intervals == [[yesterday_str, yesterday_str]]

    # 再次查询 today：由于在 pending 保护 TTL 内，不重复请求外部
    fetch_mock.reset_mock()
    res2 = await manager.get_or_fetch(
        namespace="fund_nav",
        key="510300",
        start_date=yesterday_str,
        end_date=today_str,
        fetch_fn=fetch_mock,
        date_column="nav_date"
    )
    assert len(res2) == 1
    assert fetch_mock.call_count == 0


@pytest.mark.asyncio
async def test_streaming_chunk_persistence_and_breakpoint_resumption():
    manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)

    # 模拟两页抓取：第一页成功，第二页崩溃抛异常
    async def mock_streaming_fetch_with_crash(s, e, on_chunk=None):
        if on_chunk:
            # 提交第一页数据（2024-03-15 ~ 2024-03-31）
            p1_records = [{"nav_date": "2024-03-20", "unit_nav": 3.5}]
            await on_chunk(p1_records, "2024-03-15", "2024-03-31")
            # 第二页发生网络中断 / 反爬异常
            raise RuntimeError("Network error on page 2")
        return []

    # 首次拉取：中间抛出异常
    with pytest.raises(RuntimeError, match="Network error on page 2"):
        await manager.get_or_fetch(
            namespace="fund_nav",
            key="510300",
            start_date="2024-03-01",
            end_date="2024-03-31",
            fetch_fn=mock_streaming_fetch_with_crash,
            date_column="nav_date",
        )

    # 验证关键点 1：虽然请求异常中断，但第 1 页数据已经原子写盘入库！
    records_in_storage = manager.storage.read_records("fund_nav", "510300", date_column="nav_date")
    assert len(records_in_storage) == 1
    assert records_in_storage[0]["nav_date"] == "2024-03-20"

    # 验证关键点 2：meta.json 中的覆盖区间已记录 [2024-03-15, 2024-03-31]
    meta = manager.storage.read_metadata("fund_nav", "510300")
    assert meta["intervals"] == [["2024-03-15", "2024-03-31"]]

    # 模拟第二次拉取（断点续查）：
    # 期望仅向上游请求尚未覆盖的剩余区间 [2024-03-01, 2024-03-14]
    requested_slices = []

    async def mock_resumed_fetch(s, e, on_chunk=None):
        requested_slices.append((s, e))
        p2_records = [{"nav_date": "2024-03-05", "unit_nav": 3.4}]
        if on_chunk:
            await on_chunk(p2_records, s, e)
        return p2_records

    res = await manager.get_or_fetch(
        namespace="fund_nav",
        key="510300",
        start_date="2024-03-01",
        end_date="2024-03-31",
        fetch_fn=mock_resumed_fetch,
        date_column="nav_date",
    )

    # 验证关键点 3：第二次请求绝不重复访问已缓存的 2024-03-15 ~ 2024-03-31！
    assert len(requested_slices) == 1
    assert requested_slices[0] == ("2024-03-01", "2024-03-14")

    # 验证关键点 4：结果完整合并，存储总共包含 2 条数据，区间已完整合并为 2024-03-01 ~ 2024-03-31
    assert len(res) == 2
    assert [r["nav_date"] for r in res] == ["2024-03-05", "2024-03-20"]
    meta_final = manager.storage.read_metadata("fund_nav", "510300")
    assert meta_final["intervals"] == [["2024-03-01", "2024-03-31"]]
