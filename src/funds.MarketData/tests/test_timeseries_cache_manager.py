# -*- coding: utf-8 -*-
import asyncio
import os
import shutil
import tempfile
from datetime import date, timedelta
from unittest.mock import AsyncMock
import pytest

from core.timeseries_cache.manager import TimeSeriesCacheManager
from core.timeseries_cache.storage import ParquetStorageEngine
from core.timeseries_cache.tracker import IntervalTracker

# 测试产物落在系统临时目录：仓库目录被 dev 容器挂载并 watch，
# 在源码树内反复建/删目录会让 uvicorn 的 StatReload 看门狗 rglob 撞上已消失的目录而崩溃
TEST_CACHE_DIR = os.path.join(tempfile.gettempdir(), f"funds_cache_mgr_test_{os.getpid()}")


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
@pytest.mark.parametrize(
    "start_date,end_date",
    [
        ("2020-01-01", "2020-12-31"),
        (
            (date.today() - timedelta(days=3)).strftime("%Y-%m-%d"),
            date.today().strftime("%Y-%m-%d"),
        ),
    ],
)
async def test_empty_chunk_does_not_claim_coverage(start_date, end_date):
    """空响应（可能是上游静默失败）不能记为已覆盖：不写 intervals，下次必须重问上游。"""
    manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)

    async def empty_streaming_fetch(s, e, on_chunk=None):
        # 模拟爬虫拿到空页：不触发任何 on_chunk，返回空列表
        return []

    res = await manager.get_or_fetch(
        namespace="fund_nav",
        key="012345",
        start_date=start_date,
        end_date=end_date,
        fetch_fn=empty_streaming_fetch,
        date_column="nav_date",
    )
    assert res == []
    meta = manager.storage.read_metadata("fund_nav", "012345")
    assert (meta or {}).get("intervals", []) == []

    # 上游恢复后重跑：缺口仍视为缺失，整段重取而不是命中"空缓存"
    fetch_mock = AsyncMock(return_value=[{"nav_date": start_date, "unit_nav": 1.23}])
    res2 = await manager.get_or_fetch(
        namespace="fund_nav",
        key="012345",
        start_date=start_date,
        end_date=end_date,
        fetch_fn=fetch_mock,
        date_column="nav_date",
    )
    assert fetch_mock.call_count == 1
    assert fetch_mock.await_args.args[:2] == (start_date, end_date)
    assert [r["nav_date"] for r in res2] == [start_date]


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


async def _prefill_two_intervals(manager: TimeSeriesCacheManager, key: str = "510500"):
    """预置两段缓存：[2026-01-01, 03-31] 与 [2026-05-01, 06-30]。"""
    prefill = AsyncMock(return_value=[{"date": "2026-01-15", "close": 1.0}])
    for s, e in [("2026-01-01", "2026-03-31"), ("2026-05-01", "2026-06-30")]:
        await manager.get_or_fetch(
            namespace="kline", key=key, start_date=s, end_date=e,
            fetch_fn=prefill, date_column="date",
        )
    meta = manager.storage.read_metadata("kline", key)
    assert meta["intervals"] == [["2026-01-01", "2026-03-31"], ["2026-05-01", "2026-06-30"]]


@pytest.mark.asyncio
async def test_coalesce_merges_gap_slices_into_single_fetch():
    """提供 coalesce 时，多个缺口合并为一次上游请求（请求次数是风控敏感资源）。"""
    manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    await _prefill_two_intervals(manager)

    requested = []

    async def fetch(s, e):
        requested.append((s, e))
        return [{"date": "2026-04-15", "close": 1.0}]

    res = await manager.get_or_fetch(
        namespace="kline", key="510500",
        start_date="2026-02-01", end_date="2026-08-31",
        fetch_fn=fetch, date_column="date",
        coalesce=lambda s, e: True,
    )

    # 缺口 [04-01, 04-30] 与 [07-01, 08-31] 合并成一次请求
    assert requested == [("2026-04-01", "2026-08-31")]
    assert [r["date"] for r in res] == ["2026-04-15"]  # 2026-01-15 落在请求区间外，被读取过滤
    # 中间已覆盖区间被顺带重拉，覆盖声明合并为完整一段
    assert manager.storage.read_metadata("kline", "510500")["intervals"] == [["2026-01-01", "2026-08-31"]]


@pytest.mark.asyncio
async def test_coalesce_breaks_when_span_exceeds_limit():
    """合并后超出单页容量时必须断开另起一段，不能把多次请求撑成更多次。"""
    manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    await _prefill_two_intervals(manager)

    requested = []
    probed = []

    async def fetch(s, e):
        requested.append((s, e))
        return [{"date": "2026-04-15", "close": 1.0}]

    def can_coalesce(s, e):
        probed.append((s, e))
        return False  # 模拟「合并后跨度超过单页容量」

    await manager.get_or_fetch(
        namespace="kline", key="510500",
        start_date="2026-02-01", end_date="2026-08-31",
        fetch_fn=fetch, date_column="date", coalesce=can_coalesce,
    )

    assert probed == [("2026-04-01", "2026-08-31")]  # 只探测过一次候选跨度
    assert requested == [("2026-04-01", "2026-04-30"), ("2026-07-01", "2026-08-31")]


@pytest.mark.asyncio
async def test_greedy_packing_respects_limit_per_span():
    """贪心装箱：能并的先并满，超出上限才断开——缺口段数不等于请求次数。"""
    manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    prefill = AsyncMock(return_value=[{"date": "2026-01-15", "close": 1.0}])
    for s, e in [("2026-01-01", "2026-01-31"), ("2026-03-01", "2026-03-31"), ("2026-05-01", "2026-06-30")]:
        await manager.get_or_fetch(
            namespace="kline", key="600000", start_date=s, end_date=e,
            fetch_fn=prefill, date_column="date",
        )
    assert manager.storage.read_metadata("kline", "600000")["intervals"] == [
        ["2026-01-01", "2026-01-31"], ["2026-03-01", "2026-03-31"], ["2026-05-01", "2026-06-30"],
    ]

    requested = []

    async def fetch(s, e):
        requested.append((s, e))
        return [{"date": "2026-02-15", "close": 1.0}]

    def can_coalesce(s, e):
        # 模拟单页容量上限：跨度不得超过 100 天
        return (date.fromisoformat(e) - date.fromisoformat(s)).days <= 100

    await manager.get_or_fetch(
        namespace="kline", key="600000",
        start_date="2026-01-01", end_date="2026-08-31",
        fetch_fn=fetch, date_column="date", coalesce=can_coalesce,
    )

    # 三个缺口 [02-01,02-28] / [04-01,04-30] / [07-01,08-31]：
    # 前两个并成 [02-01, 04-30]（88 天，未超限），并入第三个会达 211 天故断开
    assert requested == [("2026-02-01", "2026-04-30"), ("2026-07-01", "2026-08-31")], requested


@pytest.mark.asyncio
async def test_without_coalesce_keeps_per_slice_fetch():
    """默认不合并：存量命名空间（基金净值/份额）行为完全不变。"""
    manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    await _prefill_two_intervals(manager)

    requested = []

    async def fetch(s, e):
        requested.append((s, e))
        return [{"date": "2026-04-15", "close": 1.0}]

    await manager.get_or_fetch(
        namespace="kline", key="510500",
        start_date="2026-02-01", end_date="2026-08-31",
        fetch_fn=fetch, date_column="date",
    )

    assert requested == [("2026-04-01", "2026-04-30"), ("2026-07-01", "2026-08-31")]
