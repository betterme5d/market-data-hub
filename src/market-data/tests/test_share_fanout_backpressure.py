# -*- coding: utf-8 -*-
"""份额扇出背压与在飞批次复用（D15/D16）回归用例。

D15：全市场扇出此前是"每个窗口一个后台任务 + 每个任务内再自建 16 线程池"，
      10 年回补会同时唤起上百个落盘线程；且每个未完成的批次都常驻一份全市场数据，
      积压随回补区间线性增长（深市 60 天/片时可到数百 MB）。
D16：`_in_memory_batches` 写入键是"实际拉取窗口"、查询键是"缺失缺口"，两者口径不一致，
      在飞批次永远查不中，同一缺口被并发请求重复拉全市场。

覆盖的验收标准：
1. 同一缺口的并发请求只触发一次全市场拉取（在飞批次直接服务第二个基金）；
2. 在飞批次数与落盘线程数都有明确上限（背压），不再随回补区间线性增长。
"""
import threading
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

from core.timeseries_cache.storage import ParquetStorageEngine
from core.timeseries_cache.tracker import IntervalTracker
from providers.funds.shares.provider import FundShareProvider
from providers.exchanges.shares.sse import SseShareSource
from providers.exchanges.shares.szse import SzseShareSource

SSE_DAYS = ["2026-06-01", "2026-06-02"]


def _shutdown(provider: FundShareProvider) -> None:
    """释放常驻落盘线程池（旧实现无 close，用 getattr 兼容，保证红灯时失败在断言上）。"""
    close = getattr(provider, "close", None)
    if close is not None:
        close()


def _sse_market(day: str, extra: int = 12) -> dict:
    """构造某个交易日的全市场快照：1 只目标基金 + extra 只其它基金（>10 触发后台扇出）。"""
    market = {
        "510300": [
            {
                "code": "510300",
                "share_date": day,
                "shares": 100.0,
                "raw_shares": 1000000.0,
                "name": "300ETF",
            }
        ]
    }
    for i in range(extra):
        code = f"51{i:04d}"
        market[code] = [
            {
                "code": code,
                "share_date": day,
                "shares": float(i),
                "raw_shares": 1.0,
                "name": code,
            }
        ]
    return market


def _sse_source() -> AsyncMock:
    """沪市源桩：逐交易日回调本窗口数据（与真实源 on_window 的语义一致）。"""
    source = AsyncMock(spec=SseShareSource)

    async def fake_range(start, end, on_window=None, incomplete_days_out=None, **kwargs):
        for day in SSE_DAYS:
            if on_window is not None:
                on_window(_sse_market(day), day, day)
        return {}

    source.fetch_market_shares_range.side_effect = fake_range
    return source


async def test_inflight_batch_serves_concurrent_request_without_refetch(tmp_path: Path):
    """D16：后台批次仍在落盘时，第二个基金的请求必须直接吃在飞数据，不得重复拉全市场。"""
    storage = ParquetStorageEngine(base_dir=tmp_path)
    source = _sse_source()
    provider = FundShareProvider(
        storage=storage, tracker=IntervalTracker(), sse_source=source
    )

    gate = threading.Event()
    main_thread = threading.current_thread()
    real_persist = provider._persist_single_fund

    def gated_persist(code, records, s, e, dims, namespace):
        # 只卡住"后台线程里对非目标基金"的落盘：前台（事件循环线程）路径照常写盘，
        # 于是第一个请求返回时在飞批次仍在内存中（模拟落盘慢于后续请求到达）。
        if threading.current_thread() is not main_thread and code != "510300":
            gate.wait(5)
        return real_persist(code, records, s, e, dims, namespace)

    try:
        with patch.object(provider, "_persist_single_fund", new=gated_persist):
            res1 = await provider.get_fund_shares("510300", "2026-06-01", "2026-06-02")
            assert [r.code for r in res1] == ["510300", "510300"]
            assert source.fetch_market_shares_range.call_count == 1

            res2 = await provider.get_fund_shares("510005", "2026-06-01", "2026-06-02")
            assert [r.share_date for r in res2] == SSE_DAYS
            assert source.fetch_market_shares_range.call_count == 1, (
                "在飞批次已持有该窗口全市场数据，第二个请求不应再次触发全市场拉取"
            )
    finally:
        gate.set()
        await provider.wait_pending_fanouts()
        _shutdown(provider)


def _szse_chunk_market(share_date: str, target: str = "159901", extra: int = 20) -> dict:
    market = {
        target: [
            {
                "code": target,
                "share_date": share_date,
                "shares": 999.0,
                "raw_shares": 9990000.0,
                "name": "深证100",
            }
        ]
    }
    for i in range(extra):
        code = f"159{i:03d}"
        market[code] = [
            {
                "code": code,
                "share_date": share_date,
                "shares": float(i),
                "raw_shares": 1.0,
                "name": code,
            }
        ]
    return market


async def test_szse_fanout_backlog_and_threads_are_bounded(tmp_path: Path):
    """D15：深市多片回补时，在飞批次数与落盘线程数都必须有上限（背压）。"""
    storage = ParquetStorageEngine(base_dir=tmp_path)
    source = AsyncMock(spec=SzseShareSource)
    provider = FundShareProvider(
        storage=storage,
        tracker=IntervalTracker(),
        szse_source=source,
        max_workers=4,
        max_concurrent_fanouts=2,
    )

    # 每片开拉前采样一次"在飞批次"数量：这是内存积压的直接观测量
    backlog_samples = []

    async def fake_fetch(start, end):
        backlog_samples.append(len(provider._in_memory_batches))
        return _szse_chunk_market(end)

    source.fetch_market_shares.side_effect = fake_fetch

    counter = {"now": 0, "peak": 0}
    lock = threading.Lock()
    real_persist = provider._persist_single_fund

    def slow_persist(code, records, s, e, dims, namespace):
        with lock:
            counter["now"] += 1
            counter["peak"] = max(counter["peak"], counter["now"])
        try:
            time.sleep(0.05)
            return real_persist(code, records, s, e, dims, namespace)
        finally:
            with lock:
                counter["now"] -= 1

    try:
        with patch.object(provider, "_persist_single_fund", new=slow_persist):
            await provider.get_fund_shares("159901", "2026-01-01", "2026-12-31")
            await provider.wait_pending_fanouts()
    finally:
        _shutdown(provider)

    assert len(backlog_samples) > 3, "用例前提：区间必须被切成多片（每片一次上游请求）"
    assert max(backlog_samples) <= 2, (
        f"在飞扇出批次积压峰值 {max(backlog_samples)}，超过背压上限 2（内存随回补区间线性增长）"
    )
    # 常驻线程池 4 + 事件循环线程上的目标基金落盘 1 = 5 为硬上限
    assert counter["peak"] <= 5, f"落盘并发峰值 {counter['peak']}，超过常驻线程池上限"


# ---------------------------------------------------------------------------
# N8：沪市逐窗口扇出的「待落盘队列」必须有上限
# ---------------------------------------------------------------------------


def _sse_windows(count: int = 20):
    """构造 count 个互不相同的窗口（等价于沪市逐交易日/逐窗口的 on_window 回调）。"""
    return [f"2026-06-{d:02d}" for d in range(1, count + 1)]


async def test_sse_fanout_pending_queue_is_bounded_by_default(tmp_path: Path):
    """N8：沪市 20 个窗口连续扇出时，待落盘队列不得超过默认上限。

    每个排队中的批次都持有一份「整窗全市场快照」；旧实现只限制「同时落盘」的批次数
    （max_concurrent_fanouts），不限制「已排队未落盘」的任务数，因此 10 年回补
    （约 240 个窗口）会让数百 MB 常驻内存。
    """
    from providers.funds.shares import provider as provider_mod

    bound = getattr(provider_mod, "DEFAULT_MAX_PENDING_FANOUTS", 6)

    storage = ParquetStorageEngine(base_dir=tmp_path)
    provider = FundShareProvider(
        storage=storage,
        tracker=IntervalTracker(),
        max_workers=2,
        max_concurrent_fanouts=1,
    )
    dims = {"exchange": "sse"}

    samples = []
    try:
        for day in _sse_windows(20):
            provider._dispatch_fanout_sync(
                _sse_market(day), "510300", day, day, dims, "fund_share"
            )
            samples.append(len(provider._in_memory_batches))
        await provider.wait_pending_fanouts()
    finally:
        _shutdown(provider)

    assert samples, "用例前提：至少扇出一个窗口"
    assert max(samples) <= bound, (
        f"待落盘队列峰值 {max(samples)}，超过上限 {bound}（内存随回补区间线性增长）"
    )


async def test_sse_fanout_backpressure_does_not_lose_data(tmp_path: Path):
    """N8 配套：队列满时改为同步落盘，数据一条都不能丢（背压 ≠ 丢弃）。"""
    storage = ParquetStorageEngine(base_dir=tmp_path)
    provider = FundShareProvider(
        storage=storage,
        tracker=IntervalTracker(),
        max_workers=2,
        max_concurrent_fanouts=1,
        max_pending_fanouts=2,
    )
    dims = {"exchange": "sse"}
    windows = _sse_windows(6)

    try:
        for day in windows:
            provider._dispatch_fanout_sync(
                _sse_market(day), "510300", day, day, dims, "fund_share"
            )
        await provider.wait_pending_fanouts()
    finally:
        _shutdown(provider)

    # 目标基金 + 全市场其余基金、每个窗口，都必须落盘
    for day in windows:
        for code in ("510300", "510000", "510011"):
            records = storage.read_records(
                "fund_share", code, start_date=day, end_date=day,
                date_column="share_date", dimensions=dims,
            )
            assert records, f"{code} 在 {day} 的数据未落盘（背压路径丢了数据）"

    # 覆盖区间也必须被标记，否则下次会重复回源
    meta = storage.read_metadata("fund_share", "510011", dimensions=dims) or {}
    assert meta.get("intervals"), "背压路径未更新覆盖区间元数据"


async def test_fanout_quota_is_returned_after_completion(tmp_path: Path):
    """N8 配套：队列配额必须归还，否则队列会假性占满、后续窗口永久退化为同步落盘。"""
    storage = ParquetStorageEngine(base_dir=tmp_path)
    provider = FundShareProvider(
        storage=storage,
        tracker=IntervalTracker(),
        max_workers=2,
        max_concurrent_fanouts=1,
        max_pending_fanouts=2,
    )
    dims = {"exchange": "sse"}

    try:
        for day in _sse_windows(4):
            provider._dispatch_fanout_sync(
                _sse_market(day), "510300", day, day, dims, "fund_share"
            )
        await provider.wait_pending_fanouts()

        assert provider._pending_fanout_count == 0, "后台任务完成后配额未归还"
        assert provider._in_memory_batches == {}, "在飞批次缓冲未清理"

        # 队列已腾空 → 新窗口必须重新走「后台异步落盘」，而不是永久退化到同步落盘
        provider._dispatch_fanout_sync(
            _sse_market("2026-07-01"), "510300", "2026-07-01", "2026-07-01", dims, "fund_share"
        )
        assert len(provider._in_memory_batches) == 1
        await provider.wait_pending_fanouts()
    finally:
        _shutdown(provider)
