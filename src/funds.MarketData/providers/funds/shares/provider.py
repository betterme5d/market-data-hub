# -*- coding: utf-8 -*-
"""
基金份额门面：负责时序增量缓存调度与全市场扇出（Fan-Out）持久化。
"""
import asyncio
import concurrent.futures
from datetime import date, datetime, timedelta
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from core.models import FundShare
from core.pacing import polite_delay
from core.timeseries_cache.storage import ParquetStorageEngine
from core.timeseries_cache.tracker import IntervalTracker
from providers.exchanges.shares.sse import SSE_ETF_EARLIEST_DATE, SseShareSource
from providers.exchanges.shares.szse import SzseShareSource, split_date_range

logger = logging.getLogger(__name__)


class FundShareProvider:
    """统一基金份额服务门面（集成时序 Parquet 缓存、目标基金秒级响应与全市场并发扇出落盘）。"""

    #: 小批量阈值：剩余基金 <= 该值时同步落盘（极小市场/单测），不走后台任务
    SMALL_BATCH_THRESHOLD: int = 10
    #: 默认后台扇出批次并发上限（背压）：在飞批次数、线程数与内存占用共同以此为上限
    DEFAULT_MAX_CONCURRENT_FANOUTS: int = 3

    def __init__(
        self,
        storage: Optional[ParquetStorageEngine] = None,
        tracker: Optional[IntervalTracker] = None,
        szse_source: Optional[SzseShareSource] = None,
        sse_source: Optional[SseShareSource] = None,
        max_workers: int = 16,
        max_concurrent_fanouts: int = DEFAULT_MAX_CONCURRENT_FANOUTS,
    ):
        self.storage = storage or ParquetStorageEngine()
        self.tracker = tracker or IntervalTracker()
        self.szse_source = szse_source or SzseShareSource()
        self.sse_source = sse_source or SseShareSource()
        self.max_workers = max(1, int(max_workers))
        self.max_concurrent_fanouts = max(1, int(max_concurrent_fanouts))

        # 交易所级并发锁，防止同一交易所并发批量抓取造成的资源踩踏与重复请求
        self._exchange_locks: Dict[str, asyncio.Lock] = {}
        self._lock_guard = asyncio.Lock()

        # 正在后台落盘的进行中异步任务集合
        self._pending_tasks: Set[asyncio.Task] = set()

        # 内存缓冲：记录后台正在落盘中的批次数据 {(exchange, start_date, end_date): market_data}
        # 键是**实际拉取的窗口区间**；查询时按"区间包含"匹配（见 _serve_from_in_memory_batch）
        self._in_memory_batches: Dict[Tuple[str, str, str], Dict[str, List[Dict[str, Any]]]] = {}

        # 常驻落盘线程池（惰性创建）：杜绝"每个批次自建一个 ThreadPoolExecutor"的线程爆炸
        self._fanout_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        # 全局扇出信号量（按事件循环惰性创建：模块级单例可能被多个事件循环使用）
        self._fanout_semaphore: Optional[asyncio.Semaphore] = None
        self._fanout_semaphore_loop: Optional[asyncio.AbstractEventLoop] = None

    async def _get_exchange_lock(self, exchange: str) -> asyncio.Lock:
        async with self._lock_guard:
            if exchange not in self._exchange_locks:
                self._exchange_locks[exchange] = asyncio.Lock()
            return self._exchange_locks[exchange]

    def _get_fanout_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """常驻落盘线程池单例：所有后台批次共用，线程数上限 = max_workers。"""
        if self._fanout_executor is None:
            self._fanout_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers, thread_name_prefix="share-fanout"
            )
        return self._fanout_executor

    def close(self) -> None:
        """释放常驻落盘线程池（优雅退出/单测清理用）。"""
        executor, self._fanout_executor = self._fanout_executor, None
        if executor is not None:
            executor.shutdown(wait=False)

    def _get_fanout_semaphore(self) -> asyncio.Semaphore:
        """当前事件循环上的扇出信号量：限制同时在飞的落盘批次数（背压）。"""
        loop = asyncio.get_running_loop()
        if self._fanout_semaphore is None or self._fanout_semaphore_loop is not loop:
            self._fanout_semaphore = asyncio.Semaphore(self.max_concurrent_fanouts)
            self._fanout_semaphore_loop = loop
        return self._fanout_semaphore

    async def wait_pending_fanouts(self) -> None:
        """等待所有后台进行中的扇出落盘任务完成（主要用于单测与优雅退出）。"""
        if self._pending_tasks:
            await asyncio.gather(*list(self._pending_tasks), return_exceptions=True)

    async def get_fund_shares(
        self,
        code: str,
        start_date: str,
        end_date: str,
        exchange: Optional[str] = None,
        incomplete_days: Optional[List[str]] = None,
        force: bool = False,
    ) -> List[FundShare]:
        """
        获取指定基金在 [start_date, end_date] 区间的历史份额数据。
        支持根据代码自动判定或显式指定 exchange ('szse' | 'sse')。
        优先保证目标基金在毫秒级内完成持久化并返回，全市场剩余几百只基金通过线程池后台异步扇出落盘，
        彻底杜绝 HTTP 请求超时与主事件循环阻塞。
        """
        clean_code = code.strip()
        resolved_exchange = exchange or ("sse" if clean_code.startswith("5") else "szse")
        dims = {"exchange": resolved_exchange}
        namespace = "fund_share"

        # 1. 检测目标基金当前缓存覆盖
        meta = self.storage.read_metadata(namespace, clean_code, dimensions=dims) or {}
        intervals = meta.get("intervals", [])

        # 1.5 沪市 2012-01-04 之前上游不存在任何基金份额数据（源侧常量有实测依据）：这段直接按"已覆盖"
        # 记入目标基金。否则那段窗口拉回来是空的，会被空市场防线拒绝标记，每次补录都要重走一遍这段日历。
        if resolved_exchange == "sse" and start_date < SSE_ETF_EARLIEST_DATE:
            pre_boundary_end = (
                date.fromisoformat(SSE_ETF_EARLIEST_DATE) - timedelta(days=1)
            ).isoformat()
            if self.tracker.find_missing_slices(intervals, start_date, pre_boundary_end):
                self._update_fund_interval(clean_code, start_date, pre_boundary_end, dims, namespace)
                meta = self.storage.read_metadata(namespace, clean_code, dimensions=dims) or {}
                intervals = meta.get("intervals", [])

        missing_slices = self.tracker.find_missing_slices(intervals, start_date, end_date)
        if force:
            # force=True（方案A）：忽略已覆盖区间，强制重取整个请求范围
            missing_slices = [(start_date, end_date)]

        # 2. 若存在缺失切片，在交易所锁保护下拉取并全市场扇出落盘
        if missing_slices:
            ex_lock = await self._get_exchange_lock(resolved_exchange)
            async with ex_lock:
                # 锁内二次检查防竞态
                meta = self.storage.read_metadata(namespace, clean_code, dimensions=dims) or {}
                intervals = meta.get("intervals", [])
                missing_slices = self.tracker.find_missing_slices(intervals, start_date, end_date)
                if force:
                    missing_slices = [(start_date, end_date)]

                szse_fetched_any = False
                for s_slice, e_slice in missing_slices:
                    # 2.1 先看在飞批次（后台正在落盘的全市场数据）：命中的部分直接即时落盘，
                    # 缺口随之收缩，只剩未覆盖的部分需要回源——同一缺口不会被并发请求重复拉全市场。
                    remaining_slices = self._serve_from_in_memory_batch(
                        clean_code, s_slice, e_slice, dims, namespace
                    )
                    for r_start, r_end in remaining_slices:
                        if resolved_exchange == "sse":
                            logger.info(
                                f"Fetching SSE market fund shares for [{r_start} ~ {r_end}] (triggered by {clean_code})"
                            )
                            # 沪市是逐交易日拉全市场（每交易日 2.5~4 秒），必须边拉边落盘：
                            # 否则一次客户端断开（C# 侧超时）就让整段的逐日工作全部作废。
                            # 覆盖区间也只按"实际完整拉到的窗口"标记（窗口日历连续，会合并成一整段）——
                            # 不能再按整段补记，否则某个分类接口挂掉那天会被标成已覆盖、那天永久缺失。
                            flushed = False

                            def _on_window(window, w_start, w_end):
                                nonlocal flushed
                                flushed = True
                                self._dispatch_fanout_sync(window, clean_code, w_start, w_end, dims, namespace)

                            market_data = await self.sse_source.fetch_market_shares_range(
                                r_start, r_end, on_window=_on_window,
                                incomplete_days_out=incomplete_days,
                            )
                            if not flushed and market_data:
                                # 回调一次都没触发（旧实现/测试桩）：退回"整段一次"落盘与标记
                                self._dispatch_fanout_sync(
                                    market_data, clean_code, r_start, r_end, dims, namespace
                                )
                        else:
                            # 深交所：按 <=60 天分片拉取
                            for chunk_s, chunk_e in split_date_range(r_start, r_end, max_days=60):
                                # 片内同样先消化在飞批次，只对未覆盖的部分回源
                                remaining_in_chunk = self._serve_from_in_memory_batch(
                                    clean_code, chunk_s, chunk_e, dims, namespace
                                )
                                for c_start, c_end in remaining_in_chunk:
                                    await self._fetch_szse_chunk(
                                        c_start, c_end, clean_code, dims, namespace,
                                        apply_delay=szse_fetched_any,
                                    )
                                    szse_fetched_any = True

        # 3. 从 Parquet 读取精准区间数据
        records = self.storage.read_records(
            namespace=namespace,
            key=clean_code,
            start_date=start_date,
            end_date=end_date,
            date_column="share_date",
            dimensions=dims,
        )

        # 4. 排序并构造成统一 FundShare 实体返回
        records.sort(key=lambda r: str(r.get("share_date", "")))
        return [FundShare(**r) for r in records]

    async def _fetch_szse_chunk(
        self,
        chunk_s: str,
        chunk_e: str,
        target_code: str,
        dims: Dict[str, str],
        namespace: str,
        apply_delay: bool = False,
    ) -> None:
        """拉取深市一个 <=60 天切片并扇出落盘。

        背压：先占一个扇出槽位再回源，槽位由后台批次在落盘完成后释放。
        否则多片回补时会瞬间把所有切片的全市场数据都堆在内存里（每片可达数 MB）。
        """
        if apply_delay:
            await polite_delay()  # 60 天切片之间的礼貌延时（0.2~0.5s）

        logger.info(
            f"Fetching SZSE market fund shares for chunk [{chunk_s} ~ {chunk_e}] (triggered by {target_code})"
        )
        semaphore = self._get_fanout_semaphore()
        await semaphore.acquire()
        try:
            market_data = await self.szse_source.fetch_market_shares(chunk_s, chunk_e)
        except BaseException:
            semaphore.release()
            raise
        self._dispatch_fanout_sync(
            market_data, target_code, chunk_s, chunk_e, dims, namespace,
            acquired_semaphore=semaphore,
        )

    def _serve_from_in_memory_batch(
        self, fund_code: str, start: str, end: str, dims: Dict[str, str], namespace: str
    ) -> List[Tuple[str, str]]:
        """用"在飞批次"（后台正在落盘的全市场数据）即时满足目标基金。

        在飞批次的键是**实际拉取的窗口区间**，与请求缺口边界通常不一致，
        因此这里按"批次区间包含于请求区间"匹配（旧实现拿缺口边界做等值查询，
        键口径不一致导致永远查不中，同一缺口被并发请求重复拉全市场）。

        命中部分立即写入元数据（数据已在内存，无需回源），
        返回值为**仍未覆盖**的剩余缺口；没有任何命中时返回 [(start, end)]。
        """
        hits: List[Tuple[Tuple[str, str, str], Dict[str, List[Dict[str, Any]]]]] = []
        for key, market_data in self._in_memory_batches.items():
            if key[0] != dims.get("exchange", ""):
                continue
            # ISO 日期串可直接按字典序比较（等价于时间先后）
            if key[1] >= start and key[2] <= end:
                hits.append((key, market_data))

        if not hits:
            return [(start, end)]

        hits.sort(key=lambda item: (item[0][1], item[0][2]))
        for key, market_data in hits:
            self._persist_single_fund(
                fund_code, market_data.get(fund_code, []), key[1], key[2], dims, namespace
            )

        meta = self.storage.read_metadata(namespace, fund_code, dimensions=dims) or {}
        return self.tracker.find_missing_slices(meta.get("intervals", []), start, end)

    async def _dispatch_fanout(
        self,
        market_data: Dict[str, List[Dict[str, Any]]],
        target_code: str,
        chunk_s: str,
        chunk_e: str,
        dims: Dict[str, str],
        namespace: str,
    ) -> None:
        """异步入口，语义同 _dispatch_fanout_sync（不开槽位，由后台任务自行获取）。"""
        self._dispatch_fanout_sync(market_data, target_code, chunk_s, chunk_e, dims, namespace)

    def _dispatch_fanout_sync(
        self,
        market_data: Dict[str, List[Dict[str, Any]]],
        target_code: str,
        chunk_s: str,
        chunk_e: str,
        dims: Dict[str, str],
        namespace: str,
        acquired_semaphore: Optional[asyncio.Semaphore] = None,
    ) -> None:
        """
        分发扇出落盘：
        1. 目标基金（target_code）优先在主线程极速持久化并更新元数据（~5ms），确保立刻对当前请求可用；
        2. 剩余基金：若是小规模（<= SMALL_BATCH_THRESHOLD 只，如测试），直接线程池同步完成；
           若是真实全市场（几百只），放入后台异步任务（常驻线程池并发），彻底不阻塞主事件循环与当前响应。

        本方法为**同步**实现：除 create_task 外不 await，因此也能在"请求已被取消"的
        回调路径（沪市逐交易日增量落盘）里安全调用——那种场景下任何 await 都会立刻再抛 CancelledError。

        :param acquired_semaphore: 调用方已预先占用的扇出槽位（深市分片路径为背压而预占）。
            非 None 时槽位所有权移交本方法：任一早退分支用 finally 释放，
            进入后台批次时移交给批次任务，在批次落盘完成后释放。
        """
        slot = acquired_semaphore
        try:
            # 0. 空市场防线（Empty Market Guard）：
            # 若上游接口异常或格式解析失败导致全市场数据为空，严禁写入覆盖区间，彻底杜绝缓存毒化
            if not market_data:
                logger.warning(
                    f"Empty market data received for [{chunk_s} ~ {chunk_e}] ({dims}). Skipping interval update to prevent cache poisoning."
                )
                return

            # 1. 优先持久化目标基金
            target_records = market_data.get(target_code, [])
            self._persist_single_fund(target_code, target_records, chunk_s, chunk_e, dims, namespace)

            # 2. 分离剩余基金
            remaining_items = [(k, v) for k, v in market_data.items() if k != target_code]
            if not remaining_items:
                return

            if len(remaining_items) <= self.SMALL_BATCH_THRESHOLD:
                # 小批量（如单元测试）：直接线程池同步完成，确保后续断言直接命中
                self._persist_funds_batch(
                    remaining_items, chunk_s, chunk_e, dims, namespace, max_workers=self.max_workers
                )
                return

            # 大批量全市场（如 800+ 只基金）：
            # 暂存内存缓冲字典供瞬时并发查询直接使用（键 = 实际拉取窗口区间）
            batch_key = (dims.get("exchange", ""), chunk_s, chunk_e)
            self._in_memory_batches[batch_key] = market_data

            # 提交后台异步落盘任务，多线程并发写，并在完成时清理内存缓冲
            try:
                task = asyncio.create_task(
                    self._persist_fanout_chunk_async(
                        remaining_items, chunk_s, chunk_e, dims, namespace, batch_key, slot
                    )
                )
            except BaseException:
                self._in_memory_batches.pop(batch_key, None)
                raise
            # 槽位所有权已移交后台任务（它在 finally 里释放），此处不再由本方法释放
            slot = None
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)
        finally:
            if slot is not None:
                slot.release()

    async def _persist_items(
        self,
        items: List[Tuple[str, List[Dict[str, Any]]]],
        chunk_s: str,
        chunk_e: str,
        dims: Dict[str, str],
        namespace: str,
    ) -> None:
        """把一批基金提交到常驻线程池落盘（并发上限 = max_workers，不再每批自建线程池）。"""
        if not items:
            return
        loop = asyncio.get_running_loop()
        executor = self._get_fanout_executor()
        await asyncio.gather(*(
            loop.run_in_executor(
                executor, self._persist_fund_guarded, code, records, chunk_s, chunk_e, dims, namespace
            )
            for code, records in items
        ))

    def _persist_fund_guarded(
        self,
        fund_code: str,
        records: List[Dict[str, Any]],
        chunk_s: str,
        chunk_e: str,
        dims: Dict[str, str],
        namespace: str,
    ) -> None:
        """单只基金落盘（异常不冒泡：一只失败不影响整批）。"""
        try:
            self._persist_single_fund(fund_code, records, chunk_s, chunk_e, dims, namespace)
        except Exception as err:
            logger.error(f"Failed to persist fanout fund {fund_code}: {err}")

    async def _persist_fanout_chunk_async(
        self,
        items: List[Tuple[str, List[Dict[str, Any]]]],
        chunk_s: str,
        chunk_e: str,
        dims: Dict[str, str],
        namespace: str,
        batch_key: Tuple[str, str, str],
        acquired_semaphore: Optional[asyncio.Semaphore] = None,
    ) -> None:
        """后台异步任务：在常驻线程池中并发落盘剩余基金，避免阻塞主事件循环。

        背压：未预先持有槽位时，先排队等一个扇出槽位再开写——在飞批次数、
        线程数与常驻内存（每个批次一份全市场数据）都因此有明确上限。
        """
        acquired = False
        semaphore: Optional[asyncio.Semaphore] = None
        try:
            if acquired_semaphore is not None:
                semaphore, acquired = acquired_semaphore, True
            else:
                semaphore = self._get_fanout_semaphore()
                await semaphore.acquire()
                acquired = True
            await self._persist_items(items, chunk_s, chunk_e, dims, namespace)
        except Exception as e:
            logger.error(f"Background fan-out persistence error for {batch_key}: {e}")
        finally:
            if acquired and semaphore is not None:
                semaphore.release()
            self._in_memory_batches.pop(batch_key, None)

    def _persist_funds_batch(
        self,
        items: List[Tuple[str, List[Dict[str, Any]]]],
        chunk_s: str,
        chunk_e: str,
        dims: Dict[str, str],
        namespace: str,
        max_workers: int = 16,
    ) -> None:
        """使用线程池并发落盘多只基金（仅用于小批量同步路径）。"""
        if not items:
            return

        def worker(item: Tuple[str, List[Dict[str, Any]]]) -> None:
            fund_code, records = item
            try:
                self._persist_single_fund(fund_code, records, chunk_s, chunk_e, dims, namespace)
            except Exception as err:
                logger.error(f"Failed to persist fanout fund {fund_code}: {err}")

        workers = min(max_workers, len(items))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(worker, items))

    def _persist_single_fund(
        self,
        fund_code: str,
        records: List[Dict[str, Any]],
        chunk_s: str,
        chunk_e: str,
        dims: Dict[str, str],
        namespace: str,
    ) -> None:
        """单只基金的数据落盘与覆盖区间元数据更新。"""
        if records:
            self.storage.write_records(
                namespace=namespace,
                key=fund_code,
                records=records,
                date_column="share_date",
                dimensions=dims,
            )
        self._update_fund_interval(fund_code, chunk_s, chunk_e, dims, namespace)

    def _persist_fanout_chunk(
        self,
        market_data: Dict[str, List[Dict[str, Any]]],
        chunk_s: str,
        chunk_e: str,
        dims: Dict[str, str],
        namespace: str,
    ) -> None:
        """兼容旧版接口：全量扇出落盘。"""
        items = list(market_data.items())
        self._persist_funds_batch(items, chunk_s, chunk_e, dims, namespace, max_workers=self.max_workers)

    def _update_fund_interval(
        self,
        fund_code: str,
        chunk_s: str,
        chunk_e: str,
        dims: Dict[str, str],
        namespace: str,
    ) -> None:
        """更新单只基金的覆盖区间元数据。

        必须走 storage.update_metadata（读-改-写全程持锁）：扇出后台任务与后续请求会并发更新
        同一只基金，自己 read+write 会「读旧值 → 各自合并 → 后写覆盖先写」而丢区间。
        """

        def _mutate(meta: Dict[str, Any]) -> Dict[str, Any]:
            meta["key"] = fund_code
            meta["namespace"] = namespace
            meta["dimensions"] = dims
            meta["intervals"] = self.tracker.merge_intervals(
                meta.get("intervals", []), (chunk_s, chunk_e)
            )
            meta["date_column"] = "share_date"
            meta["updated_at"] = datetime.now().isoformat()
            return meta

        self.storage.update_metadata(namespace, fund_code, _mutate, dimensions=dims)


# 全局单例
fund_share_provider = FundShareProvider()
