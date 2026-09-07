# -*- coding: utf-8 -*-
"""
时序增量缓存系统门面：负责并发协程锁、增量切片调度、T日未发布延迟保护与落盘协同。
"""
import asyncio
import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

from core.timeseries_cache.storage import ParquetStorageEngine
from core.timeseries_cache.tracker import IntervalTracker

logger = logging.getLogger(__name__)


class TimeSeriesCacheManager:
    """通用时序增量缓存管理器。"""

    def __init__(
        self,
        base_dir: str | Path = "data/cache",
        pending_today_ttl: int = 600,
        storage: Optional[ParquetStorageEngine] = None,
        tracker: Optional[IntervalTracker] = None,
    ):
        self.storage = storage or ParquetStorageEngine(base_dir=base_dir)
        self.tracker = tracker or IntervalTracker()
        self.pending_today_ttl = pending_today_ttl

        # 标的级并发协程锁
        self._locks: Dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

        # T日未发布状态内存微缓存: lock_key -> timestamp
        self._pending_today: Dict[str, float] = {}

    def _make_key(
        self, namespace: str, key: str, dimensions: Optional[Dict[str, str]] = None
    ) -> str:
        dim_str = ",".join(f"{k}={v}" for k, v in sorted(dimensions.items())) if dimensions else ""
        return f"{namespace}:{dim_str}:{key}"

    async def _get_lock(self, lock_key: str) -> asyncio.Lock:
        async with self._locks_guard:
            if lock_key not in self._locks:
                self._locks[lock_key] = asyncio.Lock()
            return self._locks[lock_key]

    def _is_today_pending(self, lock_key: str) -> bool:
        ts = self._pending_today.get(lock_key)
        if ts and (time.time() - ts < self.pending_today_ttl):
            return True
        return False

    def _mark_today_pending(self, lock_key: str) -> None:
        self._pending_today[lock_key] = time.time()

    async def get_or_fetch(
        self,
        namespace: str,
        key: str,
        start_date: str,
        end_date: str,
        fetch_fn: Callable[[str, str], Awaitable[List[Dict[str, Any]]]],
        date_column: str = "date",
        dimensions: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        核心读取/增量抓取方法：
        1. 获取细粒度协程锁；
        2. 读取 meta.json 算出缺失切片；
        3. 对缺失切片调用 fetch_fn 拉取；
        4. 应用 T日未发布保护，将数据写入 Parquet 并更新 intervals；
        5. 返回请求日期范围内的全部记录。
        """
        lock_key = self._make_key(namespace, key, dimensions)
        lock = await self._get_lock(lock_key)

        async with lock:
            meta = self.storage.read_metadata(namespace, key, dimensions) or {}
            intervals = meta.get("intervals", [])

            raw_missing = self.tracker.find_missing_slices(intervals, start_date, end_date)

            today_str = date.today().strftime("%Y-%m-%d")
            yesterday_str = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

            # 过滤掉近期已探测过但未发布的今日切片
            effective_missing = []
            for s_slice, e_slice in raw_missing:
                if s_slice == today_str and e_slice == today_str and self._is_today_pending(lock_key):
                    logger.debug(f"{lock_key} today is pending within TTL, skipping fetch for today")
                    continue
                effective_missing.append((s_slice, e_slice))

            if effective_missing:
                new_records: List[Dict[str, Any]] = []
                new_intervals_to_merge = []

                for s_slice, e_slice in effective_missing:
                    chunk = await fetch_fn(s_slice, e_slice)
                    if chunk:
                        new_records.extend(chunk)

                    # 检查 chunk 中是否包含今天的记录
                    has_today = any(str(r.get(date_column)) == today_str for r in (chunk or []))

                    # 如果当前切片延伸到了今天，但上游并没有返回今天的有效数据
                    if e_slice >= today_str and not has_today:
                        self._mark_today_pending(lock_key)
                        # 仅把昨天及更早的部分作为已覆盖区间合并
                        if s_slice <= yesterday_str:
                            new_intervals_to_merge.append((s_slice, min(e_slice, yesterday_str)))
                    else:
                        new_intervals_to_merge.append((s_slice, e_slice))

                if new_records:
                    row_count = self.storage.write_records(
                        namespace, key, new_records, date_column=date_column, dimensions=dimensions
                    )
                else:
                    row_count = meta.get("row_count", 0)

                for it in new_intervals_to_merge:
                    intervals = self.tracker.merge_intervals(intervals, it)

                meta["key"] = key
                meta["namespace"] = namespace
                if dimensions:
                    meta["dimensions"] = dimensions
                meta["intervals"] = intervals
                meta["row_count"] = row_count
                meta["date_column"] = date_column
                meta["updated_at"] = datetime.now().isoformat()
                self.storage.write_metadata(namespace, key, meta, dimensions=dimensions)

            return self.storage.read_records(
                namespace,
                key,
                start_date=start_date,
                end_date=end_date,
                date_column=date_column,
                dimensions=dimensions,
            )
