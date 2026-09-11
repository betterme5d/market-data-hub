# -*- coding: utf-8 -*-
"""
通用时序切片分页爬取器 (PaginatedSliceCrawler)：
统一封装时序接口抓取时的以下共性能力：
1. 自动分页循环与终止判定（total_count 达标 / 返回条数小于 pageSize / 空列表）
2. 防反爬随机抖动延时（翻页间休眠）
3. 单页异常指数退避重试（带随机抖动与最大重试次数）
4. 智能推导单页覆盖区间（支持倒序 'desc' 与正序 'asc'，支持 T 日保护上界剪裁）
5. 逐页触发流式回调 on_page(records, cov_start, cov_end)，支持即时落盘与断点续查
6. 最终结果按日期升序统一返回
"""
import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import date, timedelta
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    List,
    Literal,
    Optional,
    TypeVar,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class PageBatch(Generic[T]):
    """单页拉取结果载体。"""

    items: List[T]
    total_count: Optional[int] = None


class PaginatedSliceCrawler:
    """通用时序切片分页爬取器。"""

    def __init__(
        self,
        min_delay: float = 0.2,
        max_delay: float = 0.4,
        max_retries: int = 3,
        retry_base_delay: float = 0.2,
    ):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay

    async def crawl_slice(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        page_size: int,
        fetch_page_fn: Callable[[int, int, Optional[str], Optional[str]], Awaitable[PageBatch[T]]],
        date_getter: Callable[[T], str],
        order: Literal["desc", "asc"] = "desc",
        start_page: int = 1,
        on_page: Optional[Callable[[List[T], str, str], Awaitable[None]]] = None,
    ) -> List[T]:
        """
        分页拉取一个时间切片 [start_date, end_date] 的所有数据。

        :param start_date: 切片起始日期 (YYYY-MM-DD)
        :param end_date: 切片截止日期 (YYYY-MM-DD)
        :param page_size: 单页大小 (pageSize)
        :param fetch_page_fn: 单页拉取函数 (page_index, page_size, start_date, end_date) -> PageBatch
        :param date_getter: 从记录对象中获取日期字符串的函数 (如 lambda x: x.nav_date)
        :param order: 上游返回的记录日期排序方向，"desc" (如东财，最新优先) 或 "asc" (最旧优先)
        :param start_page: 起始页码，默认 1
        :param on_page: 逐页成功回调 (page_items, cov_start, cov_end)
        :return: 汇总后的所有记录，按日期升序排列
        """
        today_str = date.today().strftime("%Y-%m-%d")
        yesterday_str = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

        effective_upper = end_date
        effective_lower = start_date

        items: List[T] = []
        page_index = start_page
        seen = 0
        total: Optional[int] = None
        # 页数兜底：上游一直返回非空短页（限流抖动）时不能无限翻页
        max_pages = 2000
        # 总数缺失时的"疑似末页"挂起槽：(页数据, 页码)。
        # 短页只能算疑似末页，必须再取一页确认（空页=确认）；否则上游静默封顶（限流/接口变更
        # 只回固定条数）会被当成末页，末页规则把覆盖区间一路标到请求边界，缺口被永久固化成"已覆盖"。
        pending: Optional[tuple] = None

        async def _emit(page_items: List[T], page_number: int, is_last_page: bool) -> None:
            """推导本页覆盖区间并触发 on_page 回调（desc/asc 两套规则）。"""
            nonlocal effective_upper
            if on_page is None or not page_items:
                return
            if order == "desc":
                # 倒序：第一页包含最新的日期，检查 T 日是否已出
                if page_number == start_page and effective_upper and effective_upper >= today_str:
                    has_today = any(date_getter(r) == today_str for r in page_items)
                    if not has_today:
                        effective_upper = min(effective_upper, yesterday_str)

                page_min_date = min(date_getter(r) for r in page_items)
                # 最后一页（最旧一页）才把覆盖下界延到切片起始日期
                cov_s = effective_lower if (is_last_page and effective_lower) else page_min_date
                cov_e = effective_upper or max(date_getter(r) for r in page_items)
            else:  # order == "asc"
                page_max_date = max(date_getter(r) for r in page_items)
                if is_last_page and effective_upper and effective_upper >= today_str:
                    has_today = any(date_getter(r) == today_str for r in page_items)
                    if not has_today:
                        effective_upper = min(effective_upper, yesterday_str)

                page_min_date = min(date_getter(r) for r in page_items)
                # 正序：只有第一页（最旧一页）才把覆盖下界延到切片起始日期。
                # 中途页若也延到请求起点，会把 [请求起点, 本页最大日期] 整段标成"已覆盖"，
                # 上游跳页/静默封顶造成的空洞就被永久固化了。
                cov_s = effective_lower if (page_number == start_page and effective_lower) else page_min_date
                cov_e = effective_upper if (is_last_page and effective_upper) else page_max_date

            if cov_s <= cov_e:
                await on_page(page_items, cov_s, cov_e)

        while True:
            # 翻页抖动延时（从第二页开始触发）
            if page_index > start_page and self.max_delay > 0:
                await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))

            # 单页指数退避重试
            batch: Optional[PageBatch[T]] = None
            for attempt in range(1, self.max_retries + 1):
                try:
                    batch = await fetch_page_fn(page_index, page_size, start_date, end_date)
                    if batch is None:
                        raise RuntimeError(f"Fetch page {page_index} returned None")
                    break
                except Exception as e:
                    if attempt < self.max_retries:
                        backoff = self.retry_base_delay * (2 ** (attempt - 1)) + random.uniform(0.05, 0.15)
                        logger.warning(
                            f"Fetch page {page_index} attempt {attempt}/{self.max_retries} failed: {e}, retrying in {backoff:.2f}s..."
                        )
                        await asyncio.sleep(backoff)
                    else:
                        logger.error(f"Fetch page {page_index} exhausted all {self.max_retries} retries: {e}")
                        raise

            if batch is None:
                raise RuntimeError(f"Fetch page {page_index} returned None")

            if total is None and batch.total_count is not None:
                total = batch.total_count

            page_items = batch.items

            # 先结清上一页的"疑似末页"：本页为空 → 上一页确实是末页（覆盖区间可延到请求边界）；
            # 本页有数据 → 上一页只是短页，按普通页结清（覆盖区间只到它自己的日期范围）。
            if pending is not None:
                pending_items, pending_page = pending
                pending = None
                if not page_items:
                    await _emit(pending_items, pending_page, is_last_page=True)
                    break
                await _emit(pending_items, pending_page, is_last_page=False)

            items.extend(page_items)
            seen += len(page_items)

            # 末页判定：
            # - 上游给了总数（>0）就以总数为准——中途短页可能是限流/抖动，若按"短页即末页"提前收尾，
            #   末页规则会把覆盖区间一路标到请求起点，缺口被永久标成"已覆盖"；
            # - 总数缺失时空页直接收尾；短页只算"疑似末页"，靠下一页确认（见上面的 pending 结清）；
            # - 页数不超过总数推导值 +5（应对偶发短页）且不超过 max_pages。
            total_known = total is not None and total > 0
            is_last = (not page_items) or (total_known and seen >= total)
            page_cap = max_pages
            if total_known:
                page_cap = min(max_pages, (total + page_size - 1) // page_size + 5)
            if page_index - start_page + 1 >= page_cap:
                logger.warning(
                    f"crawl_slice reached page cap {page_cap} (total={total}, seen={seen}); stopping pagination"
                )
                is_last = True

            if is_last:
                await _emit(page_items, page_index, is_last_page=True)
                break

            if page_items and not total_known and len(page_items) < page_size:
                # 疑似末页：挂起，等下一页确认后再按末页规则结清覆盖区间
                pending = (page_items, page_index)
            else:
                await _emit(page_items, page_index, is_last_page=False)

            page_index += 1
        # 统一按日期升序排序
        items.sort(key=date_getter)
        return items
