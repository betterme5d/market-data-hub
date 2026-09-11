# -*- coding: utf-8 -*-
"""
K 线业务门面：统一代码归一化、多数据源路由、Parquet 时序缓存（按 source+adjust+period 三维隔离）。
参照 FundNavProvider / FundShareProvider 规范实现。
"""
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

from core.bar_estimator import MAX_PAGE_SIZE, estimate_bar_count
from core.models import KLineBar, KLineResponse
from core.symbol_normalizer import resolve_symbol_identity
from core.timeseries_cache.manager import TimeSeriesCacheManager

logger = logging.getLogger(__name__)

DEFAULT_SOURCE = "xueqiu"
SUPPORTED_SOURCES = {"xueqiu", "tencent", "sina"}

ADJUST_MAP = {
    "qfq": "before",    # 前复权
    "hfq": "after",     # 后复权
    "none": "normal",   # 不复权
    "normal": "normal",
    "before": "before",
    "after": "after",
}
PERIOD_MAP = {
    "day": "day",
    "1d": "day",
    "week": "week",
    "1wk": "week",
    "month": "month",
    "1mo": "month",
}


class KLineProvider:
    """
    K 线统一业务门面。
    - 多数据源：xueqiu / tencent / sina
    - 复权类型：qfq / hfq / none 严格按 dimensions 物理隔离 Parquet 缓存
    - 对外统一接收标准代码（002092.SZ 等），内部自动转换各源专属格式
    """

    def __init__(
        self,
        base_dir: str | Path = "data/cache",
        cache_manager: Optional[TimeSeriesCacheManager] = None,
    ):
        self.cache_manager = cache_manager or TimeSeriesCacheManager(base_dir=base_dir)

    async def get_kline(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        *,
        source: Optional[str] = None,
        adjust: str = "none",
        period: str = "day",
    ) -> KLineResponse:
        """
        获取 K 线历史数据，带 Parquet 时序增量缓存。

        Args:
            symbol:     证券代码，系统标准码（002092.SZ / 510300.SH / AAPL.US / 00700.HK）或
                        数据源原生符号（HKHSI / CSI930875 / .SPGSCL / HKDCNY.FX）均可
            start_date: 起始日期 YYYY-MM-DD（必填）
            end_date:   结束日期 YYYY-MM-DD（必填）
            source:     数据源，默认 xueqiu；支持 xueqiu / tencent / sina
            adjust:     复权类型：none（默认，不复权）/ hfq（后复权）/ qfq（前复权）。
                        **默认 none 而非 qfq**：前复权值随最新价重算，跨除权日把不同时间抓的
                        窗口拼在一起会出现台阶，与增量缓存天然相冲，故不设为默认；
                        确需 qfq 时请显式传入并自行承担口径漂移风险。
            period:     K 线周期：day / week / month

        Raises:
            ValueError: symbol 形态非法、数据源不支持、日期顺序错误
        """
        # ---- 入参校验 ----
        standard_code = resolve_symbol_identity(symbol)

        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        resolved_source = (source or DEFAULT_SOURCE).strip().lower()
        if resolved_source not in SUPPORTED_SOURCES:
            raise ValueError(
                f"unsupported source: {resolved_source!r}. Supported: {sorted(SUPPORTED_SOURCES)}"
            )

        resolved_adjust = adjust.strip().lower()
        if resolved_adjust not in ADJUST_MAP:
            raise ValueError(f"unsupported adjust: {resolved_adjust!r}")

        resolved_period = PERIOD_MAP.get(period.strip().lower(), "day")

        # ---- 严格三维物理隔离维度 ----
        dimensions = {
            "source": resolved_source,
            "adjust": resolved_adjust,
            "period": resolved_period,
        }

        async def fetch_fn(
            s_slice: str,
            e_slice: str,
            on_chunk: Optional[Callable] = None,
        ) -> List[Dict[str, Any]]:
            return await self._fetch_from_source(
                standard_code=standard_code,
                source=resolved_source,
                adjust=ADJUST_MAP[resolved_adjust],
                period=resolved_period,
                start_date=s_slice,
                end_date=e_slice,
                on_chunk=on_chunk,
            )

        records = await self.cache_manager.get_or_fetch(
            namespace="kline",
            key=standard_code,
            start_date=start_date,
            end_date=end_date,
            fetch_fn=fetch_fn,
            date_column="date",
            dimensions=dimensions,
            # 缺口合并：跨度装得下一页就只发一次上游请求（请求次数是风控敏感资源）
            coalesce=lambda s, e: estimate_bar_count(s, e, resolved_period) <= MAX_PAGE_SIZE,
        )

        items = [KLineBar(**r) for r in records]
        return KLineResponse(
            code=standard_code,
            source=resolved_source,
            period=resolved_period,
            adjust=resolved_adjust,
            count=len(items),
            items=items,
        )

    async def _fetch_from_source(
        self,
        standard_code: str,
        source: str,
        adjust: str,    # 雪球原生参数: before/after/normal
        period: str,
        start_date: str,
        end_date: str,
        on_chunk: Optional[Callable] = None,
    ) -> List[Dict[str, Any]]:
        """
        路由至对应数据源底层实现。
        在测试中通过 patch.object 整体 mock 此方法。
        生产环境中各 Source 类通过此处路由调用。
        """
        if source == "xueqiu":
            from providers.quotes.xueqiu import XueqiuProvider, kline_guard

            xq = XueqiuProvider(cache_manager=self.cache_manager)
            async with kline_guard():
                return await xq._fetch_kline_slice(
                    symbol=standard_code,
                    slice_start=start_date,
                    slice_end=end_date,
                    period_str=period,
                    adjust_type=adjust,
                    on_chunk=on_chunk,
                )
        # 未来扩展: tencent / sina source 实现插入此处
        raise NotImplementedError(f"Source {source!r} not yet implemented for kline fetching")
