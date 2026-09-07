# -*- coding: utf-8 -*-
"""
基金净值业务门面（Provider）：
负责协调数据源调度（EastmoneySource / CmtidpSource）与时序增量持久化缓存（TimeSeriesCacheManager）。
"""
import inspect
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from core.models import FundNav
from core.timeseries_cache.manager import TimeSeriesCacheManager
from providers.funds.base_nav import FundNavSource
from providers.funds.cmtidp import CmtidpSource
from providers.funds.eastmoney import EastmoneySource

logger = logging.getLogger(__name__)


class FundNavProvider:
    """基金净值业务门面。"""

    def __init__(
        self,
        cache_manager: Optional[TimeSeriesCacheManager] = None,
        eastmoney_source: Optional[EastmoneySource] = None,
        cmtidp_source: Optional[CmtidpSource] = None,
    ):
        self.cache_manager = cache_manager or TimeSeriesCacheManager()
        self.eastmoney_source = eastmoney_source or EastmoneySource()
        self.cmtidp_source = cmtidp_source or CmtidpSource()

        self._sources: Dict[str, FundNavSource] = {
            "eastmoney": self.eastmoney_source,
            "cmtidp": self.cmtidp_source,
        }

    def _resolve_source(self, source: Optional[str] = None) -> Tuple[str, FundNavSource]:
        src_name = (source or "eastmoney").strip().lower()
        src_obj = self._sources.get(src_name)
        if not src_obj:
            raise ValueError(f"Unknown source: {source}. Supported sources: {list(self._sources.keys())}")
        return src_name, src_obj

    async def get_fund_nav_history(
        self,
        code: str,
        start_date: str,
        end_date: str,
        source: Optional[str] = "eastmoney",
    ) -> List[FundNav]:
        """
        获取单只基金在 [start_date, end_date] 闭区间内的历史净值（带持久增量缓存）。
        """
        norm_code = code.strip()
        src_name, src_obj = self._resolve_source(source)

        async def _fetch_slice(
            s: str,
            e: str,
            on_chunk: Optional[Callable[[List[Dict[str, Any]], str, str], Awaitable[None]]] = None,
        ) -> List[Dict[str, Any]]:
            # 定义逐页流式回调适配器
            async def _on_page_cb(page_items: List[FundNav], cov_s: str, cov_e: str) -> None:
                if on_chunk is not None:
                    page_dicts = [item.model_dump() for item in page_items]
                    await on_chunk(page_dicts, cov_s, cov_e)

            sig = inspect.signature(src_obj.get_fund_nav_history)
            if "on_page" in sig.parameters:
                items = await src_obj.get_fund_nav_history(
                    norm_code, start_date=s, end_date=e, on_page=_on_page_cb
                )
            else:
                items = await src_obj.get_fund_nav_history(norm_code, start_date=s, end_date=e)

            return [item.model_dump() for item in items]

        records = await self.cache_manager.get_or_fetch(
            namespace="fund_nav",
            key=norm_code,
            start_date=start_date,
            end_date=end_date,
            fetch_fn=_fetch_slice,
            date_column="nav_date",
            dimensions={"source": src_name},
        )

        return [
            FundNav(
                code=r.get("code") or norm_code,
                nav_date=r.get("nav_date"),
                unit_nav=r.get("unit_nav"),
                accum_nav=r.get("accum_nav"),
                daily_return=r.get("daily_return"),
                subscribe_status=r.get("subscribe_status"),
                redeem_status=r.get("redeem_status"),
                dividend=r.get("dividend"),
            )
            for r in records
        ]

    async def get_latest_all_nav(self, source: Optional[str] = "eastmoney") -> List[FundNav]:
        """
        获取全量基金最新一期净值（直接透传底层源）。
        """
        _, src_obj = self._resolve_source(source)
        return await src_obj.get_latest_all_nav()
