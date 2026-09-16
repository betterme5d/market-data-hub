# -*- coding: utf-8 -*-
"""
统一基金净值 Source 抽象。

不同数据源（cmtidp / eastmoney 等）实现同一契约。
两个能力：
- get_latest_all_nav：获取最新一期全量基金净值（各源"最新"语义，不承诺指定日期）
- get_fund_nav_history：获取单只基金在日期区间内的历史净值

分页循环在 Source 内部消化，对外一次性返回标准 FundNav 列表。
"""
from abc import ABC, abstractmethod
from typing import Awaitable, Callable, List, Optional

from core.models import FundNav


class FundNavSource(ABC):
    """净值原子数据源统一接口。实现类需同时提供健康探针（SourceProbe）。

    **能力必须显式声明，不要靠 `inspect.signature` 猜**：
    调用方（`FundNavProvider`）按 `supports_streaming` 决定是否传 `on_page`。
    旧实现用签名探测，`**kwargs` 形式的实现会被误判为「不支持流式」——
    于是缓存层以为有逐页落盘、实际没有，中途失败整段作废（D4）。
    """

    name: str = ""

    #: 是否支持逐页流式回调 `on_page`（实现类显式置 True）
    supports_streaming: bool = False

    @abstractmethod
    async def get_latest_all_nav(self) -> List[FundNav]:
        """获取最新一期所有基金净值。"""
        raise NotImplementedError

    @abstractmethod
    async def get_fund_nav_history(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        on_page: Optional[Callable[[List[FundNav], str, str], Awaitable[None]]] = None,
    ) -> List[FundNav]:
        """获取单只基金在 [start_date, end_date] 内的历史净值（内部消化分页，支持逐页流式回调）。"""
        raise NotImplementedError


# 向下兼容旧别名
FundNavProvider = FundNavSource