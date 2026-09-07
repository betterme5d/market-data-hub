# -*- coding: utf-8 -*-
"""
交易所基金列表统一编排入口。

沪/深各自的取数逻辑已下沉到 sse.py / szse.py，
这里只负责按 exchange 参数分发，支持单交易所与全部合并。
"""
import logging

from providers.exchanges.sse import SseFundListSource
from providers.exchanges.szse import SzseFundListSource

logger = logging.getLogger(__name__)

# exchange 标识 -> 交易所基金列表取数源
_FUND_LIST_PROVIDERS = {
    "SH": SseFundListSource(),
    "SZ": SzseFundListSource(),
}


class ExchangeProvider:
    """按交易所分发基金列表取数；exchange 为空或 'all' 时合并沪、深。"""

    async def fetch_funds(self, exchange: str | None = None) -> list:
        """获取基金列表。

        exchange: "SH" / "SZ"；为空、None 或 "all" 时返回沪深合并结果（先沪后深）。
        """
        if exchange is None or exchange.lower() in ("", "all"):
            results = []
            for provider in _FUND_LIST_PROVIDERS.values():
                results.extend(await provider.fetch_funds())
            return results

        key = exchange.upper()
        provider = _FUND_LIST_PROVIDERS.get(key)
        if provider is None:
            raise ValueError(f"Unsupported exchange: {exchange!r} (expected SH or SZ)")
        return await provider.fetch_funds()

    async def fetch_all_exchange_funds(self) -> list:
        """兼容旧调用：沪深全部基金列表。"""
        return await self.fetch_funds(exchange=None)