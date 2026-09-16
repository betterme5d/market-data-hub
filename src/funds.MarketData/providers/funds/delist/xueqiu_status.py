# -*- coding: utf-8 -*-
"""雪球证券状态扫描 —— 交易所公告通道的**补充召回**。

为什么需要它
------------
交易所公告只能覆盖「发过终止上市/摘牌公告」的基金。实测 501023 走的是
「基金合同终止 + 清算」，上交所公告系统里没有任何终止上市/摘牌/清算字样的公告，
只能从雪球 `status=3` 发现它已退出。

实测质量（2026-09-13 抽样）
--------------------------
- 召回 **93.8%**：已知退市 80 只中 75 只 status=3（会漏 501066 / 502003 / 168002 等）
- 误报 **0%**：在市 60 只全部 status=1

→ 它是**高精确、中召回**信号，适合做补充召回，不能当唯一判据。
  雪球漏的那批正好交易所公告能补（501066 等库里都有），两者互补。

注意
----
- 用**单只** `v5/stock/quote.json` 而非批量 `realtime/quotec.json`：
  批量接口休市时返回 HTTP 200 + 空载荷，扫不出东西。
- Cookie 复用 `XueqiuProvider._ensure_cookie()`（网关/直连/兜底三级 + Valkey 共享）。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional

import httpx

from core import config
from providers.base import SourceProbe
from providers.quotes.xueqiu import XueqiuProvider

logger = logging.getLogger(__name__)

#: status 取值（实测共四种，不只 1 / 3）
STATUS_NORMAL = 1
STATUS_DELISTED = 3

#: **判定「已退出」要包含 0 和 2，不能只看 3。**
#: 实测：501066 / 501054 / 501063 / 501002 / 502030 是 status=**2**，
#:       501041 / 501042 / 501055 是 status=**0**，
#:       这些全部都有交易所终止上市/摘牌公告确认已退市。
#: 早期版本只认 3，把这批漏掉，召回率被低估（88.3% → 91.8%）。
DELISTED_STATUSES = frozenset({0, 2, 3})


def is_delisted(status: Optional[int]) -> bool:
    """是否为「已退出」状态。空值不算退市（可能是上游空响应，见 §6.6）。"""
    return status in DELISTED_STATUSES


class XueqiuStatusSource:
    """雪球证券状态扫描源。"""

    def __init__(self, provider: Optional[XueqiuProvider] = None,
                 concurrency: Optional[int] = None):
        self._provider = provider or XueqiuProvider()
        self._sem = asyncio.Semaphore(
            concurrency or getattr(config, "XUEQIU_KLINE_CONCURRENCY", 4))

    def _symbol(self, code: str, market: str) -> str:
        """雪球要求大写前缀，如 SH510300 / SZ159969。"""
        return f"{market.upper()}{code}".upper()

    async def fetch_status(self, code: str, market: str) -> Optional[int]:
        """查单个代码的 status；查不到返回 None（含上游空响应）。"""
        cookie, ua = await self._provider._ensure_cookie()
        url = f"{self._provider.base_url}v5/stock/quote.json"
        params = {"symbol": self._symbol(code, market), "extend": "detail"}
        headers = {"User-Agent": ua, "Cookie": cookie, "Referer": "https://xueqiu.com/"}
        async with self._sem:
            try:
                async with httpx.AsyncClient(timeout=config.UPSTREAM_TIMEOUT,
                                             verify=False) as client:
                    resp = await client.get(url, params=params, headers=headers)
                    if resp.status_code in (400, 403):     # Cookie 失效，清缓存重试一次
                        self._provider._cookie = None
                        cookie, ua = await self._provider._ensure_cookie()
                        headers["Cookie"] = cookie
                        headers["User-Agent"] = ua
                        resp = await client.get(url, params=params, headers=headers)
                    resp.raise_for_status()
                    payload = resp.json() or {}
            except Exception as e:                        # noqa: BLE001
                logger.warning(f"xueqiu status fetch failed {market}{code}: {e}")
                return None
        quote = (payload.get("data") or {}).get("quote") or {}
        if not quote:
            return None                                   # 上游空响应，不等于"正常"
        return quote.get("status")

    async def scan(self, targets: List[Dict[str, str]]) -> List[Dict]:
        """批量扫描，返回 status=3 的条目。

        :param targets: [{"code": "501023", "market": "SH"}, ...]
        :return: [{"code", "market", "status", "name", "nav_last_date"}, ...]
        """
        async def one(t: Dict[str, str]) -> Optional[Dict]:
            status = await self.fetch_status(t["code"], t["market"])
            if not is_delisted(status):
                return None
            return {**t, "status": status}

        results = await asyncio.gather(*(one(t) for t in targets))
        return [r for r in results if r]


class XueqiuStatusProbe(SourceProbe):
    """雪球状态扫描探针（复用 Source 取数路径）。

    用一个**确定已退市**的代码做正向校验：若连它都查不到 status=3，
    说明接口或 Cookie 有问题，扫描结果不可信。
    """

    name = "xueqiu-status"
    category = "quotes"

    #: 已终止上市（2026-08-24），用于正向校验
    KNOWN_DELISTED = ("560650", "SH")

    async def probe(self) -> None:
        source = XueqiuStatusSource()
        code, market = self.KNOWN_DELISTED
        status = await source.fetch_status(code, market)
        if status is None:
            raise RuntimeError(
                f"xueqiu-status probe: 查询 {market}{code} 无响应（Cookie 或接口异常）")
        if not is_delisted(status):
            raise RuntimeError(
                f"xueqiu-status probe: {market}{code} 应为已退出状态"
                f"{sorted(DELISTED_STATUSES)}，实际 {status}")
