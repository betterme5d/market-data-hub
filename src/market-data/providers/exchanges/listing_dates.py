# -*- coding: utf-8 -*-
"""
交易所基金上市日期取数源与文件缓存层。

数据来源（均已人工验证字段，页面引用见各 API docstring）：
- 上交所 SSE：SseFundListSource（commonSoaQuery，JSON，原生态 listingDate）
- 深交所 SZSE：SzseFundListSource（ShowReport CATALOGID=1105，xlsx，全量 ETF/LOF/REITs）

缓存策略：上市日期近乎静态、访问低频，不做内存常驻（Valkey），改为
按交易所本地 JSON 文件落盘（data/ 目录）。全量接口校验文件 mtime，超过一天
重新全量拉取并覆盖写；单只接口先查进程内存 → 再查文件，均未命中才打上游。
"""
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

from core.filters import clean_fund_code, get_fund_exchange
from providers.exchanges.sse import SseFundListSource

logger = logging.getLogger(__name__)

# 文件缓存目录（相对项目工作目录，可在需要时由环境变量覆盖）
CACHE_DIR = Path(os.getenv("LISTING_CACHE_DIR", "data"))

# 全量接口的文件有效期：超过该秒数视为陈旧，重新全量拉取
STALE_SECONDS = int(os.getenv("LISTING_STALE_SECONDS", str(24 * 3600)))


class SzseListingDateSource:
    """深交所基金上市日期取数源（复用 SzseFundListSource 的 1105 数据）。

    列表源（SzseFundListSource）已基于 1105 全量拉取并产出 list_date，此处仅做
    {fund_code -> list_date} 投影，避免对同一上游 xlsx 重复请求。
    页面引用：https://www.szse.cn/market/product/list/all/index.html
    """

    EXCHANGE = "SZ"

    async def fetch_listing_dates(self) -> Dict[str, str]:
        """全量深交所基金上市日期映射 {fund_code: YYYY-MM-DD}。"""
        from providers.exchanges.szse import SzseFundListSource

        rows = await SzseFundListSource().fetch_funds()
        return {r["fund_code"]: r["list_date"] for r in rows if r.get("list_date")}


class SseListingDateSource:
    """上交所基金上市日期取数源（复用 SseFundListSource 原生态 listingDate）。

    页面引用：https://www.sse.com.cn/assortment/fund/list/
    """

    EXCHANGE = "SH"

    async def fetch_listing_dates(self) -> Dict[str, str]:
        """全量上交所基金上市日期映射 {fund_code: YYYY-MM-DD}。"""
        rows = await SseFundListSource().fetch_funds()
        return {r["fund_code"]: r["list_date"] for r in rows if r.get("list_date")}


# exchange -> 上市日期取数源
_LISTING_DATE_SOURCES: Dict[str, object] = {
    "SH": SseListingDateSource(),
    "SZ": SzseListingDateSource(),
}


def _cache_file(exchange: str) -> Path:
    return CACHE_DIR / f"listing_date_{exchange.lower()}.json"


def _load_cache(exchange: str) -> Optional[Dict[str, str]]:
    """从本地文件读缓存；不存在或损坏返回 None。"""
    path = _cache_file(exchange)
    try:
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {k: v for k, v in data.items()} if isinstance(data, dict) else None
    except Exception as e:
        logger.warning(f"Load listing date cache {path} failed: {e}")
        return None


def _save_cache(exchange: str, data: Dict[str, str]) -> None:
    """原子写缓存：先写临时文件再 os.replace，避免半写。"""
    path = _cache_file(exchange)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        logger.error(f"Save listing date cache {path} failed: {e}")


def _is_stale(exchange: str) -> bool:
    """文件不存在或 mtime 超过 STALE_SECONDS 视为陈旧。"""
    path = _cache_file(exchange)
    if not path.exists():
        return True
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        return datetime.now() - mtime > timedelta(seconds=STALE_SECONDS)
    except OSError:
        return True


class ListingDateProvider:
    """交易所基金上市日期编排：全量（带 mtime 校验）+ 单只（内存→文件→上游）。

    进程内 dict 缓存与文件绑定：文件被重写时同步刷新内存，保证两套一致。
    """

    def __init__(self):
        self._mem_cache: Dict[str, Dict[str, str]] = {}

    async def _fetch_and_cache(self, exchange: str, force: bool = False) -> Dict[str, str]:
        """获取某交易所全量上市日期，必要时从上游拉取并写文件/内存。

        写盘前的三道防线——交易所列表是下游净值/份额的**白名单**，一旦被"截断快照"写短，
        真实场内基金会整批被过滤掉（数据静默丢失），因此：
        1. 上游报错：有旧快照就降级服务旧快照（告警），一片空白才把错误抛给调用方；
        2. 上游返回空：不覆盖旧快照（无旧快照时抛错，不能静默吐出空白名单）；
        3. 上游返回行数比旧快照少：落盘为「旧 ∪ 新」并告警——新增代码立即生效，
           被截断/下架的旧代码不会被清掉（白名单宁可多、不可少；下架代码多留一条无副作用）。
        """
        if not force and not _is_stale(exchange):
            cached = _load_cache(exchange)
            if cached is not None:
                self._mem_cache[exchange] = cached
                return cached

        previous = _load_cache(exchange)
        source = _LISTING_DATE_SOURCES[exchange]
        try:
            data = await source.fetch_listing_dates()
        except Exception as e:
            if previous:
                logger.warning(
                    f"listing dates fetch failed for {exchange} ({e}); "
                    f"serving stale snapshot with {len(previous)} rows"
                )
                self._mem_cache[exchange] = previous
                return previous
            raise

        if not data:
            if previous:
                logger.warning(
                    f"listing dates for {exchange} came back empty; "
                    f"keeping cached snapshot with {len(previous)} rows"
                )
                self._mem_cache[exchange] = previous
                return previous
            raise RuntimeError(
                f"listing dates for {exchange} came back empty and no cache is available"
            )

        if previous and len(data) < len(previous):
            merged = {**previous, **data}   # 新值优先，旧代码保留
            logger.warning(
                f"listing dates for {exchange} shrank from {len(previous)} to {len(data)} rows; "
                f"persisting the union ({len(merged)} rows) to keep the whitelist from shrinking"
            )
            data = merged

        _save_cache(exchange, data)
        self._mem_cache[exchange] = data
        return data

    async def get_all(self, exchange: str | None = None, force: bool = False) -> Dict[str, Dict[str, str]]:
        """全量上市日期。exchange 为 None/'all' 返回沪深合并；否则单交易所。"""
        if exchange is None or exchange.lower() in ("", "all"):
            result: Dict[str, Dict[str, str]] = {}
            for ex in _LISTING_DATE_SOURCES:
                result[ex] = await self._fetch_and_cache(ex, force=force)
            return result

        ex = exchange.upper()
        if ex not in _LISTING_DATE_SOURCES:
            raise ValueError(f"Unsupported exchange: {exchange!r} (expected SH or SZ)")
        return {ex: await self._fetch_and_cache(ex, force=force)}

    async def get_one(self, code: str) -> Dict[str, Optional[str]]:
        """单只基金上市日期；查不到返回 {fund_code, list_date: None}。

        按代码首字路由交易所：5 开头 → 上交所(ETF/LOF)，1 开头 → 深交所(LOF/ETF/REITs)。
        先查内存 → 再查文件 → 均未命中才打上游全量并回写。
        """
        clean = clean_fund_code(code)
        if not clean:
            raise ValueError(f"Invalid fund code: {code!r} (expected 6 digits)")

        exchange = get_fund_exchange(clean)
        if exchange is None:
            raise ValueError(f"Unroutable fund code: {code!r} (must start with 1 or 5)")
        code = clean

        # 1) 内存
        if code in self._mem_cache.get(exchange, {}):
            return {"fund_code": code, "list_date": self._mem_cache[exchange][code]}

        # 2) 文件
        if not _is_stale(exchange):
            cached = _load_cache(exchange)
            if cached is not None:
                self._mem_cache[exchange] = cached
                if code in cached:
                    return {"fund_code": code, "list_date": cached[code]}

        # 3) 上游全量
        data = await self._fetch_and_cache(exchange, force=True)
        return {"fund_code": code, "list_date": data.get(code)}