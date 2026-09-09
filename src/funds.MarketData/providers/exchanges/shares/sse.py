# -*- coding: utf-8 -*-
"""
上交所（SSE）公募基金历史份额数据源与健康探针。
具备交易日历驱动过滤、单日 3 分类接口并发合并、对齐 C# 跨日串行调度与防风控礼貌延时。
"""
import asyncio
import collections
from datetime import date, datetime, timedelta
import logging
import random
import time
from typing import Any, Dict, List, Optional

import httpx

from core import config
from core.calendar import get_exchange_trading_days
from providers.base import SourceProbe

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

SSE_COMMON_QUERY_URL = "https://query.sse.com.cn/commonQuery.do"
SSE_REFERER = "https://www.sse.com.cn/"

# ==============================================================================
# 上交所（SSE）各公募基金份额 API 支持的最早起始交易日边界（经过全量历史穿透实测验证）：
#
# 1. 常规 ETF（股票型/跨境/债券/商品型 ETF 等）：
#    - 底层接口：COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L (参数 STAT_DATE=YYYY-MM-DD)
#    - 最早有数据交易日：2012-01-04（2012年首个交易日，首日返回 23 只 ETF，如 510010 治理ETF、510050 上证50ETF）。
#    - 实测边界证据：2011-12-30 及之前所有历史交易日，上交所该接口严格返回空列表 []（0条数据）。
#    - 判定规则：仅当 trading_day >= "2012-01-04" 时激活该接口。
SSE_ETF_EARLIEST_DATE = "2012-01-04"

# 2. 交易型货币 ETF：
#    - 底层接口：COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_JYXJJ_SEARCH_L (参数 STAT_DATE=YYYY-MM-DD)
#    - 最早有数据交易日：2013-01-28（我国首只场内交易型货币 ETF 511990 华宝添益正式上市首日，返回 1 条数据）。
#    - 实测边界证据：2013-01-25 及之前所有历史交易日，上交所该接口严格返回空列表 []（0条数据）。
#    - 判定规则：仅当 trading_day >= "2013-01-28" 时激活该接口。
SSE_MONEY_ETF_EARLIEST_DATE = "2013-01-28"

# 3. 上交所上市 LOF 基金：
#    - 底层接口：COMMON_SSE_SJ_JJSJ_JJGM_LOFGMTJ_L (参数 SEARCH_DATE=YYYY-MM-DD)
#    - 最早有数据交易日：2015-04-27（上交所 LOF 业务正式开通日，首日返回 2 条数据：502000 500等权、502003 军工等权）。
#    - 实测边界证据：2015-04-24 及之前所有历史交易日，上交所该接口严格返回空列表 []（0条数据）。
#    - 判定规则：仅当 trading_day >= "2015-04-27" 时激活该接口。
SSE_LOF_EARLIEST_DATE = "2015-04-27"

# 上交所全市场基金历史份额的最早绝对起始边界（早于 2012-01-04 上交所全市场无任何公开基金份额记录）
SSE_SHARE_ABSOLUTE_EARLIEST_DATE = SSE_ETF_EARLIEST_DATE


class SseShareSource:
    """上交所基金份额数据源。"""

    def __init__(
        self,
        min_delay: Optional[float] = None,
        max_delay: Optional[float] = None,
        delay_ms: Optional[int] = None,
        timeout: float = 20.0,
    ):
        if delay_ms is not None:
            if delay_ms == 0:
                self.min_delay = 0.0
                self.max_delay = 0.0
            else:
                self.min_delay = delay_ms * 0.001
                self.max_delay = (delay_ms + 100) * 0.001
        else:
            self.min_delay = min_delay if min_delay is not None else config.SSE_SHARE_MIN_DELAY
            self.max_delay = max_delay if max_delay is not None else config.SSE_SHARE_MAX_DELAY
        self.timeout = timeout

    def _parse_etf_response(
        self, resp: httpx.Response, trading_day: str, daily_records: Dict[str, Dict[str, Any]]
    ) -> None:
        """解析上交所常规 ETF 与 货币 ETF 接口响应。"""
        try:
            data = resp.json()
            for item in data.get("result", []):
                sec_code = str(item.get("SEC_CODE", "")).strip().zfill(6)
                if not (len(sec_code) == 6 and sec_code.isdigit()):
                    continue
                name = str(item.get("SEC_NAME", "")).strip() or None
                stat_date = str(item.get("STAT_DATE", trading_day)).strip()
                tot_vol_str = str(item.get("TOT_VOL", "")).replace(",", "").strip()
                if not tot_vol_str:
                    continue
                try:
                    shares = float(tot_vol_str)  # 上游已是万份
                    raw_shares = round(shares * 10000.0, 2)
                except (ValueError, TypeError):
                    continue

                daily_records[sec_code] = {
                    "code": sec_code,
                    "share_date": stat_date,
                    "shares": shares,
                    "raw_shares": raw_shares,
                    "name": name,
                }
        except Exception as e:
            logger.warning(f"Failed to parse SSE ETF response for {trading_day}: {e}")

    def _parse_lof_response(
        self, resp: httpx.Response, trading_day: str, daily_records: Dict[str, Dict[str, Any]]
    ) -> None:
        """解析上交所 LOF 基金接口响应。"""
        try:
            data = resp.json()
            for item in data.get("result", []):
                fund_code = str(item.get("FUND_CODE", "")).strip().zfill(6)
                if not (len(fund_code) == 6 and fund_code.isdigit()):
                    continue
                name = str(item.get("FUND_ABBR", item.get("SEC_NAME_FULL", ""))).strip() or None
                raw_date = str(item.get("TRADE_DATE", trading_day)).strip()
                if len(raw_date) == 8 and raw_date.isdigit():
                    share_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
                else:
                    share_date = raw_date

                vol_str = str(item.get("INTERNAL_VOL", "")).replace(",", "").strip()
                if not vol_str:
                    continue
                try:
                    shares = float(vol_str)  # 上游已是万份
                    raw_shares = round(shares * 10000.0, 2)
                except (ValueError, TypeError):
                    continue

                daily_records[fund_code] = {
                    "code": fund_code,
                    "share_date": share_date,
                    "shares": shares,
                    "raw_shares": raw_shares,
                    "name": name,
                }
        except Exception as e:
            logger.warning(f"Failed to parse SSE LOF response for {trading_day}: {e}")

    async def fetch_daily_market_shares(self, trading_day: str) -> Dict[str, Dict[str, Any]]:
        """
        根据交易日精确调度上交所对应的分类接口：
        - trading_day < 2012-01-04: 上交所此前无任何基金份额数据，直接返回空字典，发起 0 个网络请求；
        - 2012-01-04 <= trading_day < 2013-01-28: 仅激活【常规 ETF】接口（1 个请求）；
        - 2013-01-28 <= trading_day < 2015-04-27: 激活【常规 ETF】+【货币 ETF】接口（2 个请求）；
        - trading_day >= 2015-04-27: 激活【常规 ETF】+【货币 ETF】+【LOF】全量 3 接口。
        """
        # 若早于上交所最早数据产生日（2012-01-04），直接返回空，避免无意义的网络请求与风控风险
        if trading_day < SSE_SHARE_ABSOLUTE_EARLIEST_DATE:
            logger.debug(
                f"Trading day {trading_day} is prior to SSE earliest data boundary ({SSE_SHARE_ABSOLUTE_EARLIEST_DATE}), skipping all requests."
            )
            return {}

        headers = {
            "User-Agent": _UA,
            "Referer": SSE_REFERER,
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }

        ts = int(time.time() * 1000)
        daily_records: Dict[str, Dict[str, Any]] = {}
        tasks = []
        task_types = []

        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            # 1. 常规 ETF（>= 2012-01-04 激活）
            if trading_day >= SSE_ETF_EARLIEST_DATE:
                etf_params = {
                    "sqlId": "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",
                    "STAT_DATE": trading_day,
                    "_": str(ts),
                }
                tasks.append(client.get(SSE_COMMON_QUERY_URL, params=etf_params, headers=headers))
                task_types.append("etf")

            # 2. 货币型 ETF（>= 2013-01-28 激活）
            if trading_day >= SSE_MONEY_ETF_EARLIEST_DATE:
                mkt_params = {
                    "sqlId": "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_JYXJJ_SEARCH_L",
                    "STAT_DATE": trading_day,
                    "_": str(ts),
                }
                tasks.append(client.get(SSE_COMMON_QUERY_URL, params=mkt_params, headers=headers))
                task_types.append("money_etf")

            # 3. LOF 基金（>= 2015-04-27 激活）
            if trading_day >= SSE_LOF_EARLIEST_DATE:
                lof_params = {
                    "sqlId": "COMMON_SSE_SJ_JJSJ_JJGM_LOFGMTJ_L",
                    "SEARCH_DATE": trading_day,
                    "_": str(ts),
                }
                tasks.append(client.get(SSE_COMMON_QUERY_URL, params=lof_params, headers=headers))
                task_types.append("lof")

            if not tasks:
                return {}

            responses = await asyncio.gather(*tasks, return_exceptions=True)

        for t_type, resp in zip(task_types, responses):
            if isinstance(resp, Exception):
                logger.warning(f"SSE {t_type} fetch failed for {trading_day}: {resp}")
                continue
            if resp.status_code != 200:
                continue

            if t_type in ("etf", "money_etf"):
                self._parse_etf_response(resp, trading_day, daily_records)
            elif t_type == "lof":
                self._parse_lof_response(resp, trading_day, daily_records)

        return daily_records

    async def fetch_market_shares_range(
        self, start_date: str, end_date: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        利用交易日历筛选有效交易日，对齐 C# 串行逐日拉取并加入安全礼貌延时。
        返回格式: { "510050": [ {"code": "510050", "share_date": "2026-06-25", ...}, ... ], ... }
        """
        days_info = get_exchange_trading_days("CN", start_date, end_date)
        trading_days = [d["date"] for d in days_info if d.get("is_trading")]

        all_records: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        if not trading_days:
            return all_records

        for idx, day in enumerate(trading_days):
            daily_data = await self.fetch_daily_market_shares(day)
            for code, rec in daily_data.items():
                all_records[code].append(rec)

            # 跨交易日安全礼貌延时：仅当该交易日实际发起了上游网络调用（即 >= 2012-01-04）且不是最后一个交易日时才延时
            # 对于早于 2012-01-04 的日期，直接 0 延迟秒级跳过，避免数千天无意义等待
            if day >= SSE_SHARE_ABSOLUTE_EARLIEST_DATE and self.max_delay > 0 and idx < len(trading_days) - 1:
                sleep_sec = random.uniform(self.min_delay, self.max_delay)
                logger.debug(f"SSE polite delay: {sleep_sec:.2f}s before fetching next trading day")
                await asyncio.sleep(sleep_sec)

        return all_records


class SseShareProbe(SourceProbe):
    """上交所基金份额报表接口健康探针。"""

    name = "sse-fund-share"
    category = "exchanges"

    def __init__(self, source: Optional[SseShareSource] = None):
        self.source = source or SseShareSource(min_delay=0.0, max_delay=0.0, timeout=config.HEALTH_PROBE_TIMEOUT)

    async def probe(self) -> None:
        today_str = date.today().strftime("%Y-%m-%d")
        five_days_ago = (date.today() - timedelta(days=5)).strftime("%Y-%m-%d")
        days_info = get_exchange_trading_days("CN", five_days_ago, today_str)
        trading_days = [d["date"] for d in days_info if d.get("is_trading")]
        # 优先取最近一个已收盘并披露的交易日 (T-1)
        past_trading_days = [d for d in trading_days if d < today_str]
        latest_trade_day = past_trading_days[-1] if past_trading_days else (trading_days[-1] if trading_days else today_str)

        # 探测单日全市场接口
        res = await self.source.fetch_daily_market_shares(latest_trade_day)
        if not isinstance(res, dict) or len(res) < 50:
            raise ValueError(f"SseShareProbe expected >=50 funds, got {len(res) if isinstance(res, dict) else type(res)}")
