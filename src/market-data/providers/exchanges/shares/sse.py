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
from typing import Any, Callable, Dict, List, Optional, Tuple

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

# 逐交易日拉全市场的增量落盘间隔（交易日）。每交易日约 2.5~4 秒，取 10 天 ≈ 30 秒：
# 客户端断开/超时被打断时，最多只损失这 10 天，下次请求按覆盖区间续跑。
SSE_SHARE_FLUSH_EVERY_DAYS = 10

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
    ) -> int:
        """解析上交所常规 ETF 与 货币 ETF 接口响应，返回**成功解析的记录数**。

        结构不可解析（非 JSON / `result` 不是数组）一律抛异常，由调用方判为「不完整」。
        绝不能吞掉异常：上游 200 + 结构变更 / 风控页会被误判成「当日无数据」，
        该交易日随后进入覆盖区间，缺失的份额数据被永久固化（覆盖区间一旦标记就不再问上游）。
        """
        data = resp.json()
        if not isinstance(data, dict) or not isinstance(data.get("result"), list):
            raise ValueError(
                f"unexpected SSE ETF payload shape for {trading_day}: {type(data).__name__}"
            )
        parsed = 0
        for item in data["result"]:
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
            parsed += 1
        return parsed

    def _parse_lof_response(
        self, resp: httpx.Response, trading_day: str, daily_records: Dict[str, Dict[str, Any]]
    ) -> int:
        """解析上交所 LOF 基金接口响应，返回**成功解析的记录数**（异常语义同 `_parse_etf_response`）。"""
        data = resp.json()
        if not isinstance(data, dict) or not isinstance(data.get("result"), list):
            raise ValueError(
                f"unexpected SSE LOF payload shape for {trading_day}: {type(data).__name__}"
            )
        parsed = 0
        for item in data["result"]:
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
            parsed += 1
        return parsed

    async def fetch_daily_market_shares(self, trading_day: str) -> Dict[str, Dict[str, Any]]:
        """
        根据交易日精确调度上交所对应的分类接口：
        - trading_day < 2012-01-04: 上交所此前无任何基金份额数据，直接返回空字典，发起 0 个网络请求；
        - 2012-01-04 <= trading_day < 2013-01-28: 仅激活【常规 ETF】接口（1 个请求）；
        - 2013-01-28 <= trading_day < 2015-04-27: 激活【常规 ETF】+【货币 ETF】接口（2 个请求）；
        - trading_day >= 2015-04-27: 激活【常规 ETF】+【货币 ETF】+【LOF】全量 3 接口。
        """
        # 若早于上交所最早数据产生日（2012-01-04），直接返回空，避免无意义的网络请求与风控风险
        records, _ = await self.fetch_daily_market_shares_checked(trading_day)
        return records

    async def fetch_daily_market_shares_checked(
        self, trading_day: str
    ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
        """
        单交易日全市场份额，附完整性标记：返回 (当日记录, 是否完整)。

        判为**不完整**的三种情形（调用方必须据此把这一天排除在覆盖区间之外——
        否则那天缺的那一类基金会永久缺失，因为覆盖区间一旦标记就不会再问上游）：
        1. 任一分类接口请求异常或非 200；
        2. 任一分类接口 HTTP 200 但载荷不可用（非 JSON / 结构变更 / 被风控页替换）；
        3. 所有已激活的分类接口都解析出 0 条记录（交易日不可能出现）。
        """
        if trading_day < SSE_SHARE_ABSOLUTE_EARLIEST_DATE:
            # 早于上交所最早数据产生日：源侧明确无数据，属于"完整"（可以标记覆盖）
            logger.debug(
                f"Trading day {trading_day} is prior to SSE earliest data boundary ({SSE_SHARE_ABSOLUTE_EARLIEST_DATE}), skipping all requests."
            )
            return {}, True

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
                return {}, True

            responses = await asyncio.gather(*tasks, return_exceptions=True)

        complete = True
        parsed_counts: Dict[str, int] = {}
        for t_type, resp in zip(task_types, responses):
            if isinstance(resp, Exception):
                logger.warning(f"SSE {t_type} fetch failed for {trading_day}: {resp}")
                complete = False
                continue
            if resp.status_code != 200:
                logger.warning(f"SSE {t_type} returned HTTP {resp.status_code} for {trading_day}")
                complete = False
                continue

            try:
                if t_type in ("etf", "money_etf"):
                    parsed_counts[t_type] = self._parse_etf_response(resp, trading_day, daily_records)
                elif t_type == "lof":
                    parsed_counts[t_type] = self._parse_lof_response(resp, trading_day, daily_records)
            except Exception as e:
                # 200 但载荷不可用（非 JSON / 结构变更 / 风控页）：这是"接口坏了"，不是"当日无数据"，
                # 必须判为不完整，否则该交易日会被标成已覆盖、缺失数据永久固化。
                logger.warning(
                    f"SSE {t_type} payload unusable for {trading_day}: {e}; 该日判为不完整"
                )
                complete = False

        # 已激活的分类接口**全部**没解析出记录：交易日不可能出现（2012-01-04 起每个交易日都有 ETF 披露），
        # 按上游静默失效处理。至少一个接口有记录时才认"完整"，避免正常交易日被误判而反复重取。
        if complete and task_types and all(parsed_counts.get(t, 0) == 0 for t in task_types):
            logger.warning(
                f"SSE all {len(task_types)} classification interfaces returned 0 records for "
                f"{trading_day}; 判为不完整（不标记覆盖，留给下次重取）"
            )
            complete = False

        return daily_records, complete

    async def fetch_market_shares_range(
        self,
        start_date: str,
        end_date: str,
        on_window: Optional[Callable[[Dict[str, List[Dict[str, Any]]], str, str], None]] = None,
        flush_every_days: int = SSE_SHARE_FLUSH_EVERY_DAYS,
        incomplete_days_out: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        利用交易日历筛选有效交易日，对齐 C# 串行逐日拉取并加入安全礼貌延时。
        返回格式: { "510050": [ {"code": "510050", "share_date": "2026-06-25", ...}, ... ], ... }

        :param on_window: 增量落盘回调 (本窗口数据, 窗口起, 窗口止)，**同步**调用（取消场景下不能再 await）。
            每积累 flush_every_days 个交易日调用一次，循环结束或中途被取消（客户端断开 → 中间件立即 cancel）
            时再调用一次收尾。没有它的话，一次 HTTP 中断就意味着整段逐日拉取的工作全部作废。
            窗口是**日历连续**的（跨过期间的非交易日），因此相邻窗口的覆盖区间会合并成一整段。
        :param flush_every_days: 增量落盘的交易日间隔。
        :return: 未传 on_window 时返回整段全市场记录；传了 on_window 时数据已逐窗口交给回调，
            返回空字典（有回调方就没有必要再整段常驻内存）。
        """
        days_info = get_exchange_trading_days("CN", start_date, end_date)
        trading_days = [d["date"] for d in days_info if d.get("is_trading")]

        all_records: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        if not trading_days:
            return all_records

        window_records: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        window_start: Optional[str] = None   # 本窗口的日历起点（含期间的非交易日）
        window_end: Optional[str] = None     # 本窗口最后一个"完整"交易日
        flushed_through: Optional[str] = None
        incomplete_days: List[str] = []
        # 下一个窗口的日历起点：正常推进到"最后一个完整交易日的次日"，遇到不完整的一天则跳过它本身，
        # 覆盖区间就在那天留洞，下次补录会重取这一天
        cursor = start_date

        def _next_day(day: str) -> str:
            return (date.fromisoformat(day) + timedelta(days=1)).isoformat()

        def _flush_window() -> None:
            nonlocal window_records, window_start, window_end, flushed_through
            if on_window is not None and window_start is not None and window_end is not None:
                try:
                    on_window(window_records, window_start, window_end)
                    flushed_through = window_end
                except Exception as e:
                    logger.error(
                        f"SSE incremental flush failed for window [{window_start} ~ {window_end}]: {e}"
                    )
            window_records = collections.defaultdict(list)
            window_start = None
            window_end = None

        try:
            for idx, day in enumerate(trading_days):
                daily_data, complete = await self.fetch_daily_market_shares_checked(day)

                if complete:
                    if window_start is None:
                        window_start = cursor
                    for code, rec in daily_data.items():
                        # D17：有增量回调时数据已随窗口落盘，不再整段累积——
                        # 10 年回补 = 2000+ 交易日 × 全市场基金，整段累积会常驻数百 MB。
                        if on_window is None:
                            all_records[code].append(rec)
                        window_records[code].append(rec)
                    window_end = day
                else:
                    # 不完整的一天（某个分类接口挂了）：先把已覆盖部分落盘，再让窗口在失败日的次日重开。
                    # 该日已拿到的部分记录既不落盘也不返回——留给下次整体重取，避免缓存里留下不完整的一天。
                    _flush_window()
                    incomplete_days.append(day)
                    cursor = _next_day(day)

                if complete and on_window is not None and (idx + 1) % flush_every_days == 0:
                    _flush_window()
                    cursor = _next_day(day)

                # 跨交易日安全礼貌延时：仅当该交易日实际发起了上游网络调用（即 >= 2012-01-04）且不是最后一个交易日时才延时
                # 对于早于 2012-01-04 的日期，直接 0 延迟秒级跳过，避免数千天无意义等待
                if day >= SSE_SHARE_ABSOLUTE_EARLIEST_DATE and self.max_delay > 0 and idx < len(trading_days) - 1:
                    sleep_sec = random.uniform(self.min_delay, self.max_delay)
                    logger.debug(f"SSE polite delay: {sleep_sec:.2f}s before fetching next trading day")
                    await asyncio.sleep(sleep_sec)
        finally:
            # 正常结束与中途取消（客户端断开）都走这里：把未满一个窗口的尾巴落盘，
            # 已落盘的部分在下次请求时会被覆盖区间识别为已缓存，不会重跑
            _flush_window()
            if flushed_through is not None and flushed_through != trading_days[-1]:
                logger.warning(
                    f"SSE range fetch for [{start_date} ~ {end_date}] stopped at {flushed_through} "
                    f"(last trading day {trading_days[-1]}); partial data persisted."
                )
            if incomplete_days:
                if incomplete_days_out is not None:
                    incomplete_days_out.extend(incomplete_days)
                logger.warning(
                    f"SSE range fetch for [{start_date} ~ {end_date}] got incomplete market data on "
                    f"{len(incomplete_days)} day(s) {incomplete_days[:5]}; those days stay uncovered for retry."
                )

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

        # 探测单日全市场接口（用带完整性标记的版本：只判"有数据"会漏掉"某分类接口已失效"）
        res, complete = await self.source.fetch_daily_market_shares_checked(latest_trade_day)
        if not isinstance(res, dict) or len(res) < 50:
            raise ValueError(f"SseShareProbe expected >=50 funds, got {len(res) if isinstance(res, dict) else type(res)}")
        if not complete:
            raise ValueError(
                f"SseShareProbe: {latest_trade_day} 分类接口不完整（有接口失败/载荷不可用/全部为空），"
                f"疑似上游接口变更或风控拦截"
            )
