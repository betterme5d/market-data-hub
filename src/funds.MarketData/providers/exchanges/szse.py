# -*- coding: utf-8 -*-
"""
深交所各站点的健康探针与取数源（基金列表、官方交易日历 monthList）。
"""
import logging
import time
from datetime import date, datetime, time as dtime, timedelta
from typing import List, Optional, Tuple

import httpx

from core import config
from providers.base import SourceProbe

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# 深交所官方交易日历接口：month=YYYY-MM 返回该月完整日历，jybz=1 表示交易日。
SZSE_CALENDAR_URL = "https://www.szse.cn/api/report/exchange/onepersistenthour/monthList"


def _parse_publish_cutoff(value: str) -> dtime:
    """把配置的 "HH:MM" 解析为时间；非法值回退 15:30。"""
    try:
        hour, minute = str(value).strip().split(":")
        return dtime(int(hour), int(minute))
    except Exception:
        return dtime(15, 30)


# 当日净值披露截止时点：该时点之前，交易日当日的净值视为尚未披露
_NAV_PUBLISH_CUTOFF = _parse_publish_cutoff(config.NAV_PUBLISH_CUTOFF)


async def _reachability(url: str, verify: bool = True) -> None:
    """可达性探测：拿到任意 HTTP 响应（含 4xx）即视为站点在网；仅网络错误/超时判失败。"""
    async with httpx.AsyncClient(verify=verify, timeout=config.HEALTH_PROBE_TIMEOUT,
                                 headers={"User-Agent": _UA}, follow_redirects=True) as client:
        try:
            await client.head(url)
        except httpx.HTTPStatusError:
            pass  # raise_for_status 从未触发，这里只是防御


class SzseWwwProbe(SourceProbe):
    """www.szse.cn 主站（历史行情 JSON / 交易日历 / 行情快照 xlsx）"""
    name = "szse-www"
    category = "exchanges"

    async def probe(self) -> None:
        async with httpx.AsyncClient(verify=False, timeout=config.HEALTH_PROBE_TIMEOUT,
                                     headers={"User-Agent": _UA}) as client:
            resp = await client.get(
                f"https://www.szse.cn/api/report/exchange/onepersistenthour/monthList?month={date.today():%Y-%m}")
            resp.raise_for_status()
            resp.json()  # 语义校验：必须是 JSON


class SzseFundProbe(SourceProbe):
    """fund.szse.cn 基金主站（公告列表 POST / ShowReport xlsx）"""
    name = "szse-fund"
    category = "exchanges"

    async def probe(self) -> None:
        body = {
            "seDate": ["", ""],
            "channelCode": ["fundinfoNotice_disc"],
            "pageSize": 1,
            "pageNum": 1,
        }
        async with httpx.AsyncClient(timeout=config.HEALTH_PROBE_TIMEOUT,
                                     headers={"User-Agent": _UA,
                                              "Content-Type": "application/json"}) as client:
            resp = await client.post("http://fund.szse.cn/api/disc/announcement/annList", json=body)
            resp.raise_for_status()
            resp.json()


class SzseDocsProbe(SourceProbe):
    """reportdocs.static.szse.cn（PCF 申赎清单静态站）"""
    name = "szse-docs"
    category = "exchanges"

    async def probe(self) -> None:
        await _reachability("https://reportdocs.static.szse.cn/files/text/ETFDown/", verify=False)


class SzseDiscProbe(SourceProbe):
    """disc.static.szse.cn（公告附件下载站）"""
    name = "szse-disc"
    category = "exchanges"

    async def probe(self) -> None:
        await _reachability("https://disc.static.szse.cn/", verify=False)


class SzseCalendarProbe(SourceProbe):
    """深交所交易日历 monthList 接口的健康探针（复用 SzseCalendarSource 同一取数/解析路径）。"""

    name = "szse-calendar"
    category = "exchanges"

    async def probe(self) -> None:
        """语义探针：走 Source 的真实解析路径取当月日历，解析失败或空集即判失效。

        探针不复制 URL/解析逻辑——否则探测请求与业务请求会漂移，探针绿而业务断。
        """
        days = await SzseCalendarSource().get_month_days(date.today().strftime("%Y-%m"))
        if not days:
            raise RuntimeError("szse-calendar probe: empty month days")


class SzseCalendarSource:
    """深交所官方交易日历取数源。

    只依赖 monthList 轻量 JSON 接口（无重库），并提供按自然日缓存，
    供 cmtidp 等业务方计算「最近一个交易日（<= 当前日期）」。
    """

    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or SZSE_CALENDAR_URL).rstrip("/")
        self._cache_day: Optional[str] = None
        self._cache_latest: Optional[str] = None
        self._cache_days = {}  # (as_of, count, published_only) -> 最近 count 个交易日(新→旧)

    async def get_month_days(self, month: str) -> List[Tuple[str, bool]]:
        """获取某月（YYYY-MM）完整日历，返回 [(jyrq, is_trading), ...]，按日期升序。"""
        async with httpx.AsyncClient(timeout=config.UPSTREAM_TIMEOUT,
                                     headers={"User-Agent": _UA, "Referer": "https://www.szse.cn/"}) as client:
            resp = await client.get(f"{self.base_url}?month={month}")
            resp.raise_for_status()
            payload = resp.json()
        data = payload.get("data") or []
        days = [(row["jyrq"], row["jybz"] == "1") for row in data if row.get("jyrq")]
        days.sort(key=lambda x: x[0])
        return days

    @staticmethod
    def _reference_day(
        as_of: Optional[str], now: Optional[datetime], published_only: bool
    ) -> date:
        """计算查询参考日。

        published_only=True（已发布口径）：若当前时刻早于 _NAV_PUBLISH_CUTOFF（默认 15:30），
        则交易日当日的净值视为尚未披露，参考日回退一天（周末/节假日自然落到上一个交易日）。
        published_only=False（纯日历口径）：直接取今天。
        """
        if as_of:
            return date.fromisoformat(as_of)
        current = now or datetime.now()
        if published_only and current.time() < _NAV_PUBLISH_CUTOFF:
            return current.date() - timedelta(days=1)
        return current.date()


    async def latest_trading_day(
        self,
        as_of: Optional[str] = None,
        now: Optional[datetime] = None,
        published_only: bool = False,
    ) -> Optional[str]:
        """返回 <= 参考日 的最近一个交易日；同一天内命中缓存，不重复请求上游。

        published_only=False（默认，纯日历口径）：东财的跨年不变式必须用这个——
        若这里也用 15:30 口径，会把当日已发布的净值日期误判成跨年（年份减 1）。
        published_only=True：用于「已发布」语义（如 CMTIDP 的最近交易日窗口）。
        """
        today = self._reference_day(as_of, now, published_only)
        cache_key = f"{today.isoformat()}|pub={published_only}"
        if self._cache_day == cache_key and self._cache_latest is not None:
            return self._cache_latest

        result: Optional[str] = None
        # 跨月回溯：最多回溯 2 个月，覆盖月初/跨年边界（长假最长跨 1 个月）。
        for offset in range(2):
            month_first = today.replace(day=1) - timedelta(days=offset * 28)
            month = month_first.strftime("%Y-%m")
            try:
                days = await self.get_month_days(month)
            except Exception as e:
                logger.error(f"szse calendar fetch failed for {month}: {e}")
                continue
            # 当月内 <= today 的交易日里取最后一个
            for jyrq, is_trading in days:
                if jyrq <= cache_key and is_trading:
                    result = jyrq  # days 升序，持续覆盖即为最后一个
            if result is not None:
                break

        self._cache_day = cache_key
        self._cache_latest = result
        return result


    async def latest_trading_days(
        self,
        count: int = 3,
        as_of: Optional[str] = None,
        now: Optional[datetime] = None,
        published_only: bool = False,
    ) -> List[str]:
        """返回 <= 参考日 的最近 count 个交易日，按「由新到旧」排序。

        与 latest_trading_day 共用 monthList 接口并按日期缓存；跨月最多回溯 6 个月。
        published_only=True 时按「已发布」口径（15:30 前当日净值未披露，参考日回退一天）。
        只有确实取够 count 个交易日才写缓存——避免上游抖动把短结果固化。
        """
        if count <= 0:
            return []
        today = self._reference_day(as_of, now, published_only)
        day_str = today.isoformat()
        cache_key = (day_str, count, published_only)
        cached = self._cache_days.get(cache_key)
        if cached is not None:
            return list(cached)

        collected: List[str] = []
        seen_months = set()
        for offset in range(6):
            month_first = today.replace(day=1) - timedelta(days=offset * 28)
            month = month_first.strftime("%Y-%m")
            if month in seen_months:
                continue
            seen_months.add(month)
            try:
                days = await self.get_month_days(month)
            except Exception as e:
                logger.error(f"szse calendar fetch failed for {month}: {e}")
                continue
            for jyrq, is_trading in reversed(days):
                if is_trading and jyrq <= day_str:
                    collected.append(jyrq)
            if len(collected) >= count:
                break

        result = collected[:count]
        if len(result) >= count:
            self._cache_days[cache_key] = result
        return result


class SzseFundListProbe(SourceProbe):
    """深交所基金产品列表 ShowReport（CATALOGID=1105）接口的健康探针。"""

    name = "szse-fund-list"
    category = "exchanges"

    async def probe(self) -> None:
        """语义探针：走 Source 的真实解析路径全量拉取，解析失败或空集即判失效。

        探针不复制 URL/解析逻辑——否则探测请求与业务请求会漂移，探针绿而业务断。
        """
        rows = await SzseFundListSource().fetch_funds()
        if not rows:
            raise RuntimeError("szse-fund-list probe: empty xlsx")


# 1105「基金类别」→ 统一 fund_type 的归一映射（"不动产基金" 即公募 REITs）
_FUND_TYPE_MAP = {
    "ETF": "ETF",
    "LOF": "LOF",
    "不动产基金": "REIT",
}


class SzseFundListSource:
    """深交所基金产品列表取数源（ShowReport CATALOGID=1105，xlsx）。

    1105 全量覆盖 ETF/LOF/REITs（不动产基金），较旧的 1945（仅 ETF/LOF 两张表、
    漏 28 只 REITs）信息更全，故列表与上市日期统一到 1105 单一数据源。
    字段：基金代码、基金简称、基金类别、投资类别、上市日期、当前规模(份)、
    基金管理人、发起人、托管人。
    """

    EXCHANGE = "SZ"
    # random 为深交所页面的缓存穿透参数（同 sse/cmtidp 的 _=时间戳，防 CDN 缓存旧表）
    URL = (
        "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx"
        "&CATALOGID=1105&TABKEY=tab1"
    )

    async def _fetch_report(self) -> list:
        """请求 1105 报表并解析（业务与探针共用同一代码路径）。

        非 200 必须抛错而不是返回空列表：空列表会被上层当成"上游这条真的没有数据"，
        进而把白名单缓存清空（下游净值/份额按白名单过滤，清空等于丢弃全部场内基金）。
        """
        url = f"{self.URL}&random={time.time()}"
        async with httpx.AsyncClient(verify=False) as client:
            resp = await client.get(url, timeout=30)
            if resp.status_code != 200:
                raise RuntimeError(f"SZSE 1105 report HTTP {resp.status_code}")
            return self._parse_xlsx(resp.content)

    async def fetch_funds(self) -> list:
        """获取深交所 ETF/LOF/REITs 基金列表（含上市日期）。失败抛错，不返回空列表。"""
        try:
            return await self._fetch_report()
        except Exception as e:
            logger.error(f"Error fetching SZSE funds: {e}")
            raise

    def _parse_xlsx(self, content: bytes) -> list:
        """解析 1105 xlsx：基金代码/简称/类别/上市日期，归一 fund_type。"""
        import io

        import pandas as pd

        rows = []
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype=object)
        for _, row in df.iterrows():
            code = str(row.get("基金代码") or "").strip()
            if not code or code == "nan":
                continue
            try:
                code = str(int(float(code))).zfill(6)
            except (ValueError, TypeError):
                code = code.zfill(6)
            category = str(row.get("基金类别") or "").strip()
            list_date_raw = row.get("上市日期")
            list_date = None
            if list_date_raw is not None:
                text = str(list_date_raw).strip()
                if text and text not in ("nan", "NaT", "None", "--", "-"):
                    list_date = text[:10]
            rows.append({
                "fund_code": code,
                "fund_name": str(row.get("基金简称") or "").strip(),
                "fund_type": _FUND_TYPE_MAP.get(category, category or "OTHER"),
                "exchange": self.EXCHANGE,
                "list_date": list_date,
            })
        return rows
