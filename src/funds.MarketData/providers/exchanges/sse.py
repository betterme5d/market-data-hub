# -*- coding: utf-8 -*-
"""
上交所各站点的健康探针与基金列表取数源。
探针只做主动探测（且必须复用 Source 的取数路径）。
"""
import logging
import time
from datetime import date

import httpx

from core import config
from providers.base import SourceProbe

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_SSE_REFERER = "https://www.sse.com.cn/"


class SseQueryProbe(SourceProbe):
    """query.sse.com.cn 信息披露平台（份额/PCF/公告 commonQuery）"""
    name = "sse-query"
    category = "exchanges"

    async def probe(self) -> None:
        async with httpx.AsyncClient(timeout=config.HEALTH_PROBE_TIMEOUT,
                                     headers={"User-Agent": _UA, "Referer": _SSE_REFERER}) as client:
            params = {
                "sqlId": "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",
                "STAT_DATE": f"{date.today():%Y-%m-%d}",
                "_": int(time.time() * 1000),
            }
            resp = await client.get("https://query.sse.com.cn/commonQuery.do", params=params)
            resp.raise_for_status()
            data = resp.json()
            if "result" not in data:
                raise RuntimeError("sse-query probe: unexpected response shape (missing 'result')")


class SseYunhqProbe(SourceProbe):
    """yunhq.sse.com.cn:32042 行情云（ETF/LOF/REITs 快照与日K）"""
    name = "sse-yunhq"
    category = "exchanges"

    async def probe(self) -> None:
        async with httpx.AsyncClient(timeout=config.HEALTH_PROBE_TIMEOUT,
                                     headers={"User-Agent": _UA, "Referer": _SSE_REFERER}) as client:
            resp = await client.get(
                "https://yunhq.sse.com.cn:32042/v1/sh1/list/exchange/lof",
                params={"callback": "", "_": int(time.time() * 1000)})
            resp.raise_for_status()
            if not resp.text.strip("() \r\n"):
                raise RuntimeError("sse-yunhq probe: empty response")


class SseWwwProbe(SourceProbe):
    """www.sse.com.cn（公告附件下载站）"""
    name = "sse-www"
    category = "exchanges"

    async def probe(self) -> None:
        async with httpx.AsyncClient(timeout=config.HEALTH_PROBE_TIMEOUT,
                                     headers={"User-Agent": _UA}, follow_redirects=True) as client:
            await client.head("https://www.sse.com.cn/")


class SseFundListProbe(SourceProbe):
    """上交所 ETF/LOF 基金列表接口的健康探针（复用 SseFundListSource 同一取数/解析路径）。"""

    name = "sse-fund-list"
    category = "exchanges"

    async def probe(self) -> None:
        """语义探针：走 Source 的真实解析路径轻量拉取（pageSize=10），解析失败或空集即判失效。

        探针不复制 URL/解析逻辑——否则探测请求与业务请求会漂移，探针绿而业务断。
        """
        rows = await SseFundListSource()._fetch_list(
            fund_type="00", sub_class=SseFundListSource.ETF_SUB_CLASS, page_size=10,
        )
        if not rows or not rows[0].get("fund_code"):
            raise RuntimeError("sse-fund-list probe: unexpected response shape (missing 'result'/'fundCode')")


def _normalize_listing_date(value) -> str | None:
    """SSE listingDate 形如 20150925，归一为 ISO 日期；空值/异常格式返回 None。"""
    if not value:
        return None
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return None


class SseFundListSource:
    """上交所 ETF/LOF 基金列表取数源（commonSoaQuery，JSON）。"""

    EXCHANGE = "SH"
    ETF_SUB_CLASS = "01%2C02%2C03%2C04%2C06%2C08%2C09%2C31%2C32%2C33%2C34%2C35%2C36%2C37%2C38"
    LOF_SUB_CLASS = "11%2C14%2C15"

    def __init__(self):
        self.headers = {
            "User-Agent": _UA,
            "Referer": _SSE_REFERER,
        }

    @staticmethod
    def _list_url(fund_type: str, sub_class: str, page_size: int) -> str:
        return (
            "https://query.sse.com.cn/commonSoaQuery.do?isPagination=true"
            f"&pageHelp.pageSize={page_size}"
            "&pageHelp.pageNo=1&pageHelp.beginPage=1&pageHelp.cacheSize=1&pageHelp.endPage=1"
            "&pagecache=false&sqlId=FUND_LIST"
            f"&fundType={fund_type}&subClass={sub_class}&order="
        )

    async def _fetch_list(self, fund_type: str, sub_class: str, page_size: int) -> list:
        """请求一类基金列表并解析为统一结构（业务与探针共用同一代码路径）。"""
        rows: list = []
        async with httpx.AsyncClient(verify=False) as client:
            resp = await client.get(
                self._list_url(fund_type, sub_class, page_size),
                headers=self.headers, timeout=30,
            )
            if resp.status_code == 200:
                for item in resp.json().get("result", []):
                    rows.append({
                        "fund_code": item.get("fundCode"),
                        "fund_name": item.get("secNameFull") or item.get("fundAbbr"),
                        "fund_type": "ETF" if fund_type == "00" else "LOF",
                        "exchange": self.EXCHANGE,
                        "list_date": _normalize_listing_date(item.get("listingDate")),
                    })
        return rows

    async def fetch_funds(self) -> list:
        """获取上交所 ETF 与 LOF 基金列表。"""
        results: list = []
        for fund_type, sub_class, ftype in (
            ("00", self.ETF_SUB_CLASS, "ETF"),
            ("10", self.LOF_SUB_CLASS, "LOF"),
        ):
            try:
                results.extend(await self._fetch_list(fund_type, sub_class, page_size=10000))
            except Exception as e:
                logger.error(f"Error fetching SSE {ftype} funds: {e}")
        return results
