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
from core.pacing import polite_delay
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
            max_pages=1,   # 探针只取一页：真翻页是给业务用的，探针拉全量会拖慢健康检查
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
    def _list_url(fund_type: str, sub_class: str, page_size: int, page_no: int = 1) -> str:
        return (
            "https://query.sse.com.cn/commonSoaQuery.do?isPagination=true"
            f"&pageHelp.pageSize={page_size}"
            f"&pageHelp.pageNo={page_no}&pageHelp.beginPage={page_no}"
            "&pageHelp.cacheSize=1"
            f"&pageHelp.endPage={page_no}"
            "&pagecache=false&sqlId=FUND_LIST"
            f"&fundType={fund_type}&subClass={sub_class}&order="
        )

    @staticmethod
    def _to_row(item: dict, fund_type: str, exchange: str) -> dict:
        """上游一条 ETF/LOF 记录 → 统一结构。"""
        return {
            "fund_code": item.get("fundCode"),
            "fund_name": item.get("secNameFull") or item.get("fundAbbr"),
            "fund_type": "ETF" if fund_type == "00" else "LOF",
            "exchange": exchange,
            "list_date": _normalize_listing_date(item.get("listingDate")),
        }

    async def _fetch_page(
        self, fund_type: str, sub_class: str, page_size: int, page_no: int
    ) -> tuple:
        """请求一页，返回 (行列表, 上游自述总条数)；总条数缺失返回 None。"""
        async with httpx.AsyncClient(verify=False) as client:
            resp = await client.get(
                self._list_url(fund_type, sub_class, page_size, page_no),
                headers=self.headers, timeout=30,
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"SSE fund list HTTP {resp.status_code} (fundType={fund_type}, pageNo={page_no})"
                )
            payload = resp.json() or {}
            items = payload.get("result") or []
            page_help = payload.get("pageHelp") or {}
            try:
                total = int(page_help.get("total"))
            except (TypeError, ValueError):
                total = None   # 上游没给总数：退回"短页即末页"的单页语义
            return [self._to_row(item, fund_type, self.EXCHANGE) for item in items], total

    async def _fetch_list(
        self, fund_type: str, sub_class: str, page_size: int, max_pages: int | None = None,
    ) -> list:
        """分页拉取一类基金列表（业务与探针共用同一代码路径）。

        交易所对 pageSize 有封顶：请求 10000 条也可能只回 1000 条，单页取数会**静默截断**。
        列表是下游净值/份额的白名单，白名单变短会把真实场内基金整批过滤掉（数据静默丢失），
        因此这里按上游自述的 pageHelp.total 真翻页，并在取不满时报错（由上层保留旧快照）。

        :param max_pages: 只取前 N 页（健康探针用，避免探针拖成全量拉取）。
        """
        rows: list = []
        seen: set = set()
        total = None
        page_no = 1
        while True:
            page_rows, page_total = await self._fetch_page(fund_type, sub_class, page_size, page_no)
            if page_total is not None:
                total = page_total
            new_codes = 0
            for row in page_rows:
                code = row.get("fund_code")
                if not code or code in seen:
                    continue
                seen.add(code)
                rows.append(row)
                new_codes += 1

            if max_pages is not None and page_no >= max_pages:
                break
            if total is not None and len(rows) >= total:
                break
            if not page_rows or new_codes == 0:
                # 空页，或上游忽略了 pageNo（重复返回同一页）→ 不能再翻，交给下面的完整性校验判错
                break
            if total is None and len(page_rows) < page_size:
                break
            page_no += 1
            await polite_delay()   # 翻页之间的礼貌延时（0.2~0.5s）

        if max_pages is None and total is not None and len(rows) < total:
            raise RuntimeError(
                f"SSE fund list incomplete for fundType={fund_type}: got {len(rows)} of {total} rows"
            )
        return rows

    async def fetch_funds(self) -> list:
        """获取上交所 ETF 与 LOF 基金列表。

        任一分类拉取失败即抛错：半份列表一旦写进白名单缓存，会把真实场内基金整个过滤掉，
        必须让调用方看到失败并保留旧快照（缓存层会降级服务）。
        """
        results: list = []
        failures: list = []
        for fund_type, sub_class, ftype in (
            ("00", self.ETF_SUB_CLASS, "ETF"),
            ("10", self.LOF_SUB_CLASS, "LOF"),
        ):
            try:
                results.extend(await self._fetch_list(fund_type, sub_class, page_size=10000))
            except Exception as e:
                logger.error(f"Error fetching SSE {ftype} funds: {e}")
                failures.append(f"{ftype}: {e}")

        if failures:
            raise RuntimeError("SSE fund list fetch failed -> " + "; ".join(failures))
        return results
