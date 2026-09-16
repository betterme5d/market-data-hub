# -*- coding: utf-8 -*-
"""上交所基金公告取数源与健康探针（终止上市 / 摘牌）。

接口来自上交所基金公告页 `https://www.sse.com.cn/disclosure/fund/announcement/`
的业务脚本 `/xhtml/home/2021public/querySearch/search_fundInformation_2021.js`
—— 是**页面私有接口**，无 SLA，改版即可能失效，故必须配语义探针（见下方 Probe）。

已知要点（全部实测，见 docs/2026-09-12-fund-delisting-data-source.md §4）：
- `isPagination` 与 `pageHelp.pageSize` **不可省**：不传返回 0 条，只传前者会截断到 50
- `START_DATE` / `END_DATE` **留空 = 全量**（数据最早 2006 年）
- 终止上市公告分布在 `fund04`（临时报告）与 `fund05`（基金运作）两个栏目，
  留空等价于「全部」，故默认不传 BULLETIN_TYPE
"""
from __future__ import annotations

import asyncio
import logging
from io import BytesIO
from typing import Any, Dict, List, Optional

import httpx

from core import config
from providers.base import SourceProbe

logger = logging.getLogger(__name__)

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
_REFERER = "https://www.sse.com.cn/"

QUERY_URL = "https://query.sse.com.cn/commonQuery.do"
SQL_ID = "COMMON_PL_JJXX_JJGG_NEW_L"
PDF_PREFIX = "https://www.sse.com.cn"


class SseFundBulletinSource:
    """上交所基金公告取数源（commonQuery.do，JSON）。"""

    #: 一次拿全所需的页大小；实测 pageSize=50 会截断、500 可一次拿全 383 条
    DEFAULT_PAGE_SIZE = 500

    def __init__(self, base_url: str = QUERY_URL, page_size: int = DEFAULT_PAGE_SIZE):
        self.base_url = base_url
        self.page_size = page_size

    def _params(self, keyword: str, security_code: str, page_no: int,
                start_date: str, end_date: str) -> Dict[str, str]:
        return {
            "type": "inParams",
            "sqlId": SQL_ID,
            "isPagination": "true",                     # 必需，缺失则 total=0
            "pageHelp.pageSize": str(self.page_size),   # 必需，缺失则截断
            "pageHelp.pageNo": str(page_no),
            "pageHelp.beginPage": str(page_no),
            "pageHelp.cacheSize": "1",
            "pageHelp.endPage": str(page_no),
            "TITLE": keyword,
            "SECURITY_CODE": security_code,
            "BULLETIN_TYPE": "",
            "START_DATE": start_date,                   # 留空 = 全量
            "END_DATE": end_date,
            "DATE_DESC": "1",
        }

    async def _fetch_page(self, client: httpx.AsyncClient, **kw: Any):
        resp = await client.get(self.base_url, params=self._params(**kw),
                                headers={"User-Agent": _UA, "Referer": _REFERER},
                                timeout=config.UPSTREAM_TIMEOUT)
        if resp.status_code != 200:
            raise RuntimeError(f"SSE bulletin HTTP {resp.status_code}")
        payload = resp.json() or {}
        page_help = payload.get("pageHelp") or {}
        return page_help.get("data") or [], page_help.get("total")

    async def search_page(
        self,
        keyword: str,
        page_no: int = 1,
        security_code: str = "",
        start_date: str = "",
        end_date: str = "",
    ) -> tuple[List[Dict[str, str]], Optional[int]]:
        """拉**单页**（增量同步用；断点续传要求能按页推进）。

        :return: (本页记录, 上游 total)
        """
        async with httpx.AsyncClient() as client:
            page_rows, total = await self._fetch_page(
                client, keyword=keyword, security_code=security_code,
                page_no=page_no, start_date=start_date, end_date=end_date)
        return [{
            "code": r.get("SECURITY_CODE"),
            "day": r.get("SSEDATE"),
            "title": r.get("TITLE"),
            "pdf_url": PDF_PREFIX + (r.get("URL") or ""),
            "org_type": r.get("ORG_BULLETIN_TYPE_DESC"),
        } for r in page_rows], total

    async def search(
        self,
        keyword: str,
        security_code: str = "",
        start_date: str = "",
        end_date: str = "",
        max_pages: int = 50,
    ) -> List[Dict[str, str]]:
        """按标题关键词检索公告（拉全量）。

        :param keyword: 标题子串，如「终止上市」「摘牌」
        :param start_date / end_date: 留空表示不限时间（**不要随手设起点**，会漏历史数据）
        :return: [{code, day, title, pdf_url, org_type}]
        :raises RuntimeError: 上游返回结构异常或疑似截断
        """
        rows: List[Dict[str, str]] = []
        total: Optional[int] = None
        page_no = 1
        while True:
            page_rows, page_total = await self.search_page(
                keyword, page_no=page_no, security_code=security_code,
                start_date=start_date, end_date=end_date)
            if total is None:
                total = page_total
            rows.extend(page_rows)
            if not page_rows or (total is not None and len(page_rows) < self.page_size):
                break
            page_no += 1
            if page_no > max_pages:
                raise RuntimeError(
                    f"SSE bulletin pagination exceeded max_pages={max_pages} "
                    f"(got {len(rows)} of total={total})")

        # 完整性校验：拿不满就是不完整，宁可报错也不能静默返回半份
        # （项目约定：宁可显式报错，也不要静默返回半份数据）
        if total is not None and len(rows) < total:
            raise RuntimeError(
                f"SSE bulletin incomplete for keyword={keyword!r}: "
                f"got {len(rows)} of total={total}")
        return rows


    async def fetch_pdf_text(self, url: str, retries: int = 3) -> str:
        """下载公告 PDF 并抽出纯文本。

        沪市公告只有 PDF（`.json` / `.html` 均 404，已实测），日期只能从这里拿。
        上交所会**偶发返回 0 字节 / 连接超时**（疑似轻量限流），故必须重试；
        调用方在批量拉取时还应配合 `core.pacing.polite_delay()` 做节流。
        """
        from pypdf import PdfReader  # 局部导入：只有沪市路径需要

        last: Optional[Exception] = None
        for attempt in range(retries):
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.get(
                        url, headers={"User-Agent": _UA, "Referer": _REFERER},
                        timeout=config.UPSTREAM_TIMEOUT)
                if resp.status_code != 200:
                    raise RuntimeError(f"HTTP {resp.status_code}")
                content = resp.content
                if not content:
                    raise RuntimeError("empty body (疑似限流)")
                reader = PdfReader(BytesIO(content))
                return "\n".join((p.extract_text() or "") for p in reader.pages)
            except Exception as e:                       # noqa: BLE001
                last = e
                logger.warning(f"SSE pdf fetch failed ({attempt + 1}/{retries}) {url}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(0.4 * (2 ** attempt))   # 0.4 → 0.8 → 1.6s
        raise RuntimeError(f"SSE pdf fetch failed after {retries} attempts: {url}") from last


class SseFundBulletinProbe(SourceProbe):
    """上交所基金公告接口的健康探针。

    复用 Source 的真实取数路径（不复制 URL/解析逻辑），否则会「探针绿而业务断」。
    语义校验而非仅 HTTP 200 —— 上游改签名/加验证/改结构时仍会返回 200。
    """

    name = "sse-fund-bulletin"
    category = "exchanges"

    async def probe(self) -> None:
        source = SseFundBulletinSource(page_size=10)
        rows = await source.search("终止上市")
        if not rows:
            raise RuntimeError("sse-fund-bulletin probe: empty result")
        first = rows[0]
        if not first.get("code") or not first.get("pdf_url"):
            raise RuntimeError(
                "sse-fund-bulletin probe: unexpected response shape "
                "(missing SECURITY_CODE / URL)")
