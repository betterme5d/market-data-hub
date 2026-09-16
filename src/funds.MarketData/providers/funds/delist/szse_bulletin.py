# -*- coding: utf-8 -*-
"""深交所基金公告取数源与健康探针（终止上市）。

接口来自深交所基金公告页 `https://www.szse.cn/disclosure/notice/fund/index.html`
—— 是**页面私有接口**，无 SLA，故必须配语义探针。

已知要点（全部实测，见 docs/2026-09-12-fund-delisting-data-source.md §5）：
- **不需要 cookie**，但需 `Referer` 与 `X-Requested-With`
- `time` / `endTime` 是**毫秒时间戳**（不是日期字符串），传错会返回 HTTP 200 空载荷
- 结果里的 `docpubjsonurl` 可直接取到正文 JSON，**无需下载解析 PDF**
- 但正文是**缩略版**，不含「最后运作日 / 清算」，那些要另走巨潮 PDF
"""
from __future__ import annotations

import datetime
import logging
import random
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import httpx

from core import config
from providers.base import SourceProbe
from providers.funds.delist.parser import strip_html

logger = logging.getLogger(__name__)

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36")
_REFERER = "https://www.szse.cn/disclosure/notice/fund/index.html"
_ORIGIN = "https://www.szse.cn"

SEARCH_URL = "https://www.szse.cn/api/search/content"
DOC_JSON_PREFIX = "https://www.szse.cn"

#: 基金公告频道（页面下拉框的值，照抄页面，不要自创）
CHANNEL_FUND_NEWS = "fund_news"


def _ms(day: Optional[str]) -> str:
    """YYYY-MM-DD → 毫秒时间戳；None/空返回 '0'（上游约定 0 = 不限）。"""
    if not day:
        return "0"
    return str(int(datetime.datetime.fromisoformat(day).replace(
        tzinfo=datetime.UTC).timestamp() * 1000))


class SzseFundBulletinSource:
    """深交所基金公告取数源（api/search/content）。"""

    DEFAULT_PAGE_SIZE = 30

    def __init__(self, base_url: str = SEARCH_URL, page_size: int = DEFAULT_PAGE_SIZE):
        self.base_url = base_url
        self.page_size = page_size

    def _headers(self) -> Dict[str, str]:
        return {
            "User-Agent": _UA,
            "Origin": _ORIGIN,
            "Referer": _REFERER,
            "X-Requested-With": "XMLHttpRequest",
            "X-Request-Type": "ajax",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def _body(self, keyword: str, page: int, start_date: Optional[str],
              end_date: Optional[str], search_range: str) -> str:
        """表单体。channelCode 是数组参数，须编码成 `channelCode%5B%5D=fund_news`。

        这里显式 urlencode 后用 `content=` 发送 —— 直接把 list of tuple 交给
        httpx 的 `data=` 会被判定为同步流，在 AsyncClient 下报
        "Attempted to send an sync request with an AsyncClient instance"。
        """
        return urlencode([
            ("keyword", keyword),
            ("range", search_range),
            ("channelCode[]", CHANNEL_FUND_NEWS),
            ("currentPage", str(page)),
            ("pageSize", str(self.page_size)),
            ("scope", "0"),
            ("time", _ms(start_date)),
            ("endTime", _ms(end_date)),
        ])

    async def search_page(
        self,
        keyword: str,
        page: int = 1,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        search_range: str = "title",
    ) -> tuple[List[Dict[str, Any]], Optional[int]]:
        """拉**单页**（增量同步用）。

        深市 `time` 传毫秒时间戳即可只拉该时刻之后发布的公告 —— 已实测有效
        （2026-01 起仅 1 条、2025-01 起 9 条、全量 248 条）。
        """
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self.base_url,
                params={"random": f"{random.random():.6f}"},
                content=self._body(keyword, page, start_date, end_date, search_range),
                headers=self._headers(),
                timeout=config.UPSTREAM_TIMEOUT,
            )
        if resp.status_code != 200:
            raise RuntimeError(f"SZSE bulletin HTTP {resp.status_code}")
        payload = resp.json() or {}
        rows = []
        for r in payload.get("data") or []:
            rows.append({
                "id": r.get("id"),
                # 标题带 <span class="keyword">高亮</span>，必须去标签
                "title": strip_html(r.get("doctitle") or ""),
                "day": _day(r.get("docpubtime")),
                "published_ms": r.get("docpubtime"),
                "doc_url": r.get("docpuburl"),
                "json_url": (DOC_JSON_PREFIX + r["docpubjsonurl"]
                             if r.get("docpubjsonurl") else None),
                "summary": r.get("doccontent"),
            })
        return rows, payload.get("totalSize")

    async def search(
        self,
        keyword: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        search_range: str = "title",
        max_pages: int = 50,
    ) -> List[Dict[str, Any]]:
        """按关键词检索基金公告（拉全量）。

        :param search_range: title / content / all（实测 content|all 召回略多）
        :return: [{id, title, day, doc_url, json_url, summary}]
        """
        rows: List[Dict[str, Any]] = []
        total: Optional[int] = None
        page = 1
        while True:
            page_rows, page_total = await self.search_page(
                keyword, page=page, start_date=start_date,
                end_date=end_date, search_range=search_range)
            if total is None:
                total = page_total
            rows.extend(page_rows)
            if not page_rows or (total is not None and len(rows) >= total):
                break
            page += 1
            if page > max_pages:
                raise RuntimeError(
                    f"SZSE bulletin pagination exceeded max_pages={max_pages}")

        if total is not None and len(rows) < total:
            raise RuntimeError(
                f"SZSE bulletin incomplete for keyword={keyword!r}: "
                f"got {len(rows)} of totalSize={total}")
        return rows

    async def fetch_content(self, json_url: str) -> str:
        """取公告正文（HTML 字符串）。逐条调用时务必在调用方做节流。"""
        async with httpx.AsyncClient() as client:
            resp = await client.get(json_url, headers=self._headers(),
                                    timeout=config.UPSTREAM_TIMEOUT)
            if resp.status_code != 200:
                raise RuntimeError(f"SZSE doc content HTTP {resp.status_code}")
            payload = resp.json() or {}
            return (payload.get("data") or {}).get("content") or ""


def _day(ms: Optional[int]) -> Optional[str]:
    if not ms:
        return None
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.UTC).strftime("%Y-%m-%d")


class SzseFundBulletinProbe(SourceProbe):
    """深交所基金公告接口的健康探针（复用 Source 取数路径）。"""

    name = "szse-fund-bulletin"
    category = "exchanges"

    async def probe(self) -> None:
        source = SzseFundBulletinSource(page_size=5)
        rows = await source.search("终止上市")
        if not rows:
            raise RuntimeError("szse-fund-bulletin probe: empty result")
        json_url = rows[0].get("json_url")
        if not json_url:
            raise RuntimeError(
                "szse-fund-bulletin probe: unexpected response shape (missing docpubjsonurl)")
        # 语义校验：正文必须真能取到，否则「列表通、正文断」会被漏掉
        content = await source.fetch_content(json_url)
        if len(content) < 20:
            raise RuntimeError("szse-fund-bulletin probe: doc content too short")
