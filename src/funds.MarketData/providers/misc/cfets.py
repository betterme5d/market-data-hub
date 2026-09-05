# -*- coding: utf-8 -*-
"""
中国外汇交易中心 CFETS（chinamoney.com.cn）薄代理 + 健康探针。
C# 侧 CfetsService 消费契约：GET /ags/ms/cm-u-bk-ccpr/CcprHisNew?... 返回含 records 的 JSON。
"""
import logging
from datetime import date, timedelta

import httpx

from core import config
from providers.base import SourceProbe

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class CfetsProvider(SourceProbe):
    name = "cfets"
    category = "misc"

    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or config.CFETS_BASE_URL).rstrip("/")

    async def get_middle_price_history(self, query_string: str) -> dict:
        """
        透传历史中间价查询：原样转发查询串，原样返回上游 JSON。
        分页/日期切分等逻辑保留在 C# 调用方。
        """
        url = f"{self.base_url}/ags/ms/cm-u-bk-ccpr/CcprHisNew?{query_string}"
        async with httpx.AsyncClient(timeout=config.UPSTREAM_TIMEOUT) as client:
            resp = await client.get(url, headers={"User-Agent": _USER_AGENT})
            resp.raise_for_status()
            return resp.json()

    async def probe(self) -> None:
        """探针：查最近 7 天 USD/CNY 一页，要求返回可解析的 JSON（允许 records 为空）。"""
        end = date.today()
        start = end - timedelta(days=7)
        qs = f"startDate={start}&endDate={end}&currency=USD/CNY&pageNum=1&pageSize=5"
        data = await self.get_middle_price_history(qs)
        if not isinstance(data, dict) or "records" not in data:
            raise RuntimeError("cfets probe: unexpected response shape (missing 'records')")
