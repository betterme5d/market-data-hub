# -*- coding: utf-8 -*-
"""
节假日数据源（api.jiejiariapi.com）薄代理 + 健康探针。
C# 侧 HolidaysService 的消费契约：GET /v1/workdays/{year} 返回 {"YYYY-MM-DD": {...}} 字典。
"""
import logging
from datetime import datetime

import httpx

from core import config
from providers.base import SourceProbe

logger = logging.getLogger(__name__)


class HolidaysProvider(SourceProbe):
    name = "holidays"
    category = "misc"

    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or config.HOLIDAYS_BASE_URL).rstrip("/")

    async def get_workdays(self, year: int) -> dict:
        """透传上游某年度工作日表，原样返回上游 JSON。"""
        url = f"{self.base_url}/v1/workdays/{year}"
        async with httpx.AsyncClient(timeout=config.UPSTREAM_TIMEOUT) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()

    async def probe(self) -> None:
        """探针：取当年工作日表，要求返回非空字典。"""
        data = await self.get_workdays(datetime.now().year)
        if not isinstance(data, dict) or not data:
            raise RuntimeError("holidays probe: upstream returned empty workdays")
