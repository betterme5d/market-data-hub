# -*- coding: utf-8 -*-
"""
证监会信息披露平台 CMTIDP（eid.csrc.gov.cn）薄代理 + 健康探针。
C# 侧 CMTIDPService 消费契约：GET /fund/disclose/getPublicFundJZInfoMore.do?aoData=...&_=...
返回含 aaData/iTotalRecords 的 JSON。分页与聚合逻辑保留在 C# 侧。
"""
import json
import logging
import time
from urllib.parse import quote

import httpx

from core import config
from providers.base import SourceProbe

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)


class CmtidpProvider(SourceProbe):
    name = "cmtidp"
    category = "funds"

    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or config.CMTIDP_BASE_URL).rstrip("/")

    async def get_fund_net_values(self, query_string: str) -> dict:
        """透传净值查询：原样转发查询串，原样返回上游 JSON。"""
        url = f"{self.base_url}/fund/disclose/getPublicFundJZInfoMore.do?{query_string}"
        headers = {"User-Agent": _USER_AGENT, "Referer": f"{self.base_url}/"}
        async with httpx.AsyncClient(timeout=config.UPSTREAM_TIMEOUT) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()

    async def probe(self) -> None:
        """探针：取一只常见基金当日 1 条净值，要求返回含 iTotalRecords 的 JSON。"""
        from datetime import date

        today = date.today().strftime("%Y-%m-%d")
        ao_data = [
            {"name": "sEcho", "value": 1},
            {"name": "iColumns", "value": 5},
            {"name": "sColumns", "value": ",,,,"},
            {"name": "iDisplayStart", "value": 0},
            {"name": "iDisplayLength", "value": 1},
            {"name": "mDataProp_0", "value": "fund"},
            {"name": "mDataProp_1", "value": "fund"},
            {"name": "mDataProp_2", "value": "fund"},
            {"name": "mDataProp_3", "value": "fund"},
            {"name": "mDataProp_4", "value": "valuationDate"},
            {"name": "fundType", "value": "all"},
            {"name": "fundCompanyShortName", "value": ""},
            {"name": "fundCode", "value": "000001"},
            {"name": "fundName", "value": ""},
            {"name": "startDate", "value": today},
            {"name": "endDate", "value": today},
        ]
        qs = f"aoData={quote(json.dumps(ao_data, separators=(',', ':')))}&_={int(time.time() * 1000)}"
        data = await self.get_fund_net_values(qs)
        if not isinstance(data, dict) or "iTotalRecords" not in data:
            raise RuntimeError("cmtidp probe: unexpected response shape (missing 'iTotalRecords')")
