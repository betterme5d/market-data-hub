# -*- coding: utf-8 -*-
"""
上交所各站点的健康探针（业务调用经 core.reverse_proxy 转发，这里只做主动探测）。
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
