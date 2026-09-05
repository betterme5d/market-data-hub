# -*- coding: utf-8 -*-
"""
深交所各站点的健康探针（业务调用经 core.reverse_proxy 转发，这里只做主动探测）。
"""
import logging
from datetime import date

import httpx

from core import config
from providers.base import SourceProbe

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


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
