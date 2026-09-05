# -*- coding: utf-8 -*-
"""
HaoETF / Palmmicro（HTML 抓取源）的健康探针。

业务调用经 core.reverse_proxy 转发（前缀 haoetf / palmmicro），这里只做主动探测。
两者都是静态 HTML 页面，最轻量探测即 HEAD 首页：拿到任意响应码（含 4xx）即视站点在网，
仅网络错误/超时判失败。
"""
import httpx

from core import config
from providers.base import SourceProbe

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


async def _get_reachability(url: str) -> None:
    """用 GET 做可达性探测（HaoETF 不支持 HEAD，返回 405）。任意 2xx/3xx 即视为在网。"""
    async with httpx.AsyncClient(timeout=config.HEALTH_PROBE_TIMEOUT,
                                 headers={"User-Agent": _UA}, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()


class HaoEtfProbe(SourceProbe):
    """www.haoetf.com（QDII/LOF 估值 HTML 页）"""
    name = "haoetf"
    category = "funds"

    async def probe(self) -> None:
        await _get_reachability("https://www.haoetf.com/")


class PalmmicroProbe(SourceProbe):
    """www.palmmicro.com（QDII 估值 HTML 页）"""
    name = "palmmicro"
    category = "funds"

    async def probe(self) -> None:
        await _get_reachability("https://www.palmmicro.com/woody/res/qdiicn.php")