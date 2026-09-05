# -*- coding: utf-8 -*-
"""
通用流式反向代理：按 source 前缀把请求转发到对应上游 host。

- 路径规则：/proxy/{source}/{原始路径} → {上游 host}/{原始路径}（查询串与请求体原样转发）
- 响应流式回传（公告附件等二进制不落内存），Content-Type 等响应头透传
- 逐源独立配置：UA/Referer/附加头/SSL 校验开关
- 每次转发记录到 core.health 被动指标（key 为 source）
"""
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

import httpx
from fastapi import Request
from fastapi.responses import StreamingResponse

from core import config
from core import health as health_svc

logger = logging.getLogger(__name__)

_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# 会由 httpx 自己生成/不应转发的头
_DROP_REQUEST_HEADERS = {"host", "content-length", "connection", "accept-encoding", "transfer-encoding"}
_DROP_RESPONSE_HEADERS = {"content-encoding", "transfer-encoding", "connection", "content-length"}


@dataclass(frozen=True)
class UpstreamSite:
    key: str
    host: str                       # 含 scheme 与端口，如 https://yunhq.sse.com.cn:32042
    user_agent: str = _CHROME_UA
    referer: Optional[str] = None
    verify_ssl: bool = True
    timeout: float = config.UPSTREAM_TIMEOUT
    extra_headers: Dict[str, str] = field(default_factory=dict)


# 上游站点注册表：proxy 前缀 → 站点配置
UPSTREAM_SITES: Dict[str, UpstreamSite] = {
    # 深交所
    "szse-fund": UpstreamSite("szse-fund", "http://fund.szse.cn"),
    "szse-www": UpstreamSite("szse-www", "https://www.szse.cn", verify_ssl=False),
    "szse-docs": UpstreamSite("szse-docs", "https://reportdocs.static.szse.cn", verify_ssl=False),
    "szse-disc": UpstreamSite("szse-disc", "https://disc.static.szse.cn", verify_ssl=False),
    # 上交所
    "sse-query": UpstreamSite("sse-query", "https://query.sse.com.cn", referer="https://www.sse.com.cn/"),
    "sse-yunhq": UpstreamSite("sse-yunhq", "https://yunhq.sse.com.cn:32042", referer="https://www.sse.com.cn/"),
    "sse-www": UpstreamSite("sse-www", "https://www.sse.com.cn"),
    # HTML 抓取源（QDII/LOF 估值）
    "haoetf": UpstreamSite("haoetf", "https://www.haoetf.com"),
    "palmmicro": UpstreamSite(
        "palmmicro", "https://www.palmmicro.com",
        extra_headers={
            "Cookie": "PHPSESSID=75e55bf31955e8f388f59fd435c801cb; screenheight=1080; screenwidth=1920; _ga=GA1.1.839737416.1773129256; _ga_DQ4P3FHV66=GS2.1.s1773129255$o1$g1$t1773129262$j53$l0$h0",
        },
    ),
}

_SITE_CLIENTS: Dict[str, httpx.AsyncClient] = {}


def _get_client(site: UpstreamSite) -> httpx.AsyncClient:
    client = _SITE_CLIENTS.get(site.key)
    if client is None:
        client = httpx.AsyncClient(verify=site.verify_ssl, http2=False)
        _SITE_CLIENTS[site.key] = client
    return client


async def proxy_request(site: UpstreamSite, path: str, request: Request) -> StreamingResponse:
    """
    把入站请求流式转发到 site.host/path，并流式回传响应。
    """
    url = f"{site.host}/{path}"
    query = request.url.query
    if query:
        url = f"{url}?{query}"

    headers = {"User-Agent": site.user_agent}
    if site.referer:
        headers["Referer"] = site.referer
    headers.update(site.extra_headers)
    # 透传调用方附加头（如 C# 侧 SZSE 历史行情需要的 X-Requested-With 等）
    for k, v in request.headers.items():
        kl = k.lower()
        if kl not in _DROP_REQUEST_HEADERS and kl not in {h.lower() for h in headers}:
            headers[k] = v

    body = await request.body()
    started = time.monotonic()
    client = _get_client(site)
    req = client.build_request(request.method, url, headers=headers, content=body or None,
                               timeout=site.timeout)
    try:
        upstream = await client.send(req, stream=True)
    except Exception as e:
        health_svc.record_call(site.key, False, (time.monotonic() - started) * 1000, error=str(e))
        logger.error(f"Proxy {site.key} {request.method} /{path} failed: {e}")
        raise

    latency = (time.monotonic() - started) * 1000
    ok = upstream.status_code < 500
    health_svc.record_call(site.key, ok, latency,
                           error=None if ok else f"upstream status {upstream.status_code}")
    if not ok:
        logger.warning(f"Proxy {site.key} {request.method} /{path} upstream status {upstream.status_code}")

    resp_headers = {
        k: v for k, v in upstream.headers.items()
        if k.lower() not in _DROP_RESPONSE_HEADERS
    }

    async def _stream():
        try:
            async for chunk in upstream.aiter_bytes():
                yield chunk
        finally:
            await upstream.aclose()

    return StreamingResponse(_stream(), status_code=upstream.status_code, headers=resp_headers)


def get_site(source: str) -> Optional[UpstreamSite]:
    return UPSTREAM_SITES.get(source)
