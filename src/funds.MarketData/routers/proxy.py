# -*- coding: utf-8 -*-
"""
通用反向代理路由：/proxy/{source}/{路径} → 对应上游 host。
支持 GET/POST，响应流式回传（公告附件/Excel 等二进制不缓冲）。
"""
import logging

from fastapi import APIRouter, HTTPException, Path, Request

from core import reverse_proxy

logger = logging.getLogger(__name__)

router = APIRouter()


@router.api_route(
    "/proxy/{source}/{full_path:path}",
    methods=["GET", "POST"],
    tags=["薄代理"],
    summary="按 source 前缀流式转发到对应上游站点",
)
async def proxy(
    request: Request,
    source: str = Path(..., description="上游站点前缀，如 szse-fund / sse-query"),
    full_path: str = Path(..., description="转发到上游的原始路径"),
):
    site = reverse_proxy.get_site(source)
    if site is None:
        raise HTTPException(status_code=404, detail=f"Unknown proxy source: {source}")
    try:
        return await reverse_proxy.proxy_request(site, full_path, request)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"proxy to {source} failed: {e}")
