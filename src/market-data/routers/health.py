# -*- coding: utf-8 -*-
"""
健康与数据源监控端点：
- /health                     服务存活 + Valkey 连接
- /health/sources             逐源健康面板（被动指标 + 状态）
- /health/sources/{name}/probe 手动触发单源主动探测
- /api/sources/status         行情源熔断指标（与原有契约一致）
- /api/sources/unblock        手动解封（与原有契约一致）
"""
import logging

from fastapi import APIRouter, HTTPException, Query

from core import health as health_svc
from core.cache import redis_client
from core.dispatcher import QuoteDispatcher, PROVIDERS

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health", tags=["系统监控"], summary="服务健康检查")
async def health():
    """
    检查服务及其依赖组件（如 Valkey 缓存数据库）的连接状态。
    """
    return {"status": "ok", "valkey": "connected" if redis_client else "disconnected"}


@router.get("/health/sources", tags=["系统监控"], summary="逐数据源健康面板（状态+被动指标）")
async def sources_health():
    """
    返回所有已注册数据源的健康状态（healthy/degraded/down/unknown）、
    滑动窗口成功率、平均耗时、最近错误及上次成功时间。
    """
    return health_svc.get_all_sources_health()


@router.get("/health/sources/{name}/probe", tags=["系统监控"], summary="手动触发单源主动探测")
async def probe_source(name: str):
    """
    立即对指定数据源执行一次主动探测（调用上游最轻量接口），并记录到被动指标。
    """
    if name not in health_svc.registered_sources():
        raise HTTPException(status_code=404, detail=f"No probe registered for source: {name}")
    return await health_svc.run_probe(name)


@router.get("/api/sources/status", tags=["系统监控"], summary="获取各行情源实时健康状态与熔断指标")
async def get_sources_status():
    """
    暴露所有已注册行情数据源的实时熔断状态、失败故障计数及隔离剩余秒数。
    """
    status_list = []
    for src in PROVIDERS.keys():
        status_list.append(QuoteDispatcher.get_source_metrics(src))
    return status_list


@router.post("/api/sources/unblock", tags=["系统监控"], summary="手动重新启用（解封）指定行情数据源")
async def unblock_source(
    source: str = Query(..., description="要解封的行情源，如 sina, tencent, xueqiu, yfinance")
):
    """
    手动清除指定行情源在 Valkey 中的熔断冷却与失败计次，强制其立即重新启用。
    """
    source_lower = source.lower()
    if source_lower not in PROVIDERS:
        raise HTTPException(status_code=400, detail=f"Source provider {source} not found")

    success = QuoteDispatcher.unblock_source(source_lower)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to unblock source")
    return {"status": "ok", "message": f"Source {source_lower} has been successfully unblocked"}


@router.get("/quote/unblock-all", tags=["调试与维护"], summary="一键清除所有数据源的熔断状态")
async def unblock_all_sources():
    if not redis_client:
        return {"status": "error", "message": "Valkey/Redis 缓存未连接"}
    try:
        from core.dispatcher import get_blocked_key
        cleared = []
        for src in PROVIDERS.keys():
            if redis_client.delete(get_blocked_key(src)) > 0:
                cleared.append(src)
        return {"status": "ok", "cleared_sources": cleared}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
