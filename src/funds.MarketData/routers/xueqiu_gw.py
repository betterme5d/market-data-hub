# -*- coding: utf-8 -*-
"""
雪球 K 线转发端点：经 Playwright 鉴权网关解决反爬与 Cookie 问题。
"""
import logging

import httpx
from fastapi import APIRouter, HTTPException, Query

from core import config

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/xueqiu/kline", tags=["雪球数据"], summary="转发雪球K线获取请求")
async def get_xueqiu_kline(
    symbol: str = Query(..., description="雪球标的代码，如 SH510300"),
    begin: int = Query(..., description="起始毫秒时间戳"),
    period: str = Query("day", description="周期：day (日K), week (周K), month (月K) 等"),
    type: str = Query("normal", description="复权形式：normal (不复权), before (前复权)"),
    count: int = Query(-31, description="获取K线柱的数量（负数表示向前获取，如-31表示最近31根）"),
    indicator: str = Query("kline", description="技术指标，默认 kline")
):
    """
    转发雪球K线获取请求到 Playwright 鉴权网关（解决雪球防爬取与会话 Cookie 问题）。
    """
    gateway_url = f"{config.PLAYWRIGHT_GATEWAY_URL}/xueqiu/kline"
    params = {
        "symbol": symbol,
        "begin": begin,
        "period": period,
        "type": type,
        "count": count,
        "indicator": indicator
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(gateway_url, params=params, timeout=60)
            if resp.status_code != 200:
                logger.error(f"Playwright gateway returned non-200 code: {resp.status_code}, body: {resp.text}")
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.HTTPError as he:
        logger.error(f"HTTP error during communication with Playwright gateway: {he}")
        raise HTTPException(status_code=502, detail=f"Playwright Gateway communication failed: {str(he)}")
    except Exception as e:
        logger.error(f"Unexpected error when proxying to Playwright gateway: {e}")
        raise HTTPException(status_code=500, detail=str(e))
