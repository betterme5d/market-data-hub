# -*- coding: utf-8 -*-
"""
交易日历端点。
"""
import logging

from fastapi import APIRouter, HTTPException, Query

from core.calendar import get_exchange_trading_days

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/trading-days", tags=["系统监控"], summary="获取指定市场的交易日历")
async def get_trading_days(
    exchange: str = Query(..., description="交易所或市场标识：CN (中国大陆), HK (香港), US (美国)"),
    start: str = Query(..., description="开始日期 (格式: YYYY-MM-DD)"),
    end: str = Query(..., description="结束日期 (格式: YYYY-MM-DD)")
):
    """
    获取某个交易所在指定日期范围内的所有开市交易日列表。
    """
    try:
        days = get_exchange_trading_days(exchange, start, end)
        return {"exchange": exchange, "days": days}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get trading days for {exchange} ({start} ~ {end}): {e}")
        raise HTTPException(status_code=500, detail=str(e))
