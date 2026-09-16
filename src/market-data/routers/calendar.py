# -*- coding: utf-8 -*-
"""
交易日历端点。
"""
import logging

from fastapi import APIRouter, HTTPException, Query

from core.calendar import get_exchange_trading_days
from providers.exchanges.szse import SzseCalendarSource

logger = logging.getLogger(__name__)

router = APIRouter()

_szse_calendar = SzseCalendarSource()


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


@router.get("/api/trading-day/latest", tags=["系统监控"], summary="获取最近一个交易日（深交所官方日历）")
async def get_latest_trading_day(
    as_of: str | None = Query(None, description="基准日期 YYYY-MM-DD，默认今天；返回 <= 该日期的最近交易日"),
):
    """
    返回最近一个交易日（<= 基准日期），基于深交所官方 monthList 日历，按自然日缓存。
    用于各数据源判定「最新净值日」。
    """
    try:
        latest = await _szse_calendar.latest_trading_day(as_of)
        return {"as_of": as_of or None, "latest_trading_day": latest}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get latest trading day (as_of={as_of}): {e}")
        raise HTTPException(status_code=502, detail=str(e))
