# -*- coding: utf-8 -*-
"""
基金数据端点：持仓 / 基本信息 / 东财估值 / 交易所基金列表。
"""
import logging

from fastapi import APIRouter, HTTPException, Path, Query

from providers.exchanges.exchange_provider import ExchangeProvider
from providers.funds.eastmoney import EastmoneyProvider

logger = logging.getLogger(__name__)

router = APIRouter()

eastmoney_provider = EastmoneyProvider()
exchange_provider = ExchangeProvider()


@router.get("/fund/{symbol}/portfolio", tags=["基金数据"], summary="获取基金持仓结构")
async def get_fund_portfolio(
    symbol: str = Path(..., description="基金代码，如 510300"),
    year: int = Query(..., description="查询的报告年份，如 2024")
):
    """
    获取单只公募基金在特定年份的重仓股票及持仓比重等数据。
    """
    try:
        return await eastmoney_provider.get_portfolio(symbol, year)
    except Exception as e:
        logger.error(f"Get portfolio failed for {symbol} in {year}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fund/{symbol}/info", tags=["基金数据"], summary="获取基金基本信息")
async def get_fund_info(
    symbol: str = Path(..., description="基金代码")
):
    """
    获取单只公募基金的名称、管理费率、托管费率、成立时间等基本概况。
    """
    try:
        return eastmoney_provider.get_fund_info(symbol)
    except Exception as e:
        logger.error(f"Get fund info failed for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/eastmoney/valuations", tags=["基金数据"], summary="获取东财基金估值列表")
async def get_eastmoney_valuations(
    force_refresh: bool = Query(False, description="是否强制清空本地缓存，从东方财富网刷新获取最新数据")
):
    """
    获取去重合并并过滤后的以 16 或 5 开头的场内基金估值列表（默认缓存 3 分钟）。
    """
    try:
        return await eastmoney_provider.get_valuations(force_refresh=force_refresh)
    except Exception as e:
        logger.error(f"Failed to get Eastmoney valuations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/exchange/funds", tags=["基金数据"], summary="获取上交所和深交所ETF/LOF列表")
async def get_exchange_funds():
    """
    抓取并解析上交所和深交所的最新 ETF 和 LOF 基金列表。
    用于手动数据同步。
    """
    try:
        return await exchange_provider.fetch_all_exchange_funds()
    except Exception as e:
        logger.error(f"Failed to get exchange funds: {e}")
        raise HTTPException(status_code=500, detail=str(e))
