# -*- coding: utf-8 -*-
"""
基金数据端点：持仓 / 基本信息 / 东财估值 / 交易所基金列表 / 统一净值。
"""
import logging

from fastapi import APIRouter, HTTPException, Path, Query

from core.models import FundNavResponse
from providers.exchanges.exchange_provider import ExchangeProvider
from providers.funds.cmtidp import CmtidpSource
from providers.funds.eastmoney import EastmoneySource
from providers.funds.base_nav import FundNavProvider
from providers.funds.fund_profile import EastmoneyProfileSource, collect_fund_profiles

logger = logging.getLogger(__name__)

router = APIRouter()

eastmoney_provider = EastmoneySource()
exchange_provider = ExchangeProvider()
eastmoney_profile_provider = EastmoneyProfileSource()

# 净值数据源注册表：source -> FundNavProvider 实现
_NAV_PROVIDERS: dict[str, FundNavProvider] = {
    "eastmoney": eastmoney_provider,
    "cmtidp": CmtidpSource(),
}


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


@router.get("/api/v1/funds/profiles", tags=["基金数据"], summary="获取基金档案（成立/上市日期，一次性采集）")
async def get_fund_profiles():
    """
    合并东财场内基金排行（成立日期）与沪深交易所基金列表（上市日期）的全量基金档案。
    档案为一次性数据，供 C# FundProfileCollectionJob 回填 funds 表。
    """
    try:
        return {"profiles": await collect_fund_profiles()}
    except Exception as e:
        logger.error(f"Get fund profiles failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fund/{symbol}/profile", tags=["基金数据"], summary="获取单只基金档案（成立日期）")
async def get_fund_profile(
    symbol: str = Path(..., description="基金代码，如 161724")
):
    """
    从东财 F10 基本概况页解析单只基金的成立日期，供批量档案覆盖不到的基金兜底。
    """
    try:
        return await eastmoney_profile_provider.get_fund_profile(symbol.strip())
    except Exception as e:
        logger.error(f"Get fund profile failed for {symbol}: {e}")
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


@router.get("/api/v1/exchange/funds/{exchange}", tags=["基金数据"], summary="获取指定交易所ETF/LOF列表")
async def get_exchange_funds_by(exchange: str):
    """
    抓取指定交易所（SH | SZ）的最新 ETF 和 LOF 基金列表。
    """
    try:
        return await exchange_provider.fetch_funds(exchange=exchange)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get exchange funds for {exchange}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/fund-nav/latest", tags=["基金数据"], summary="获取最新一期全量基金净值")
async def get_latest_all_nav(
    source: str = Query(..., description="数据源：cmtidp | eastmoney")
):
    """
    获取最新一期所有基金净值。各源"最新"语义：
    eastmoney 取当前最新交易日全量；cmtidp 取最新更新日全量。
    """
    provider = _NAV_PROVIDERS.get(source.lower())
    if provider is None:
        raise HTTPException(status_code=400, detail=f"Unknown source: {source}")
    try:
        items = await provider.get_latest_all_nav()
        return FundNavResponse(source=source.lower(), count=len(items), items=items)
    except Exception as e:
        logger.error(f"Get latest all nav failed for {source}: {e}")
        raise HTTPException(status_code=502, detail=f"{source} upstream failed: {e}")


@router.get("/api/fund-nav/history", tags=["基金数据"], summary="获取指定基金历史净值")
async def get_fund_nav_history(
    source: str = Query(..., description="数据源：cmtidp | eastmoney"),
    code: str = Query(..., description="基金代码（6 位）"),
    start_date: str | None = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: str | None = Query(None, description="结束日期 YYYY-MM-DD"),
):
    """
    获取单只基金在 [start_date, end_date] 区间的逐日历史净值（升序）。
    """
    provider = _NAV_PROVIDERS.get(source.lower())
    if provider is None:
        raise HTTPException(status_code=400, detail=f"Unknown source: {source}")
    try:
        items = await provider.get_fund_nav_history(code, start_date, end_date)
        return FundNavResponse(source=source.lower(), count=len(items), items=items)
    except Exception as e:
        logger.error(f"Get fund nav history failed for {source}/{code}: {e}")
        raise HTTPException(status_code=502, detail=f"{source} upstream failed: {e}")
