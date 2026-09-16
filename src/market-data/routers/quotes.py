# -*- coding: utf-8 -*-
"""
行情数据端点：实时报价 / 批量报价 / 历史K线 / 标的信息。
"""
import logging
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel

from core.dispatcher import PROVIDERS, QuoteDispatcher
from core.exceptions import BusinessException
from core.models import UnifiedQuoteOut
from core.routing import route_provider, translate_standard_symbol

logger = logging.getLogger(__name__)

router = APIRouter()


class BatchQuoteItem(BaseModel):
    symbol: str
    allowed_sources: Dict[str, str]


class BatchQuoteRequest(BaseModel):
    items: List[BatchQuoteItem]


@router.post("/quote/batch", response_model=List[UnifiedQuoteOut], tags=["行情数据"], summary="批量获取实时行情")
async def get_quotes_batch(
    request: BatchQuoteRequest,
    with_depth: bool = Query(False, description="是否包含五档深度盘口数据"),
    refresh: bool = Query(False, description="true 时跳过缓存读取，强制打上游并回写缓存（方案A）"),
):
    """
    批量获取多个证券的最新实时报价。
    C#端通过此接口发送包含每个标的可用源映射的字典。
    """
    try:
        items_dict = [{"symbol": item.symbol, "allowed_sources": item.allowed_sources} for item in request.items]
        results = await QuoteDispatcher.get_quotes_batch(
            items_dict, with_depth=with_depth, refresh=refresh
        )
        return results
    except Exception as e:
        logger.error(f"Batch quote fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quote/{symbol}", response_model=UnifiedQuoteOut, tags=["行情数据"], summary="获取实时行情")
async def get_quote(
    symbol: str = Path(..., description="标的代码，例如 AAPL 或 000001.SZ"),
    source: Optional[str] = Query(None, description="强制行情提供商（如 tencent, sina, xueqiu, yfinance），不填则自动多源 Fallback"),
    with_depth: bool = Query(False, description="是否包含五档深度盘口数据"),
    refresh: bool = Query(False, description="true 时跳过缓存读取，强制打上游并回写缓存（方案A）"),
):
    """
    获取单个股票或 ETF 的最新实时报价，支持自动代码翻译与多源自适应熔断 Fallback。
    """
    if source:
        source_lower = source.lower()
        translated = translate_standard_symbol(symbol)
        if source_lower in translated:
            allowed_sources = {source_lower: translated[source_lower]}
        else:
            allowed_sources = {source_lower: symbol}
    else:
        allowed_sources = translate_standard_symbol(symbol)

    try:
        result = await QuoteDispatcher.get_quote_with_fallback(
            symbol, allowed_sources, with_depth=with_depth, refresh=refresh
        )
        return result
    except BusinessException as be:
        raise HTTPException(status_code=400, detail=str(be))
    except Exception as e:
        logger.error(f"Get quote failed for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history/{symbol}", tags=["行情数据"], summary="获取历史 K 线")
async def get_history(
    symbol: str = Path(..., description="标的代码，例如 AAPL 或 000001.SZ"),
    period: str = Query("1mo", description="查询的时间跨度，如 1d, 5d, 1mo, 3mo, 1y, max"),
    interval: str = Query("1d", description="K线周期，如 1m, 5m, 1h, 1d, 1wk, 1mo"),
    start: Optional[str] = Query(None, description="开始日期 (格式: YYYY-MM-DD)，若传入则 period 失效"),
    end: Optional[str] = Query(None, description="结束日期 (格式: YYYY-MM-DD)，若传入则 period 失效"),
    adj: str = Query("hfq", description="复权类型：hfq (后复权), qfq (前复权), None (不复权)"),
    source: Optional[str] = Query(None, description="行情提供商，不填则自动路由")
):
    """
    查询指定标的的历史 K 线数据。
    """
    provider_name = route_provider(symbol, source)
    provider = PROVIDERS.get(provider_name)
    if not provider:
        raise HTTPException(status_code=400, detail=f"Provider {provider_name} not found")

    try:
        return await provider.get_history(symbol, period, interval, start, end, adj)
    except BusinessException as be:
        raise HTTPException(status_code=400, detail=str(be))
    except Exception as e:
        logger.error(f"Get history failed for {symbol} via {provider_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/info/{symbol}", tags=["行情数据"], summary="获取标的信息")
async def get_info(
    symbol: str = Path(..., description="标的代码"),
    source: Optional[str] = Query(None, description="行情提供商，不填则自动路由")
):
    """
    获取指定标的的元数据与详细基础信息。
    """
    provider_name = route_provider(symbol, source)
    provider = PROVIDERS.get(provider_name)
    if not provider:
        raise HTTPException(status_code=400, detail=f"Provider {provider_name} not found")

    try:
        if hasattr(provider, "get_info"):
            return await provider.get_info(symbol)
        raise HTTPException(status_code=400, detail=f"Provider {provider_name} does not support get_info")
    except Exception as e:
        logger.error(f"Get info failed for {symbol} via {provider_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
