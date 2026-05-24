from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import logging

from core.models import UnifiedQuoteOut
from core.cache import get_cached_quote, set_cached_quote
from providers.yfinance import YFinanceProvider

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Market Data Proxy Service")

# 实例化提供商
yfinance_provider = YFinanceProvider()

PROVIDERS = {
    "yfinance": yfinance_provider
}

def route_provider(symbol: str, source: Optional[str] = None) -> str:
    """
    根据标的代码特征或显式参数选择行情提供商。
    未来可以方便地扩展：
    if symbol.endswith(".SH") or symbol.endswith(".SZ"):
        return "tushare"
    """
    if source and source in PROVIDERS:
        return source
    return "yfinance"

@app.get("/health")
async def health():
    from core.cache import redis_client
    return {"status": "ok", "valkey": "connected" if redis_client else "disconnected"}

@app.get("/quote/{symbol}", response_model=UnifiedQuoteOut)
async def get_quote(symbol: str, source: Optional[str] = None):
    provider_name = route_provider(symbol, source)
    
    # 1. 尝试优先从缓存读取
    cached_data = get_cached_quote(symbol, provider=provider_name)
    if cached_data:
        return cached_data

    # 2. 缓存未命中，调用提供商获取
    provider = PROVIDERS.get(provider_name)
    if not provider:
        raise HTTPException(status_code=400, detail=f"Provider {provider_name} not found")
    
    try:
        result_model, ttl = await provider.get_quote(symbol)
        
        # 3. 写入缓存并返回
        set_cached_quote(symbol, result_model.model_dump(), ttl, provider=provider_name)
        return result_model
    except Exception as e:
        logger.error(f"Get quote failed for {symbol} via {provider_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/history/{symbol}")
async def get_history(
    symbol: str, 
    period: str = Query("1mo"),
    interval: str = Query("1d"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    adj: str = Query("hfq"),
    source: Optional[str] = None
):
    provider_name = route_provider(symbol, source)
    provider = PROVIDERS.get(provider_name)
    if not provider:
        raise HTTPException(status_code=400, detail=f"Provider {provider_name} not found")

    try:
        return await provider.get_history(symbol, period, interval, start, end, adj)
    except Exception as e:
        logger.error(f"Get history failed for {symbol} via {provider_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/info/{symbol}")
async def get_info(symbol: str, source: Optional[str] = None):
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

@app.get("/trading-days")
async def get_trading_days(
    exchange: str = Query(..., description="Exchange name: CN, HK, US"),
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str = Query(..., description="End date YYYY-MM-DD")
):
    try:
        from core.calendar import get_exchange_trading_days
        days = get_exchange_trading_days(exchange, start, end)
        return {"exchange": exchange, "days": days}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get trading days for {exchange} ({start} ~ {end}): {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

