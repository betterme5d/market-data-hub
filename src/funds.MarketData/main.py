from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import logging
import httpx

from core.models import UnifiedQuoteOut
from core.cache import get_cached_quote, set_cached_quote
from providers.yfinance import YFinanceProvider
from providers.eastmoney import EastmoneyProvider
from providers.kraneshares import KraneSharesProvider
from providers.ishares import IsharesProvider

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Market Data Proxy Service")

# 实例化提供商
yfinance_provider = YFinanceProvider()

PROVIDERS = {
    "yfinance": yfinance_provider
}

eastmoney_provider = EastmoneyProvider()
krane_provider = KraneSharesProvider()
ishares_provider = IsharesProvider()

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

@app.get("/api/krane/premium-discount/{pid}")
async def get_krane_premium_discount(
    pid: str,
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str = Query(..., description="End date YYYY-MM-DD")
):
    try:
        return await krane_provider.get_premium_discount(pid, start, end)
    except Exception as e:
        logger.error(f"Get KraneShares premium discount failed for {pid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ishares/premium-discount/{product_path:path}")
async def get_ishares_premium_discount(
    product_path: str,
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str = Query(..., description="End date YYYY-MM-DD")
):
    try:
        return await ishares_provider.get_premium_discount(product_path, start, end)
    except Exception as e:
        logger.error(f"Get iShares premium discount failed for {product_path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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

@app.get("/fund/{symbol}/portfolio")
async def get_fund_portfolio(symbol: str, year: int = Query(..., description="Query year, e.g., 2024")):
    try:
        return await eastmoney_provider.get_portfolio(symbol, year)
    except Exception as e:
        logger.error(f"Get portfolio failed for {symbol} in {year}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/fund/{symbol}/info")
async def get_fund_info(symbol: str):
    try:
        return eastmoney_provider.get_fund_info(symbol)
    except Exception as e:
        logger.error(f"Get fund info failed for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def clean_dataframe(df) -> list:
    import pandas as pd
    import datetime
    from core.filters import is_exchange_traded_fund
    
    # 先做一层交易所交易基金的过滤，仅保留场内交易基金
    df['temp_code'] = df['基金代码'].astype(str).str.zfill(6)
    df = df[df['temp_code'].apply(is_exchange_traded_fund)]
    df = df.drop(columns=['temp_code'])
    
    records = df.to_dict(orient="records")
    cleaned_records = []
    for r in records:
        cleaned_r = {}
        for k, v in r.items():
            if pd.isna(v):
                cleaned_r[k] = None
            else:
                if isinstance(v, (pd.Timestamp, datetime.date, datetime.datetime)):
                    cleaned_r[k] = v.strftime("%Y-%m-%d")
                else:
                    if k == "基金代码":
                        cleaned_r[k] = str(v).zfill(6)
                    else:
                        cleaned_r[k] = v
        cleaned_records.append(cleaned_r)
    return cleaned_records

@app.get("/api/public/fund_purchase_em")
async def get_fund_purchase_em():
    try:
        import akshare as ak
        df = ak.fund_purchase_em()
        return clean_dataframe(df)
    except Exception as e:
        logger.error(f"Failed to fetch fund_purchase_em: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/public/fund_scale_open_sina")
async def get_fund_scale_open_sina(symbol: str = Query(..., description="Sina Open Fund Type, e.g. 股票型基金")):
    try:
        import akshare as ak
        df = ak.fund_scale_open_sina(symbol=symbol)
        return clean_dataframe(df)
    except Exception as e:
        logger.error(f"Failed to fetch fund_scale_open_sina for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/xueqiu/kline")
async def get_xueqiu_kline(
    symbol: str,
    begin: int,
    period: str = "day",
    type: str = "normal",
    count: int = -31,
    indicator: str = "kline"
):
    """
    转发雪球K线获取请求到 Playwright 鉴权网关
    """
    # 容器间网络域名为 funds.playwright.gateway，端口为 8081
    gateway_url = "http://funds.playwright.gateway:8081/xueqiu/kline"
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


# ============================================================
# 申万行业指数 API
# ============================================================

import urllib.request
import http.cookiejar
import ssl
import json

# 全局共享的 HTTP opener（带 SSL 忽略）
_sws_opener = None
_sws_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Referer': 'https://www.swsresearch.com/',
    'X-Requested-With': 'XMLHttpRequest',
}

def _get_sws_opener():
    global _sws_opener
    if _sws_opener is None:
        cj = http.cookiejar.CookieJar()
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        _sws_opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=ctx),
            urllib.request.HTTPCookieProcessor(cj)
        )
    return _sws_opener

def _sws_api(path: str) -> dict:
    from urllib.parse import quote
    opener = _get_sws_opener()
    url = f"https://www.swsresearch.com/institute-sw/api/{path}"
    req = urllib.request.Request(url, headers=_sws_headers)
    with opener.open(req, timeout=30) as resp:
        return json.loads(resp.read())


@app.get("/api/sws/industries")
async def get_sws_industries(indextype: str = "一级行业"):
    """获取申万行业指数代码列表"""
    from urllib.parse import quote
    result = _sws_api(f"index_name/?indextype={quote(indextype)}")
    return result


@app.get("/api/sws/industry-kline")
async def get_sws_industry_kline(code: str, period: str = "DAY"):
    """获取指定行业指数的日K线数据（全量历史，无分页）"""
    path = f"index_publish/trend/?swindexcode={code}&period={period}"
    result = _sws_api(path)
    return result


@app.get("/api/sws/industry-realtime")
async def get_sws_industry_realtime(indextype: str = "一级行业"):
    """获取所有行业的当日实时行情"""
    from urllib.parse import quote
    path = f"index_publish/current/?indextype={quote(indextype)}&page=1&page_size=100"
    result = _sws_api(path)
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

