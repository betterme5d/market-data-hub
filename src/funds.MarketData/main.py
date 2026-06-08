from fastapi import FastAPI, HTTPException, Query, Path
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

app = FastAPI(
    title="Market Data Proxy Service",
    description="提供集成的行情代理与指数数据接口，支持实时报价、历史K线、申万行业指数、折溢价率以及公募基金数据查询。",
    version="1.0.0"
)

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

@app.get("/health", tags=["系统监控"], summary="服务健康检查")
async def health():
    """
    检查服务及其依赖组件（如 Valkey 缓存数据库）的连接状态。
    """
    from core.cache import redis_client
    return {"status": "ok", "valkey": "connected" if redis_client else "disconnected"}

@app.get("/api/krane/premium-discount/{pid}", tags=["折溢价率"], summary="获取 KraneShares 基金折溢价率")
async def get_krane_premium_discount(
    pid: str = Path(..., description="产品 ID，例如 KWEB"),
    start: str = Query(..., description="开始日期 (格式: YYYY-MM-DD)"),
    end: str = Query(..., description="结束日期 (格式: YYYY-MM-DD)")
):
    """
    拉取 KraneShares 官网的 ETF 产品历史折溢价数据，并按日期范围过滤返回。
    """
    try:
        return await krane_provider.get_premium_discount(pid, start, end)
    except Exception as e:
        logger.error(f"Get KraneShares premium discount failed for {pid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ishares/premium-discount/{product_path:path}", tags=["折溢价率"], summary="获取 iShares 基金折溢价率")
async def get_ishares_premium_discount(
    product_path: str = Path(..., description="iShares 官网的产品路径标识"),
    start: str = Query(..., description="开始日期 (格式: YYYY-MM-DD)"),
    end: str = Query(..., description="结束日期 (格式: YYYY-MM-DD)")
):
    """
    拉取并解析 iShares 官网的 ETF 产品历史折溢价率数据。
    """
    try:
        return await ishares_provider.get_premium_discount(product_path, start, end)
    except Exception as e:
        logger.error(f"Get iShares premium discount failed for {product_path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/quote/{symbol}", response_model=UnifiedQuoteOut, tags=["行情数据"], summary="获取实时行情")
async def get_quote(
    symbol: str = Path(..., description="标的代码，例如 AAPL 或 000001.SZ"), 
    source: Optional[str] = Query(None, description="行情提供商（如 yfinance），不填则自动路由")
):
    """
    获取股票或 ETF 的最新实时报价，优先从 Valkey 缓存读取，未命中则调用底端接口并缓存。
    """
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

@app.get("/history/{symbol}", tags=["行情数据"], summary="获取历史 K 线")
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
    except Exception as e:
        logger.error(f"Get history failed for {symbol} via {provider_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/info/{symbol}", tags=["行情数据"], summary="获取标的信息")
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

@app.get("/trading-days", tags=["系统监控"], summary="获取指定市场的交易日历")
async def get_trading_days(
    exchange: str = Query(..., description="交易所或市场标识：CN (中国大陆), HK (香港), US (美国)"),
    start: str = Query(..., description="开始日期 (格式: YYYY-MM-DD)"),
    end: str = Query(..., description="结束日期 (格式: YYYY-MM-DD)")
):
    """
    获取某个交易所在指定日期范围内的所有开市交易日列表。
    """
    try:
        from core.calendar import get_exchange_trading_days
        days = get_exchange_trading_days(exchange, start, end)
        return {"exchange": exchange, "days": days}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get trading days for {exchange} ({start} ~ {end}): {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/fund/{symbol}/portfolio", tags=["基金数据"], summary="获取基金持仓结构")
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

@app.get("/fund/{symbol}/info", tags=["基金数据"], summary="获取基金基本信息")
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

@app.get("/api/eastmoney/valuations", tags=["基金数据"], summary="获取东财基金估值列表")
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

@app.get("/api/public/fund_purchase_em", tags=["AkShare公共数据"], summary="获取东方财富基金申购状态表")
async def get_fund_purchase_em():
    """
    调用 AkShare 的 fund_purchase_em 数据接口，获取公募基金的申购与赎回状态限制。
    """
    try:
        import akshare as ak
        df = ak.fund_purchase_em()
        return clean_dataframe(df)
    except Exception as e:
        logger.error(f"Failed to fetch fund_purchase_em: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/public/fund_scale_open_sina", tags=["AkShare公共数据"], summary="获取新浪开放式基金规模数据")
async def get_fund_scale_open_sina(
    symbol: str = Query(..., description="基金类别，例如：股票型基金, 混合型基金, 债券型基金, 指数型基金, QDII基金")
):
    """
    通过 AkShare 获取新浪财经上指定类型的开放式基金规模排行和份额明细。
    """
    try:
        import akshare as ak
        df = ak.fund_scale_open_sina(symbol=symbol)
        return clean_dataframe(df)
    except Exception as e:
        logger.error(f"Failed to fetch fund_scale_open_sina for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/xueqiu/kline", tags=["雪球数据"], summary="转发雪球K线获取请求")
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


@app.get("/api/sws/industries", tags=["申万行业"], summary="获取申万行业指数代码列表")
async def get_sws_industries(
    indextype: str = Query("一级行业", description="行业类别等级，例如：一级行业, 二级行业, 三级行业")
):
    """
    抓取申万研究官网的一级、二级、三级行业指数名称与代码映射。
    """
    from urllib.parse import quote
    result = _sws_api(f"index_name/?indextype={quote(indextype)}")
    return result


@app.get("/api/sws/industry-kline", tags=["申万行业"], summary="获取指定行业指数的历史K线数据")
async def get_sws_industry_kline(
    code: str = Query(..., description="申万行业指数代码，例如 801010"), 
    period: str = Query("DAY", description="数据间隔周期，默认 DAY")
):
    """
    从申万官网抓取该指数自发布以来的全量日K线行情历史数据（无分页）。
    """
    path = f"index_publish/trend/?swindexcode={code}&period={period}"
    result = _sws_api(path)
    return result


@app.get("/api/sws/industry-realtime", tags=["申万行业"], summary="获取所有申万行业的当日实时行情")
async def get_sws_industry_realtime(
    indextype: str = Query("一级行业", description="行业类别等级，如一级行业, 二级行业")
):
    """
    抓取当日所有申万行业的盘中实时估值与涨跌幅排行数据。
    """
    from urllib.parse import quote
    path = f"index_publish/current/?indextype={quote(indextype)}&page=1&page_size=100"
    result = _sws_api(path)
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
