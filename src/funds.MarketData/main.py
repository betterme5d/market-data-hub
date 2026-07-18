from fastapi import FastAPI, HTTPException, Query, Path
from typing import Optional, Dict, List
from pydantic import BaseModel
import logging
import httpx

from core.models import UnifiedQuoteOut
from core.cache import get_cached_quote, set_cached_quote
from core.dispatcher import QuoteDispatcher
from providers.yfinance import YFinanceProvider
from providers.sina import SinaProvider
from providers.tencent import TencentProvider
from providers.xueqiu import XueqiuProvider
from providers.eastmoney import EastmoneyProvider
from providers.kraneshares import KraneSharesProvider
from providers.ishares import IsharesProvider
from providers.exchange_provider import ExchangeProvider
from core.exceptions import BusinessException

# 配置日志
import os
DEBUG_MODE = os.getenv("DEBUG", "false").lower() == "true"
LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# 调试模式下强开底层包的 DEBUG 级日志
if DEBUG_MODE:
    logging.getLogger("providers").setLevel(logging.DEBUG)
    logging.getLogger("core").setLevel(logging.DEBUG)
    logging.getLogger("httpx").setLevel(logging.DEBUG)
    logger.info("Debug mode enabled. Full execution logs are active.")

app = FastAPI(
    title="Market Data Proxy Service",
    description="提供集成的行情代理与指数数据接口，支持实时报价、历史K线、申万行业指数、折溢价率以及公募基金数据查询。",
    version="1.0.0"
)

# 实例化提供商
yfinance_provider = YFinanceProvider()
sina_provider = SinaProvider()
tencent_provider = TencentProvider()
xueqiu_provider = XueqiuProvider()

PROVIDERS = {
    "yfinance": yfinance_provider,
    "sina": sina_provider,
    "tencent": tencent_provider,
    "xueqiu": xueqiu_provider
}

eastmoney_provider = EastmoneyProvider()
krane_provider = KraneSharesProvider()
ishares_provider = IsharesProvider()
exchange_provider = ExchangeProvider()

def route_provider(symbol: str, source: Optional[str] = None) -> str:
    """
    根据标的代码特征或显式参数选择行情提供商。
    """
    if source and source.lower() in PROVIDERS:
        return source.lower()
        
    symbol_lower = symbol.lower()
    if symbol_lower.startswith(("nf_", "hf_")):
        return "sina"
    elif symbol_lower.endswith((".sh", ".sz")) or symbol_lower.startswith(("sh", "sz")):
        return "xueqiu"
    return "yfinance"

@app.get("/health", tags=["系统监控"], summary="服务健康检查")
async def health():
    """
    检查服务及其依赖组件（如 Valkey 缓存数据库）的连接状态。
    """
    from core.cache import redis_client
    return {"status": "ok", "valkey": "connected" if redis_client else "disconnected"}

@app.get("/api/sources/status", tags=["系统监控"], summary="获取各行情源实时健康状态与熔断指标")
async def get_sources_status():
    """
    暴露所有已注册行情数据源的实时熔断状态、失败故障计数及隔离剩余秒数。
    """
    sources = list(PROVIDERS.keys())
    status_list = []
    for src in sources:
        metrics = QuoteDispatcher.get_source_metrics(src)
        status_list.append(metrics)
    return status_list

@app.post("/api/sources/unblock", tags=["系统监控"], summary="手动重新启用（解封）指定行情数据源")
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

def translate_standard_symbol(symbol: str) -> Dict[str, str]:
    """
    根据标的特征翻译为数据源的标准代码。
    例如：
      - 510300.SH -> {"tencent": "sh510300", "sina": "sh510300", "xueqiu": "SH510300"}
      - 00700.HK -> {"tencent": "hk00700", "sina": "rt_hk00700", "xueqiu": "00700"}
      - AAPL -> {"yfinance": "AAPL", "xueqiu": "AAPL", "sina": "gb_aapl"}
    """
    symbol_upper = symbol.upper()
    
    # 1. 新浪/腾讯/雪球专有格式
    if symbol.lower().startswith(("sh", "sz", "hk", "rt_hk", "gb_", "nf_", "hf_", "fx_", "znb_")):
        s = symbol.lower()
        if s.startswith("rt_hk"):
            return {"sina": s, "xueqiu": s[5:]}
        elif s.startswith("gb_"):
            return {"sina": s, "yfinance": s[3:].upper(), "xueqiu": s[3:].upper()}
        elif s.startswith("nf_") or s.startswith("hf_"):
            return {"sina": s}
        elif s.startswith("fx_"):
            return {"xueqiu": s.upper()}
        elif s.startswith("sh") or s.startswith("sz"):
            return {"tencent": s, "sina": s, "xueqiu": s.upper()}
        return {"sina": s}

    # 2. 带有后缀的标准化格式 (510300.SH, 000001.SZ, 00700.HK 等)
    if "." in symbol_upper:
        code, suffix = symbol_upper.split('.', 1)
        if suffix in ["SH", "SS"]:
            return {
                "tencent": f"sh{code}",
                "sina": f"sh{code}",
                "xueqiu": f"SH{code}"
            }
        elif suffix == "SZ":
            return {
                "tencent": f"sz{code}",
                "sina": f"sz{code}",
                "xueqiu": f"SZ{code}"
            }
        elif suffix == "HK":
            padded_code = code.zfill(5)
            return {
                "tencent": f"hk{padded_code}",
                "sina": f"rt_hk{padded_code}",
                "xueqiu": padded_code
            }

    # 3. 常见美股标的代码 (2-5位英文字母)
    if symbol_upper.isalpha() and 2 <= len(symbol_upper) <= 5:
        return {
            "yfinance": symbol_upper,
            "xueqiu": symbol_upper,
            "sina": f"gb_{symbol.lower()}",
            "tencent": f"us{symbol_upper}"
        }

    # 4. 透传做为兜底
    return {"yfinance": symbol, "xueqiu": symbol_upper, "sina": symbol.lower()}

class BatchQuoteItem(BaseModel):
    symbol: str
    allowed_sources: Dict[str, str]

class BatchQuoteRequest(BaseModel):
    items: List[BatchQuoteItem]

@app.post("/quote/batch", response_model=List[UnifiedQuoteOut], tags=["行情数据"], summary="批量获取实时行情")
async def get_quotes_batch(request: BatchQuoteRequest, with_depth: bool = Query(False, description="是否包含五档深度盘口数据")):
    """
    批量获取多个证券的最新实时报价。
    C#端通过此接口发送包含每个标的可用源映射的字典。
    """
    try:
        items_dict = [{"symbol": item.symbol, "allowed_sources": item.allowed_sources} for item in request.items]
        results = await QuoteDispatcher.get_quotes_batch(items_dict, with_depth=with_depth)
        return results
    except Exception as e:
        logger.error(f"Batch quote fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/quote/{symbol}", response_model=UnifiedQuoteOut, tags=["行情数据"], summary="获取实时行情")
async def get_quote(
    symbol: str = Path(..., description="标的代码，例如 AAPL 或 000001.SZ"), 
    source: Optional[str] = Query(None, description="强制行情提供商（如 tencent, sina, xueqiu, yfinance），不填则自动多源 Fallback"),
    with_depth: bool = Query(False, description="是否包含五档深度盘口数据")
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
        result = await QuoteDispatcher.get_quote_with_fallback(symbol, allowed_sources, with_depth=with_depth)
        return result
    except BusinessException as be:
        raise HTTPException(status_code=400, detail=str(be))
    except Exception as e:
        logger.error(f"Get quote failed for {symbol}: {e}")
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
    except BusinessException as be:
        raise HTTPException(status_code=400, detail=str(be))
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

@app.get("/api/v1/exchange/funds", tags=["基金数据"], summary="获取上交所和深交所ETF/LOF列表")
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
    抓取当日所有申万行业的盘中实时估值与涨跌幅排行数据（已转换为日K线类似格式）。
    """
    return await get_sws_bulk_industry_kline(indextype)


@app.get("/api/sws/bulk-industry-kline", tags=["申万行业"], summary="获取指定等级下所有行业的当日K线数据(批量)")
async def get_sws_bulk_industry_kline(
    indextype: str = Query("一级行业", description="行业类别等级，如一级行业, 二级行业")
):
    """
    批量抓取当日所有申万行业的估值行情，并从详情接口动态提取日期，拼接为标准K线格式返回。
    """
    from urllib.parse import quote
    
    # 1. 批量获取实时行情 (使用 page_size=200 以保证一次性获取完二级行业的 134 个指数)
    path = f"index_publish/current/?indextype={quote(indextype)}&page=1&page_size=200"
    try:
        bulk_result = _sws_api(path)
    except Exception as e:
        logger.error(f"Failed to fetch bulk current data for {indextype}: {e}")
        return {"code": "500", "message": f"获取批量数据失败: {str(e)}", "data": []}
    
    results = bulk_result.get("data", {}).get("results", [])
    if not results:
        return {"code": "200", "message": "ok", "data": []}
    
    # 2. 动态取出第一个指数的 code 来获取它的交易日期
    first_code = results[0].get("swindexcode")
    date_str = None
    if first_code:
        try:
            date_path = f"index_publish/details/index_spread/?swindexcode={first_code}"
            date_result = _sws_api(date_path)
            if date_result.get("code") == "200" and date_result.get("data"):
                raw_date = date_result["data"][0].get("trading_date")  # 格式如 "20260702"
                if raw_date and len(raw_date) == 8:
                    date_str = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        except Exception as e:
            logger.error(f"Failed to fetch trading date for index {first_code}: {e}")
            
    # 3. 严格规则校验：如果日期获取失败，返回空数据且 code=500 (选项B)
    if not date_str:
        return {"code": "500", "message": "获取交易日期失败", "data": []}
        
    # 4. 组装并映射为类似日K线结构
    data_list = []
    for item in results:
        try:
            close_val = float(item.get("l3", 0))
            open_val = float(item.get("l4", 0))
            high_val = float(item.get("l6", 0))
            low_val = float(item.get("l7", 0))
            pre_close = float(item.get("l8", 0))
            
            # 计算 markup 涨跌幅 %
            markup_val = round(((close_val - pre_close) / pre_close * 100), 2) if pre_close > 0 else 0.0
            
            data_list.append({
                "swindexcode": item.get("swindexcode"),
                "swindexname": item.get("swindexname"),
                "bargaindate": date_str,
                "openindex": open_val,
                "maxindex": high_val,
                "minindex": low_val,
                "closeindex": close_val,
                "markup": markup_val,
                "bargainamount": float(item.get("l5", 0)) / 100.0,
                "bargainsum": float(item.get("l11", 0)) / 100.0
            })
        except Exception as ex:
            logger.warning(f"Error parsing bulk item {item}: {ex}")
            
    return {
        "code": "200",
        "message": "ok",
        "data": data_list
    }


@app.get("/quote/unblock-all", tags=["调试与维护"], summary="一键清除所有数据源的熔断状态")
async def unblock_all_sources():
    if not redis_client:
        return {"status": "error", "message": "Valkey/Redis 缓存未连接"}
    try:
        from core.dispatcher import get_blocked_key
        cleared = []
        for src in ["tencent", "sina", "xueqiu", "yfinance"]:
            key = get_blocked_key(src)
            res = redis_client.delete(key)
            if res > 0:
                cleared.append(src)
        return {"status": "ok", "cleared_sources": cleared}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
