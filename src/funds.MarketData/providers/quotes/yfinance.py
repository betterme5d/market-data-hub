import asyncio
import logging
import math
import pytz
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from typing import Optional, Tuple

from core.models import UnifiedQuote
from providers.base import BaseProvider
from core.cache import get_cached_anchor, set_cached_anchor
from core.exceptions import BusinessException


import os
import requests
from urllib.parse import urlparse, urlunparse
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

# 自定义 HTTPAdapter，拦截发往 yahoo 的请求并重定向到 Cloudflare Worker
class CFWorkerAdapter(HTTPAdapter):
    def __init__(self, cf_worker_url, *args, **kwargs):
        self.cf_worker_url = cf_worker_url.rstrip('/')
        self.cf_worker_netloc = urlparse(self.cf_worker_url).netloc
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        parsed = urlparse(request.url)
        # 如果是请求 yahoo，重写为发给 CF Worker
        if parsed.netloc.endswith('yahoo.com'):
            request.headers['X-Target-Host'] = parsed.netloc
            request.url = urlunparse((
                parsed.scheme,
                self.cf_worker_netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
        return super().send(request, **kwargs)

# 创建一个全局的 requests Session，并设置标准的浏览器 User-Agent
# 这样可以强制 yfinance 使用标准的 requests 库，而不再使用容易在老系统崩溃的 curl_cffi
_yf_session = requests.Session()
_yf_session.verify = False
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
_yf_session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
})

# 如果配置了环境变量 YF_CF_WORKER_URL，则挂载 CF 代理适配器
_cf_worker_url = os.getenv("YF_CF_WORKER_URL", "").strip()
_cf_worker_token = os.getenv("YF_CF_WORKER_TOKEN", "").strip()

if _cf_worker_url:
    _cf_adapter = CFWorkerAdapter(_cf_worker_url)
    _yf_session.mount("https://", _cf_adapter)
    _yf_session.mount("http://", _cf_adapter)
    if _cf_worker_token:
        _yf_session.headers.update({'X-Proxy-Auth': _cf_worker_token})
    logger.info(f"Enabled Cloudflare Worker Proxy for yfinance: {_cf_worker_url} (Auth: {'Yes' if _cf_worker_token else 'No'})")

def safe_float(val, default=0.0) -> float:
    try:
        f = float(val)
        return f if math.isfinite(f) else default
    except (ValueError, TypeError):
        return default

def get_market_ttl(fast_info) -> int:
    """
    根据市场状态动态计算缓存有效时间(TTL)，支持跨周末识别
    """
    try:
        # 获取市场状态: REGULAR, PRE, POST, CLOSED
        state = getattr(fast_info, 'market_state', 'REGULAR') or 'REGULAR'
        state = state.upper()
        
        # 如果是交易时段（含盘前盘后），保持 10 分钟更新
        if state != 'CLOSED' and state != 'POSTPOST':
            return 600
        
        # 如果已收盘，计算距离下一个交易日盘前的时间
        tz_name = getattr(fast_info, 'timezone', 'UTC')
        if not tz_name or not isinstance(tz_name, str):
            tz_name = 'UTC'
            
        try:
            tz = pytz.timezone(tz_name)
        except Exception:
            tz = pytz.timezone('UTC')
            
        now_tz = datetime.now(tz)
        
        # 计算需要增加的天数
        weekday = now_tz.weekday()
        days_to_add = 1
        
        if weekday == 4: # 周五收盘 -> 延至周一
            days_to_add = 3
        elif weekday == 5: # 周六 -> 延至周一
            days_to_add = 2
        
        # 目标时间：下一个交易日的凌晨 04:00
        target_time = (now_tz + timedelta(days=days_to_add)).replace(hour=4, minute=0, second=0, microsecond=0)
        
        ttl = int((target_time - now_tz).total_seconds())
        return max(600, min(ttl, 259200))
    except Exception as e:
        logger.warning(f"Calculate TTL failed: {e}")
        return 600

def _fetch_quote_sync(symbol: str) -> dict:
    """阻塞式拉取 yfinance 报价原始字段（**必须**在工作线程里调用）。

    Ticker/fast_info 的每个属性访问都可能触发一次网络往返，所以一次性在同一线程里取全，
    返回纯 Python 值，调用方只做无网络的组装（D6：同步网络调用跑在事件循环上会卡住整个服务）。
    """
    ticker = yf.Ticker(symbol, session=_yf_session)
    fast = ticker.fast_info

    data = {
        "price": safe_float(getattr(fast, "last_price", 0)),
        "last_close": safe_float(getattr(fast, "regular_market_previous_close", 0)),
        "open": safe_float(getattr(fast, "open", 0)),
        "high": safe_float(getattr(fast, "day_high", 0)),
        "low": safe_float(getattr(fast, "day_low", 0)),
        "volume": float(safe_float(getattr(fast, "last_volume", 0))),
        "currency": getattr(fast, "currency", None),
        "exchange": getattr(fast, "exchange", None),
        "ttl": get_market_ttl(fast),
    }

    # 价格日期（取不到就退回今天，不影响报价主体）
    try:
        ts = getattr(fast, "timestamp", None)
        if ts:
            data["price_date"] = pd.to_datetime(ts, unit="s", utc=True).strftime("%Y-%m-%d")
        else:
            h1 = ticker.history(period="1d", auto_adjust=False)
            data["price_date"] = (
                h1.index[-1].strftime("%Y-%m-%d")
                if not h1.empty
                else datetime.now().strftime("%Y-%m-%d")
            )
    except Exception:
        data["price_date"] = datetime.now().strftime("%Y-%m-%d")

    return data


class YFinanceProvider(BaseProvider):
    async def get_quote(self, symbol: str, with_depth: bool = False) -> Tuple[UnifiedQuote, int]:
        try:
            # D6：yfinance 是同步网络访问，整体搬到工作线程，避免把事件循环卡住数秒。
            data = await asyncio.to_thread(_fetch_quote_sync, symbol)

            # 获取基础价格 (安全防御：确保 NaN/None 变为 0)
            price = data["price"]
            last_close = data["last_close"]
            open_val = data["open"] or price
            high_val = data["high"] or price
            low_val = data["low"] or price

            # 涨跌计算
            change = price - last_close
            percent = (change / last_close * 100.0) if last_close != 0 else 0
            volume = data["volume"]

            result = UnifiedQuote(
                symbol=symbol,
                name=symbol,
                date=data["price_date"],
                price=round(price, 4),
                last_close=round(last_close, 4),
                change=round(change, 4),
                percent=round(percent, 6),
                open=round(open_val, 4),
                high=round(high_val, 4),
                low=round(low_val, 4),
                volume=volume,
                amount=round(float(volume) * float(price), 2),
                currency=data["currency"],
                exchange=data["exchange"],
                timestamp=datetime.utcnow().isoformat(),
                security_type="stock",
                source="yfinance"
            )

            return result, data["ttl"]

        except Exception as e:
            err_msg = str(e)
            if "No data found" in err_msg or "not found" in err_msg or "invalid" in err_msg:
                raise BusinessException(f"YahooFinance returned empty result for symbol {symbol} due to code invalidity: {err_msg}")
            logger.error(f"Error fetching quote for {symbol} via yfinance: {err_msg}")
            raise e

    async def get_history(self, symbol: str, period: str, interval: str, 
                          start: Optional[str], end: Optional[str], adj: str) -> dict:
        try:
            # D6：Ticker 构造本身不打网络，真正的网络访问（history/详情）必须放到工作线程
            ticker = yf.Ticker(symbol, session=_yf_session)
            if start and end:
                hist = await asyncio.to_thread(
                    ticker.history, start=start, end=end, interval=interval, auto_adjust=False
                )
            else:
                hist = await asyncio.to_thread(
                    ticker.history, period=period, interval=interval, auto_adjust=False
                )

            # 实时行情补充与追加
            if interval == "1d" and not hist.empty:
                try:
                    # 优先使用缓存以防重复拉取
                    from core.cache import get_cached_quote
                    q_res = get_cached_quote(symbol, provider="yfinance")
                    if not q_res:
                        q_model, _ = await self.get_quote(symbol)
                        q_res = q_model.model_dump()

                    q_date_str = q_res.get('date')
                    last_hist_date_str = hist.index[-1].strftime('%Y-%m-%d')
                    
                    # 如果日期相同，检查是否需要修补全 0 数据
                    if q_date_str == last_hist_date_str:
                        last_row = hist.iloc[-1]
                        # 只要 Open, High, Low, Close, Adj Close 中有任何一个为 0 或 NaN，就尝试修补
                        check_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close']
                        needs_patch = any(pd.isna(last_row[col]) or last_row[col] == 0 for col in check_cols)
                        
                        if needs_patch:
                            price = q_res.get('price', 0)
                            # 如果 q_res 中的开高低为 0，则用 price 兜底
                            q_open = q_res.get('open', 0) or price
                            q_high = q_res.get('high', 0) or price
                            q_low = q_res.get('low', 0) or price
                            
                            hist.loc[hist.index[-1], ['Open', 'High', 'Low', 'Close', 'Adj Close']] = [
                                q_open, q_high, q_low, price, price
                            ]
                            logger.info(f"Patched last row for {symbol} on {q_date_str} (reason: 0 or NaN found)")
                    
                    # 如果行情日期更新，则作为新的一行追加
                    elif q_date_str > last_hist_date_str:
                        price = q_res.get('price', 0)
                        q_open = q_res.get('open', 0) or price
                        q_high = q_res.get('high', 0) or price
                        q_low = q_res.get('low', 0) or price
                        
                        new_row = pd.DataFrame({
                            'Open': [q_open],
                            'High': [q_high],
                            'Low': [q_low],
                            'Close': [price],
                            'Adj Close': [price],
                            'Volume': [q_res.get('volume', 0)]
                        }, index=[pd.Timestamp(q_date_str)])
                        if hist.index.tz: new_row.index = new_row.index.tz_localize(hist.index.tz)
                        hist = pd.concat([hist, new_row])
                        logger.info(f"Appended new row for {symbol} on {q_date_str}")
                        
                except Exception as patch_ex:
                    logger.warning(f"History patch/append failed: {patch_ex}")

            # 过滤无效数据 (仅剔除依然为 0 的行)
            hist = hist[hist['Close'] > 0]
            if hist.empty: return {"symbol": symbol, "data": []}
                
            if adj == "hfq":
                anchor = get_cached_anchor(symbol, provider="yfinance")

                if not anchor:
                    full_hist = await asyncio.to_thread(
                        ticker.history, period="max", auto_adjust=False
                    )
                    if not full_hist.empty:
                        anchor = {"adj_close": float(full_hist.iloc[0]['Adj Close']), "close": float(full_hist.iloc[0]['Close'])}
                        set_cached_anchor(symbol, anchor, provider="yfinance")

                if anchor and anchor["adj_close"] > 0:
                    # 计算归一化因子：(当前复权比例) / (初始复权比例)
                    # 这样可以保证第一天的 Factor 为 1
                    initial_ratio = anchor["adj_close"] / anchor["close"]
                    adj_factor = (hist['Adj Close'] / hist['Close']) / initial_ratio
                    
                    for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
                        hist[col] = hist[col] * adj_factor
                else:
                    # 兜底：如果没拿到锚点，使用本段数据的第一行作为临时锚点
                    r0 = hist['Adj Close'].iloc[0] / hist['Close'].iloc[0]
                    adj_factor = (hist['Adj Close'] / hist['Close']) / r0
                    for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
                        hist[col] = hist[col] * adj_factor
                        
            elif adj == "qfq":
                adj_factor = hist['Adj Close'] / hist['Close']
                for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']: 
                    hist[col] = hist[col] * adj_factor

            hist['PreClose'] = hist['Close'].shift(1)
            hist['Amount'] = (hist['Close'] * hist['Volume'])
            hist['ChangeAmount'] = (hist['Close'] - hist['PreClose'])
            hist['ChangeRate'] = ((hist['Adj Close'] / hist['Adj Close'].shift(1)) - 1) * 100
            hist['Amplitude'] = (((hist['High'] - hist['Low']) / hist['PreClose']) * 100)

            # 统一精度处理 (保留 4 位小数)
            price_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'PreClose', 'ChangeAmount', 'ChangeRate', 'Amplitude', 'Amount']
            for col in price_cols:
                if col in hist.columns:
                    hist[col] = hist[col].round(4)

            hist.fillna(0, inplace=True)
            # 按日期降序排列 (最新的在前)
            hist.sort_index(ascending=False, inplace=True)
            
            hist.reset_index(inplace=True)
            date_col = 'Date' if 'Date' in hist.columns else ('Datetime' if 'Datetime' in hist.columns else 'index')
            hist['Date'] = hist[date_col].dt.strftime('%Y-%m-%d') if interval == "1d" else hist[date_col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            return {"symbol": symbol, "period": period, "interval": interval, "adj": adj, "data": hist.to_dict(orient='records')}

        except Exception as e:
            logger.error(f"Error fetching history for {symbol} via yfinance: {str(e)}")
            raise e

    async def get_info(self, symbol: str) -> dict:
        try:
            # D6：.info 是同步网络访问（且会拉多个上游接口），必须放到工作线程
            return await asyncio.to_thread(lambda: yf.Ticker(symbol, session=_yf_session).info)
        except Exception as e:
            logger.error(f"Error fetching info for {symbol} via yfinance: {str(e)}")
            raise e
