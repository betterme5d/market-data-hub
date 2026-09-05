import logging
import httpx
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

from core.models import UnifiedQuote
from providers.base import BaseProvider
from core.exceptions import BusinessException

logger = logging.getLogger(__name__)

class XueqiuProvider(BaseProvider):
    def __init__(self, base_url: str = "https://stock.xueqiu.com/", 
                 auth_service_url: str = "http://localhost:8081",
                 user_agent: Optional[str] = None):
        self.base_url = base_url.rstrip('/') + '/'
        self.auth_service_url = auth_service_url.rstrip('/')
        self.default_ua = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        
        # 缓存的 Cookie 和 User-Agent
        self._cookie: Optional[str] = None
        self._user_agent: str = self.default_ua
        self._cookie_expiry: float = 0.0

    def _safe_float(self, val: Any) -> Optional[float]:
        try:
            if val is None or str(val).strip() == "":
                return None
            f = float(val)
            import math
            return f if math.isfinite(f) else None
        except (ValueError, TypeError):
            return None

    async def _ensure_cookie(self) -> Tuple[str, str]:
        """
        保证雪球 Cookie 的有效性
        """
        import time
        # 如果 Cookie 存在且未过期（如 10 分钟内不重复请求网关），直接返回
        if self._cookie and time.time() < self._cookie_expiry:
            return self._cookie, self._user_agent

        # 1. 尝试从本地 Playwright 鉴权网关获取
        try:
            async with httpx.AsyncClient(timeout=3, verify=False) as client:
                resp = await client.get(f"{self.auth_service_url}/xueqiu/auth")
                if resp.status_code == 200:
                    res_json = resp.json()
                    if res_json.get("success"):
                        self._cookie = res_json.get("cookie")
                        self._user_agent = res_json.get("userAgent", self.default_ua)
                        self._cookie_expiry = time.time() + 600  # 10 分钟缓存
                        logger.info("Successfully fetched Xueqiu Cookie from auth service gateway.")
                        return self._cookie, self._user_agent
        except Exception as e:
            logger.warning(f"Fetch Xueqiu Cookie from gateway failed: {e}. Fallback to direct fetching.")

        # 2. 备用方案：直接向雪球官网发起 GET 请求抓取 Cookies
        try:
            async with httpx.AsyncClient(timeout=5, verify=False) as client:
                headers = {"User-Agent": self.default_ua}
                resp = await client.get("https://xueqiu.com/", headers=headers)
                cookies_list = []
                for name, value in resp.cookies.items():
                    cookies_list.append(f"{name}={value}")
                
                if cookies_list:
                    self._cookie = "; ".join(cookies_list)
                    self._user_agent = self.default_ua
                    self._cookie_expiry = time.time() + 600
                    logger.info("Successfully fetched Xueqiu Cookie directly from xueqiu.com")
                    return self._cookie, self._user_agent
        except Exception as ex:
            logger.error(f"Fetch Xueqiu Cookie directly from xueqiu.com failed: {ex}")
            
        # 3. 终极兜底：如果没有 Cookie，尝试空 Cookie 返回 (雪球部分行情接口有时免 Cookie)
        return self._cookie or "", self._user_agent

    async def get_quotes(self, symbols: List[str], with_depth: bool = False) -> Dict[str, UnifiedQuote]:
        """
        批量获取实时行情 (雪球核心推荐方法)
        """
        if not symbols:
            return {}

        cookie, ua = await self._ensure_cookie()
        headers = {
            "User-Agent": ua,
            "Cookie": cookie,
            "Referer": "https://xueqiu.com/"
        }

        # 转换并拼接 Symbol
        # 雪球的 Symbol 期望是大写，例如 SH510300
        upper_symbols = [s.upper() for s in symbols]
        symbols_str = ",".join(upper_symbols)
        
        import time
        timestamp = int(time.time() * 1000)
        url = f"{self.base_url}v5/stock/realtime/quotec.json?symbol={symbols_str}&_={timestamp}"

        try:
            async with httpx.AsyncClient(timeout=5, verify=False) as client:
                resp = await client.get(url, headers=headers)
                
                # 如果遇到 400/403，说明 Cookie 可能失效，强制清除缓存并重试一次
                if resp.status_code in [400, 403]:
                    logger.warning(f"Xueqiu returned {resp.status_code}, clearing cookie and retrying...")
                    self._cookie = None
                    cookie, ua = await self._ensure_cookie()
                    headers["Cookie"] = cookie
                    headers["User-Agent"] = ua
                    resp = await client.get(url, headers=headers)

                resp.raise_for_status()
                res_data = resp.json()
                logger.debug("Xueqiu API returned raw JSON: %s", res_data)
                
                if res_data.get("error_code") != 0:
                    raise Exception(f"Xueqiu API error: {res_data.get('error_description')}")
                
                items = res_data.get("data", [])
                result_dict = {}
                
                for item in items:
                    raw_symbol = item.get("symbol")
                    if not raw_symbol:
                        continue
                    
                    price = self._safe_float(item.get("current")) or 0.0
                    last_close = self._safe_float(item.get("last_close"))
                    
                    # 昨收安全防御
                    if not last_close or last_close <= 0:
                        last_close = price

                    # 涨跌幅百分比直接保存
                    percent = self._safe_float(item.get("percent"))

                    # 换手率百分比直接保存
                    turnover_rate = self._safe_float(item.get("turnover_rate"))

                    # 振幅百分比直接保存
                    amplitude = self._safe_float(item.get("amplitude"))

                    # 确定时间
                    ts = item.get("timestamp")
                    update_time = None
                    if ts:
                        update_time = datetime.fromtimestamp(ts / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
                    
                    date_str = update_time[:10] if update_time else datetime.now().strftime("%Y-%m-%d")

                    # 判断 Exchange
                    market = raw_symbol[:2].upper() if len(raw_symbol) >= 2 else None

                    q = UnifiedQuote(
                        symbol=raw_symbol.lower(),  # 统一小写键返回
                        name=item.get("name") or raw_symbol,
                        date=date_str,
                        price=price,
                        last_close=last_close,
                        open=self._safe_float(item.get("open")),
                        high=self._safe_float(item.get("high")),
                        low=self._safe_float(item.get("low")),
                        close=price,
                        change=self._safe_float(item.get("chg")),
                        percent=percent,
                        volume=self._safe_float(item.get("volume")),
                        amount=self._safe_float(item.get("amount")),
                        turnover_rate=turnover_rate,
                        amplitude=amplitude,
                        update_time=update_time,
                        currency="CNY",
                        exchange=market,
                        security_type="stock",
                        source="xueqiu",
                        timestamp=datetime.utcnow().isoformat()
                    )
                    # 同时用原始大写和小写做 Key 兼容
                    result_dict[raw_symbol.lower()] = q
                    result_dict[raw_symbol.upper()] = q
                    
                return result_dict
        except Exception as e:
            logger.error(f"Xueqiu batch fetch failed for {symbols}: {e}")
            raise e

    async def get_quote(self, symbol: str, with_depth: bool = False) -> Tuple[UnifiedQuote, int]:
        res_dict = await self.get_quotes([symbol], with_depth=with_depth)
        # 兼容大小写 Key 获取
        q = res_dict.get(symbol.lower()) or res_dict.get(symbol.upper())
        if not q:
            raise BusinessException(f"Xueqiu returned empty result for symbol {symbol}")
        return q, 10

    async def get_history(self, symbol: str, period: str, interval: str, 
                          start: Optional[str], end: Optional[str], adj: str) -> dict:
        """
        获取雪球历史 K 线
        """
        # 1. 映射复权类型
        adjust_type = "normal"
        if adj == "qfq":
            adjust_type = "before"
        elif adj == "hfq":
            adjust_type = "after"
            
        # 2. 映射 K 线周期
        period_str = "day"
        if interval == "1wk":
            period_str = "week"
        elif interval == "1mo":
            period_str = "month"

        # 3. 估算获取的数据条数
        count = 1000
        if start and end:
            try:
                from datetime import datetime
                d1 = datetime.strptime(start, "%Y-%m-%d")
                d2 = datetime.strptime(end, "%Y-%m-%d")
                days = (d2 - d1).days
                count = max(100, min(3000, int(days * 1.5)))
            except Exception:
                pass

        # 4. 获取 begin 毫秒时间戳 (向前取 count 条)
        import time
        begin_ts = int(time.time() * 1000)
        if end:
            try:
                from datetime import datetime
                dt = datetime.strptime(end, "%Y-%m-%d")
                begin_ts = int(datetime(dt.year, dt.month, dt.day, 23, 59, 59).timestamp() * 1000)
            except Exception:
                pass

        # 5. 请求雪球 K 线接口
        # 雪球的 Symbol 期望是大写，例如 SH510300
        upper_symbol = symbol.upper()
        url = f"https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol={upper_symbol}&begin={begin_ts}&period={period_str}&type={adjust_type}&count=-{count}&indicator=kline"
        
        cookie, ua = await self._ensure_cookie()
        headers = {
            "User-Agent": ua,
            "Cookie": cookie,
            "Referer": "https://xueqiu.com/"
        }

        try:
            async with httpx.AsyncClient(timeout=10, verify=False) as client:
                resp = await client.get(url, headers=headers)
                
                # 如果遇到 400/403，自愈重试
                if resp.status_code in [400, 403]:
                    logger.warning("Xueqiu KLine cookie expired, clearing and retrying...")
                    self._cookie = None
                    cookie, ua = await self._ensure_cookie()
                    headers["Cookie"] = cookie
                    headers["User-Agent"] = ua
                    resp = await client.get(url, headers=headers)
                    
                if resp.status_code in [400, 403] and "xq_a_token" not in (cookie or ""):
                    raise BusinessException("Xueqiu KLine API requires a valid 'xq_a_token' cookie. Please make sure the auth_service is running on port 8081.")
                    
                resp.raise_for_status()
                res_data = resp.json()
                logger.debug("Xueqiu KLine API returned raw JSON: %s", res_data)
                
                if res_data.get("error_code") != 0:
                    raise BusinessException(f"Xueqiu API error: {res_data.get('error_description')}")
                    
                data = res_data.get("data", {})
                if not data:
                    return {"symbol": symbol, "data": []}
                    
                column = data.get("column", [])
                items = data.get("item", [])
                
                if not column or not items:
                    return {"symbol": symbol, "data": []}
                    
                col_map = {col: idx for idx, col in enumerate(column)}
                
                # 必须包含日期戳和收盘价
                if "timestamp" not in col_map or "close" not in col_map:
                    raise Exception("Missing vital column mappings (timestamp, close) from Xueqiu response")
                    
                data_list = []
                for row in items:
                    ts = row[col_map["timestamp"]]
                    from datetime import datetime, timezone, timedelta
                    # 转为本地日期
                    dt = datetime.fromtimestamp(ts / 1000.0)
                    dt_str = dt.strftime("%Y-%m-%d")
                    
                    if start and dt_str < start:
                        continue
                    if end and dt_str > end:
                        continue
                        
                    open_val = row[col_map["open"]] if "open" in col_map else 0.0
                    high_val = row[col_map["high"]] if "high" in col_map else 0.0
                    low_val = row[col_map["low"]] if "low" in col_map else 0.0
                    close_val = row[col_map["close"]] if "close" in col_map else 0.0
                    volume_val = row[col_map["volume"]] if "volume" in col_map else 0.0
                    amount_val = row[col_map["amount"]] if "amount" in col_map else 0.0
                    
                    chg_val = row[col_map["chg"]] if "chg" in col_map else None
                    percent_val = row[col_map["percent"]] if "percent" in col_map else None
                    turnoverrate_val = row[col_map["turnoverrate"]] if "turnoverrate" in col_map else None
                    
                    data_list.append({
                        "date": dt_str,
                        "open": self._safe_float(open_val) or 0.0,
                        "high": self._safe_float(high_val) or 0.0,
                        "low": self._safe_float(low_val) or 0.0,
                        "close": self._safe_float(close_val) or 0.0,
                        "volume": self._safe_float(volume_val) or 0.0,
                        "amount": self._safe_float(amount_val) or 0.0,
                        "change": self._safe_float(chg_val),
                        "percent": self._safe_float(percent_val),
                        "turnover_rate": self._safe_float(turnoverrate_val)
                    })
                    
                data_list = sorted(data_list, key=lambda x: x["date"])
                return {"symbol": symbol, "data": data_list}
                
        except Exception as e:
            logger.error(f"Failed to fetch history for Xueqiu {symbol}: {e}")
            raise e
