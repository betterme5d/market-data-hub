import logging
import httpx
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List

from core.models import UnifiedQuote
from providers.base import BaseProvider
from core.exceptions import BusinessException

logger = logging.getLogger(__name__)

TENCENT_MAP = {
    "MK": 0,
    "NAME": 1,
    "CODE": 2,
    "PRICE": 3,
    "CP": 4,
    "OP": 5,
    "TOTAL": 6,
    "OUTER_DISC": 7,
    "INNER_DISC": 8,
    "B1_VAL": 9,
    "B1_VOL": 10,
    "B2_VAL": 11,
    "B2_VOL": 12,
    "B3_VAL": 13,
    "B3_VOL": 14,
    "B4_VAL": 15,
    "B4_VOL": 16,
    "B5_VAL": 17,
    "B5_VOL": 18,
    "S1_VAL": 19,
    "S1_VOL": 20,
    "S2_VAL": 21,
    "S2_VOL": 22,
    "S3_VAL": 23,
    "S3_VOL": 24,
    "S4_VAL": 25,
    "S4_VOL": 26,
    "S5_VAL": 27,
    "S5_VOL": 28,
    "DETAIL": 29,
    "TIME": 30,
    "CHANGE": 31,
    "CHANGE_RATE": 32,
    "MAX_VAL": 33,
    "MIN_VAL": 34,
    "FORMER": 35,
    "VOLUME": 36,
    "TURNOVER": 37,
    "TURNOVER_RATE": 38,
    "PE_RATIO": 39,
    "STATUS": 40,
    "MAX_VAL_2": 41,
    "MIN_VAL_2": 42,
    "AMPLITUDE": 43,
    "MCCS": 44,
    "MARKET_CAP": 45,
    "RISE_STOP": 47,
    "FALL_STOP": 48,
    "VOLUME_RATIO": 49
}

class TencentProvider(BaseProvider):
    def __init__(self, base_url: str = "https://sqt.gtimg.cn/", user_agent: Optional[str] = None):
        self.base_url = base_url.rstrip('/') + '/'
        self.headers = {
            "User-Agent": user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://gu.qq.com/"
        }

    def _get_val(self, t: List[str], key: str, default: Any = None) -> Any:
        idx = TENCENT_MAP.get(key)
        if idx is not None and idx < len(t):
            val = t[idx]
            return val if val != "" else default
        return default

    def _get_float(self, t: List[str], key: str, default: Optional[float] = None) -> Optional[float]:
        val = self._get_val(t, key)
        if val is None or str(val).strip() == "":
            return default
        try:
            f = float(val)
            import math
            return f if math.isfinite(f) else default
        except (ValueError, TypeError):
            return default

    def _safe_float(self, val: Any) -> Optional[float]:
        try:
            if val is None or str(val).strip() == "":
                return None
            f = float(val)
            import math
            return f if math.isfinite(f) else None
        except (ValueError, TypeError):
            return None

    def _parse_datetime(self, date_str: str) -> Optional[str]:
        # 腾讯返回的时间格式通常是 YYYYMMDDHHmmss 或是 YYYY-MM-DD HH:mm:ss，港股为 YYYY/MM/DD HH:mm:ss
        if not date_str or len(date_str) < 8:
            return None
        date_str = date_str.replace("-", "").replace("/", "").replace(" ", "").replace(":", "")
        try:
            if len(date_str) >= 14:
                dt = datetime.strptime(date_str[:14], "%Y%m%d%H%M%S")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            elif len(date_str) == 8:
                dt = datetime.strptime(date_str, "%Y%m%d")
                return dt.strftime("%Y-%m-%d 00:00:00")
        except Exception:
            pass
        return None

    async def get_quotes(self, symbols: List[str], with_depth: bool = False) -> Dict[str, UnifiedQuote]:
        if not symbols:
            return {}
            
        processed_symbols = []
        for s in symbols:
            s_lower = s.lower()
            if s_lower.startswith("us"):
                processed_symbols.append("us" + s[2:].upper())
            else:
                processed_symbols.append(s_lower)

        q_str = ",".join(processed_symbols)
        url = f"{self.base_url}?q={q_str}&fmt=json&app=wzq"
        
        async with httpx.AsyncClient(timeout=5, verify=False) as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            
            # 腾讯返回的是 GBK 编码
            content_str = response.content.decode("gbk", errors="ignore")
            import json
            data = json.loads(content_str)
            
            # 将键统一转为小写以兼容匹配
            data_lower = {k.lower(): v for k, v in data.items()}
            
            result_dict = {}
            logger.debug("Tencent API returned raw JSON: %s", data)
            for sym in symbols:
                sym_lower = sym.lower()
                if sym_lower not in data_lower:
                    continue
                t = data_lower[sym_lower]
                
                price = self._get_float(t, "PRICE") or 0.0
                last_close = self._get_float(t, "CP")
                
                # 昨收兜底
                if not last_close or last_close <= 0:
                    last_close = price

                # 涨跌幅百分比直接保存
                percent = self._get_float(t, "CHANGE_RATE")
                    
                # 换手率百分比直接保存
                turnover_rate = self._get_float(t, "TURNOVER_RATE")
                    
                # 振幅百分比直接保存
                amplitude = self._get_float(t, "AMPLITUDE")

                market = sym[:2].upper() if len(sym) >= 2 else None

                # 货币类型与成交额解析
                currency = "CNY"
                if market == "HK":
                    currency = "HKD"
                    amount = self._get_float(t, "TURNOVER")
                elif market == "US":
                    currency = "USD"
                    amount = self._get_float(t, "TURNOVER")
                else:
                    # A股成交额以万元为单位，转为元。优先取更精确的 t[57] (不在统一大表中，保留物理下标)
                    amount = self._safe_float(t[57]) if len(t) > 57 else self._get_float(t, "TURNOVER")
                    if amount is not None:
                        amount = amount * 10000.0
                
                # 确定行情交易日期
                update_time = self._parse_datetime(self._get_val(t, "TIME")) if len(t) > 30 else None
                date_str = update_time[:10] if update_time else datetime.now().strftime("%Y-%m-%d")

                # 解析五档买卖盘
                depth_data = None
                if with_depth and len(t) > 28:
                    from core.models import MarketDepth, DepthItem
                    bids = []
                    for i in range(5):
                        p = self._get_float(t, f"B{i+1}_VAL")
                        v = self._get_float(t, f"B{i+1}_VOL")
                        if p is not None or v is not None:
                            bids.append(DepthItem(price=p, volume=v))
                    asks = []
                    for i in range(5):
                        p = self._get_float(t, f"S{i+1}_VAL")
                        v = self._get_float(t, f"S{i+1}_VOL")
                        if p is not None or v is not None:
                            asks.append(DepthItem(price=p, volume=v))
                    depth_data = MarketDepth(bids=bids, asks=asks)

                q = UnifiedQuote(
                    symbol=sym,
                    name=self._get_val(t, "NAME", default=sym),
                    date=date_str,
                    price=price,
                    last_close=last_close,
                    open=self._get_float(t, "OP"),
                    high=self._get_float(t, "MAX_VAL"),
                    low=self._get_float(t, "MIN_VAL"),
                    close=price,
                    change=self._get_float(t, "CHANGE"),
                    percent=percent,
                    volume=self._get_float(t, "VOLUME"),
                    amount=amount,
                    turnover_rate=turnover_rate,
                    amplitude=amplitude,
                    update_time=update_time,
                    # IOPV/NAV 同样保留原来 A 股行情数组特有的第 78、81 位物理下标，避免影响 ETF
                    iopv=self._safe_float(t[78]) if len(t) > 78 else None,
                    nav=self._safe_float(t[81]) if len(t) > 81 else None,
                    currency=currency,
                    exchange=market,
                    security_type="stock",
                    source="tencent",
                    depth=depth_data,
                    timestamp=datetime.utcnow().isoformat()
                )
                result_dict[sym_lower] = q
                
            return result_dict

    async def get_quote(self, symbol: str, with_depth: bool = False) -> Tuple[UnifiedQuote, int]:
        res_dict = await self.get_quotes([symbol], with_depth=with_depth)
        q = res_dict.get(symbol.lower())
        if not q:
            raise BusinessException(f"Tencent returned empty result for symbol {symbol}")
        return q, 10

    async def get_history(self, symbol: str, period: str, interval: str, 
                          start: Optional[str], end: Optional[str], adj: str) -> dict:
        raise NotImplementedError("Tencent provider does not support get_history")
