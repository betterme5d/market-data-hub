import logging
import httpx
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List

from core.models import UnifiedQuote
from providers.base import BaseProvider
from core.exceptions import BusinessException

logger = logging.getLogger(__name__)

class TencentProvider(BaseProvider):
    def __init__(self, base_url: str = "https://sqt.gtimg.cn/", user_agent: Optional[str] = None):
        self.base_url = base_url.rstrip('/') + '/'
        self.headers = {
            "User-Agent": user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://gu.qq.com/"
        }

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
        # 腾讯返回的时间格式通常是 YYYYMMDDHHmmss 或是 YYYY-MM-DD HH:mm:ss
        if not date_str or len(date_str) < 8:
            return None
        date_str = date_str.replace("-", "").replace(" ", "").replace(":", "")
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
            
        lower_symbols = [s.lower() for s in symbols]
        q_str = ",".join(lower_symbols)
        url = f"{self.base_url}?q={q_str}&fmt=json&app=wzq"
        
        async with httpx.AsyncClient(timeout=5, verify=False) as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            
            # 腾讯返回的是 GBK 编码
            content_str = response.content.decode("gbk", errors="ignore")
            import json
            data = json.loads(content_str)
            
            result_dict = {}
            logger.debug("Tencent API returned raw JSON: %s", data)
            for sym in lower_symbols:
                if sym not in data:
                    continue
                t = data[sym]
                if len(t) < 30:
                    continue
                
                price = self._safe_float(t[3]) or 0.0
                last_close = self._safe_float(t[4])
                
                # 昨收兜底
                if not last_close or last_close <= 0:
                    last_close = price

                # 涨跌幅百分比直接保存
                percent = self._safe_float(t[32])
                    
                # 换手率百分比直接保存
                turnover_rate = self._safe_float(t[38])
                    
                # 振幅百分比直接保存
                amplitude = self._safe_float(t[43])

                # 成交额以万元为单位，转为元
                amount = self._safe_float(t[57])
                if amount is not None:
                    amount = amount * 10000.0

                market = sym[:2].upper() if len(sym) >= 2 else None
                
                # 确定行情交易日期
                update_time = self._parse_datetime(t[30]) if len(t) > 30 else None
                date_str = update_time[:10] if update_time else datetime.now().strftime("%Y-%m-%d")

                # 解析五档买卖盘
                depth_data = None
                if with_depth and len(t) > 28:
                    from core.models import MarketDepth, DepthItem
                    bids = []
                    for i in range(5):
                        p = self._safe_float(t[9 + i * 2])
                        v = self._safe_float(t[10 + i * 2])
                        if p is not None or v is not None:
                            bids.append(DepthItem(price=p, volume=v))
                    asks = []
                    for i in range(5):
                        p = self._safe_float(t[19 + i * 2])
                        v = self._safe_float(t[20 + i * 2])
                        if p is not None or v is not None:
                            asks.append(DepthItem(price=p, volume=v))
                    depth_data = MarketDepth(bids=bids, asks=asks)

                q = UnifiedQuote(
                    symbol=sym,
                    name=t[1] if len(t) > 1 else sym,
                    date=date_str,
                    price=price,
                    last_close=last_close,
                    open=self._safe_float(t[5]),
                    high=self._safe_float(t[33]),
                    low=self._safe_float(t[34]),
                    close=price,
                    change=self._safe_float(t[31]),
                    percent=percent,
                    volume=self._safe_float(t[36]),
                    amount=amount,
                    turnover_rate=turnover_rate,
                    amplitude=amplitude,
                    update_time=update_time,
                    iopv=self._safe_float(t[78]) if len(t) > 78 else None,
                    nav=self._safe_float(t[81]) if len(t) > 81 else None,
                    currency="CNY",
                    exchange=market,
                    security_type="stock",
                    source="tencent",
                    depth=depth_data,
                    timestamp=datetime.utcnow().isoformat()
                )
                result_dict[sym] = q
                
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
