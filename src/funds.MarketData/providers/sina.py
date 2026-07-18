import logging
import httpx
import re
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

from core.models import UnifiedQuote
from providers.base import BaseProvider
from core.exceptions import BusinessException

logger = logging.getLogger(__name__)

class SinaProvider(BaseProvider):
    def __init__(self, base_url: str = "https://hq.sinajs.cn/", user_agent: Optional[str] = None):
        self.base_url = base_url.rstrip('/') + '/'
        self.headers = {
            "User-Agent": user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "http://finance.sina.com.cn"
        }

    def _safe_float(self, val: Any) -> Optional[float]:
        try:
            if val is None or str(val).strip() == "" or str(val).strip() == "--":
                return None
            f = float(val)
            import math
            return f if math.isfinite(f) else None
        except (ValueError, TypeError):
            return None

    def _parse_datetime(self, date_str: str, time_str: str) -> Optional[str]:
        if not date_str:
            return None
        dt_str = f"{date_str} {time_str}".strip()
        try:
            # 常见格式: YYYY-MM-DD HH:mm:ss
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        
        try:
            # 兼容期货或外汇无年月日时间格式，直接拼接今日
            dt = datetime.strptime(time_str, "%H:%M:%S")
            return f"{datetime.now().strftime('%Y-%m-%d')} {dt.strftime('%H:%M:%S')}"
        except Exception:
            pass
        
        return None

    def _parse_a_stock(self, symbol: str, parts: List[str], with_depth: bool = False) -> UnifiedQuote:
        # A股格式
        price = self._safe_float(parts[3]) or 0.0
        pre_close = self._safe_float(parts[2]) or 0.0
        change = price - pre_close
        percent = (change / pre_close * 100.0) if pre_close > 0 else 0.0
        
        dt_str = parts[30] if len(parts) > 30 else ""
        tm_str = parts[31] if len(parts) > 31 else ""
        update_time = None
        if dt_str and tm_str:
            update_time = f"{dt_str} {tm_str}"
        
        market = symbol[:2].upper() if len(symbol) >= 2 else "SH"

        # 解析五档买卖盘
        depth_data = None
        if with_depth and len(parts) > 29:
            from core.models import MarketDepth, DepthItem
            bids = []
            for i in range(5):
                v_val = self._safe_float(parts[10 + i * 2])
                p_val = self._safe_float(parts[11 + i * 2])
                if v_val is not None:
                    # 股转手 (进一法)
                    v_val = int((v_val + 99) // 100)
                if p_val is not None or v_val is not None:
                    bids.append(DepthItem(price=p_val, volume=v_val))
            asks = []
            for i in range(5):
                v_val = self._safe_float(parts[20 + i * 2])
                p_val = self._safe_float(parts[21 + i * 2])
                if v_val is not None:
                    # 股转手 (进一法)
                    v_val = int((v_val + 99) // 100)
                if p_val is not None or v_val is not None:
                    asks.append(DepthItem(price=p_val, volume=v_val))
            depth_data = MarketDepth(bids=bids, asks=asks)

        return UnifiedQuote(
            symbol=symbol,
            name=parts[0] if len(parts) > 0 else symbol,
            date=dt_str or datetime.now().strftime("%Y-%m-%d"),
            price=price,
            last_close=pre_close,
            open=self._safe_float(parts[1]),
            high=self._safe_float(parts[4]),
            low=self._safe_float(parts[5]),
            close=price,
            change=change,
            percent=percent,
            volume=self._safe_float(parts[8]),
            amount=self._safe_float(parts[9]),
            update_time=update_time,
            currency="CNY",
            exchange=market,
            security_type="stock",
            source="sina",
            depth=depth_data,
            timestamp=datetime.utcnow().isoformat()
        )

    def _parse_hk_stock(self, symbol: str, parts: List[str], with_depth: bool = False) -> UnifiedQuote:
        # 港股格式
        price = self._safe_float(parts[6]) or 0.0
        pre_close = self._safe_float(parts[3]) or 0.0
        change = price - pre_close
        percent = (change / pre_close * 100.0) if pre_close > 0 else 0.0

        update_time = None
        if len(parts) > 18:
            update_time = f"{parts[17]} {parts[18]}"

        # 解析港股买一卖一挂单作为深度盘口第一档
        depth_data = None
        if with_depth and len(parts) > 10:
            from core.models import MarketDepth, DepthItem
            sp1 = self._safe_float(parts[9])  # Sell1
            bp1 = self._safe_float(parts[10]) # Buy1
            bids = [DepthItem(price=bp1)] if bp1 is not None else []
            asks = [DepthItem(price=sp1)] if sp1 is not None else []
            if bids or asks:
                depth_data = MarketDepth(bids=bids, asks=asks)

        return UnifiedQuote(
            symbol=symbol,
            name=parts[1] if len(parts) > 1 else (parts[0] if len(parts) > 0 else symbol),
            date=parts[17] if len(parts) > 17 else datetime.now().strftime("%Y-%m-%d"),
            price=price,
            last_close=pre_close,
            open=self._safe_float(parts[2]) if len(parts) > 2 else None,
            high=self._safe_float(parts[4]) if len(parts) > 4 else None,
            low=self._safe_float(parts[5]) if len(parts) > 5 else None,
            close=price,
            change=change,
            percent=percent,
            volume=self._safe_float(parts[12]) if len(parts) > 12 else None,
            amount=self._safe_float(parts[11]) if len(parts) > 11 else None,
            update_time=update_time,
            currency="HKD",
            exchange="HK",
            security_type="stock",
            source="sina",
            depth=depth_data,
            timestamp=datetime.utcnow().isoformat()
        )

    def _parse_us_stock(self, symbol: str, parts: List[str], with_depth: bool = False) -> UnifiedQuote:
        # 美股格式
        price = self._safe_float(parts[1]) or 0.0
        pre_close = self._safe_float(parts[26]) or 0.0
        if pre_close <= 0 and price > 0:
            pre_close = price
        change = price - pre_close
        percent = (change / pre_close * 100.0) if pre_close > 0 else 0.0

        raw_date_str = parts[3] if len(parts) > 3 else ""
        date_str = datetime.now().strftime("%Y-%m-%d")
        if raw_date_str:
            try:
                dt = datetime.strptime(raw_date_str, "%Y-%m-%d %H:%M:%S")
                date_str = dt.strftime("%Y-%m-%d")
            except Exception:
                pass

        return UnifiedQuote(
            symbol=symbol,
            name=parts[0] if len(parts) > 0 else symbol,
            date=date_str,
            price=price,
            last_close=pre_close,
            open=self._safe_float(parts[5]) if len(parts) > 5 else None,
            high=self._safe_float(parts[6]) if len(parts) > 6 else None,
            low=self._safe_float(parts[7]) if len(parts) > 7 else None,
            close=price,
            change=change,
            percent=percent,
            volume=self._safe_float(parts[10]) if len(parts) > 10 else None,
            update_time=raw_date_str,
            currency="USD",
            exchange="US",
            security_type="stock",
            source="sina",
            timestamp=datetime.utcnow().isoformat()
        )

    def _parse_neidi_futures(self, symbol: str, parts: List[str], with_depth: bool = False) -> UnifiedQuote:
        # 内盘期货格式
        price = self._safe_float(parts[8]) or 0.0
        pre_close = self._safe_float(parts[5]) or 0.0
        
        # 期货有昨结算(10)做基准，如果昨结算有效优先昨结算作为 pre_close
        pre_settle = self._safe_float(parts[10]) if len(parts) > 10 else None
        if pre_settle and pre_settle > 0:
            pre_close = pre_settle

        # 期货未成交时最新价若为0，使用结算价(9)
        if price == 0 and len(parts) > 9:
            settle = self._safe_float(parts[9])
            if settle and settle > 0:
                price = settle

        change = price - pre_close
        percent = (change / pre_close * 100.0) if pre_close > 0 else 0.0

        vwap = self._safe_float(parts[27]) if len(parts) > 27 else None

        update_time = None
        if len(parts) > 17 and len(parts[1]) >= 6:
            update_time = f"{parts[17]} {parts[1][:6]}"

        return UnifiedQuote(
            symbol=symbol,
            name=parts[0] if len(parts) > 0 else symbol,
            date=parts[17] if len(parts) > 17 else datetime.now().strftime("%Y-%m-%d"),
            price=price,
            last_close=pre_close,
            open=self._safe_float(parts[2]) if len(parts) > 2 else None,
            high=self._safe_float(parts[3]) if len(parts) > 3 else None,
            low=self._safe_float(parts[4]) if len(parts) > 4 else None,
            close=self._safe_float(parts[9]) if len(parts) > 9 else None,
            change=change,
            percent=percent,
            volume=self._safe_float(parts[14]) if len(parts) > 14 else None,
            amount=self._safe_float(parts[13]) if len(parts) > 13 else None,
            update_time=update_time,
            vwap=vwap,
            currency="CNY",
            exchange="QH",
            security_type="future",
            source="sina",
            timestamp=datetime.utcnow().isoformat()
        )

    def _parse_haiwai_futures(self, symbol: str, parts: List[str], with_depth: bool = False) -> UnifiedQuote:
        # 外盘期货格式
        price = self._safe_float(parts[0]) or 0.0
        pre_close = self._safe_float(parts[7]) or 0.0
        if price == 0:
            price = pre_close
        change = price - pre_close
        percent = (change / pre_close * 100.0) if pre_close > 0 else 0.0

        update_time = None
        if len(parts) > 12:
            update_time = f"{parts[12]} {parts[6]}"

        return UnifiedQuote(
            symbol=symbol,
            name=parts[13] if len(parts) > 13 else symbol,
            date=parts[12] if len(parts) > 12 else datetime.now().strftime("%Y-%m-%d"),
            price=price,
            last_close=pre_close,
            open=self._safe_float(parts[8]) if len(parts) > 8 else None,
            high=self._safe_float(parts[4]) if len(parts) > 4 else None,
            low=self._safe_float(parts[5]) if len(parts) > 5 else None,
            change=change,
            percent=percent,
            amount=self._safe_float(parts[9]) if len(parts) > 9 else None,
            update_time=update_time,
            currency="USD",
            exchange="HF",
            security_type="future",
            source="sina",
            timestamp=datetime.utcnow().isoformat()
        )

    def _parse_forex(self, symbol: str, parts: List[str], with_depth: bool = False) -> UnifiedQuote:
        # 外汇格式
        price = self._safe_float(parts[8]) or 0.0
        pre_close = self._safe_float(parts[3]) or 0.0
        
        # 涨跌幅百分数直接保存
        percent = self._safe_float(parts[10])
        if percent is None:
            percent = ((price - pre_close) / pre_close * 100.0) if pre_close > 0 else 0.0
            
        change = self._safe_float(parts[11]) or (price - pre_close)

        update_time = None
        if len(parts) > 17:
            update_time = f"{parts[17]} {parts[0]}"

        amplitude = self._safe_float(parts[4])
        if amplitude is not None:
            amplitude = amplitude / 100.0

        return UnifiedQuote(
            symbol=symbol,
            name=parts[9] if len(parts) > 9 else symbol,
            date=parts[17] if len(parts) > 17 else datetime.now().strftime("%Y-%m-%d"),
            price=price,
            last_close=pre_close,
            open=self._safe_float(parts[5]) if len(parts) > 5 else None,
            high=self._safe_float(parts[6]) if len(parts) > 6 else None,
            low=self._safe_float(parts[7]) if len(parts) > 7 else None,
            change=change,
            percent=percent,
            amplitude=amplitude,
            update_time=update_time,
            currency="CNY",
            exchange="FX",
            security_type="forex",
            source="sina",
            timestamp=datetime.utcnow().isoformat()
        )

    def _parse_world_index(self, symbol: str, parts: List[str], with_depth: bool = False) -> UnifiedQuote:
        # 指数格式
        pre_close = self._safe_float(parts[9])
        if not pre_close and len(parts) > 8:
            pre_close = self._safe_float(parts[8])
        pre_close = pre_close or 0.0

        price = self._safe_float(parts[1]) if len(parts) > 1 else None
        if not price or price == 0:
            price = pre_close
            
        change = price - pre_close
        percent = (change / pre_close * 100.0) if pre_close > 0 else 0.0

        update_time = None
        if len(parts) > 7:
            update_time = f"{parts[6]} {parts[7]}"

        return UnifiedQuote(
            symbol=symbol,
            name=parts[0] if len(parts) > 0 else symbol,
            date=parts[6] if len(parts) > 6 else datetime.now().strftime("%Y-%m-%d"),
            price=price,
            last_close=pre_close,
            open=self._safe_float(parts[8]) if len(parts) > 8 else None,
            high=self._safe_float(parts[10]) if len(parts) > 10 else None,
            low=self._safe_float(parts[11]) if len(parts) > 11 else None,
            change=change,
            percent=percent,
            volume=self._safe_float(parts[11]) if len(parts) > 11 else None,
            update_time=update_time,
            currency="CNY",
            exchange="WI",
            security_type="index",
            source="sina",
            timestamp=datetime.utcnow().isoformat()
        )

    async def get_quotes(self, symbols: List[str], with_depth: bool = False) -> Dict[str, UnifiedQuote]:
        if not symbols:
            return {}
            
        q_list = []
        for s in symbols:
            s_lower = s.lower()
            if s_lower.startswith("nf_"):
                parts = s.split('_')
                q_list.append(f"nf_{parts[1].upper()}" if len(parts) > 1 else s.upper())
            elif s_lower.startswith("hf_"):
                parts = s.split('_')
                q_list.append(f"hf_{parts[1].upper()}" if len(parts) > 1 else s.upper())
            elif s_lower.startswith("fx_"):
                parts = s.split('_')
                q_list.append(f"fx_{parts[1].upper()}" if len(parts) > 1 else s.upper())
            elif s_lower.startswith("znb_"):
                parts = s.split('_')
                q_list.append(f"znb_{parts[1].upper()}" if len(parts) > 1 else s.upper())
            elif s_lower.startswith(("sh", "sz", "rt_hk")):
                q_list.append(s_lower)
            else:
                q_list.append(s)
                
        q_str = ",".join(q_list)
        url = f"{self.base_url}?list={q_str}"
        
        async with httpx.AsyncClient(timeout=5, verify=False) as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            
            # 新浪返回是 GBK 编码
            content_str = response.content.decode("gbk", errors="ignore")
            logger.debug("Sina API returned raw response: \n%s", content_str)
            
            result_dict = {}
            lines = content_str.split('\n')
            
            for line in lines:
                if not line.strip():
                    continue
                
                # 匹配格式: var hq_str_sh510300="...";
                match = re.search(r'var hq_str_([a-zA-Z0-9_]+)="([^"]*)"', line)
                if not match:
                    continue
                
                sym_key = match.group(1).lower()
                raw_data = match.group(2)
                
                if not raw_data.strip():
                    continue
                    
                parts = raw_data.split(',')
                
                # 判断资产类型进行转换
                if sym_key.startswith("rt_hk"):
                    q = self._parse_hk_stock(sym_key, parts, with_depth=with_depth)
                elif sym_key.startswith("gb_"):
                    q = self._parse_us_stock(sym_key, parts, with_depth=with_depth)
                elif sym_key.startswith("nf_"):
                    q = self._parse_neidi_futures(sym_key, parts, with_depth=with_depth)
                elif sym_key.startswith("hf_"):
                    q = self._parse_haiwai_futures(sym_key, parts, with_depth=with_depth)
                elif sym_key.startswith("fx_"):
                    q = self._parse_forex(sym_key, parts, with_depth=with_depth)
                elif sym_key.startswith("znb_"):
                    q = self._parse_world_index(sym_key, parts, with_depth=with_depth)
                else:
                    q = self._parse_a_stock(sym_key, parts, with_depth=with_depth)
                    
                result_dict[sym_key] = q
                
            return result_dict

    async def get_quote(self, symbol: str, with_depth: bool = False) -> Tuple[UnifiedQuote, int]:
        res_dict = await self.get_quotes([symbol], with_depth=with_depth)
        q = res_dict.get(symbol.lower())
        if not q:
            raise BusinessException(f"Sina returned empty result for symbol {symbol}")
        return q, 10

    async def get_history(self, symbol: str, period: str, interval: str, 
                          start: Optional[str], end: Optional[str], adj: str) -> dict:
        """
        获取新浪期货（内盘/外盘）历史 K 线
        """
        symbol_lower = symbol.lower()
        if not (symbol_lower.startswith("nf_") or symbol_lower.startswith("hf_")):
            raise NotImplementedError("Sina provider get_history only supports futures (nf_ or hf_ prefix)")
            
        if interval != "1d":
            raise NotImplementedError("Sina futures history only supports daily ('1d') KLines")

        code = symbol.split('_')[1].upper() if '_' in symbol else symbol.upper()
        
        import time
        from datetime import datetime
        now = datetime.now()
        date_str = f"{now.year}_{now.month}_{now.day}"
        
        if symbol_lower.startswith("nf_"):
            # 内盘期货日 K 线 API
            url = f"https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20_{code}{date_str}=/InnerFuturesNewService.getDailyKLine?symbol={code}&_={date_str}"
        else:
            # 外盘期货日 K 线 API
            url = f"https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20_{code}{date_str}=/GlobalFuturesService.getGlobalFuturesDailyKLine?symbol={code}&_={date_str}&source=web"

        try:
            async with httpx.AsyncClient(timeout=10, verify=False) as client:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Referer": "https://finance.sina.com.cn/"
                }
                resp = await client.get(url, headers=headers)
                resp.raise_for_status()
                
                # 新浪接口为 GBK 编码
                content = resp.content.decode("gbk", errors="ignore")
                logger.debug("Sina futures KLine raw response: %s", content)
                
                # 去除注释 /* ... */
                content_clean = re.sub(r'/\*.*?\*/', '', content, flags=re.S).strip()
                
                # 正则提取 JSON 数组 [...]
                match = re.search(r'=\s*(?:var\s+\w+=)?\s*\(?(\[[\s\S]*?\])\)?', content_clean)
                if not match:
                    # 容灾提取，直接寻找方括号
                    match = re.search(r'(\[[\s\S]*?\])', content_clean)
                    
                if not match:
                    logger.warning(f"Could not parse futures JSON array from response for {symbol}")
                    return {"symbol": symbol, "data": []}
                    
                json_str = match.group(1)
                import json
                raw_list = json.loads(json_str)
                
                data_list = []
                is_nf = symbol_lower.startswith("nf_")
                
                for item in raw_list:
                    # 根据内/外盘提取不同属性
                    if is_nf:
                        d_str = item.get("d")
                        open_val = self._safe_float(item.get("o"))
                        high_val = self._safe_float(item.get("h"))
                        low_val = self._safe_float(item.get("l"))
                        close_val = self._safe_float(item.get("c"))
                        volume_val = self._safe_float(item.get("v"))
                        position_val = self._safe_float(item.get("p"))
                        settlement_val = self._safe_float(item.get("s"))
                    else:
                        d_str = item.get("date")
                        open_val = self._safe_float(item.get("open"))
                        high_val = self._safe_float(item.get("high"))
                        low_val = self._safe_float(item.get("low"))
                        close_val = self._safe_float(item.get("close"))
                        volume_val = self._safe_float(item.get("volume"))
                        position_val = self._safe_float(item.get("position"))
                        settlement_val = self._safe_float(item.get("settlement"))
                        
                    if not d_str:
                        continue
                        
                    # 日期切片过滤 (支持 YYYY-MM-DD)
                    if start and d_str < start:
                        continue
                    if end and d_str > end:
                        continue
                        
                    data_list.append({
                        "date": d_str,
                        "open": open_val or 0.0,
                        "high": high_val or 0.0,
                        "low": low_val or 0.0,
                        "close": close_val or 0.0,
                        "volume": volume_val or 0.0,
                        "amount": round((volume_val or 0.0) * (close_val or 0.0), 2),
                        "settlement_price": settlement_val,
                        "position": position_val
                    })
                    
                # 排序后返回
                data_list = sorted(data_list, key=lambda x: x["date"])
                return {"symbol": symbol, "data": data_list}
                
        except Exception as e:
            logger.error(f"Failed to fetch history for Sina futures {symbol}: {e}")
            raise e
