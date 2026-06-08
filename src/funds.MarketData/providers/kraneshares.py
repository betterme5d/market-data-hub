import httpx
import logging
from datetime import datetime, timezone
from typing import Dict

logger = logging.getLogger(__name__)

class KraneSharesProvider:
    def __init__(self):
        self.base_url = "https://kraneshares.com/product-json/"

    async def get_premium_discount(self, pid: str, start: str, end: str) -> Dict[str, float]:
        url = f"{self.base_url}?pid={pid}&type=premium-discount&start={start}&end={end}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, timeout=20.0)
            if resp.status_code != 200:
                logger.error(f"Failed to fetch KraneShares data, status: {resp.status_code}")
                return {}
            
            data = resp.json()
            # 预期格式为 [[timestamp, rate], ...]
            result = {}
            for item in data:
                if len(item) >= 2:
                    ts = item[0]
                    rate = item[1]
                    # 转换毫秒级 Unix 时间戳为 UTC 日期字符串
                    dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
                    date_str = dt.strftime("%Y-%m-%d")
                    
                    if start <= date_str <= end:
                        result[date_str] = float(rate)
            return result
