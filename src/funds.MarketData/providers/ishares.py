import re
import html
import json
import httpx
import logging
from datetime import date
from typing import Dict

logger = logging.getLogger(__name__)

class IsharesProvider:
    def __init__(self):
        self.base_url = "https://www.ishares.com/us/products/"
        # 兼容: x: Date.UTC(yyyy, m, d), y: Number((-0.19).toFixed(2)) 或 y: 0.15 等格式
        self.data_pattern = re.compile(
            r"x:\s*Date\.UTC\((\d{4}),\s*(\d{1,2}),\s*(\d{1,2})\),\s*y:\s*(?:Number\(\()?([-?\d\.]+)(?:\)\.toFixed\(\d+\)\))?"
        )

    async def get_premium_discount(self, product_path: str, start: str, end: str) -> Dict[str, float]:
        url = f"{self.base_url}{product_path}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, timeout=20.0)
            if resp.status_code != 200:
                logger.error(f"Failed to fetch iShares data, status: {resp.status_code}")
                return {}
            
            content = resp.text
            result = {}
            
            # 1. 尝试从 HTML 的 componentprops 属性中解析 JSON 格式的数据（新版 Astro 渲染模式）
            parsed_via_json = False
            for match in re.finditer(r'componentprops=["\'](.*?)["\']', content):
                raw_props = match.group(1)
                if "premium-discount-chart" in raw_props:
                    try:
                        props_str = html.unescape(raw_props)
                        data = json.loads(props_str)
                        chart_data = data["containersByNameMap"]["premium-discount-chart"]["dataPointsByNameMap"]["premiumDiscountChartData"]
                        as_of_dates = chart_data["asOfDate"]
                        values = chart_data["value"]
                        
                        for d_int, val_str in zip(as_of_dates, values):
                            d_str = str(d_int)
                            date_str = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
                            if start <= date_str <= end:
                                rate_val = float(val_str)
                                result[date_str] = round(rate_val / 100.0, 6)
                        parsed_via_json = True
                        break
                    except Exception as e:
                        logger.warning(f"Failed to parse componentprops JSON: {e}")
                        continue
            
            # 2. 如果 JSON 解析未成功，则回退 to 原有的正则匹配模式（传统 Highcharts 嵌入）
            if not parsed_via_json:
                matches = self.data_pattern.findall(content)
                for match in matches:
                    if len(match) >= 4:
                        try:
                            year = int(match[0])
                            month = int(match[1]) + 1
                            day = int(match[2])
                            rate_val = float(match[3])
                            
                            date_obj = date(year, month, day)
                            date_str = date_obj.strftime("%Y-%m-%d")
                            
                            if start <= date_str <= end:
                                result[date_str] = round(rate_val / 100.0, 6)
                        except ValueError as ve:
                            logger.warning(f"Parse iShares data match failed: {match}, err: {ve}")
            return result
