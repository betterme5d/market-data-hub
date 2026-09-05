# -*- coding: utf-8 -*-
"""
申万行业指数数据源（swsresearch.com 官网接口）。
"""
import http.cookiejar
import json
import logging
import ssl
import urllib.request
from urllib.parse import quote

logger = logging.getLogger(__name__)

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Referer': 'https://www.swsresearch.com/',
    'X-Requested-With': 'XMLHttpRequest',
}


class SwsProvider:
    def __init__(self):
        self._opener = None

    def _get_opener(self):
        if self._opener is None:
            cj = http.cookiejar.CookieJar()
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self._opener = urllib.request.build_opener(
                urllib.request.HTTPSHandler(context=ctx),
                urllib.request.HTTPCookieProcessor(cj)
            )
        return self._opener

    def api(self, path: str) -> dict:
        url = f"https://www.swsresearch.com/institute-sw/api/{path}"
        req = urllib.request.Request(url, headers=_HEADERS)
        with self._get_opener().open(req, timeout=30) as resp:
            return json.loads(resp.read())

    def get_industries(self, indextype: str) -> dict:
        return self.api(f"index_name/?indextype={quote(indextype)}")

    def get_industry_kline(self, code: str, period: str = "DAY") -> dict:
        return self.api(f"index_publish/trend/?swindexcode={code}&period={period}")

    def get_bulk_industry_kline(self, indextype: str) -> dict:
        """
        批量抓取当日所有申万行业的估值行情，并从详情接口动态提取日期，拼接为标准K线格式返回。
        """
        # 1. 批量获取实时行情 (使用 page_size=200 以保证一次性获取完二级行业的 134 个指数)
        path = f"index_publish/current/?indextype={quote(indextype)}&page=1&page_size=200"
        try:
            bulk_result = self.api(path)
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
                date_result = self.api(f"index_publish/details/index_spread/?swindexcode={first_code}")
                if date_result.get("code") == "200" and date_result.get("data"):
                    raw_date = date_result["data"][0].get("trading_date")  # 格式如 "20260702"
                    if raw_date and len(raw_date) == 8:
                        date_str = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
            except Exception as e:
                logger.error(f"Failed to fetch trading date for index {first_code}: {e}")

        # 3. 严格规则校验：如果日期获取失败，返回空数据且 code=500
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

        return {"code": "200", "message": "ok", "data": data_list}
