import re
import json
import logging
import asyncio
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class EastmoneyProvider:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "http://fundf10.eastmoney.com/"
        }
        self.url = "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"

    def _fetch_html(self, params: Dict[str, str]) -> Optional[str]:
        """
        同步获取 HTML，内部处理转义字符
        """
        try:
            r = requests.get(self.url, params=params, headers=self.headers, timeout=10)
            if r.status_code != 200:
                logger.warning(f"Fetch Eastmoney holdings failed with status {r.status_code}")
                return None
            
            text = r.text
            # 匹配 content 里面的转义 HTML 字符串
            match = re.search(r'content\s*:\s*"([\s\S]*?)"\s*,\s*key', text)
            if not match:
                match = re.search(r'content\s*:\s*"([\s\S]*?)"', text)
            
            if not match:
                logger.warning("Could not find content in Eastmoney response")
                return None
            
            raw_content = match.group(1)
            # 使用 json.loads 进行反转义
            try:
                html = json.loads(f'"{raw_content}"')
            except Exception:
                # 简单手动替换作为后备
                html = raw_content.replace(r'\"', '"').replace(r'\/', '/').replace(r'\n', '\n').replace(r'\r', '\r').replace(r'\t', '\t')
            
            return html
        except Exception as e:
            logger.error(f"Error fetching Eastmoney holdings: {e}")
            return None

    def _parse_report_date(self, h4_text: str, font_text: Optional[str]) -> Optional[str]:
        """
        根据 font 标签文本或 h4 文本推理出 YYYY-MM-DD 的报告期日期
        """
        if font_text:
            date_match = re.search(r'\d{4}-\d{2}-\d{2}', font_text)
            if date_match:
                return date_match.group(0)

        # 尝试正则从标题文字（如 "2024年4季度"）匹配并推算
        match = re.search(r'(\d{4})年(\d)季度', h4_text)
        if match:
            year = match.group(1)
            quarter = match.group(2)
            if quarter == '1':
                return f"{year}-03-31"
            elif quarter == '2':
                return f"{year}-06-30"
            elif quarter == '3':
                return f"{year}-09-30"
            elif quarter == '4':
                return f"{year}-12-31"
        
        # 兼容 "1季报" / "3季报" 这种常见写法
        match_report = re.search(r'(\d{4})年(\d)季报', h4_text)
        if match_report:
            year = match_report.group(1)
            quarter = match_report.group(2)
            if quarter == '1':
                return f"{year}-03-31"
            elif quarter == '2':
                return f"{year}-06-30"
            elif quarter == '3':
                return f"{year}-09-30"
            elif quarter == '4':
                return f"{year}-12-31"
                
        return None

    def _clean_numeric(self, text: str) -> Optional[float]:
        """
        清洗数值文本，去除逗号和百分号
        """
        if not text:
            return None
        text_clean = text.strip().replace(",", "").replace("%", "")
        if not text_clean or text_clean == "-" or text_clean == "---":
            return None
        try:
            return float(text_clean)
        except ValueError:
            return None

    def _find_col_indices(self, headers: List[str], is_bond: bool = False) -> Dict[str, int]:
        """
        根据表头文本模糊匹配，推导出数据字段所在的列索引，提供默认物理索引兜底
        """
        indices = {}
        for i, h in enumerate(headers):
            h_clean = h.strip()
            if "代码" in h_clean:
                indices["code"] = i
            elif "名称" in h_clean:
                indices["name"] = i
            elif "占净值" in h_clean or "比例" in h_clean:
                indices["percent"] = i
            elif "持股数" in h_clean or "持股数量" in h_clean:
                indices["shares"] = i
            elif "市值" in h_clean or "持仓市值" in h_clean:
                indices["amount"] = i
                
        # 兜底物理索引默认值
        if "code" not in indices:
            indices["code"] = 1
        if "name" not in indices:
            indices["name"] = 2
            
        if is_bond:
            if "percent" not in indices:
                indices["percent"] = 3
            if "amount" not in indices:
                indices["amount"] = 4
        else:
            if "percent" not in indices:
                indices["percent"] = 4
            if "shares" not in indices:
                indices["shares"] = 5
            if "amount" not in indices:
                indices["amount"] = 6
                
        return indices

    def _parse_stocks(self, html: str) -> List[Dict[str, Any]]:
        """
        解析股票持仓列表 (jjcc)
        """
        results = []
        if not html:
            return results
            
        soup = BeautifulSoup(html, "lxml")
        boxitems = soup.find_all("div", class_="boxitem")
        
        for box in boxitems:
            h4 = box.find("h4")
            table = box.find("table")
            if not h4 or not table:
                continue
                
            font = h4.find("font", class_="px12")
            font_text = font.text if font else None
            report_date = self._parse_report_date(h4.text, font_text)
            if not report_date:
                continue
                
            q_match = re.search(r'\d{4}年\d季[度报]', h4.text)
            quarter_name = q_match.group(0) if q_match else "未知季度"
            
            trs = table.find_all("tr")
            if not trs:
                continue
                
            # 解析表头获取列索引
            headers = [th_td.text.strip() for th_td in trs[0].find_all(["th", "td"])]
            idx = self._find_col_indices(headers, is_bond=False)
            max_idx = max(idx.values())
            
            # 过滤表头行及非数据行
            for tr in trs[1:]:
                tds = tr.find_all("td")
                if not tds or len(tds) <= max_idx:
                    continue
                
                # 检查第一列是不是数字序号，以滤除非数据行
                rank_val = self._clean_numeric(tds[0].text)
                if rank_val is None:
                    continue
                
                symbol_code = tds[idx["code"]].text.strip()
                symbol_name = tds[idx["name"]].text.strip()
                if not symbol_code:
                    continue
                    
                percent = self._clean_numeric(tds[idx["percent"]].text)
                shares = self._clean_numeric(tds[idx["shares"]].text)
                amount = self._clean_numeric(tds[idx["amount"]].text)
                
                results.append({
                    "report_date": report_date,
                    "quarter_name": quarter_name,
                    "asset_type": "Stock",
                    "rank": int(rank_val),
                    "symbol_code": symbol_code,
                    "symbol_name": symbol_name,
                    "holding_percent": percent,
                    "holding_shares": shares,
                    "holding_amount": amount
                })
        return results

    def _parse_bonds(self, html: str) -> List[Dict[str, Any]]:
        """
        解析债券持仓列表 (zqcc)
        """
        results = []
        if not html:
            return results
            
        soup = BeautifulSoup(html, "lxml")
        boxitems = soup.find_all("div", class_="boxitem")
        
        for box in boxitems:
            h4 = box.find("h4")
            table = box.find("table")
            if not h4 or not table:
                continue
                
            font = h4.find("font", class_="px12")
            font_text = font.text if font else None
            report_date = self._parse_report_date(h4.text, font_text)
            if not report_date:
                continue
                
            q_match = re.search(r'\d{4}年\d季[度报]', h4.text)
            quarter_name = q_match.group(0) if q_match else "未知季度"
            
            trs = table.find_all("tr")
            if not trs:
                continue
                
            headers = [th_td.text.strip() for th_td in trs[0].find_all(["th", "td"])]
            idx = self._find_col_indices(headers, is_bond=True)
            max_idx = max(idx.values())
            
            for tr in trs[1:]:
                tds = tr.find_all("td")
                if not tds or len(tds) <= max_idx:
                    continue
                
                rank_val = self._clean_numeric(tds[0].text)
                if rank_val is None:
                    continue
                
                symbol_code = tds[idx["code"]].text.strip()
                symbol_name = tds[idx["name"]].text.strip()
                if not symbol_code:
                    continue
                    
                percent = self._clean_numeric(tds[idx["percent"]].text)
                amount = self._clean_numeric(tds[idx["amount"]].text)
                
                results.append({
                    "report_date": report_date,
                    "quarter_name": quarter_name,
                    "asset_type": "Bond",
                    "rank": int(rank_val),
                    "symbol_code": symbol_code,
                    "symbol_name": symbol_name,
                    "holding_percent": percent,
                    "holding_shares": None,
                    "holding_amount": amount
                })
        return results


    async def get_portfolio(self, symbol: str, year: int) -> Dict[str, Any]:
        """
        并行获取指定基金该年度的股票与债券持仓，并在内存中归并
        """
        loop = asyncio.get_event_loop()
        
        # 1. 构造请求参数
        stock_params = {
            "type": "jjcc",
            "code": symbol,
            "topline": "10000",
            "year": str(year),
            "rt": "0.12345678"
        }
        bond_params = {
            "type": "zqcc",
            "code": symbol,
            "year": str(year),
            "rt": "0.12345678"
        }
        
        # 2. 在线程池中并行请求 HTML
        stock_task = loop.run_in_executor(None, self._fetch_html, stock_params)
        bond_task = loop.run_in_executor(None, self._fetch_html, bond_params)
        
        stock_html, bond_html = await asyncio.gather(stock_task, bond_task)
        
        # 3. 解析持仓列表
        stocks = self._parse_stocks(stock_html) if stock_html else []
        bonds = self._parse_bonds(bond_html) if bond_html else []
        
        # 4. 按 report_date 归并资产组合
        portfolios_map = {}
        
        # 合并列表
        for item in stocks + bonds:
            rep_date = item["report_date"]
            q_name = item["quarter_name"]
            
            if rep_date not in portfolios_map:
                portfolios_map[rep_date] = {
                    "report_date": rep_date,
                    "quarter_name": q_name,
                    "holdings": []
                }
            
            # 移除外层多余的季度标志信息后加入
            holding_item = {
                "asset_type": item["asset_type"],
                "rank": item["rank"],
                "symbol_code": item["symbol_code"],
                "symbol_name": item["symbol_name"],
                "holding_percent": item["holding_percent"],
                "holding_shares": item["holding_shares"],
                "holding_amount": item["holding_amount"]
            }
            portfolios_map[rep_date]["holdings"].append(holding_item)
            
        # 按报告期截至日期降序排列
        sorted_portfolios = sorted(portfolios_map.values(), key=lambda x: x["report_date"], reverse=True)
        
        # 对每一个报告期内的持仓按 rank 升序排列
        for p in sorted_portfolios:
            p["holdings"] = sorted(p["holdings"], key=lambda x: (x["asset_type"], x["rank"]))
            
        return {
            "symbol": symbol,
            "year": year,
            "portfolios": sorted_portfolios
        }

    def get_fund_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取基金基本信息，包括业绩比较基准。
        通过 AkShare fund_individual_basic_info_xq 接口从雪球获取。
        返回 item/value 两列 DataFrame，转为 key-value dict 后提取字段。
        """
        import akshare as ak
        import pandas as pd
        try:
            df = ak.fund_individual_basic_info_xq(symbol=symbol)
            if df is None or df.empty:
                logger.warning(f"No fund info returned for {symbol}")
                return {"fund_code": symbol, "error": "No data returned"}

            items = dict(zip(df["item"], df["value"]))

            def _safe_str(val) -> Optional[str]:
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                s = str(val).strip()
                return s if s else None

            return {
                "fund_code": str(items.get("基金代码", symbol)),
                "fund_name": str(items.get("基金名称", "")),
                "fund_type": str(items.get("基金类型", "")),
                "benchmark_desc": _safe_str(items.get("业绩比较基准")),
                "tracking_index": _safe_str(items.get("跟踪标的")),
            }
        except Exception as e:
            logger.error(f"Failed to fetch fund info for {symbol}: {e}")
            return {"fund_code": symbol, "error": str(e)}

    async def get_valuations(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        抓取、合并去重天天基金估值数据，仅返回以 16 和 5 开头的基金。
        支持 3 分钟 Redis 缓存。
        """
        import time
        import urllib.request
        import urllib.parse
        from datetime import datetime, timezone, timedelta
        from core.cache import get_cached_valuations, set_cached_valuations
        
        # 1. 尝试缓存命中
        if not force_refresh:
            cached = get_cached_valuations()
            if cached is not None:
                return cached

        loop = asyncio.get_event_loop()
        
        # 2. 获取 type=0 和 type=9 原始数据
        # 使用 run_in_executor 避免阻塞 FastAPI 异步主循环
        params_0 = {
            "type": "0", "sort": "3", "orderType": "desc",
            "canbuy": "0", "pageIndex": "1", "pageSize": "20000",
            "callback": "", "_": str(int(time.time() * 1000))
        }
        params_9 = {
            "type": "9", "sort": "3", "orderType": "desc",
            "canbuy": "0", "pageIndex": "1", "pageSize": "20000",
            "callback": "", "_": str(int(time.time() * 1000) + 1)
        }

        # 为了避免短时间内两个请求并发引起外部防爬封锁，引入 0.5s 的间隔
        def fetch_sync(params):
            url = f"https://api.fund.eastmoney.com/FundGuZhi/GetFundGZList?{urllib.parse.urlencode(params)}"
            req = urllib.request.Request(
                url,
                headers={
                    "Referer": "https://fund.eastmoney.com/",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                }
            )
            try:
                with urllib.request.urlopen(req, timeout=15) as response:
                    return json.loads(response.read().decode("utf-8"))
            except Exception as ex:
                logger.error(f"Fetch Eastmoney list failed with params {params}: {ex}")
                return {}

        task_0 = loop.run_in_executor(None, fetch_sync, params_0)
        await asyncio.sleep(0.5) # 串行延时，保护 IP 稳定性
        task_9 = loop.run_in_executor(None, fetch_sync, params_9)

        data_0, data_9 = await asyncio.gather(task_0, task_9)

        list_0 = data_0.get("Data", {}).get("list", []) or []
        list_9 = data_9.get("Data", {}).get("list", []) or []

        # 3. 合并去重并筛选 16 和 5 开头的基金
        merged = {}
        for item in list_0 + list_9:
            bzdm = item.get("bzdm")
            if bzdm and (bzdm.startswith("16") or bzdm.startswith("5")):
                merged[bzdm] = item

        # 4. 数据清洗和标准化
        fetched_at = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
        
        def to_float(val) -> Optional[float]:
            if not val or val in ("---", "-", None):
                return None
            try:
                return float(str(val).replace("%", "").replace(",", "").strip())
            except ValueError:
                return None

        result_list = []
        for bzdm, item in merged.items():
            est_val = to_float(item.get("gsz"))
            if est_val is None:
                continue

            result_list.append({
                "fund_code": bzdm,
                "fund_name": item.get("jjjc"),
                "fund_type": item.get("FType"),
                "net_value": to_float(item.get("dwjz")),
                "estimated_value": est_val,
                "estimated_growth_rate": to_float(item.get("gszzl")),
                "valuation_date": item.get("gzrq"),
                "update_date": fetched_at # 抓取发生那一刻的时间
            })

        # 5. 写入 Redis 缓存 (3分钟过期)
        set_cached_valuations(result_list, ttl=180)
        return result_list

