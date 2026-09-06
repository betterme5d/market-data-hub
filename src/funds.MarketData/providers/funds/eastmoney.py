import re
import json
import logging
import asyncio
import requests
import urllib3
import httpx
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional

from core import config
from core.models import FundNav
from providers.base import SourceProbe
from providers.funds.base_nav import FundNavProvider

# 禁用未验证的 HTTPS 请求警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# lsjz 单基金历史净值单页上限（反爬限制，实测 pageSize 恒被上游封顶为 20）
_LSJZ_PAGE_SIZE = 20
# Fund_JJJZ_Data.aspx 全量最新净值单页上限（实测 20000）
_JJJZ_PAGE_SIZE = 20000


class EastmoneyProbe(SourceProbe):
    """天天基金 lsjz（单基金历史净值）接口的健康探针。"""

    name = "eastmoney"
    category = "funds"

    async def probe(self) -> None:
        """对 lsjz 发起一次最轻量请求（1 页 1 条），失败抛异常。"""
        data = await EastmoneySource()._fetch_lsjz_page("000001", 1, 1)
        if data is None or str(data.get("ErrCode", "0")) != "0":
            raise RuntimeError("eastmoney probe: lsjz abnormal response")
        if not (data.get("Data") or {}).get("LSJZList"):
            raise RuntimeError("eastmoney probe: lsjz returned empty list")


class EastmoneyJjjzProbe(SourceProbe):
    """天天基金 Fund_JJJZ_Data（全量最新净值）接口的健康探针。

    与 lsjz 是同一个源（eastmoney）下的两个不同上游接口——外部 API 的参数/
    验证/返回结构随时可能单独变化，1 个上游接口对应 1 个探针，缺一即静默失效。
    """

    name = "eastmoney-jjjz"
    category = "funds"

    async def probe(self) -> None:
        """对 Fund_JJJZ_Data 发起一次最轻量请求（1 页 1 条），校验解析结构。"""
        data = await EastmoneySource()._fetch_jjjz_page(1, 1)
        if data is None:
            raise RuntimeError("eastmoney-jjjz probe: request failed")
        if not data.get("datas"):
            raise RuntimeError("eastmoney-jjjz probe: empty datas")
        if not data.get("showday"):
            raise RuntimeError("eastmoney-jjjz probe: empty showday")


class EastmoneySource(FundNavProvider):
    def __init__(self):
        self.headers = {
            "User-Agent": _UA,
            "Referer": "http://fundf10.eastmoney.com/",
        }
        self.url = "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"

    def _fetch_html(self, params: Dict[str, str]) -> Optional[str]:
        """
        同步获取 HTML，内部处理转义字符
        """
        try:
            r = requests.get(
                self.url,
                params=params,
                headers=self.headers,
                timeout=10,
                verify=False,
            )
            if r.status_code != 200:
                logger.warning(
                    f"Fetch Eastmoney holdings failed with status {r.status_code}"
                )
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
                html = (
                    raw_content.replace(r"\"", '"')
                    .replace(r"\/", "/")
                    .replace(r"\n", "\n")
                    .replace(r"\r", "\r")
                    .replace(r"\t", "\t")
                )

            return html
        except Exception as e:
            logger.error(f"Error fetching Eastmoney holdings: {e}")
            return None

    def _parse_report_date(
        self, h4_text: str, font_text: Optional[str]
    ) -> Optional[str]:
        """
        根据 font 标签文本或 h4 文本推理出 YYYY-MM-DD 的报告期日期
        """
        if font_text:
            date_match = re.search(r"\d{4}-\d{2}-\d{2}", font_text)
            if date_match:
                return date_match.group(0)

        # 尝试正则从标题文字（如 "2024年4季度"）匹配并推算
        match = re.search(r"(\d{4})年(\d)季度", h4_text)
        if match:
            year = match.group(1)
            quarter = match.group(2)
            if quarter == "1":
                return f"{year}-03-31"
            elif quarter == "2":
                return f"{year}-06-30"
            elif quarter == "3":
                return f"{year}-09-30"
            elif quarter == "4":
                return f"{year}-12-31"

        # 兼容 "1季报" / "3季报" 这种常见写法
        match_report = re.search(r"(\d{4})年(\d)季报", h4_text)
        if match_report:
            year = match_report.group(1)
            quarter = match_report.group(2)
            if quarter == "1":
                return f"{year}-03-31"
            elif quarter == "2":
                return f"{year}-06-30"
            elif quarter == "3":
                return f"{year}-09-30"
            elif quarter == "4":
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

    def _find_col_indices(
        self, headers: List[str], is_bond: bool = False
    ) -> Dict[str, int]:
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

            q_match = re.search(r"\d{4}年\d季[度报]", h4.text)
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

                results.append(
                    {
                        "report_date": report_date,
                        "quarter_name": quarter_name,
                        "asset_type": "Stock",
                        "rank": int(rank_val),
                        "symbol_code": symbol_code,
                        "symbol_name": symbol_name,
                        "holding_percent": percent,
                        "holding_shares": shares,
                        "holding_amount": amount,
                    }
                )
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

            q_match = re.search(r"\d{4}年\d季[度报]", h4.text)
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

                results.append(
                    {
                        "report_date": report_date,
                        "quarter_name": quarter_name,
                        "asset_type": "Bond",
                        "rank": int(rank_val),
                        "symbol_code": symbol_code,
                        "symbol_name": symbol_name,
                        "holding_percent": percent,
                        "holding_shares": None,
                        "holding_amount": amount,
                    }
                )
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
            "rt": "0.12345678",
        }
        bond_params = {
            "type": "zqcc",
            "code": symbol,
            "year": str(year),
            "rt": "0.12345678",
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
                    "holdings": [],
                }

            # 移除外层多余的季度标志信息后加入
            holding_item = {
                "asset_type": item["asset_type"],
                "rank": item["rank"],
                "symbol_code": item["symbol_code"],
                "symbol_name": item["symbol_name"],
                "holding_percent": item["holding_percent"],
                "holding_shares": item["holding_shares"],
                "holding_amount": item["holding_amount"],
            }
            portfolios_map[rep_date]["holdings"].append(holding_item)

        # 按报告期截至日期降序排列
        sorted_portfolios = sorted(
            portfolios_map.values(), key=lambda x: x["report_date"], reverse=True
        )

        # 对每一个报告期内的持仓按 rank 升序排列
        for p in sorted_portfolios:
            p["holdings"] = sorted(
                p["holdings"], key=lambda x: (x["asset_type"], x["rank"])
            )

        return {"symbol": symbol, "year": year, "portfolios": sorted_portfolios}

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

    # ------------------------------------------------------------------
    # 统一净值接口实现（FundNavProvider）
    # ------------------------------------------------------------------

    @staticmethod
    def _to_nav_float(val) -> Optional[float]:
        """把上游字符串转 float，空串/无意义标记返回 None。"""
        if val is None or val == "" or val == "---" or val == "-":
            return None
        try:
            return float(str(val).replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    async def _fetch_lsjz_page(
        self,
        fund_code: str,
        page_index: int,
        page_size: int,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Optional[Dict[str, Any]]:
        """请求单基金历史净值 lsjz 一页，返回 JSON（反爬要求 Referer + UA）。"""
        url = f"{config.EASTMONEY_F10_BASE_URL}/f10/lsjz"
        params: Dict[str, Any] = {
            "fundCode": fund_code,
            "pageIndex": page_index,
            "pageSize": page_size,
        }
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date
        headers = {
            "User-Agent": _UA,
            "Referer": "http://fundf10.eastmoney.com/",
        }
        try:
            async with httpx.AsyncClient(
                timeout=config.UPSTREAM_TIMEOUT, verify=False
            ) as client:
                resp = await client.get(url, params=params, headers=headers)
                resp.raise_for_status()
                return resp.json()
        except Exception as e:
            logger.error(f"Fetch lsjz failed for {fund_code} page {page_index}: {e}")
            return None

    async def get_fund_nav_history(
        self, code: str, start_date: str | None = None, end_date: str | None = None
    ) -> List[FundNav]:
        """
        获取单只基金历史净值。内部 while 循环分页（pageSize 上限 20），
        日期区间直接透传上游（startDate/endDate）以减小分页量。
        Returns 按 nav_date 升序。
        """
        items: List[FundNav] = []
        page_index = 1
        seen = 0
        total = None

        while True:
            data = await self._fetch_lsjz_page(
                code, page_index, _LSJZ_PAGE_SIZE, start_date, end_date
            )
            if data is None or str(data.get("ErrCode", "0")) != "0":
                logger.warning(f"lsjz abnormal response for {code} page {page_index}: {str(data)[:200]}")
                break

            if total is None:
                try:
                    total = int(data.get("TotalCount", 0))
                except (TypeError, ValueError):
                    total = 0

            lst = (data.get("Data") or {}).get("LSJZList") or []
            for row in lst:
                nav_date = row.get("FSRQ")
                if not nav_date:
                    continue
                items.append(
                    FundNav(
                        code=code,
                        nav_date=nav_date,
                        unit_nav=self._to_nav_float(row.get("DWJZ")),
                        accum_nav=self._to_nav_float(row.get("LJJZ")),
                    )
                )

            seen += len(lst)
            if not lst or (total and seen >= total) or len(lst) < _LSJZ_PAGE_SIZE:
                break
            page_index += 1

        # 升序（lsjz 默认按日期倒序返回，历史补全需要升序）
        items.sort(key=lambda x: x.nav_date)
        return items

    async def _fetch_jjjz_page(
        self, page_index: int, page_size: int
    ) -> Optional[Dict[str, Any]]:
        """请求全量最新净值 Fund_JJJZ_Data.aspx 一页，解析 `var db={...}` JS。"""
        url = f"{config.EASTMONEY_FUND_BASE_URL}/Data/Fund_JJJZ_Data.aspx"
        params = {
            "t": "1",
            "lx": "1",
            "sort": "rzdf,desc",
            "page": f"{page_index},{page_size}",
            "onlySale": "0",
            "isLatest": "0",
        }
        headers = {
            "User-Agent": _UA,
            "Referer": "https://fund.eastmoney.com/fund.html",
        }
        try:
            async with httpx.AsyncClient(
                timeout=config.UPSTREAM_TIMEOUT, verify=False
            ) as client:
                resp = await client.get(url, params=params, headers=headers)
                resp.raise_for_status()
                text = resp.text
        except Exception as e:
            logger.error(f"Fetch JJJZ failed page {page_index}: {e}")
            return None

        # 返回形如 var db={chars:["0",...],datas:[["...","..."],...],pages:"2",...};
        # 是 JS 对象字面量（外层 key 为裸标识符，数组值为双引号字符串），不能直接 json.loads。
        # datas 数组内可能嵌套空数组 []，不能用简单正则，改用括号配对扫描精确截取。
        def _extract_array(name: str) -> Optional[str]:
            key_idx = text.find(name + ":")
            if key_idx < 0:
                return None
            start = text.find("[", key_idx)
            if start < 0:
                return None
            depth = 0
            in_str = False
            esc = False
            for i in range(start, len(text)):
                ch = text[i]
                if in_str:
                    if esc:
                        esc = False
                    elif ch == "\\":
                        esc = True
                    elif ch == '"':
                        in_str = False
                    continue
                if ch == '"':
                    in_str = True
                elif ch == "[":
                    depth += 1
                elif ch == "]":
                    depth -= 1
                    if depth == 0:
                        return text[start : i + 1]
            return None

        try:
            datas_raw = _extract_array("datas")
            showday_raw = _extract_array("showday")
            # pages 是字符串如 "2"（非数组），单独用正则取
            pages_match = re.search(r"\bpages\s*:\s*\"([^\"]*)\"", text)
            pages_val = pages_match.group(1) if pages_match else "1"
            return {
                "datas": json.loads(datas_raw) if datas_raw else [],
                "showday": json.loads(showday_raw) if showday_raw else [],
                "pages": pages_val,
            }
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"JJJZ parse failed page {page_index}: {e}")
            return None

    async def get_latest_all_nav(self) -> List[FundNav]:
        """
        获取最新一期所有基金净值（全量）。内部循环分页（pageSize 20000，实测 2 页）。
        净值日期来自顶层 showday[0]（最新日）。
        """
        items: List[FundNav] = []
        page_index = 1
        total_pages: Optional[int] = None

        while True:
            data = await self._fetch_jjjz_page(page_index, _JJJZ_PAGE_SIZE)
            if data is None:
                break

            showday = data.get("showday") or []
            latest_date = showday[0] if showday else ""

            if total_pages is None:
                try:
                    total_pages = int(data.get("pages", "1"))
                except (TypeError, ValueError):
                    total_pages = 1

            datas = data.get("datas") or []
            for row in datas:
                if not isinstance(row, (list, tuple)) or len(row) < 5:
                    continue
                code = row[0]
                if not code or not latest_date:
                    continue
                unit_nav = self._to_nav_float(row[3])
                accum_nav = self._to_nav_float(row[4])
                # 跳过无净值数据的基金（第 3/4 列均为空）
                if unit_nav is None and accum_nav is None:
                    continue
                items.append(
                    FundNav(
                        code=code,
                        nav_date=latest_date,
                        unit_nav=unit_nav,
                        accum_nav=accum_nav,
                    )
                )

            if not datas:
                break
            if total_pages and page_index >= total_pages:
                break
            page_index += 1

        return items

    # ------------------------------------------------------------------
    # 健康探针已上移到 EastmoneyProbe（见文件头部）
    # ------------------------------------------------------------------
