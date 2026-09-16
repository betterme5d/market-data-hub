import re
import json
import logging
import asyncio
import random
from datetime import date, timedelta
from typing import Awaitable, Callable, Dict, List, Any, Optional
import requests
import urllib3
import httpx
from bs4 import BeautifulSoup

from core import config
from core.models import FundNav
from core.timeseries_cache.crawler import PageBatch, PaginatedSliceCrawler
from providers.base import SourceProbe
from providers.funds.base_nav import FundNavProvider
from core.filters import get_exchange_listed_codes, is_target_exchange_fund
from core.pacing import polite_delay
from providers.exchanges.szse import SzseCalendarSource

# 禁用未验证的 HTTPS 请求警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# lsjz 单基金历史净值单页上限（反爬限制，实测 pageSize 恒被上游封顶为 20）
_LSJZ_PAGE_SIZE = 20
# 深交所官方交易日历：用于校验并修正东财净值日期（净值日期不可能晚于最近一个交易日）
_calendar = SzseCalendarSource()

# 东财基金排行（场内最新净值的来源）。
#
# 页面引用：
#   - 场内交易基金排行（ETF/封闭）：https://fund.eastmoney.com/data/fbsfundranking.html  → dt=fb
#   - 开放基金排行（含 LOF）：      https://fund.eastmoney.com/data/fundranking.html   → dt=kf
# 底层接口：https://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt={fb|kf}&ft=all&...
#
# 为什么必须两个榜单合并（2026-09-11 实测，与交易所上市白名单比对）：
#   - dt=fb：allRecords=1648，覆盖白名单 1631/2067；ETF（510300/588000/159915…）全在此榜；
#   - dt=kf：allRecords=24459，覆盖白名单 403/2067；LOF 多在此榜（161725/160105/163407/501050…）；
#   - 只用 dt=fb 会漏掉约 403 只 LOF；两榜合并覆盖 2034/2067，其余 33 只两榜都没有。
#
# 注意：Fund_JJJZ_Data.aspx 是"开放式基金（场外）"列表，实测六种参数组合均不含 510300/159915，
#       不能用于场内净值，故本项目不再使用它。
#
# 两榜净值列位一致（逗号分隔字符串）：
#   [0]=基金代码 [1]=基金简称 [2]=拼音 [3]=净值日期 [4]=单位净值 [5]=累计净值 [6]=日增长率
_EM_RANK_URL = "https://fund.eastmoney.com/data/rankhandler.aspx"
_EM_RANK_DTS = ("fb", "kf")
# 单页条数实测：pn>=5000 时上游一次全量返回（无条数上限），
# 故取 20000 —— 两榜合计仅 3 次请求（fb 1 次 + kf 2 次），而 pn=500 需 53 次。
_EM_RANK_PAGE_SIZE = 20000
_EM_RANK_COL_DATE = 3
_EM_RANK_COL_UNIT_NAV = 4
_EM_RANK_COL_ACCUM_NAV = 5
_EM_RANK_REFERER = {
    "fb": "https://fund.eastmoney.com/data/fbsfundranking.html",
    "kf": "https://fund.eastmoney.com/data/fundranking.html",
}


def _extract_json_array(text: str, name: str) -> Optional[list]:
    """按括号配对从 JS 对象字面量里截取 name:[...] 并 json 解析（rankhandler 返回非严格 JSON）。"""
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
                try:
                    return json.loads(text[start : i + 1])
                except json.JSONDecodeError as e:
                    logger.error(f"extract array {name} json decode failed: {e}")
                    return None
    return None


def _extract_scalar_int(text: str, name: str) -> Optional[int]:
    """截取 name:123 或 name:"123" 形式的上游标量并转 int。"""
    m = re.search(name + ':\\s*"?(\\d+)"?', text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None



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


class EastmoneySource(FundNavProvider):
    #: 支持逐页流式回调（见 FundNavSource.supports_streaming）
    supports_streaming = True

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
        max_retries: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """请求单基金历史净值 lsjz 一页，带重试与反爬头部。"""
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
        for attempt in range(1, max_retries + 1):
            try:
                async with httpx.AsyncClient(
                    timeout=config.UPSTREAM_TIMEOUT, verify=False
                ) as client:
                    resp = await client.get(url, params=params, headers=headers)
                    resp.raise_for_status()
                    return resp.json()
            except Exception as e:
                logger.warning(
                    f"Fetch lsjz for {fund_code} page {page_index} attempt {attempt}/{max_retries} failed: {e}"
                )
                if attempt < max_retries:
                    backoff = 0.2 * (2 ** (attempt - 1)) + random.uniform(0.05, 0.15)
                    await asyncio.sleep(backoff)
                else:
                    logger.error(
                        f"Fetch lsjz for {fund_code} page {page_index} exhausted all {max_retries} retries"
                    )
                    return None

    async def get_fund_nav_history(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        on_page: Optional[Callable[[List[FundNav], str, str], Awaitable[None]]] = None,
    ) -> List[FundNav]:
        """
        获取单只基金历史净值。
        使用通用 PaginatedSliceCrawler 统一处理分页循环、防反爬随机抖动、
        单页指数退避重试与逐页流式落盘回调。
        Returns 按 nav_date 升序。
        """
        crawler = PaginatedSliceCrawler(
            min_delay=0.2,
            max_delay=0.4,
            max_retries=3,
        )

        async def _fetch_single_page(
            page_idx: int, page_sz: int, s_dt: Optional[str], e_dt: Optional[str]
        ) -> PageBatch[FundNav]:
            data = await self._fetch_lsjz_page(
                code, page_idx, page_sz, s_dt, e_dt
            )
            if data is None or str(data.get("ErrCode", "0")) != "0":
                err_msg = f"lsjz abnormal response for {code} page {page_idx}: {str(data)[:200]}"
                logger.error(err_msg)
                raise RuntimeError(err_msg)

            total = None
            try:
                total = int(data.get("TotalCount", 0))
            except (TypeError, ValueError):
                total = 0

            lst = (data.get("Data") or {}).get("LSJZList") or []
            page_items: List[FundNav] = []
            for row in lst:
                nav_date = row.get("FSRQ")
                if not nav_date:
                    continue
                page_items.append(
                    FundNav(
                        code=code,
                        nav_date=nav_date,
                        unit_nav=self._to_nav_float(row.get("DWJZ")),
                        accum_nav=self._to_nav_float(row.get("LJJZ")),
                        daily_return=self._to_nav_float(row.get("JZZZL")),
                        subscribe_status=row.get("SGZT") or None,
                        redeem_status=row.get("SHZT") or None,
                        dividend=row.get("FHSP") or None,
                    )
                )
            return PageBatch(items=page_items, total_count=total)

        return await crawler.crawl_slice(
            start_date=start_date,
            end_date=end_date,
            page_size=_LSJZ_PAGE_SIZE,
            fetch_page_fn=_fetch_single_page,
            date_getter=lambda item: item.nav_date,
            order="desc",
            on_page=on_page,
        )

    @staticmethod
    def _normalize_nav_date(day: str, latest_trading_day: str) -> str:
        """把上游净值日期规范为 YYYY-MM-DD，并处理跨年。

        实证（2026-09-11 全量扫描）：rankhandler 的日期列一律是完整的 YYYY-MM-DD
        （dt=fb 1648/1648、dt=kf 24427/24427 非空行，0 行格式异常），因此只需处理"年份未回退"：
          1) 年份未回退：2026-01-05 抓到 2026-12-31，实际应为 2025-12-31；

        规则：净值日期一定 <= 最近一个交易日。
          - 带年份且 > 最近交易日 → 年份减 1；
          - 带年份且 <= 最近交易日 → 原样返回；
          - 格式完全无法识别 → 打 WARN 并原样返回（不猜测）。
        """
        if not day:
            return day
        text = day.strip()
        if not latest_trading_day:
            return text

        # 1) 完整日期 YYYY-MM-DD
        full = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", text)
        if full:
            year = int(full.group(1))
            month, day_of_month = full.group(2), full.group(3)
            candidate = f"{year:04d}-{month}-{day_of_month}"
            if candidate <= latest_trading_day:
                return candidate
            fixed = f"{year - 1:04d}-{month}-{day_of_month}"
            logger.warning(
                f"eastmoney rank 净值日期 {candidate} 晚于最近交易日 {latest_trading_day}，按跨年修正为 {fixed}"
            )
            return fixed


        logger.warning(f"eastmoney rank 非预期净值日期格式: {text!r}，保持原值")
        return text


    @staticmethod
    def _require_all_records(data: Dict[str, Any], dt: str) -> int:
        """取上游自述总记录数（allRecords）；缺失/非法即抛错。

        完整性判据与翻页判据同源：一旦把「解析不到」退化为 0，翻页会停在首页、
        完整性校验也会被跳过，等于把「上游改字段名」变成静默丢数据
        （dt=kf 自述 24459 条，而单页 pn 上限 20000）。
        """
        raw = data.get("all_records")
        if raw is None:
            raise RuntimeError(
                f"eastmoney rank(dt={dt}) 未返回 allRecords，无法判定完整性；拒绝返回半份数据"
            )
        try:
            return int(raw)
        except (TypeError, ValueError):
            raise RuntimeError(f"eastmoney rank(dt={dt}) allRecords 非法: {raw!r}")

    @staticmethod
    def _resolve_all_pages(raw_pages: Any, all_records: int) -> int:
        """总页数：优先用上游 allPages；缺失/非法时按 allRecords 推导（不得退化成只取首页）。"""
        try:
            pages = int(raw_pages) if raw_pages is not None else 0
        except (TypeError, ValueError):
            pages = 0
        if pages > 0:
            return pages
        if all_records > 0:
            return (all_records + _EM_RANK_PAGE_SIZE - 1) // _EM_RANK_PAGE_SIZE
        return 1

    async def get_latest_all_nav(self) -> List[FundNav]:
        """获取全市场场内 ETF/LOF 的最新净值（东财场内 + 开放两个榜单合并）。

        页面引用：
        - 场内交易基金排行（ETF/封闭）：https://fund.eastmoney.com/data/fbsfundranking.html
        - 开放基金排行（含 LOF）：      https://fund.eastmoney.com/data/fundranking.html
        底层接口：https://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt={fb|kf}&...

        为什么必须两榜合并（2026-09-11 实测，与交易所上市白名单比对）：
        - 只用 dt=fb 覆盖 1631/2067，**漏掉约 403 只 LOF**（161725/160105/163407 只在 kf）；
        - 两榜的净值列位一致：[3]=净值日期 [4]=单位净值 [5]=累计净值；
        - 两榜合并覆盖 2034/2067；按代码去重（同码以先出现的榜单为准）。

        完整性契约：任一榜单任一分页请求失败、或累计行数少于上游自述 allRecords，
        一律抛错而不返回半份数据。
        日期契约：净值日期不可能晚于「最近一个交易日」，晚了按跨年处理（年份减 1）。
        """
        listed_codes = await get_exchange_listed_codes()
        latest_trading_day = await _calendar.latest_trading_day() or ""

        items: List[FundNav] = []
        seen_codes: set = set()

        for dt in _EM_RANK_DTS:
            if dt != _EM_RANK_DTS[0]:
                await polite_delay()  # 换榜单之间的礼貌延时
            page_index = 1
            all_pages: Optional[int] = None
            all_records: Optional[int] = None
            raw_rows = 0

            while True:
                if page_index > 1:
                    await polite_delay()  # 翻页礼貌延时（0.2~0.5s）
                data = await self._fetch_rank_page(dt, page_index, _EM_RANK_PAGE_SIZE)
                if data is None:
                    raise RuntimeError(
                        f"eastmoney rank(dt={dt}) page {page_index} fetch failed; "
                        f"全量最新净值不完整，拒绝返回半份数据"
                    )

                if all_pages is None:
                    # 判据不可得时直接抛错，绝不静默退化为「只取首页且不校验完整性」
                    all_records = self._require_all_records(data, dt)
                    all_pages = self._resolve_all_pages(data.get("all_pages"), all_records)

                rows = data.get("rows") or []
                raw_rows += len(rows)
                for item in rows:
                    if not isinstance(item, str):
                        continue
                    fields = item.split(",")
                    if len(fields) <= _EM_RANK_COL_ACCUM_NAV:
                        continue
                    code = fields[0].strip()
                    if (
                        not code
                        or code in seen_codes
                        or not is_target_exchange_fund(code, listed_codes)
                    ):
                        continue
                    unit_nav = self._to_nav_float(fields[_EM_RANK_COL_UNIT_NAV])
                    accum_nav = self._to_nav_float(fields[_EM_RANK_COL_ACCUM_NAV])
                    # 单位净值缺失即丢弃（与 CMTIDP 路径同口径）：只放行「只有累计净值」的行会得到
                    # 「有记录但单位净值为空」的脏行，C# 侧按 NetValue is not null 过滤时口径就对不上。
                    if unit_nav is None:
                        continue
                    nav_date = self._normalize_nav_date(
                        fields[_EM_RANK_COL_DATE].strip(), latest_trading_day
                    )
                    if not nav_date:
                        continue
                    items.append(
                        FundNav(
                            code=code,
                            nav_date=nav_date,
                            unit_nav=unit_nav,
                            accum_nav=accum_nav,
                        )
                    )
                    seen_codes.add(code)

                if not rows:
                    break
                if all_pages and page_index >= all_pages:
                    break
                page_index += 1

            # 上游自述 allRecords 与实际累计行数比对：不足即视为截断
            if all_records and raw_rows < all_records:
                raise RuntimeError(
                    f"eastmoney rank(dt={dt}) incomplete: got {raw_rows} rows, "
                    f"allRecords={all_records}; 拒绝返回半份数据"
                )
            # 自述 0 行却返回了行：载荷不自洽，不能把「0」当成「无需校验」
            if raw_rows and not all_records:
                raise RuntimeError(
                    f"eastmoney rank(dt={dt}) 自述 allRecords=0 却返回 {raw_rows} 行；"
                    f"载荷不自洽，拒绝采信"
                )

        return items

    async def _fetch_rank_page(
        self, dt: str, page_index: int, page_size: int, max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """请求东财基金排行一页，返回 {rows, all_records, all_pages}；重试耗尽返回 None。

        dt=fb：场内交易基金排行 https://fund.eastmoney.com/data/fbsfundranking.html
        dt=kf：开放基金排行     https://fund.eastmoney.com/data/fundranking.html
        行格式：逗号分隔字符串，[0]=代码 [3]=净值日期 [4]=单位净值 [5]=累计净值。

        带重试：全量最新净值只有 3 次请求（fb 1 次 + kf 2 次），此前一次都不重试，
        单次网络抖动就会让整批采集失败（lsjz 路径一直是 3 次重试，这里对齐）。
        """
        params = {
            "op": "ph",
            "dt": dt,
            "ft": "all",
            "rs": "",
            "gs": "0",
            "sc": "1nzf",
            "st": "desc",
            "pi": str(page_index),
            "pn": str(page_size),
            "v": "0.1234567890",
        }
        headers = {
            "User-Agent": _UA,
            "Referer": _EM_RANK_REFERER.get(dt, _EM_RANK_REFERER["kf"]),
        }
        text: Optional[str] = None
        for attempt in range(1, max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=config.UPSTREAM_TIMEOUT, verify=False) as client:
                    resp = await client.get(_EM_RANK_URL, params=params, headers=headers)
                    resp.raise_for_status()
                    text = resp.text
                break
            except Exception as e:
                if attempt < max_retries:
                    backoff = 0.2 * (2 ** (attempt - 1)) + random.uniform(0.05, 0.15)
                    logger.warning(
                        f"Fetch eastmoney rank(dt={dt}) page {page_index} attempt "
                        f"{attempt}/{max_retries} failed: {e}; retrying in {backoff:.2f}s"
                    )
                    await asyncio.sleep(backoff)
                else:
                    logger.error(
                        f"Fetch eastmoney rank(dt={dt}) page {page_index} exhausted "
                        f"all {max_retries} retries: {e}"
                    )
                    return None

        if text is None:
            return None
        rows = _extract_json_array(text, "datas")
        if rows is None:
            logger.error(f"eastmoney rank(dt={dt}) page {page_index}: datas array not found")
            return None
        return {
            "rows": rows,
            "all_records": _extract_scalar_int(text, "allRecords"),
            "all_pages": _extract_scalar_int(text, "allPages"),
        }

    # ------------------------------------------------------------------
    # 健康探针已上移到 EastmoneyProbe（见文件头部）
    # ------------------------------------------------------------------
