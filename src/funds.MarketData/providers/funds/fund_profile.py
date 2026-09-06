# -*- coding: utf-8 -*-
"""
基金档案取数源(成立日期/上市日期)与健康探针。

档案数据为一次性采集(非每日增量):
- 成立日期 ← 东财场内交易基金排行(rankhandler.aspx, dt=fb, 全量单页)
- 上市日期 ← SSE 基金列表原生 listingDate(见 exchanges/sse.py, 上交所)
            + SZSE 基金产品列表份额表(fund.szse.cn ShowReport CATALOGID=1000_lf, 深交所)
合并入口 collect_fund_profiles(),供 C# FundProfileCollectionJob 回填 funds 表。
"""
import io
import json
import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import httpx

from core import config
from providers.base import SourceProbe
from providers.exchanges.sse import SseFundListSource

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# rankhandler 返回 datas 为逗号拼接字符串数组，按逗号拆分后取列位
# （实测对齐 akshare fund_exchange_rank_em 的列映射：0=代码、15=成立日期）
_EM_RANK_CODE_IDX = 0
_EM_RANK_ESTABLISH_IDX = 15
# 全量行数下限(2026-09 实测 1639 行),低于此值视为上游截断,宁可失败不可给半份数据
_EM_RANK_MIN_ROWS = 1000
# SZSE 1000_lf tab1 行数下限(2026-09 实测 1041 行)
_SZSE_LISTING_MIN_ROWS = 800


def _normalize_date(value: Any) -> Optional[str]:
    """上游日期归一为 ISO(YYYY-MM-DD);空值/占位符返回 None。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text or text in ("--", "-", "nan", "NaT", "None"):
        return None
    # SSE listingDate 形如 20150925
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    # pandas 时间戳字符串 "2004-12-20 00:00:00" 截断到日期
    return text[:10]


def _extract_json_array(text: str, key: str) -> Optional[list]:
    """从 JS 对象字面量里按括号配对截取 key 对应的数组并 json 解析(与 eastmoney.py 同思路)。"""
    key_idx = text.find(key + ":")
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
                    logger.error(f"extract array {key} json decode failed: {e}")
                    return None
    return None


class EastmoneyProfileProbe(SourceProbe):
    """天天基金 F10 基本概况页(jbgk_{code}.html)的健康探针。

    与场内排行/lsjz 是同一个源(eastmoney)下的不同上游接口——1 个上游接口 1 个探针。
    """

    name = "eastmoney-profile"
    category = "funds"

    async def probe(self) -> None:
        """走 Source 真实取数路径校验概况页解析(510300 为常驻场内基金)。"""
        profile = await EastmoneyProfileSource().get_fund_profile("510300")
        if not profile.get("establish_date"):
            raise RuntimeError("eastmoney-profile probe: establish_date not parsed")


def _parse_establish_date(html: str) -> Optional[str]:
    """从 jbgk 概况页提取成立日期:优先概况表格,回退页头摘要。"""
    # 概况表格:<th>成立日期/规模</th><td>2015年05月20日 / 5.933亿份</td>
    m = re.search(r"成立日期/规模\s*</th>\s*<td[^>]*>\s*(\d{4})年(\d{1,2})月(\d{1,2})日", html)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # 页头摘要:成立日期：<span>2015-05-20</span>
    m = re.search(r"成立日期[：:]\s*(?:<span[^>]*>)?\s*(\d{4}-\d{2}-\d{2})", html)
    if m:
        return m.group(1)
    return None


class EastmoneyProfileSource:
    """天天基金 F10 基本概况页取数源(jbgk_{code}.html,逐基金,提供成立日期)。

    场内排行表只覆盖 ETF,LOF 等缺档基金由本源逐基金兜底(一次性采集,非每日增量)。
    """

    def __init__(self):
        self.url_tpl = "https://fundf10.eastmoney.com/jbgk_{code}.html"
        self.headers = {
            "User-Agent": _UA,
            "Referer": "http://fundf10.eastmoney.com/",
        }

    def _parse_html(self, html: str) -> Dict[str, Optional[str]]:
        return {"establish_date": _parse_establish_date(html)}

    async def get_fund_profile(self, code: str) -> Dict[str, Optional[str]]:
        """获取单只基金档案(成立日期)。上游无该基金/页面无字段时 establish_date 为 None。"""
        url = self.url_tpl.format(code=code)
        try:
            async with httpx.AsyncClient(timeout=config.UPSTREAM_TIMEOUT, verify=False) as client:
                resp = await client.get(url, headers=self.headers, follow_redirects=True)
                if resp.status_code != 200:
                    logger.warning(f"jbgk {code} fetch status {resp.status_code}")
                    return {"fund_code": code, "establish_date": None}
                profile = self._parse_html(resp.text)
                profile["fund_code"] = code
                return profile
        except Exception as e:
            logger.error(f"Fetch jbgk profile failed for {code}: {e}")
            raise


class EastmoneyFbRankProbe(SourceProbe):
    """东财场内交易基金排行(rankhandler.aspx dt=fb)的健康探针。"""

    name = "eastmoney-fb-rank"
    category = "funds"

    async def probe(self) -> None:
        """走 Source 真实取数路径全量校验(单页请求,顺带覆盖行数下限校验)。"""
        rows = await EastmoneyExchangeRankSource()._fetch_rows()
        if not rows:
            raise RuntimeError("eastmoney-fb-rank probe: empty datas")
        if not any(r.get("establish_date") for r in rows):
            raise RuntimeError("eastmoney-fb-rank probe: no establish_date parsed")


class EastmoneyExchangeRankSource:
    """东财场内交易基金排行取数源(全量单页 pn=30000,提供成立日期)。"""

    def __init__(self):
        self.url = "https://fund.eastmoney.com/data/rankhandler.aspx"
        self.headers = {
            "User-Agent": _UA,
            "Referer": "https://fund.eastmoney.com/fundguzhi.html",
        }

    def _parse_text(self, text: str) -> List[Dict[str, Optional[str]]]:
        """解析排行响应文本(datas 为逗号分隔的行数组),业务与探针共用。"""
        datas = _extract_json_array(text, "datas")
        if datas is None:
            raise RuntimeError("eastmoney exchange rank: datas array not found")

        rows: List[Dict[str, Optional[str]]] = []
        for item in datas:
            if not isinstance(item, str):
                continue
            fields = item.split(",")
            if len(fields) <= max(_EM_RANK_CODE_IDX, _EM_RANK_ESTABLISH_IDX):
                continue
            code = fields[_EM_RANK_CODE_IDX].strip()
            if not code:
                continue
            rows.append({
                "fund_code": code,
                "establish_date": _normalize_date(fields[_EM_RANK_ESTABLISH_IDX]),
            })

        if len(rows) < _EM_RANK_MIN_ROWS:
            raise RuntimeError(
                f"eastmoney exchange rank: suspicious truncation ({len(rows)} rows < {_EM_RANK_MIN_ROWS})")
        return rows

    async def _fetch_rows(self) -> List[Dict[str, Optional[str]]]:
        params = {
            "op": "ph", "dt": "fb", "ft": "ct", "rs": "", "gs": "0",
            "sc": "1nzf", "st": "desc", "pi": "1", "pn": "30000", "v": "0.1234567890",
        }
        try:
            async with httpx.AsyncClient(timeout=config.UPSTREAM_TIMEOUT, verify=False) as client:
                resp = await client.get(self.url, params=params, headers=self.headers)
                resp.raise_for_status()
                return self._parse_text(resp.text)
        except RuntimeError:
            raise
        except Exception as e:
            logger.error(f"Fetch eastmoney exchange rank failed: {e}")
            raise

    async def get_establish_dates(self) -> Dict[str, str]:
        """全量场内基金成立日期映射 {fund_code: YYYY-MM-DD}。"""
        rows = await self._fetch_rows()
        return {r["fund_code"]: r["establish_date"] for r in rows if r["establish_date"]}


class SzseFundListingProbe(SourceProbe):
    """深交所基金产品列表份额表(ShowReport CATALOGID=1000_lf)的健康探针。"""

    name = "szse-fund-listing"
    category = "funds"

    async def probe(self) -> None:
        """走 Source 真实取数路径全量校验(单表请求,顺带覆盖行数下限校验)。"""
        rows = await SzseFundListingSource()._fetch_rows()
        if not rows:
            raise RuntimeError("szse-fund-listing probe: empty xlsx")
        if not any(r.get("list_date") for r in rows):
            raise RuntimeError("szse-fund-listing probe: no list_date parsed")


class SzseFundListingSource:
    """深交所基金产品列表取数源(CATALOGID=1000_lf tab1,ETF/LOF/REITs 全量,含上市日期)。"""

    url = (
        "https://fund.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx"
        "&CATALOGID=1000_lf&TABKEY=tab1&random=0.07610353191740105"
    )

    def _parse_xlsx(self, content: bytes) -> List[Dict[str, Optional[str]]]:
        import pandas as pd

        df = pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype={"基金代码": str})
        for col in ("基金代码", "上市日期"):
            if col not in df.columns:
                raise RuntimeError(f"szse fund listing: missing column {col}")

        rows: List[Dict[str, Optional[str]]] = []
        for _, row in df.iterrows():
            code = str(row.get("基金代码") or "").strip().zfill(6)
            if not code or code == "nan":
                continue
            rows.append({
                "fund_code": code,
                "list_date": _normalize_date(row.get("上市日期")),
            })

        if len(rows) < _SZSE_LISTING_MIN_ROWS:
            raise RuntimeError(
                f"szse fund listing: suspicious truncation ({len(rows)} rows < {_SZSE_LISTING_MIN_ROWS})")
        return rows

    async def _fetch_rows(self) -> List[Dict[str, Optional[str]]]:
        headers = {
            "User-Agent": _UA,
            "Referer": "https://fund.szse.cn/marketdata/fundslist/index.html",
        }
        try:
            async with httpx.AsyncClient(timeout=config.UPSTREAM_TIMEOUT, verify=False) as client:
                resp = await client.get(self.url, headers=headers)
                resp.raise_for_status()
                return self._parse_xlsx(resp.content)
        except RuntimeError:
            raise
        except Exception as e:
            logger.error(f"Fetch szse fund listing failed: {e}")
            raise

    async def get_list_dates(self) -> Dict[str, str]:
        """全量深交所基金上市日期映射 {fund_code: YYYY-MM-DD}。"""
        rows = await self._fetch_rows()
        return {r["fund_code"]: r["list_date"] for r in rows if r["list_date"]}


async def collect_fund_profiles() -> List[Dict[str, Optional[str]]]:
    """合并基金档案:成立日期(东财场内排行) + 上市日期(SSE 原生 + SZSE 份额表)。

    档案为加法型数据(缺失即 None,由 C# 侧只填空回退),个别子源的交易所内部
    容错(SSE 单所失败返回半量)不阻断整体,但东财排行/SZSE 份额表截断会抛错。
    """
    establish_map = await EastmoneyExchangeRankSource().get_establish_dates()
    szse_list_map = await SzseFundListingSource().get_list_dates()
    sse_rows = await SseFundListSource().fetch_funds()

    profiles: Dict[str, Dict[str, Optional[str]]] = {}

    def _touch(code: str) -> Dict[str, Optional[str]]:
        return profiles.setdefault(code, {
            "fund_code": code,
            "establish_date": None,
            "list_date": None,
        })

    for code, value in establish_map.items():
        _touch(code)["establish_date"] = value
    for code, value in szse_list_map.items():
        _touch(code)["list_date"] = value
    for row in sse_rows:
        code = row.get("fund_code")
        list_date = row.get("list_date")
        if code and list_date:
            _touch(code)["list_date"] = list_date

    sse_list_count = sum(1 for r in sse_rows if r.get("list_date"))
    logger.info(
        f"collected fund profiles: {len(profiles)} funds "
        f"(establish={len(establish_map)}, list_date szse={len(szse_list_map)} sse={sse_list_count})")
    return list(profiles.values())
