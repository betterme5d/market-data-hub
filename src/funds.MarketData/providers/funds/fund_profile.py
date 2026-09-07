# -*- coding: utf-8 -*-
"""
基金档案取数源(成立日期/上市日期)与健康探针。

档案数据为一次性采集(非每日增量):
- 成立日期 ← 东财场内交易基金排行(rankhandler.aspx, dt=fb, 全量单页)
- 上市日期 ← SSE 基金列表原生 listingDate(见 exchanges/sse.py, 上交所)
            + SZSE 基金上市日期报表(exchanges/listing_dates.py, ShowReport CATALOGID=1105, 深交所)
合并入口 collect_fund_profiles(),供 C# FundProfileCollectionJob 回填 funds 表。
"""
import json
import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import httpx

from core import config
from providers.base import SourceProbe
from providers.exchanges.sse import SseFundListSource
from providers.exchanges.listing_dates import SzseListingDateSource

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# rankhandler 返回 datas 为逗号拼接字符串数组，按逗号拆分后取列位
# （列位实测随 dt 参数不同而偏移：dt=fb 场内榜单 15=成立日期，dt=kf 开放基金榜单 16=成立日期）
_EM_RANK_CODE_IDX = 0
# dt -> 成立日期列位
_EM_ESTABLISH_IDX = {"fb": 15, "kf": 16}
# 全量行数下限(2026-09 实测:fb 场 1639 行、kf 场 24411 行),低于此值视为上游截断,宁可失败不可给半份数据
_EM_RANK_MIN_ROWS = 1000


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


from providers.funds.establish_dates import (
    EastmoneyEstablishDateProbe,
    EastmoneyEstablishDateSource,
    EstablishDateProvider,
)

# 兼容既有单测与旧引用的别名
EastmoneyFbRankProbe = EastmoneyEstablishDateProbe
EastmoneyExchangeRankSource = EastmoneyEstablishDateSource


async def collect_fund_dates() -> List[Dict[str, Optional[str]]]:
    """合并基金关键日期: 成立日期(东财场内+开放排行缓存) + 上市日期(SSE 原生 + SZSE 1105)。

    数据为加法型字典列表(缺失即 None,由 C# 侧只填空回退)。
    """
    establish_map = await EstablishDateProvider().get_all()
    szse_list_map = await SzseListingDateSource().fetch_listing_dates()
    sse_rows = await SseFundListSource().fetch_funds()

    date_items: Dict[str, Dict[str, Optional[str]]] = {}

    def _touch(code: str) -> Dict[str, Optional[str]]:
        return date_items.setdefault(code, {
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
        f"collected fund dates: {len(date_items)} funds "
        f"(establish={len(establish_map)}, list_date szse={len(szse_list_map)} sse={sse_list_count})")
    return list(date_items.values())


# 兼容旧函数命名
collect_fund_profiles = collect_fund_dates


async def get_fund_dates(code: str) -> Dict[str, Optional[str]]:
    """获取单只基金的关键日期汇总（成立日期与上市日期）。"""
    from core.filters import clean_fund_code
    from providers.exchanges.listing_dates import ListingDateProvider

    clean = clean_fund_code(code)
    if not clean:
        return {"fund_code": code, "establish_date": None, "list_date": None}

    establish_info = await EstablishDateProvider().get_one(clean)
    try:
        listing_info = await ListingDateProvider().get_one(clean)
    except Exception:
        listing_info = {"list_date": None}

    return {
        "fund_code": clean,
        "establish_date": establish_info.get("establish_date"),
        "list_date": listing_info.get("list_date"),
    }
