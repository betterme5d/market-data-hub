# -*- coding: utf-8 -*-
"""
深交所（SZSE）公募基金历史份额数据源与健康探针。
支持 <=60 天安全切片防 65,536 行截断、流式解析 XLSX、万份换算与全市场代码分组。
"""
import collections
from datetime import date, datetime, timedelta
import io
import logging
import random
from typing import Any, Dict, List, Optional, Tuple

import httpx
import openpyxl

from core import config
from providers.base import SourceProbe

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

SZSE_SHARE_REPORT_URL = "https://fund.szse.cn/api/report/ShowReport"
SZSE_FUNDS_REFERER = "https://fund.szse.cn/marketdata/fundslist/index.html"


def split_date_range(start_date: str, end_date: str, max_days: int = 60) -> List[Tuple[str, str]]:
    """
    将日期区间拆分为不超过 max_days 天的子区间，保证深交所接口导出数据不触发 65,536 行截断。
    """
    s_dt = date.fromisoformat(start_date)
    e_dt = date.fromisoformat(end_date)
    if s_dt > e_dt:
        return []

    slices: List[Tuple[str, str]] = []
    curr = s_dt
    while curr <= e_dt:
        nxt = min(curr + timedelta(days=max_days - 1), e_dt)
        slices.append((curr.isoformat(), nxt.isoformat()))
        curr = nxt + timedelta(days=1)
    return slices


class SzseShareSource:
    """深交所基金份额数据源。"""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def fetch_market_shares(
        self, start_date: str, end_date: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        全市场批量拉取 [start_date, end_date] 区间内所有深市基金的逐日份额数据。
        返回格式: { "159901": [ {"code": "159901", "share_date": "2026-06-30", "shares": 245890.0, ...}, ... ], ... }
        """
        rnd = random.random()
        params = {
            "SHOWTYPE": "xlsx",
            "CATALOGID": "fund_jjgm",
            "TABKEY": "tab1",
            "txtStart": start_date,
            "txtEnd": end_date,
            "random": str(rnd),
        }
        headers = {
            "User-Agent": _UA,
            "Referer": SZSE_FUNDS_REFERER,
            "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
        }

        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            resp = await client.get(SZSE_SHARE_REPORT_URL, params=params, headers=headers)
            resp.raise_for_status()

        return self.parse_xlsx_bytes(resp.content)

    def parse_xlsx_bytes(self, content: bytes) -> Dict[str, List[Dict[str, Any]]]:
        """解析 XLSX 二进制流并按基金代码聚合。"""
        market_records: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=False, data_only=True)
        try:
            ws = wb.active
            for row in ws.iter_rows(values_only=True):
                if not row or not any(row) or len(row) < 4:
                    continue
                d_val, c_val, n_val, s_val = row[0], row[1], row[2], row[3]
                if not d_val or str(d_val).strip() == "日期":
                    continue

                # 解析日期
                if isinstance(d_val, (date, datetime)):
                    share_date = d_val.strftime("%Y-%m-%d")
                else:
                    share_date = str(d_val).strip()

                # 解析基金代码（标准化为 6 位）
                clean_code = str(c_val).strip().zfill(6)
                if not clean_code.isdigit():
                    continue

                # 基金简称
                name = str(n_val).strip() if n_val else None

                # 基金份额数值转换 (原始单位：份，系统对齐标准单位：万份)
                if s_val is None:
                    continue
                try:
                    raw_str = str(s_val).replace(",", "").strip()
                    raw_shares = float(raw_str)
                    shares = round(raw_shares / 10000.0, 4)
                except (ValueError, TypeError):
                    continue

                record = {
                    "code": clean_code,
                    "share_date": share_date,
                    "shares": shares,
                    "raw_shares": raw_shares,
                    "name": name,
                }
                market_records[clean_code].append(record)
        finally:
            wb.close()

        return market_records


class SzseShareProbe(SourceProbe):
    """深交所基金份额报表接口健康探针。"""

    name = "szse-fund-share"
    category = "exchanges"

    def __init__(self, source: Optional[SzseShareSource] = None):
        self.source = source or SzseShareSource(timeout=config.HEALTH_PROBE_TIMEOUT)

    async def probe(self) -> None:
        today_str = date.today().strftime("%Y-%m-%d")
        three_days_ago = (date.today() - timedelta(days=3)).strftime("%Y-%m-%d")
        # 探测最近3天，若接口正常则返回数据且应为 dict 结构
        res = await self.source.fetch_market_shares(three_days_ago, today_str)
        if not isinstance(res, dict):
            raise ValueError(f"SzseShareProbe expected dict, got {type(res)}")
