# -*- coding: utf-8 -*-
"""
证监会信息披露平台 CMTIDP（eid.csrc.gov.cn）取数源 + 健康探针。
查询转发契约：GET /fund/disclose/getPublicFundJZInfoMore.do?aoData=...&_=...
返回含 aaData/iTotalRecords 的 JSON。分页与聚合逻辑保留在 C# 消费方。
"""
import json
import logging
import time
from typing import Awaitable, Callable, List, Optional
from urllib.parse import quote

import httpx

from core import config
from core.models import FundNav
from providers.base import SourceProbe
from providers.exchanges.szse import SzseCalendarSource
from providers.funds.base_nav import FundNavProvider

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)

# CMTIDP getPublicFundJZInfoMore.do 分页上限（沿用 C# 侧既有值）
_CMTIDP_PAGE_SIZE = 5000

# 深交所官方交易日历单例，用于计算「最新净值日」（同自然日缓存，避免频繁请求上游）
_szse_calendar = SzseCalendarSource()


class CmtidpProbe(SourceProbe):
    """CMTIDP 公募基金净值接口的健康探针。"""

    name = "cmtidp"
    category = "funds"

    async def probe(self) -> None:
        """探针：取一只常见基金当日 1 条净值，要求返回含 iTotalRecords 的 JSON。"""
        from datetime import date

        today = date.today().strftime("%Y-%m-%d")
        ao_data = [
            {"name": "sEcho", "value": 1},
            {"name": "iColumns", "value": 5},
            {"name": "sColumns", "value": ",,,,"},
            {"name": "iDisplayStart", "value": 0},
            {"name": "iDisplayLength", "value": 1},
            {"name": "mDataProp_0", "value": "fund"},
            {"name": "mDataProp_1", "value": "fund"},
            {"name": "mDataProp_2", "value": "fund"},
            {"name": "mDataProp_3", "value": "fund"},
            {"name": "mDataProp_4", "value": "valuationDate"},
            {"name": "fundType", "value": "all"},
            {"name": "fundCompanyShortName", "value": ""},
            {"name": "fundCode", "value": "000001"},
            {"name": "fundName", "value": ""},
            {"name": "startDate", "value": today},
            {"name": "endDate", "value": today},
        ]
        qs = f"aoData={quote(json.dumps(ao_data, separators=(',', ':')))}&_={int(time.time() * 1000)}"
        data = await CmtidpSource().get_fund_net_values(qs)
        if not isinstance(data, dict) or "iTotalRecords" not in data:
            raise RuntimeError("cmtidp probe: unexpected response shape (missing 'iTotalRecords')")


class CmtidpSource(FundNavProvider):
    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or config.CMTIDP_BASE_URL).rstrip("/")

    async def get_fund_net_values(self, query_string: str) -> dict:
        """透传净值查询：原样转发查询串，原样返回上游 JSON。"""
        url = f"{self.base_url}/fund/disclose/getPublicFundJZInfoMore.do?{query_string}"
        headers = {"User-Agent": _USER_AGENT, "Referer": f"{self.base_url}/"}
        async with httpx.AsyncClient(timeout=config.UPSTREAM_TIMEOUT) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()

    # ------------------------------------------------------------------
    # 统一净值接口实现（FundNavProvider）
    # ------------------------------------------------------------------

    @staticmethod
    def _build_ao_data(
        *,
        fund_code: str,
        start_date: str,
        end_date: str,
        start: int,
        length: int,
    ) -> List[dict]:
        """构造 getPublicFundJZInfoMore.do 的 aoData 参数（对齐 C# 侧结构）。"""
        return [
            {"name": "sEcho", "value": 15},
            {"name": "iColumns", "value": 4},
            {"name": "sColumns", "value": ",,,"},
            {"name": "iDisplayStart", "value": start},
            {"name": "iDisplayLength", "value": length},
            {"name": "mDataProp_0", "value": "code"},
            {"name": "mDataProp_1", "value": "shortName"},
            {"name": "mDataProp_2", "value": "valuationDate"},
            {"name": "mDataProp_3", "value": "shareNetValue"},
            {"name": "mDataProp_4", "value": "totalNetValue"},
            {"name": "fundType", "value": "all"},
            {"name": "fundCompanyShortName", "value": ""},
            {"name": "fundCode", "value": fund_code},
            {"name": "fundName", "value": ""},
            {"name": "startDate", "value": start_date},
            {"name": "endDate", "value": end_date},
        ]

    async def _fetch_page(
        self,
        *,
        fund_code: str,
        start_date: str,
        end_date: str,
        start: int,
        length: int,
    ) -> Optional[dict]:
        ao_data = self._build_ao_data(
            fund_code=fund_code,
            start_date=start_date,
            end_date=end_date,
            start=start,
            length=length,
        )
        qs = f"aoData={quote(json.dumps(ao_data, separators=(',', ':')))}&_={int(time.time() * 1000)}"
        try:
            return await self.get_fund_net_values(qs)
        except Exception as e:
            logger.error(f"cmtidp fetch page failed fund={fund_code}: {e}")
            return None

    @staticmethod
    def _to_nav_float(val) -> Optional[float]:
        if val is None or val == "":
            return None
        try:
            return float(str(val).replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    async def get_latest_all_nav(self) -> List[FundNav]:
        """获取最新一期所有基金净值（全量，内部 while 分页 5000/页）。

        净值日取「最近一个交易日」而非今天，对标 C# 侧 TradeDayService.GetLatestTradingDayAsync，
        避免周末/节假日当天 CMTIDP 无数据导致返回空集。
        """
        nav_day = await _szse_calendar.latest_trading_day()
        if not nav_day:
            from datetime import date
            nav_day = date.today().isoformat()

        items: List[FundNav] = []
        start = 0
        total: Optional[int] = None

        while True:
            data = await self._fetch_page(
                fund_code="", start_date=nav_day, end_date=nav_day,
                start=start, length=_CMTIDP_PAGE_SIZE,
            )
            if data is None:
                break

            if total is None:
                try:
                    total = int(data.get("iTotalRecords", 0))
                except (TypeError, ValueError):
                    total = 0

            rows = data.get("aaData") or []
            for row in rows:
                code = row.get("code")
                nav_date = row.get("valuationDate")
                if not code or not nav_date:
                    continue
                items.append(
                    FundNav(
                        code=code,
                        nav_date=nav_date,
                        unit_nav=self._to_nav_float(row.get("shareNetValue")),
                        accum_nav=self._to_nav_float(row.get("totalNetValue")),
                    )
                )

            start += len(rows)
            if not rows or (total and start >= total):
                break

        items.sort(key=lambda x: x.nav_date)
        return items

    async def get_fund_nav_history(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        on_page: Optional[Callable[[List[FundNav], str, str], Awaitable[None]]] = None,
    ) -> List[FundNav]:
        """获取单只基金历史净值（跨天范围，内部 while 分页）。"""
        from datetime import date
        start = start_date or "2000-01-01"
        end = end_date or date.today().strftime("%Y-%m-%d")

        items: List[FundNav] = []
        offset = 0
        total: Optional[int] = None

        while True:
            data = await self._fetch_page(
                fund_code=code, start_date=start, end_date=end,
                start=offset, length=_CMTIDP_PAGE_SIZE,
            )
            if data is None:
                break

            if total is None:
                try:
                    total = int(data.get("iTotalRecords", 0))
                except (TypeError, ValueError):
                    total = 0

            rows = data.get("aaData") or []
            for row in rows:
                nav_date = row.get("valuationDate")
                if not nav_date:
                    continue
                items.append(
                    FundNav(
                        code=code,
                        nav_date=nav_date,
                        unit_nav=self._to_nav_float(row.get("shareNetValue")),
                        accum_nav=self._to_nav_float(row.get("totalNetValue")),
                    )
                )

            offset += len(rows)
            if not rows or (total and offset >= total):
                break

        items.sort(key=lambda x: x.nav_date)
        return items
