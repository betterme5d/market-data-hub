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
from core.filters import get_exchange_listed_codes, is_target_exchange_fund
from core.pacing import polite_delay

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)

# CMTIDP getPublicFundJZInfoMore.do 分页上限（沿用 C# 侧既有值）
_CMTIDP_PAGE_SIZE = 5000

# 全量最新净值：内部循环最近 N 个交易日，同一只基金只保留最新日期的净值
_CMTIDP_LATEST_DAYS = 3

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
        data = await CmtidpSource()._fetch_page(
            fund_code="000001",
            start_date=today,
            end_date=today,
            start=0,
            length=1,
        )
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
        """构造 getPublicFundJZInfoMore.do 的 aoData 参数（1:1 严格对齐 C# CMTIDPService）。"""
        return [
            {"name": "sEcho", "value": 15},
            {"name": "iColumns", "value": 5},
            {"name": "sColumns", "value": ",,,,"},
            {"name": "iDisplayStart", "value": start},
            {"name": "iDisplayLength", "value": length},
            {"name": "mDataProp_0", "value": "fund"},
            {"name": "mDataProp_1", "value": "fund"},
            {"name": "mDataProp_2", "value": "fund"},
            {"name": "mDataProp_3", "value": "fund"},
            {"name": "mDataProp_4", "value": "valuationDate"},
            {"name": "fundType", "value": "all"},
            {"name": "fundCompanyShortName", "value": ""},
            {"name": "fundCode", "value": fund_code or ""},
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
        """获取全市场 ETF/LOF 的最新净值。

        CMTIDP 必须指定净值日期（上游契约）；同一交易日各基金披露进度不同，
        因此内部循环最近 _CMTIDP_LATEST_DAYS 个交易日，逐日拉全量后按基金代码合并，
        同一只基金只保留**最新日期**的那条净值；最终按 core.filters 的交易所上市白名单过滤。

        完整性契约：任一分页请求失败即抛错，不静默返回半份数据。
        """
        # published_only=True：按「已发布」口径取最近交易日——15:30 前当日净值未披露，
        # 参考日回退一天（周末/节假日自然落到上一个交易日）。
        nav_days = await _szse_calendar.latest_trading_days(
            _CMTIDP_LATEST_DAYS, published_only=True
        )
        if not nav_days:
            raise RuntimeError("CMTIDP 无法确定最近交易日，拒绝返回空结果")

        listed_codes = await get_exchange_listed_codes()
        merged: Dict[str, FundNav] = {}

        for nav_day in nav_days:  # 由新到旧：先写入者即为该基金「最新日期」
            day_codes: set = set()  # 本交易日已写入的代码（同日重复取最后一条）
            if nav_day != nav_days[0]:
                await polite_delay()  # 跨交易日的礼貌延时
            start = 0
            total: Optional[int] = None

            while True:
                if start > 0:
                    await polite_delay()  # 翻页礼貌延时（0.2~0.5s）
                data = await self._fetch_page(
                    fund_code="", start_date=nav_day, end_date=nav_day,
                    start=start, length=_CMTIDP_PAGE_SIZE,
                )
                if data is None:
                    raise RuntimeError(
                        f"CMTIDP 最新净值分页失败（{nav_day}, offset={start}）；不完整，拒绝返回半份数据"
                    )

                if total is None:
                    try:
                        total = int(data.get("iTotalRecords", 0))
                    except (TypeError, ValueError):
                        total = 0

                rows = data.get("aaData") or []
                for row in rows:
                    code = str(row.get("code") or "").strip()
                    if not code or not is_target_exchange_fund(code, listed_codes):
                        continue
                    # 同一代码同一日期可能返回多条（不同份额类别，如 168701：一条只有单位净值 0.9598、
                    # 累计净值为空，另一条 0.9731/0.9731 齐全；上游用 classification.code 区分）。
                    # 取值规则：**优先保留「单位净值 + 累计净值都齐全」的那条**；
                    # 只有两条完整度相同时，才按 C# 语义（AddOrUpdateRangeAsync 的 GroupBy.Last()）取后一条。
                    unit_nav = self._to_nav_float(row.get("shareNetValue"))
                    if unit_nav is None:
                        continue
                    accum_nav = self._to_nav_float(row.get("totalNetValue"))
                    existing = merged.get(code)
                    if existing is not None:
                        if code not in day_codes:
                            continue  # 已有「更新日期」的记录，不被更早日期覆盖
                        if existing.accum_nav is not None and accum_nav is None:
                            continue  # 不用「缺累计净值」的行覆盖完整行
                    merged[code] = FundNav(
                        code=code,
                        nav_date=row.get("valuationDate") or nav_day,
                        unit_nav=unit_nav,
                        accum_nav=accum_nav,
                    )
                    day_codes.add(code)

                start += len(rows)
                if not rows:
                    break
                if total and start >= total:
                    break

        return sorted(merged.values(), key=lambda x: (x.nav_date, x.code))

    async def get_fund_nav_history(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        on_page: Optional[Callable[[List[FundNav], str, str], Awaitable[None]]] = None,
    ) -> List[FundNav]:
        """获取单只基金历史净值（跨天范围，内部 while 分页）。

        完整性契约：分页请求失败，或空页时 offset 仍未达到上游自述总数，
        一律抛错而不返回半份数据。
        """
        from datetime import date
        start = start_date or "2000-01-01"
        end = end_date or date.today().strftime("%Y-%m-%d")

        # 同日同代码可能有多条（不同份额类别）：优先保留「单位净值 + 累计净值都齐全」的那条，
        # 完整度相同时取后一条（对齐 C# GroupBy.Last()）；目标是同 (代码, 日期) 只返回一条。
        collected: Dict[tuple, FundNav] = {}
        offset = 0
        total: Optional[int] = None

        while True:
            if offset > 0:
                await polite_delay()  # 翻页礼貌延时（0.2~0.5s）
            data = await self._fetch_page(
                fund_code=code, start_date=start, end_date=end,
                start=offset, length=_CMTIDP_PAGE_SIZE,
            )
            if data is None:
                raise RuntimeError(
                    f"CMTIDP 历史净值分页失败（{code}, offset={offset}）；不完整，拒绝返回半份数据"
                )

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
                unit_nav = self._to_nav_float(row.get("shareNetValue"))
                if unit_nav is None:
                    continue
                accum_nav = self._to_nav_float(row.get("totalNetValue"))
                key = (code, nav_date)
                prev = collected.get(key)
                if prev is not None and prev.accum_nav is not None and accum_nav is None:
                    continue  # 已有完整行，不用「缺累计净值」的行覆盖
                collected[key] = FundNav(
                    code=code,
                    nav_date=nav_date,
                    unit_nav=unit_nav,
                    accum_nav=accum_nav,
                )

            offset += len(rows)
            if not rows:
                if total and offset < total:
                    raise RuntimeError(
                        f"CMTIDP 历史净值不完整：{code} 已取 {offset}/{total} 行，拒绝返回半份数据"
                    )
                break
            if total and offset >= total:
                break

        return sorted(collected.values(), key=lambda x: x.nav_date)
