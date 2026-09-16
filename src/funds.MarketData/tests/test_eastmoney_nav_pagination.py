# -*- coding: utf-8 -*-
import asyncio
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from core.models import FundNav
from providers.funds.eastmoney import EastmoneySource


@pytest.mark.asyncio
async def test_eastmoney_pagination_streaming_callback():
    source = EastmoneySource()

    # 模拟东财 2 页数据，将分页大小打桩为 2
    page1_data = {
        "ErrCode": "0",
        "TotalCount": "3",
        "Data": {
            "LSJZList": [
                {
                    "FSRQ": "2024-03-29",
                    "DWJZ": "3.5000",
                    "LJJZ": "4.5000",
                    "JZZZL": "1.25",
                    "SGZT": "开放申购",
                    "SHZT": "开放赎回",
                    "FHSP": "",
                },
                {
                    "FSRQ": "2024-03-28",
                    "DWJZ": "3.4500",
                    "LJJZ": "4.4500",
                    "JZZZL": "-0.50",
                    "SGZT": "开放申购",
                    "SHZT": "开放赎回",
                    "FHSP": "",
                },
            ]
        },
    }
    page2_data = {
        "ErrCode": "0",
        "TotalCount": "3",
        "Data": {
            "LSJZList": [
                {
                    "FSRQ": "2024-03-01",
                    "DWJZ": "3.3000",
                    "LJJZ": "4.3000",
                    "JZZZL": "0.10",
                    "SGZT": "开放申购",
                    "SHZT": "开放赎回",
                    "FHSP": "分红0.1元",
                }
            ]
        },
    }

    callback_calls = []

    async def mock_on_page(page_items, cov_s, cov_e):
        callback_calls.append((page_items, cov_s, cov_e))

    with patch("providers.funds.eastmoney._LSJZ_PAGE_SIZE", 2), patch.object(
        source,
        "_fetch_lsjz_page",
        side_effect=[page1_data, page2_data],
    ), patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        res = await source.get_fund_nav_history(
            code="510300",
            start_date="2024-03-01",
            end_date="2024-03-31",
            on_page=mock_on_page,
        )

        assert len(res) == 3
        # 结果应升序排列
        assert [r.nav_date for r in res] == ["2024-03-01", "2024-03-28", "2024-03-29"]
        assert res[-1].daily_return == 1.25
        assert res[0].dividend == "分红0.1元"

        # 验证 on_page 逐页被调用 2 次
        assert len(callback_calls) == 2
        # 第 1 页：覆盖到该页最小日期 2024-03-28 至 2024-03-31
        assert callback_calls[0][1] == "2024-03-28"
        assert callback_calls[0][2] == "2024-03-31"
        assert len(callback_calls[0][0]) == 2

        # 第 2 页（最后一页）：覆盖至请求起始日期 2024-03-01
        assert callback_calls[1][1] == "2024-03-01"
        assert callback_calls[1][2] == "2024-03-31"
        assert len(callback_calls[1][0]) == 1

        # 验证防反爬抖动休眠被调用（第二页开始触发）
        assert mock_sleep.call_count >= 1


@pytest.mark.asyncio
async def test_eastmoney_fetch_retry_and_partial_failure():
    source = EastmoneySource()

    page1_data = {
        "ErrCode": "0",
        "TotalCount": "40",
        "Data": {
            "LSJZList": [
                {
                    "FSRQ": "2024-03-29",
                    "DWJZ": "3.5000",
                    "LJJZ": "4.5000",
                    "JZZZL": "1.25",
                }
            ]
        },
    }

    callback_calls = []

    async def mock_on_page(page_items, cov_s, cov_e):
        callback_calls.append((page_items, cov_s, cov_e))

    # 将单页大小打桩为 1，第 1 页有 1 条，第 2 页失败 (None)，支持 3 次重试
    with patch("providers.funds.eastmoney._LSJZ_PAGE_SIZE", 1), patch.object(
        source,
        "_fetch_lsjz_page",
        side_effect=[page1_data, None, None, None],
    ), patch("asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="lsjz abnormal response for 510300 page 2"):
            await source.get_fund_nav_history(
                code="510300",
                start_date="2024-01-01",
                end_date="2024-03-31",
                on_page=mock_on_page,
            )

        # 核心保证：即使第二页抛出异常，第一页成功的数据已经通过 on_page 提交并落盘！
        assert len(callback_calls) == 1
        assert callback_calls[0][1] == "2024-03-29"
        assert callback_calls[0][2] == "2024-03-31"


@pytest.mark.asyncio
async def test_fetch_lsjz_page_retry_succeeds():
    source = EastmoneySource()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"ErrCode": "0", "TotalCount": "1", "Data": {"LSJZList": []}}
    mock_resp.raise_for_status = MagicMock()

    # 模拟第一次超时失败，第二次成功
    client_instance = AsyncMock()
    client_instance.get.side_effect = [Exception("Timeout"), mock_resp]

    client_context = MagicMock()
    client_context.__aenter__.return_value = client_instance
    client_context.__aexit__.return_value = None

    with patch("httpx.AsyncClient", return_value=client_context), patch(
        "asyncio.sleep", new_callable=AsyncMock
    ) as mock_sleep:
        data = await source._fetch_lsjz_page("510300", 1, 20, max_retries=3)
        assert data is not None
        assert data["ErrCode"] == "0"
        assert client_instance.get.call_count == 2
        assert mock_sleep.call_count == 1


# ---------------------------------------------------------------------------
# 场内最新净值：东财「场内交易基金排行 dt=fb」+「开放基金排行 dt=kf」两榜合并
#   页面：https://fund.eastmoney.com/data/fbsfundranking.html (fb)
#         https://fund.eastmoney.com/data/fundranking.html  (kf)
#   行格式（逗号分隔字符串）：[0]代码 [1]简称 [2]拼音 [3]净值日期 [4]单位净值 [5]累计净值
#   依据：dt=fb 只覆盖白名单 1631/2067（ETF 全在 fb），LOF（161725/160105…）只在 kf。
# ---------------------------------------------------------------------------

WHITELIST = {"510300", "159915", "161725"}
LATEST_TRADING_DAY = "2026-09-11"


@pytest.fixture(autouse=True)
def _stub_trading_day():
    """固定「最近一个交易日」，避免测试打真实深交所日历。"""
    with patch(
        "providers.funds.eastmoney._calendar.latest_trading_day",
        new_callable=AsyncMock,
        return_value=LATEST_TRADING_DAY,
    ):
        yield


def _rank_row(code: str, nav_date: str, unit: str, accum: str) -> str:
    """构造 rankhandler datas 里的一行（逗号拼接）。"""
    return ",".join([code, code + "基金", "PY", nav_date, unit, accum, "-1.23"])


def _rank_page(rows, all_records=None, all_pages=1):
    return {
        "rows": rows,
        "all_records": len(rows) if all_records is None else all_records,
        "all_pages": all_pages,
    }


def _fetch_by_dt(by_dt):
    """按 dt 分派的假分页函数：by_dt = {"fb": [第1页, 第2页...], "kf": [...]}，缺省为空榜。"""

    async def _fetch(dt, page_index, page_size):
        pages = by_dt.get(dt) or []
        if page_index - 1 < len(pages):
            return pages[page_index - 1]
        return _rank_page([])

    return _fetch


def _patch_rank(by_dt):
    """统一 patch：交易所白名单 + 分页拉取。"""
    return [
        patch("providers.funds.eastmoney.get_exchange_listed_codes", new_callable=AsyncMock, return_value=WHITELIST),
        patch.object(EastmoneySource, "_fetch_rank_page", side_effect=_fetch_by_dt(by_dt)),
    ]


@pytest.mark.asyncio
async def test_latest_all_nav_merges_fb_and_kf_and_filters_non_exchange_listed():
    """两榜合并：fb 取 ETF，kf 取 LOF；不在白名单的场外基金被过滤。"""
    source = EastmoneySource()
    by_dt = {
        "fb": [_rank_page([
            _rank_row("510300", "2026-09-11", "4.5794", "2.0252"),
            _rank_row("000001", "2026-09-11", "9.9", "9.9"),
        ])],
        "kf": [_rank_page([_rank_row("161725", "2026-09-11", "0.5337", "2.2498")])],
    }
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        items = await source.get_latest_all_nav()

    got = {it.code: (it.nav_date, it.unit_nav, it.accum_nav) for it in items}
    assert got == {
        "510300": ("2026-09-11", 4.5794, 2.0252),
        "161725": ("2026-09-11", 0.5337, 2.2498),
    }


@pytest.mark.asyncio
async def test_latest_all_nav_paginates_each_list():
    """allPages=2 时必须翻到第 2 页，不能只取首页。"""
    source = EastmoneySource()
    by_dt = {
        "fb": [
            _rank_page(
                [_rank_row("510300", "2026-09-11", "1.0", "1.0"), _rank_row("159915", "2026-09-11", "2.0", "2.0")],
                all_records=3,
                all_pages=2,
            ),
            _rank_page([_rank_row("159919", "2026-09-11", "3.0", "3.0")], all_records=3, all_pages=2),
        ],
    }
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        items = await source.get_latest_all_nav()

    assert sorted(it.code for it in items) == ["159915", "510300"]


@pytest.mark.asyncio
async def test_latest_all_nav_raises_on_page_failure():
    """任一榜单第 2 页失败必须抛错，禁止把第 1 页当全量返回。"""
    source = EastmoneySource()
    by_dt = {
        "fb": [
            _rank_page([_rank_row("510300", "2026-09-11", "1.0", "1.0")], all_records=2, all_pages=2),
            None,
        ],
    }
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        with pytest.raises(RuntimeError, match="dt=fb"):
            await source.get_latest_all_nav()


@pytest.mark.asyncio
async def test_latest_all_nav_raises_when_rows_short_of_all_records():
    """实际行数少于上游自述 allRecords → 抛错，不返回半份。"""
    source = EastmoneySource()
    by_dt = {
        "fb": [_rank_page([_rank_row("510300", "2026-09-11", "1.0", "1.0")], all_records=10, all_pages=1)],
    }
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        with pytest.raises(RuntimeError, match="incomplete"):
            await source.get_latest_all_nav()


@pytest.mark.asyncio
async def test_latest_all_nav_fixes_cross_year_date():
    """元旦后上游年份未回退：净值日期晚于最近交易日 → 年份减 1。"""
    source = EastmoneySource()
    by_dt = {"fb": [_rank_page([_rank_row("510300", "2027-12-31", "1.236", "1.236")])]}
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        stack.enter_context(
            patch(
                "providers.funds.eastmoney._calendar.latest_trading_day",
                new_callable=AsyncMock,
                return_value="2027-01-05",
            )
        )
        items = await source.get_latest_all_nav()

    assert [(it.code, it.nav_date) for it in items] == [("510300", "2026-12-31")]


@pytest.mark.asyncio
async def test_latest_all_nav_keeps_normal_date_unchanged():
    """正常日期（<= 最近交易日）不得被改动。"""
    source = EastmoneySource()
    by_dt = {"fb": [_rank_page([_rank_row("510300", "2026-09-11", "1.5", "2.5")])]}
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        items = await source.get_latest_all_nav()

    assert [(it.code, it.nav_date, it.unit_nav, it.accum_nav) for it in items] == [
        ("510300", "2026-09-11", 1.5, 2.5)
    ]


@pytest.mark.asyncio
async def test_latest_all_nav_keeps_previous_year_date():
    """元旦后：上游已正确给出上一年 12-31，不得被改动。"""
    source = EastmoneySource()
    by_dt = {"fb": [_rank_page([_rank_row("510300", "2025-12-31", "1.0", "1.0")])]}
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        stack.enter_context(
            patch(
                "providers.funds.eastmoney._calendar.latest_trading_day",
                new_callable=AsyncMock,
                return_value="2026-01-05",
            )
        )
        items = await source.get_latest_all_nav()

    assert [(it.code, it.nav_date) for it in items] == [("510300", "2025-12-31")]


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_latest_all_nav_skips_row_with_empty_date():
    """上游存在日期为空的记录（dt=kf 实测 33 行）：无法定日期，必须跳过而不是硬塞。"""
    source = EastmoneySource()
    by_dt = {
        "fb": [_rank_page([
            _rank_row("510300", "2026-09-11", "1.0", "1.0"),
            _rank_row("159915", "", "2.0", "2.0"),
        ])],
    }
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        items = await source.get_latest_all_nav()

    assert [(it.code, it.nav_date) for it in items] == [("510300", "2026-09-11")]


@pytest.mark.asyncio
async def test_latest_all_nav_applies_polite_delay_between_pages():
    """两榜翻页（allPages=2）之间必须有礼貌延时。"""
    source = EastmoneySource()
    by_dt = {
        "fb": [
            _rank_page(
                [_rank_row("510300", "2026-09-11", "1.0", "1.0")],
                all_records=2,
                all_pages=2,
            ),
            _rank_page([_rank_row("159915", "2026-09-11", "2.0", "2.0")], all_records=2, all_pages=2),
        ],
    }
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        delay = stack.enter_context(
            patch("providers.funds.eastmoney.polite_delay", new_callable=AsyncMock)
        )
        await source.get_latest_all_nav()

    assert delay.call_count >= 2, "翻页 + 换榜单 都要延时"


# ---------------------------------------------------------------------------
# N6：单位净值为空、只有累计净值的行不得放行（与 CMTIDP 路径口径一致）
# N7：完整性/翻页判据不得在解析失败时静默降级；rank 分页要能重试
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_latest_all_nav_skips_row_without_unit_nav():
    """N6：单位净值为空的行必须丢弃；只有累计净值有值也不行。

    CMTIDP 路径是 `if unit_nav is None: continue`，两条净值源对同一概念必须同口径，
    否则 C# 侧（按 NetValue is not null 过滤）会拿到「有记录但单位净值为空」的脏行。
    """
    source = EastmoneySource()
    by_dt = {
        "fb": [_rank_page([
            _rank_row("510300", "2026-09-11", "", "2.0252"),        # 只有累计净值 → 丢弃
            _rank_row("159915", "2026-09-11", "1.2345", ""),        # 只有单位净值 → 保留
        ])],
        "kf": [],
    }
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        items = await source.get_latest_all_nav()

    assert {it.code: (it.unit_nav, it.accum_nav) for it in items} == {
        "159915": (1.2345, None),
    }


@pytest.mark.asyncio
async def test_latest_all_nav_raises_when_completeness_metadata_missing():
    """N7：allRecords/allPages 都解析不到时，旧实现退化成「只取首页且不校验」。

    上游改字段名/改报文格式时，这等于静默丢数据（dt=kf 自述 24459 条而单页上限 20000），
    必须显式报错。
    """
    source = EastmoneySource()
    by_dt = {
        "fb": [{"rows": [_rank_row("510300", "2026-09-11", "1.0", "1.0")], "all_records": None, "all_pages": None}],
        "kf": [{"rows": [_rank_row("161725", "2026-09-11", "1.0", "1.0")], "all_records": None, "all_pages": None}],
    }
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        with pytest.raises(RuntimeError, match="allRecords"):
            await source.get_latest_all_nav()


@pytest.mark.asyncio
async def test_latest_all_nav_derives_pages_when_all_pages_missing():
    """N7：allPages 缺失但 allRecords 可得时，按总数推导页数，不得只取首页。"""
    source = EastmoneySource()
    by_dt = {
        "fb": [
            {"rows": [_rank_row("510300", "2026-09-11", "1.0", "1.0"),
                      _rank_row("159915", "2026-09-11", "2.0", "2.0")],
             "all_records": 3, "all_pages": None},
            {"rows": [_rank_row("161725", "2026-09-11", "3.0", "3.0")],
             "all_records": 3, "all_pages": None},
        ],
        "kf": [],
    }
    with ExitStack() as stack:
        for patcher in _patch_rank(by_dt):
            stack.enter_context(patcher)
        stack.enter_context(patch("providers.funds.eastmoney._EM_RANK_PAGE_SIZE", 2))
        items = await source.get_latest_all_nav()

    assert sorted(it.code for it in items) == ["159915", "161725", "510300"]


@pytest.mark.asyncio
async def test_fetch_rank_page_retries_transient_failure():
    """N7：rank 分页此前一次都不重试，单次网络抖动就会让整批全量净值失败。"""
    source = EastmoneySource()
    calls = {"n": 0}

    class _Resp:
        status_code = 200
        text = 'var rankData={datas:["510300,300ETF,PY,2026-09-11,1.0,1.0,-1.23"],allRecords:1,allPages:1};'

        def raise_for_status(self):
            return None

    async def _get(url, params=None, headers=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("connection reset by peer")
        return _Resp()

    with patch("httpx.AsyncClient.get", side_effect=_get), patch(
        "asyncio.sleep", new_callable=AsyncMock
    ):
        data = await source._fetch_rank_page("fb", 1, 20000)

    assert calls["n"] == 2, "首次失败必须重试"
    assert data["all_records"] == 1
    assert data["rows"][0].startswith("510300")


@pytest.mark.asyncio
async def test_fetch_rank_page_returns_none_after_exhausting_retries():
    """重试耗尽仍失败 → 返回 None，由调用方抛「不完整」错误（不静默降级）。"""
    source = EastmoneySource()
    calls = {"n": 0}

    async def _get(url, params=None, headers=None):
        calls["n"] += 1
        raise RuntimeError("upstream down")

    with patch("httpx.AsyncClient.get", side_effect=_get), patch(
        "asyncio.sleep", new_callable=AsyncMock
    ):
        data = await source._fetch_rank_page("fb", 1, 20000)

    assert data is None
    assert calls["n"] == 3
