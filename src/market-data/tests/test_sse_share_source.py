import asyncio

import pytest
from unittest.mock import AsyncMock, patch
from providers.exchanges.shares.sse import SseShareSource

@pytest.mark.asyncio
async def test_sse_fetch_daily_market_shares_merge():
    source = SseShareSource(delay_ms=0)
    
    # 模拟股票ETF、货币ETF与LOF三个接口的返回
    etf_json = {
        "result": [
            {"SEC_CODE": "510050", "SEC_NAME": "50ETF", "STAT_DATE": "2026-06-25", "TOT_VOL": "15000.5"}
        ]
    }
    mkt_json = {
        "result": [
            {"SEC_CODE": "511990", "SEC_NAME": "华宝添益", "STAT_DATE": "2026-06-25", "TOT_VOL": "20000.0"}
        ]
    }
    lof_json = {
        "result": [
            {"FUND_CODE": "501001", "FUND_ABBR": "财通精选", "TRADE_DATE": "20260625", "INTERNAL_VOL": "550.45"}
        ]
    }

    async def mock_get(url, *args, **kwargs):
        resp = AsyncMock()
        resp.status_code = 200
        resp.raise_for_status = lambda: None
        params = kwargs.get("params", {})
        sql_id = params.get("sqlId", "")
        if "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L" in sql_id or "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L" in url:
            resp.json = lambda: etf_json
        elif "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_JYXJJ_SEARCH_L" in sql_id or "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_JYXJJ_SEARCH_L" in url:
            resp.json = lambda: mkt_json
        elif "COMMON_SSE_SJ_JJSJ_JJGM_LOFGMTJ_L" in sql_id or "COMMON_SSE_SJ_JJSJ_JJGM_LOFGMTJ_L" in url:
            resp.json = lambda: lof_json
        else:
            resp.json = lambda: {"result": []}
        return resp

    with patch("httpx.AsyncClient.get", side_effect=mock_get):
        daily_records = await source.fetch_daily_market_shares("2026-06-25")
        assert len(daily_records) == 3
        assert "510050" in daily_records
        assert "511990" in daily_records
        assert "501001" in daily_records

        # 校验字段转换与单位
        e50 = daily_records["510050"]
        assert e50["shares"] == 15000.5
        assert e50["raw_shares"] == 150005000.0
        assert e50["name"] == "50ETF"
        assert e50["share_date"] == "2026-06-25"

        # 校验 LOF 字段
        lof = daily_records["501001"]
        assert lof["shares"] == 550.45
        assert lof["name"] == "财通精选"
        assert lof["share_date"] == "2026-06-25"

@pytest.mark.asyncio
async def test_sse_fetch_range_with_trading_day_filter():
    source = SseShareSource(delay_ms=0)
    
    # 模拟周末 2026-06-27 (周六) 至 2026-06-28 (周日)，无交易日
    records = await source.fetch_market_shares_range("2026-06-27", "2026-06-28")
    assert records == {}

@pytest.mark.asyncio
async def test_sse_fetch_range_delay_control():
    # 默认延时应为 0.5s - 3.0s
    source = SseShareSource()
    assert source.min_delay == 0.5
    assert source.max_delay == 3.0

    sleep_calls = []
    async def mock_sleep(sec):
        sleep_calls.append(sec)

    with patch.object(source, "fetch_daily_market_shares_checked", return_value=({"510050": {"code": "510050", "share_date": "d", "shares": 1.0}}, True)), \
         patch("asyncio.sleep", side_effect=mock_sleep):
        # 2026-06-25 与 2026-06-26 均为周四、周五交易日
        await source.fetch_market_shares_range("2026-06-25", "2026-06-26")

        assert len(sleep_calls) == 1
        assert 0.5 <= sleep_calls[0] <= 3.0


@pytest.mark.asyncio
async def test_sse_fetch_range_flushes_incrementally():
    """每积累 flush_every_days 个交易日落盘一次，收尾再落一次未满窗口的尾巴。"""
    source = SseShareSource(delay_ms=0)
    flush_calls = []

    async def mock_checked(day):
        return {"510050": {"code": "510050", "share_date": day, "shares": 1.0}}, True

    def on_window(records, w_start, w_end):
        flush_calls.append((w_start, w_end, len(records.get("510050", []))))

    with patch.object(source, "fetch_daily_market_shares_checked", side_effect=mock_checked):
        # 2026-06-22(周一)~06-26(周五) 共 5 个交易日，flush_every_days=2 → 2+2+1
        await source.fetch_market_shares_range(
            "2026-06-22", "2026-06-26", on_window=on_window, flush_every_days=2
        )

    assert flush_calls == [
        ("2026-06-22", "2026-06-23", 2),
        ("2026-06-24", "2026-06-25", 2),
        ("2026-06-26", "2026-06-26", 1),
    ]


@pytest.mark.asyncio
async def test_sse_fetch_range_flushes_partial_window_on_cancel():
    """客户端断开（中间件 cancel）时，已拉到的交易日必须落盘，不能整段作废。"""
    source = SseShareSource(delay_ms=0)
    flush_calls = []
    fetched = []

    async def mock_checked(day):
        if day == "2026-06-24":
            raise asyncio.CancelledError()
        fetched.append(day)
        return {"510050": {"code": "510050", "share_date": day, "shares": 1.0}}, True

    def on_window(records, w_start, w_end):
        flush_calls.append((w_start, w_end, len(records.get("510050", []))))

    with patch.object(source, "fetch_daily_market_shares_checked", side_effect=mock_checked):
        with pytest.raises(asyncio.CancelledError):
            await source.fetch_market_shares_range(
                "2026-06-22", "2026-06-26", on_window=on_window, flush_every_days=10
            )

    # 22/23 已拉到但未满一个窗口，取消时收尾落盘；24 及之后未取到，不能被标记为已缓存
    assert fetched == ["2026-06-22", "2026-06-23"]
    assert flush_calls == [("2026-06-22", "2026-06-23", 2)]


@pytest.mark.asyncio
async def test_sse_fetch_daily_prior_to_2012_skips_all_requests():
    """验证 2012-01-04 之前，上交所不发任何网络请求，直接返回空字典。"""
    source = SseShareSource(delay_ms=0)
    mock_get = AsyncMock()

    with patch("httpx.AsyncClient.get", mock_get):
        # 2011-12-30 为 2011 年最后一个交易日
        records = await source.fetch_daily_market_shares("2011-12-30")
        assert records == {}
        # 验证 0 网络调用
        assert mock_get.call_count == 0


@pytest.mark.asyncio
async def test_sse_fetch_daily_between_2012_and_2013_only_calls_etf():
    """验证 2012-01-04 至 2013-01-27 之间，仅调用常规 ETF 接口（1 个请求）。"""
    source = SseShareSource(delay_ms=0)
    requested_sql_ids = []

    async def mock_get(url, *args, **kwargs):
        resp = AsyncMock()
        resp.status_code = 200
        params = kwargs.get("params", {})
        sql_id = params.get("sqlId", "")
        requested_sql_ids.append(sql_id)
        if "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L" in sql_id:
            resp.json = lambda: {
                "result": [
                    {"SEC_CODE": "510050", "SEC_NAME": "50ETF", "STAT_DATE": "2012-06-15", "TOT_VOL": "1000.0"}
                ]
            }
        else:
            resp.json = lambda: {"result": []}
        return resp

    with patch("httpx.AsyncClient.get", side_effect=mock_get):
        records = await source.fetch_daily_market_shares("2012-06-15")
        assert len(requested_sql_ids) == 1
        assert "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L" in requested_sql_ids[0]
        assert "510050" in records


@pytest.mark.asyncio
async def test_sse_fetch_daily_between_2013_and_2015_calls_etf_and_money_etf():
    """验证 2013-01-28 至 2015-04-26 之间，调用常规 ETF + 货币 ETF（2 个请求），不调用 LOF。"""
    source = SseShareSource(delay_ms=0)
    requested_sql_ids = []

    async def mock_get(url, *args, **kwargs):
        resp = AsyncMock()
        resp.status_code = 200
        params = kwargs.get("params", {})
        sql_id = params.get("sqlId", "")
        requested_sql_ids.append(sql_id)
        resp.json = lambda: {"result": []}
        return resp

    with patch("httpx.AsyncClient.get", side_effect=mock_get):
        await source.fetch_daily_market_shares("2014-06-15")
        assert len(requested_sql_ids) == 2
        assert any("COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L" in s for s in requested_sql_ids)
        assert any("COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_JYXJJ_SEARCH_L" in s for s in requested_sql_ids)
        assert not any("COMMON_SSE_SJ_JJSJ_JJGM_LOFGMTJ_L" in s for s in requested_sql_ids)


@pytest.mark.asyncio
async def test_sse_fetch_range_prior_to_2012_has_no_delay():
    """验证拉取 2012 年之前的历史交易日时，0 延迟秒级跳过，不触发 sleep。"""
    source = SseShareSource()
    sleep_calls = []

    async def mock_sleep(sec):
        sleep_calls.append(sec)

    with patch("asyncio.sleep", side_effect=mock_sleep):
        # 2011-12-26 至 2011-12-30 均为交易日，但均在 2012-01-04 之前
        records = await source.fetch_market_shares_range("2011-12-26", "2011-12-30")
        assert records == {}
        assert len(sleep_calls) == 0




@pytest.mark.asyncio
async def test_sse_incomplete_day_stays_uncovered():
    """某个分类接口挂掉的那一天：不落盘、不进入覆盖区间，留给下次补录重取。

    否则整个窗口被标成已覆盖，而那天缺的那一类基金会永久缺失。
    """
    source = SseShareSource(delay_ms=0)
    windows = []

    async def mock_checked(day):
        rec = {"510050": {"code": "510050", "share_date": day, "shares": 1.0}}
        if day == "2026-06-24":
            return rec, False   # 该日某个分类接口失败 → 不完整
        return rec, True

    with patch.object(source, "fetch_daily_market_shares_checked", side_effect=mock_checked):
        records = await source.fetch_market_shares_range(
            "2026-06-22", "2026-06-26",
            on_window=lambda w, s, e: windows.append((s, e, len(w.get("510050", [])))),
            flush_every_days=10,
        )

    # 22/23 一个窗口；24 不完整 → 窗口在 23 结束，25 处重开
    assert windows == [("2026-06-22", "2026-06-23", 2), ("2026-06-25", "2026-06-26", 2)]
    # 失败日已拿到的部分记录既不落盘也不返回，避免半天数据进缓存；
    # 且 D17 之后"有增量回调时"不再保留整段全市场记录（数据已逐窗口落盘）
    assert records == {}


@pytest.mark.asyncio
async def test_sse_fetch_range_with_on_window_does_not_retain_full_records():
    """D17：有增量回调时不再在内存里累积整段全市场记录，只保留当前窗口缓冲。

    10 年沪市回补 = 2000+ 个交易日 × 全市场基金，整段累积会常驻数百 MB。
    """
    source = SseShareSource(delay_ms=0)
    windows = []

    async def mock_checked(day):
        return {"510050": {"code": "510050", "share_date": day, "shares": 1.0}}, True

    with patch.object(source, "fetch_daily_market_shares_checked", side_effect=mock_checked):
        records = await source.fetch_market_shares_range(
            "2026-06-22", "2026-06-26",
            on_window=lambda w, s, e: windows.append((s, e)),
            flush_every_days=2,
        )

    assert windows == [
        ("2026-06-22", "2026-06-23"),
        ("2026-06-24", "2026-06-25"),
        ("2026-06-26", "2026-06-26"),
    ]
    assert records == {}, "有回调时数据已逐窗口落盘，不应再返回/保留整段记录"

@pytest.mark.asyncio
async def test_sse_pre_2012_days_are_complete():
    """2012-01-04 之前源侧不发请求，但属于"完整"（可以标记覆盖），否则永远重复走这段日历。"""
    source = SseShareSource(delay_ms=0)
    records, complete = await source.fetch_daily_market_shares_checked("2005-03-01")
    assert records == {}
    assert complete is True


# ---------------------------------------------------------------------------
# N1：HTTP 200 但响应不可用（空 result / 结构变更 / 非 JSON）必须判为「不完整」，
# 否则该交易日会进入覆盖区间，缺失数据被永久固化（覆盖区间一旦标记就再也不会问上游）。
# ---------------------------------------------------------------------------


def _sse_response(json_impl, status_code: int = 200):
    """构造一个最小 httpx 响应替身（get 的返回值）。"""
    resp = AsyncMock()
    resp.status_code = status_code
    resp.raise_for_status = lambda: None
    resp.json = json_impl
    return resp


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case_name,json_impl",
    [
        ("三个接口全返回空 result", lambda sql_id: {"result": []}),
        ("result 键缺失（上游改结构）", lambda sql_id: {"pageHelp": {"total": 0}}),
        ("记录存在但字段缺失（TOT_VOL 全空）", lambda sql_id: {"result": [{"SEC_CODE": "510050", "SEC_NAME": "50ETF"}]}),
    ],
)
async def test_sse_empty_or_malformed_payload_is_incomplete(case_name, json_impl):
    """2026-06-25 为交易日；已激活的分类接口拿不到任何有效记录 → 必须 complete=False。

    否则 fetch_market_shares_range 会把这一天并入覆盖窗口，此后永不重取。
    """
    source = SseShareSource(delay_ms=0)

    async def mock_get(url, *args, **kwargs):
        sql_id = kwargs.get("params", {}).get("sqlId", "")
        return _sse_response(lambda: json_impl(sql_id))

    with patch("httpx.AsyncClient.get", side_effect=mock_get):
        records, complete = await source.fetch_daily_market_shares_checked("2026-06-25")

    assert records == {}, case_name
    assert complete is False, f"{case_name}：必须判为不完整，否则该交易日被永久固化"


@pytest.mark.asyncio
async def test_sse_non_json_payload_is_incomplete():
    """HTTP 200 但响应体不是 JSON（典型：被 WAF/反爬页挡下）→ 必须 complete=False。"""
    source = SseShareSource(delay_ms=0)

    def _raise_json_error():
        raise ValueError("Expecting value: line 1 column 1 (char 0)")

    async def mock_get(url, *args, **kwargs):
        return _sse_response(_raise_json_error)

    with patch("httpx.AsyncClient.get", side_effect=mock_get):
        records, complete = await source.fetch_daily_market_shares_checked("2026-06-25")

    assert records == {}
    assert complete is False


@pytest.mark.asyncio
async def test_sse_partially_parsable_day_is_still_complete():
    """有任一分类接口拿到有效记录时仍判完整：避免把正常交易日误判为不完整而反复重取。"""
    source = SseShareSource(delay_ms=0)

    async def mock_get(url, *args, **kwargs):
        sql_id = kwargs.get("params", {}).get("sqlId", "")
        if "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L" in sql_id:
            return _sse_response(
                lambda: {"result": [{"SEC_CODE": "510050", "SEC_NAME": "50ETF", "TOT_VOL": "15000.5"}]}
            )
        # 货币 ETF / LOF 当日无数据（合法情况）
        return _sse_response(lambda: {"result": []})

    with patch("httpx.AsyncClient.get", side_effect=mock_get):
        records, complete = await source.fetch_daily_market_shares_checked("2026-06-25")

    assert list(records.keys()) == ["510050"]
    assert complete is True


@pytest.mark.asyncio
async def test_sse_all_empty_day_stays_uncovered_in_range_fetch():
    """端到端：某交易日三个接口全空 → 该日不得进入覆盖窗口，且要报进 incomplete_days。"""
    source = SseShareSource(delay_ms=0)
    windows = []

    async def mock_checked(day):
        if day == "2026-06-24":
            return {}, False          # 三个接口全空 → 上游侧判为不完整
        return {"510050": {"code": "510050", "share_date": day, "shares": 1.0}}, True

    with patch.object(source, "fetch_daily_market_shares_checked", side_effect=mock_checked):
        incomplete = []
        records = await source.fetch_market_shares_range(
            "2026-06-22", "2026-06-26",
            on_window=lambda w, s, e: windows.append((s, e)),
            flush_every_days=10,
            incomplete_days_out=incomplete,
        )

    assert records == {}
    # 24 日不完整 → 窗口在 23 日结束、25 日重开；24 日留洞
    assert windows == [("2026-06-22", "2026-06-23"), ("2026-06-25", "2026-06-26")]
    assert incomplete == ["2026-06-24"]
