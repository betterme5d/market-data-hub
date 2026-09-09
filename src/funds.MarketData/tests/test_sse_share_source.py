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

    with patch.object(source, "fetch_daily_market_shares", return_value={"510050": {"code": "510050", "share_date": "d", "shares": 1.0}}), \
         patch("asyncio.sleep", side_effect=mock_sleep):
        # 2026-06-25 与 2026-06-26 均为周四、周五交易日
        await source.fetch_market_shares_range("2026-06-25", "2026-06-26")

        assert len(sleep_calls) == 1
        assert 0.5 <= sleep_calls[0] <= 3.0


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


