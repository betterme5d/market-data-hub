# -*- coding: utf-8 -*-
import os
import shutil
import tempfile
from unittest.mock import AsyncMock, patch
import pytest

from core.models import FundNav
from core.timeseries_cache.manager import TimeSeriesCacheManager
from providers.funds.fund_nav import FundNavProvider

# 测试产物落在系统临时目录：仓库目录被 dev 容器挂载并 watch，
# 在源码树内反复建/删目录会让 uvicorn 的 StatReload 看门狗 rglob 撞上已消失的目录而崩溃
TEST_CACHE_DIR = os.path.join(tempfile.gettempdir(), f"funds_nav_provider_test_{os.getpid()}")


@pytest.fixture(autouse=True)
def clean_cache():
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)
    yield
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)


@pytest.mark.asyncio
async def test_get_fund_nav_history_cached():
    cache_manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    mock_em = AsyncMock()
    mock_em.get_fund_nav_history.return_value = [
        FundNav(code="510300", nav_date="2024-01-02", unit_nav=3.5, accum_nav=3.6),
        FundNav(code="510300", nav_date="2024-01-03", unit_nav=3.55, accum_nav=3.65),
    ]

    provider = FundNavProvider(
        cache_manager=cache_manager,
        eastmoney_source=mock_em
    )

    # 首次查询：拉取并缓存
    res1 = await provider.get_fund_nav_history("510300", "2024-01-01", "2024-01-10", source="eastmoney")
    assert len(res1) == 2
    assert isinstance(res1[0], FundNav)
    assert res1[0].nav_date == "2024-01-02"
    assert mock_em.get_fund_nav_history.call_count == 1

    # 第二次查询：完全命中缓存
    mock_em.get_fund_nav_history.reset_mock()
    res2 = await provider.get_fund_nav_history("510300", "2024-01-01", "2024-01-10", source="eastmoney")
    assert len(res2) == 2
    assert mock_em.get_fund_nav_history.call_count == 0


@pytest.mark.asyncio
async def test_empty_upstream_response_does_not_poison_coverage():
    """上游静默失败（ErrCode=0 但列表为空）不得写入覆盖区间，否则重跑永远只命中空缓存。
    端到端链路：FundNavProvider → TimeSeriesCacheManager → EastmoneySource（真实分页爬虫）。"""
    from providers.funds.eastmoney import EastmoneySource

    cache_manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    em = EastmoneySource()
    provider = FundNavProvider(cache_manager=cache_manager, eastmoney_source=em)

    empty_page = {"ErrCode": "0", "TotalCount": "0", "Data": {"LSJZList": []}}
    with patch.object(em, "_fetch_lsjz_page", return_value=empty_page) as mock_page:
        res = await provider.get_fund_nav_history("159976", "2020-01-01", "2020-12-31", source="eastmoney")
        assert res == []

        # 空响应不记覆盖：meta 里不能出现任何 intervals
        meta = cache_manager.storage.read_metadata(
            "fund_nav", "159976", dimensions={"source": "eastmoney"}
        )
        assert (meta or {}).get("intervals", []) == []

        # 上游恢复后重跑：缺口仍视为缺失，必须重新问上游并正常落盘
        mock_page.return_value = {
            "ErrCode": "0",
            "TotalCount": "1",
            "Data": {"LSJZList": [{"FSRQ": "2020-06-30", "DWJZ": "1.5000", "LJJZ": "1.6000"}]},
        }
        res2 = await provider.get_fund_nav_history("159976", "2020-01-01", "2020-12-31", source="eastmoney")
        assert [r.nav_date for r in res2] == ["2020-06-30"]
        assert mock_page.call_count == 2

        meta2 = cache_manager.storage.read_metadata(
            "fund_nav", "159976", dimensions={"source": "eastmoney"}
        )
        assert meta2["intervals"] == [["2020-01-01", "2020-12-31"]]


@pytest.mark.asyncio
async def test_source_selection():
    cache_manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    mock_em = AsyncMock()
    mock_cm = AsyncMock()
    mock_cm.get_fund_nav_history.return_value = [
        FundNav(code="510300", nav_date="2024-01-02", unit_nav=3.5)
    ]

    provider = FundNavProvider(
        cache_manager=cache_manager,
        eastmoney_source=mock_em,
        cmtidp_source=mock_cm
    )

    res = await provider.get_fund_nav_history("510300", "2024-01-01", "2024-01-10", source="cmtidp")
    assert len(res) == 1
    assert mock_cm.get_fund_nav_history.call_count == 1
    assert mock_em.get_fund_nav_history.call_count == 0

    # 验证未知数据源抛出 ValueError
    with pytest.raises(ValueError, match="Unknown source"):
        await provider.get_fund_nav_history("510300", "2024-01-01", "2024-01-10", source="invalid_source")


@pytest.mark.asyncio
async def test_cmtidp_probe_success():
    from providers.funds.cmtidp import CmtidpProbe

    probe = CmtidpProbe()
    with patch("providers.funds.cmtidp.CmtidpSource._fetch_page", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = {"iTotalRecords": 1, "aaData": [{"code": "000001"}]}
        # 正常执行不抛异常
        await probe.probe()
        assert mock_fetch.call_count == 1
        call_kwargs = mock_fetch.call_args.kwargs
        assert call_kwargs["fund_code"] == "000001"
        assert call_kwargs["start"] == 0
        assert call_kwargs["length"] == 1


@pytest.mark.asyncio
async def test_cmtidp_probe_failure():
    from providers.funds.cmtidp import CmtidpProbe

    probe = CmtidpProbe()
    with patch("providers.funds.cmtidp.CmtidpSource._fetch_page", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = None
        with pytest.raises(RuntimeError, match="unexpected response shape"):
            await probe.probe()


@pytest.mark.asyncio
async def test_cmtidp_pagination_consistency():
    from providers.funds.cmtidp import CmtidpSource

    src = CmtidpSource()
    # 构造 6 条标准 mock 上游数据
    all_mock_rows = [
        {"code": "510300", "valuationDate": f"2024-01-0{i}", "shareNetValue": f"3.5{i}", "totalNetValue": f"3.5{i}"}
        for i in range(1, 7)
    ]

    async def mock_fetch_page(*, fund_code, start_date, end_date, start, length):
        slice_rows = all_mock_rows[start : start + length]
        return {"iTotalRecords": len(all_mock_rows), "aaData": slice_rows}

    with patch.object(src, "_fetch_page", side_effect=mock_fetch_page):
        # 1. 连续分页 3 次，每页 length=2 (start=0, 2, 4)
        paged_rows = []
        for offset in [0, 2, 4]:
            data = await src._fetch_page(fund_code="510300", start_date="2024-01-01", end_date="2024-01-10", start=offset, length=2)
            paged_rows.extend(data["aaData"])

        # 2. 一次性获取 length=6 (start=0)
        direct_data = await src._fetch_page(fund_code="510300", start_date="2024-01-01", end_date="2024-01-10", start=0, length=6)
        direct_rows = direct_data["aaData"]

        assert len(paged_rows) == 6
        assert len(direct_rows) == 6
        assert paged_rows == direct_rows
