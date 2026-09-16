# -*- coding: utf-8 -*-
import os
import shutil
import tempfile
from unittest.mock import AsyncMock, patch
import pytest

from core.models import FundNav
from core.timeseries_cache.manager import TimeSeriesCacheManager
from providers.funds.base_nav import FundNavSource
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


@pytest.mark.asyncio
async def test_cmtidp_partial_persist_on_midway_failure():
    """CMTIDP 中途分页失败：已拉到的页必须已通过 on_page 落盘，不能整段作废。"""
    from providers.funds.cmtidp import CmtidpSource

    cache_manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    src = CmtidpSource()
    calls = {"n": 0}

    async def _flaky(*, fund_code, start_date, end_date, start, length):
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "iTotalRecords": 2,
                "aaData": [
                    {
                        "code": "510300",
                        "valuationDate": "2026-09-09",
                        "shareNetValue": "3.5000",
                        "totalNetValue": "4.5000",
                    }
                ],
            }
        return None

    provider = FundNavProvider(cache_manager=cache_manager, cmtidp_source=src)

    with patch("providers.funds.cmtidp._CMTIDP_PAGE_SIZE", 1), patch.object(
        src, "_fetch_page", side_effect=_flaky
    ):
        with pytest.raises(RuntimeError):
            await provider.get_fund_nav_history(
                "510300", "2026-09-01", "2026-09-30", source="cmtidp"
            )

    meta = cache_manager.storage.read_metadata(
        "fund_nav", "510300", dimensions={"source": "cmtidp"}
    )
    assert meta is not None, "第一页必须已落盘（on_page 流式）"
    assert meta["intervals"] == [["2026-09-09", "2026-09-09"]]


# ---------------------------------------------------------------------------
# C1：逐页流式能力改为「显式声明」，不再用 inspect.signature 隐式探测
# ---------------------------------------------------------------------------


class _KwargsStreamSource(FundNavSource):
    """用 **kwargs 接收 on_page：inspect.signature 看不到具名参数，会被误判为不支持流式。"""

    name = "kw"
    supports_streaming = True

    def __init__(self):
        self.received_on_page = False

    async def get_latest_all_nav(self):
        return []

    async def get_fund_nav_history(self, code, start_date=None, end_date=None, **kwargs):
        self.received_on_page = kwargs.get("on_page") is not None
        return [FundNav(code=code, nav_date="2024-01-02", unit_nav=1.0)]


class _DeclaredNoStreamSource(FundNavSource):
    """签名里有 on_page，但显式声明不支持流式 → 不得传入。"""

    name = "nostream"
    supports_streaming = False

    def __init__(self):
        self.received_on_page = False

    async def get_latest_all_nav(self):
        return []

    async def get_fund_nav_history(self, code, start_date=None, end_date=None, on_page=None):
        self.received_on_page = on_page is not None
        return [FundNav(code=code, nav_date="2024-01-02", unit_nav=1.0)]


@pytest.mark.asyncio
async def test_streaming_capability_is_declared_not_inspected():
    """C1：声明 supports_streaming=True 就必须收到 on_page（**kwargs 形式也要能收到）。

    旧实现用 `inspect.signature(...).parameters` 探测，`**kwargs` 形式会被误判为不支持，
    于是「缓存层以为有逐页落盘、实际没有」，中途失败整段作废。
    """
    cache_manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    src = _KwargsStreamSource()
    provider = FundNavProvider(cache_manager=cache_manager, eastmoney_source=src)

    await provider.get_fund_nav_history("510300", "2024-01-01", "2024-01-10", source="eastmoney")

    assert src.received_on_page is True


@pytest.mark.asyncio
async def test_streaming_capability_false_blocks_on_page():
    """C1：显式声明 supports_streaming=False 时不得传 on_page（哪怕签名里有）。"""
    cache_manager = TimeSeriesCacheManager(base_dir=TEST_CACHE_DIR)
    src = _DeclaredNoStreamSource()
    provider = FundNavProvider(cache_manager=cache_manager, eastmoney_source=src)

    await provider.get_fund_nav_history("510300", "2024-01-01", "2024-01-10", source="eastmoney")

    assert src.received_on_page is False


def test_builtin_sources_declare_streaming_capability():
    """两个内置源都必须显式声明支持流式，否则逐页落盘会静默退化。"""
    from providers.funds.cmtidp import CmtidpSource
    from providers.funds.eastmoney import EastmoneySource

    assert FundNavSource.supports_streaming is False, "基类默认不支持，由实现方显式开启"
    assert EastmoneySource.supports_streaming is True
    assert CmtidpSource.supports_streaming is True
