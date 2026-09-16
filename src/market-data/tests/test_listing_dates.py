# -*- coding: utf-8 -*-
"""交易所基金列表分页完整性 + 上市日期缓存防截断覆盖（D3）回归用例。

背景：交易所列表是下游净值/份额的**白名单**（`core.filters.get_exchange_listed_codes`），
一旦被"截断快照"写短，真实场内基金会整批被过滤掉（数据静默丢失），因此：

1. SSE `commonSoaQuery` 必须按上游自述的 `pageHelp.total` 真翻页（pageSize 被封顶时单页会静默截断）；
2. 任一分类拉取失败必须抛错，不得返回半份列表；
3. 上市日期缓存写盘前必须比较：空快照/变小快照不得清掉旧白名单。
"""
import json
from unittest.mock import AsyncMock, patch

import pytest

from providers.exchanges import listing_dates as listing_mod
from providers.exchanges import sse as sse_mod
from providers.exchanges import szse as szse_mod
from providers.exchanges.listing_dates import ListingDateProvider
from providers.exchanges.sse import SseFundListSource
from providers.exchanges.szse import SzseFundListSource


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    """httpx.AsyncClient 替身：按 URL 里的 pageNo/pageSize 返回对应分页。"""

    def __init__(self, pages: dict, total=None, status_code: int = 200, content: bytes = b""):
        self.pages = pages
        self.total = total
        self.status_code = status_code
        self.content = content
        self.calls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, headers=None, timeout=None):
        self.calls.append(url)
        if self.status_code != 200:
            return _FakeResponse({}, status_code=self.status_code)
        page_no = int(url.split("pageHelp.pageNo=")[1].split("&")[0])
        page_size = int(url.split("pageHelp.pageSize=")[1].split("&")[0])
        return _FakeResponse(
            {
                "result": self.pages.get(page_no, []),
                "pageHelp": {"total": self.total, "pageSize": page_size, "pageNo": page_no},
            }
        )


def _sse_item(code: str) -> dict:
    return {"fundCode": code, "secNameFull": f"基金{code}", "listingDate": "20150925"}


def _patch_sse_client(client: _FakeClient):
    return patch.object(sse_mod.httpx, "AsyncClient", return_value=client)


@pytest.mark.asyncio
async def test_sse_fund_list_pages_until_total():
    """上游把 total 报成多页时必须翻页取全（pageSize 封顶时单页只有一页的数据）。"""
    client = _FakeClient(
        {1: [_sse_item("510000"), _sse_item("510001"), _sse_item("510002")],
         2: [_sse_item("510010"), _sse_item("510011")]},
        total=5,
    )
    with _patch_sse_client(client), patch.object(sse_mod, "polite_delay", new_callable=AsyncMock):
        rows = await SseFundListSource()._fetch_list(
            "00", SseFundListSource.ETF_SUB_CLASS, page_size=3
        )

    assert [r["fund_code"] for r in rows] == ["510000", "510001", "510002", "510010", "510011"]
    assert len(client.calls) == 2, "第二页必须真的被请求"
    assert rows[0]["list_date"] == "2015-09-25"
    assert rows[0]["fund_type"] == "ETF"


@pytest.mark.asyncio
async def test_sse_fund_list_raises_when_incomplete():
    """上游自述 total=5 但只取到 3 行 → 必须抛错，不得静默返回截断列表。"""
    client = _FakeClient({1: [_sse_item("510000"), _sse_item("510001"), _sse_item("510002")]}, total=5)
    with _patch_sse_client(client), patch.object(sse_mod, "polite_delay", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="incomplete"):
            await SseFundListSource()._fetch_list(
                "00", SseFundListSource.ETF_SUB_CLASS, page_size=3
            )


@pytest.mark.asyncio
async def test_sse_fund_list_single_page_when_total_absent():
    """上游未给 total 时退回"短页即末页"的单页语义（不得无限翻页）。"""
    rows_all = [_sse_item(f"51000{i}") for i in range(4)]
    client = _FakeClient({1: rows_all}, total=None)
    with _patch_sse_client(client), patch.object(sse_mod, "polite_delay", new_callable=AsyncMock):
        rows = await SseFundListSource()._fetch_list(
            "00", SseFundListSource.ETF_SUB_CLASS, page_size=10
        )

    assert len(rows) == 4
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_sse_fund_list_probe_stays_single_page():
    """健康探针只取一页（max_pages=1），不能因为真翻页变成全量拉取。"""
    client = _FakeClient({1: [_sse_item(f"51000{i}") for i in range(10)]}, total=1600)
    with _patch_sse_client(client), patch.object(sse_mod, "polite_delay", new_callable=AsyncMock):
        rows = await SseFundListSource()._fetch_list(
            "00", SseFundListSource.ETF_SUB_CLASS, page_size=10, max_pages=1
        )

    assert len(rows) == 10
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_sse_fetch_funds_raises_on_partial_class_failure():
    """ETF 拿到、LOF 失败 → 必须抛错，不能把半份列表当全量交给缓存层。"""
    source = SseFundListSource()

    async def fake_list(fund_type, sub_class, page_size, max_pages=None):
        if fund_type == "10":
            raise RuntimeError("LOF 接口 500")
        return [{"fund_code": "510300", "fund_name": "300ETF", "fund_type": "ETF",
                 "exchange": "SH", "list_date": "2012-05-28"}]

    with patch.object(source, "_fetch_list", side_effect=fake_list):
        with pytest.raises(RuntimeError, match="LOF"):
            await source.fetch_funds()


@pytest.mark.asyncio
async def test_szse_fund_list_raises_on_http_error():
    """深交所 1105 非 200 必须抛错，不能返回空列表（空列表会被当成"上游无数据"）。"""
    client = _FakeClient({}, status_code=500)
    with patch.object(szse_mod.httpx, "AsyncClient", return_value=client):
        with pytest.raises(RuntimeError, match="1105"):
            await SzseFundListSource().fetch_funds()


def _seed_cache(monkeypatch, tmp_path, exchange: str, mapping: dict) -> None:
    monkeypatch.setattr(listing_mod, "CACHE_DIR", tmp_path)
    listing_mod._save_cache(exchange, mapping)


@pytest.mark.asyncio
async def test_listing_cache_not_shrunk_by_truncated_snapshot(tmp_path, monkeypatch, caplog):
    """截断快照（1500 行缓存 vs 10 行上游）不得把白名单写短。"""
    old = {f"5100{i:02d}": "2015-01-01" for i in range(1500)}
    _seed_cache(monkeypatch, tmp_path, "SH", old)
    truncated = {f"5100{i:02d}": "2015-01-01" for i in range(10)}
    truncated["511999"] = "2026-09-01"   # 新上市基金：必须立即生效

    provider = ListingDateProvider()
    source = listing_mod._LISTING_DATE_SOURCES["SH"]
    with patch.object(source, "fetch_listing_dates", new_callable=AsyncMock, return_value=truncated):
        with caplog.at_level("WARNING"):
            data = await provider._fetch_and_cache("SH", force=True)

    assert "511999" in data, "新上市基金必须写进白名单"
    assert len(data) >= 1500, "旧代码不得被截断快照清掉"
    assert len(listing_mod._load_cache("SH")) >= 1500, "文件缓存同样不得被写短"
    assert any("shrank" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_listing_cache_rejects_empty_snapshot(tmp_path, monkeypatch):
    """上游返回空快照时保留旧缓存（空白名单会把所有场内基金过滤掉）。"""
    old = {f"5100{i:02d}": "2015-01-01" for i in range(20)}
    _seed_cache(monkeypatch, tmp_path, "SH", old)

    provider = ListingDateProvider()
    source = listing_mod._LISTING_DATE_SOURCES["SH"]
    with patch.object(source, "fetch_listing_dates", new_callable=AsyncMock, return_value={}):
        data = await provider._fetch_and_cache("SH", force=True)

    assert data == old
    assert listing_mod._load_cache("SH") == old


@pytest.mark.asyncio
async def test_listing_cache_raises_when_empty_and_no_cache(tmp_path, monkeypatch):
    """没有任何旧缓存、上游又返回空 → 必须抛错，不能静默吐出空白名单。"""
    monkeypatch.setattr(listing_mod, "CACHE_DIR", tmp_path)
    provider = ListingDateProvider()
    source = listing_mod._LISTING_DATE_SOURCES["SZ"]
    with patch.object(source, "fetch_listing_dates", new_callable=AsyncMock, return_value={}):
        with pytest.raises(RuntimeError):
            await provider._fetch_and_cache("SZ", force=True)


@pytest.mark.asyncio
async def test_listing_cache_serves_stale_snapshot_on_upstream_error(tmp_path, monkeypatch):
    """上游报错时降级服务旧快照（有旧数据就不该让调用方 500）。"""
    old = {f"1590{i:02d}": "2015-01-01" for i in range(30)}
    _seed_cache(monkeypatch, tmp_path, "SZ", old)

    provider = ListingDateProvider()
    source = listing_mod._LISTING_DATE_SOURCES["SZ"]
    with patch.object(source, "fetch_listing_dates", side_effect=RuntimeError("upstream down")):
        data = await provider._fetch_and_cache("SZ", force=True)

    assert data == old
    assert listing_mod._load_cache("SZ") == old


@pytest.mark.asyncio
async def test_listing_cache_raises_when_upstream_fails_and_no_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(listing_mod, "CACHE_DIR", tmp_path)
    provider = ListingDateProvider()
    source = listing_mod._LISTING_DATE_SOURCES["SH"]
    with patch.object(source, "fetch_listing_dates", side_effect=RuntimeError("upstream down")):
        with pytest.raises(RuntimeError):
            await provider._fetch_and_cache("SH", force=True)


@pytest.mark.asyncio
async def test_listing_cache_grows_normally(tmp_path, monkeypatch):
    """正常增长（快照更大）时照常覆盖，且内存/文件一致。"""
    _seed_cache(monkeypatch, tmp_path, "SH", {"510300": "2012-05-28"})
    grown = {"510300": "2012-05-28", "588000": "2020-11-16"}

    provider = ListingDateProvider()
    source = listing_mod._LISTING_DATE_SOURCES["SH"]
    with patch.object(source, "fetch_listing_dates", new_callable=AsyncMock, return_value=grown):
        data = await provider._fetch_and_cache("SH", force=True)

    assert data == grown
    assert listing_mod._load_cache("SH") == grown
    assert provider._mem_cache["SH"] == grown
