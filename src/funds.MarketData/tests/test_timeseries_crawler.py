# -*- coding: utf-8 -*-
import asyncio
from unittest.mock import AsyncMock, patch
import pytest

from core.timeseries_cache.crawler import PageBatch, PaginatedSliceCrawler


@pytest.mark.asyncio
async def test_crawler_descending_multi_page_with_on_page():
    crawler = PaginatedSliceCrawler(min_delay=0.1, max_delay=0.2, max_retries=3)

    # 模拟两页倒序数据 (最新在前)
    p1 = PageBatch(
        items=[
            {"date": "2024-03-29", "val": 10},
            {"date": "2024-03-20", "val": 9},
        ],
        total_count=3,
    )
    p2 = PageBatch(
        items=[
            {"date": "2024-03-01", "val": 8},
        ],
        total_count=3,
    )

    fetch_mock = AsyncMock(side_effect=[p1, p2])
    callback_calls = []

    async def mock_on_page(items, s, e):
        callback_calls.append((items, s, e))

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        res = await crawler.crawl_slice(
            start_date="2024-03-01",
            end_date="2024-03-31",
            page_size=2,
            fetch_page_fn=fetch_mock,
            date_getter=lambda x: x["date"],
            order="desc",
            on_page=mock_on_page,
        )

        assert len(res) == 3
        # 结果必须按升序排列
        assert [r["date"] for r in res] == ["2024-03-01", "2024-03-20", "2024-03-29"]

        # 验证逐页回调
        assert len(callback_calls) == 2
        # 第 1 页：覆盖到该页最小日期 2024-03-20 至 2024-03-31
        assert callback_calls[0][1] == "2024-03-20"
        assert callback_calls[0][2] == "2024-03-31"

        # 第 2 页（最后一页）：覆盖向下延伸至切片起始日期 2024-03-01
        assert callback_calls[1][1] == "2024-03-01"
        assert callback_calls[1][2] == "2024-03-31"

        # 第二页开始触发抖动休眠
        assert mock_sleep.call_count >= 1


@pytest.mark.asyncio
async def test_crawler_ascending_multi_page_with_on_page():
    crawler = PaginatedSliceCrawler(min_delay=0.1, max_delay=0.2, max_retries=3)

    # 模拟两页正序数据 (最旧在前)
    p1 = PageBatch(
        items=[
            {"date": "2024-01-02", "val": 1},
            {"date": "2024-01-10", "val": 2},
        ],
        total_count=3,
    )
    p2 = PageBatch(
        items=[
            {"date": "2024-01-25", "val": 3},
        ],
        total_count=3,
    )

    fetch_mock = AsyncMock(side_effect=[p1, p2])
    callback_calls = []

    async def mock_on_page(items, s, e):
        callback_calls.append((items, s, e))

    with patch("asyncio.sleep", new_callable=AsyncMock):
        res = await crawler.crawl_slice(
            start_date="2024-01-01",
            end_date="2024-01-31",
            page_size=2,
            fetch_page_fn=fetch_mock,
            date_getter=lambda x: x["date"],
            order="asc",
            on_page=mock_on_page,
        )

        assert len(res) == 3
        assert [r["date"] for r in res] == ["2024-01-02", "2024-01-10", "2024-01-25"]

        assert len(callback_calls) == 2
        # 第 1 页：覆盖起始 2024-01-01 至最大日期 2024-01-10
        assert callback_calls[0][1] == "2024-01-01"
        assert callback_calls[0][2] == "2024-01-10"

        # 第 2 页（最后一页）：覆盖至切片截止日期 2024-01-31
        assert callback_calls[1][1] == "2024-01-01"
        assert callback_calls[1][2] == "2024-01-31"


@pytest.mark.asyncio
async def test_crawler_single_page_retry_succeeds():
    crawler = PaginatedSliceCrawler(max_retries=3, retry_base_delay=0.01)

    p1 = PageBatch(items=[{"date": "2024-01-05", "val": 1}])
    # 第一次抛异常，第二次成功
    fetch_mock = AsyncMock(side_effect=[Exception("503 Service Unavailable"), p1])

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        res = await crawler.crawl_slice(
            start_date="2024-01-01",
            end_date="2024-01-10",
            page_size=10,
            fetch_page_fn=fetch_mock,
            date_getter=lambda x: x["date"],
        )
        assert len(res) == 1
        assert fetch_mock.call_count == 2
        # 验证退避重试休眠被调用
        assert mock_sleep.call_count >= 1


@pytest.mark.asyncio
async def test_crawler_exhaust_retries_and_partial_failure():
    crawler = PaginatedSliceCrawler(max_retries=2, retry_base_delay=0.01)

    p1 = PageBatch(items=[{"date": "2024-03-25", "val": 1}], total_count=10)
    # 第一页成功，第二页重试耗尽失败
    fetch_mock = AsyncMock(side_effect=[p1, Exception("Rate limit"), Exception("Rate limit")])

    callback_calls = []

    async def mock_on_page(items, s, e):
        callback_calls.append((items, s, e))

    with patch("asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(Exception, match="Rate limit"):
            await crawler.crawl_slice(
                start_date="2024-03-01",
                end_date="2024-03-31",
                page_size=1,
                fetch_page_fn=fetch_mock,
                date_getter=lambda x: x["date"],
                on_page=mock_on_page,
            )

        # 验证第 1 页成功触发并保存，未受第 2 页异常影响
        assert len(callback_calls) == 1
        assert callback_calls[0][1] == "2024-03-25"
        assert callback_calls[0][2] == "2024-03-31"


@pytest.mark.asyncio
async def test_crawler_empty_page_does_not_claim_coverage():
    """空页不再记为"已覆盖"：上游静默失败与真无数据不可区分，记覆盖会把错误静默固化。"""
    crawler = PaginatedSliceCrawler()

    fetch_mock = AsyncMock(return_value=PageBatch(items=[]))
    callback_calls = []

    async def mock_on_page(items, s, e):
        callback_calls.append((items, s, e))

    res = await crawler.crawl_slice(
        start_date="2024-10-01",
        end_date="2024-10-07",
        page_size=20,
        fetch_page_fn=fetch_mock,
        date_getter=lambda x: x["date"],
        on_page=mock_on_page,
    )

    assert res == []
    # 空结果不触发 on_page（不写覆盖区间），留给下次重取
    assert callback_calls == []


@pytest.mark.asyncio
async def test_crawler_short_middle_page_does_not_end_pagination():
    """中途短页不能当末页：末页会把覆盖区间拉到请求起点，缺口会被永久标成已覆盖。

    上游给了 TotalCount（=43）时，第 2 页只回了 3 条也必须继续翻到取满。
    """
    from core.timeseries_cache.crawler import PageBatch

    pages = {
        1: [f"2024-01-{d:02d}" for d in range(1, 21)],          # 满页 20
        2: [f"2024-01-{d:02d}" for d in range(21, 24)],          # 短页 3（抖动）
        3: [f"2024-01-{d:02d}" for d in range(24, 43)],          # 剩余 19
    }
    calls = []

    async def fetch_page(page_index, page_size, s, e):
        calls.append(page_index)
        return PageBatch(items=pages.get(page_index, []), total_count=42)

    crawler = PaginatedSliceCrawler(min_delay=0, max_delay=0)
    covered = []

    async def _on_page(recs, cs, ce):
        covered.append((cs, ce, len(recs)))

    items = await crawler.crawl_slice(
        "2024-01-01", "2024-01-31",
        page_size=20,
        fetch_page_fn=fetch_page,
        date_getter=lambda x: x,
        order="desc",
        on_page=_on_page,
    )

    assert calls == [1, 2, 3]
    assert len(items) == 42
    # 最后一页是第 3 页（19 < 20）且已取满 total，覆盖区间按末页规则延到请求起点
    assert covered[-1][0] == "2024-01-01"
