# -*- coding: utf-8 -*-
import asyncio
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

    # 将单页大小打桩为 1，第 1 页有 1 条，第 2 页失败 (None)
    with patch("providers.funds.eastmoney._LSJZ_PAGE_SIZE", 1), patch.object(
        source,
        "_fetch_lsjz_page",
        side_effect=[page1_data, None],
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
