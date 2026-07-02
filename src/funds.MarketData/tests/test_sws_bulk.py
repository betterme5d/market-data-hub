import sys
import os
import pytest
import httpx

# Add parent dir to path to import main
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from main import app

@pytest.mark.asyncio
async def test_bulk_industry_kline():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # 测试一级行业
        response = await client.get("/api/sws/bulk-industry-kline?indextype=一级行业")
        assert response.status_code == 200
        data = response.json()
        assert data["code"] == "200"
        assert len(data["data"]) > 0
        first_item = data["data"][0]
        assert "swindexcode" in first_item
        assert "openindex" in first_item
        assert "closeindex" in first_item
        assert "bargaindate" in first_item

        # 测试二级行业
        response2 = await client.get("/api/sws/bulk-industry-kline?indextype=二级行业")
        assert response2.status_code == 200
        data2 = response2.json()
        assert data2["code"] == "200"
        assert len(data2["data"]) > 0

        # 测试 industry-realtime 一级行业格式转换
        response_rt = await client.get("/api/sws/industry-realtime?indextype=一级行业")
        assert response_rt.status_code == 200
        data_rt = response_rt.json()
        assert data_rt["code"] == "200"
        assert len(data_rt["data"]) > 0
        first_rt_item = data_rt["data"][0]
        assert "swindexcode" in first_rt_item
        assert "openindex" in first_rt_item
        assert "closeindex" in first_rt_item
        assert "bargaindate" in first_rt_item
