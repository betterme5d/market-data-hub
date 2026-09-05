# -*- coding: utf-8 -*-
"""
CMTIDP 净值薄代理契约测试（respx 拦截上游，锁定查询串透传与响应透传）。
"""
import pytest
import respx
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from core import health as health_svc
from routers.passthrough import router as passthrough_router


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(passthrough_router)
    return app


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _recorded_query_string() -> str:
    """录制 fixture 时保存的真实查询串（含 aoData 与时间戳）。"""
    import os
    path = os.path.join(os.path.dirname(__file__), "fixtures", "cmtidp_aodata.txt")
    with open(path, encoding="utf-8") as f:
        return f.read().strip()


class TestCmtidpPassthrough:
    @respx.mock
    async def test_net_values_passthrough(self, client, fixture_loader):
        fixture = fixture_loader("cmtidp_netvalue_000001.json")
        qs = _recorded_query_string()
        route = respx.get(
            url__startswith="http://eid.csrc.gov.cn/fund/disclose/getPublicFundJZInfoMore.do"
        ).mock(return_value=Response(200, json=fixture))

        resp = await client.get(f"/fund/disclose/getPublicFundJZInfoMore.do?{qs}")

        assert resp.status_code == 200
        assert resp.json() == fixture
        assert route.called
        # 查询串必须原样透传（aoData 编码后的内容一个字符都不能变）
        assert str(route.calls[0].request.url.query, "utf-8") == qs
        # Referer/UA 必须带（上游风控会查）
        assert route.calls[0].request.headers["Referer"] == "http://eid.csrc.gov.cn/"
        assert health_svc.get_source_health("cmtidp")["status"] == health_svc.STATUS_HEALTHY

    @respx.mock
    async def test_upstream_failure_marks_source(self, client):
        respx.get(
            url__startswith="http://eid.csrc.gov.cn/fund/disclose/getPublicFundJZInfoMore.do"
        ).mock(return_value=Response(500, text="boom"))

        last_resp = None
        for _ in range(3):
            last_resp = await client.get("/fund/disclose/getPublicFundJZInfoMore.do?aoData=x&_=1")

        assert last_resp.status_code == 502
        assert health_svc.get_source_health("cmtidp")["status"] == health_svc.STATUS_DOWN
