# -*- coding: utf-8 -*-
"""
薄代理端点契约测试（respx 拦截上游 HTTP，锁定路径/参数/响应透传行为，
并验证转发结果进入被动健康指标）。
"""
import json

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


class TestHolidaysPassthrough:
    @respx.mock
    async def test_workdays_passthrough(self, client, fixture_loader):
        fixture = fixture_loader("holidays_workdays_2026.json")
        route = respx.get("https://api.jiejiariapi.com/v1/workdays/2026").mock(
            return_value=Response(200, json=fixture)
        )

        resp = await client.get("/v1/workdays/2026")

        assert resp.status_code == 200
        assert resp.json() == fixture
        assert route.called
        # 业务流量计入被动指标
        snap = health_svc.get_source_health("holidays")
        assert snap["status"] == health_svc.STATUS_HEALTHY

    @respx.mock
    async def test_workdays_upstream_500_returns_502_and_marks_unhealthy(self, client):
        respx.get("https://api.jiejiariapi.com/v1/workdays/2026").mock(
            return_value=Response(500, text="boom")
        )

        last_resp = None
        # 连续失败达到样本门槛后源状态应为 down（少样本期只降级为 degraded）
        for _ in range(3):
            last_resp = await client.get("/v1/workdays/2026")

        assert last_resp.status_code == 502
        snap = health_svc.get_source_health("holidays")
        assert snap["status"] == health_svc.STATUS_DOWN
        assert snap["last_error"]


class TestCfetsPassthrough:
    @respx.mock
    async def test_ccpr_history_forwards_query_verbatim(self, client, fixture_loader):
        fixture = fixture_loader("cfets_ccprhis_usdcny.json")
        route = respx.get(
            "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-ccpr/CcprHisNew",
            params={
                "startDate": "2026-08-25",
                "endDate": "2026-09-01",
                "currency": "USD/CNY",
                "pageNum": "1",
                "pageSize": "5",
            },
        ).mock(return_value=Response(200, json=fixture))

        resp = await client.get(
            "/ags/ms/cm-u-bk-ccpr/CcprHisNew"
            "?startDate=2026-08-25&endDate=2026-09-01&currency=USD/CNY&pageNum=1&pageSize=5"
        )

        assert resp.status_code == 200
        assert resp.json() == fixture
        assert route.called
        assert health_svc.get_source_health("cfets")["status"] == health_svc.STATUS_HEALTHY

    @respx.mock
    async def test_ccpr_upstream_failure_marks_source(self, client):
        respx.get(
            url__startswith="https://www.chinamoney.com.cn/ags/ms/cm-u-bk-ccpr/CcprHisNew"
        ).mock(return_value=Response(503, text="down"))

        last_resp = None
        for _ in range(3):
            last_resp = await client.get(
                "/ags/ms/cm-u-bk-ccpr/CcprHisNew?startDate=2026-08-25&endDate=2026-09-01&currency=USD/CNY&pageNum=1&pageSize=5"
            )

        assert last_resp.status_code == 503
        snap = health_svc.get_source_health("cfets")
        assert snap["status"] == health_svc.STATUS_DOWN
