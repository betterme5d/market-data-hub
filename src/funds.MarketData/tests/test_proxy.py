# -*- coding: utf-8 -*-
"""
通用反向代理契约测试：路径映射、查询串/请求体透传、响应流式回传、健康指标记录。
"""
import pytest
import respx
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from core import health as health_svc
from routers.proxy import router as proxy_router


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(proxy_router)
    return app


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestPathMapping:
    @respx.mock
    async def test_szse_www_json_passthrough(self, client):
        payload = {"data": [{"jyrq": "20260901", "jybz": "1"}]}
        route = respx.get(
            url__startswith="https://www.szse.cn/api/report/exchange/onepersistenthour/monthList"
        ).mock(return_value=Response(200, json=payload))

        resp = await client.get("/proxy/szse-www/api/report/exchange/onepersistenthour/monthList?month=2026-09&random=0.1")

        assert resp.status_code == 200
        assert resp.json() == payload
        assert route.called
        assert str(route.calls[0].request.url.query, "utf-8") == "month=2026-09&random=0.1"
        assert health_svc.get_source_health("szse-www")["status"] == health_svc.STATUS_HEALTHY

    @respx.mock
    async def test_szse_fund_post_body_forwarded(self, client):
        payload = {"data": [], "announceCount": 0}
        route = respx.post(
            url__startswith="http://fund.szse.cn/api/disc/announcement/annList"
        ).mock(return_value=Response(200, json=payload))

        body = {"seDate": ["2026-09-01", "2026-09-02"], "channelCode": ["fundinfoNotice_disc"], "pageSize": 50, "pageNum": 1}
        resp = await client.post("/proxy/szse-fund/api/disc/announcement/annList?random=0.1", json=body)

        assert resp.status_code == 200
        assert route.called
        import json as _json
        assert _json.loads(route.calls[0].request.content) == body
        assert route.calls[0].request.headers["Content-Type"].startswith("application/json")

    @respx.mock
    async def test_sse_yunhq_sends_required_headers(self, client):
        route = respx.get(
            url__startswith="https://yunhq.sse.com.cn:32042/v1/sh1/list/exchange/lof"
        ).mock(return_value=Response(200, text="({\"list\": []})"))

        resp = await client.get("/proxy/sse-yunhq/v1/sh1/list/exchange/lof?callback=&_=123")

        assert resp.status_code == 200
        assert resp.text == '({"list": []})'
        assert route.calls[0].request.headers["Referer"] == "https://www.sse.com.cn/"

    @respx.mock
    async def test_binary_stream_passthrough(self, client):
        """附件/Excel 等二进制必须逐字节透传，且 Content-Type 不丢。"""
        blob = bytes(range(256)) * 100
        respx.get(
            url__startswith="https://disc.static.szse.cn/download/announcement.pdf"
        ).mock(return_value=Response(200, content=blob, headers={"Content-Type": "application/pdf"}))

        resp = await client.get("/proxy/szse-disc/download/announcement.pdf?n=test.pdf")

        assert resp.status_code == 200
        assert resp.content == blob
        assert resp.headers["Content-Type"] == "application/pdf"

    @respx.mock
    async def test_haoetf_home_html_passthrough(self, client):
        html = "<html><body><table><tbody><tr><td>510300</td></tr></tbody></table></body></html>"
        route = respx.get(url__startswith="https://www.haoetf.com/").mock(
            return_value=Response(200, text=html, headers={"Content-Type": "text/html; charset=utf-8"})
        )

        resp = await client.get("/proxy/haoetf/")

        assert resp.status_code == 200
        assert resp.text == html
        assert route.called

    @respx.mock
    async def test_haoetf_lof_html_passthrough(self, client):
        html = "<html><body>lof</body></html>"
        route = respx.get(url__startswith="https://www.haoetf.com/lof").mock(
            return_value=Response(200, text=html)
        )

        resp = await client.get("/proxy/haoetf/lof")

        assert resp.status_code == 200
        assert resp.text == html
        assert route.called

    @respx.mock
    async def test_palmmicro_injects_cookie(self, client):
        html = "<html><table id='estimationtable'><tbody><tr><td>x</td></tr></tbody></table></html>"
        route = respx.get(url__startswith="https://www.palmmicro.com/woody/res/qdiicn.php").mock(
            return_value=Response(200, text=html)
        )

        resp = await client.get("/proxy/palmmicro/woody/res/qdiicn.php")

        assert resp.status_code == 200
        assert resp.text == html
        assert route.called
        # 代理侧注入硬编码 Cookie，调用方不再自行携带
        assert "PHPSESSID" in route.calls[0].request.headers["Cookie"]

    async def test_unknown_source_404(self, client):
        resp = await client.get("/proxy/no-such-source/api/whatever")
        assert resp.status_code == 404

    @respx.mock
    async def test_upstream_failure_marks_source(self, client):
        respx.get(url__startswith="https://query.sse.com.cn/commonQuery.do").mock(
            return_value=Response(500, text="boom")
        )

        for _ in range(3):
            resp = await client.get("/proxy/sse-query/commonQuery.do?sqlId=X")
        assert resp.status_code == 500  # 上游状态码原样回传
        assert health_svc.get_source_health("sse-query")["status"] == health_svc.STATUS_DOWN
