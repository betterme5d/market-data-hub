# -*- coding: utf-8 -*-
"""
应用装配入口：只做日志配置、FastAPI 创建、路由挂载、健康探针注册与后台探测循环。
业务端点一律在 routers/ 下，禁止往本文件添加路由。
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from scalar_fastapi import get_scalar_api_reference

from core import config
from core import health as health_svc
from core.middleware import CancelOnDisconnectMiddleware
from providers.exchanges.sse import SseFundListProbe, SseQueryProbe, SseWwwProbe, SseYunhqProbe
from providers.exchanges.szse import (
    SzseCalendarProbe,
    SzseDiscProbe,
    SzseDocsProbe,
    SzseFundListProbe,
    SzseFundProbe,
    SzseWwwProbe,
)
from providers.funds.cmtidp import CmtidpProbe
from providers.funds.eastmoney import EastmoneyJjjzProbe, EastmoneyProbe
from providers.funds.establish_dates import EastmoneyEstablishDateProbe
from providers.funds.fund_profile import EastmoneyProfileProbe
from providers.exchanges.shares.sse import SseShareProbe
from providers.exchanges.shares.szse import SzseShareProbe
from providers.misc.cfets import CfetsProbe
from routers import (
    akshare,
    calendar,
    funds,
    health,
    premium,
    quotes,
    sws,
    xueqiu_gw,
)

LOG_LEVEL = logging.DEBUG if config.DEBUG else logging.INFO
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# 持续剖析（仅测试环境：dev compose 注入 PYROSCOPE_SERVER_ADDRESS；生产不注入则跳过）
_pyroscope_server = os.getenv("PYROSCOPE_SERVER_ADDRESS")
if _pyroscope_server:
    import pyroscope

    pyroscope.configure(
        application_name="funds-marketdata",
        server_address=_pyroscope_server,
    )
    logger.info("Pyroscope profiling enabled -> %s", _pyroscope_server)

# 分布式追踪（仅测试环境：dev compose 注入 OTEL_EXPORTER_OTLP_ENDPOINT；生产不注入则跳过）
# 入站 FastAPI span 会提取上游（webapi）透传的 traceparent，出站 httpx 自动注入 traceparent 给下游（gateway/上游数据源）
_otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
if _otel_endpoint:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

    # 生产可用 OTEL_TRACES_SAMPLER_ARG 控制采样比例（0.1 = 采 10%），不设则全量
    _tracer_provider = TracerProvider(
        resource=Resource.create({"service.name": "funds-marketdata"}),
        sampler=ParentBased(TraceIdRatioBased(float(os.getenv("OTEL_TRACES_SAMPLER_ARG", "1")))),
    )
    _tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{_otel_endpoint.rstrip('/')}/v1/traces"))
    )
    trace.set_tracer_provider(_tracer_provider)
    HTTPXClientInstrumentor().instrument()
    logger.info("OTel tracing enabled -> %s", _otel_endpoint)

if config.DEBUG:
    logging.getLogger("providers").setLevel(logging.DEBUG)
    logging.getLogger("core").setLevel(logging.DEBUG)
    logging.getLogger("httpx").setLevel(logging.DEBUG)
    logger.info("Debug mode enabled. Full execution logs are active.")


def _register_probes() -> None:
    """向健康体系注册各数据源的主动探针。"""
    probes = (
        CfetsProbe(), CmtidpProbe(), EastmoneyProbe(), EastmoneyJjjzProbe(),
        EastmoneyEstablishDateProbe(), EastmoneyProfileProbe(),
        SzseWwwProbe(), SzseFundProbe(), SzseDocsProbe(), SzseDiscProbe(),
        SseQueryProbe(), SseYunhqProbe(), SseWwwProbe(),
        # 公开业务接口的语义化探针（站点可达 ≠ 接口仍可用）
        SseFundListProbe(), SzseFundListProbe(), SzseCalendarProbe(),
        SzseShareProbe(), SseShareProbe(),
    )
    for provider in probes:
        health_svc.register_probe(provider.name, provider.category, provider.probe)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _register_probes()
    stop_event = asyncio.Event()
    probe_task = asyncio.create_task(health_svc.probe_loop(stop_event))
    try:
        yield
    finally:
        stop_event.set()
        await probe_task


app = FastAPI(
    title="Market Data Proxy Service",
    description="提供集成的行情代理与指数数据接口，支持实时报价、历史K线、申万行业指数、折溢价率以及公募基金数据查询。",
    version="1.1.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)

# 挂载客户端断开自动取消中间件：当客户端中断/取消请求时，秒级中断后台循环采集任务
app.add_middleware(CancelOnDisconnectMiddleware)

if _otel_endpoint:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)


@app.get("/docs", include_in_schema=False)
async def scalar_docs() -> HTMLResponse:
    """以 Scalar UI 呈现 OpenAPI 文档，作为系统唯一的 API 文档。"""
    return get_scalar_api_reference(
        openapi_url=app.openapi_url,
        title="Market Data Proxy Service",
    )

app.include_router(health.router)
app.include_router(quotes.router)
app.include_router(funds.router)
app.include_router(premium.router)
app.include_router(akshare.router)
app.include_router(sws.router)
app.include_router(calendar.router)
app.include_router(xueqiu_gw.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
