# -*- coding: utf-8 -*-
"""
Browser Proxy Gateway：轻量反向代理 + 容器唤醒 / 空闲休眠。

职责
----
1. 接收来自 market-data 或其他调用方的请求，转发给 browser-proxy 主服务。
2. Docker 模式下按需唤醒 / 空闲关停 browser-proxy 容器（5 分钟无请求自动停机）。
3. 透传 OpenTelemetry traceparent，保证跨服务链路追踪。
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
import time

from fastapi import FastAPI, Request, Response
import httpx
import uvicorn

try:
    import docker
except ImportError:
    docker = None

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
RUNNING_IN_DOCKER = os.path.exists("/.dockerenv") or os.getenv("RUNNING_IN_DOCKER", "false").lower() == "true"
TARGET_URL = os.getenv(
    "BROWSER_PROXY_TARGET_URL",
    "http://browser-proxy:8098" if RUNNING_IN_DOCKER else "http://localhost:8098",
)
CONTAINER_NAME = os.getenv("BROWSER_PROXY_CONTAINER", "browser-proxy")
IDLE_LIMIT = int(os.getenv("BROWSER_PROXY_IDLE_LIMIT", "300"))  # 秒

# ---------------------------------------------------------------------------
# Docker 客户端（仅容器模式）
# ---------------------------------------------------------------------------
docker_client = None
if RUNNING_IN_DOCKER and docker is not None:
    try:
        docker_client = docker.from_env()
    except Exception as e:
        logger.warning("Failed to connect to docker daemon: %s", e)

# ---------------------------------------------------------------------------
# 空闲检测后台线程
# ---------------------------------------------------------------------------
_last_access_time: float = 0.0


def _idle_checker() -> None:
    """定期检查空闲时间，超时则停止容器。"""
    while True:
        time.sleep(30)
        if _last_access_time <= 0 or not docker_client:
            continue
        if (time.time() - _last_access_time) <= IDLE_LIMIT:
            continue
        try:
            container = docker_client.containers.get(CONTAINER_NAME)
            if container.status == "running":
                logger.info("Service idle for %ds, stopping %s container...", IDLE_LIMIT, CONTAINER_NAME)
                container.stop()
        except Exception as e:
            logger.error("idle_checker error: %s", e)


if docker_client:
    threading.Thread(target=_idle_checker, daemon=True).start()

# ---------------------------------------------------------------------------
# FastAPI 应用
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Browser Proxy Gateway",
    description="轻量反向代理，按需唤醒 browser-proxy 容器",
    version="2.0.0",
)

# ---------------------------------------------------------------------------
# 分布式追踪（可选）
# ---------------------------------------------------------------------------
_OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
if _OTEL_ENDPOINT:
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

        _tracer_provider = TracerProvider(
            resource=Resource.create({"service.name": "browser-proxy-gateway"}),
            sampler=ParentBased(TraceIdRatioBased(float(os.getenv("OTEL_TRACES_SAMPLER_ARG", "1")))),
        )
        _tracer_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{_OTEL_ENDPOINT.rstrip('/')}/v1/traces"))
        )
        trace.set_tracer_provider(_tracer_provider)
        HTTPXClientInstrumentor().instrument()
        FastAPIInstrumentor.instrument_app(app)
        logger.info("OTel tracing enabled -> %s", _OTEL_ENDPOINT)
    except ImportError:
        logger.warning("OpenTelemetry packages not installed, tracing disabled.")


# ---------------------------------------------------------------------------
# 等待主服务就绪
# ---------------------------------------------------------------------------

async def _wait_for_ready(timeout: float = 25.0) -> bool:
    """轮询 /health 直到主服务就绪或超时。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(f"{TARGET_URL}/health", timeout=2)
                if r.status_code == 200:
                    return True
        except Exception:
            pass
        await asyncio.sleep(1)
    return False


# ---------------------------------------------------------------------------
# 透明代理路由
# ---------------------------------------------------------------------------

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(request: Request, path: str):
    """透明代理：根据请求唤醒容器并转发到 browser-proxy 主服务。"""
    global _last_access_time
    _last_access_time = time.time()

    try:
        # Docker 模式：按需唤醒容器
        if docker_client:
            try:
                container = docker_client.containers.get(CONTAINER_NAME)
                if container.status != "running":
                    logger.info("Waking up %s container...", CONTAINER_NAME)
                    container.start()
            except docker.errors.NotFound:
                logger.warning("Container '%s' not found. Is Docker Compose running?", CONTAINER_NAME)

        # 等待主服务就绪
        if not await _wait_for_ready():
            logger.error("browser-proxy service not ready after waiting.")
            return Response(content="Gateway Error: backend service not ready", status_code=502)

        # 转发请求
        async with httpx.AsyncClient() as client:
            resp = await client.request(
                method=request.method,
                url=f"{TARGET_URL}/{path}",
                params=request.query_params,
                content=await request.body(),
                headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
                timeout=60,
            )
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers=dict(resp.headers),
            )

    except Exception as e:
        logger.error("Proxy error: %s", e)
        return Response(content=f"Gateway Error: {e}", status_code=500)


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8081"))
    uvicorn.run(app, host="0.0.0.0", port=port)