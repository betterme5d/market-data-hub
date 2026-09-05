# -*- coding: utf-8 -*-
"""
应用装配入口：只做日志配置、FastAPI 创建、路由挂载、健康探针注册与后台探测循环。
业务端点一律在 routers/ 下，禁止往本文件添加路由。
"""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from core import config
from core import health as health_svc
from providers.exchanges.sse import SseQueryProbe, SseWwwProbe, SseYunhqProbe
from providers.exchanges.szse import SzseDiscProbe, SzseDocsProbe, SzseFundProbe, SzseWwwProbe
from providers.funds.cmtidp import CmtidpProvider
from providers.misc.cfets import CfetsProvider
from providers.misc.haoetf_palmmicro import HaoEtfProbe, PalmmicroProbe
from providers.misc.holidays import HolidaysProvider
from routers import (
    akshare,
    calendar,
    funds,
    health,
    passthrough,
    premium,
    proxy,
    quotes,
    sws,
    xueqiu_gw,
)

LOG_LEVEL = logging.DEBUG if config.DEBUG else logging.INFO
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

if config.DEBUG:
    logging.getLogger("providers").setLevel(logging.DEBUG)
    logging.getLogger("core").setLevel(logging.DEBUG)
    logging.getLogger("httpx").setLevel(logging.DEBUG)
    logger.info("Debug mode enabled. Full execution logs are active.")


def _register_probes() -> None:
    """向健康体系注册各数据源的主动探针。"""
    probes = (
        HolidaysProvider(), CfetsProvider(), CmtidpProvider(),
        SzseWwwProbe(), SzseFundProbe(), SzseDocsProbe(), SzseDiscProbe(),
        SseQueryProbe(), SseYunhqProbe(), SseWwwProbe(),
        HaoEtfProbe(), PalmmicroProbe(),
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
)

app.include_router(health.router)
app.include_router(quotes.router)
app.include_router(funds.router)
app.include_router(premium.router)
app.include_router(akshare.router)
app.include_router(sws.router)
app.include_router(calendar.router)
app.include_router(xueqiu_gw.router)
app.include_router(passthrough.router)
app.include_router(proxy.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
