# -*- coding: utf-8 -*-
"""
Browser Proxy Service：持久化浏览器实例管理 + Cookie / 数据抓取网关。

端点约定
--------
GET  /health                          健康探针
GET  /api/v1/{site}/cookies           获取指定站点的认证 Cookie
GET  /api/v1/{site}/kline             通过持久化页面复用获取站点 K 线数据
GET  /xueqiu/auth                     雪球鉴权 Cookie（旧版路径，兼容 funds C# 与 market-data）

新增站点只需在 ``_SITE_HANDLERS`` 注册即可，路由无需改动。
"""
from __future__ import annotations

import asyncio
import gc
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 全局浏览器状态
# ---------------------------------------------------------------------------
browser_context: Dict[str, Any] = {
    "playwright": None,
    "browser": None,
    "context": None,
    "page": None,
}

page_lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# 环境变量
# ---------------------------------------------------------------------------
PLAYWRIGHT_HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "True").lower() == "true"
PLAYWRIGHT_DEVTOOLS = os.getenv("PLAYWRIGHT_DEVTOOLS", "False").lower() == "true"

# ---------------------------------------------------------------------------
# Pydantic 响应模型
# ---------------------------------------------------------------------------

class XueqiuAuthResponse(BaseModel):
    """``/xueqiu/auth`` 的响应契约（旧版路径，详见该路由 docstring）。"""
    success: bool = Field(..., description="是否成功获取到目标 Cookie")
    cookie: str = Field(..., description="拼接后的 Cookie 字符串")
    user_agent: str = Field(..., description="实际使用的 User-Agent（旧契约字段名）")
    userAgent: str = Field(..., description="同一 UA 的 camelCase 别名（两个调用方都按这个拼法读）")
    cookies_raw: List[Dict[str, Any]] = Field(default_factory=list, description="原始 Cookie 列表")
    debug_info: str = Field(default="", description="调试信息")


class CookieResponse(BaseModel):
    """Cookie 获取结果。"""
    success: bool = Field(..., description="是否成功获取到目标 Cookie")
    cookie: str = Field(..., description="拼接后的 Cookie 字符串")
    user_agent: str = Field(..., description="实际使用的 User-Agent")
    cookies_raw: List[Dict[str, Any]] = Field(default_factory=list, description="原始 Cookie 列表")
    debug_info: str = Field(default="", description="调试信息")

# ---------------------------------------------------------------------------
# FastAPI 生命周期
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """启动时创建持久化浏览器实例，退出时关闭。"""
    logger.info("Starting up persistent browser...")
    try:
        p = await async_playwright().start()

        headless_val = False if PLAYWRIGHT_DEVTOOLS else PLAYWRIGHT_HEADLESS

        launch_args = [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-zygote",
        ]
        if PLAYWRIGHT_DEVTOOLS:
            launch_args.append("--auto-open-devtools-for-tabs")

        logger.info("Launching browser: headless=%s, devtools=%s", headless_val, PLAYWRIGHT_DEVTOOLS)
        browser = await p.chromium.launch(headless=headless_val, args=launch_args)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            ignore_https_errors=True,
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)

        # 首次访问，打通会话
        try:
            logger.info("Navigating to xueqiu homepage...")
            await page.goto("https://xueqiu.com/", wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(2)
            logger.info("Navigating to stock page to trigger xq_a_token...")
            await page.goto("https://xueqiu.com/S/SH000001", wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(2)
        except Exception as e:
            logger.warning("Initial navigation to xueqiu failed: %s. Will retry on demand.", e)

        browser_context.update(playwright=p, browser=browser, context=context, page=page)
        logger.info("Persistent browser successfully initialized.")
    except Exception as ex:
        logger.error("Failed to initialize persistent browser: %s", ex)

    yield

    logger.info("Shutting down persistent browser...")
    try:
        if browser_context["browser"]:
            await browser_context["browser"].close()
        if browser_context["playwright"]:
            await browser_context["playwright"].stop()
        logger.info("Persistent browser closed successfully.")
    except Exception as ex:
        logger.error("Error closing persistent browser: %s", ex)


# ---------------------------------------------------------------------------
# FastAPI 应用
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Browser Proxy Service",
    description="持久化浏览器实例管理与 Cookie / 数据抓取网关",
    version="2.0.0",
    lifespan=lifespan,
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
            resource=Resource.create({"service.name": "browser-proxy"}),
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

# ===========================================================================
# 站点 Cookie 获取 —— 各站点独立实现，统一注册
# ===========================================================================

_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


async def _fetch_cookies_xueqiu(user_agent: str | None = None) -> CookieResponse:
    """通过浏览器访问雪球获取认证 Cookie。"""
    target_ua = user_agent or _DEFAULT_UA

    # 优先复用持久化 context 中已有的 cookie
    ctx = browser_context.get("context")
    if ctx:
        try:
            cookies = await ctx.cookies()
            cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
            if "xq_a_token" in cookie_str:
                logger.info("Reusing cookies from persistent browser context.")
                return CookieResponse(
                    success=True,
                    cookie=cookie_str,
                    user_agent=target_ua,
                    cookies_raw=cookies,
                    debug_info="reused persistent context",
                )
        except Exception as e:
            logger.warning("Checking persistent context cookies failed: %s", e)

    # 降级：临时启动浏览器获取
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
                  "--disable-gpu", "--no-zygote"],
        )
        context = await browser.new_context(
            user_agent=target_ua,
            viewport={"width": 1920, "height": 1080},
            ignore_https_errors=True,
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)

        try:
            logger.info("Step 1: Accessing xueqiu homepage...")
            await page.goto("https://xueqiu.com/", wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(2)

            logger.info("Step 2: Accessing stock page to trigger token...")
            await page.goto("https://xueqiu.com/S/SH000001", wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(3)

            screenshot_path = "debug.png"
            await page.screenshot(path=screenshot_path)
            logger.info("Screenshot saved to %s", screenshot_path)

            cookies = await context.cookies()
            cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
            ua_result = await page.evaluate("navigator.userAgent")
            has_token = "xq_a_token" in cookie_str
            logger.info("Successfully retrieved cookies. xq_a_token found: %s", has_token)

            return CookieResponse(
                success=has_token,
                cookie=cookie_str,
                user_agent=ua_result,
                cookies_raw=cookies,
                debug_info="Check debug.png in project folder",
            )
        except Exception as e:
            logger.error("Error during cookie fetch: %s", e)
            try:
                await page.screenshot(path="error_debug.png")
            except Exception:
                pass
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            await browser.close()
            gc.collect()


# 站点处理器注册表：site_name -> cookie_fetch_handler
_COOKIE_HANDLERS: Dict[str, Any] = {
    "xueqiu": _fetch_cookies_xueqiu,
}

# ===========================================================================
# 站点 K 线获取 —— 各站点独立实现，统一注册
# ===========================================================================

async def _fetch_kline_xueqiu(
    symbol: str,
    begin: int,
    period: str,
    adjust: str,
    count: int,
    indicator: str,
) -> dict:
    """通过持久化页面复用获取雪球 K 线。"""
    page = browser_context["page"]
    if not page:
        raise HTTPException(status_code=500, detail="Browser is not initialized.")

    async def do_fetch() -> str:
        fake_path = f"/S/{symbol}"
        api_url = (
            f"https://xueqiu.com/service/v5/stock/chart/kline"
            f"?symbol={symbol}&begin={begin}&period={period}"
            f"&type={adjust}&count={count}&indicator={indicator}"
        )
        logger.info("Fetching kline for %s via page.evaluate. Target path: %s", symbol, fake_path)
        return await page.evaluate(
            """
            async (args) => {
                history.replaceState(null, '', args.path);
                const resp = await fetch(args.url);
                return await resp.text();
            }
            """,
            {"path": fake_path, "url": api_url},
        )

    try:
        async with page_lock:
            # 防御：页面偏离雪球域名时重新初始化
            current_url = page.url
            if not current_url or not current_url.startswith("https://xueqiu.com"):
                logger.info("Page URL '%s' is not on xueqiu.com. Performing defensive goto...", current_url)
                await page.goto("https://xueqiu.com/", wait_until="networkidle", timeout=20000)
                await asyncio.sleep(1)

            json_text = await do_fetch()
            data = json.loads(json_text)

            # 自愈：Cookie 失效时自动刷新
            if data.get("error_code") == "400016" or "遇到错误，请刷新页面" in data.get("error_description", ""):
                logger.warning("400016 token error. Refreshing token...")
                await page.goto("https://xueqiu.com/S/SH000001", wait_until="networkidle", timeout=20000)
                await asyncio.sleep(3)
                logger.info("Retrying fetch after token refresh...")
                json_text = await do_fetch()
                data = json.loads(json_text)

            return data
    except Exception as e:
        logger.error("Playwright kline scraper failed for %s: %s", symbol, e)
        try:
            await page.goto("https://xueqiu.com/", wait_until="networkidle", timeout=10000)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))


# 站点处理器注册表：site_name -> kline_fetch_handler
_KLINE_HANDLERS: Dict[str, Any] = {
    "xueqiu": _fetch_kline_xueqiu,
}

# ===========================================================================
# 路由
# ===========================================================================

@app.get("/health")
async def health():
    """健康探针。"""
    return {"status": "ok"}


@app.get("/api/v1/{site}/cookies", response_model=CookieResponse)
async def get_site_cookies(
    site: str,
    user_agent: Optional[str] = Query(None, description="自定义 User-Agent"),
):
    """获取指定站点的认证 Cookie。

    目前支持的站点：``xueqiu``。新增站点只需在 ``_COOKIE_HANDLERS`` 中注册。
    """
    handler = _COOKIE_HANDLERS.get(site)
    if handler is None:
        available = ", ".join(sorted(_COOKIE_HANDLERS)) or "(无)"
        raise HTTPException(
            status_code=404,
            detail=f"Unknown site: '{site}'. Available: {available}",
        )
    return await handler(user_agent)


@app.get(
    "/xueqiu/auth",
    response_model=XueqiuAuthResponse,
    tags=["兼容接口"],
    summary="雪球鉴权 Cookie（旧版路径）",
)
async def get_xueqiu_auth(
    ua: Optional[str] = Query(None, description="自定义 User-Agent（旧版参数名）"),
):
    """旧版雪球鉴权端点，兼容历史调用方。

    背景：2026-09 拆库时本文件经历 BOM + 单行损坏（robocopy），恢复后的版本只保留了
    ``/api/v1/{site}/cookies``，``/xueqiu/auth`` 被弄丢。而两个调用方仍按旧路径调用：

    - funds 的 C# ``XueqiuTokenService``：404 后它没有可用兜底（``Xueqiu__Cookie`` 为空），
      会直接抛「无法获取有效的雪球 Token」，雪球 Token 刷新失败；
    - market-data 的雪球 Provider：失败后只能退回直连 xueqiu.com 抓 Cookie，
      绕过了浏览器指纹——正是本服务要解决的问题。

    响应同时给出 ``user_agent``（旧契约字段名）与 ``userAgent``（两个调用方读取的拼法）；
    只给前者会让它们静默退回各自的默认 UA，等于白拿一次真实 UA。
    """
    handler = _COOKIE_HANDLERS.get("xueqiu")
    if handler is None:
        raise HTTPException(status_code=404, detail="xueqiu cookie handler not registered")
    resp: CookieResponse = await handler(ua)
    return XueqiuAuthResponse(
        success=resp.success,
        cookie=resp.cookie,
        user_agent=resp.user_agent,
        userAgent=resp.user_agent,
        cookies_raw=resp.cookies_raw,
        debug_info=resp.debug_info,
    )


@app.get("/api/v1/{site}/kline")
async def get_site_kline(
    site: str,
    symbol: str = Query(..., description="标的代码，如 SH000001"),
    begin: int = Query(..., description="起始时间戳（毫秒）"),
    period: str = Query("day", description="K 线周期：day / week / month"),
    adjust: str = Query("normal", description="复权类型：normal / before / after"),
    count: int = Query(-31, description="拉取根数，负数表示向前"),
    indicator: str = Query("kline", description="指标类型"),
):
    """通过浏览器获取指定站点的 K 线数据。

    目前支持的站点：``xueqiu``。新增站点只需在 ``_KLINE_HANDLERS`` 中注册。
    """
    handler = _KLINE_HANDLERS.get(site)
    if handler is None:
        available = ", ".join(sorted(_KLINE_HANDLERS)) or "(无)"
        raise HTTPException(
            status_code=404,
            detail=f"Unknown site: '{site}'. Available: {available}",
        )
    return await handler(
        symbol=symbol,
        begin=begin,
        period=period,
        adjust=adjust,
        count=count,
        indicator=indicator,
    )


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8098)