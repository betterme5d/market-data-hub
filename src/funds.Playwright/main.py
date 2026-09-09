from fastapi import FastAPI, HTTPException
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 全局共享的浏览器状态
browser_context = {
    "playwright": None,
    "browser": None,
    "context": None,
    "page": None
}

page_lock = asyncio.Lock()

import os

# 读取环境变量，默认容器中是 Headless
PLAYWRIGHT_HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "True").lower() == "true"
PLAYWRIGHT_DEVTOOLS = os.getenv("PLAYWRIGHT_DEVTOOLS", "False").lower() == "true"

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 生命周期管理器：在启动时创建持久化浏览器与页面，并在退出时自动关闭
    """
    logger.info("Starting up Playwright persistent browser...")
    try:
        p = await async_playwright().start()
        
        # 如果启用了调试工具，则强制将 headless 设置为 False
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
            
        logger.info(f"Launching browser: headless={headless_val}, devtools={PLAYWRIGHT_DEVTOOLS}")
        browser = await p.chromium.launch(
            headless=headless_val,
            args=launch_args,
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
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
            logger.warning(f"Initial navigation to xueqiu homepage failed: {e}. Will retry on demand.")
            
        browser_context["playwright"] = p
        browser_context["browser"] = browser
        browser_context["context"] = context
        browser_context["page"] = page
        logger.info("Persistent browser successfully initialized.")
    except Exception as ex:
        logger.error(f"Failed to initialize persistent browser during startup: {ex}")
        
    yield
    
    logger.info("Shutting down Playwright persistent browser...")
    try:
        if browser_context["browser"]:
            await browser_context["browser"].close()
        if browser_context["playwright"]:
            await browser_context["playwright"].stop()
        logger.info("Persistent browser closed successfully.")
    except Exception as ex:
        logger.error(f"Error closing persistent browser: {ex}")

app = FastAPI(title="Xueqiu Auth Service", lifespan=lifespan)

# 分布式追踪（仅测试环境：dev compose 注入 OTEL_EXPORTER_OTLP_ENDPOINT；生产不注入则跳过）
_OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
if _OTEL_ENDPOINT:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

    _tracer_provider = TracerProvider(
        resource=Resource.create({"service.name": "funds-playwright"}),
        sampler=ParentBased(TraceIdRatioBased(float(os.getenv("OTEL_TRACES_SAMPLER_ARG", "1")))),
    )
    _tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{_OTEL_ENDPOINT.rstrip('/')}/v1/traces"))
    )
    trace.set_tracer_provider(_tracer_provider)
    HTTPXClientInstrumentor().instrument()
    FastAPIInstrumentor.instrument_app(app)
    logger.info("OTel tracing enabled -> %s", _OTEL_ENDPOINT)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/xueqiu/auth")
async def get_xueqiu_auth(ua: str = None):
    """
    增强版：获取雪球 Auth 信息，包含调试截图和多步访问逻辑
    """
    default_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    target_ua = ua if ua else default_ua

    # 优先检查持久化 context 是否已有有效 cookie
    ctx = browser_context.get("context")
    if ctx:
        try:
            cookies = await ctx.cookies()
            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
            if "xq_a_token" in cookie_str:
                logger.info("Reusing cookies from persistent browser context.")
                return {
                    "success": True,
                    "cookie": cookie_str,
                    "user_agent": target_ua,
                    "cookies_raw": cookies,
                    "debug_info": "reused persistent context"
                }
        except Exception as e:
            logger.warning(f"Checking persistent context cookies failed: {e}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-zygote",
            ],
        )

        # 模拟更真实的浏览器环境
        context = await browser.new_context(
            user_agent=target_ua,
            viewport={"width": 1920, "height": 1080},
            ignore_https_errors=True,
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
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
            # 第一步：先访问首页，建立基础会话
            logger.info("Step 1: Accessing xueqiu homepage...")
            await page.goto("https://xueqiu.com/", wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(2)

            # 第二步：访问具体的股票页面，触发鉴权 Token 下发
            logger.info("Step 2: Accessing stock page...")
            await page.goto("https://xueqiu.com/S/SH000001", wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(3)

            # 第三步：保存调试截图，看是否被验证码拦截
            screenshot_path = "debug.png"
            await page.screenshot(path=screenshot_path)
            logger.info(f"Screenshot saved to {screenshot_path}")

            # 获取所有 cookies
            cookies = await context.cookies()
            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
            ua_result = await page.evaluate("navigator.userAgent")

            # 检查关键 token 是否存在
            has_token = "xq_a_token" in cookie_str
            logger.info(f"Successfully retrieved cookies. xq_a_token found: {has_token}")

            return {
                "success": has_token,
                "cookie": cookie_str,
                "user_agent": ua_result,
                "cookies_raw": cookies,
                "debug_info": "Check debug.png in project folder"
            }

        except Exception as e:
            logger.error(f"Error during playwright execution: {str(e)}")
            # 出错也尝试截个图
            try: await page.screenshot(path="error_debug.png")
            except: pass
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            await browser.close()
            import gc
            gc.collect()



@app.get("/xueqiu/kline")
async def get_xueqiu_kline(
    symbol: str,
    begin: int,
    period: str = "day",
    type: str = "normal",
    count: int = -31,
    indicator: str = "kline"
):
    """
    持久化页面复用版本的雪球K线接口
    """
    page = browser_context["page"]
    if not page:
        logger.warning("Global page instance is missing. Trying to re-initialize browser...")
        raise HTTPException(status_code=500, detail="Browser is not initialized.")

    async def do_fetch():
        fake_path = f"/S/{symbol}"
        api_url = f"https://xueqiu.com/service/v5/stock/chart/kline?symbol={symbol}&begin={begin}&period={period}&type={type}&count={count}&indicator={indicator}"
        logger.info(f"Fetching kline for {symbol} via page.evaluate. Target path: {fake_path}")
        
        return await page.evaluate(
            """
            async (args) => {
                history.replaceState(null, '', args.path);
                const resp = await fetch(args.url);
                return await resp.text();
            }
            """,
            {"path": fake_path, "url": api_url}
        )

    try:
        async with page_lock:
            # 防御机制：检测页面是否偏离雪球域名，若偏离则重新初始化首页
            current_url = page.url
            if not current_url or not current_url.startswith("https://xueqiu.com"):
                logger.info(f"Page URL '{current_url}' is not on xueqiu.com. Performing defensive page.goto...")
                await page.goto("https://xueqiu.com/", wait_until="networkidle", timeout=20000)
                await asyncio.sleep(1)

            json_text = await do_fetch()
            
            import json
            data = json.loads(json_text)
            
            # 自愈逻辑：检测 400016 错误 (Cookie 失效)
            if data.get("error_code") == "400016" or "遇到错误，请刷新页面" in data.get("error_description", ""):
                logger.warning("Retrieved 400016 token error. Attempting to refresh token by accessing stock page...")
                # 重新跳转个股页面以获取/更新 token
                await page.goto("https://xueqiu.com/S/SH000001", wait_until="networkidle", timeout=20000)
                await asyncio.sleep(3)
                # 再次尝试抓取
                logger.info("Retrying fetch after token refresh...")
                json_text = await do_fetch()
                data = json.loads(json_text)

            return data
    except Exception as e:
        logger.error(f"Playwright kline scraper failed for {symbol}: {str(e)}")
        # 异常后尝试重置首页以自我修复
        try:
            await page.goto("https://xueqiu.com/", wait_until="networkidle", timeout=10000)
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8098)
