from fastapi import FastAPI, HTTPException
from playwright.async_api import async_playwright
import asyncio
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Xueqiu Auth Service")


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

        try:
            # 第一步：先访问首页，建立基础会话
            logger.info("Step 1: Accessing xueqiu homepage...")
            await page.goto("https://xueqiu.com/", wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            # 第二步：访问具体的股票页面，触发鉴权 Token 下发
            logger.info("Step 2: Accessing stock page...")
            await page.goto("https://xueqiu.com/S/SH000001", wait_until="networkidle", timeout=30000)
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
