import docker
import time
import asyncio
from fastapi import FastAPI, Request, Response
import httpx
import uvicorn
import threading
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

app = FastAPI(title="Playwright Gateway")
docker_client = docker.from_env()
CONTAINER_NAME = "funds_playwright"
TARGET_URL = "http://funds.playwright:8000"
IDLE_LIMIT = 300  # 5分钟无人访问则关机
last_access_time = 0

def idle_checker():
    """
    后台线程：检查空闲时间并停止重型容器
    """
    global last_access_time
    while True:
        time.sleep(30)
        if last_access_time > 0 and (time.time() - last_access_time) > IDLE_LIMIT:
            try:
                container = docker_client.containers.get(CONTAINER_NAME)
                if container.status == "running":
                    logger.info("Service idle, stopping playwright container...")
                    container.stop()
                    last_access_time = 0
            except Exception as e:
                logger.error(f"Error in idle_checker: {e}")

# 启动空闲检查线程
threading.Thread(target=idle_checker, daemon=True).start()

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy(request: Request, path: str):
    """
    透明代理：根据请求唤醒容器并转发
    """
    global last_access_time
    last_access_time = time.time()
    
    try:
        # 获取或唤醒容器
        container = docker_client.containers.get(CONTAINER_NAME)
        if container.status != "running":
            logger.info("Waking up Playwright service...")
            container.start()
            # 轮询检查服务是否就绪 (健康检查接口)
            for i in range(15):
                await asyncio.sleep(1)
                try:
                    async with httpx.AsyncClient() as client:
                        r = await client.get(f"{TARGET_URL}/health", timeout=1)
                        if r.status_code == 200:
                            logger.info("Playwright service is ready.")
                            break
                except:
                    if i == 14:
                        logger.error("Wait for playwright service timeout.")
                    continue

        # 构造转发请求
        async with httpx.AsyncClient() as client:
            url = f"{TARGET_URL}/{path}"
            params = request.query_params
            content = await request.body()
            headers = dict(request.headers)
            # 移除可能引起冲突的 host
            headers.pop("host", None)
            
            resp = await client.request(
                method=request.method,
                url=url,
                params=params,
                content=content,
                headers=headers,
                timeout=60
            )
            
            # 返回转发后的响应
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers=dict(resp.headers)
            )
            
    except Exception as e:
        logger.error(f"Proxy error: {e}")
        return Response(content=f"Gateway Error: {str(e)}", status_code=500)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
