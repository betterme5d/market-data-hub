# -*- coding: utf-8 -*-
"""
核心中间件集合：
包含纯 ASGI 规范的 CancelOnDisconnectMiddleware，当客户端断开连接时，自动触发后台长耗时任务取消。
"""
import asyncio
import logging
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)


class CancelOnDisconnectMiddleware:
    """
    纯 ASGI 客户端断开连接自动取消中间件：
    
    工作原理：
    1. 在 ASGI 接收管道中启动一个轻量 background listener，监听 receive() 消息；
    2. 如果收到 http.disconnect 消息且内部 app_task 仍在运行，立即对 app_task 调用 cancel()；
    3. Python 的密集网络 I/O (httpx) 与休眠 (asyncio.sleep) 会瞬间收到 CancelledError 中断，
       触发业务代码的 finally 块（释放锁、关闭连接等），杜绝上游孤儿爬虫无谓空转；
    4. 所有普通的请求体消息（如 POST 的 http.request）都会透明通过 receive_queue 传递给底层 app，
       不影响请求体的正常解析与流式消费。
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Dict[str, Any], receive: Callable, send: Callable) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        disconnect_event = asyncio.Event()
        receive_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()

        async def receive_listener() -> None:
            try:
                while True:
                    message = await receive()
                    msg_type = message.get("type")
                    if msg_type == "http.disconnect":
                        disconnect_event.set()
                        await receive_queue.put(message)
                        break
                    await receive_queue.put(message)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug("Exception in receive_listener: %s", e)

        async def wrapped_receive() -> Dict[str, Any]:
            return await receive_queue.get()

        listener_task = asyncio.create_task(receive_listener())

        async def run_app() -> None:
            await self.app(scope, wrapped_receive, send)

        app_task = asyncio.create_task(run_app())
        disconnect_waiter = asyncio.create_task(disconnect_event.wait())

        try:
            done, _ = await asyncio.wait(
                [app_task, disconnect_waiter],
                return_when=asyncio.FIRST_COMPLETED
            )

            if disconnect_waiter in done and not app_task.done():
                path = scope.get("path", "")
                method = scope.get("method", "")
                logger.warning(
                    "Client disconnected for %s %s. Cancelling running background task immediately...",
                    method,
                    path
                )
                app_task.cancel()
                try:
                    await app_task
                except asyncio.CancelledError:
                    logger.info("Successfully cancelled task for %s %s.", method, path)
                except Exception as ex:
                    logger.debug("Cancelled task for %s finished with exception: %s", path, ex)

        finally:
            # 清理所有后台监听任务
            for t in (listener_task, disconnect_waiter):
                if not t.done():
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass

        # 若 app 正常执行完（非被中间件主动取消），检查并向外冒泡应用层抛出的原始未捕获异常
        if app_task.done() and not app_task.cancelled():
            exc = app_task.exception()
            if exc:
                raise exc
