# -*- coding: utf-8 -*-
import asyncio
import pytest
from core.middleware import CancelOnDisconnectMiddleware


@pytest.mark.asyncio
async def test_normal_request_completes_successfully():
    """正常请求：无断开时，应用正常执行并返回响应"""
    async def app(scope, receive, send):
        msg = await receive()
        assert msg["type"] == "http.request"
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"hello"})

    middleware = CancelOnDisconnectMiddleware(app)

    sent_messages = []
    received_first = False

    async def mock_receive():
        nonlocal received_first
        if not received_first:
            received_first = True
            return {"type": "http.request", "body": b"", "more_body": False}
        # 模拟 ASGI 服务端行为：无更多请求体时挂起等待连接事件
        await asyncio.Event().wait()

    async def mock_send(message):
        sent_messages.append(message)

    scope = {"type": "http", "method": "GET", "path": "/test"}
    await middleware(scope, mock_receive, mock_send)

    assert len(sent_messages) == 2
    assert sent_messages[0]["status"] == 200
    assert sent_messages[1]["body"] == b"hello"


@pytest.mark.asyncio
async def test_cancel_on_disconnect_stops_long_loop():
    """断开测试：当客户端发送 http.disconnect 时，长耗时业务循环立即被取消中断"""
    loop_progress = []
    task_cancelled = asyncio.Event()

    async def slow_crawler_app(scope, receive, send):
        try:
            for i in range(10):
                loop_progress.append(i)
                await asyncio.sleep(0.1)
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"done"})
        except asyncio.CancelledError:
            task_cancelled.set()
            raise

    middleware = CancelOnDisconnectMiddleware(slow_crawler_app)

    # 模拟 receive：在 0.15s 后发送 http.disconnect
    async def mock_receive():
        await asyncio.sleep(0.15)
        return {"type": "http.disconnect"}

    async def mock_send(message):
        pass

    scope = {"type": "http", "method": "GET", "path": "/slow-crawl"}
    await middleware(scope, mock_receive, mock_send)

    # 验证任务被 cancel 且循环在第 2 次左右被截断，绝不会跑完 10 次
    assert task_cancelled.is_set()
    assert len(loop_progress) < 10
    assert len(loop_progress) in [1, 2, 3]


@pytest.mark.asyncio
async def test_cancel_on_disconnect_releases_lock():
    """锁安全测试：业务持有的 asyncio.Lock 在被 cancel 退出后立即释放，杜绝死锁"""
    lock = asyncio.Lock()
    cancelled_event = asyncio.Event()

    async def locked_app(scope, receive, send):
        async with lock:
            assert lock.locked()
            try:
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                cancelled_event.set()
                raise

    middleware = CancelOnDisconnectMiddleware(locked_app)

    async def mock_receive():
        await asyncio.sleep(0.05)
        return {"type": "http.disconnect"}

    async def mock_send(message):
        pass

    scope = {"type": "http", "method": "GET", "path": "/locked"}
    await middleware(scope, mock_receive, mock_send)

    assert cancelled_event.is_set()
    # 验证锁已完全释放，后续任务可以立即获取
    assert not lock.locked()
    async with lock:
        assert lock.locked()


@pytest.mark.asyncio
async def test_fastapi_app_with_middleware_get_and_post():
    """验证真实 FastAPI 应用挂载该中间件后，GET 与 POST 请求体解析完全正常"""
    from fastapi import FastAPI
    from pydantic import BaseModel
    import httpx

    sub_app = FastAPI()
    sub_app.add_middleware(CancelOnDisconnectMiddleware)

    class EchoRequest(BaseModel):
        name: str
        count: int

    @sub_app.get("/ping")
    async def ping():
        return {"status": "pong"}

    @sub_app.post("/echo")
    async def echo(req: EchoRequest):
        return {"name": req.name, "doubled": req.count * 2}

    transport = httpx.ASGITransport(app=sub_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # 验证 GET
        r1 = await client.get("/ping")
        assert r1.status_code == 200
        assert r1.json() == {"status": "pong"}

        # 验证 POST 请求体正常被 wrapped_receive 读取解析
        r2 = await client.post("/echo", json={"name": "510050", "count": 10})
        assert r2.status_code == 200
        assert r2.json() == {"name": "510050", "doubled": 20}


@pytest.mark.asyncio
async def test_timeseries_cache_interrupted_by_disconnect(tmp_path):
    """验证时序缓存拉取过程中客户端断开：已落盘 chunk 安全保留，锁即时释放，支持后续断点续查"""
    try:
        import pyarrow
    except ImportError:
        pytest.skip("pyarrow not installed in local environment")

    from core.timeseries_cache.storage import ParquetStorageEngine
    from core.timeseries_cache.manager import TimeSeriesCacheManager

    storage = ParquetStorageEngine(base_dir=str(tmp_path))
    cache_mgr = TimeSeriesCacheManager(storage=storage)

    chunk1_written = False

    async def fetch_app(scope, receive, send):
        async def fetch_fn(s, e, on_chunk=None):
            nonlocal chunk1_written
            # 第 1 块数据
            chunk1 = [{"date": "2024-01-01", "val": 1.0}, {"date": "2024-01-02", "val": 1.1}]
            if on_chunk:
                await on_chunk(chunk1, "2024-01-01", "2024-01-02")
                chunk1_written = True
            # 模拟第 2 块长耗时采集，中途客户端断开
            await asyncio.sleep(2.0)
            return chunk1

        await cache_mgr.get_or_fetch(
            namespace="test_kline",
            key="TEST01",
            start_date="2024-01-01",
            end_date="2024-01-10",
            fetch_fn=fetch_fn,
            date_column="date"
        )
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"done"})

    middleware = CancelOnDisconnectMiddleware(fetch_app)

    # 客户端在 0.1s 时断开（此时 chunk1 已写入，但第 2 块在 sleep）
    async def mock_receive():
        await asyncio.sleep(0.1)
        return {"type": "http.disconnect"}

    async def mock_send(msg):
        pass

    scope = {"type": "http", "method": "GET", "path": "/history/TEST01"}
    await middleware(scope, mock_receive, mock_send)

    # 1. 验证 chunk 1 已经成功即时落盘
    assert chunk1_written is True
    records = storage.read_records("test_kline", "TEST01", start_date="2024-01-01", end_date="2024-01-02")
    assert len(records) == 2
    assert records[0]["date"] == "2024-01-01"

    # 2. 验证元数据已记录已覆盖的闭合区间
    meta = storage.read_metadata("test_kline", "TEST01")
    assert meta is not None
    assert ["2024-01-01", "2024-01-02"] in meta["intervals"]

    # 3. 验证标的协程锁已释放，后续请求可以秒级获取锁无死锁
    lock = await cache_mgr._get_lock(cache_mgr._make_key("test_kline", "TEST01"))
    assert not lock.locked()
