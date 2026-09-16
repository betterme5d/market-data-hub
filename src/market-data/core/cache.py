import json
import logging
from typing import Optional

import redis

from core import config

logger = logging.getLogger(__name__)

# 兼容旧引用（配置统一走 core.config）
REDIS_HOST = config.VALKEY_HOST
REDIS_PORT = config.VALKEY_PORT
REDIS_PWD = config.VALKEY_PASSWORD


def _create_client():
    """创建 Valkey 客户端（惰性连接）。

    刻意**不在导入期 ping**：Valkey 若在进程启动瞬间抖动一次，旧实现会把 redis_client 永久置为 None，
    导致行情缓存、熔断、健康指标、雪球 Cookie 共享全部失效直到进程重启。
    redis.Redis 本身是惰性连接（构造不建连），各调用点均已各自 try/except 容错，按调用失败降级即可。
    """
    try:
        return redis.Redis(
            host=config.VALKEY_HOST,
            port=config.VALKEY_PORT,
            password=config.VALKEY_PASSWORD,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=2,
        )
    except Exception as e:
        logger.error(f"Failed to create Valkey client: {e}")
        return None


redis_client = _create_client()

if redis_client is not None:
    logger.info(f"Valkey client created for {REDIS_HOST}:{REDIS_PORT} (lazy connect)")

def get_cached_quote(symbol: str, provider: str = "yfinance") -> Optional[dict]:
    if not redis_client:
        return None
    try:
        key = f"{provider}:quote:{symbol}"
        data = redis_client.get(key)
        if data:
            return json.loads(data)
    except Exception as e:
        logger.warning(f"Get cached quote failed for {symbol}: {e}")
    return None

def set_cached_quote(symbol: str, data: dict, ttl: int, provider: str = "yfinance") -> None:
    if not redis_client:
        return
    try:
        key = f"{provider}:quote:{symbol}"
        redis_client.setex(key, ttl, json.dumps(data))
    except Exception as e:
        logger.warning(f"Set cached quote failed for {symbol}: {e}")

def get_cached_anchor(symbol: str, provider: str = "yfinance") -> Optional[dict]:
    if not redis_client:
        return None
    try:
        key = f"{provider}:anchor:{symbol}"
        data = redis_client.get(key)
        if data:
            return json.loads(data)
    except Exception as e:
        logger.warning(f"Get cached anchor failed for {symbol}: {e}")
    return None

def set_cached_anchor(symbol: str, data: dict, provider: str = "yfinance") -> None:
    if not redis_client:
        return
    try:
        key = f"{provider}:anchor:{symbol}"
        redis_client.set(key, json.dumps(data))
    except Exception as e:
        logger.warning(f"Set cached anchor failed for {symbol}: {e}")


# 雪球鉴权 Cookie 的共享缓存：
# 独立于 C# 的 `xueqiu:cookies`（对方用 NewLife 自有序列化格式，跨语言共用有解析风险），
# 但目的相同——让 Cookie 跨实例/跨进程复用，避免每次缓存未命中都经 Playwright 网关重取。
XUEQIU_AUTH_KEY = "xueqiu:auth:marketdata"


def get_xueqiu_auth() -> Optional[dict]:
    """读取共享的雪球鉴权信息，返回 {"cookie": str, "userAgent": str} 或 None。"""
    if not redis_client:
        return None
    try:
        data = redis_client.get(XUEQIU_AUTH_KEY)
        if data:
            payload = json.loads(data)
            if payload.get("cookie"):
                return payload
    except Exception as e:
        logger.warning(f"Get shared xueqiu auth failed: {e}")
    return None


def set_xueqiu_auth(cookie: str, user_agent: str, ttl: int = 3600) -> None:
    """写入共享的雪球鉴权信息（默认 1 小时；过期或失效后自然重取）。"""
    if not redis_client or not cookie:
        return
    try:
        redis_client.setex(
            XUEQIU_AUTH_KEY, ttl, json.dumps({"cookie": cookie, "userAgent": user_agent})
        )
    except Exception as e:
        logger.warning(f"Set shared xueqiu auth failed: {e}")


def clear_xueqiu_auth() -> None:
    """清除共享的雪球鉴权信息（收到 400/403 判定 Cookie 失效时调用）。"""
    if not redis_client:
        return
    try:
        redis_client.delete(XUEQIU_AUTH_KEY)
    except Exception as e:
        logger.warning(f"Clear shared xueqiu auth failed: {e}")

