# -*- coding: utf-8 -*-
"""业务缓存门面：报价 / 后复权锚点 / 雪球鉴权，底层走 core.state_store。

历史沿革：本模块此前是 Valkey 客户端（含惰性连接、跨进程共享）。拆库后按部署形态
（单容器单进程）改为进程内缓存，见 core/state_store.py 的模块说明。

分层：
- ``quote_cache``    纯内存。10 秒级 TTL 的报价短路数据，落盘没有意义。
- ``breaker_store``  落文件。行情源熔断状态（冷却 300 秒 / 失败窗口 60 秒）——
  重启后重建的代价是「再打一次已知挂掉的上游」，值得保留。
- ``anchor_store``   落文件。yfinance 后复权锚点几乎不变，重启后重建要拉一次全量历史。
- ``auth_store``     落文件。雪球鉴权 Cookie（TTL 1 小时），重启后重建要唤醒一次网关。
"""
import json
import logging
from typing import Optional

from core.state_store import StateStore

logger = logging.getLogger(__name__)

# 报价短路缓存（10 秒级）：纯内存
quote_cache = StateStore("quote_cache")
# 行情源熔断状态：跨重启保留
breaker_store = StateStore("quote_breaker", persist=True)
# yfinance 后复权锚点：跨重启保留
anchor_store = StateStore("yfinance_anchor", persist=True)
# 雪球鉴权 Cookie：跨重启保留
auth_store = StateStore("xueqiu_auth", persist=True)

# 兼容旧引用（原先是 Valkey 的 host/port，现在无外部依赖，保留常量便于排查日志）
CACHE_BACKEND = "memory"

# 雪球鉴权信息在 state_store 中的键（原 Valkey 键名沿用，便于对照历史日志）
XUEQIU_AUTH_KEY = "xueqiu:auth:marketdata"


def _quote_key(symbol: str, provider: str) -> str:
    return f"{provider}:quote:{symbol}"


def _anchor_key(symbol: str, provider: str) -> str:
    return f"{provider}:anchor:{symbol}"


# ---------------------------------------------------------------------------
# 报价（供 yfinance 的当日 K 线修补读取；写入方是 dispatcher 自己的键空间）
# ---------------------------------------------------------------------------
def get_cached_quote(symbol: str, provider: str = "yfinance") -> Optional[dict]:
    raw = quote_cache.get(_quote_key(symbol, provider))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError) as e:
        logger.warning(f"Get cached quote for {symbol} failed: {e}")
        return None


def set_cached_quote(symbol: str, data: dict, ttl: int, provider: str = "yfinance") -> None:
    try:
        quote_cache.setex(_quote_key(symbol, provider), ttl, json.dumps(data))
    except (TypeError, ValueError) as e:
        logger.warning(f"Set cached quote for {symbol} failed: {e}")


# ---------------------------------------------------------------------------
# yfinance 后复权锚点（永不过期，随文件跨重启保留）
# ---------------------------------------------------------------------------
def get_cached_anchor(symbol: str, provider: str = "yfinance") -> Optional[dict]:
    raw = anchor_store.get(_anchor_key(symbol, provider))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError) as e:
        logger.warning(f"Get cached anchor for {symbol} failed: {e}")
        return None


def set_cached_anchor(symbol: str, data: dict, provider: str = "yfinance") -> None:
    try:
        anchor_store.set(_anchor_key(symbol, provider), json.dumps(data))
    except (TypeError, ValueError) as e:
        logger.warning(f"Set cached anchor for {symbol} failed: {e}")


# ---------------------------------------------------------------------------
# 雪球鉴权 Cookie
# ---------------------------------------------------------------------------
def get_xueqiu_auth() -> Optional[dict]:
    """读取雪球鉴权信息，返回 {"cookie": str, "userAgent": str} 或 None。"""
    raw = auth_store.get(XUEQIU_AUTH_KEY)
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as e:
        logger.warning(f"Get cached xueqiu auth failed: {e}")
        return None
    return payload if payload.get("cookie") else None


def set_xueqiu_auth(cookie: str, user_agent: str, ttl: int = 3600) -> None:
    """写入雪球鉴权信息（默认 1 小时；过期或失效后自然重取）。"""
    if not cookie:
        return
    try:
        auth_store.setex(
            XUEQIU_AUTH_KEY, ttl, json.dumps({"cookie": cookie, "userAgent": user_agent})
        )
    except (TypeError, ValueError) as e:
        logger.warning(f"Set cached xueqiu auth failed: {e}")


def clear_xueqiu_auth() -> None:
    """清除雪球鉴权信息（收到 400/403 判定 Cookie 失效时调用）。"""
    auth_store.delete(XUEQIU_AUTH_KEY)


# ---------------------------------------------------------------------------
# 观测
# ---------------------------------------------------------------------------
def describe() -> dict:
    """缓存层快照，供 /health 展示。"""
    return {
        "backend": CACHE_BACKEND,
        "stores": {
            store.name: store.stats()
            for store in (quote_cache, breaker_store, anchor_store, auth_store)
        },
    }