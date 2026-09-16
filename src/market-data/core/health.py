# -*- coding: utf-8 -*-
"""
数据源健康体系：

- 被动指标：所有外部调用经 record_call() 记录 源/成败/耗时（业务流量自然产生）。
- 主动探测：providers 注册 probe()，探测循环仅对 idle（长期无业务流量）的源兜底发起。
- 状态机：unknown / healthy / degraded / down（idle 只是触发探测的条件，不作为对外状态）。

Valkey 不可用时退化为进程内存存储（仅影响指标持久性，不影响状态判定）。
"""
import asyncio
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from core import config
from core.cache import redis_client

logger = logging.getLogger(__name__)

STATUS_UNKNOWN = "unknown"
STATUS_HEALTHY = "healthy"
STATUS_DEGRADED = "degraded"
STATUS_DOWN = "down"

_CALLS_KEY_PREFIX = "marketdata:health:calls:"

# Valkey 不可用时的内存兜底
_memory_calls: Dict[str, deque] = {}

# 已注册的主动探针：source -> (category, probe_coro)
_probes: Dict[str, Dict[str, Any]] = {}

# 上次状态，用于状态流转日志
_last_status: Dict[str, str] = {}


def _calls_key(source: str) -> str:
    return f"{_CALLS_KEY_PREFIX}{source}"


def register_probe(source: str, category: str, probe: Callable[[], Awaitable[None]]) -> None:
    """注册某数据源的主动探针。probe 抛出异常即视为探测失败。"""
    _probes[source] = {"category": category, "probe": probe}


def registered_sources() -> List[str]:
    return sorted(_probes.keys())


def _append_call(source: str, record: dict) -> None:
    window = config.HEALTH_WINDOW_SIZE
    if redis_client is not None:
        try:
            key = _calls_key(source)
            pipe = redis_client.pipeline()
            pipe.rpush(key, json.dumps(record, ensure_ascii=False))
            pipe.ltrim(key, -window, -1)
            pipe.execute()
            return
        except Exception as e:
            logger.warning(f"Record health call to Valkey failed for {source}: {e}")
    buf = _memory_calls.setdefault(source, deque(maxlen=window))
    buf.append(record)


def _read_calls(source: str) -> List[dict]:
    if redis_client is not None:
        try:
            raw = redis_client.lrange(_calls_key(source), 0, -1)
            return [json.loads(r) for r in raw]
        except Exception as e:
            logger.warning(f"Read health calls from Valkey failed for {source}: {e}")
    return list(_memory_calls.get(source, ()))


def record_call(source: str, ok: bool, latency_ms: float, via: str = "business", error: Optional[str] = None) -> None:
    """
    记录一次外部调用。via: business=业务流量 / probe=主动探测。
    """
    record = {
        "ts": time.time(),
        "ok": bool(ok),
        "latency_ms": round(latency_ms, 1),
        "via": via,
    }
    if error:
        record["error"] = str(error)[:300]
    _append_call(source, record)
    _refresh_status(source)


def _evaluate(calls: List[dict], now: float) -> str:
    if not calls:
        return STATUS_UNKNOWN
    recent_failures = [c for c in calls if not c["ok"]]
    if len(calls) >= config.HEALTH_DOWN_MIN_SAMPLES and len(recent_failures) == len(calls):
        return STATUS_DOWN
    success_rate = 1.0 - len(recent_failures) / len(calls)
    if success_rate < config.HEALTH_MIN_SUCCESS_RATE:
        return STATUS_DEGRADED
    return STATUS_HEALTHY


def _refresh_status(source: str) -> str:
    now = time.time()
    calls = _read_calls(source)
    status = _evaluate(calls, now)
    previous = _last_status.get(source)
    if previous is not None and previous != status:
        logger.warning(f"Source health transition: {source}: {previous} -> {status}")
    _last_status[source] = status
    return status


def get_source_health(source: str) -> Dict[str, Any]:
    """单个源的健康快照（状态 + 被动指标 + 最近一次错误）。"""
    now = time.time()
    calls = _read_calls(source)
    status = _evaluate(calls, now)

    last_call = calls[-1] if calls else None
    last_success = next((c for c in reversed(calls) if c["ok"]), None)
    ok_count = sum(1 for c in calls if c["ok"])
    total = len(calls)
    latencies = [c["latency_ms"] for c in calls]

    last_age = now - last_call["ts"] if last_call else None
    idle = last_age is None or last_age > config.HEALTH_IDLE_SECONDS

    return {
        "source": source,
        "category": _probes.get(source, {}).get("category", "unregistered"),
        "status": status,
        "idle": idle,
        "window": {
            "size": total,
            "success": ok_count,
            "failed": total - ok_count,
            "success_rate": round(ok_count / total, 4) if total else None,
            "avg_latency_ms": round(sum(latencies) / len(latencies), 1) if latencies else None,
        },
        "last_call_at": _fmt_ts(last_call["ts"]) if last_call else None,
        "last_success_at": _fmt_ts(last_success["ts"]) if last_success else None,
        "last_error": last_call.get("error") if last_call and not last_call["ok"] else None,
        "storage": "valkey" if redis_client is not None else "memory",
    }


def get_all_sources_health() -> List[Dict[str, Any]]:
    return [get_source_health(src) for src in registered_sources()]


def _fmt_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


async def run_probe(source: str) -> Dict[str, Any]:
    """对指定源执行一次主动探测并记录结果。"""
    entry = _probes.get(source)
    if entry is None:
        raise KeyError(f"No probe registered for source: {source}")
    started = time.monotonic()
    try:
        await asyncio.wait_for(entry["probe"](), timeout=config.HEALTH_PROBE_TIMEOUT)
        latency = (time.monotonic() - started) * 1000
        record_call(source, True, latency, via="probe")
        return {"source": source, "status": "healthy", "latency_ms": round(latency, 1)}
    except Exception as e:
        latency = (time.monotonic() - started) * 1000
        record_call(source, False, latency, via="probe", error=str(e))
        logger.warning(f"Active probe failed for {source}: {e}")
        return {"source": source, "status": "down", "latency_ms": round(latency, 1), "error": str(e)}


async def probe_idle_sources() -> List[Dict[str, Any]]:
    """探测 idle 的已注册源（按「最近一次记录最久」优先，每轮有上限）。

    节流说明：主动探测本身也会写入一条 via="probe" 记录，而 idle 判定看的是「最近一条记录」，
    因此同一源被探测后，在 HEALTH_IDLE_SECONDS 内不会再次入选 —— 天然形成
    「同一源最多约每 HEALTH_IDLE_SECONDS 探测一次」的节流（默认 600s，轮询间隔 300s）。

    错峰闸门：到期的源若在同一轮里 asyncio.gather 一起探测，多个重型探针（全市场份额/列表）
    会同时打上游；这里按最久未记录优先排序，每轮最多探测 HEALTH_PROBE_MAX_PER_ROUND 个，
    余下的下一轮继续。
    """
    now = time.time()
    candidates: List[Tuple[float, str]] = []
    for source in registered_sources():
        calls = _read_calls(source)
        last_ts = calls[-1]["ts"] if calls else None
        if last_ts is None or now - last_ts > config.HEALTH_IDLE_SECONDS:
            candidates.append((last_ts or 0.0, source))

    if not candidates:
        return []

    candidates.sort()  # 最久没有记录的优先
    limit = max(1, config.HEALTH_PROBE_MAX_PER_ROUND)
    targets = [source for _, source in candidates[:limit]]
    results = await asyncio.gather(*(run_probe(s) for s in targets))
    return list(results)


async def probe_loop(stop_event: asyncio.Event, interval: Optional[float] = None) -> None:
    """后台探测循环：周期性探测 idle 源。由 asyncio 生命周期托管。"""
    interval = interval if interval is not None else config.HEALTH_PROBE_INTERVAL
    logger.info(f"Health probe loop started (interval={interval}s, idle_threshold={config.HEALTH_IDLE_SECONDS}s)")
    try:
        while not stop_event.is_set():
            try:
                await probe_idle_sources()
            except Exception as e:
                logger.error(f"Probe loop iteration failed: {e}")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
    finally:
        logger.info("Health probe loop stopped")
