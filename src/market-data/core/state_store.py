# -*- coding: utf-8 -*-
"""
进程内状态缓存：内存热层 + 可选 JSON 文件落盘（替代原 Valkey 依赖）。

为什么改
--------
原实现把跨请求状态放在 Valkey 里。但「跨进程共享」在本服务的部署形态下并不存在
（单容器、单 uvicorn 进程、无 --workers），代价却是常驻一个中间件、一套连接配置与
一条启动顺序依赖（market-data 要等 valkey healthy）。改成进程内缓存后行为等价，
唯一的语义差异是「多 worker / 多副本之间不再共享」——当前形态不涉及。

分层依据（数据寿命决定要不要落盘）
----------------------------------
- 报价短路缓存（10 秒 TTL）           → 纯内存：落盘毫无意义
- 熔断状态（冷却 300 秒）             → 落文件：重启后重建的代价是「再打一次已知挂掉的上游」
- 雪球鉴权 Cookie（1 小时 TTL）       → 落文件：重启后重建要唤醒一次 Playwright 网关
- yfinance 后复权锚点（几乎不变）     → 落文件：重启后重建要拉一次全量历史

API 为什么长得像 Redis
----------------------
调用方（core.dispatcher）原本就是按 Redis 语义写的（get/setex/incr/expire/ttl/mget）。
保持同构可以把改动限制在「换存储」，不重写业务逻辑，也就不会在熔断/缓存的判定细节上
引入回归。`exists()` 返回 0/1、`delete()` 返回删除条数，都是为兼容原调用点。

文件格式
--------
::

    {"schema_version": 1, "updated_at": "...", "entries": {"<key>": {"v": <值>, "exp": <时间戳秒>|null}}}

`exp` 是绝对过期时间戳，null 表示永不过期。读到比本模块更高的 schema_version 时整份丢弃
（宁可重取，也不能按旧语义猜错覆盖区间）——与 core/timeseries_cache 的 meta.json 同策略。
写文件用「临时文件 + os.replace」原子替换，避免半写把状态写坏。
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from core import config

logger = logging.getLogger(__name__)

#: 状态文件结构版本：不兼容变更时递增。
SCHEMA_VERSION = 1

#: 单个存储的条目上限（防无界增长）。超出时先清过期项，再按插入顺序淘汰最旧的。
DEFAULT_MAX_ENTRIES = 20000

#: 每多少次写操作做一次过期清扫（读路径是惰性过期，不依赖这里）。
_SWEEP_EVERY_WRITES = 128

_state_dir_override: Optional[Path] = None
_stores: List["StateStore"] = []
_stores_guard = threading.Lock()


def state_dir() -> Path:
    """状态目录（默认 data/state，可用 STATE_DIR 覆盖）。"""
    if _state_dir_override is not None:
        return _state_dir_override
    return Path(config.STATE_DIR)


def set_state_dir(path) -> None:
    """覆盖状态目录（测试隔离用）；只影响后续路径解析与加载。"""
    global _state_dir_override
    _state_dir_override = Path(path)


def reset_all() -> None:
    """清空所有已创建的存储（测试隔离用）。"""
    with _stores_guard:
        stores = list(_stores)
    for store in stores:
        store.reset()


def _atomic_replace(tmp_path: str, target_path: str, retries: int = 5) -> None:
    """os.replace 带重试：Windows 上目标被占用时会抛 PermissionError（同 timeseries_cache）。"""
    for attempt in range(retries):
        try:
            os.replace(tmp_path, target_path)
            return
        except PermissionError:
            if attempt == retries - 1:
                raise
            time.sleep(0.05 * (attempt + 1))


class StateStore:
    """内存热层 + 可选文件落盘的小型 KV 存储。

    - 值以字符串（通常是 JSON）为主，调用方自行序列化；
    - 过期是惰性的（读取时判定），另有按写次数触发的清扫；
    - 线程安全（threading.RLock）：yfinance 等取数路径会经 asyncio.to_thread 调到底层。
    """

    def __init__(self, name: str, persist: bool = False, max_entries: int = DEFAULT_MAX_ENTRIES):
        self.name = name
        self.persist = persist
        self.max_entries = max_entries
        self._data: Dict[str, Any] = {}
        self._expires: Dict[str, Optional[float]] = {}
        self._lock = threading.RLock()
        self._loaded = not persist
        self._writes_since_sweep = 0
        self._counters = {"reads": 0, "hits": 0, "writes": 0, "evictions": 0, "expired": 0}
        with _stores_guard:
            _stores.append(self)

    # ------------------------------------------------------------------
    # 文件
    # ------------------------------------------------------------------
    @property
    def path(self) -> Path:
        return state_dir() / f"{self.name}.json"

    def _ensure_loaded_locked(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        if not self.persist:
            return
        path = self.path
        try:
            if not path.exists():
                return
            payload = json.loads(path.read_text(encoding="utf-8"))
            if int(payload.get("schema_version", 0)) > SCHEMA_VERSION:
                logger.warning(
                    "state file %s has schema_version=%s > %s; 丢弃并按空状态启动",
                    path, payload.get("schema_version"), SCHEMA_VERSION,
                )
                return
            now = time.time()
            kept = 0
            for key, item in (payload.get("entries") or {}).items():
                if not isinstance(item, dict):
                    continue
                exp = item.get("exp")
                if exp is not None and float(exp) <= now:
                    continue
                self._data[key] = item.get("v")
                self._expires[key] = exp
                kept += 1
            logger.info("State file %s loaded: %d 条（过期项已丢弃）", path, kept)
        except Exception as e:  # noqa: BLE001 - 状态文件损坏不应阻断启动
            logger.warning(f"Load state file {path} failed: {e}")

    def _flush_locked(self) -> None:
        if not self.persist:
            return
        path = self.path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "schema_version": SCHEMA_VERSION,
                "updated_at": datetime.now().isoformat(),
                "entries": {
                    key: {"v": value, "exp": self._expires.get(key)}
                    for key, value in self._data.items()
                },
            }
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            _atomic_replace(str(tmp), str(path))
        except OSError as e:
            logger.warning(f"Save state file {path} failed: {e}")

    # ------------------------------------------------------------------
    # 过期与容量
    # ------------------------------------------------------------------
    def _is_expired_locked(self, key: str) -> bool:
        exp = self._expires.get(key)
        return exp is not None and exp <= time.time()

    def _purge_locked(self, key: str) -> bool:
        if key in self._data and self._is_expired_locked(key):
            self._data.pop(key, None)
            self._expires.pop(key, None)
            self._counters["expired"] += 1
            return True
        return False

    def _sweep_locked(self) -> int:
        expired = [k for k in list(self._data) if self._is_expired_locked(k)]
        for key in expired:
            self._data.pop(key, None)
            self._expires.pop(key, None)
            self._counters["expired"] += 1
        return len(expired)

    def _enforce_capacity_locked(self) -> None:
        if len(self._data) <= self.max_entries:
            return
        self._sweep_locked()
        while len(self._data) > self.max_entries:
            oldest = next(iter(self._data))
            self._data.pop(oldest, None)
            self._expires.pop(oldest, None)
            self._counters["evictions"] += 1
            logger.debug("StateStore %s 超出上限，淘汰最旧键 %s", self.name, oldest)

    def _after_write_locked(self) -> None:
        self._counters["writes"] += 1
        self._writes_since_sweep += 1
        if self._writes_since_sweep >= _SWEEP_EVERY_WRITES:
            self._writes_since_sweep = 0
            self._sweep_locked()
        self._enforce_capacity_locked()
        self._flush_locked()

    # ------------------------------------------------------------------
    # KV
    # ------------------------------------------------------------------
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            self._ensure_loaded_locked()
            self._counters["reads"] += 1
            if self._purge_locked(key):
                self._flush_locked()
                return None
            value = self._data.get(key)
            if value is not None:
                self._counters["hits"] += 1
            return value

    def mget(self, keys: Iterable[str]) -> List[Optional[Any]]:
        return [self.get(key) for key in keys]

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        with self._lock:
            self._ensure_loaded_locked()
            self._data[key] = value
            self._expires[key] = (time.time() + ttl) if ttl else None
            self._after_write_locked()

    def setex(self, key: str, ttl: int, value: Any) -> None:
        self.set(key, value, ttl=ttl)

    def delete(self, *keys: str) -> int:
        with self._lock:
            self._ensure_loaded_locked()
            removed = 0
            for key in keys:
                if key in self._data or key in self._expires:
                    self._data.pop(key, None)
                    self._expires.pop(key, None)
                    removed += 1
            if removed:
                self._after_write_locked()
            return removed

    def exists(self, *keys: str) -> int:
        """返回存在的键数量（与 Redis 语义一致，原调用点按 == 1 判定）。"""
        count = 0
        for key in keys:
            if self.get(key) is not None:
                count += 1
        return count

    def ttl(self, key: str) -> int:
        """-2 键不存在；-1 存在但无过期；否则为剩余秒数（向上取整）。"""
        with self._lock:
            self._ensure_loaded_locked()
            if self._purge_locked(key):
                self._flush_locked()
                return -2
            if key not in self._data:
                return -2
            exp = self._expires.get(key)
            if exp is None:
                return -1
            return max(0, math.ceil(exp - time.time()))

    def incr(self, key: str) -> int:
        """自增计数；值不可解析为整数时按 0 起算（并告警，不抛异常打断取数）。"""
        with self._lock:
            self._ensure_loaded_locked()
            raw = self._data.get(key)
            try:
                current = int(raw) if raw is not None else 0
            except (TypeError, ValueError):
                logger.warning("StateStore %s: key %s 的值 %r 不是整数，按 0 起算", self.name, key, raw)
                current = 0
            value = current + 1
            self._data[key] = str(value)
            exp = self._expires.get(key)
            if key not in self._expires or exp is None:
                # 无过期的计数器永远不过期（与 Redis 的 INCR 一致，由调用方 expire() 兜底）
                self._expires.setdefault(key, None)
            self._after_write_locked()
            return value

    def expire(self, key: str, ttl: int) -> bool:
        with self._lock:
            self._ensure_loaded_locked()
            if self._purge_locked(key) or key not in self._data:
                return False
            self._expires[key] = time.time() + ttl
            self._after_write_locked()
            return True

    def keys(self) -> List[str]:
        with self._lock:
            self._ensure_loaded_locked()
            self._sweep_locked()
            return list(self._data.keys())

    def sweep(self) -> int:
        with self._lock:
            self._ensure_loaded_locked()
            removed = self._sweep_locked()
            if removed:
                self._flush_locked()
            return removed

    def reset(self) -> None:
        """清空内存态并重新加载标记（测试隔离用；不删除文件）。"""
        with self._lock:
            self._data.clear()
            self._expires.clear()
            self._loaded = not self.persist
            for key in self._counters:
                self._counters[key] = 0
            self._writes_since_sweep = 0

    def clear(self) -> None:
        """清空内存态与文件。"""
        with self._lock:
            self._ensure_loaded_locked()
            self._data.clear()
            self._expires.clear()
            self._flush_locked()
            if self.persist:
                try:
                    self.path.unlink(missing_ok=True)
                except OSError as e:
                    logger.warning(f"Remove state file {self.path} failed: {e}")

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "persist": self.persist,
                "entries": len(self._data),
                "path": str(self.path) if self.persist else None,
                **self._counters,
            }