# -*- coding: utf-8 -*-
"""进程内状态缓存（core/state_store.py）行为测试。

替代原 test_cache_client.py（那测的是 Valkey 客户端创建）。这里覆盖：
TTL 惰性过期、计数器 + 过期窗口、文件落盘与跨进程重载、schema 版本不兼容丢弃、
容量上限淘汰，以及「纯内存存储不产生文件」。
"""
import json
import time

from core import state_store
from core.state_store import SCHEMA_VERSION, StateStore


def test_set_get_ttl_and_delete():
    store = StateStore("t_basic")
    store.setex("k", 10, "v")
    assert store.get("k") == "v"
    assert 9 <= store.ttl("k") <= 10

    store.set("plain", "v2")
    assert store.ttl("plain") == -1, "无 TTL 的键必须返回 -1（与 Redis 语义一致）"
    assert store.ttl("missing") == -2
    assert store.exists("k", "plain", "missing") == 2
    assert store.delete("k", "plain") == 2
    assert store.get("k") is None


def test_expiry_is_lazy_but_effective():
    store = StateStore("t_expire")
    store.setex("k", 1, "v")
    assert store.get("k") == "v"
    time.sleep(1.05)
    assert store.get("k") is None, "过期后必须读不到"
    assert store.ttl("k") == -2
    assert store.exists("k") == 0
    assert store.keys() == []


def test_incr_and_failure_window():
    """熔断失败计数的语义：incr 自增 + expire 设滑动窗口。"""
    store = StateStore("t_incr")
    assert store.incr("failures") == 1
    assert store.incr("failures") == 2
    assert store.get("failures") == "2"
    assert store.ttl("failures") == -1
    assert store.expire("failures", 60) is True
    assert 59 <= store.ttl("failures") <= 60
    assert store.expire("missing", 60) is False
    store.delete("failures")
    assert store.incr("failures") == 1, "删除后计数必须从头开始"


def test_persistent_store_survives_process_restart():
    """落文件的状态（熔断冷却）必须能被新进程实例读回。"""
    first = StateStore("t_persist", persist=True)
    first.setex("blocked:sina", 300, "1")
    path = state_store.state_dir() / "t_persist.json"
    assert path.exists(), "persist=True 必须落盘"

    # 新实例 = 模拟进程重启：内存为空，只能从文件恢复
    second = StateStore("t_persist", persist=True)
    assert second.get("blocked:sina") == "1"
    assert 290 <= second.ttl("blocked:sina") <= 300


def test_expired_entries_are_dropped_on_reload():
    store = StateStore("t_reload", persist=True)
    store.setex("short", 1, "v")
    store.set("forever", "v2")
    time.sleep(1.05)

    reloaded = StateStore("t_reload", persist=True)
    assert reloaded.get("short") is None, "加载时就要丢弃已过期项"
    assert reloaded.get("forever") == "v2"


def test_unknown_schema_version_is_discarded():
    """读到更高版本的状态文件必须整份丢弃（宁可重取，不猜旧语义）。"""
    path = state_store.state_dir() / "t_schema.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"schema_version": SCHEMA_VERSION + 1, "entries": {"k": {"v": "v", "exp": None}}}),
        encoding="utf-8",
    )
    store = StateStore("t_schema", persist=True)
    assert store.get("k") is None


def test_capacity_is_bounded():
    store = StateStore("t_capacity", max_entries=3)
    for i in range(5):
        store.set(f"k{i}", "v")
    assert len(store.keys()) == 3, "超出上限必须淘汰最旧的键"
    assert store.stats()["evictions"] >= 2


def test_memory_only_store_writes_no_file():
    store = StateStore("t_memory")
    store.setex("k", 10, "v")
    assert store.persist is False
    assert (state_store.state_dir() / "t_memory.json").exists() is False