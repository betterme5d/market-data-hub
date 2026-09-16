# -*- coding: utf-8 -*-
"""退市本地库与水位线的单元测试（纯本地，无网络）。

重点覆盖**字段级合并**语义：深交所正文是缩略版，很多字段本来就没有，
绝不能用「没有值」去覆盖沪市 PDF 里已经拿到的值。
"""
import pytest

from providers.funds.delist.store import DelistStore, DelistSyncState


@pytest.fixture
def store(tmp_path):
    return DelistStore(tmp_path / "store.json")


@pytest.fixture
def state(tmp_path):
    return DelistSyncState(tmp_path / "state.json")


def test_merge_writes_new_record(store):
    rec = store.merge({"code": "560650", "market": "SH", "delist_date": "2026-08-24"})
    assert rec["delist_date"] == "2026-08-24"
    assert store.get("560650")["delist_date"] == "2026-08-24"


def test_merge_overwrites_when_new_value_present(store):
    store.merge({"code": "560650", "delist_date": "2026-08-24"})
    store.merge({"code": "560650", "delist_date": "2026-08-25"})
    assert store.get("560650")["delist_date"] == "2026-08-25"


@pytest.mark.parametrize("field", ["delist_date", "last_operation_date",
                                   "register_date", "suspend_date"])
def test_merge_keeps_old_when_new_empty(store, field):
    """深市没有这些字段，同步时不能把沪市已有的值清掉。"""
    store.merge({"code": "560650", field: "2026-07-02"})
    store.merge({"code": "560650", field: None})
    assert store.get("560650")[field] == "2026-07-02"


def test_merge_allows_overwriting_metadata(store):
    """非日期类元数据（如公告 URL）允许被新值覆盖。"""
    store.merge({"code": "560650", "announcement_url": "a"})
    store.merge({"code": "560650", "announcement_url": "b"})
    assert store.get("560650")["announcement_url"] == "b"


def test_code_is_normalised(store):
    """代码统一补零到 6 位，避免 '159969' 与 159969 存成两条。"""
    store.merge({"code": "159969", "delist_date": "2026-03-12"})
    assert store.get("159969") is not None
    assert len(store.load_all()) == 1


def test_merge_many_single_write(store):
    n = store.merge_many([
        {"code": "560650", "delist_date": "2026-08-24"},
        {"code": "159969", "delist_date": "2026-03-12"},
    ])
    assert n == 2
    assert len(store.load_all()) == 2


def test_lookup_missing_returns_none(store):
    """没退市的基金返回 None（调用方转成 found=false，不是 404）。"""
    assert store.get("510300") is None


# --------------------------------------------------------------- 水位线


def test_state_per_keyword_is_independent(state):
    """沪市「终止上市」与「摘牌」是两条独立流，水位线不能共用。

    共用一个 cursor 会让后者被前者的水位线截断，实测导致 519118 等摘牌公告漏掉。
    """
    state.save("SH:终止上市", cursor="2026-08-20", page=1, done=True)
    state.save("SH:摘牌", cursor="2025-08-09", page=1, done=True)
    assert state.get("SH:终止上市")["cursor"] == "2026-08-20"
    assert state.get("SH:摘牌")["cursor"] == "2025-08-09"


def test_state_resume_marks_not_done(state):
    """未跑完时 done=False，下轮从记录的 page 续拉。"""
    state.save("SH:摘牌", cursor="2025-08-09", page=3, done=False)
    st = state.get("SH:摘牌")
    assert st["done"] is False and st["page"] == 3


def test_state_clear_prefix(state):
    state.save("SH:终止上市", cursor="a")
    state.save("SH:摘牌", cursor="b")
    state.save("SZ:终止上市", cursor="c")
    state.clear_prefix("SH")
    assert state.get("SH:终止上市") == {}
    assert state.get("SH:摘牌") == {}
    assert state.get("SZ:终止上市")["cursor"] == "c"   # 其他市场不受影响
