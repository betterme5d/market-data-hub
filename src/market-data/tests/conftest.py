# -*- coding: utf-8 -*-
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core import health as health_svc  # noqa: E402
from core import state_store  # noqa: E402


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    """用例级隔离：状态缓存落到临时目录并清空，健康指标窗口同样清零。

    状态缓存已改为进程内实现（core/state_store.py）：这里既保证用例之间不互相污染，
    也保证测试不会写真实仓库目录 data/state/。
    """
    monkeypatch.setattr(state_store, "_state_dir_override", tmp_path)
    state_store.reset_all()
    health_svc._memory_calls.clear()
    health_svc._last_status.clear()
    yield
    state_store.reset_all()