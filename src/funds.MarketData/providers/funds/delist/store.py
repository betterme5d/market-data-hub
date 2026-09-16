# -*- coding: utf-8 -*-
"""场内基金退市本地库：主体存储 + 增量同步水位线。

设计取舍
--------
- **用文件不用 Valkey**：退市数据全量只有几百条、一天最多变 1~2 只，
  文件足够且天然持久化，进程重启不丢；Valkey 里的缓存反而是易失的。
- **原子写**：先写 `.tmp` 再 `os.replace`，避免半写把整库写坏。
- **键 = 6 位代码**：查询入口就是「传代码直接返回」，键设计必须按代码。

增量水位线
----------
| 市场 | 标记 | 语义 | 拉取时的用法 |
| --- | --- | --- | --- |
| SH | `last_date` | 公告日期 `SSEDATE`（YYYY-MM-DD） | `START_DATE = last_date` |
| SZ | `last_ms`   | 发布时间 `docpubtime`（毫秒）     | `time = last_ms` |

另存 `last_page` 做**页级断点续传**：上次没跑完时从该页继续，
避免中途失败后整批重拉。
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

CACHE_DIR = Path(os.getenv("DELIST_CACHE_DIR", "data"))
STORE_FILE = CACHE_DIR / "delist_store.json"
STATE_FILE = CACHE_DIR / "delist_state.json"

#: 合并时若新值为空则保留旧值的字段（深市正文是缩略版，很多字段本来就没有，
#: 不能用「没有值」去覆盖沪市 PDF 里已有的值）
MERGE_KEEP_OLD_IF_EMPTY = (
    "delist_date", "last_operation_date", "register_date", "suspend_date",
)


def _ensure_dir(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"mkdir failed for {path.parent}: {e}")


def _atomic_write_json(path: Path, payload) -> None:
    _ensure_dir(path)
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, path)
    except OSError as e:
        logger.error(f"save {path} failed: {e}")


def _read_json(path: Path):
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:                                # noqa: BLE001
        logger.warning(f"load {path} failed: {e}")
        return None


class DelistStore:
    """退市记录本地库（code → record），带字段级合并。"""

    def __init__(self, path: Path = STORE_FILE):
        self.path = path

    def load_all(self) -> Dict[str, dict]:
        data = _read_json(self.path)
        return data.get("records", {}) if isinstance(data, dict) else {}

    def get(self, code: str) -> Optional[dict]:
        return self.load_all().get(str(code).strip().zfill(6))

    def merge(self, record: dict) -> dict:
        """字段级 upsert：新值非空则覆盖，新值为空且旧值非空则保留旧值。

        这样深市（缩略正文，无最后运作日）不会把沪市已有的值清掉，
        也保证同一只基金被多个关键词/多次同步命中时不会互相抹掉信息。
        """
        code = str(record.get("code") or "").strip().zfill(6)
        if not code:
            raise ValueError("record 缺少 code，无法入库")

        records = self.load_all()
        old = records.get(code, {})
        merged = dict(old)

        for k, v in record.items():
            if v in (None, "", []):
                if k in MERGE_KEEP_OLD_IF_EMPTY and old.get(k):
                    continue          # 保留旧值
            merged[k] = v

        merged["code"] = code
        merged["updated_at"] = datetime.now().isoformat(timespec="seconds")
        records[code] = merged
        _atomic_write_json(self.path, {
            "as_of": datetime.now().isoformat(timespec="seconds"),
            "count": len(records),
            "records": records,
        })
        return merged

    def merge_many(self, records: List[dict]) -> int:
        """批量合并，一次落盘（避免逐条写几百次文件）。"""
        if not records:
            return 0
        all_records = self.load_all()
        now = datetime.now().isoformat(timespec="seconds")
        n = 0
        for record in records:
            code = str(record.get("code") or "").strip().zfill(6)
            if not code:
                continue
            old = all_records.get(code, {})
            merged = dict(old)
            for k, v in record.items():
                if v in (None, "", []) and k in MERGE_KEEP_OLD_IF_EMPTY and old.get(k):
                    continue
                merged[k] = v
            merged["code"] = code
            merged["updated_at"] = now
            all_records[code] = merged
            n += 1
        _atomic_write_json(self.path, {
            "as_of": now, "count": len(all_records), "records": all_records})
        return n


class DelistKnownCodes:
    """**累积**见过的基金代码全集 —— 雪球扫描的扫描对象。

    为什么需要累积：退市基金会从挂牌列表里消失，若只扫「当前列表」，
    那些已经消失的就永远扫不到了。这里把每次见到的代码都记下来，
    扫描对象 = 当前列表 ∪ 历史累积 ∪ 本地库记录。

    随着运行时间推移，全集会自然覆盖所有曾出现过的代码。
    """

    def __init__(self, path: Path = CACHE_DIR / "delist_known.json"):
        self.path = path

    def load(self) -> set:
        data = _read_json(self.path)
        codes = data.get("codes", []) if isinstance(data, dict) else []
        return set(codes)

    def add(self, codes) -> int:
        """并入新代码，返回新增数量。"""
        known = self.load()
        before = len(known)
        known.update(str(c).strip().zfill(6) for c in codes if c)
        if len(known) != before:
            _atomic_write_json(self.path, {
                "count": len(known),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "codes": sorted(known),
            })
        return len(known) - before


class DelistSyncState:
    """增量同步水位线（按市场）。"""

    def __init__(self, path: Path = STATE_FILE):
        self.path = path

    def load(self) -> dict:
        data = _read_json(self.path)
        return data if isinstance(data, dict) else {}

    def get(self, key: str) -> dict:
        """取水位置状态。

        **key 必须按 (市场, 关键词) 粒度** —— 沪市「终止上市」与「摘牌」是两条
        独立的召回流，日期分布完全不同。共用一个 cursor 会让后者被前者的
        水位线截断（实测导致 519118 等摘牌公告全部漏掉）。
        约定 key 形如 `SH:终止上市`。
        """
        return self.load().get(key, {})

    def save(self, key: str, **kw) -> None:
        """更新某条流的水位线（只传要改的字段）。"""
        state = self.load()
        cur = dict(state.get(key, {}))
        cur.update(kw)
        cur["updated_at"] = datetime.now().isoformat(timespec="seconds")
        state[key] = cur
        _atomic_write_json(self.path, state)

    def clear(self, key: str) -> None:
        state = self.load()
        state.pop(key, None)
        _atomic_write_json(self.path, state)

    def _all_updated(self) -> List[datetime]:
        stamps: List[datetime] = []
        for v in self.load().values():
            raw = v.get("updated_at")
            if not raw:
                continue
            try:
                stamps.append(datetime.fromisoformat(raw))
            except ValueError:
                continue
        return stamps

    def latest_updated(self) -> Optional[datetime]:
        """所有流里**最新**的 updated_at —— 用于判断「最近有没有同步过」。

        启动时判断是否可以跳过（容器频繁重启时不重复打上游）用这个。
        """
        stamps = self._all_updated()
        return max(stamps) if stamps else None

    def oldest_updated(self) -> Optional[datetime]:
        """所有流里**最旧**的 updated_at —— 用于判断「有没有流卡住」。

        探针必须用**最旧**而不是最新：若只取最新，某一条流
        （如「摘牌」）卡死而其他流正常，取 max 就会被掩盖，探针全绿但数据残缺。
        """
        stamps = self._all_updated()
        return min(stamps) if stamps else None

    def clear_prefix(self, prefix: str) -> None:
        """清空某市场下的所有流（全量回填时用），如 prefix="SH"。"""
        state = self.load()
        for k in [k for k in state if k.startswith(prefix)]:
            state.pop(k)
        _atomic_write_json(self.path, state)
