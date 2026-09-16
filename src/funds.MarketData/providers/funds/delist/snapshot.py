# -*- coding: utf-8 -*-
"""交易所基金列表快照与差集：把「全量扫公告」收敛成「只查消失的那几只」。

背景：全量模式要下载 200+ 份 PDF，不可能每天跑。但真实退市每天最多 1~2 只
（2025 全年深市仅 9 只），所以正确做法是先用列表差集定位候选，再只查这些代码。

**差集只负责召回候选，不产出退市结论** —— 上游列表会截断，消失 ≠ 退市。
因此这里同时做两道保护：
1. 行数下限：拿到的列表明显偏少 → 判为上游异常，不产出候选
2. 消失比例阈值：一次消失太多 → 判为上游异常，不产出候选

结论永远由公告正文（终止上市日）给出，见 `provider.py`。
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from providers.exchanges.sse import SseFundListSource
from providers.exchanges.szse import SzseFundListSource

logger = logging.getLogger(__name__)

CACHE_DIR = Path(os.getenv("DELIST_CACHE_DIR", "data"))
RESULT_STALE_SECONDS = int(os.getenv("DELIST_RESULT_STALE_SECONDS", str(24 * 3600)))

#: 实测行数（2026-09）：沪 ETF+LOF=1047，深 1105=1044。低于下限视为上游截断。
MIN_EXPECTED_ROWS = {
    "SH": int(os.getenv("DELIST_MIN_ROWS_SH", "800")),
    "SZ": int(os.getenv("DELIST_MIN_ROWS_SZ", "900")),
}
#: 一次消失超过该比例即判为上游异常（真实退市远达不到）
MAX_MISSING_RATIO = float(os.getenv("DELIST_MAX_MISSING_RATIO", "0.05"))

_LIST_SOURCES = {"SH": SseFundListSource, "SZ": SzseFundListSource}


def _snapshot_file(market: str) -> Path:
    return CACHE_DIR / f"delist_snapshot_{market.lower()}.json"


def _result_file() -> Path:
    return CACHE_DIR / "delist_result.json"


def _ensure_dir(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"mkdir failed for {path.parent}: {e}")


def _atomic_write_json(path: Path, payload) -> None:
    """原子写：先写 .tmp 再 os.replace，避免半写。"""
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


class DelistSnapshotStore:
    """交易所基金列表快照（按市场存）。"""

    def load(self, market: str) -> Optional[Dict]:
        data = _read_json(_snapshot_file(market))
        return data if isinstance(data, dict) else None

    def save(self, market: str, codes: List[str], as_of: Optional[str] = None) -> None:
        _atomic_write_json(_snapshot_file(market), {
            "as_of": as_of or datetime.now().date().isoformat(),
            "count": len(codes),
            "codes": sorted(codes),
        })


async def fetch_current_codes(market: str) -> List[str]:
    """拉取某交易所当前挂牌的场内基金代码。"""
    rows = await _LIST_SOURCES[market]().fetch_funds()
    codes = {str(r.get("fund_code") or "").strip() for r in rows}
    codes.discard("")
    return sorted(codes)


async def detect_candidates(
    store: Optional[DelistSnapshotStore] = None,
) -> Tuple[List[Dict], List[str]]:
    """与上一轮快照比对，产出「疑似退市」候选。

    :return: (候选列表, 告警列表)
             候选形如 {"code": "560650", "market": "SH", "missing_since": "..."}
    """
    store = store or DelistSnapshotStore()
    candidates: List[Dict] = []
    warnings: List[str] = []
    today = datetime.now().date().isoformat()

    for market in ("SH", "SZ"):
        try:
            current = await fetch_current_codes(market)
        except Exception as e:                            # noqa: BLE001
            warnings.append(f"{market} 列表拉取失败，跳过: {e}")
            logger.error(f"delist snapshot fetch failed [{market}]: {e}")
            continue

        if len(current) < MIN_EXPECTED_ROWS[market]:
            warnings.append(
                f"{market} 列表仅 {len(current)} 行，低于下限 "
                f"{MIN_EXPECTED_ROWS[market]}，判为上游截断，本次不产出候选")
            continue

        previous = store.load(market)
        if not previous or not previous.get("codes"):
            # 首次运行：只建基线，不产出候选（没有可比对象，产出就是全量）
            store.save(market, current, today)
            warnings.append(f"{market} 首次运行，已建立基线快照（{len(current)} 只），本次不产出候选")
            continue

        prev_codes = set(previous["codes"])
        missing = prev_codes - set(current)
        if missing:
            ratio = len(missing) / max(len(prev_codes), 1)
            if ratio > MAX_MISSING_RATIO:
                warnings.append(
                    f"{market} 一次消失 {len(missing)} 只（占比 {ratio:.1%}），"
                    f"超过阈值 {MAX_MISSING_RATIO:.0%}，判为上游异常，本次不产出候选")
                continue
            for code in sorted(missing):
                candidates.append({
                    "code": code, "market": market,
                    "missing_since": today,
                    "reason": "不在交易所最新列表（候选，需公告确认）",
                })

        store.save(market, current, today)

    return candidates, warnings


# --------------------------------------------------------------------- 结果缓存


def load_cached_result() -> Optional[List[Dict]]:
    """读取上一次采集结果（供只读场景快速返回）。"""
    data = _read_json(_result_file())
    return data.get("records") if isinstance(data, dict) else None


def save_cached_result(records: List[Dict]) -> None:
    _atomic_write_json(_result_file(), {
        "as_of": datetime.now().isoformat(timespec="seconds"),
        "count": len(records),
        "records": records,
    })


def is_result_stale() -> bool:
    path = _result_file()
    if not path.exists():
        return True
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        return datetime.now() - mtime > timedelta(seconds=RESULT_STALE_SECONDS)
    except OSError:
        return True
