# -*- coding: utf-8 -*-
"""缓存覆盖区间完整性扫描（**只读**：不改任何缓存/元数据）。

用途
----
排查 N1（沪市份额「假完整交易日」）与 N3（爬虫页数封顶/短页被当成末页）可能留下的
**脏覆盖区间**：区间已声明为「已覆盖」，但区间内实际没有数据——
而覆盖区间一旦标记就不会再回上游，于是那段数据永久缺失。

可疑判据
--------
A. **区间零记录**：声明已覆盖，但区间内一条数据都没有（最强信号）。
   已自动排除「区间整体落在首条记录之前（上市前）」与「末条记录之后（退市后）」，
   那两类是全市场扇出时的正常现象。
B. **内部空洞**：空洞两侧都有数据、中间缺失。

已知误报来源（**务必回查上游再下结论**）
----------------------------------------
1. 上游确实存在「某些基金某天不报数据」的情况。实测：
   - 深市 2025-05-19 上游只返回 741 只（相邻交易日 787 只）→ 46 只基金的当日空洞是忠实的；
   - 159008 在 2026-01-12~2026-04-14 上游 946 只里就不含它 → 零记录区间是忠实的；
   - 501023 在 2022-03-31~2022-04-21 上游本身就没有净值（03-30 直接跳到 04-22）。
2. QDII / 跨境基金按**境外**日历发布净值，而本脚本用的是 CN 交易日历，
   于是圣诞周、美国国庆这类日子会成片报成"空洞"——这是日历不匹配的系统性误报。

核验方式：`market_have / market_peak` 只是辅助指标，真正判据是**回查上游**。

用法（在容器内跑，依赖 pandas/pyarrow + 交易日历）：
    docker exec funds_marketdata sh -lc "cd /app && python scripts/scan_cache_integrity.py --top 20"
    docker exec funds_marketdata sh -lc "cd /app && python scripts/scan_cache_integrity.py --namespace fund_share --top 30"

需要机器可读结果时用 --json 输出到指定路径。
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402

from core.calendar import get_exchange_trading_days  # noqa: E402
from core.timeseries_cache.storage import ParquetStorageEngine  # noqa: E402

DEFAULT_BASE = Path("data/cache")
NAMESPACES = ("fund_share", "fund_nav")


def iter_cache_entries(base: Path, namespaces: Sequence[str]):
    """遍历缓存目录，产出 (namespace, key, dimensions, parquet_path)。"""
    for ns in namespaces:
        ns_dir = base / ns
        if not ns_dir.exists():
            continue
        for p in sorted(ns_dir.rglob("*.parquet")):
            rel = p.relative_to(ns_dir)
            dims: Dict[str, str] = {}
            for part in rel.parts[:-1]:
                if "=" in part:
                    k, v = part.split("=", 1)
                    dims[k] = v
            yield ns, p.stem, dims, p


def load_present_dates(parquet_path: Path, date_column: str) -> set:
    """读出 parquet 里已有的日期集合（读不到就当空）。"""
    try:
        df = pd.read_parquet(parquet_path, engine="pyarrow", columns=[date_column])
    except Exception:
        try:
            df = pd.read_parquet(parquet_path, engine="pyarrow")
        except Exception:
            return set()
    if date_column not in df.columns or df.empty:
        return set()
    return set(df[date_column].astype(str).tolist())


def find_interior_holes(
    trading_days: Sequence[str], present: set
) -> List[Tuple[str, str, int]]:
    """找出「两侧都有数据」的内部空洞，返回 [(起, 止, 缺交易日数)]。"""
    holes: List[Tuple[str, str, int]] = []
    run: List[str] = []
    run_has_left = False

    for i, day in enumerate(trading_days):
        if day not in present:
            if not run:
                run_has_left = i > 0  # 左边有数据 → 是内部空洞的候选
            run.append(day)
            continue
        if run:
            if run_has_left:  # 右边也有数据（当前 day 就在右边）
                holes.append((run[0], run[-1], len(run)))
            run = []
            run_has_left = False

    # 结尾的空缺：左边有数据但右边没有了 → 属于「退市后」的正常边界缺口，不报
    return holes


def scan(base: Path, namespaces: Sequence[str]) -> dict:
    engine = ParquetStorageEngine(base_dir=base)

    entries = list(iter_cache_entries(base, namespaces))
    if not entries:
        return {"scanned": 0, "zero_record": [], "interior_holes": [], "no_metadata": 0}

    # 先收集全部覆盖区间，用于一次性拉取交易日历（避免逐基金打日历）
    intervals_by_file: List[Tuple[str, str, Dict[str, str], Path, List[Tuple[str, str]]]] = []
    all_s: List[str] = []
    all_e: List[str] = []
    no_metadata = 0

    for ns, key, dims, p in entries:
        meta = engine.read_metadata(ns, key, dimensions=dims) or {}
        intervals = [tuple(iv) for iv in meta.get("intervals", []) if len(iv) >= 2]
        if not intervals:
            no_metadata += 1
            continue
        intervals_by_file.append((ns, key, dims, p, intervals))
        for s, e in intervals:
            all_s.append(s)
            all_e.append(e)

    if not intervals_by_file:
        return {"scanned": 0, "zero_record": [], "interior_holes": [], "no_metadata": no_metadata}

    cal_start, cal_end = min(all_s), max(all_e)
    trading_days = [
        d["date"]
        for d in get_exchange_trading_days("CN", cal_start, cal_end)
        if d.get("is_trading")
    ]

    zero_suspicious: List[dict] = []
    zero_normal: List[dict] = []
    hole_stats: Dict[Tuple[str, str], dict] = defaultdict(
        lambda: {"namespace": "", "code": "", "dims": {}, "holes": 0, "missing_days": 0, "worst": None}
    )
    # 全市场「每个交易日有多少只标的有数据」：用于区分「上游当天整体缺失」与「单只标的漏了」
    day_density: Dict[Tuple[str, str], int] = defaultdict(int)

    # 每个 parquet 只读一次（6600+ 文件，重复读会直接把扫描拖到十几分钟）
    present_cache: Dict[Path, set] = {}
    for ns, key, dims, p, intervals in intervals_by_file:
        present = present_cache.get(p)
        if present is None:
            date_column = "share_date" if ns == "fund_share" else "nav_date"
            present = load_present_dates(p, date_column)
            present_cache[p] = present
        bucket = dims.get("exchange", dims.get("source", ""))
        for d in present:
            day_density[(ns, bucket, d)] += 1

    for ns, key, dims, p, intervals in intervals_by_file:
        present = present_cache[p]
        bucket = dims.get("exchange", dims.get("source", ""))
        first_date = min(present) if present else None
        last_date = max(present) if present else None

        for s, e in intervals:
            tds = _slice_trading_days(trading_days, s, e)
            if not tds:
                continue
            have = present & set(tds)
            if not have:
                row = {
                    "namespace": ns,
                    "code": key,
                    "dims": dict(dims),
                    "interval": [s, e],
                    "trading_days": len(tds),
                    "first_date": first_date,
                    "last_date": last_date,
                }
                # 区间整体落在「首条记录之前」= 上市/成立前（正常，全市场扇出时这只还没有数据）；
                # 区间整体落在「末条记录之后」= 退市后（正常）。其余才算可疑。
                if first_date and e < first_date:
                    row["reason"] = "pre_listing"
                    zero_normal.append(row)
                elif last_date and s > last_date:
                    row["reason"] = "post_delisting"
                    zero_normal.append(row)
                else:
                    row["reason"] = "suspicious"
                    zero_suspicious.append(row)
                continue
            for hs, he, n in find_interior_holes(tds, present):
                stat = hole_stats[(ns, key)]
                stat["namespace"] = ns
                stat["code"] = key
                stat["dims"] = dict(dims)
                stat["holes"] += 1
                stat["missing_days"] += n
                if stat["worst"] is None or n > stat["worst"][2]:
                    stat["worst"] = [hs, he, n]

    # 给每个空洞补上「当天全市场有多少只标的有数据 / 相邻窗口的峰值」，便于判断是不是上游整体缺失
    for stat in hole_stats.values():
        ns, code = stat["namespace"], stat["code"]
        bucket = stat["dims"].get("exchange", stat["dims"].get("source", ""))
        w = stat["worst"]
        if not w:
            continue
        day = w[0]
        window = _slice_trading_days(trading_days, *_plus_minus_days(day, 7))
        around = [day_density[(ns, bucket, d)] for d in window if (ns, bucket, d) in day_density]
        stat["market_have"] = day_density.get((ns, bucket, day), 0)
        stat["market_peak"] = max(around) if around else 0

    interior = sorted(
        hole_stats.values(), key=lambda x: (-x["missing_days"], -x["holes"])
    )
    return {
        "scanned": len(entries),
        "with_intervals": len(intervals_by_file),
        "no_metadata": no_metadata,
        "calendar": [cal_start, cal_end],
        "zero_suspicious": sorted(zero_suspicious, key=lambda x: (-x["trading_days"], x["code"])),
        "zero_normal": len(zero_normal),
        "zero_normal_reasons": _count_reasons(zero_normal),
        "interior_holes": interior,
    }


def _slice_trading_days(trading_days: Sequence[str], s: str, e: str) -> List[str]:
    """按闭区间切出交易日（二分，避免逐基金线性过滤整条日历）。"""
    import bisect

    lo = bisect.bisect_left(trading_days, s)
    hi = bisect.bisect_right(trading_days, e)
    return list(trading_days[lo:hi])


def _plus_minus_days(day: str, delta: int) -> Tuple[str, str]:
    from datetime import date, timedelta

    d = date.fromisoformat(day)
    return (d - timedelta(days=delta)).isoformat(), (d + timedelta(days=delta)).isoformat()


def _day_gap(a: str, b: str) -> int:
    from datetime import date

    return abs((date.fromisoformat(a) - date.fromisoformat(b)).days)


def _count_reasons(rows: List[dict]) -> Dict[str, int]:
    out: Dict[str, int] = defaultdict(int)
    for r in rows:
        out[r.get("reason", "?")] += 1
    return dict(out)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="缓存覆盖区间完整性扫描（只读）")
    ap.add_argument("--base", default=str(DEFAULT_BASE), help="缓存根目录（默认 data/cache）")
    ap.add_argument(
        "--namespace", action="append", choices=list(NAMESPACES), help="只扫指定命名空间，可重复"
    )
    ap.add_argument("--top", type=int, default=20, help="报告里最多列出多少条目")
    ap.add_argument("--json", dest="json_path", help="把完整结果写到该 JSON 文件")
    args = ap.parse_args(argv)

    base = Path(args.base)
    namespaces = tuple(args.namespace) if args.namespace else NAMESPACES
    result = scan(base, namespaces)

    print("=" * 72)
    print(f"缓存完整性扫描（只读） base={base} namespaces={namespaces}")
    print("=" * 72)
    print(f"扫描 parquet        : {result['scanned']}")
    print(f"有覆盖区间的标的     : {result.get('with_intervals', 0)}")
    print(f"无覆盖区间（未采集） : {result.get('no_metadata', 0)}")
    if result.get("calendar"):
        print(f"交易日历范围         : {result['calendar'][0]} ~ {result['calendar'][1]}")

    zr = result["zero_suspicious"]
    print(f"\n[A] 覆盖区间内零记录")
    print(f"    正常（区间落在首条记录之前/末条记录之后）: {result['zero_normal']} 处 {result['zero_normal_reasons']}")
    print(f"    可疑（区间落在有数据的时段内却零记录）    : {len(zr)} 处")
    for row in zr[: args.top]:
        print(
            f"      {row['namespace']:<11} {row['code']:<8} dims={row['dims']} "
            f"区间={row['interval'][0]}~{row['interval'][1]} 含交易日={row['trading_days']} "
            f"首/末记录={row['first_date']}/{row['last_date']}"
        )
    if len(zr) > args.top:
        print(f"      ... 另有 {len(zr) - args.top} 处")

    ih = result["interior_holes"]
    total_missing = sum(x["missing_days"] for x in ih)
    print(f"\n[B] 内部空洞（两侧都有数据、中间缺失）: {len(ih)} 只基金 / 共缺 {total_missing} 个交易日")
    print("    注意：上游确实存在「某些基金某天不报份额」的情况，空洞未必是缓存缺陷；")
    print("    用 market_have/market_peak 判断：两者接近说明当天上游整体就缺，缓存是忠实的。")
    for row in ih[: args.top]:
        w = row["worst"]
        print(
            f"      {row['namespace']:<11} {row['code']:<8} dims={row['dims']} "
            f"空洞={row['holes']} 段 缺{row['missing_days']} 天 最长={w[0]}~{w[1]}({w[2]}天) "
            f"当天全市场有数据={row.get('market_have')}/{row.get('market_peak')}"
        )
    if len(ih) > args.top:
        print(f"      ... 另有 {len(ih) - args.top} 只基金")

    if not zr and not ih:
        print("\n结论：未发现可疑的脏覆盖区间（A 可疑、B 均为 0）。")
    elif not zr:
        print("\n结论：无「零记录」型脏区间；[B] 空洞需结合 market_have/market_peak 人工判断。")

    if args.json_path:
        Path(args.json_path).write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\n完整结果已写入 {args.json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
