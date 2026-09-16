#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""退市库同步 **CLI**（内部运维入口）。

为什么是 CLI 而不是 HTTP 接口
-----------------------------
同步会下载上百份公告 PDF 打上游（首次全量 4~5 分钟），误触代价高；
而本服务**没有任何鉴权机制**，暴露成 HTTP 等于任何人都能触发。
所以同步入口刻意不进 OpenAPI，日常由后台任务（`delist_sync_loop`，
每天 03:00）自动跑，手动回填/排障走本脚本。

用法（容器内执行，`src/funds.MarketData` 以 bind mount 挂在 /app）
------------------------------------------------------------------
    docker exec funds_marketdata python scripts/sync_delist.py              # 增量
    docker exec funds_marketdata python scripts/sync_delist.py --full        # 全量回填
    docker exec funds_marketdata python scripts/sync_delist.py --market SH   # 只跑沪市
    docker exec funds_marketdata python scripts/sync_delist.py --query 560650

退出码：0 成功；1 有失败。
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import Optional, Sequence

# 让 `python scripts/sync_delist.py` 能 import 到项目内模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from providers.funds.delist.provider import DelistProvider  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sync_delist")


def _print_result(result: dict) -> None:
    print("\n同步结果")
    print("-" * 60)
    if result.get("skipped"):
        print(f"  跳过：{result.get('reason')}")
        return
    for market, r in (result.get("markets") or {}).items():
        cursors = r.get("cursors") or {}
        print(f"  {market}: 翻页 {r.get('pages')} 页，合并 {r.get('merged')} 条")
        for kw, cur in cursors.items():
            print(f"      水位 {kw} = {cur}")
    for w in result.get("warnings") or []:
        print(f"  [告警] {w}")
    print(f"  库内记录数：{result.get('count')}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="场内基金退市库同步（内部运维入口，非 HTTP 接口）")
    ap.add_argument("--full", action="store_true",
                    help="清掉水位线做全量回填（需下载 200+ 份 PDF，约 4~5 分钟）")
    ap.add_argument("--market", choices=["SH", "SZ"], default=None,
                    help="只同步指定市场；默认沪深都跑")
    ap.add_argument("--query", metavar="CODE", default=None,
                    help="同步后顺便按代码查一次并打印（便于验证）")
    args = ap.parse_args(argv)

    if args.full:
        logger.warning("--full 会清空水位线重新拉全量，耗时约 4~5 分钟")

    provider = DelistProvider()
    markets = [args.market] if args.market else None
    result = asyncio.run(provider.sync(full=args.full, markets=markets))
    _print_result(result)

    if args.query:
        rec = provider.lookup(args.query)
        print(f"\n查询 {args.query}:")
        if rec:
            print(f"  退市日 = {rec.get('delist_date')}")
            print(f"  最后运作日 = {rec.get('last_operation_date')}")
            print(f"  公告日 = {rec.get('announced_at')} | 来源 = {rec.get('source')}")
        else:
            print("  无记录（该基金未退市或尚未同步到）")

    return 1 if result.get("warnings") else 0


if __name__ == "__main__":
    sys.exit(main())
