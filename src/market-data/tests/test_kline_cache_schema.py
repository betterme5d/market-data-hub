# -*- coding: utf-8 -*-
"""D12：K 线缓存口径统一（不迁移存量）。

背景：同一个 "kline" 命名空间原本有两套口径——
- 旧 `XueqiuProvider.get_history`：维度 `{adj: normal|before|after, interval}`、key 用雪球码 `SZ002092`；
- `KLineProvider.get_kline`：维度 `{source, adjust: none|qfq|hfq, period}`、key 用标准码 `002092.SZ`。
两套口径互相看不到对方的缓存：同一份 K 线既重复回源、又占双份磁盘。

定案（用户口径）：统一到 KLineProvider 口径，**不做存量目录迁移**——旧口径目录不再被读取，
那部分区间按"未覆盖"重新采集即可（旧目录原样保留，不删不改）。
"""
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from core.timeseries_cache.manager import TimeSeriesCacheManager
from providers.quotes.kline_provider import KLineProvider
from providers.quotes.xueqiu import XueqiuProvider

BAR = {
    "date": "2026-01-02",
    "open": 6.0,
    "high": 6.1,
    "low": 5.9,
    "close": 6.05,
    "volume": 1000.0,
    "amount": 6050.0,
    "change": 0.05,
    "percent": 0.83,
    "turnover_rate": 0.0,
}



@pytest.mark.asyncio
async def test_history_and_kline_provider_share_one_cache_slot(tmp_path: Path):
    """两条取数路径必须落在同一个缓存 slot：XueqiuProvider 写、KLineProvider 直接命中。"""
    cache_manager = TimeSeriesCacheManager(base_dir=tmp_path)
    xq = XueqiuProvider(cache_manager=cache_manager)

    calls = []

    async def fake_slice(
        symbol, slice_start, slice_end, period_str="day", adjust_type="normal", on_chunk=None
    ):
        calls.append((symbol, slice_start, slice_end, period_str, adjust_type))
        return [dict(BAR, date=slice_start)]

    with patch.object(xq, "_fetch_kline_slice", side_effect=fake_slice):
        res = await xq.get_history(
            "002092.SZ", period="1mo", interval="1d",
            start="2026-01-02", end="2026-01-05", adj="hfq",
        )

    assert res["symbol"] == "002092.SZ"
    assert len(calls) == 1
    # 上游仍必须用雪球自己的代码格式与原生复权值，只有缓存口径归一
    assert calls[0][0] == "SZ002092"
    assert calls[0][3:] == ("day", "after")

    expected = (
        tmp_path / "kline" / "adjust=hfq" / "period=day" / "source=xueqiu" / "002092.SZ.parquet"
    )
    assert expected.exists(), f"落地路径不符：{[str(p) for p in tmp_path.rglob('*.parquet')]}"
    assert not (tmp_path / "kline" / "adj=after").exists(), "不得再写旧口径目录"
    meta = json.loads(
        (expected.parent / "002092.SZ.meta.json").read_text(encoding="utf-8")
    )
    assert meta["dimensions"] == {"source": "xueqiu", "adjust": "hfq", "period": "day"}

    # 反向读取：KLineProvider 命中同一份缓存，回源即失败
    kline = KLineProvider(base_dir=tmp_path)
    with patch.object(kline, "_fetch_from_source", side_effect=AssertionError("不应回源")):
        resp = await kline.get_kline(
            "002092.SZ", "2026-01-02", "2026-01-05", adjust="hfq", period="day"
        )

    assert resp.count == 1
    assert resp.items[0].date == "2026-01-02"


def _write_legacy_cache(base: Path) -> Path:
    """旧口径目录（不迁移）：kline/adj=normal/interval=day/source=xueqiu/SZ002092.*"""
    legacy_dir = base / "kline" / "adj=normal" / "interval=day" / "source=xueqiu"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / "SZ002092.parquet").write_bytes(b"legacy-parquet")
    (legacy_dir / "SZ002092.meta.json").write_text(
        json.dumps(
            {
                "key": "SZ002092",
                "namespace": "kline",
                "dimensions": {"source": "xueqiu", "adj": "normal", "interval": "day"},
                "intervals": [["2026-01-02", "2026-01-05"]],
                "date_column": "date",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return legacy_dir


@pytest.mark.asyncio
async def test_legacy_cache_shape_is_ignored_not_migrated(tmp_path: Path):
    """定案：不迁移存量。旧口径目录不再被读取（该区间按未覆盖重新采集），且不删不改旧文件。"""
    legacy_dir = _write_legacy_cache(tmp_path)

    kline = KLineProvider(base_dir=tmp_path)
    calls = []

    async def fake_fetch(**kwargs):
        calls.append(kwargs)
        return [dict(BAR, date=kwargs["start_date"])]

    with patch.object(kline, "_fetch_from_source", side_effect=fake_fetch):
        resp = await kline.get_kline(
            "002092.SZ", "2026-01-02", "2026-01-05", adjust="none", period="day"
        )

    assert calls, "旧口径缓存不再被读取 → 必须回源按未覆盖重新采集"
    assert resp.count == 1
    # 新口径落盘，旧目录原样保留
    assert (
        tmp_path / "kline" / "adjust=none" / "period=day" / "source=xueqiu" / "002092.SZ.parquet"
    ).exists()
    assert (legacy_dir / "SZ002092.parquet").read_bytes() == b"legacy-parquet"
    assert (legacy_dir / "SZ002092.meta.json").exists()
