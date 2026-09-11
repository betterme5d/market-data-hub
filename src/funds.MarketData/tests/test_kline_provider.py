# -*- coding: utf-8 -*-
"""Task 123: KLineProvider 业务门面单元测试 — 严格验证多维 Parquet 物理隔离"""
import pytest
from unittest.mock import AsyncMock, patch
from providers.quotes.kline_provider import KLineProvider, DEFAULT_SOURCE
from core.models import KLineResponse

BARS_QFQ = [{"date": "2026-01-02", "open": 6.0, "high": 6.1, "low": 5.9, "close": 6.05,
              "volume": 1000.0, "amount": 6000.0}]
BARS_HFQ = [{"date": "2026-01-02", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.20,
              "volume": 500.0, "amount": 5000.0}]
BARS_TC  = [{"date": "2026-01-02", "open": 6.0, "high": 6.1, "low": 5.9, "close": 6.99,
              "volume": 1000.0, "amount": 6000.0}]


@pytest.fixture
def tmp(tmp_path):
    return str(tmp_path)


@pytest.mark.asyncio
async def test_default_source_is_xueqiu():
    assert DEFAULT_SOURCE == "xueqiu"


@pytest.mark.asyncio
async def test_unsupported_source_raises(tmp):
    p = KLineProvider(base_dir=tmp)
    with pytest.raises(ValueError, match="unsupported source"):
        await p.get_kline("002092.SZ", "2026-01-01", "2026-01-31", source="bad_src")


@pytest.mark.asyncio
async def test_invalid_symbol_raises(tmp):
    p = KLineProvider(base_dir=tmp)
    with pytest.raises(ValueError, match="symbol"):
        await p.get_kline("!!bad!!", "2026-01-01", "2026-01-31")


@pytest.mark.asyncio
async def test_date_order_raises(tmp):
    p = KLineProvider(base_dir=tmp)
    with pytest.raises(ValueError, match="start_date"):
        await p.get_kline("002092.SZ", "2026-12-31", "2026-01-01")


@pytest.mark.asyncio
async def test_get_kline_returns_kline_response(tmp):
    p = KLineProvider(base_dir=tmp)
    with patch.object(p, "_fetch_from_source", AsyncMock(return_value=BARS_QFQ)):
        resp = await p.get_kline("002092.SZ", "2026-01-01", "2026-01-31", source="xueqiu", adjust="qfq")
    assert isinstance(resp, KLineResponse)
    assert resp.code == "002092.SZ"
    assert resp.source == "xueqiu"
    assert resp.adjust == "qfq"
    assert resp.count == 1
    assert resp.items[0].close == 6.05


@pytest.mark.asyncio
async def test_default_adjust_is_none_not_qfq(tmp):
    """默认复权必须是不复权：qfq 值随最新价重算，与增量缓存相冲"""
    p = KLineProvider(base_dir=tmp)
    with patch.object(p, "_fetch_from_source", AsyncMock(return_value=BARS_QFQ)):
        resp = await p.get_kline("002092.SZ", "2026-01-01", "2026-01-31", source="xueqiu")
    assert resp.adjust == "none"


@pytest.mark.asyncio
async def test_cache_hit_skips_upstream(tmp):
    """缓存命中时，_fetch_from_source 第二次不应被调用"""
    p = KLineProvider(base_dir=tmp)
    mock = AsyncMock(return_value=BARS_QFQ)
    with patch.object(p, "_fetch_from_source", mock):
        await p.get_kline("002092.SZ", "2026-01-01", "2026-01-31", source="xueqiu", adjust="qfq")
        await p.get_kline("002092.SZ", "2026-01-01", "2026-01-31", source="xueqiu", adjust="qfq")
    assert mock.call_count == 1


@pytest.mark.asyncio
async def test_adjust_isolation(tmp):
    """qfq 与 hfq 数据严格物理隔离，互不覆盖"""
    p = KLineProvider(base_dir=tmp)
    with patch.object(p, "_fetch_from_source", AsyncMock(return_value=BARS_QFQ)):
        r_qfq = await p.get_kline("002092.SZ", "2026-01-01", "2026-01-31", source="xueqiu", adjust="qfq")
    with patch.object(p, "_fetch_from_source", AsyncMock(return_value=BARS_HFQ)):
        r_hfq = await p.get_kline("002092.SZ", "2026-01-01", "2026-01-31", source="xueqiu", adjust="hfq")
    assert r_qfq.items[0].close == 6.05
    assert r_hfq.items[0].close == 10.20
    assert r_qfq.adjust == "qfq"
    assert r_hfq.adjust == "hfq"


@pytest.mark.asyncio
async def test_source_isolation(tmp):
    """xueqiu 与 tencent 同一标的同一 adjust 数据严格物理隔离"""
    p = KLineProvider(base_dir=tmp)
    with patch.object(p, "_fetch_from_source", AsyncMock(return_value=BARS_QFQ)):
        r_xq = await p.get_kline("002092.SZ", "2026-01-01", "2026-01-31", source="xueqiu", adjust="qfq")
    with patch.object(p, "_fetch_from_source", AsyncMock(return_value=BARS_TC)):
        r_tc = await p.get_kline("002092.SZ", "2026-01-01", "2026-01-31", source="tencent", adjust="qfq")
    assert r_xq.items[0].close == 6.05
    assert r_tc.items[0].close == 6.99
    assert r_xq.source == "xueqiu"
    assert r_tc.source == "tencent"


@pytest.mark.asyncio
async def test_gap_slices_coalesced_into_single_upstream_call(tmp):
    """两段缓存之间的缺口合并为一次上游调用：日线跨度远小于 5000 根时不重复发请求。"""
    p = KLineProvider(base_dir=tmp)
    mock = AsyncMock(return_value=BARS_QFQ)
    with patch.object(p, "_fetch_from_source", mock):
        await p.get_kline("510500.SH", "2026-01-01", "2026-03-31", source="xueqiu", adjust="qfq")
        await p.get_kline("510500.SH", "2026-05-01", "2026-06-30", source="xueqiu", adjust="qfq")
        assert mock.call_count == 2

        mock.reset_mock()
        await p.get_kline("510500.SH", "2026-02-01", "2026-08-31", source="xueqiu", adjust="qfq")

    # 缺口 [04-01, 04-30] 与 [07-01, 08-31] 合并成一次请求（不合并时是 2 次）
    assert mock.call_count == 1
    assert mock.call_args.kwargs["start_date"] == "2026-04-01"
    assert mock.call_args.kwargs["end_date"] == "2026-08-31"


@pytest.mark.asyncio
async def test_wide_merged_span_keeps_slices_separate(tmp):
    """合并后超过单页（>5000 工作日）时不合并，避免把两次请求撑成三次。"""
    p = KLineProvider(base_dir=tmp)
    mock = AsyncMock(return_value=BARS_QFQ)
    with patch.object(p, "_fetch_from_source", mock):
        for s, e in [("2000-01-01", "2000-12-31"), ("2013-01-01", "2013-12-31"), ("2025-01-01", "2025-12-31")]:
            await p.get_kline("510050.SH", s, e, source="xueqiu", adjust="qfq")

        mock.reset_mock()
        await p.get_kline("510050.SH", "2000-01-01", "2025-12-31", source="xueqiu", adjust="qfq")

    # 两个缺口 [2001-01-01,2012-12-31] 与 [2014-01-01,2024-12-31]：
    # 合并跨度约 24 年（>5000 工作日），故保持逐缺口拉取
    assert mock.call_count == 2
    slices = [(c.kwargs["start_date"], c.kwargs["end_date"]) for c in mock.call_args_list]
    assert slices == [("2001-01-01", "2012-12-31"), ("2014-01-01", "2024-12-31")]


@pytest.mark.asyncio
async def test_kline_fetch_short_circuits_when_source_blocked(tmp_path):
    """雪球被熔断时 K 线必须短路（此前 K 线完全不受熔断约束）。"""
    from unittest.mock import patch

    from core.exceptions import BusinessException
    from providers.quotes.kline_provider import KLineProvider

    provider = KLineProvider(base_dir=tmp_path)
    with patch(
        "core.dispatcher.QuoteDispatcher.is_source_blocked", return_value=True
    ), patch("providers.quotes.xueqiu.XueqiuProvider._fetch_kline_slice") as mock_fetch:
        with pytest.raises(BusinessException):
            await provider.get_kline(
                "510300.SH", "2026-01-01", "2026-01-05", source="xueqiu"
            )

    assert mock_fetch.call_count == 0, "熔断后不得再打上游"


@pytest.mark.asyncio
async def test_kline_fetch_records_health_sample(tmp_path):
    """K 线业务调用必须写入健康统计（此前缺失，导致该源 idle 判定失真）。"""
    from unittest.mock import AsyncMock, patch

    from providers.quotes import xueqiu as xq_mod
    from providers.quotes.kline_provider import KLineProvider

    async def fake_slice(*args, **kwargs):
        return []

    provider = KLineProvider(base_dir=tmp_path)
    with patch.object(
        xq_mod.XueqiuProvider, "_fetch_kline_slice", new_callable=AsyncMock, side_effect=fake_slice
    ), patch("core.health.record_call") as mock_record:
        await provider.get_kline(
            "510300.SH", "2026-01-01", "2026-01-05", source="xueqiu"
        )

    assert mock_record.call_count >= 1
    assert mock_record.call_args.args[0] == "xueqiu"
    assert mock_record.call_args.args[1] is True

@pytest.mark.asyncio
async def test_kline_guard_excludes_business_exception_from_failures(monkeypatch):
    """D14：业务型异常（上游有应答但无数据/代码不存在/明确拒绝）不算源故障。

    口径与 QuoteDispatcher 一致：源可达则记 ok=True；只有系统级异常（网络/解析）才计失败，
    否则一次无效代码就能把整个 xueqiu 源标记成不健康。
    """
    from core import health as health_svc
    from core.exceptions import BusinessException
    from providers.quotes import xueqiu as xq_mod

    calls = []
    monkeypatch.setattr(
        health_svc,
        "record_call",
        lambda source, ok, latency_ms, via="business", error=None: calls.append((source, ok, via)),
    )

    # 与本次修复无关：避免在无 Valkey/DNS 的环境里等连接超时（is_source_blocked 会连 valkey）
    from core.dispatcher import QuoteDispatcher
    monkeypatch.setattr(QuoteDispatcher, "is_source_blocked", staticmethod(lambda source: False))

    with pytest.raises(BusinessException):
        async with xq_mod.kline_guard():
            raise BusinessException("Xueqiu API error: 该代码不存在")

    assert calls == [("xueqiu", True, "business")]


@pytest.mark.asyncio
async def test_kline_guard_counts_system_exception_as_failure(monkeypatch):
    """D14 反面：系统级异常仍必须计入失败（否则熔断/健康面板就失去意义）。"""
    from core import health as health_svc
    from providers.quotes import xueqiu as xq_mod

    calls = []
    monkeypatch.setattr(
        health_svc,
        "record_call",
        lambda source, ok, latency_ms, via="business", error=None: calls.append((source, ok, via)),
    )

    # 与本次修复无关：避免在无 Valkey/DNS 的环境里等连接超时（is_source_blocked 会连 valkey）
    from core.dispatcher import QuoteDispatcher
    monkeypatch.setattr(QuoteDispatcher, "is_source_blocked", staticmethod(lambda source: False))

    with pytest.raises(RuntimeError):
        async with xq_mod.kline_guard():
            raise RuntimeError("connection reset by peer")

    assert calls == [("xueqiu", False, "business")]

@pytest.mark.asyncio
async def test_kline_provider_reuses_single_xueqiu_instance(tmp, monkeypatch):
    """D13：XueqiuProvider 必须复用同一实例，否则实例级 Cookie 缓存（10 分钟）形同虚设，
    每次 K 线请求都可能重新走一次鉴权网关（Valkey 不可用时更明显）。"""
    import contextlib

    from providers.quotes import xueqiu as xq_mod

    created = []
    fetched = []

    class _FakeXueqiu:
        def __init__(self, cache_manager=None):
            created.append(self)
            self.cache_manager = cache_manager

        async def _fetch_kline_slice(self, symbol, slice_start, slice_end, period_str, adjust_type, on_chunk=None):
            fetched.append((slice_start, slice_end))
            return [{"date": slice_start, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
                     "volume": 1.0, "amount": 1.0}]

    @contextlib.asynccontextmanager
    async def _noop_guard():
        yield

    monkeypatch.setattr(xq_mod, "XueqiuProvider", _FakeXueqiu)
    monkeypatch.setattr(xq_mod, "kline_guard", _noop_guard)

    p = KLineProvider(base_dir=tmp)
    await p.get_kline("002092.SZ", "2026-01-01", "2026-01-31")
    await p.get_kline("002092.SZ", "2026-02-01", "2026-02-28")

    assert len(fetched) == 2
    assert len(created) == 1, f"XueqiuProvider 被创建了 {len(created)} 次，Cookie 缓存无法复用"

