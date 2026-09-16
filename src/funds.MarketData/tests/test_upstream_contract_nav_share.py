# -*- coding: utf-8 -*-
"""真实上游契约用例（默认跳过，需显式 `-m integration`）。

存在意义：本轮修复引入了两条**依赖真实报文**的硬约束，离线桩测不出来——
1. N7：`EastmoneySource._require_all_records` 要求 rankhandler 报文必须含 `allRecords`
   （缺失即抛错）。若上游改字段名，全量净值接口会 502 而不是静默半份——
   这是刻意选择的方向，但必须有真实报文用例来第一时间发现。
2. N1：沪市份额「所有已激活分类接口全空 → 不完整」必须**不**误伤正常交易日，
   否则每个交易日都会被反复重取。
"""
from datetime import date, timedelta

import pytest

pytestmark = pytest.mark.integration  # 需要真实外部网络，默认跳过


@pytest.mark.asyncio
async def test_eastmoney_rank_payload_exposes_completeness_metadata():
    """N7：真实 rankhandler 报文必须能解析出 allRecords / allPages（否则接口会 502）。"""
    from providers.funds.eastmoney import _EM_RANK_PAGE_SIZE, EastmoneySource

    source = EastmoneySource()
    for dt in ("fb", "kf"):
        data = await source._fetch_rank_page(dt, 1, _EM_RANK_PAGE_SIZE)
        assert data is not None, f"dt={dt} 请求或 datas 解析失败"
        assert data["all_records"] is not None, f"dt={dt} 报文缺少 allRecords（N7 硬约束被打破）"
        assert data["all_pages"] is not None, f"dt={dt} 报文缺少 allPages"
        assert data["rows"], f"dt={dt} datas 为空"


@pytest.mark.asyncio
async def test_eastmoney_latest_all_nav_end_to_end():
    """N6+N7 端到端：真实调用必须返回全量（历史基线约 2000+ 只 ETF/LOF），且不抛错。"""
    from providers.funds.eastmoney import EastmoneySource

    items = await EastmoneySource().get_latest_all_nav()
    assert len(items) > 1000, f"只返回 {len(items)} 条，疑似判据/过滤把数据砍掉了"
    assert all(it.unit_nav is not None for it in items), "存在单位净值为空的行（N6 口径被绕过）"


@pytest.mark.asyncio
async def test_sse_daily_shares_are_complete_on_a_trading_day():
    """N1：正常交易日必须 complete=True 且条数正常（否则该日会被永久重取）。"""
    from core.calendar import get_exchange_trading_days
    from providers.exchanges.shares.sse import SseShareSource

    today = date.today().strftime("%Y-%m-%d")
    start = (date.today() - timedelta(days=10)).strftime("%Y-%m-%d")
    days = [d["date"] for d in get_exchange_trading_days("CN", start, today) if d.get("is_trading")]
    past = [d for d in days if d < today]
    day = past[-1] if past else (days[-1] if days else today)

    records, complete = await SseShareSource(delay_ms=0).fetch_daily_market_shares_checked(day)
    assert complete is True, f"{day} 被判为不完整（正常交易日被误判 → 会反复重取）"
    assert len(records) > 50, f"{day} 只解析出 {len(records)} 只基金"
