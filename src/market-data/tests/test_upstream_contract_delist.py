# -*- coding: utf-8 -*-
"""退市取数源的真实上游契约测试。

默认跳过（需真实网络 + 沪深站点可达）：
    docker exec market-data sh -lc "cd /app && OTEL_SDK_DISABLED=true \
        python -m pytest -q -m integration tests/test_upstream_contract_delist.py"

校验的是**上游契约本身**：接口还在、参数语义没变、返回结构没变。
这是页面私有接口（无 SLA），上游一改版就会静默失效，故用这些用例兜底。
"""
import pytest

from providers.funds.delist.parser import (
    extract_security_code,
    is_formal_announcement,
    parse_announcement,
)
from providers.funds.delist.provider import SH_KEYWORDS, SZ_KEYWORDS, DelistProvider
from providers.funds.delist.sse_bulletin import SseFundBulletinSource
from providers.funds.delist.szse_bulletin import SzseFundBulletinSource

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_sse_bulletin_search_returns_full_history():
    """留空时间范围应拿到全量（数据最早 2006 年，不是只有近几年）。"""
    rows = await SseFundBulletinSource().search("终止上市")
    assert rows, "上交所公告检索为空"
    years = {r["day"][:4] for r in rows if r.get("day")}
    assert min(years) <= "2012", f"未取到历史数据，最早年份={min(years)}"
    assert all(r.get("pdf_url") for r in rows)


@pytest.mark.asyncio
async def test_sse_zhapai_keyword_is_separate_bucket():
    """「摘牌」是与「终止上市」不同的召回桶，不能只搜一个。"""
    zhaipai = await SseFundBulletinSource().search("摘牌")
    assert zhaipai, "「摘牌」检索为空"
    titles = {r["title"] for r in zhaipai}
    assert any("终止上市" not in t for t in titles), "「摘牌」桶应含不含『终止上市』字样的公告"


@pytest.mark.asyncio
async def test_szse_bulletin_search_and_content():
    """深交所列表可搜，且 docpubjsonurl 能取到正文（免 PDF 的关键）。"""
    source = SzseFundBulletinSource()
    rows = await source.search("终止上市")
    assert rows, "深交所公告检索为空"
    target = next((r for r in rows if r.get("json_url")), None)
    assert target, "结果缺少 docpubjsonurl"
    content = await source.fetch_content(target["json_url"])
    assert len(content) > 20, "正文过短"


@pytest.mark.asyncio
async def test_szse_real_announcement_yields_delist_date():
    """端到端：深交所真实公告能抽出终止上市日。"""
    source = SzseFundBulletinSource()
    rows = await source.search("终止上市")
    formal = [r for r in rows if is_formal_announcement(r.get("title") or "")]
    assert formal, "没有正式公告（全是提示性？）"
    hit = 0
    for r in formal[:5]:
        if not r.get("json_url"):
            continue
        content = await source.fetch_content(r["json_url"])
        if parse_announcement(content).get("delist_date"):
            hit += 1
    assert hit, "前 5 条正式公告一条都没抽出终止上市日，正则或接口可能已漂移"


@pytest.mark.asyncio
async def test_provider_collects_both_markets():
    """编排层：沪深都要有输出，且结构完整。

    这里**不**断言 delist_date —— 抽到的几条里若某只 PDF 偶发下载失败
    （上交所会限流返回 0 字节），断言就会假红。
    日期正确性由 `test_sync_and_lookup_by_code` 用固定代码 560650 覆盖。
    """
    records = await DelistProvider().collect(limit=4)
    assert records, "编排层无输出"
    assert {r.market for r in records}, "没有市场标记"
    for r in records:
        assert r.source in ("sse-bulletin", "szse-bulletin"), f"未知来源 {r.source}"
        assert r.announcement_url, f"{r.market} 记录缺少公告地址"
        # 取正文失败必须留痕，不能静默当"没退市"
        if r.delist_date is None:
            assert r.warn, f"{r.market} {r.code} 无日期却无告警，会误导调用方"


@pytest.mark.asyncio
async def test_szse_code_is_extracted_from_body():
    """深交所返回没有代码字段，必须从正文「证券代码：XXXXXX」抽。"""
    source = SzseFundBulletinSource()
    rows = await source.search("终止上市")
    target = next((r for r in rows if r.get("json_url")), None)
    assert target
    content = await source.fetch_content(target["json_url"])
    code = extract_security_code(content)
    assert code and len(code) == 6, f"未从正文抽到 6 位代码: {code!r}"


@pytest.mark.asyncio
async def test_sh_zhapai_bucket_is_covered():
    """沪市「摘牌」是独立召回桶，编排层必须覆盖（否则漏 51 只）。

    这里只校验关键词配置，不做全量 collect —— 全量要下 200+ 份 PDF，太慢。
    """
    assert "摘牌" in SH_KEYWORDS, "沪市关键词缺少「摘牌」，会漏掉 519xxx 那批公告"
    # 深市相反：「摘牌」实测 0 条，保持单关键词即可
    assert SZ_KEYWORDS == ("终止上市",)


@pytest.mark.asyncio
async def test_sync_and_lookup_by_code():
    """增量同步后，按代码应能直接查到终止上市日。

    注意：首次运行（无水位线）是「全量」，要下 200+ 份 PDF，约 4~5 分钟；
    有水位线后是增量，实测 3~5 秒。
    """
    p = DelistProvider()
    result = await p.sync()
    assert not result.get("skipped"), "同步被并发锁跳过"
    assert result["count"] > 0, "同步后库内为空"

    rec = p.lookup("560650")          # 终止上市 2026-08-24
    assert rec, "560650 应已入库"
    assert rec["delist_date"] == "2026-08-24"
    assert rec["market"] == "SH"

    # 未退市的基金应返回 None（调用方转 found=false，不是错误）
    assert p.lookup("510300") is None


@pytest.mark.asyncio
async def test_second_sync_is_incremental_and_fast():
    """有水位线后二次同步应显著变快（只拉增量）。"""
    p = DelistProvider()
    await p.sync()                    # 确保水位线已建立
    import time
    t = time.time()
    await p.sync()
    assert time.time() - t < 120, "二次同步耗时过长，增量未生效"


@pytest.mark.asyncio
async def test_xueqiu_status_semantics():
    """退市判定必须包含 status=0 / 2，不能只看 3。

    实测 501066=2、501041=0，这些都有交易所终止上市/摘牌公告确认已退市。
    早期版本只认 3，把它们漏掉了。
    """
    from providers.funds.delist.xueqiu_status import (
        DELISTED_STATUSES, XueqiuStatusSource, is_delisted,
    )

    assert DELISTED_STATUSES == frozenset({0, 2, 3}), "退市状态集合被改动"
    assert is_delisted(0) and is_delisted(2) and is_delisted(3)
    assert not is_delisted(1)
    assert not is_delisted(None), "空响应不等于退市（可能只是上游没返回）"

    src = XueqiuStatusSource()
    assert is_delisted(await src.fetch_status("501023", "SH")), \
        "501023 已退出（清盘无交易所公告，正是雪球要补的场景）"
    assert is_delisted(await src.fetch_status("560650", "SH")), "560650 已终止上市"
    assert not is_delisted(await src.fetch_status("510300", "SH")), "510300 在市"


@pytest.mark.asyncio
async def test_xueqiu_status_probe():
    from providers.funds.delist.xueqiu_status import XueqiuStatusProbe
    await XueqiuStatusProbe().probe()


@pytest.mark.asyncio
async def test_snapshot_diff_catches_delisted_fund(tmp_path, monkeypatch):
    """差集要能抓到已退市代码。用 560650（终止上市 2026-08-24）验证。"""
    from providers.funds.delist import snapshot as snap_mod

    monkeypatch.setattr(snap_mod, "CACHE_DIR", tmp_path)
    store = snap_mod.DelistSnapshotStore()

    current = await snap_mod.fetch_current_codes("SH")
    assert len(current) >= snap_mod.MIN_EXPECTED_ROWS["SH"], "列表行数低于下限，上游可能截断"

    # 造快照：现行列表 + 2 只已知已退市的代码
    store.save("SH", sorted(set(current) | {"560650", "519622"}))
    store.save("SZ", await snap_mod.fetch_current_codes("SZ"))

    candidates, warnings = await snap_mod.detect_candidates(store)
    assert not warnings, f"不应有告警: {warnings}"
    codes = {c["code"] for c in candidates}
    assert {"560650", "519622"} <= codes, f"差集未抓到已退市代码: {codes}"


@pytest.mark.asyncio
async def test_snapshot_guard_rejects_mass_disappearance(tmp_path, monkeypatch):
    """一次消失过多 = 上游异常，必须拒绝出数而不是产出大批脏候选。"""
    from providers.funds.delist import snapshot as snap_mod

    monkeypatch.setattr(snap_mod, "CACHE_DIR", tmp_path)
    store = snap_mod.DelistSnapshotStore()
    current = await snap_mod.fetch_current_codes("SH")

    # 注入 200 个假代码，模拟上游列表截断
    store.save("SH", sorted(set(current) | {f"9{i:05d}" for i in range(200)}))
    store.save("SZ", await snap_mod.fetch_current_codes("SZ"))

    candidates, warnings = await snap_mod.detect_candidates(store)
    assert not candidates, "上游异常时不应产出候选"
    assert any("超过阈值" in w for w in warnings), f"缺少阈值告警: {warnings}"
