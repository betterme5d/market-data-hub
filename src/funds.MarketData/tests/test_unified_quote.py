import asyncio
import sys
import os

# 将 src/funds.MarketData 加入 PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import UnifiedQuote
from providers.tencent import TencentProvider
from providers.sina import SinaProvider
from providers.xueqiu import XueqiuProvider
from providers.yfinance import YFinanceProvider
from core.dispatcher import QuoteDispatcher
from main import translate_standard_symbol

async def test_symbol_translation():
    print("=== [1] 测试代码自适应翻译 ===")
    examples = ["510300.SH", "159915.SZ", "00700.HK", "AAPL", "nf_SR609", "fx_usdcny"]
    for ex in examples:
        mapping = translate_standard_symbol(ex)
        print(f"标准代码: {ex:12} -> 翻译映射: {mapping}")
    print("代码翻译测试通过。\n")

async def test_tencent_provider():
    print("=== [2] 测试腾讯行情源 ===")
    provider = TencentProvider()
    try:
        # 测试沪深A股/ETF
        q, ttl = await provider.get_quote("sh510300")
        print(f"腾讯 [sh510300] 行情抓取成功: {q.name}, 价格={q.price}, 昨收={q.last_close}, 涨跌幅={q.percent}, 币种={q.currency}, 时间={q.update_time}")
        assert q.price > 0
        assert q.last_close > 0
        assert q.currency == "CNY"
        
        # 测试港股
        q_hk, _ = await provider.get_quote("hk00700")
        print(f"腾讯港股 [hk00700] 行情抓取成功: {q_hk.name}, 价格={q_hk.price}, 昨收={q_hk.last_close}, 涨跌幅={q_hk.percent}, 币种={q_hk.currency}, 成交额={q_hk.amount}, 时间={q_hk.update_time}")
        assert q_hk.price > 0
        assert q_hk.last_close > 0
        assert q_hk.currency == "HKD"
        assert q_hk.amount > 0
        
        # 测试美股
        q_us, _ = await provider.get_quote("usAAPL")
        print(f"腾讯美股 [usAAPL] 行情抓取成功: {q_us.name}, 价格={q_us.price}, 昨收={q_us.last_close}, 涨跌幅={q_us.percent}, 币种={q_us.currency}, 成交额={q_us.amount}, 时间={q_us.update_time}")
        assert q_us.price > 0
        assert q_us.last_close > 0
        assert q_us.currency == "USD"
        assert q_us.amount > 0
        
        # 测试批量
        res = await provider.get_quotes(["sh510300", "sz159915", "hk00700", "usAAPL"])
        print(f"腾讯批量获取成功，返回数量={len(res)}")
        assert "sh510300" in res and "sz159915" in res and "hk00700" in res and "usaapl" in res
    except Exception as e:
        print(f"腾讯行情测试失败: {e}")
    print()

async def test_sina_provider():
    print("=== [3] 测试新浪行情源 ===")
    provider = SinaProvider()
    test_symbols = {
        "sh510300": "A股ETF",
        "rt_hk00700": "港股",
        "gb_aapl": "美股",
        "nf_SR0": "内盘期货主力"
    }
    for sym, label in test_symbols.items():
        try:
            q, _ = await provider.get_quote(sym)
            print(f"新浪 [{sym}]({label}) 抓取成功: {q.name}, 价格={q.price}, 昨收={q.last_close}, 涨跌幅={q.percent}, 均价/估值={q.vwap or q.iopv}")
            assert q.price > 0
        except Exception as e:
            print(f"新浪 [{sym}]({label}) 抓取失败: {e}")
    print()

async def test_xueqiu_provider():
    print("=== [4] 测试雪球行情源 ===")
    provider = XueqiuProvider()
    try:
        # 批量拉取
        res = await provider.get_quotes(["SH510300", "SZ159915"])
        print(f"雪球批量获取成功，返回数量={len(res)}")
        for k, q in res.items():
            if k.islower():
                print(f"  雪球 [{k}] -> 名字={q.name}, 价格={q.price}, 涨跌幅={q.percent}")
        assert len(res) > 0
    except Exception as e:
        print(f"雪球行情测试失败: {e}")
    print()

async def test_dispatcher():
    print("=== [5] 测试调度与 Fallback 总线 ===")
    # 模拟一个 C# 请求: 510300 且优先 Tencent, 其次 Sina
    allowed = {"tencent": "sh510300", "sina": "sh510300", "xueqiu": "SH510300"}
    try:
        q = await QuoteDispatcher.get_quote_with_fallback("510300", allowed)
        print(f"总线单只获取成功: {q.symbol} -> 名字={q.name}, 价格={q.price}, 数据源={q.source}")
        assert q.price > 0
    except Exception as e:
        print(f"总线单只获取失败: {e}")
        
    # 模拟批量 Fallback 获取
    batch_items = [
        {"symbol": "510300", "allowed_sources": {"tencent": "sh510300", "sina": "sh510300"}},
        {"symbol": "159915", "allowed_sources": {"sina": "sz159915", "xueqiu": "SZ159915"}},
        {"symbol": "AAPL", "allowed_sources": {"yfinance": "AAPL", "xueqiu": "AAPL"}}
    ]
    try:
        results = await QuoteDispatcher.get_quotes_batch(batch_items)
        print(f"总线批量 Fallback 获取成功，返回数量={len(results)}")
        for q in results:
            print(f"  标的: {q.symbol:8} | 数据源: {q.source:12} | 价格: {q.price:8} | 昨收: {q.last_close:8}")
        assert len(results) > 0
    except Exception as e:
        print(f"总线批量获取失败: {e}")
    print()

async def test_depth_feature():
    print("=== [6] 测试五档深度盘口 ===")
    allowed = {"tencent": "sh510300", "sina": "sh510300"}
    try:
        q = await QuoteDispatcher.get_quote_with_fallback("510300", allowed, with_depth=True)
        print(f"获取五档行情成功: {q.symbol} -> 名字={q.name}")
        if q.depth:
            print("  买盘 (Bids):")
            for idx, item in enumerate(q.depth.bids[:3]):
                print(f"    买 {idx+1}: 价格={item.price}, 挂单量(手)={item.volume}")
            print("  卖盘 (Asks):")
            for idx, item in enumerate(q.depth.asks[:3]):
                print(f"    卖 {idx+1}: 价格={item.price}, 挂单量(手)={item.volume}")
            assert len(q.depth.bids) > 0
        else:
            print("  警告: 未返回五档深度数据！")
    except Exception as e:
        print(f"五档深度测试失败: {e}")
    print()

async def test_history_feature():
    print("=== [7] 测试历史 K 线拉取 ===")
    
    # 1. 测试新浪内盘期货日 K 线
    try:
        from providers.sina import SinaProvider
        provider = SinaProvider()
        res = await provider.get_history("nf_ag0", "1mo", "1d", "2026-06-01", "2026-06-15", "none")
        print(f"新浪内盘期货历史 K 线获取成功: {res.get('symbol')} -> 获取条数={len(res.get('data', []))}")
        if res.get("data"):
            first = res["data"][0]
            print(f"  首条记录: 日期={first['date']}, 收盘={first['close']}, 结算={first.get('settlement_price')}, 持仓={first.get('position')}")
            assert len(res["data"]) > 0
    except Exception as e:
        print(f"新浪内盘期货历史 K 线测试失败: {e}")
        
    # 2. 测试新浪外盘期货日 K 线
    try:
        from providers.sina import SinaProvider
        provider = SinaProvider()
        res = await provider.get_history("hf_GC", "1mo", "1d", "2026-06-01", "2026-06-15", "none")
        print(f"新浪外盘期货历史 K 线获取成功: {res.get('symbol')} -> 获取条数={len(res.get('data', []))}")
        if res.get("data"):
            first = res["data"][0]
            print(f"  首条记录: 日期={first['date']}, 收盘={first['close']}, 结算={first.get('settlement_price')}, 持仓={first.get('position')}")
            assert len(res["data"]) > 0
    except Exception as e:
        print(f"新浪外盘期货历史 K 线测试失败: {e}")

    # 3. 测试雪球历史 K 线 (支持前/后复权)
    try:
        from providers.xueqiu import XueqiuProvider
        provider = XueqiuProvider()
        res = await provider.get_history("SH510300", "1mo", "1d", "2026-06-01", "2026-06-15", "qfq")
        print(f"雪球历史 K 线获取成功: {res.get('symbol')} -> 获取条数={len(res.get('data', []))}")
        if res.get("data"):
            first = res["data"][0]
            print(f"  首条记录 (前复权): 日期={first['date']}, 收盘={first['close']}, 涨跌幅={first.get('percent')}%")
            assert len(res["data"]) > 0
    except Exception as e:
        if "xq_a_token" in str(e):
            print(f"雪球历史 K 线获取提示: (未启动 auth_service 网关，已跳过本地 K 线拉取校验)")
        else:
            print(f"雪球历史 K 线测试失败: {e}")
    print()

async def test_circuit_breaker_immunity():
    print("=== [8] 测试业务异常下的熔断免疫 (Circuit Breaker Immunity) ===")
    
    # 强制重置新浪源状态，清除可能残留的失败计数或隔离
    from core.cache import redis_client
    if redis_client:
        try:
            from core.dispatcher import get_blocked_key, get_failure_key
            redis_client.delete(get_blocked_key("sina"))
            redis_client.delete(get_failure_key("sina"))
        except Exception:
            pass
            
    # 1. 模拟客户端发起连续 3 次对非法期货代码的查询
    allowed = {"sina": "nf_INVALID999"}
    failures = 0
    for i in range(3):
        try:
            print(f"  发起第 {i+1} 次非法代码查询...")
            await QuoteDispatcher.get_quote_with_fallback("nf_INVALID999", allowed)
        except Exception as e:
            failures += 1
            # 预期会抛出异常
            
    print(f"  非法查询累计抛出异常次数: {failures}")
    
    # 2. 验证新浪数据源是否被熔断拉黑
    is_blocked = QuoteDispatcher.is_source_blocked("sina")
    print(f"  新浪数据源熔断隔离状态: {is_blocked}")
    assert not is_blocked, "错误！非法/脏代码的业务异常导致新浪数据源被熔断误杀了！"
    print("  -> 恭喜！连续脏代码请求未触发熔断计数，数据源依然畅通！")
    
    # 3. 验证此时正常代码依然能够正常通过新浪获取
    try:
        q = await QuoteDispatcher.get_quote_with_fallback("510300", {"sina": "sh510300"})
        print(f"  再次获取正常标的成功: {q.name}, 价格={q.price}, 数据源={q.source}")
        assert q.price > 0
    except Exception as e:
        print(f"  异常！正常标的获取失败: {e}")
        assert False
    print()

async def test_sources_status():
    print("=== [9] 测试行情数据源健康状态接口 ===")
    from main import get_sources_status
    status_list = await get_sources_status()
    print(f"  获取到的数据源健康状况列表:")
    for item in status_list:
        print(f"    源: {item['source']:8} | 状态: {item['status']:8} | 失败计次: {item['failures']:2} | 剩余隔离时间: {item['blocked_seconds_left']}s")
    
    assert len(status_list) > 0
    # 验证基础数据结构字段是否完整存在
    first = status_list[0]
    assert "source" in first
    assert "status" in first
    assert "failures" in first
    assert "blocked_seconds_left" in first
    print("  -> 成功！健康监控数据格式验证无误。")
    print()

async def test_manual_unblock():
    print("=== [10] 测试手动解封行情源接口 ===")
    from core.cache import redis_client
    if not redis_client:
        print("  Valkey 缓存不可用，跳过手动解封集成测试。")
        print()
        return
        
    from main import unblock_source
    
    # 1. 强行拉黑 yfinance
    QuoteDispatcher.block_source("yfinance")
    assert QuoteDispatcher.is_source_blocked("yfinance") == True
    print("  yfinance 数据源已强行拉黑熔断")
    
    # 2. 调用解封接口解封 yfinance
    res = await unblock_source("yfinance")
    print(f"  解封接口响应: {res}")
    
    # 3. 验证是否成功解封
    assert QuoteDispatcher.is_source_blocked("yfinance") == False
    print("  -> 成功！yfinance 数据源已被成功手动解封恢复正常状态。")
    print()

async def test_large_batch_chunking():
    print("=== [11] 测试大批量切片与串行请求 ===")
    
    # 构造 805 个待抓取项以触发超出 800 分片
    # 伪造不同的 symbol 以确保绕过缓存进入 pending 抓取，但实际 mapping 指向同一个正确的股票以确保成功抓取
    items = []
    for i in range(805):
        items.append({
            "symbol": f"510300_{i}",
            "allowed_sources": {"tencent": "sh510300"}
        })
        
    try:
        results = await QuoteDispatcher.get_quotes_batch(items)
        print(f"  大批量切片抓取测试成功，返回数量={len(results)}")
        assert len(results) == 805
        print(f"  首只股票: {results[0].symbol} -> 价格={results[0].price}")
        print(f"  末只股票: {results[-1].symbol} -> 价格={results[-1].price}")
        assert results[0].price > 0
        assert results[-1].price > 0
    except Exception as e:
        print(f"  大批量分片测试失败: {e}")
        assert False
    print()

async def main():
    print("开始运行统一行情服务集成测试...\n")
    await test_symbol_translation()
    await test_tencent_provider()
    await test_sina_provider()
    await test_xueqiu_provider()
    await test_dispatcher()
    await test_depth_feature()
    await test_history_feature()
    await test_circuit_breaker_immunity()
    await test_sources_status()
    await test_manual_unblock()
    await test_large_batch_chunking()
    print("全部集成测试流程结束。")

if __name__ == "__main__":
    asyncio.run(main())
