import logging
import asyncio
import time
import json
from typing import List, Dict, Any, Tuple, Optional
from core.exceptions import BusinessException

from core.models import UnifiedQuote
from core.cache import redis_client
from providers.tencent import TencentProvider
from providers.sina import SinaProvider
from providers.xueqiu import XueqiuProvider
from providers.yfinance import YFinanceProvider

logger = logging.getLogger(__name__)

# 初始化各大行情源的 Provider
TENCENT_PROVIDER = TencentProvider()
SINA_PROVIDER = SinaProvider()
XUEQIU_PROVIDER = XueqiuProvider()
YFINANCE_PROVIDER = YFinanceProvider()

PROVIDERS = {
    "tencent": TENCENT_PROVIDER,
    "sina": SINA_PROVIDER,
    "xueqiu": XUEQIU_PROVIDER,
    "yfinance": YFINANCE_PROVIDER
}

# 并发限制信号量
SEMAPHORES = {
    "tencent": asyncio.Semaphore(20),
    "sina": asyncio.Semaphore(20),
    "xueqiu": asyncio.Semaphore(5),
    "yfinance": asyncio.Semaphore(10)
}

# 默认缓存时间 (交易时段 10 秒)
DEFAULT_CACHE_TTL = 10
# 故障熔断隔离时间 (300 秒 = 5 分钟)
CIRCUIT_BREAKER_TTL = 300
# 允许的最大连续错误次数 (触发熔断的阈值)
MAX_FAILURES = 3
# 错误统计的滑动时间窗口 (60秒)
FAILURE_WINDOW = 60

def get_blocked_key(source: str) -> str:
    return f"marketdata:blocked:{source}"

def get_failure_key(source: str) -> str:
    return f"marketdata:failures:{source}"

def get_quote_cache_key(symbol: str) -> str:
    # 统一的扁平缓存 Key
    return f"marketdata:quote:{symbol.lower()}"

class QuoteDispatcher:
    @staticmethod
    def get_source_metrics(source: str) -> dict:
        """
        获取指定源的监控指标，包含熔断状态、失败计次及剩余隔离时间
        """
        if not redis_client:
            return {"source": source, "status": "healthy", "failures": 0, "blocked_seconds_left": 0}
        try:
            blocked_key = get_blocked_key(source)
            failure_key = get_failure_key(source)
            
            is_blocked = redis_client.exists(blocked_key) == 1
            failures_val = redis_client.get(failure_key)
            failures = int(failures_val) if failures_val else 0
            
            blocked_seconds_left = 0
            if is_blocked:
                ttl = redis_client.ttl(blocked_key)
                blocked_seconds_left = max(0, ttl)
                
            return {
                "source": source,
                "status": "blocked" if is_blocked else "healthy",
                "failures": failures,
                "blocked_seconds_left": blocked_seconds_left
            }
        except Exception as e:
            logger.warning(f"Get metrics failed for {source}: {e}")
            return {"source": source, "status": "healthy", "failures": 0, "blocked_seconds_left": 0}

    @staticmethod
    def unblock_source(source: str) -> bool:
        """
        手动将黑名单数据源重新启用（清除隔离状态与失败计次）
        """
        if not redis_client:
            return False
        try:
            blocked_key = get_blocked_key(source)
            failure_key = get_failure_key(source)
            
            # 删除 Redis 对应键以恢复健康
            redis_client.delete(blocked_key)
            redis_client.delete(failure_key)
            logger.warning(f"Manual override: SOURCE '{source}' has been manually unblocked.")
            return True
        except Exception as e:
            logger.error(f"Failed to manually unblock source {source}: {e}")
            return False

    @staticmethod
    def is_source_blocked(source: str) -> bool:
        """
        判断某个行情源是否处于熔断冷却期
        """
        if not redis_client:
            return False
        try:
            return redis_client.exists(get_blocked_key(source)) == 1
        except Exception as e:
            logger.warning(f"Check source blocked status failed for {source}: {e}")
            return False

    @staticmethod
    def block_source(source: str, ttl: int = CIRCUIT_BREAKER_TTL) -> None:
        """
        强制隔离拉黑某个数据源 (触发熔断)
        """
        if not redis_client:
            return
        try:
            redis_client.setex(get_blocked_key(source), ttl, "1")
            logger.warning(f"Circuit breaker triggered: SOURCE '{source}' is blocked for {ttl}s due to cumulative errors.")
        except Exception as e:
            logger.warning(f"Block source {source} failed: {e}")

    @staticmethod
    def record_failure(source: str) -> None:
        """
        记录一次失败。若在 FAILURE_WINDOW (60s) 内失败累计达到 MAX_FAILURES (3次)，则触发熔断。
        """
        if not redis_client:
            return
        try:
            key = get_failure_key(source)
            count = redis_client.incr(key)
            if count == 1:
                # 首次失败，设置滑动过期窗口
                redis_client.expire(key, FAILURE_WINDOW)
            
            if count >= MAX_FAILURES:
                # 累计达标，正式拉黑
                QuoteDispatcher.block_source(source)
                redis_client.delete(key)
            else:
                logger.warning(f"Source '{source}' failure count: {count}/{MAX_FAILURES} within {FAILURE_WINDOW}s.")
        except Exception as e:
            logger.warning(f"Record failure for {source} failed: {e}")

    @staticmethod
    def reset_failures(source: str) -> None:
        """
        当行情源成功获取行情时，重置并清空其失败计数器
        """
        if not redis_client:
            return
        try:
            redis_client.delete(get_failure_key(source))
        except Exception as e:
            logger.warning(f"Reset failures for {source} failed: {e}")

    @staticmethod
    async def get_quote_with_fallback(symbol: str, allowed_sources: Dict[str, str], with_depth: bool = False) -> UnifiedQuote:
        """
        单只行情智能 Fallback 抓取，支持可选的五档深度数据
        """
        # 1. 尝试读 Valkey 缓存 (仅在不带深度数据时使用缓存)
        cache_key = get_quote_cache_key(symbol)
        if not with_depth and redis_client:
            try:
                cached = redis_client.get(cache_key)
                if cached:
                    return UnifiedQuote.model_validate_json(cached)
            except Exception as ce:
                logger.warning(f"Read quote cache failed for {symbol}: {ce}")

        # 2. 依次尝试受授权且未熔断的行情源
        last_exception = None
        for source, source_symbol in allowed_sources.items():
            source = source.lower()
            provider = PROVIDERS.get(source)
            if not provider:
                continue

            # 检查是否熔断
            if QuoteDispatcher.is_source_blocked(source):
                logger.info(f"Skip blocked source '{source}' for symbol '{symbol}'")
                continue

            sem = SEMAPHORES.get(source) or asyncio.Semaphore(5)
            async with sem:
                try:
                    logger.info(f"Fetching '{symbol}' via '{source}' using symbol '{source_symbol}' (with_depth={with_depth})")
                    result_model, ttl = await provider.get_quote(source_symbol, with_depth=with_depth)
                    
                    # 强修补结果的 symbol 为 C# 请求的标准代码
                    result_model.symbol = symbol
                    
                    # 3. 写入缓存并返回 (仅在不带深度数据时缓存)
                    if not with_depth and redis_client:
                        try:
                            redis_client.setex(cache_key, ttl or DEFAULT_CACHE_TTL, result_model.model_dump_json())
                            logger.debug(f"Successfully cached quote for {symbol} to Valkey, ttl={ttl or DEFAULT_CACHE_TTL}")
                        except Exception as c_ex:
                            logger.warning(f"Cache write failed: {c_ex}")
                    
                    # 成功抓取，重置该源的失败计数器
                    QuoteDispatcher.reset_failures(source)
                    return result_model
                except BusinessException as bex:
                    logger.warning(f"Business query empty via '{source}' for '{symbol}' using '{source_symbol}': {bex}")
                    last_exception = bex
                except Exception as ex:
                    logger.error(f"System fetch failed via '{source}' for '{symbol}' using '{source_symbol}': {ex}")
                    last_exception = ex
                    # 递增失败计数器，达到阈值才熔断
                    QuoteDispatcher.record_failure(source)

        blocked_sources = [src for src in allowed_sources.keys() if QuoteDispatcher.is_source_blocked(src)]
        err_msg = f"All allowed sources failed for symbol {symbol}."
        if blocked_sources:
            err_msg += f" (Blocked sources: {blocked_sources})"
        err_msg += f" Last error: {last_exception}"
        raise Exception(err_msg)

    @staticmethod
    async def get_quotes_batch(items: List[Dict[str, Any]], with_depth: bool = False) -> List[UnifiedQuote]:
        """
        批量行情高并发、自适应合并打包 Fallback 调度，支持可选的五档深度数据
        """
        if not items:
            return []

        results_map: Dict[str, UnifiedQuote] = {}
        
        # 1. 批量批量读 Valkey 缓存 (仅在不带深度数据时使用缓存)
        if not with_depth and redis_client:
            try:
                cache_keys = [get_quote_cache_key(item["symbol"]) for item in items]
                cached_values = redis_client.mget(cache_keys)
                for item, cached_val in zip(items, cached_values):
                    if cached_val:
                        try:
                            q = UnifiedQuote.model_validate_json(cached_val)
                            results_map[item["symbol"].lower()] = q
                            logger.debug(f"Valkey cache hit for batch symbol: {item['symbol']}")
                        except Exception:
                            pass
            except Exception as e:
                logger.warning(f"MGET cache failed: {e}")

        # 2. 收集未命中的 Symbol 任务
        pending_items = [item for item in items if item["symbol"].lower() not in results_map]
        if not pending_items:
            # 全部命中缓存，直接按输入顺序排序输出
            return [results_map[item["symbol"].lower()] for item in items if item["symbol"].lower() in results_map]

        # 3. 开始多轮批量抓取调度，直到所有 pending 都被解决或无可用源
        round_num = 1
        while pending_items and round_num <= 3:
            logger.info(f"Batch Dispatch Round {round_num}: Pending count={len(pending_items)}")
            
            source_groups: Dict[str, List[Tuple[Dict[str, Any], str]]] = {}
            unsupported_items = []

            for item in pending_items:
                allowed_sources = item.get("allowed_sources", {})
                
                # 寻找该标的允许的、未熔断的、优先级最高的源
                allocated = False
                for src, src_symbol in allowed_sources.items():
                    src = src.lower()
                    if src in PROVIDERS and not QuoteDispatcher.is_source_blocked(src):
                        source_groups.setdefault(src, []).append((item, src_symbol))
                        allocated = True
                        break
                
                if not allocated:
                    unsupported_items.append(item)

            if not source_groups:
                logger.warning("No unblocked allowed sources found for remaining pending items.")
                break

            # 并发执行各组的批量抓取
            async def fetch_group(source: str, group_list: List[Tuple[Dict[str, Any], str]]) -> Dict[str, UnifiedQuote]:
                provider = PROVIDERS[source]
                sem = SEMAPHORES[source]
                
                symbol_mapping = {src_symbol.lower(): item["symbol"] for item, src_symbol in group_list}
                fetch_symbols = list(symbol_mapping.keys())
                
                async with sem:
                    try:
                        logger.info(f"Group batch fetching {len(fetch_symbols)} symbols via '{source}' (with_depth={with_depth})")
                        
                        if hasattr(provider, "get_quotes"):
                            batch_results = await provider.get_quotes(fetch_symbols, with_depth=with_depth)
                        else:
                            async def fetch_single(single_symbol: str) -> Optional[UnifiedQuote]:
                                try:
                                    q, _ = await provider.get_quote(single_symbol, with_depth=with_depth)
                                    return q
                                except Exception:
                                    return None
                            
                            tasks = [fetch_single(s) for s in fetch_symbols]
                            singles = await asyncio.gather(*tasks)
                            batch_results = {s: q for s, q in zip(fetch_symbols, singles) if q}
                        
                        processed_results = {}
                        for src_sym, q in batch_results.items():
                            std_symbol = symbol_mapping.get(src_sym.lower())
                            if std_symbol:
                                q.symbol = std_symbol
                                processed_results[std_symbol.lower()] = q
                        
                        # 抓取成功，重置失败计数
                        QuoteDispatcher.reset_failures(source)
                        return processed_results
                    except BusinessException as bex:
                        logger.warning(f"Batch fetch business query error via '{source}': {bex}")
                        return {}
                    except Exception as ex:
                        logger.error(f"Batch fetch failed via '{source}': {ex}")
                        # 递增失败计数器，达到阈值才熔断
                        QuoteDispatcher.record_failure(source)
                        return {}

            tasks = [fetch_group(src, g_list) for src, g_list in source_groups.items()]
            group_results_list = await asyncio.gather(*tasks)
            
            round_success_count = 0
            for group_res in group_results_list:
                for std_sym, q in group_res.items():
                    results_map[std_sym] = q
                    round_success_count += 1
                    
                    # 仅在不带深度数据时写入 Redis 缓存
                    if not with_depth and redis_client:
                        try:
                            redis_client.setex(get_quote_cache_key(std_sym), DEFAULT_CACHE_TTL, q.model_dump_json())
                            logger.debug(f"Successfully cached batch quote for {std_sym} to Valkey, ttl={DEFAULT_CACHE_TTL}")
                        except Exception:
                            pass

            logger.info(f"Round {round_num} finished, successfully retrieved {round_success_count} quotes.")
            
            pending_items = [item for item in items if item["symbol"].lower() not in results_map]
            round_num += 1

        final_list = []
        for item in items:
            key = item["symbol"].lower()
            if key in results_map:
                final_list.append(results_map[key])
                
        return final_list
