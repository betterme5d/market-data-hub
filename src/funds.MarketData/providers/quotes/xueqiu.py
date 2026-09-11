import asyncio
import logging
import os
import random
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import httpx

from core import cache as cache_store
from core import config
from core.bar_estimator import MAX_PAGE_SIZE, estimate_bar_count
from core.exceptions import BusinessException
from core.models import UnifiedQuote
from core.routing import translate_standard_symbol
from core.timeseries_cache.manager import TimeSeriesCacheManager
from providers.base import BaseProvider, SourceProbe

logger = logging.getLogger(__name__)


def normalize_xueqiu_symbol(symbol: str) -> str:
    """
    将输入代码转换为雪球专有格式，如 510050.SH -> SH510050，000001.SZ -> SZ000001，
    00700.HK -> 00700，AAPL.US -> AAPL（雪球用裸代码）。
    原生符号（HKHSI / CSI930875 / .SPGSCL / HKDCNY.FX）原样透传。
    """
    mapping = translate_standard_symbol(symbol)
    if "xueqiu" in mapping:
        return mapping["xueqiu"]
    if "." in symbol:
        code, suffix = symbol.upper().split(".", 1)
        if suffix in ["SH", "SS"]:
            return f"SH{code}"
        elif suffix == "SZ":
            return f"SZ{code}"
        elif suffix == "US":
            # 前缀分支会先吞掉 HKHSI.US 这类以 hk 开头的输入，故此处兜底再剥一次 .US
            return code
    return symbol.upper()


class XueqiuKlineProbe(SourceProbe):
    """雪球历史 K 线（v5/stock/chart/kline.json）接口探针。

    实时行情走的是另一条上游接口（realtime/quotec.json），契约不同，接口变更互不牵连，
    故按「1 个上游接口 = 1 个探针」各配一个。本探针复用 K 线取数/解析代码路径
    （_fetch_kline_slice），仅把窗口收到最近几天以保持最轻量；不落任何缓存。
    """

    name = "xueqiu-kline"
    category = "quotes"

    # 上证指数：长期存在且每个交易日都有数据，作为可用性金丝雀最稳
    PROBE_SYMBOL = "SH000001"
    PROBE_DAYS = 10
    # 最新 K 线距今超过该天数即视为停更（>春节/国庆长假窗口，正常不触发）
    STALE_DAYS = 15

    def __init__(self) -> None:
        # 复用同一实例以保留 Cookie 内存缓存（10 分钟），避免每次探测都经网关重取
        self._provider = XueqiuProvider()

    async def probe(self) -> None:
        """取最近一段日 K（单次上游请求），校验解析结构与数值合理性。"""
        today = date.today()
        records = await self._provider._fetch_kline_slice(
            symbol=self.PROBE_SYMBOL,
            slice_start=(today - timedelta(days=self.PROBE_DAYS)).strftime("%Y-%m-%d"),
            slice_end=today.strftime("%Y-%m-%d"),
            period_str="day",
            adjust_type="normal",
        )
        if not records:
            raise RuntimeError(f"{self.name}: {self.PROBE_SYMBOL} 上游返回 0 根 K 线")

        latest = max(records, key=lambda r: r["date"])
        missing = [f for f in ("date", "open", "high", "low", "close", "volume", "amount") if f not in latest]
        if missing:
            raise RuntimeError(f"{self.name}: 解析结果缺字段 {missing}")
        if not latest["close"] or latest["close"] <= 0:
            raise RuntimeError(f"{self.name}: 收盘价非法 {latest['close']!r}")
        if latest["high"] < latest["low"]:
            raise RuntimeError(f"{self.name}: 最高价低于最低价")

        age_days = (today - datetime.strptime(latest["date"], "%Y-%m-%d").date()).days
        if age_days > self.STALE_DAYS:
            raise RuntimeError(
                f"{self.name}: 最新 K 线为 {latest['date']}（距今 {age_days} 天），疑似接口停更"
            )


class XueqiuProvider(BaseProvider):
    def __init__(self, base_url: str = "https://stock.xueqiu.com/", 
                 auth_service_url: Optional[str] = None,
                 user_agent: Optional[str] = None,
                 cache_manager: Optional[TimeSeriesCacheManager] = None):
        self.base_url = base_url.rstrip('/') + '/'
        default_auth_url = os.getenv("AUTH_SERVICE_URL", config.PLAYWRIGHT_GATEWAY_URL)
        self.auth_service_url = (auth_service_url or default_auth_url).rstrip('/')
        self.default_ua = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        self.cache_manager = cache_manager or TimeSeriesCacheManager()
        
        # 缓存的 Cookie 和 User-Agent
        self._cookie: Optional[str] = None
        self._user_agent: str = self.default_ua
        self._cookie_expiry: float = 0.0

    def _safe_float(self, val: Any) -> Optional[float]:
        try:
            if val is None or str(val).strip() == "":
                return None
            f = float(val)
            import math
            return f if math.isfinite(f) else None
        except (ValueError, TypeError):
            return None

    async def _ensure_cookie(self) -> Tuple[str, str]:
        """
        保证雪球 Cookie 的有效性
        """
        import time
        # 如果 Cookie 存在且未过期（如 10 分钟内不重复请求网关），直接返回
        if self._cookie and time.time() < self._cookie_expiry:
            return self._cookie, self._user_agent

        # 0. 共享缓存（Valkey）：跨实例/跨进程复用，避免每次缓存未命中都经网关重取 Cookie
        shared = cache_store.get_xueqiu_auth()
        if shared:
            self._cookie = shared["cookie"]
            self._user_agent = shared.get("userAgent") or self.default_ua
            self._cookie_expiry = time.time() + 600
            logger.debug("Reused Xueqiu Cookie from shared cache.")
            return self._cookie, self._user_agent

        # 1. 尝试从本地 Playwright 鉴权网关获取
        try:
            async with httpx.AsyncClient(timeout=15, verify=False) as client:
                resp = await client.get(f"{self.auth_service_url}/xueqiu/auth")
                if resp.status_code == 200:
                    res_json = resp.json()
                    if res_json.get("success"):
                        self._cookie = res_json.get("cookie")
                        self._user_agent = res_json.get("userAgent", self.default_ua)
                        self._cookie_expiry = time.time() + 600  # 10 分钟进程内缓存
                        cache_store.set_xueqiu_auth(self._cookie, self._user_agent)
                        logger.info("Successfully fetched Xueqiu Cookie from auth service gateway.")
                        return self._cookie, self._user_agent
        except Exception as e:
            logger.warning(f"Fetch Xueqiu Cookie from gateway failed: {e}. Fallback to direct fetching.")

        # 2. 备用方案：直接向雪球官网发起 GET 请求抓取 Cookies
        try:
            async with httpx.AsyncClient(timeout=5, verify=False) as client:
                headers = {"User-Agent": self.default_ua}
                resp = await client.get("https://xueqiu.com/about", headers=headers)
                cookies_list = []
                for name, value in resp.cookies.items():
                    cookies_list.append(f"{name}={value}")

                if cookies_list:
                    self._cookie = "; ".join(cookies_list)
                    self._user_agent = self.default_ua
                    self._cookie_expiry = time.time() + 600
                    cache_store.set_xueqiu_auth(self._cookie, self._user_agent)
                    logger.info("Successfully fetched Xueqiu Cookie directly from xueqiu.com")
                    return self._cookie, self._user_agent
        except Exception as ex:
            logger.error(f"Fetch Xueqiu Cookie directly from xueqiu.com failed: {ex}")
            
        # 3. 终极兜底：如果没有 Cookie，尝试空 Cookie 返回 (雪球部分行情接口有时免 Cookie)
        return self._cookie or "", self._user_agent

    async def get_quotes(self, symbols: List[str], with_depth: bool = False) -> Dict[str, UnifiedQuote]:
        """
        批量获取实时行情 (雪球核心推荐方法)
        """
        if not symbols:
            return {}

        cookie, ua = await self._ensure_cookie()
        headers = {
            "User-Agent": ua,
            "Cookie": cookie,
            "Referer": "https://xueqiu.com/"
        }

        # 转换并拼接 Symbol
        # 雪球的 Symbol 期望是大写，例如 SH510300
        upper_symbols = [s.upper() for s in symbols]
        symbols_str = ",".join(upper_symbols)
        
        import time
        timestamp = int(time.time() * 1000)
        url = f"{self.base_url}v5/stock/realtime/quotec.json?symbol={symbols_str}&_={timestamp}"

        try:
            async with httpx.AsyncClient(timeout=5, verify=False) as client:
                resp = await client.get(url, headers=headers)
                
                # 如果遇到 400/403，说明 Cookie 可能失效，强制清除缓存并重试一次
                if resp.status_code in [400, 403]:
                    logger.warning(f"Xueqiu returned {resp.status_code}, clearing cookie and retrying...")
                    self._cookie = None
                    cache_store.clear_xueqiu_auth()
                    cookie, ua = await self._ensure_cookie()
                    headers["Cookie"] = cookie
                    headers["User-Agent"] = ua
                    resp = await client.get(url, headers=headers)

                resp.raise_for_status()
                res_data = resp.json()
                logger.debug("Xueqiu API returned raw JSON: %s", res_data)
                
                if res_data.get("error_code") != 0:
                    raise Exception(f"Xueqiu API error: {res_data.get('error_description')}")
                
                items = res_data.get("data", [])
                result_dict = {}
                
                for item in items:
                    raw_symbol = item.get("symbol")
                    if not raw_symbol:
                        continue
                    
                    price = self._safe_float(item.get("current")) or 0.0
                    last_close = self._safe_float(item.get("last_close"))
                    
                    # 昨收安全防御
                    if not last_close or last_close <= 0:
                        last_close = price

                    # 涨跌幅百分比直接保存
                    percent = self._safe_float(item.get("percent"))

                    # 换手率百分比直接保存
                    turnover_rate = self._safe_float(item.get("turnover_rate"))

                    # 振幅百分比直接保存
                    amplitude = self._safe_float(item.get("amplitude"))

                    # 确定时间
                    ts = item.get("timestamp")
                    update_time = None
                    if ts:
                        update_time = datetime.fromtimestamp(ts / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
                    
                    date_str = update_time[:10] if update_time else datetime.now().strftime("%Y-%m-%d")

                    # 判断 Exchange
                    market = raw_symbol[:2].upper() if len(raw_symbol) >= 2 else None

                    q = UnifiedQuote(
                        symbol=raw_symbol.lower(),  # 统一小写键返回
                        name=item.get("name") or raw_symbol,
                        date=date_str,
                        price=price,
                        last_close=last_close,
                        open=self._safe_float(item.get("open")),
                        high=self._safe_float(item.get("high")),
                        low=self._safe_float(item.get("low")),
                        close=price,
                        change=self._safe_float(item.get("chg")),
                        percent=percent,
                        volume=self._safe_float(item.get("volume")),
                        amount=self._safe_float(item.get("amount")),
                        turnover_rate=turnover_rate,
                        amplitude=amplitude,
                        update_time=update_time,
                        currency="CNY",
                        exchange=market,
                        security_type="stock",
                        source="xueqiu",
                        timestamp=datetime.utcnow().isoformat()
                    )
                    # 同时用原始大写和小写做 Key 兼容
                    result_dict[raw_symbol.lower()] = q
                    result_dict[raw_symbol.upper()] = q
                    
                return result_dict
        except Exception as e:
            logger.error(f"Xueqiu batch fetch failed for {symbols}: {e}")
            raise e

    async def get_quote(self, symbol: str, with_depth: bool = False) -> Tuple[UnifiedQuote, int]:
        res_dict = await self.get_quotes([symbol], with_depth=with_depth)
        # 兼容大小写 Key 获取
        q = res_dict.get(symbol.lower()) or res_dict.get(symbol.upper())
        if not q:
            raise BusinessException(f"Xueqiu returned empty result for symbol {symbol}")
        return q, 10

    def _parse_kline_items(self, column: List[str], items: List[List[Any]]) -> List[Dict[str, Any]]:
        if not column or not items:
            return []
        col_map = {col: idx for idx, col in enumerate(column)}
        if "timestamp" not in col_map or "close" not in col_map:
            raise Exception("Missing vital column mappings (timestamp, close) from Xueqiu response")

        data_list = []
        for row in items:
            ts = row[col_map["timestamp"]]
            dt = datetime.fromtimestamp(ts / 1000.0)
            dt_str = dt.strftime("%Y-%m-%d")

            open_val = row[col_map["open"]] if "open" in col_map else 0.0
            high_val = row[col_map["high"]] if "high" in col_map else 0.0
            low_val = row[col_map["low"]] if "low" in col_map else 0.0
            close_val = row[col_map["close"]] if "close" in col_map else 0.0
            volume_val = row[col_map["volume"]] if "volume" in col_map else 0.0
            amount_val = row[col_map["amount"]] if "amount" in col_map else 0.0
            chg_val = row[col_map["chg"]] if "chg" in col_map else None
            percent_val = row[col_map["percent"]] if "percent" in col_map else None
            turnoverrate_val = row[col_map["turnoverrate"]] if "turnoverrate" in col_map else None

            data_list.append({
                "date": dt_str,
                "open": self._safe_float(open_val) or 0.0,
                "high": self._safe_float(high_val) or 0.0,
                "low": self._safe_float(low_val) or 0.0,
                "close": self._safe_float(close_val) or 0.0,
                "volume": self._safe_float(volume_val) or 0.0,
                "amount": self._safe_float(amount_val) or 0.0,
                "change": self._safe_float(chg_val),
                "percent": self._safe_float(percent_val),
                "turnover_rate": self._safe_float(turnoverrate_val)
            })
        return data_list

    async def _fetch_kline_slice(
        self,
        symbol: str,
        slice_start: str,
        slice_end: str,
        period_str: str = "day",
        adjust_type: str = "normal",
        on_chunk: Optional[Callable[[List[Dict[str, Any]], str, str], Awaitable[None]]] = None
    ) -> List[Dict[str, Any]]:
        """
        向前倒序自动翻页拉取指定切片区间的雪球 K 线：
        - 单页容量按窗口自适应：取 min(区间根数上界, MAX_PAGE_SIZE)，小窗口不再固定拉满 5000 根；
        - 每获取一页立即触发 on_chunk 写入 Parquet 并合并已覆盖区间；
        - 终止条件：未满页（触底到头）/ 最早日期 <= slice_start（已回到窗口左界）/
          单页装满且估算未被上限截断（窗口根数上界已被一页取完）；
        - 翻页间隙随机休眠 2~5 秒防风控。
        """
        upper_symbol = normalize_xueqiu_symbol(symbol)

        # 单页根数按窗口自适应：区间根数上界即覆盖窗口所需的最大根数，超过 MAX_PAGE_SIZE 才翻页
        est = estimate_bar_count(slice_start, slice_end, period_str)
        if est == 0:
            logger.info(f"Xueqiu KLine slice {slice_start}~{slice_end} contains no weekday, skip fetching.")
            return []
        page_count = min(est, MAX_PAGE_SIZE)

        end_dt = datetime.strptime(slice_end, "%Y-%m-%d")
        current_end_ts = int(datetime(end_dt.year, end_dt.month, end_dt.day, 23, 59, 59).timestamp() * 1000)

        all_records: List[Dict[str, Any]] = []
        seen_dates = set()
        page = 0
        max_pages = 50
        current_page_end = slice_end

        while page < max_pages:
            page += 1
            url = f"{self.base_url}v5/stock/chart/kline.json?symbol={upper_symbol}&begin={current_end_ts}&period={period_str}&type={adjust_type}&count=-{page_count}&indicator=kline"
            cookie, ua = await self._ensure_cookie()
            headers = {
                "User-Agent": ua,
                "Cookie": cookie,
                "Referer": "https://xueqiu.com/"
            }

            try:
                async with httpx.AsyncClient(timeout=15, verify=False) as client:
                    resp = await client.get(url, headers=headers)
                    if resp.status_code in [400, 403]:
                        logger.warning("Xueqiu KLine cookie expired, clearing and retrying...")
                        self._cookie = None
                        cache_store.clear_xueqiu_auth()
                        cookie, ua = await self._ensure_cookie()
                        headers["Cookie"] = cookie
                        headers["User-Agent"] = ua
                        resp = await client.get(url, headers=headers)

                    if resp.status_code in [400, 403] and "xq_a_token" not in (cookie or ""):
                        raise BusinessException("Xueqiu KLine API requires a valid 'xq_a_token' cookie. Please make sure the auth_service is running on port 8081.")

                    resp.raise_for_status()
                    res_data = resp.json()
                    if res_data.get("error_code") != 0:
                        raise BusinessException(f"Xueqiu API error: {res_data.get('error_description')}")

                    data = res_data.get("data", {})
                    column = data.get("column", [])
                    items = data.get("item", [])
                    if not items:
                        # 首屏就空必须抛异常，不能返回空：空结果会被缓存管理器记为「该区间已覆盖」，
                        # 之后重跑直接从缓存返回空、永远不再问上游；上层 C# 还会把整段标成"上游无数据"
                        # （实证：160324/161038/511260/512570 雪球各有 1500~2200 根 K 线，却被整段误标）。
                        # 只有翻到后面的页才空，才是正常的"触底回到历史起点"。
                        if page == 1:   # 首次翻页（本函数从 page=1 开始）
                            raise BusinessException(
                                f"Xueqiu KLine returned empty items for {upper_symbol} "
                                f"(slice {slice_start}~{slice_end}); 首屏无数据，按失败处理（不写缓存覆盖、不标上游无）"
                            )
                        logger.debug(
                            f"Xueqiu KLine pages exhausted for {upper_symbol} at {current_end_ts} "
                            f"(slice {slice_start}~{slice_end})"
                        )
                        break

                    parsed_page = self._parse_kline_items(column, items)
                    if not parsed_page:
                        # 解析后为空同样按失败处理（首屏）——例如字段结构变了但 HTTP 仍 200
                        if page == 1:   # 首次翻页（本函数从 page=1 开始）
                            raise BusinessException(
                                f"Xueqiu KLine parsed to empty for {upper_symbol} "
                                f"(slice {slice_start}~{slice_end}); 解析结果为空，按失败处理"
                            )
                        break

                    # 去重并收集新记录
                    new_page_records = [r for r in parsed_page if r["date"] not in seen_dates]
                    for r in new_page_records:
                        seen_dates.add(r["date"])
                    all_records.extend(new_page_records)

                    earliest_date = parsed_page[0]["date"]
                    ts_idx = column.index("timestamp")
                    earliest_ts = items[0][ts_idx]

                    # 本切片是否已取全，三条依据任一成立：
                    #   1) 未满页 —— 已触及上市首日/历史起点底线；
                    #   2) 最早日期 <= slice_start —— 目标区间已全覆盖；
                    #   3) page_count == est —— 估算未被上限截断，装满一页即等于取完整个窗口。
                    # 依据 3 不可省：窗口起始日落在周末/节假日时，最早 K 线日期必然晚于 slice_start，依据 2 命不中。
                    is_last = (
                        len(items) < page_count
                        or earliest_date <= slice_start
                        or page_count == est
                    )

                    # 即时流式落盘：若提供了 on_chunk 回调，立即写入 Parquet 并合并区间元数据
                    if on_chunk and new_page_records:
                        sorted_chunk = sorted(new_page_records, key=lambda x: x["date"])
                        cov_s = slice_start if is_last else sorted_chunk[0]["date"]
                        cov_e = current_page_end
                        # 盘中半日 K 线不固化：区间一旦覆盖到今天，覆盖声明只到昨天——
                        # 否则今天那根会被永久冻结成"已就绪"，收盘后不再刷新（T 日未发布保护由此触发）。
                        today_str = date.today().strftime("%Y-%m-%d")
                        if cov_e >= today_str:
                            cov_e = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
                            if cov_s > cov_e:
                                # 切片仅含今天一天：无跨度可声明，退化为只声明昨天（已被前序切片覆盖）
                                cov_s = cov_e
                        if cov_s <= cov_e:
                            await on_chunk(sorted_chunk, cov_s, cov_e)

                    # 终止判定：三条依据见上方 is_last
                    if is_last:
                        logger.info(
                            f"Xueqiu KLine slice covered for {upper_symbol}: {len(all_records)} bars, "
                            f"earliest {earliest_date} (slice_start {slice_start}, page_count {page_count}, est {est})"
                        )
                        break

                    # 避免时间戳不推进造成死循环
                    if earliest_ts >= current_end_ts:
                        break

                    earliest_dt = datetime.strptime(earliest_date, "%Y-%m-%d")
                    current_page_end = (earliest_dt - timedelta(days=1)).strftime("%Y-%m-%d")
                    current_end_ts = earliest_ts - 1

                    # 翻页安全防风控延时 2-5 秒
                    delay = random.uniform(2.0, 5.0)
                    logger.debug(f"Sleeping {delay:.2f}s before fetching previous page...")
                    await asyncio.sleep(delay)

            except Exception as e:
                logger.error(f"Failed to fetch KLine slice for {upper_symbol} at {current_end_ts}: {e}")
                raise e

        all_records.sort(key=lambda x: x["date"])
        return all_records

    async def get_history(self, symbol: str, period: str = "1mo", interval: str = "1d", 
                          start: Optional[str] = None, end: Optional[str] = None, adj: str = "hfq") -> dict:
        """
        获取雪球历史 K 线（支持自动向前翻页与 Parquet 时序增量持久化缓存）
        """
        # 1. 映射复权类型
        adj_norm = (adj or "normal").lower()
        adjust_type = "normal"
        if adj_norm in ["qfq", "before"]:
            adjust_type = "before"
        elif adj_norm in ["hfq", "after"]:
            adjust_type = "after"

        # 2. 映射 K 线周期
        period_str = "day"
        if interval in ["1d", "day", "daily"]:
            period_str = "day"
        elif interval in ["1wk", "1w", "week", "weekly"]:
            period_str = "week"
        elif interval in ["1mo", "month", "monthly"]:
            period_str = "month"
        elif interval:
            period_str = interval

        # 3. 规范化 start 与 end 日期
        today_str = date.today().strftime("%Y-%m-%d")
        effective_end = end or today_str
        if not start:
            if period == "1d":
                effective_start = (date.today() - timedelta(days=5)).strftime("%Y-%m-%d")
            elif period == "5d":
                effective_start = (date.today() - timedelta(days=10)).strftime("%Y-%m-%d")
            elif period == "1mo":
                effective_start = (date.today() - timedelta(days=35)).strftime("%Y-%m-%d")
            elif period == "3mo":
                effective_start = (date.today() - timedelta(days=100)).strftime("%Y-%m-%d")
            elif period == "1y":
                effective_start = (date.today() - timedelta(days=370)).strftime("%Y-%m-%d")
            elif period == "max":
                effective_start = "1990-01-01"
            else:
                effective_start = (date.today() - timedelta(days=35)).strftime("%Y-%m-%d")
        else:
            effective_start = start

        upper_symbol = normalize_xueqiu_symbol(symbol)
        dimensions = {
            "source": "xueqiu",
            "adj": adj_norm,
            "interval": interval or "1d"
        }

        async def fetch_fn(s_slice: str, e_slice: str, on_chunk=None):
            return await self._fetch_kline_slice(
                symbol=upper_symbol,
                slice_start=s_slice,
                slice_end=e_slice,
                period_str=period_str,
                adjust_type=adjust_type,
                on_chunk=on_chunk
            )

        records = await self.cache_manager.get_or_fetch(
            namespace="kline",
            key=upper_symbol,
            start_date=effective_start,
            end_date=effective_end,
            fetch_fn=fetch_fn,
            date_column="date",
            dimensions=dimensions,
            # 缺口合并：跨度装得下一页就只发一次上游请求（请求次数是风控敏感资源）
            coalesce=lambda s, e: estimate_bar_count(s, e, period_str) <= MAX_PAGE_SIZE,
        )

        return {
            "symbol": symbol,
            "data": records
        }
