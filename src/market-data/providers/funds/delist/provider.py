# -*- coding: utf-8 -*-
"""场内基金终止上市取数编排（业务门面）。

只走**交易所公告**两条通道，不依赖任何第三方行情/资讯站：
- 沪市：上交所 `commonQuery` → 公告 PDF 正文
- 深市：深交所 `api/search/content` → `docpubjsonurl` JSON 正文

流程：检索 → 剔提示性取正式公告 → 逐条取正文 → 抽代码与日期。
检索在 Source，日期/代码抽取在 `parser`（纯函数），串联在这里。

两侧口径差异（别互相套用）：
- 沪市关键词要 `终止上市 ∪ 摘牌`；深市只要 `终止上市`（「摘牌」实测 0 条）
- 沪市时间范围**留空 = 全量**；深市要**毫秒时间戳**
- **沪市有 `SECURITY_CODE`；深市返回里没有代码**，只能从正文「证券代码：XXXXXX」抽
- 深市正文是缩略版，通常没有「最后运作日 / 清算」，属正常
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from core.pacing import polite_delay
from providers.base import SourceProbe
from providers.funds.delist.store import DelistKnownCodes, DelistStore, DelistSyncState
from providers.funds.delist.parser import (
    extract_security_code,
    is_formal_announcement,
    parse_announcement,
)
from providers.funds.delist.snapshot import (
    DelistSnapshotStore,
    detect_candidates,
    load_cached_result,
    save_cached_result,
)
from providers.funds.delist.sse_bulletin import SseFundBulletinSource
from providers.funds.delist.szse_bulletin import SzseFundBulletinSource

logger = logging.getLogger(__name__)

#: 沪市召回关键词。只搜「终止上市」会漏掉 519xxx 那批「摘牌」公告（实测 51 条）
SH_KEYWORDS = ("终止上市", "摘牌")
#: 深市召回关键词。「摘牌」在深市实测 0 条
SZ_KEYWORDS = ("终止上市",)


@dataclass
class DelistRecord:
    """一只场内基金的终止上市信息。"""

    code: str                                     # 6 位代码（深市从正文抽，可能为空）
    market: str                                   # SH / SZ
    announced_at: Optional[str] = None            # 公告日
    title: str = ""
    source: str = ""                              # sse-bulletin / szse-bulletin
    announcement_url: Optional[str] = None        # 公告原文，便于回溯
    delist_date: Optional[str] = None             # 终止上市日（主字段）
    last_operation_date: Optional[str] = None
    register_date: Optional[str] = None
    suspend_date: Optional[str] = None
    warn: Optional[str] = None                    # 解析告警 / 失败原因

    def to_dict(self) -> Dict:
        return asdict(self)


class DelistProvider:
    """终止上市取数编排：串联沪深两个 Source 与解析器。"""

    def __init__(
        self,
        sse: Optional[SseFundBulletinSource] = None,
        szse: Optional[SzseFundBulletinSource] = None,
        store: Optional[DelistStore] = None,
        state: Optional[DelistSyncState] = None,
    ):
        self.sse = sse or SseFundBulletinSource()
        self.szse = szse or SzseFundBulletinSource()
        self.store = store or DelistStore()
        self.state = state or DelistSyncState()
        # 串行化同步任务：并行跑会重复拉上游、还会互相覆盖水位线
        self._sync_lock = asyncio.Lock()

    async def collect(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[DelistRecord]:
        """收集沪深两市场内基金的终止上市信息。

        :param start_date / end_date: 公告日期窗口（`YYYY-MM-DD`）；None = 全量
        :param limit: 最多处理多少只（沪市每只要下一份 PDF，全量约 200 只）
        """
        candidates = await self._collect_candidates(start_date, end_date)
        if limit:
            candidates = candidates[:limit]

        records: List[DelistRecord] = []
        for c in candidates:
            records.append(await self._resolve(c))
            await polite_delay()

        # 同一代码可能被多个关键词命中（沪市「终止上市」与「摘牌」有交集）
        return self._dedupe(records)

    async def collect_cached(self, force: bool = False, **kw) -> List[DelistRecord]:
        """全量采集 + 结果缓存（默认 24h 有效）。

        全量要下 200+ 份 PDF，不该每次调用都重跑；退市数据一天最多变几只，
        缓存一天足够。历史回填或需要最新时用 `force=True`。
        """
        if not force:
            cached = load_cached_result()
            if cached:
                logger.info(f"delist result served from cache ({len(cached)} 条)")
                return [DelistRecord(**r) for r in cached]
        records = await self.collect(**kw)
        save_cached_result([r.to_dict() for r in records])
        return records

    # ------------------------------------------------------------------ 内部

    # ---------------------------------------------------------------- 增量同步

    async def sync(self, full: bool = False, markets: Optional[List[str]] = None) -> Dict:
        """增量同步到本地库：只拉上次水位之后的公告，逐页入库。

        增量标记（持久化在 `DelistSyncState`）：
        - SH：`SSEDATE`（公告日期）→ 下次 `START_DATE = cursor`
        - SZ：`docpubtime`（毫秒）  → 下次 `time = cursor`（已实测可按发布时间过滤）

        断点续传：状态里记了 `page`；上次没跑完（`done=false`）时从该页继续，
        不整批重拉。每页处理完**立即**落盘并推进水位线，崩溃最多重跑一页。

        并发：`asyncio.Lock` 保证同时只有一个同步任务（否则两个任务会重复拉、
        还会互相覆盖水位线）。

        :param full: True 时清掉水位线做全量回填（历史数据用，慢）
        :param markets: 只同步指定市场（CLI 单独重试某侧用）；None = 沪深都跑
        """
        if self._sync_lock.locked():
            return {"skipped": True, "reason": "已有同步任务在运行"}
        async with self._sync_lock:
            result: Dict = {"markets": {}, "warnings": []}
            for market in (markets or ("SH", "SZ")):
                if full:
                    self.state.clear_prefix(market)
                try:
                    result["markets"][market] = await self._sync_market(market)
                except Exception as e:                    # noqa: BLE001
                    msg = f"{market} 同步失败: {e}"
                    logger.error(f"delist sync failed: {msg}")
                    result["warnings"].append(msg)
            result["count"] = len(self.store.load_all())
            return result

    async def _sync_market(self, market: str) -> Dict:
        """同步单个市场。

        水位线按 **(market, keyword)** 独立存储 —— 沪市「终止上市」与「摘牌」
        是两条独立的召回流，日期分布不同，共用 cursor 会互相截断。
        """
        keywords = SH_KEYWORDS if market == "SH" else SZ_KEYWORDS
        page_size = self.sse.page_size if market == "SH" else self.szse.page_size
        cursors: Dict[str, object] = {}
        pages_done = merged = 0

        for kw in keywords:
            key = f"{market}:{kw}"
            st = self.state.get(key)
            resumed = bool(st) and not st.get("done", True)
            cursor = st.get("cursor")
            page = int(st.get("page") or 1) if resumed else 1
            self.state.save(key, cursor=cursor, page=page, done=False)

            while True:
                rows, total = await self._search_page(market, kw, page, cursor)
                if not rows:
                    break
                records = []
                for r in rows:
                    if not is_formal_announcement(r.get("title") or ""):
                        continue
                    rec = await self._resolve(self._to_candidate(market, r))
                    await polite_delay()
                    if rec.code:
                        records.append(rec.to_dict())
                merged += self.store.merge_many(records)   # 每页立即落盘

                new_cursor = self._max_cursor(market, rows, cursor)
                page += 1
                pages_done += 1
                # 先推进水位线再判断是否结束 —— 崩溃时最多重跑一页
                self.state.save(key, cursor=new_cursor, page=page, done=False)
                cursor = new_cursor
                if total is not None and pages_done * page_size >= total:
                    break
                if len(rows) < page_size:
                    break

            self.state.save(key, cursor=cursor, page=1, done=True)
            cursors[kw] = cursor

        return {"pages": pages_done, "merged": merged, "cursors": cursors}

    async def _search_page(self, market, kw, page, cursor):
        if market == "SH":
            return await self.sse.search_page(kw, page_no=page,
                                              start_date=cursor or "")
        ms = cursor if cursor else None
        return await self.szse.search_page(kw, page=page, start_date=_ms_to_date(ms))

    @staticmethod
    def _max_cursor(market, rows, fallback):
        if market == "SH":
            days = [r.get("day") for r in rows if r.get("day")]
            return max(days) if days else fallback
        mss = [r.get("published_ms") for r in rows if r.get("published_ms")]
        return max(mss) if mss else fallback

    @staticmethod
    def _to_candidate(market: str, r: Dict) -> Dict:
        if market == "SH":
            return {"code": r.get("code"), "market": "SH", "day": r.get("day"),
                    "title": r.get("title") or "", "source": "sse-bulletin",
                    "url": r.get("pdf_url"), "key": r.get("code") or ""}
        return {"code": "", "market": "SZ", "day": r.get("day"),
                "title": r.get("title") or "", "source": "szse-bulletin",
                "url": r.get("json_url"), "key": (r.get("title") or "")[:80]}

    async def scan_xueqiu(self) -> Dict:
        """雪球全量状态扫描：补上交易所公告覆盖不到的退市基金（如 501023）。

        扫描对象 = 当前挂牌列表 ∪ 历史累积代码 ∪ 本地库记录。
        命中的 status=3 且库里没有的，入库并标记 `pending_review`，
        `delist_date` 留空 —— 雪球给不了终止上市日，需人工补。

        :return: {"scanned": 扫描数, "delisted": 命中数, "new": 新入库数}
        """
        from providers.funds.delist.xueqiu_status import XueqiuStatusSource

        known = DelistKnownCodes()
        targets = await self._scan_targets(known)

        hits = await XueqiuStatusSource().scan(targets)
        new_records = []
        for h in hits:
            if self.store.get(h["code"]):
                continue                       # 库里已有（多半是交易所通道抓到的）
            new_records.append({
                "code": h["code"], "market": h["market"],
                "source": "xueqiu", "pending_review": True,
                "delist_date": None,
                "warn": "雪球判定退市但交易所无终止上市公告，日期需人工补",
            })
        merged = self.store.merge_many(new_records)
        return {"scanned": len(targets), "delisted": len(hits), "new": merged}

    async def _scan_targets(self, known: DelistKnownCodes) -> List[Dict[str, str]]:
        """扫描对象：当前挂牌列表 ∪ 历史累积 ∪ 本地库记录。"""
        from providers.exchanges.sse import SseFundListSource
        from providers.exchanges.szse import SzseFundListSource

        targets: Dict[str, str] = {}
        for market, source in (("SH", SseFundListSource), ("SZ", SzseFundListSource)):
            try:
                rows = await source().fetch_funds()
            except Exception as e:                        # noqa: BLE001
                logger.warning(f"scan targets: {market} 列表拉取失败: {e}")
                continue
            for r in rows:
                code = str(r.get("fund_code") or "").strip().zfill(6)
                if code:
                    targets[code] = market

        # 历史累积（含已退市、已从列表消失的）
        for code in known.load():
            if code and code not in targets:
                targets[code] = "SH" if code[0] in "5" else "SZ"
        # 本地库记录
        for code, rec in self.store.load_all().items():
            targets.setdefault(code, rec.get("market") or
                               ("SH" if code[0] in "5" else "SZ"))

        known.add(targets.keys())
        return [{"code": c, "market": m} for c, m in targets.items()]

    def lookup(self, code: str) -> Optional[Dict]:
        """传代码直接返回退市信息；库里没有则返回 None（调用方转成空响应）。"""
        return self.store.get(code)

    async def collect_incremental(self) -> Dict:
        """增量采集：先用列表差集定位候选，再只查这些代码。

        全量要下 200+ 份 PDF，不适合每天跑；真实退市每天最多 1~2 只，
        所以日常应该走这条路。全量（`collect()`）留作历史回填。

        :return: {"candidates": [...], "warnings": [...], "records": [...]}
                 warnings 非空时 records 可能为空 —— 宁可不出数，也不出脏数
        """
        candidates, warnings = await detect_candidates(DelistSnapshotStore())
        records: List[DelistRecord] = []
        if candidates:
            records = await self._resolve_candidates(candidates)
        return {"candidates": candidates, "warnings": warnings,
                "records": [r.to_dict() for r in records]}

    async def _resolve_candidates(self, candidates: List[Dict]) -> List[DelistRecord]:
        """只查给定代码的公告。沪市可按代码过滤；深市无代码过滤，只能本地匹配。"""
        records: List[DelistRecord] = []
        sh_codes = [c["code"] for c in candidates if c["market"] == "SH"]
        sz_codes = {c["code"] for c in candidates if c["market"] == "SZ"}

        for code in sh_codes:
            for kw in SH_KEYWORDS:
                rows = await self.sse.search(kw, security_code=code)
                formal = [r for r in rows if is_formal_announcement(r["title"])]
                if not formal:
                    continue
                r = min(formal, key=lambda x: x["day"] or "")
                records.append(await self._resolve({
                    "code": code, "market": "SH", "day": r["day"],
                    "title": r["title"], "source": "sse-bulletin",
                    "url": r["pdf_url"], "key": code}))
                await polite_delay()
                break

        if sz_codes:
            for kw in SZ_KEYWORDS:
                rows = await self.szse.search(kw)
                for r in rows:
                    title = r.get("title") or ""
                    if not is_formal_announcement(title):
                        continue
                    rec = await self._resolve({
                        "code": "", "market": "SZ", "day": r["day"], "title": title,
                        "source": "szse-bulletin", "url": r.get("json_url"), "key": title[:80]})
                    await polite_delay()
                    if rec.code in sz_codes:
                        records.append(rec)
        return self._dedupe(records)

    async def _collect_candidates(self, start_date, end_date) -> List[Dict]:
        """检索两侧公告，剔提示性后按「代码/标题」保留一条正式公告。"""
        pool: Dict[str, Dict] = {}

        for kw in SH_KEYWORDS:
            for r in await self.sse.search(kw, start_date=start_date or "",
                                           end_date=end_date or ""):
                self._offer(pool, key=r["code"] or r["title"], market="SH",
                            day=r["day"], title=r["title"] or "",
                            source="sse-bulletin", url=r["pdf_url"], code=r["code"])

        for kw in SZ_KEYWORDS:
            for r in await self.szse.search(kw, start_date=start_date,
                                            end_date=end_date):
                # 深市返回无代码，先用标题做 key，取正文后再补
                self._offer(pool, key=(r.get("title") or "")[:80], market="SZ",
                            day=r["day"], title=r.get("title") or "",
                            source="szse-bulletin", url=r.get("json_url"), code=None)

        return sorted(pool.values(), key=lambda x: (x["day"] or ""), reverse=True)

    def _offer(self, pool, key, market, day, title, source, url, code):
        """同一事件发两条（提示性 + 正式），只保留正式且最早的一条。"""
        if not key or not is_formal_announcement(title):
            return
        cur = pool.get(key)
        if cur and (cur["day"] or "") <= (day or ""):
            return
        pool[key] = {"key": key, "market": market, "day": day, "title": title,
                     "source": source, "url": url, "code": code}

    async def _resolve(self, c: Dict) -> DelistRecord:
        """取正文 → 抽代码与日期。"""
        rec = DelistRecord(
            code=c["code"] or "", market=c["market"], announced_at=c["day"],
            title=c["title"], source=c["source"], announcement_url=c["url"],
        )
        try:
            if c["market"] == "SH":
                text = await self.sse.fetch_pdf_text(c["url"])
            else:
                text = await self.szse.fetch_content(c["url"])
        except Exception as e:                            # noqa: BLE001
            # 单只失败不中断整体，但必须留下原因 —— 不能静默当"没退市"
            rec.warn = f"正文获取失败: {e}"
            logger.warning(f"delist content failed [{c['market']}] {c['key']}: {e}")
            return rec

        if not c["code"]:
            rec.code = extract_security_code(text) or ""
        parsed = parse_announcement(text)
        rec.delist_date = parsed.get("delist_date")
        rec.last_operation_date = parsed.get("last_operation_date")
        rec.register_date = parsed.get("register_date")
        rec.suspend_date = parsed.get("suspend_date")
        rec.warn = parsed.get("warn")
        return rec

    @staticmethod
    def _dedupe(records: List[DelistRecord]) -> List[DelistRecord]:
        """按 code 去重；无代码的（深市没抽到）按标题保留，避免整条丢掉。"""
        best: Dict[str, DelistRecord] = {}
        no_code: List[DelistRecord] = []
        for r in records:
            if not r.code:
                no_code.append(r)
                continue
            cur = best.get(r.code)
            # 已有记录若已抽到终止上市日，优先保留
            if cur and (cur.delist_date or not r.delist_date):
                continue
            best[r.code] = r
        out = sorted(best.values(), key=lambda r: (r.announced_at or ""), reverse=True)
        return out + no_code


def _next_run_delay(hour: int, minute: int = 0) -> float:
    """距下一个**固定时刻**还有多少秒（用于每日定点触发而非固定间隔）。

    用固定时刻而不是「每 N 小时」：重启不会让同步时间漂移，
    日志时间点可预期，也便于避开交易时段。
    """
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def delist_sync_loop(stop_event: asyncio.Event) -> None:
    """后台增量同步循环（在 `main.py` 的 lifespan 里起，模式同 `probe_loop`）。

    **纯内部触发，不暴露 HTTP 接口** —— 同步会下载上百份 PDF 打上游，
    不适合做成外部可调用的入口；手动回填走 CLI（`scripts/sync_delist.py`）。

    调度：每天固定时刻（`DELIST_SYNC_AT_HOUR`，默认 **03:00**，避开交易时段）。

    启动：默认跑一次补上停机期间的新公告，但受 `DELIST_SYNC_MIN_INTERVAL_HOURS`
    （默认 1h）限制 —— 容器频繁重启时不至于反复打上游。

    失败：单次失败只记日志，不退出循环；已入库数据不受影响
    （水位线每页推进，崩溃最多重跑一页）。任务整体挂掉由 `DelistSyncProbe` 告警。
    """
    hour = int(os.getenv("DELIST_SYNC_AT_HOUR", "3"))
    on_startup = os.getenv("DELIST_SYNC_ON_STARTUP", "1") not in ("0", "false", "False")
    min_interval_h = float(os.getenv("DELIST_SYNC_MIN_INTERVAL_HOURS", "1"))

    async def _run_once(tag: str) -> None:
        try:
            result = await delist_provider.sync()
            logger.info(
                f"delist sync[{tag}] done: 库内 {result.get('count')} 条"
                + (f", warnings={result['warnings']}" if result.get("warnings") else ""))
        except Exception as e:                            # noqa: BLE001
            logger.error(f"delist sync[{tag}] failed: {e}")

    if on_startup:
        await asyncio.sleep(5)      # 让应用先完成装配，避免启动期抢资源
        last = delist_provider.state.latest_updated()
        if last and (datetime.now() - last) < timedelta(hours=min_interval_h):
            logger.info(f"delist sync[startup] skipped: 距上次同步不足 {min_interval_h}h")
        else:
            await _run_once("startup")

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=_next_run_delay(hour))
            return                                        # 收到停止信号
        except asyncio.TimeoutError:
            pass
        await _run_once("scheduled")


class DelistSyncProbe(SourceProbe):
    """同步任务**新鲜度**探针 —— 上游接口探针覆盖不到的那一半。

    `SseFundBulletinProbe` / `SzseFundBulletinProbe` 只能证明「接口还活着」，
    证明不了「同步任务还在跑」。任务若因异常退出、卡死或调度失效，
    接口全是绿的但库会一直陈旧 —— 所以单独用同步时间戳兜一层。

    判据：`delist_state.json` 里**最旧**那条流的 `updated_at` 超过
    `DELIST_SYNC_MAX_AGE_HOURS`（默认 48h）没动即判失败。

    **必须用最旧而不是最新**：每条关键词流（如沪市「终止上市」「摘牌」）各存
    一个水位线。若只看最新的那条，某一条流卡死而其他流正常时取 max 会被掩盖，
    探针全绿但数据已经残缺。

    为什么是 48h 不是 24h：调度是每天一次，允许错过一轮（如停机一天）再报警，
    避免重启就误报。
    """

    name = "delist-sync"
    category = "funds"

    async def probe(self) -> None:
        max_age_h = float(os.getenv("DELIST_SYNC_MAX_AGE_HOURS", "48"))
        oldest = delist_provider.state.oldest_updated()
        if oldest is None:
            raise RuntimeError("退市库从未同步过（无水位线），同步任务可能未启动")
        age = datetime.now() - oldest
        if age > timedelta(hours=max_age_h):
            raise RuntimeError(
                f"退市同步已停滞 {age.total_seconds() / 3600:.1f}h，"
                f"超过阈值 {max_age_h}h；最旧的流同步于 {oldest.isoformat()}")


def _ms_to_date(ms: Optional[int]) -> Optional[str]:
    """深市水位线是毫秒时间戳，转成 YYYY-MM-DD 给 `search_page` 用。"""
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d")


delist_provider = DelistProvider()
