# -*- coding: utf-8 -*-
"""
基金成立日期取数源与本地文件缓存层。

数据来源（均已人工实测验证，页面引用见各 API docstring 与注释）：
1. 场内交易基金（ETF/封闭式基金）：
   - 网页引用：https://fund.eastmoney.com/data/fbsfundranking.html (东方财富网-数据中心-场内交易基金排行)
   - 底层接口：https://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=fb&ft=all...
   - 字段规则：按逗号切分，第 0 列为基金代码，第 15 列为成立日期 (YYYY-MM-DD)。
2. 开放式基金（LOF/股票/混合/债券/QDII 等）：
   - 网页引用：https://fund.eastmoney.com/data/fundranking.html (东方财富网-数据中心-开放基金排行)
   - 底层接口：https://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all...
   - 字段规则：按逗号切分，第 0 列为基金代码，第 16 列为成立日期 (YYYY-MM-DD)。
3. 单只基金兜底（天天基金 F10 基本概况页）：
   - 网页引用：https://fundf10.eastmoney.com/jbgk_{code}.html (天天基金-基金档案-基本概况)

过滤规则：
根据系统业务场景（场内套利/LOF/ETF），最终写入缓存与返回的数据仅保留在证券交易所（上交所与深交所）
正式上市交易的 ETF/LOF 基金（通过 ListingDateProvider 获取上交所与深交所官方上市清单），
自动滤除 519xxx、110xxx 等虽然以 5 或 1 开头但属于纯场外开放式基金的品种，以及其余 2 万+ 纯场外开放式基金，
保证缓存仅包含场内上市交易品种。

缓存策略：
成立日期为近乎静态数据，更新极低频。采用与 listing_dates.py 一致的策略：
- 按分类（fb / kf）落盘为本地 JSON 文件（data/establish_date_{category}.json）。
- 全量接口校验文件 mtime，超过 24 小时（可配）重新全量拉取上游并覆盖；支持 refresh=true 强制刷新。
- 进程内维持 _mem_cache 字典，单只与全量查询秒级返回。
"""
import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from core import config
from core.filters import (
    clean_fund_code,
    get_exchange_listed_codes as _get_exchange_listed_codes,
    is_target_exchange_fund,
)
from providers.base import SourceProbe

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_REFERER = "https://fund.eastmoney.com/data/fundranking.html"

# rankhandler.aspx 返回 datas 字段中成立日期所在列索引
# dt=fb 场内榜单第 15 列为成立日期；dt=kf 开放基金榜单第 16 列为成立日期
_EM_ESTABLISH_IDX = {"fb": 15, "kf": 16}

# 防截断下限行数（低于此值视为上游反爬或网络截断，拒绝接收半份数据）
_EM_MIN_ROWS = {"fb": 1000, "kf": 15000}

# 文件缓存目录（相对项目工作目录，可在需要时由环境变量覆盖）
CACHE_DIR = Path(os.getenv("ESTABLISH_CACHE_DIR", "data"))

# 全量接口的文件有效期（秒）：超过该秒数视为陈旧，重新拉取，默认 24 小时
STALE_SECONDS = int(os.getenv("ESTABLISH_STALE_SECONDS", str(24 * 3600)))


def _normalize_date(value: Any) -> Optional[str]:
    """上游日期归一化为 ISO(YYYY-MM-DD)；空值/占位符返回 None。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text or text in ("--", "-", "nan", "NaT", "None"):
        return None
    # 纯数字格式 20150925
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    # 处理带斜杠格式 2015/09/25
    if "/" in text:
        parts = text.split("/")
        if len(parts) == 3:
            return f"{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    # 带时间截断 "2004-12-20 00:00:00"
    return text[:10]


def _is_target_fund(code: str, target_codes: Optional[set[str]] = None) -> bool:
    """判定是否为系统关心的场内上市基金（委托给 core.filters.is_target_exchange_fund）。"""
    return is_target_exchange_fund(code, listed_codes=target_codes)


def _extract_json_array(text: str, key: str) -> Optional[list]:
    """从 JS 响应字面量中通过括号匹配截取 key 对应的数组并反序列化。"""
    key_idx = text.find(key + ":")
    if key_idx < 0:
        return None
    start = text.find("[", key_idx)
    if start < 0:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start : i + 1])
                except json.JSONDecodeError as e:
                    logger.error(f"extract array {key} json decode failed: {e}")
                    return None
    return None


class EastmoneyEstablishDateSource:
    """天天基金成立日期取数源（rankhandler.aspx）。

    支持 dt=fb（场内）与 dt=kf（开放式），严格排除 dt=hb（货币基金）。
    """

    def __init__(self):
        self.url = "https://fund.eastmoney.com/data/rankhandler.aspx"
        self.headers = {
            "User-Agent": _UA,
            "Referer": _REFERER,
        }

    def _parse_text(
        self,
        text: str,
        dt: str = "fb",
        min_rows: Optional[int] = None,
        target_codes: Optional[set[str]] = None,
    ) -> List[Dict[str, Optional[str]]]:
        """解析排行响应文本，过滤仅保留目标场内上市基金。"""
        establish_idx = _EM_ESTABLISH_IDX[dt]
        threshold = min_rows if min_rows is not None else _EM_MIN_ROWS[dt]

        datas = _extract_json_array(text, "datas")
        if datas is None:
            raise RuntimeError(f"eastmoney establish rank ({dt}): datas array not found")

        # 校验上游返回的原始行数，防御反爬截断
        if len(datas) < threshold:
            raise RuntimeError(
                f"eastmoney establish rank ({dt}): suspicious truncation ({len(datas)} rows < {threshold})"
            )

        rows: List[Dict[str, Optional[str]]] = []
        for item in datas:
            if not isinstance(item, str):
                continue
            fields = item.split(",")
            if len(fields) <= establish_idx:
                continue
            code = fields[0].strip()
            # 过滤仅保留目标场内上市基金
            if not _is_target_fund(code, target_codes):
                continue
            rows.append({
                "fund_code": code,
                "establish_date": _normalize_date(fields[establish_idx]),
            })
        return rows

    async def _fetch_rows(
        self,
        dt: str = "fb",
        page_size: int = 30000,
        min_rows: Optional[int] = None,
        target_codes: Optional[set[str]] = None,
    ) -> List[Dict[str, Optional[str]]]:
        """拉取指定榜单并解析。"""
        params = {
            "op": "ph",
            "dt": dt,
            "ft": "all",
            "rs": "",
            "gs": "0",
            "sc": "1nzf",
            "st": "desc",
            "pi": "1",
            "pn": str(page_size),
            "v": "0.1234567890",
        }
        async with httpx.AsyncClient(timeout=config.UPSTREAM_TIMEOUT, verify=False) as client:
            resp = await client.get(self.url, params=params, headers=self.headers)
            resp.raise_for_status()
            return self._parse_text(resp.text, dt, min_rows=min_rows, target_codes=target_codes)

    async def fetch_establish_dates(
        self, dt: str, target_codes: Optional[set[str]] = None
    ) -> Dict[str, str]:
        """拉取单榜单全量并转为 {fund_code: establish_date} 映射字典。"""
        if dt not in _EM_ESTABLISH_IDX:
            raise ValueError(f"Unsupported dt: {dt!r} (expected 'fb' or 'kf')")
        rows = await self._fetch_rows(dt, target_codes=target_codes)
        return {r["fund_code"]: r["establish_date"] for r in rows if r.get("establish_date")}


class EastmoneyEstablishDateProbe(SourceProbe):
    """天天基金成立日期 rankhandler.aspx (fb+kf) 的健康探针。"""

    name = "eastmoney-establish-date"
    category = "funds"

    async def probe(self) -> None:
        """语义探针：走 Source 真实取数路径拉取前 100 条（开放基金 0 开头居多，需 100 条确保覆盖 5/1 基金），校验返回并确保日期解析正常。"""
        source = EastmoneyEstablishDateSource()
        for dt in ("fb", "kf"):
            rows = await source._fetch_rows(dt=dt, page_size=100, min_rows=1)
            if not rows:
                raise RuntimeError(f"eastmoney-establish-date probe ({dt}): empty target rows")
            if not any(r.get("establish_date") for r in rows):
                raise RuntimeError(f"eastmoney-establish-date probe ({dt}): no establish_date parsed")


def _cache_file(category: str) -> Path:
    return CACHE_DIR / f"establish_date_{category.lower()}.json"


def _load_cache(category: str) -> Optional[Dict[str, str]]:
    """从本地文件读取缓存；不存在或损坏返回 None。"""
    path = _cache_file(category)
    try:
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {k: v for k, v in data.items()} if isinstance(data, dict) else None
    except Exception as e:
        logger.warning(f"Load establish date cache {path} failed: {e}")
        return None


def _save_cache(category: str, data: Dict[str, str]) -> None:
    """原子写缓存：先写临时文件再 os.replace，避免读写冲突与半写损坏。"""
    path = _cache_file(category)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        logger.error(f"Save establish date cache {path} failed: {e}")


def _is_stale(category: str) -> bool:
    """文件不存在或 mtime 超过 STALE_SECONDS 视为陈旧。"""
    path = _cache_file(category)
    if not path.exists():
        return True
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        return datetime.now() - mtime > timedelta(seconds=STALE_SECONDS)
    except OSError:
        return True


class EstablishDateProvider:
    """基金成立日期编排门面：全量缓存管理 + 单只查询兜底。

    数据严格过滤为在交易所挂牌上市的 ETF / LOF 基金，未在交易所上市的普通开放式基金不入库、不返回。
    """

    CATEGORIES = ("fb", "kf")

    def __init__(self):
        self._mem_cache: Dict[str, Dict[str, str]] = {}
        self._source = EastmoneyEstablishDateSource()

    async def get_exchange_listed_codes(self) -> set[str]:
        """获取上交所与深交所上市基金全量代码集合（委托给 core.filters.get_exchange_listed_codes）。"""
        return await _get_exchange_listed_codes()

    async def _fetch_and_cache(
        self,
        category: str,
        force: bool = False,
        exchange_codes: Optional[set[str]] = None,
    ) -> Dict[str, str]:
        """获取某类别成立日期，未过期走文件/内存，过期或 force 走上游并刷盘。"""
        if not force and not _is_stale(category):
            cached = _load_cache(category)
            if cached is not None:
                self._mem_cache[category] = cached
                return cached

        if exchange_codes is None:
            exchange_codes = await self.get_exchange_listed_codes()

        data = await self._source.fetch_establish_dates(category, target_codes=exchange_codes)
        _save_cache(category, data)
        self._mem_cache[category] = data
        return data

    async def get_all(
        self, category: Optional[str] = None, force: bool = False
    ) -> Dict[str, str]:
        """获取全量成立日期。category 为 None、空或 'all' 时返回 fb 与 kf 合并字典。"""
        exchange_codes = None
        if force or any(_is_stale(cat) for cat in self.CATEGORIES):
            exchange_codes = await self.get_exchange_listed_codes()

        if category is None or category.lower() in ("", "all"):
            merged: Dict[str, str] = {}
            for cat in self.CATEGORIES:
                data = await self._fetch_and_cache(cat, force=force, exchange_codes=exchange_codes)
                merged.update(data)
            return merged

        cat = category.lower()
        if cat not in self.CATEGORIES:
            raise ValueError(f"Unsupported category: {category!r} (expected 'fb', 'kf' or 'all')")
        return await self._fetch_and_cache(cat, force=force, exchange_codes=exchange_codes)

    async def get_one(self, code: str) -> Dict[str, Optional[str]]:
        """单只基金成立日期。

        仅限交易所上市的 ETF/LOF 基金。
        先查内存 → 再查本地文件缓存 → 均未命中时校验交易所名单，在所才打 F10 概况页兜底。
        """
        clean_code = clean_fund_code(code)
        if not clean_code:
            return {"fund_code": code, "establish_date": None}

        # 1. 查内存
        for cat_data in self._mem_cache.values():
            if clean_code in cat_data:
                return {"fund_code": clean_code, "establish_date": cat_data[clean_code]}

        # 2. 查文件缓存
        for cat in self.CATEGORIES:
            cached = _load_cache(cat)
            if cached:
                self._mem_cache[cat] = cached
                if clean_code in cached:
                    return {"fund_code": clean_code, "establish_date": cached[clean_code]}

        # 3. 校验是否属于交易所上市基金，非交易所上市标的直接返回 None
        exchange_codes = await self.get_exchange_listed_codes()
        if clean_code not in exchange_codes:
            return {"fund_code": clean_code, "establish_date": None}

        # 4. 交易所上市标的但缓存未收录（如新近上市），兜底请求单只 F10 概况
        from providers.funds.fund_profile import EastmoneyProfileSource

        profile = await EastmoneyProfileSource().get_fund_profile(clean_code)
        return {
            "fund_code": clean_code,
            "establish_date": profile.get("establish_date"),
        }
