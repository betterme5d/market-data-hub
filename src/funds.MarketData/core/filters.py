# -*- coding: utf-8 -*-
"""
交易所交易基金（场内基金）代码规则定义及过滤公共模块。

功能包含：
1. 基金代码基础规范化与所属交易所路由 (clean_fund_code, get_fund_exchange)
2. 静态快速规则初筛 (is_likely_exchange_fund, is_exchange_traded_fund, get_fund_exchange_type)
3. 官方上市基金白名单判别 (get_exchange_listed_codes, is_target_exchange_fund)
4. 字典与序列通用过滤工具 (filter_fund_mapping, filter_exchange_funds, clean_dataframe)
"""
import datetime
import logging
from typing import Any, Container, Dict, Iterable, List, Mapping, Optional, Set, TypeVar

import pandas as pd

logger = logging.getLogger(__name__)

T = TypeVar("T")

# 明确的场外公募号段（虽部分以 5 或 1 开头，但为历史场外代销或申赎代码，非交易所撮合交易品种）
KNOWN_OTC_PREFIXES = ("519", "110", "100", "121")

# 沪市场内基金细分规则 (5打头)
SH_EXCHANGE_RULES = {
    # 优先精确匹配 3 位前缀
    "501": "沪市上市开放式基金(LOF)",
    "502": "沪市分级基金(LOF)",
    "505": "沪市创新型封闭式证券投资基金",
    "506": "沪市科创板上市开放式基金(LOF)",
    "508": "沪市基础设施公募REITs",
    "500": "沪市传统封闭式基金",
    "588": "沪市科创板ETF",
    # 匹配 2 位前缀
    "51": "沪市主力交易型开放式指数基金(ETF)（含股票型ETF、511货币/债券ETF、513跨境ETF、518商品/黄金ETF等）",
    "56": "沪市新增交易型开放式指数基金(ETF)",
    "58": "沪市科创板及战略新兴产业主题ETF(除588外)",
    "50": "沪市传统封闭式/LOF/创新封闭式基金(除501、502、505、506、508外)",
    "52": "沪市创新型基金/特殊策略产品段",
}

# 深市场内基金细分规则 (1打头)
SZ_EXCHANGE_RULES = {
    # 优先精确匹配 3 位前缀
    "180": "深市基础设施公募REITs",
    "184": "深市传统契约型封闭式基金",
    "159": "深市主力交易型开放式指数基金(ETF)（包含1595xx、1596xx、1599xx等）",
    # 匹配 2 位前缀
    "15": "深市交易型开放式指数基金(ETF)/分级子份额等场内产品(除159外)",
    "16": "深市上市开放式基金(LOF) 及 早期混用16号段深市ETF",
    "18": "深市封闭式基金/特殊交易产品(除180、184外)",
    "19": "深市新增REITs及创新型场内交易基金",
}


def clean_fund_code(code: Any, allow_affix: bool = False) -> Optional[str]:
    """规整基金代码：去除前后空白并校验是否为标准 6 位数字代码。

    - 默认严格校验 6 位纯数字；
    - 当 allow_affix=True 时，允许剥离 sh/sz 等非数字前缀（如 'sh510300' -> '510300'）。
    示例：
      - '510300' -> '510300'
      - ' 159915 ' -> '159915'
      - 'sh510300' -> None (默认严格)
      - clean_fund_code('sh510300', allow_affix=True) -> '510300'
      - '51030' -> None (位数不足)
    """
    if code is None:
        return None
    s = str(code).strip()
    if len(s) == 6 and s.isdigit():
        return s
    if allow_affix:
        digits = "".join(filter(str.isdigit, s))
        if len(digits) == 6:
            return digits
    return None


def get_fund_exchange(code: str) -> Optional[str]:
    """根据 6 位基金代码首位判断所属证券交易所代码。

    - 以 '5' 开头 -> 'SH' (上交所)
    - 以 '1' 开头 -> 'SZ' (深交所)
    - 其余代码 -> None
    """
    clean = clean_fund_code(code)
    if not clean:
        return None
    if clean.startswith("5"):
        return "SH"
    if clean.startswith("1"):
        return "SZ"
    return None


def is_likely_exchange_fund(code: str) -> bool:
    """场内基金轻量初筛（纯静态规则，零网络开销）：

    1. 必须为 6 位纯数字代码；
    2. 必须以 '5'（沪市）或 '1'（深市）开头；
    3. 显式排除已知的以 5/1 开头的传统场外公募基金号段（如 519xxx, 110xxx, 100xxx, 121xxx）。
    """
    clean = clean_fund_code(code)
    if not clean:
        return False
    if clean.startswith(KNOWN_OTC_PREFIXES):
        return False
    return clean[0] in ("5", "1")


def get_fund_exchange_type(fund_code: str) -> Optional[str]:
    """根据基金代码判定其在交易所上市交易的基金类型和说明。

    如果该代码属于场外基金或不属于任何沪深交易所上市交易基金规则，返回 None。
    """
    clean_code = clean_fund_code(fund_code)
    if not clean_code:
        return None

    # 显式排除场外公募号段
    if clean_code.startswith(KNOWN_OTC_PREFIXES):
        return None

    prefix_3 = clean_code[:3]
    prefix_2 = clean_code[:2]

    # 沪市判断 (以 5 开头)
    if clean_code.startswith("5"):
        if prefix_3 in SH_EXCHANGE_RULES:
            return SH_EXCHANGE_RULES[prefix_3]
        if prefix_2 in SH_EXCHANGE_RULES:
            return SH_EXCHANGE_RULES[prefix_2]
        return "沪市交易所基金(保底匹配)"

    # 深市判断 (以 1 开头)
    if clean_code.startswith("1"):
        if prefix_3 in SZ_EXCHANGE_RULES:
            return SZ_EXCHANGE_RULES[prefix_3]
        if prefix_2 in SZ_EXCHANGE_RULES:
            return SZ_EXCHANGE_RULES[prefix_2]
        return "深市交易所基金(保底匹配)"

    return None


def is_exchange_traded_fund(fund_code: str) -> bool:
    """判断基金代码是否是可以在交易所进行交易的场内基金（静态规则）。"""
    return get_fund_exchange_type(fund_code) is not None


async def get_exchange_listed_codes(force: bool = False) -> Set[str]:
    """获取上交所与深交所官方公布的全部上市 ETF/LOF/REITs 基金代码集合（权威白名单）。

    内部通过 ListingDateProvider 从本地落盘缓存（带 24 小时过期保护）或交易所官方接口加载。
    """
    from providers.exchanges.listing_dates import ListingDateProvider

    dates = await ListingDateProvider().get_all(force=force)
    sh_codes = set(dates.get("SH", {}).keys())
    sz_codes = set(dates.get("SZ", {}).keys())
    return sh_codes | sz_codes


def is_target_exchange_fund(
    code: str, listed_codes: Optional[Container[str]] = None
) -> bool:
    """统一场内基金判别规则：

    - 若传入了 listed_codes（交易所上市白名单）：严格校验 clean_code in listed_codes；
    - 若未传入 listed_codes：使用 is_likely_exchange_fund(clean_code) 静态初筛。
    """
    clean = clean_fund_code(code)
    if not clean:
        return False
    if listed_codes is not None:
        return clean in listed_codes
    return is_likely_exchange_fund(clean)


def filter_fund_mapping(
    data: Mapping[str, T], listed_codes: Optional[Container[str]] = None
) -> Dict[str, T]:
    """过滤 {fund_code: value} 字典，仅保留符合场内规则的条目。"""
    return {
        k: v
        for k, v in data.items()
        if is_target_exchange_fund(k, listed_codes=listed_codes)
    }


def filter_exchange_funds(
    codes: Iterable[str], listed_codes: Optional[Container[str]] = None
) -> List[str]:
    """从基金代码可迭代对象中过滤出符合场内规则的代码列表。"""
    return [
        c
        for c in codes
        if is_target_exchange_fund(c, listed_codes=listed_codes)
    ]


def clean_dataframe(df: pd.DataFrame, listed_codes: Optional[Container[str]] = None) -> list:
    """将 akshare 等上游返回的 DataFrame 清洗为可 JSON 序列化的记录列表：

    仅保留场内基金，NaN→None，日期转字符串，基金代码补零。
    支持传入 listed_codes 白名单做高精度过滤。
    """
    # 先做一层交易所交易基金的过滤，仅保留场内交易基金
    df["temp_code"] = df["基金代码"].astype(str).str.zfill(6)
    if listed_codes is not None:
        df = df[df["temp_code"].apply(lambda c: is_target_exchange_fund(c, listed_codes=listed_codes))]
    else:
        df = df[df["temp_code"].apply(is_exchange_traded_fund)]
    df = df.drop(columns=["temp_code"])

    records = df.to_dict(orient="records")
    cleaned_records = []
    for r in records:
        cleaned_r = {}
        for k, v in r.items():
            if pd.isna(v):
                cleaned_r[k] = None
            else:
                if isinstance(v, (pd.Timestamp, datetime.date, datetime.datetime)):
                    cleaned_r[k] = v.strftime("%Y-%m-%d")
                else:
                    if k == "基金代码":
                        cleaned_r[k] = str(v).zfill(6)
                    else:
                        cleaned_r[k] = v
        cleaned_records.append(cleaned_r)
    return cleaned_records
