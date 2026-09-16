# -*- coding: utf-8 -*-
"""
证券代码归一化工具。
对外只暴露 normalize_symbol / to_xueqiu_symbol / to_sina_symbol / to_tencent_symbol。
所有数据源专属格式转换均由此文件统一维护，禁止在 Provider 内散落字符串变换逻辑。
"""
from typing import Optional, Tuple
import re

# 可接受的符号形态：字母数字开头（允许 .SPGSCL 这种前导点），可含 . - _ ，长度 ≤ 20
_SYMBOL_PATTERN = re.compile(r"^\.?[A-Za-z0-9][A-Za-z0-9._-]{0,19}$")


def resolve_symbol_identity(raw: str) -> str:
    """
    解析符号身份，供统一取数门面作为缓存键与响应标识：

    - 带后缀或纯数字的（002092 / 510300.SH / SH510300 / AAPL.US / 00700.HK）→ 返回归一化标准码，
      其中裸 6 位数字必须展开（002092 → 002092.SZ），否则上游不认；
    - 纯字母的（AAPL / USO / HKHSI）→ **保持原样**：裸字母本身就是各数据源通用的代码形态，
      而标准归一化只能按「2-5 位字母 → 美股」推断，会把恒指 HKHSI 误判成 HKHSI.US；
    - 其余识别不了的（CSI930875 / .SPGSCL / HKDCNY.FX）→ 原样返回，格式转换交由各数据源自身的转换逻辑。

    仅做基本形态校验，非法形态抛 ValueError。
    """
    if not raw or not isinstance(raw, str):
        raise ValueError(f"Invalid symbol: {raw!r}")
    s = raw.strip()
    if not _SYMBOL_PATTERN.match(s):
        raise ValueError(f"Invalid symbol: {raw!r}")
    if s.isalpha():
        return s.upper()
    norm = normalize_symbol(s)
    return norm[0] if norm else s.upper()


def normalize_symbol(raw: str) -> Optional[Tuple[str, str]]:
    """
    将任意格式的证券代码归一化为 (标准代码 "CODE.SUFFIX", 市场后缀) 二元组。
    无法识别的格式返回 None。

    支持识别：
      标准格式：002092.SZ / 510300.SH / AAPL.US / 00700.HK
      雪球格式：SZ002092 / SH510300（大小写不敏感）
      腾讯/新浪格式：sz002092 / sh510300（小写前缀）
      纯6位A股数字代码：自动按首位推断沪深市场
      纯英文字母美股代码：自动识别为 US
    """
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None

    # 1. 标准格式 CODE.SUFFIX（含 SS -> SH 兼容）
    if "." in s:
        parts = s.rsplit(".", 1)
        if len(parts) == 2:
            code_part, suffix_part = parts[0].upper(), parts[1].upper()
            suffix_map = {"SH": "SH", "SS": "SH", "SZ": "SZ", "HK": "HK", "US": "US"}
            if suffix_part in suffix_map:
                return f"{code_part}.{suffix_map[suffix_part]}", suffix_map[suffix_part]
        return None

    # 2. 前缀2字母格式（雪球/腾讯/新浪），大小写不敏感
    su = s.upper()
    for prefix, suffix in [("SH", "SH"), ("SZ", "SZ"), ("HK", "HK")]:
        if su.startswith(prefix) and len(s) > 2 and s[2:].isdigit():
            return f"{s[2:]}.{suffix}", suffix

    # 3. 纯6位数字 A股代码 —— 按首位推断市场
    if s.isdigit() and len(s) == 6:
        first = s[0]
        if first in ("5", "6"):
            return f"{s}.SH", "SH"
        if first in ("0", "1", "2", "3"):
            return f"{s}.SZ", "SZ"
        return None  # 4/7/8/9 开头暂不支持

    # 4. 纯英文字母 2-5 位 -> 美股
    if s.isalpha() and 2 <= len(s) <= 5:
        return f"{s.upper()}.US", "US"

    # 5. 港股5位纯数字（带前导零，如 00700）
    if s.isdigit() and len(s) == 5:
        return f"{s}.HK", "HK"

    return None


def to_xueqiu_symbol(standard_code: str) -> str:
    """002092.SZ -> SZ002092 / 510300.SH -> SH510300"""
    result = normalize_symbol(standard_code)
    if not result:
        return standard_code
    code, suffix = result
    pure_code = code.rsplit(".", 1)[0]
    if suffix in ("SH", "SZ"):
        return f"{suffix}{pure_code}"
    return pure_code


def to_sina_symbol(standard_code: str) -> str:
    """002092.SZ -> sz002092 / 510300.SH -> sh510300"""
    result = normalize_symbol(standard_code)
    if not result:
        return standard_code
    code, suffix = result
    pure_code = code.rsplit(".", 1)[0]
    if suffix == "SH":
        return f"sh{pure_code}"
    if suffix == "SZ":
        return f"sz{pure_code}"
    return standard_code


def to_tencent_symbol(standard_code: str) -> str:
    """002092.SZ -> sz002092 / AAPL.US -> usAAPL / 00700.HK -> hk00700"""
    result = normalize_symbol(standard_code)
    if not result:
        return standard_code
    code, suffix = result
    pure_code = code.rsplit(".", 1)[0]
    prefix_map = {"SH": "sh", "SZ": "sz", "US": "us", "HK": "hk"}
    return f"{prefix_map.get(suffix, '')}{pure_code}"
