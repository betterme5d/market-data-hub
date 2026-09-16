# -*- coding: utf-8 -*-
"""
行情路由与代码翻译：把标准/各源格式互相转换，并按标的特征选择行情源。
由 routers/quotes.py 使用；独立于 main 便于测试。
"""
from typing import Dict, Optional


def translate_standard_symbol(symbol: str) -> Dict[str, str]:
    """
    根据标的特征翻译为数据源的标准代码。
    例如：
      - 510300.SH -> {"tencent": "sh510300", "sina": "sh510300", "xueqiu": "SH510300"}
      - 00700.HK -> {"tencent": "hk00700", "sina": "rt_hk00700", "xueqiu": "00700"}
      - AAPL -> {"yfinance": "AAPL", "xueqiu": "AAPL", "sina": "gb_aapl"}
    """
    symbol_upper = symbol.upper()

    # 1. 新浪/腾讯/雪球专有格式
    if symbol.lower().startswith(("sh", "sz", "hk", "rt_hk", "gb_", "nf_", "hf_", "fx_", "znb_")):
        s = symbol.lower()
        if s.startswith("rt_hk"):
            return {"sina": s, "xueqiu": s[5:]}
        elif s.startswith("gb_"):
            return {"sina": s, "yfinance": s[3:].upper(), "xueqiu": s[3:].upper()}
        elif s.startswith("nf_") or s.startswith("hf_"):
            return {"sina": s}
        elif s.startswith("fx_"):
            return {"xueqiu": s.upper()}
        elif s.startswith("sh") or s.startswith("sz"):
            return {"tencent": s, "sina": s, "xueqiu": s.upper()}
        return {"sina": s}

    # 2. 带有后缀的标准化格式 (510300.SH, 000001.SZ, 00700.HK 等)
    if "." in symbol_upper:
        code, suffix = symbol_upper.split('.', 1)
        if suffix in ["SH", "SS"]:
            return {
                "tencent": f"sh{code}",
                "sina": f"sh{code}",
                "xueqiu": f"SH{code}"
            }
        elif suffix == "SZ":
            return {
                "tencent": f"sz{code}",
                "sina": f"sz{code}",
                "xueqiu": f"SZ{code}"
            }
        elif suffix == "HK":
            padded_code = code.zfill(5)
            return {
                "tencent": f"hk{padded_code}",
                "sina": f"rt_hk{padded_code}",
                "xueqiu": padded_code
            }
        elif suffix == "US":
            # 系统标准码带 .US 后缀，但雪球/新浪等源用裸代码（AAPL），带点前缀的（.SPGSCL）另走原生透传
            return {
                "yfinance": code,
                "xueqiu": code,
                "sina": f"gb_{code.lower()}",
                "tencent": f"us{code}",
            }

    # 3. 常见美股标的代码 (2-5位英文字母)
    if symbol_upper.isalpha() and 2 <= len(symbol_upper) <= 5:
        return {
            "yfinance": symbol_upper,
            "xueqiu": symbol_upper,
            "sina": f"gb_{symbol.lower()}",
            "tencent": f"us{symbol_upper}"
        }

    # 4. 透传做为兜底
    return {"yfinance": symbol, "xueqiu": symbol_upper, "sina": symbol.lower()}


def route_provider(symbol: str, source: Optional[str] = None) -> str:
    """
    根据标的代码特征或显式参数选择行情提供商。
    """
    from core.dispatcher import PROVIDERS

    if source and source.lower() in PROVIDERS:
        return source.lower()

    symbol_lower = symbol.lower()
    if symbol_lower.startswith(("nf_", "hf_")):
        return "sina"
    elif symbol_lower.endswith((".sh", ".sz")) or symbol_lower.startswith(("sh", "sz")):
        return "xueqiu"
    return "yfinance"
