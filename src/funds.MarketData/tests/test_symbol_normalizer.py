# -*- coding: utf-8 -*-
"""
Task 122: SymbolNormalizer 代码归一化纯函数 — 单元测试
"""
import pytest
from core.symbol_normalizer import normalize_symbol, to_xueqiu_symbol, to_sina_symbol, to_tencent_symbol


@pytest.mark.parametrize("raw,expected_code,expected_suffix", [
    # 标准格式透传
    ("002092.SZ",  "002092.SZ", "SZ"),
    ("510300.SH",  "510300.SH", "SH"),
    ("600519.SH",  "600519.SH", "SH"),
    # 雪球大写前缀格式
    ("SZ002092",   "002092.SZ", "SZ"),
    ("SH510300",   "510300.SH", "SH"),
    # 小写前缀格式（腾讯/新浪）
    ("sz002092",   "002092.SZ", "SZ"),
    ("sh510300",   "510300.SH", "SH"),
    # 纯6位数字自动识别
    ("510300",     "510300.SH", "SH"),
    ("600519",     "600519.SH", "SH"),
    ("002092",     "002092.SZ", "SZ"),
    ("159901",     "159901.SZ", "SZ"),
    ("300750",     "300750.SZ", "SZ"),
    # 美股
    ("AAPL",       "AAPL.US",   "US"),
    ("SPY",        "SPY.US",    "US"),
    # 港股
    ("00700.HK",   "00700.HK",  "HK"),
])
def test_normalize_symbol(raw, expected_code, expected_suffix):
    result = normalize_symbol(raw)
    assert result is not None, f"normalize_symbol({raw!r}) returned None"
    code, suffix = result
    assert code == expected_code, f"Expected code {expected_code!r}, got {code!r}"
    assert suffix == expected_suffix, f"Expected suffix {expected_suffix!r}, got {suffix!r}"


def test_normalize_symbol_invalid_returns_none():
    assert normalize_symbol("!!invalid!!") is None
    assert normalize_symbol("") is None


def test_to_xueqiu_symbol_sh():
    assert to_xueqiu_symbol("510300.SH") == "SH510300"


def test_to_xueqiu_symbol_sz():
    assert to_xueqiu_symbol("002092.SZ") == "SZ002092"


def test_to_sina_symbol_sh():
    assert to_sina_symbol("510300.SH") == "sh510300"


def test_to_sina_symbol_sz():
    assert to_sina_symbol("002092.SZ") == "sz002092"


def test_to_tencent_symbol_sh():
    assert to_tencent_symbol("510300.SH") == "sh510300"


def test_to_tencent_symbol_us():
    assert to_tencent_symbol("AAPL.US") == "usAAPL"
