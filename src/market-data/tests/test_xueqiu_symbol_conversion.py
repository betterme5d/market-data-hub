# -*- coding: utf-8 -*-
"""雪球符号转换与符号身份解析测试（覆盖原生符号直通与 .US 后缀）"""
import pytest

from core.routing import translate_standard_symbol
from core.symbol_normalizer import resolve_symbol_identity
from providers.quotes.xueqiu import normalize_xueqiu_symbol


@pytest.mark.parametrize("raw,expected", [
    # 带后缀或纯数字 -> 归一化标准码（裸 6 位必须展开，否则上游不认）
    ("002092", "002092.SZ"),
    ("510300.SH", "510300.SH"),
    ("SH510300", "510300.SH"),
    ("sz399807", "399807.SZ"),
    ("AAPL.US", "AAPL.US"),
    ("00700.HK", "00700.HK"),
    ("00700", "00700.HK"),
    # 纯字母 -> 保持原样：裸字母是各源通用形态，而归一化只能按「2-5 位字母 → 美股」推断
    ("AAPL", "AAPL"),
    ("USO", "USO"),
    ("HKHSI", "HKHSI"),        # 恒指：原样交给雪球，不被推断成 HKHSI.US
    # 其余识别不了的 -> 原样返回，转换交给数据源自身逻辑
    ("CSI930875", "CSI930875"),
    (".SPGSCL", ".SPGSCL"),
    ("HKDCNY.FX", "HKDCNY.FX"),
])
def test_resolve_symbol_identity(raw, expected):
    assert resolve_symbol_identity(raw) == expected


def test_resolve_symbol_identity_rejects_garbage():
    for bad in ["!!bad!!", "", "   ", "a" * 25, "沪深300"]:
        with pytest.raises(ValueError):
            resolve_symbol_identity(bad)


@pytest.mark.parametrize("raw,expected", [
    ("510300.SH", "SH510300"),
    ("SH510300", "SH510300"),
    ("002092", "002092"),           # 裸 6 位由上层归一化为 002092.SZ 后再转换
    ("002092.SZ", "SZ002092"),
    ("00700.HK", "00700"),
    ("AAPL.US", "AAPL"),            # 雪球美股用裸代码
    ("USO.US", "USO"),
    ("HKHSI.US", "HKHSI"),          # 前缀分支会先吞掉 HKHSI.US，需后缀兜底
    # 原生符号原样透传
    ("HKHSI", "HKHSI"),
    ("USO", "USO"),
    ("CSI930875", "CSI930875"),
    (".SPGSCL", ".SPGSCL"),
    ("HKDCNY.FX", "HKDCNY.FX"),
])
def test_normalize_xueqiu_symbol(raw, expected):
    assert normalize_xueqiu_symbol(raw) == expected


def test_translate_standard_symbol_us_suffix():
    """系统标准码 .US 后缀翻译为各源裸代码形态"""
    m = translate_standard_symbol("AAPL.US")
    assert m["xueqiu"] == "AAPL"
    assert m["yfinance"] == "AAPL"
    assert m["sina"] == "gb_aapl"
    assert m["tencent"] == "usAAPL"
