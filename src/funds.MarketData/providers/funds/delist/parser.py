# -*- coding: utf-8 -*-
"""场内基金终止上市公告正文解析（纯函数，无 IO）。

措辞在沪深之间、在「终止上市」与「摘牌」公告之间都不统一，
且数字与「年月日」之间的空格不规则。所有模式都来自实测样本，见
docs/2026-09-12-fund-delisting-data-source.md §6.3。

改动本文件的正则前，请先补样本并跑 tests/test_delist_parser.py。
"""
from __future__ import annotations

import re
from typing import Dict, Optional

# 数字与「年月日」之间可能有空格，且不规则：
#   深市 PDF 「2025 年6 月13 日」—— 年后有空格、月后没有
#   沪市 PDF 「2026年8月24日」—— 完全无空格
NUM = r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日"

# 终止上市日：深市有带冒号与不带冒号两种写法，冒号必须可选
RE_DELIST = re.compile(rf"终止上市日\s*[:：]?\s*{NUM}")
# 深交所 JSON 正文的主要句式
RE_FROM = re.compile(rf"自\s*{NUM}\s*起[^。]{{0,20}}?终止上市")
# 沪市摘牌公告：摘牌时间：本基金将于2025年8月13日收盘后…摘牌
RE_ZHAIPAI = re.compile(rf"摘牌时间\s*[:：][^\n。]{{0,60}}?{NUM}")
# 将于2025 年6 月13 日终止上市交易
RE_WILL = re.compile(rf"将于\s*{NUM}[^。]{{0,25}}?(?:终止上市|摘牌)")

# 最后运作日：两种句式都要匹配，只写一种会漏
RE_LAST_OP = re.compile(rf"最后运作日(?:为)?\s*[:：]?\s*{NUM}")            # 日期在后
RE_LAST_OP_PREV = re.compile(rf"{NUM}\s*(?:为)?[^。]{{0,15}}最后运作日")   # 日期在前

# 权益登记日实测为「权益登记日为2025 年 1 月 14 日」，中间的「为」不能漏
RE_REGISTER = re.compile(rf"权益登记日(?:为)?\s*[:：]?\s*{NUM}")
RE_SUSPEND = re.compile(rf"{NUM}\s*开市起停牌")

# 「可能触发基金合同终止情形的风险提示公告」≠ 已终止，必须排除
RE_TENTATIVE = re.compile(r"可能|风险提示|预警|拟终止")

# 深交所搜索接口**返回里没有证券代码**（只有标题），但正文里有
# 「深100ETF银华，证券代码：159969）自2026年3月12日起终止上市交易」
RE_SECURITY_CODE = re.compile(r"证券代码\s*[:：]?\s*(\d{6})")

_TAG = re.compile(r"<[^>]+>")


def strip_html(text: str) -> str:
    """去掉 HTML 标签（深交所正文是带样式的 HTML 片段）。"""
    return _TAG.sub("", text or "")


def _iso(m: re.Match) -> str:
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


def _first(text: str, *patterns: re.Pattern) -> Optional[str]:
    for rx in patterns:
        m = rx.search(text)
        if m:
            return _iso(m)
    return None


def is_formal_announcement(title: str) -> bool:
    """是否为「正式公告」。

    同一事件通常发两条：提示性公告（预告）+ 正式公告（含生效日）。
    正式公告在前，取正式的那条即可。同时排除未定态措辞。
    """
    if not title:
        return False
    if "提示性" in title:
        return False
    return not bool(RE_TENTATIVE.search(title))


def extract_security_code(text: str) -> Optional[str]:
    """从公告正文抽证券代码。

    深交所 `api/search/content` 的返回里**没有证券代码字段**（只有标题），
    但正文里有「证券代码：159969」，只能从这里拿。
    """
    m = RE_SECURITY_CODE.search(strip_html(text or ""))
    return m.group(1) if m else None


def parse_announcement(text: str) -> Dict[str, Optional[str]]:
    """从公告正文抽取终止上市相关日期。

    :param text: 公告正文（可含 HTML 标签，会自动去除）
    :return: dict，未抽到则为 None；`warn` 为可选告警
    """
    plain = strip_html(text)
    out: Dict[str, Optional[str]] = {
        "delist_date": None,
        "last_operation_date": None,
        "register_date": None,
        "suspend_date": None,
    }

    out["delist_date"] = _first(plain, RE_DELIST, RE_FROM, RE_ZHAIPAI, RE_WILL)
    out["last_operation_date"] = _first(plain, RE_LAST_OP, RE_LAST_OP_PREV)
    out["register_date"] = _first(plain, RE_REGISTER)
    out["suspend_date"] = _first(plain, RE_SUSPEND)

    if len(plain.strip()) < 50:
        # 正常公告正文至少几百字；过短通常意味着 PDF 是扫描件或正文没取到
        out["warn"] = "正文过短，疑似扫描件或取正文失败"
    return out
