# -*- coding: utf-8 -*-
"""场内基金终止上市公告解析器的用例。

样本全部来自真实公告（沪深各若干），见
docs/2026-09-12-fund-delisting-data-source.md §6.3、§7。
改动正则导致这些用例失败，说明会漏真实数据，不要直接改断言。
"""
import pytest

from providers.funds.delist.parser import (
    is_formal_announcement,
    parse_announcement,
    strip_html,
)


# 真实样本片段（均为公告正文里出现过的原文）
CASES = [
    # 沪市·终止上市：终止上市日：2026年8月24日
    ("终止上市日：2026年8月24日", "2026-08-24", None),
    # 深市·PDF 带冒号且空格不规则
    ("终止上市日：2026 年 3 月 12 日", "2026-03-12", None),
    # 深市·PDF 不带冒号（冒号必须可选，否则这一条会漏）
    ("终止上市日 2025 年6 月13 日", "2025-06-13", None),
    # 深交所 JSON 正文句式
    ("证券代码：159969）自2026年3月12日起终止上市交易。", "2026-03-12", None),
    # 沪市·摘牌公告：摘牌时间…将于…收盘后摘牌
    ("摘牌时间：本基金将于2025年8月13日收盘后在上海证券交易所摘牌。", "2025-08-13", None),
    # 将于…终止上市
    ("本基金将于2025 年6 月13 日终止上市交易", "2025-06-13", None),
]

LAST_OP_CASES = [
    # 日期在后
    ("本基金最后运作日为2025 年 6 月 4 日", "2025-06-04"),
    # 日期在前（560650 就是这种，只写一种句式会漏）
    ("2026年7月2日为本基金的最后运作日", "2026-07-02"),
]


@pytest.mark.parametrize("text,expected_delist,_", CASES)
def test_delist_date(text, expected_delist, _):
    assert parse_announcement(text)["delist_date"] == expected_delist


@pytest.mark.parametrize("text,expected", LAST_OP_CASES)
def test_last_operation_date(text, expected):
    assert parse_announcement(text)["last_operation_date"] == expected


def test_register_and_suspend_date():
    text = ("终止上市的权益登记日为2025 年 1 月 14 日。"
            "本基金已于 2025 年 1 月 7 日开市起停牌")
    out = parse_announcement(text)
    assert out["register_date"] == "2025-01-14"
    assert out["suspend_date"] == "2025-01-07"


def test_strip_html():
    assert strip_html('<span class="keyword">终止</span>上市') == "终止上市"


def test_html_body_is_parsed():
    """深交所正文是带样式的 HTML，去标签后仍能抽到日期。"""
    html = ('<p><span style="font-size:12pt">自2026年3月12日起终止上市交易。</span></p>')
    assert parse_announcement(html)["delist_date"] == "2026-03-12"


@pytest.mark.parametrize("title,expected", [
    ("关于银华深证100交易型开放式指数证券投资基金终止上市的公告", True),
    ("关于…基金终止上市的提示性公告", False),                # 提示性要剔除
    ("关于…可能触发基金合同终止情形的风险提示公告", False),   # 未定态要剔除
    ("", False),
])
def test_is_formal_announcement(title, expected):
    assert is_formal_announcement(title) is expected


def test_short_body_warns():
    """正文过短通常是扫描件或取正文失败，必须告警而不是静默返回空。"""
    assert "warn" in parse_announcement("太短")
