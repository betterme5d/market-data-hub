# -*- coding: utf-8 -*-
import pytest
from unittest.mock import AsyncMock, patch

from core.filters import (
    clean_fund_code,
    get_fund_exchange,
    get_fund_exchange_type,
    is_exchange_traded_fund,
    is_likely_exchange_fund,
    is_target_exchange_fund,
    filter_fund_mapping,
    filter_exchange_funds,
    get_exchange_listed_codes,
    KNOWN_OTC_PREFIXES,
)


def test_clean_fund_code():
    assert clean_fund_code('510300') == '510300'
    assert clean_fund_code(' 159915 ') == '159915'
    assert clean_fund_code('sh510300') is None  # 默认严格
    assert clean_fund_code('SZ159915') is None  # 默认严格
    assert clean_fund_code('sh510300', allow_affix=True) == '510300'
    assert clean_fund_code('SZ159915', allow_affix=True) == '159915'
    assert clean_fund_code('51030') is None  # 5 位
    assert clean_fund_code('5103001') is None  # 7 位
    assert clean_fund_code('') is None
    assert clean_fund_code(None) is None
    assert clean_fund_code(510300) == '510300'


def test_get_fund_exchange():
    assert get_fund_exchange('510300') == 'SH'
    assert get_fund_exchange('501018') == 'SH'
    assert get_fund_exchange('159915') == 'SZ'
    assert get_fund_exchange('161724') == 'SZ'
    assert get_fund_exchange('000001') is None
    assert get_fund_exchange('invalid') is None


def test_is_likely_exchange_fund():
    # 典型场内 ETF / LOF
    assert is_likely_exchange_fund('510300') is True
    assert is_likely_exchange_fund('159915') is True
    assert is_likely_exchange_fund('161724') is True
    assert is_likely_exchange_fund('501018') is True

    # 典型以 5 或 1 开头但属于场外公募的号段 (如 519xxx, 110xxx)
    assert is_likely_exchange_fund('519018') is False
    assert is_likely_exchange_fund('110005') is False
    assert is_likely_exchange_fund('100038') is False
    assert is_likely_exchange_fund('121001') is False

    # 普通 0、2、3、9 开头场外基金
    assert is_likely_exchange_fund('000001') is False
    assert is_likely_exchange_fund('001234') is False
    assert is_likely_exchange_fund('210001') is False
    assert is_likely_exchange_fund('960033') is False


def test_get_fund_exchange_type():
    # 场内产品
    assert 'ETF' in get_fund_exchange_type('510300')
    assert 'ETF' in get_fund_exchange_type('159915')
    assert 'LOF' in get_fund_exchange_type('161724')
    assert 'LOF' in get_fund_exchange_type('501018')

    # 场外公募基金必须返回 None，不能误匹配到保底或 ETF 规则
    assert get_fund_exchange_type('519018') is None
    assert get_fund_exchange_type('110005') is None
    assert get_fund_exchange_type('000001') is None


def test_is_exchange_traded_fund():
    assert is_exchange_traded_fund('510300') is True
    assert is_exchange_traded_fund('159915') is True
    assert is_exchange_traded_fund('519018') is False
    assert is_exchange_traded_fund('110005') is False
    assert is_exchange_traded_fund('000001') is False


def test_is_target_exchange_fund():
    # 1. 静态初筛（未传入 listed_codes）
    assert is_target_exchange_fund('510300') is True
    assert is_target_exchange_fund('519018') is False

    # 2. 精确白名单匹配（传入 listed_codes）
    whitelist = {'510300', '159915'}
    assert is_target_exchange_fund('510300', listed_codes=whitelist) is True
    assert is_target_exchange_fund('159915', listed_codes=whitelist) is True
    assert is_target_exchange_fund('161724', listed_codes=whitelist) is False
    assert is_target_exchange_fund('519018', listed_codes=whitelist) is False


def test_filter_fund_mapping():
    data = {
        '510300': '300ETF',
        '159915': '创业板ETF',
        '519018': '汇添富均衡',
        '000001': '华夏成长',
    }
    # 静态初筛
    filtered = filter_fund_mapping(data)
    assert set(filtered.keys()) == {'510300', '159915'}

    # 精确白名单
    filtered_strict = filter_fund_mapping(data, listed_codes={'510300'})
    assert set(filtered_strict.keys()) == {'510300'}


def test_filter_exchange_funds():
    codes = ['510300', '159915', '519018', '000001']
    res = filter_exchange_funds(codes)
    assert res == ['510300', '159915']

    res_strict = filter_exchange_funds(codes, listed_codes={'159915'})
    assert res_strict == ['159915']


@pytest.mark.asyncio
async def test_get_exchange_listed_codes():
    mock_listing = {
        'SH': {'510300': '2012-05-28'},
        'SZ': {'159915': '2011-12-09'},
    }
    with patch('providers.exchanges.listing_dates.ListingDateProvider.get_all', new=AsyncMock(return_value=mock_listing)):
        codes = await get_exchange_listed_codes()
        assert codes == {'510300', '159915'}
