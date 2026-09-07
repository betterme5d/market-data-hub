# -*- coding: utf-8 -*-
import json
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from providers.funds.establish_dates import (
    EastmoneyEstablishDateSource,
    EstablishDateProvider,
    EastmoneyEstablishDateProbe,
    _extract_json_array,
    _normalize_date,
    _is_target_fund,
)


def test_normalize_date():
    assert _normalize_date("2020-05-18") == "2020-05-18"
    assert _normalize_date("2020/05/18") == "2020-05-18"
    assert _normalize_date("20200518") == "2020-05-18"
    assert _normalize_date("2020-05-18 00:00:00") == "2020-05-18"
    assert _normalize_date("--") is None
    assert _normalize_date("-") is None
    assert _normalize_date("nan") is None
    assert _normalize_date(None) is None
    assert _normalize_date("") is None


def test_is_target_fund():
    # 仅保留以 5 或 1 开头的 6 位数字代码
    assert _is_target_fund("510300") is True
    assert _is_target_fund("159915") is True
    assert _is_target_fund("161724") is True
    assert _is_target_fund("501018") is True
    # 场外或非 5/1 开头基金应被过滤
    assert _is_target_fund("000001") is False
    assert _is_target_fund("001234") is False
    assert _is_target_fund("960033") is False
    assert _is_target_fund("210001") is False
    assert _is_target_fund("51030") is False  # 非 6 位
    assert _is_target_fund("sh510300") is False  # 含非数字

    # 配合 target_codes（交易所上市清单）过滤场外公募（如 519018、110005）
    exchange_set = {"510300", "159915", "161724"}
    assert _is_target_fund("510300", target_codes=exchange_set) is True
    assert _is_target_fund("159915", target_codes=exchange_set) is True
    assert _is_target_fund("519018", target_codes=exchange_set) is False
    assert _is_target_fund("110005", target_codes=exchange_set) is False
    assert _is_target_fund("000001", target_codes=exchange_set) is False


def test_parse_rows_fb():
    # 模拟 dt=fb，列15为成立日期，且包含 5/1 基金与 0 开头非目标基金
    source = EastmoneyEstablishDateSource()
    sample_fb = (
        "510300,300ETF,0.1,2026-09-04,1,1,1,1,1,1,1,1,1,1,1,2012-05-04,0,0,0,0,0,0,0"
    )
    sample_other = (
        "000001,某场外,0.1,2026-09-04,1,1,1,1,1,1,1,1,1,1,1,2001-12-18,0,0,0,0,0,0,0"
    )
    mock_text = f'var rankData = {{datas:["{sample_fb}","{sample_other}"]}};'
    rows = source._parse_text(mock_text, dt="fb", min_rows=1)
    
    # 000001 应被过滤，只留下 510300
    assert len(rows) == 1
    assert rows[0]["fund_code"] == "510300"
    assert rows[0]["establish_date"] == "2012-05-04"


def test_parse_rows_with_target_codes():
    source = EastmoneyEstablishDateSource()
    sample_etf = (
        "510300,300ETF,0.1,2026-09-04,1,1,1,1,1,1,1,1,1,1,1,2012-05-04,0,0,0,0,0,0,0"
    )
    sample_off_market = (
        "519018,汇添富均衡,0.1,2026-09-04,1,1,1,1,1,1,1,1,1,1,1,2006-08-07,0,0,0,0,0,0,0"
    )
    sample_other = (
        "000001,某场外,0.1,2026-09-04,1,1,1,1,1,1,1,1,1,1,1,2001-12-18,0,0,0,0,0,0,0"
    )
    mock_text = f'var rankData = {{datas:["{sample_etf}","{sample_off_market}","{sample_other}"]}};'
    
    # 传入交易所上市集合 target_codes，即使 519018 以 5 开头也被剔除
    target_codes = {"510300"}
    rows = source._parse_text(mock_text, dt="fb", min_rows=1, target_codes=target_codes)
    assert len(rows) == 1
    assert rows[0]["fund_code"] == "510300"
    assert rows[0]["establish_date"] == "2012-05-04"


def test_parse_rows_kf():
    # 模拟 dt=kf，列16为成立日期
    source = EastmoneyEstablishDateSource()
    sample_lof = (
        "161724,招商煤炭,0.1,2026-09-04,1,1,1,1,1,1,1,1,1,1,1,1,2015-05-20,0,0,0,0,0,0,0,0"
    )
    sample_open = (
        "001234,普通开放,0.1,2026-09-04,1,1,1,1,1,1,1,1,1,1,1,1,2018-01-01,0,0,0,0,0,0,0,0"
    )
    mock_text = f'var rankData = {{datas:["{sample_lof}","{sample_open}"]}};'
    rows = source._parse_text(mock_text, dt="kf", min_rows=1)
    
    # 001234 被过滤，留下 161724
    assert len(rows) == 1
    assert rows[0]["fund_code"] == "161724"
    assert rows[0]["establish_date"] == "2015-05-20"


def test_truncation_raises():
    source = EastmoneyEstablishDateSource()
    mock_text = 'var rankData = {datas:[]};'
    with pytest.raises(RuntimeError, match="suspicious truncation"):
        source._parse_text(mock_text, dt="fb", min_rows=10)


@pytest.mark.asyncio
async def test_provider_cache(tmp_path, monkeypatch):
    monkeypatch.setattr("providers.funds.establish_dates.CACHE_DIR", tmp_path)
    provider = EstablishDateProvider()

    # 预写模拟缓存文件
    test_data = {"510300": "2012-05-04"}
    cache_file = tmp_path / "establish_date_fb.json"
    cache_file.write_text(json.dumps(test_data), encoding="utf-8")

    res = await provider.get_all(category="fb", force=False)
    assert res == test_data

    # get_one 从内存/缓存读取
    one = await provider.get_one("510300")
    assert one == {"fund_code": "510300", "establish_date": "2012-05-04"}


@pytest.mark.asyncio
async def test_provider_get_all_merged(tmp_path, monkeypatch):
    monkeypatch.setattr("providers.funds.establish_dates.CACHE_DIR", tmp_path)
    provider = EstablishDateProvider()

    fb_data = {"510300": "2012-05-04"}
    kf_data = {"161724": "2015-05-20"}
    (tmp_path / "establish_date_fb.json").write_text(json.dumps(fb_data), encoding="utf-8")
    (tmp_path / "establish_date_kf.json").write_text(json.dumps(kf_data), encoding="utf-8")

    merged = await provider.get_all(category="all", force=False)
    assert merged == {"510300": "2012-05-04", "161724": "2015-05-20"}


@pytest.mark.asyncio
async def test_provider_get_one_exchange_filter(tmp_path, monkeypatch):
    monkeypatch.setattr("providers.funds.establish_dates.CACHE_DIR", tmp_path)
    provider = EstablishDateProvider()

    # 模拟交易所清单只包含 510300 和 159915
    provider.get_exchange_listed_codes = AsyncMock(return_value={"510300", "159915"})

    # 1. 519018 是场外基金，未在交易所上市，且不在缓存中 -> 应直接返回 None，不走 F10
    with patch("providers.funds.fund_profile.EastmoneyProfileSource.get_fund_profile") as mock_f10:
        res = await provider.get_one("519018")
        assert res == {"fund_code": "519018", "establish_date": None}
        mock_f10.assert_not_called()

    # 2. 159915 是交易所上市基金，但未在缓存中 -> 应调用 F10 概况兜底
    with patch(
        "providers.funds.fund_profile.EastmoneyProfileSource.get_fund_profile",
        new=AsyncMock(return_value={"establish_date": "2011-12-09"}),
    ) as mock_f10:
        res = await provider.get_one("159915")
        assert res == {"fund_code": "159915", "establish_date": "2011-12-09"}
        mock_f10.assert_awaited_once_with("159915")
