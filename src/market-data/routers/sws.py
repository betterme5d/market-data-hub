# -*- coding: utf-8 -*-
"""
申万行业指数端点。
"""
import logging

from fastapi import APIRouter, Query

from providers.misc.sws import SwsProvider

logger = logging.getLogger(__name__)

router = APIRouter()

sws_provider = SwsProvider()


@router.get("/api/sws/industries", tags=["申万行业"], summary="获取申万行业指数代码列表")
async def get_sws_industries(
    indextype: str = Query("一级行业", description="行业类别等级，例如：一级行业, 二级行业, 三级行业")
):
    """
    抓取申万研究官网的一级、二级、三级行业指数名称与代码映射。
    """
    return sws_provider.get_industries(indextype)


@router.get("/api/sws/industry-kline", tags=["申万行业"], summary="获取指定行业指数的历史K线数据")
async def get_sws_industry_kline(
    code: str = Query(..., description="申万行业指数代码，例如 801010"),
    period: str = Query("DAY", description="数据间隔周期，默认 DAY")
):
    """
    从申万官网抓取该指数自发布以来的全量日K线行情历史数据（无分页）。
    """
    return sws_provider.get_industry_kline(code, period)


@router.get("/api/sws/industry-realtime", tags=["申万行业"], summary="获取所有申万行业的当日实时行情")
async def get_sws_industry_realtime(
    indextype: str = Query("一级行业", description="行业类别等级，如一级行业, 二级行业")
):
    """
    抓取当日所有申万行业的盘中实时估值与涨跌幅排行数据（已转换为日K线类似格式）。
    """
    return sws_provider.get_bulk_industry_kline(indextype)


@router.get("/api/sws/bulk-industry-kline", tags=["申万行业"], summary="获取指定等级下所有行业的当日K线数据(批量)")
async def get_sws_bulk_industry_kline(
    indextype: str = Query("一级行业", description="行业类别等级，如一级行业, 二级行业")
):
    """
    批量抓取当日所有申万行业的估值行情，并从详情接口动态提取日期，拼接为标准K线格式返回。
    """
    return sws_provider.get_bulk_industry_kline(indextype)
