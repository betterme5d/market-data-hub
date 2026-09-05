# -*- coding: utf-8 -*-
"""
AkShare 公共数据端点（akshare 按需延迟导入，避免启动负担）。
"""
import logging

from fastapi import APIRouter, HTTPException, Query

from core.filters import clean_dataframe

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/public/fund_purchase_em", tags=["AkShare公共数据"], summary="获取东方财富基金申购状态表")
async def get_fund_purchase_em():
    """
    调用 AkShare 的 fund_purchase_em 数据接口，获取公募基金的申购与赎回状态限制。
    """
    try:
        import akshare as ak
        df = ak.fund_purchase_em()
        return clean_dataframe(df)
    except Exception as e:
        logger.error(f"Failed to fetch fund_purchase_em: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/public/fund_scale_open_sina", tags=["AkShare公共数据"], summary="获取新浪开放式基金规模数据")
async def get_fund_scale_open_sina(
    symbol: str = Query(..., description="基金类别，例如：股票型基金, 混合型基金, 债券型基金, 指数型基金, QDII基金")
):
    """
    通过 AkShare 获取新浪财经上指定类型的开放式基金规模排行和份额明细。
    """
    try:
        import akshare as ak
        df = ak.fund_scale_open_sina(symbol=symbol)
        return clean_dataframe(df)
    except Exception as e:
        logger.error(f"Failed to fetch fund_scale_open_sina for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
