# -*- coding: utf-8 -*-
"""
海外 ETF 折溢价率端点（KraneShares / iShares）。
"""
import logging

from fastapi import APIRouter, HTTPException, Path, Query

from providers.misc.ishares import IsharesProvider
from providers.misc.kraneshares import KraneSharesProvider

logger = logging.getLogger(__name__)

router = APIRouter()

krane_provider = KraneSharesProvider()
ishares_provider = IsharesProvider()


@router.get("/api/krane/premium-discount/{pid}", tags=["折溢价率"], summary="获取 KraneShares 基金折溢价率")
async def get_krane_premium_discount(
    pid: str = Path(..., description="产品 ID，例如 KWEB"),
    start: str = Query(..., description="开始日期 (格式: YYYY-MM-DD)"),
    end: str = Query(..., description="结束日期 (格式: YYYY-MM-DD)")
):
    """
    拉取 KraneShares 官网的 ETF 产品历史折溢价数据，并按日期范围过滤返回。
    """
    try:
        return await krane_provider.get_premium_discount(pid, start, end)
    except Exception as e:
        logger.error(f"Get KraneShares premium discount failed for {pid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/ishares/premium-discount/{product_path:path}", tags=["折溢价率"], summary="获取 iShares 基金折溢价率")
async def get_ishares_premium_discount(
    product_path: str = Path(..., description="iShares 官网的产品路径标识"),
    start: str = Query(..., description="开始日期 (格式: YYYY-MM-DD)"),
    end: str = Query(..., description="结束日期 (格式: YYYY-MM-DD)")
):
    """
    拉取并解析 iShares 官网的 ETF 产品历史折溢价率数据。
    """
    try:
        return await ishares_provider.get_premium_discount(product_path, start, end)
    except Exception as e:
        logger.error(f"Get iShares premium discount failed for {product_path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
