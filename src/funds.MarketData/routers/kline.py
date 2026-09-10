# -*- coding: utf-8 -*-
"""
K 线标准接口路由。
提供 /api/v1/securities/{code}/kline RESTful 端点。
旧接口 /history/{symbol} 保留在 routers/quotes.py 做桥接，不在此文件重复定义。
"""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Path, Query

from core.exceptions import BusinessException
from core.models import KLineResponse
from providers.quotes.kline_provider import KLineProvider

logger = logging.getLogger(__name__)
router = APIRouter()
kline_provider = KLineProvider()


@router.get(
    "/api/v1/securities/{code}/kline",
    response_model=KLineResponse,
    tags=["K线行情"],
    summary="获取历史 K 线（标准接口，按数据源+复权类型严格 Parquet 物理隔离缓存）",
)
async def get_security_kline(
    code: str = Path(..., description="证券代码（任意格式：002092.SZ / SZ002092 / 510300.SH / AAPL.US）"),
    start_date: str = Query(..., description="起始日期 YYYY-MM-DD（必填）"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD（必填）"),
    source: Optional[str] = Query(None, description="数据源：xueqiu（已实现）；tencent / sina 为预留未实现（返回 501）"),
    adjust: str = Query("none", description="复权：none (不复权，默认) | hfq (后复权) | qfq (前复权；值随最新价重算，与增量缓存相冲，需显式指定)"),
    period: str = Query("day", description="周期：day (日K) | week (周K) | month (月K)"),
):
    """
    获取指定证券的历史 K 线数据。

    特性：
    - 自动识别并归一化任意格式的代码（雪球/腾讯/新浪格式均支持）
    - 按 (source, adjust, period) 三维严格隔离 Parquet 缓存，各数据源与复权口径数据互不干扰
    - 增量断点续传：已缓存区间零网络请求，毫秒级返回
    - 客户端断开自动取消：依托 CancelOnDisconnectMiddleware 全局生效
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail=f"start_date ({start_date}) cannot be after end_date ({end_date})"
        )
    try:
        return await kline_provider.get_kline(
            symbol=code,
            start_date=start_date,
            end_date=end_date,
            source=source,
            adjust=adjust,
            period=period,
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except BusinessException as be:
        raise HTTPException(status_code=400, detail=str(be))
    except NotImplementedError as nie:
        # 预留数据源尚未实现：属于「已知不支持」，不是服务端故障，勿报 500 触发告警
        raise HTTPException(status_code=501, detail=str(nie))
    except Exception as e:
        logger.error(f"Get kline failed for {code}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
