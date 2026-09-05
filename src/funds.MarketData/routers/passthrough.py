# -*- coding: utf-8 -*-
"""
路径保持型薄代理端点（第一批试点：节假日 + CFETS）。

刻意保持与上游一致的路径，使 C# 侧只需把 BaseUrl 从外部域名切换到本服务即可，
解析/分页逻辑零改动。每次转发都会记录到被动健康指标。
"""
import logging
import time

import httpx
from fastapi import APIRouter, HTTPException, Path, Request

from core import health as health_svc
from providers.funds.cmtidp import CmtidpProvider
from providers.misc.cfets import CfetsProvider
from providers.misc.holidays import HolidaysProvider

logger = logging.getLogger(__name__)

router = APIRouter()

holidays_provider = HolidaysProvider()
cfets_provider = CfetsProvider()
cmtidp_provider = CmtidpProvider()


@router.get("/v1/workdays/{year}", tags=["薄代理"], summary="节假日工作日表（代理 api.jiejiariapi.com）")
async def get_workdays(year: int = Path(..., description="年份，如 2026")):
    started = time.monotonic()
    try:
        data = await holidays_provider.get_workdays(year)
        health_svc.record_call("holidays", True, (time.monotonic() - started) * 1000)
        return data
    except Exception as e:
        health_svc.record_call("holidays", False, (time.monotonic() - started) * 1000, error=str(e))
        logger.error(f"Proxy workdays({year}) failed: {e}")
        raise HTTPException(status_code=502, detail=f"holidays upstream failed: {e}")


@router.get("/ags/ms/cm-u-bk-ccpr/CcprHisNew", tags=["薄代理"], summary="CFETS 历史中间价（代理 chinamoney.com.cn）")
async def get_cfets_history(request: Request):
    query_string = request.url.query
    started = time.monotonic()
    try:
        data = await cfets_provider.get_middle_price_history(query_string)
        health_svc.record_call("cfets", True, (time.monotonic() - started) * 1000)
        return data
    except httpx.HTTPStatusError as e:
        health_svc.record_call("cfets", False, (time.monotonic() - started) * 1000, error=str(e))
        logger.error(f"Proxy CFETS history failed: {e}")
        raise HTTPException(status_code=e.response.status_code, detail=f"cfets upstream status {e.response.status_code}")
    except Exception as e:
        health_svc.record_call("cfets", False, (time.monotonic() - started) * 1000, error=str(e))
        logger.error(f"Proxy CFETS history failed: {e}")
        raise HTTPException(status_code=502, detail=f"cfets upstream failed: {e}")


@router.get("/fund/disclose/getPublicFundJZInfoMore.do", tags=["薄代理"], summary="CMTIDP 公募基金净值（代理 eid.csrc.gov.cn）")
async def get_cmtidp_fund_net_values(request: Request):
    query_string = request.url.query
    started = time.monotonic()
    try:
        data = await cmtidp_provider.get_fund_net_values(query_string)
        health_svc.record_call("cmtidp", True, (time.monotonic() - started) * 1000)
        return data
    except Exception as e:
        health_svc.record_call("cmtidp", False, (time.monotonic() - started) * 1000, error=str(e))
        logger.error(f"Proxy CMTIDP net values failed: {e}")
        raise HTTPException(status_code=502, detail=f"cmtidp upstream failed: {e}")
