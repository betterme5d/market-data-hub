# -*- coding: utf-8 -*-
"""
基金数据端点：持仓 / 基本信息 / 东财估值 / 交易所基金列表 / 统一净值。
"""
import logging

from fastapi import APIRouter, HTTPException, Path, Query

from core.models import FundNavResponse
from providers.exchanges.exchange import ExchangeProvider
from providers.exchanges.listing_dates import ListingDateProvider
from providers.funds.base_nav import FundNavSource
from providers.funds.cmtidp import CmtidpSource
from providers.funds.eastmoney import EastmoneySource
from providers.funds.establish_dates import EstablishDateProvider
from providers.funds.fund_nav import FundNavProvider
from providers.funds.fund_profile import (
    EastmoneyProfileSource,
    collect_fund_dates,
    collect_fund_profiles,
    get_fund_dates,
)

logger = logging.getLogger(__name__)

router = APIRouter()

eastmoney_provider = EastmoneySource()
exchange_provider = ExchangeProvider()
eastmoney_profile_provider = EastmoneyProfileSource()
listing_date_provider = ListingDateProvider()
establish_date_provider = EstablishDateProvider()
fund_nav_provider = FundNavProvider()

# 净值数据源注册表：source -> FundNavSource 实现（兼容保留）
_NAV_PROVIDERS: dict[str, FundNavSource] = {
    "eastmoney": eastmoney_provider,
    "cmtidp": CmtidpSource(),
}


@router.get("/fund/{symbol}/portfolio", tags=["基金数据"], summary="获取基金持仓结构")
async def get_fund_portfolio(
    symbol: str = Path(..., description="基金代码，如 510300"),
    year: int = Query(..., description="查询的报告年份，如 2024")
):
    """
    获取单只公募基金在特定年份的重仓股票及持仓比重等数据。
    """
    try:
        return await eastmoney_provider.get_portfolio(symbol, year)
    except Exception as e:
        logger.error(f"Get portfolio failed for {symbol} in {year}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fund/{symbol}/info", tags=["基金数据"], summary="获取基金基本信息")
async def get_fund_info(
    symbol: str = Path(..., description="基金代码")
):
    """
    获取单只公募基金的名称、管理费率、托管费率、成立时间等基本概况。
    """
    try:
        return eastmoney_provider.get_fund_info(symbol)
    except Exception as e:
        logger.error(f"Get fund info failed for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/funds/dates", tags=["基金数据"], summary="获取基金关键日期汇总（成立/上市日期，批量采集）")
@router.get("/api/v1/funds/profiles", tags=["基金数据"], summary="获取基金关键日期汇总（旧接口别名，兼容用）", deprecated=True)
async def get_fund_dates_batch():
    """
    合并东财场内基金（成立日期）与沪深交易所列表（上市日期）的全量基金关键日期。
    供 C# 等后台任务批量回填 funds 表。
    响应同时提供 'dates' 与 'profiles' 键，兼顾新标准与旧客户端兼容。
    """
    try:
        items = await collect_fund_dates()
        return {"dates": items, "profiles": items}
    except Exception as e:
        logger.error(f"Get fund dates failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/funds/{code}/dates", tags=["基金数据"], summary="获取单只基金关键日期汇总（成立/上市日期）")
async def get_single_fund_dates(code: str = Path(..., description="基金代码（6 位）")):
    """
    获取单只基金的成立日期与上市日期汇总。
    """
    try:
        return await get_fund_dates(code)
    except Exception as e:
        logger.error(f"Get fund dates failed for {code}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fund/{symbol}/profile", tags=["基金数据"], summary="获取单只基金档案（旧接口别名，兼容用）", deprecated=True)
async def get_fund_profile(
    symbol: str = Path(..., description="基金代码，如 161724")
):
    """
    从东财 F10 基本概况页解析单只基金的成立日期，供批量档案覆盖不到的基金兜底。
    """
    try:
        return await eastmoney_profile_provider.get_fund_profile(symbol.strip())
    except Exception as e:
        logger.error(f"Get fund profile failed for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/funds/establish-dates", tags=["基金数据"], summary="获取沪深基金成立日期")
async def get_establish_dates(
    category: str | None = Query(None, description="分类：fb (场内ETF/封闭) | kf (开放式/LOF) | all (缺省合并两者)"),
    refresh: bool = Query(False, description="true 时强制重新拉取上游并覆盖本地文件缓存"),
):
    """
    返回在证券交易所上市的沪深基金（ETF、LOF、REITs、封闭式基金）成立日期。

    数据来源页面引用（排查问题请直接访问对比）：
    - 场内交易（ETF/封闭）：https://fund.eastmoney.com/data/fbsfundranking.html (东财-场内交易基金排行)
    - 开放基金（LOF/主动等）：https://fund.eastmoney.com/data/fundranking.html (东财-开放基金排行)
    - 底层接口：https://fund.eastmoney.com/data/rankhandler.aspx
    - 交易所上市白名单：上交所 (https://www.sse.com.cn/assortment/fund/list/)、深交所 (https://www.szse.cn/market/product/list/all/index.html)

    过滤与缓存策略：
    - 采用本地文件缓存（data/ 目录），文件 mtime 超过 24 小时会自动重新拉取；
    - 传 refresh=true 可主动强制刷新缓存；
    - 严格过滤：仅保留在证券交易所真实上市的 ETF/LOF/REITs 品种（与上交所/深交所上市清单严格对齐），
      自动滤除 519xxx、110xxx 等虽然以 5 或 1 开头但属于纯场外开放式基金的品种，以及其余 2 万+ 纯场外基金。
    """
    try:
        return await establish_date_provider.get_all(category=category, force=refresh)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get establish dates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/funds/establish-date/{code}", tags=["基金数据"], summary="获取单只基金成立日期")
async def get_establish_date(code: str = Path(..., description="基金代码（6 位）")):
    """
    获取单只基金成立日期（仅支持交易所上市 ETF/LOF/REITs 基金）。

    数据来源页面引用：
    - 天天基金 F10 概况页：https://fundf10.eastmoney.com/jbgk_{code}.html

    查询流转：
    - 先查进程内存 → 再查本地文件缓存 → 校验是否在交易所上市名单中（非上市基金直接返回 None）→ 在所标的请求东财 F10 概况页兜底。
    """
    try:
        return await establish_date_provider.get_one(code)
    except Exception as e:
        logger.error(f"Failed to get establish date for {code}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/exchange/funds", tags=["基金数据"], summary="获取上交所和深交所ETF/LOF列表")
async def get_exchange_funds():
    """
    抓取并解析上交所和深交所的最新 ETF 和 LOF 基金列表。
    用于手动数据同步。
    """
    try:
        return await exchange_provider.fetch_all_exchange_funds()
    except Exception as e:
        logger.error(f"Failed to get exchange funds: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/exchange/funds/{exchange}", tags=["基金数据"], summary="获取指定交易所ETF/LOF列表")
async def get_exchange_funds_by(exchange: str):
    """
    抓取指定交易所（SH | SZ）的最新 ETF 和 LOF 基金列表。
    """
    try:
        return await exchange_provider.fetch_funds(exchange=exchange)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get exchange funds for {exchange}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/exchange/listing-dates", tags=["基金数据"], summary="获取沪深全部基金上市日期")
async def get_listing_dates(
    exchange: str | None = Query(None, description="交易所：SH | SZ；缺省返回沪深合并"),
    refresh: bool = Query(False, description="true 时强制重新拉取上游并覆盖文件缓存"),
):
    """
    返回沪深交易所全部基金上市日期。

    上交所页面引用：https://www.sse.com.cn/assortment/fund/list/
    深交所页面引用：https://www.szse.cn/market/product/list/all/index.html

    采用本地文件缓存（data/ 目录），文件 mtime 超过一天会自动重新全量拉取；
    传 refresh=true 可主动强制刷新。
    """
    try:
        return await listing_date_provider.get_all(exchange=exchange, force=refresh)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get listing dates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/exchange/listing-date/{code}", tags=["基金数据"], summary="获取指定基金上市日期")
async def get_listing_date(code: str = Path(..., description="基金代码（6 位）")):
    """
    获取单只基金上市日期。按代码首字路由交易所：5 开头→上交所，1 开头→深交所。

    上交所页面引用：https://www.sse.com.cn/assortment/fund/list/
    深交所页面引用：https://www.szse.cn/market/product/list/all/index.html

    先查内存 → 再查文件缓存 → 均未命中才全量拉取上游并回写；查不到返回 list_date 为 null。
    """
    try:
        return await listing_date_provider.get_one(code)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get listing date for {code}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/api/v1/funds/{code}/navs",
    response_model=FundNavResponse,
    tags=["基金数据"],
    summary="获取指定基金历史净值（带时序增量缓存）",
)
async def get_fund_navs(
    code: str = Path(..., description="基金代码（6 位数字）"),
    start_date: str = Query(..., description="起始日期 (YYYY-MM-DD)，必填"),
    end_date: str = Query(..., description="结束日期 (YYYY-MM-DD)，必填"),
    source: str | None = Query(None, description="数据源：eastmoney (默认) | cmtidp"),
):
    """
    获取指定基金在 [start_date, end_date] 区间的历史净值列表。
    内部接入通用时序 Parquet 增量缓存，具备闭合区间精准剪裁与 T 日未发布延迟保护。
    """
    clean_code = code.strip()
    s_date = start_date.strip()
    e_date = end_date.strip()

    if not (len(clean_code) == 6 and clean_code.isdigit()):
        raise HTTPException(status_code=400, detail=f"Invalid fund code: {code}, must be 6 digits")

    if s_date > e_date:
        raise HTTPException(
            status_code=400,
            detail=f"start_date ({s_date}) cannot be after end_date ({e_date})"
        )

    try:
        items = await fund_nav_provider.get_fund_nav_history(
            clean_code, start_date=s_date, end_date=e_date, source=source
        )
        resolved_source = (source or "eastmoney").strip().lower()
        return FundNavResponse(source=resolved_source, count=len(items), items=items)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get fund navs for {code} ({s_date} ~ {e_date}): {e}")
        raise HTTPException(status_code=502, detail=f"Get fund navs failed: {e}")


@router.get("/api/fund-nav/latest", tags=["基金数据"], summary="获取最新一期全量基金净值")
async def get_latest_all_nav(
    source: str = Query(..., description="数据源：cmtidp | eastmoney")
):
    """
    获取最新一期所有基金净值。各源"最新"语义：
    eastmoney 取当前最新交易日全量；cmtidp 取最新更新日全量。
    """
    try:
        items = await fund_nav_provider.get_latest_all_nav(source=source)
        return FundNavResponse(source=source.strip().lower(), count=len(items), items=items)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Get latest all nav failed for {source}: {e}")
        raise HTTPException(status_code=502, detail=f"{source} upstream failed: {e}")


@router.get(
    "/api/fund-nav/history",
    response_model=FundNavResponse,
    tags=["基金数据"],
    summary="获取指定基金历史净值（旧接口别名，兼容用）",
    deprecated=True,
)
async def get_fund_nav_history(
    source: str = Query(..., description="数据源：cmtidp | eastmoney"),
    code: str = Query(..., description="基金代码（6 位）"),
    start_date: str | None = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: str | None = Query(None, description="结束日期 YYYY-MM-DD"),
):
    """
    兼容旧端点，内部代理至带缓存的实现。
    为避免上游密集翻页反爬熔断，必须显式提供 start_date 与 end_date。
    """
    if not start_date or not end_date:
        raise HTTPException(
            status_code=400,
            detail="Both start_date and end_date are required in YYYY-MM-DD format to prevent unbounded pagination."
        )
    return await get_fund_navs(
        code=code, start_date=start_date, end_date=end_date, source=source
    )
