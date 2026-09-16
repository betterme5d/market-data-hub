import math
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime

class DepthItem(BaseModel):
    price: Optional[float] = None
    volume: Optional[float] = None  # 对应成交量 (如手/张)

class MarketDepth(BaseModel):
    bids: List[DepthItem] = []  # 买一到买五
    asks: List[DepthItem] = []  # 卖一到卖五

class UnifiedQuote(BaseModel):
    symbol: str  # 抓取时使用的数据源Symbol，例如 "sh510300"
    price: float  # 当前实时价格/最新参考价格
    date: str     # 行情交易日期 (格式: YYYY-MM-DD)
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())  # 数据拉取时间 (ISO格式)
    name: Optional[str] = None  # 证券名称
    currency: Optional[str] = "CNY"  # 结算币种，如 CNY, HKD, USD
    exchange: Optional[str] = None  # 市场或交易所标识，如 SH, SZ, HK, US
    security_type: str = "stock"  # 证券类型，例如 stock, future, forex, index
    source: str = "unknown"  # 数据来源，例如 tencent, sina, xueqiu, yfinance
    depth: Optional[MarketDepth] = None  # 五档盘口深度行情



    # 核心行情基础属性 (可选)
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    last_close: Optional[float] = None  # 昨收价/昨结算价
    change: Optional[float] = None     # 涨跌额
    percent: Optional[float] = None    # 涨跌幅 (保存为百分数数值，例如 1.23% 存储为 1.23)
    volume: Optional[float] = None     # 成交量 (单位：股/份/手/张)
    amount: Optional[float] = None     # 成交额 (单位：元/原始币种)
    turnover_rate: Optional[float] = None  # 换手率 (保存为百分数数值，例如 1.23% 存储为 1.23)
    amplitude: Optional[float] = None  # 振幅 (保存为百分数数值，例如 1.23% 存储为 1.23)
    update_time: Optional[str] = None  # 数据源端提供的行情更新时间 (格式: YYYY-MM-DD HH:mm:ss)

    # 扩展特色属性
    iopv: Optional[float] = None        # 基金的估算盘中净值 (IOPV)
    vwap: Optional[float] = None        # 期货的成交均价 (Volume Weighted Average Price)
    nav: Optional[float] = None         # 基金的历史/最新单位净值
    cumulative_nav: Optional[float] = None # 基金的历史/最新累计净值

# 保持接口兼容别名
UnifiedQuoteOut = UnifiedQuote


class FundNav(BaseModel):
    """统一基金净值单条数据（各数据源映射为同一结构）。"""

    code: str  # 基金代码（6 位）
    nav_date: str  # 净值日期 (YYYY-MM-DD)
    unit_nav: Optional[float] = None  # 单位净值（份额净值）
    accum_nav: Optional[float] = None  # 累计净值
    daily_return: Optional[float] = None  # 日增长率 (%)
    subscribe_status: Optional[str] = None  # 申购状态
    redeem_status: Optional[str] = None  # 赎回状态
    dividend: Optional[str] = None  # 分红送配

    @field_validator("dividend", "subscribe_status", "redeem_status", mode="before")
    @classmethod
    def sanitize_str(cls, v):
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        s = str(v).strip()
        return s if s else None

    @field_validator("unit_nav", "accum_nav", "daily_return", mode="before")
    @classmethod
    def sanitize_float(cls, v):
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


class FundNavResponse(BaseModel):
    """统一净值接口响应包装。"""

    source: str  # 实际数据源（回显）
    count: int  # 本批条数
    items: List[FundNav] = []


class FundShare(BaseModel):
    """统一基金份额单条记录（对齐交易所与 C# 实体结构）。"""

    code: str  # 基金代码（6 位数字）
    share_date: str  # 份额日期 (YYYY-MM-DD)
    shares: Optional[float] = None  # 场内份额（单位：万份，对齐 C# 实体）
    raw_shares: Optional[float] = None  # 原始份额（单位：份）
    name: Optional[str] = None  # 基金简称

    @field_validator("code", mode="before")
    @classmethod
    def sanitize_code(cls, v):
        if v is None:
            return ""
        return str(v).strip()

    @field_validator("shares", "raw_shares", mode="before")
    @classmethod
    def sanitize_shares(cls, v):
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    @field_validator("name", mode="before")
    @classmethod
    def sanitize_name(cls, v):
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        s = str(v).strip()
        return s if s else None


class FundShareResponse(BaseModel):
    """统一基金份额接口响应包装。"""

    code: str  # 基金代码
    exchange: str  # 交易所标识 (szse | sse)
    count: int  # 本批条数
    items: List[FundShare] = []
    # 本区间内"上游某个分类接口失败、没能取全"的交易日（沪市逐日拉取才有）：
    # 调用方（C#）必须把这些日子排除在"上游无数据"标注之外，留给下次重取。
    incomplete_days: List[str] = []


class KLineBar(BaseModel):
    """标准单根 K 线条目，对齐雪球 column 字段名称，供所有数据源统一使用。"""

    date: str                              # 交易日期 YYYY-MM-DD
    open: float
    high: float
    low: float
    close: float
    volume: float                          # 成交量（手/份）
    amount: float                          # 成交额（元）
    change: Optional[float] = None         # 涨跌额
    percent: Optional[float] = None        # 涨跌幅（%）
    turnover_rate: Optional[float] = None  # 换手率（%）

    @field_validator("change", "percent", "turnover_rate", mode="before")
    @classmethod
    def sanitize_float(cls, v):
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


class KLineResponse(BaseModel):
    """统一 K 线接口响应包装，对齐 FundShareResponse 格式。"""

    code: str            # 归一化标准代码，如 002092.SZ
    source: str          # 实际数据源，如 xueqiu / tencent / sina
    period: str = "day"  # K 线周期：day | week | month
    adjust: str = "none"  # 复权类型：none（不复权，缓存安全默认）| hfq（后复权）| qfq（前复权，值随最新价重算）
    count: int           # 本批条数
    items: List[KLineBar] = []
