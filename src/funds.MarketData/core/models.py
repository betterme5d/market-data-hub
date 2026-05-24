from pydantic import BaseModel, Field
from typing import Optional, Union, Literal
from datetime import datetime

# 1. 共有基类
class BaseQuote(BaseModel):
    symbol: str
    price: float
    date: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    name: Optional[str] = None
    currency: Optional[str] = None
    exchange: Optional[str] = None

# 2. 股票专属行情模型
class StockQuote(BaseQuote):
    security_type: Literal["stock"] = "stock"
    lastClose: float
    change: float
    percent: float
    open: float
    high: float
    low: float
    volume: int
    amount: float

# 3. 期货专属行情模型（占位）
class FutureQuote(BaseQuote):
    security_type: Literal["future"] = "future"
    open_interest: int
    settlement_price: float
    pre_settlement: float

# 4. 联合多态返回模型
UnifiedQuoteOut = Union[StockQuote, FutureQuote]
