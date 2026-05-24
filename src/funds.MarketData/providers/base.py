from abc import ABC, abstractmethod
from typing import Optional, Tuple
from core.models import UnifiedQuoteOut

class BaseProvider(ABC):
    @abstractmethod
    async def get_quote(self, symbol: str) -> Tuple[UnifiedQuoteOut, int]:
        """
        获取实时行情数据
        返回: (QuoteModel, CacheTTLSeconds)
        """
        pass

    @abstractmethod
    async def get_history(self, symbol: str, period: str, interval: str, 
                          start: Optional[str], end: Optional[str], adj: str) -> dict:
        """
        获取历史 K 线数据
        """
        pass
