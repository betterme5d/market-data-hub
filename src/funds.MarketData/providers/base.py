from abc import ABC, abstractmethod
from typing import Optional, Tuple
from core.models import UnifiedQuoteOut


class SourceProbe(ABC):
    """
    数据源健康探针。每个 provider 目录下的数据源实现一个，
    在应用装配时通过 core.health.register_probe 注册。
    probe() 抛出异常即视为该源当前不可用。
    """

    name: str = ""
    category: str = ""

    @abstractmethod
    async def probe(self) -> None:
        """对上游发起一次最轻量的真实调用；成功则静默返回，失败抛异常。"""
        raise NotImplementedError


class BaseProvider(ABC):
    @abstractmethod
    async def get_quote(self, symbol: str, with_depth: bool = False) -> Tuple[UnifiedQuoteOut, int]:
        """
        获取实时行情数据，包含可选的盘口深度
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
