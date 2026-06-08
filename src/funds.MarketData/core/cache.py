import os
import redis
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("VALKEY_HOST", "valkey")
REDIS_PORT = int(os.getenv("VALKEY_PORT", 6380))
REDIS_PWD = os.getenv("VALKEY_PASSWORD", "")

try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PWD,
        decode_responses=True,
        socket_timeout=5
    )
    redis_client.ping()
    logger.info(f"Successfully connected to Valkey at {REDIS_HOST}:{REDIS_PORT}")
except Exception as r_ex:
    logger.error(f"Failed to connect to Valkey: {r_ex}")
    redis_client = None

def get_cached_quote(symbol: str, provider: str = "yfinance") -> Optional[dict]:
    if not redis_client:
        return None
    try:
        key = f"{provider}:quote:{symbol}"
        data = redis_client.get(key)
        if data:
            return json.loads(data)
    except Exception as e:
        logger.warning(f"Get cached quote failed for {symbol}: {e}")
    return None

def set_cached_quote(symbol: str, data: dict, ttl: int, provider: str = "yfinance") -> None:
    if not redis_client:
        return
    try:
        key = f"{provider}:quote:{symbol}"
        redis_client.setex(key, ttl, json.dumps(data))
    except Exception as e:
        logger.warning(f"Set cached quote failed for {symbol}: {e}")

def get_cached_anchor(symbol: str, provider: str = "yfinance") -> Optional[dict]:
    if not redis_client:
        return None
    try:
        key = f"{provider}:anchor:{symbol}"
        data = redis_client.get(key)
        if data:
            return json.loads(data)
    except Exception as e:
        logger.warning(f"Get cached anchor failed for {symbol}: {e}")
    return None

def set_cached_anchor(symbol: str, data: dict, provider: str = "yfinance") -> None:
    if not redis_client:
        return
    try:
        key = f"{provider}:anchor:{symbol}"
        redis_client.set(key, json.dumps(data))
    except Exception as e:
        logger.warning(f"Set cached anchor failed for {symbol}: {e}")

def get_cached_valuations() -> Optional[list]:
    if not redis_client:
        return None
    try:
        key = "eastmoney:valuations:16_5"
        data = redis_client.get(key)
        if data:
            return json.loads(data)
    except Exception as e:
        logger.warning(f"Get cached valuations failed: {e}")
    return None

def set_cached_valuations(data: list, ttl: int = 180) -> None:
    if not redis_client:
        return
    try:
        key = "eastmoney:valuations:16_5"
        redis_client.setex(key, ttl, json.dumps(data))
    except Exception as e:
        logger.warning(f"Set cached valuations failed: {e}")

