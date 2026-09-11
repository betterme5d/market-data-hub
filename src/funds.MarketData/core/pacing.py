# -*- coding: utf-8 -*-
"""分页/切片类上游请求之间的礼貌延时（随机抖动）。

默认 0.2~0.5 秒（UPSTREAM_PAGE_MIN_DELAY / UPSTREAM_PAGE_MAX_DELAY 可配），
用于避免对上游（交易所、政府披露平台等）造成密集请求压力。
注：SSE 份额有自己的更严格的一套（SSE_SHARE_MIN/MAX_DELAY = 0.5~3.0 秒），不在此处。
"""
import asyncio
import random

from core import config

MIN_DELAY = config.UPSTREAM_PAGE_MIN_DELAY
MAX_DELAY = config.UPSTREAM_PAGE_MAX_DELAY


async def polite_delay() -> None:
    """在两次上游请求之间随机休眠 [MIN_DELAY, MAX_DELAY] 秒；MAX_DELAY<=0 时不休眠。"""
    if MAX_DELAY <= 0:
        return
    await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

