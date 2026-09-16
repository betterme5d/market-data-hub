# -*- coding: utf-8 -*-
"""场内基金终止上市取数。

按 `ARCHITECTURE.md` 三词契约组织：
- `parser`          纯函数，公告正文 → 日期字段（无 IO，易测）
- `sse_bulletin`    上交所 Source + Probe
- `szse_bulletin`   深交所 Source + Probe

退市相关设计说明见 docs/2026-09-12-fund-delisting-data-source.md
"""
