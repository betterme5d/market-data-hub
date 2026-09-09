# -*- coding: utf-8 -*-
"""
集中配置：所有外部上游地址、超时与健康检查参数统一在这里读取环境变量。
不允许在 provider / router 中散落 os.getenv。
"""
import os


def _get(key: str, default: str) -> str:
    return os.getenv(key, default)


def _get_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


def _get_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except ValueError:
        return default


# ---------- 日志 ----------
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# ---------- Valkey ----------
VALKEY_HOST = _get("VALKEY_HOST", "valkey")
VALKEY_PORT = _get_int("VALKEY_PORT", 6380)
VALKEY_PASSWORD = _get("VALKEY_PASSWORD", "")

# ---------- 上游数据源地址 ----------
CFETS_BASE_URL = _get("CFETS_BASE_URL", "https://www.chinamoney.com.cn")
CMTIDP_BASE_URL = _get("CMTIDP_BASE_URL", "http://eid.csrc.gov.cn")
# 天天基金（东方财富）：历史净值接口域名 + 全量最新净值接口
EASTMONEY_F10_BASE_URL = _get("EASTMONEY_F10_BASE_URL", "https://api.fund.eastmoney.com")
EASTMONEY_FUND_BASE_URL = _get("EASTMONEY_FUND_BASE_URL", "https://fund.eastmoney.com")
PLAYWRIGHT_GATEWAY_URL = _get("PLAYWRIGHT_GATEWAY_URL", "http://funds.playwright.gateway:8081")

# ---------- 健康检查参数 ----------
# 被动指标滑动窗口：统计最近 N 次调用
HEALTH_WINDOW_SIZE = _get_int("HEALTH_WINDOW_SIZE", 20)
# 成功率低于该值判定 degraded
HEALTH_MIN_SUCCESS_RATE = _get_float("HEALTH_MIN_SUCCESS_RATE", 0.5)
# 连续窗口内全部失败判定 down（要求窗口内至少有这么多样本）
HEALTH_DOWN_MIN_SAMPLES = _get_int("HEALTH_DOWN_MIN_SAMPLES", 3)
# 超过该秒数没有任何业务调用 → idle，触发主动探测兜底
HEALTH_IDLE_SECONDS = _get_int("HEALTH_IDLE_SECONDS", 600)
# 主动探测轮询间隔（秒）
HEALTH_PROBE_INTERVAL = _get_int("HEALTH_PROBE_INTERVAL", 300)
# 单次主动探测超时（秒）
HEALTH_PROBE_TIMEOUT = _get_float("HEALTH_PROBE_TIMEOUT", 5.0)

# ---------- 透传请求的默认超时（秒） ----------
UPSTREAM_TIMEOUT = _get_float("UPSTREAM_TIMEOUT", 30.0)

# ---------- 基金份额数据源参数 ----------
SSE_SHARE_MIN_DELAY = _get_float("SSE_SHARE_MIN_DELAY", 0.5)
SSE_SHARE_MAX_DELAY = _get_float("SSE_SHARE_MAX_DELAY", 3.0)
