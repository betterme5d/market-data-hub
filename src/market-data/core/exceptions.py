class BusinessException(Exception):
    """
    代表业务层面的数据异常（如标的不存在、格式不合法等）。
    此类异常不会被熔断器（Circuit Breaker）计入失败次数。
    """
    pass
