# -*- coding: utf-8 -*-
"""
交易所交易基金（场内基金）代码规则定义及过滤模块
"""

# 沪市场内基金细分规则 (5打头)
SH_EXCHANGE_RULES = {
    # 优先精确匹配 3 位前缀
    "501": "沪市上市开放式基金(LOF)",
    "505": "沪市创新型封闭式证券投资基金",
    "506": "沪市科创板上市开放式基金(LOF)",
    "508": "沪市基础设施公募REITs",
    "500": "沪市传统封闭式基金",
    "588": "沪市科创板ETF",
    # 匹配 2 位前缀
    "51": "沪市主力交易型开放式指数基金(ETF)（含股票型ETF、511货币/债券ETF、513跨境ETF、518商品/黄金ETF等）",
    "56": "沪市新增交易型开放式指数基金(ETF)",
    "58": "沪市科创板及战略新兴产业主题ETF(除588外)",
    "50": "沪市传统封闭式/LOF/创新封闭式基金(除501、505、506、508外)",
    "52": "沪市创新型基金/特殊策略产品段"
}

# 深市场内基金细分规则 (1打头)
SZ_EXCHANGE_RULES = {
    # 优先精确匹配 3 位前缀
    "180": "深市基础设施公募REITs",
    "184": "深市传统契约型封闭式基金",
    "159": "深市主力交易型开放式指数基金(ETF)（包含1595xx、1596xx、1599xx等）",
    # 匹配 2 位前缀
    "15": "深市交易型开放式指数基金(ETF)/分级子份额等场内产品(除159外)",
    "16": "深市上市开放式基金(LOF) 及 早期混用16号段深市ETF",
    "18": "深市封闭式基金/特殊交易产品(除180、184外)",
    "19": "深市新增REITs及创新型场内交易基金"
}

def get_fund_exchange_type(fund_code: str) -> str | None:
    """
    根据基金代码判定其在交易所上市交易的基金类型和说明。
    如果该代码不属于任何沪深交易所上市交易基金规则，返回 None。
    
    规则依据：
    1. 代码非空，且必须为 6 位数字代码。
    2. 沪市场内基金（前缀为：50, 51, 52, 56, 58）
    3. 深市场内基金（前缀为：15, 16, 18, 19）
    """
    if not fund_code or not isinstance(fund_code, str):
        return None
        
    # 规整代码：只保留数字并确保是 6 位
    clean_code = "".join(filter(str.isdigit, fund_code))
    if len(clean_code) != 6:
        return None
        
    prefix_3 = clean_code[:3]
    prefix_2 = clean_code[:2]
    
    # 优先匹配 3 位前缀，其次匹配 2 位前缀
    # 沪市判断 (以 5 开头)
    if clean_code.startswith('5'):
        if prefix_3 in SH_EXCHANGE_RULES:
            return SH_EXCHANGE_RULES[prefix_3]
        if prefix_2 in SH_EXCHANGE_RULES:
            return SH_EXCHANGE_RULES[prefix_2]
        # 保底逻辑：未能匹配到精细规则，但以 5 开头，仍判定为上交所上市基金
        return "沪市交易所基金(保底匹配)"
            
    # 深市判断 (以 1 开头)
    if clean_code.startswith('1'):
        if prefix_3 in SZ_EXCHANGE_RULES:
            return SZ_EXCHANGE_RULES[prefix_3]
        if prefix_2 in SZ_EXCHANGE_RULES:
            return SZ_EXCHANGE_RULES[prefix_2]
        # 保底逻辑：未能匹配到精细规则，但以 1 开头，仍判定为深交所上市基金
        return "深市交易所基金(保底匹配)"
            
    return None

def is_exchange_traded_fund(fund_code: str) -> bool:
    """
    判断基金代码是否是可以在交易所进行交易的场内基金。
    """
    return get_fund_exchange_type(fund_code) is not None


def clean_dataframe(df) -> list:
    """
    将 akshare 返回的 DataFrame 清洗为可 JSON 序列化的记录列表：
    仅保留场内基金，NaN→None，日期转字符串，基金代码补零。
    """
    import datetime

    import pandas as pd

    # 先做一层交易所交易基金的过滤，仅保留场内交易基金
    df['temp_code'] = df['基金代码'].astype(str).str.zfill(6)
    df = df[df['temp_code'].apply(is_exchange_traded_fund)]
    df = df.drop(columns=['temp_code'])

    records = df.to_dict(orient="records")
    cleaned_records = []
    for r in records:
        cleaned_r = {}
        for k, v in r.items():
            if pd.isna(v):
                cleaned_r[k] = None
            else:
                if isinstance(v, (pd.Timestamp, datetime.date, datetime.datetime)):
                    cleaned_r[k] = v.strftime("%Y-%m-%d")
                else:
                    if k == "基金代码":
                        cleaned_r[k] = str(v).zfill(6)
                    else:
                        cleaned_r[k] = v
        cleaned_records.append(cleaned_r)
    return cleaned_records
