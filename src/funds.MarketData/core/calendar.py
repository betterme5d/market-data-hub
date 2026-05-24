import logging
import pandas_market_calendars as mcal
import pandas as pd
import holidays
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

# 交易所标识映射到 pandas_market_calendars 的名称
EXCHANGE_MAPPING = {
    "CN": "SSE",
    "HK": "XHKG",
    "US": "NYSE",
    "JP": "JPX",
    "GB": "LSE",
    "UK": "LSE"
}

HOLIDAY_TRANSLATIONS = {
    # 美国 US
    "New Year's Day": "元旦",
    "Martin Luther King Jr. Day": "马丁·路德·金纪念日",
    "Washington's Birthday": "华盛顿诞辰纪念日 (总统日)",
    "Memorial Day": "阵亡将士纪念日",
    "Juneteenth National Independence Day": "六月节 (国家独立纪念日)",
    "Independence Day": "独立日 (美国国庆)",
    "Labor Day": "劳动节",
    "Columbus Day": "哥伦布日",
    "Veterans Day": "退伍军人节",
    "Thanksgiving": "感恩节",
    "Christmas Day": "圣诞节",
    
    # 英国 UK/GB
    "Good Friday": "耶稣受难日",
    "Easter Monday": "复活节星期一",
    "Early May Bank Holiday": "五月初银行假日",
    "Spring Bank Holiday": "春季银行假日",
    "Summer Bank Holiday": "夏季银行假日",
    "Boxing Day": "节礼日",
    
    # 日本 JP
    "元日": "元旦",
    "成人の日": "成人之日",
    "建国記念の日": "建国纪念日",
    "天皇誕生日": "天皇诞辰",
    "春分の日": "春分",
    "昭和の日": "昭和之日",
    "宪法記念日": "宪法纪念日",
    "みどりの日": "绿之日",
    "こどもの日": "儿童节",
    "海の日": "海洋之日",
    "山の日": "山之日",
    "敬老の日": "敬老之日",
    "秋分の日": "秋分",
    "スポーツの日": "体育之日",
    "文化の日": "文化之日",
    "勤労感謝の日": "勤劳感谢日",
    "振替休日": "补假日"
}

def translate_holiday(name: str) -> str:
    if not name:
        return name
    
    is_observed = False
    clean_name = name
    if " (Observed)" in name:
        clean_name = name.replace(" (Observed)", "")
        is_observed = True
    elif " (observed)" in name:
        clean_name = name.replace(" (observed)", "")
        is_observed = True
        
    translated = HOLIDAY_TRANSLATIONS.get(clean_name, clean_name)
    if is_observed:
        return f"{translated} (补假)"
    return translated

def get_exchange_trading_days(exchange: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    获取指定交易所在指定日期范围内的完整日期列表及其交易状态与节假日名称 (YYYY-MM-DD)
    返回格式适配 C# TradeDay 属性，包含:
    - date: YYYY-MM-DD
    - exchange: 交易所标识 (CN/HK/US)
    - is_trading: 是否为交易日
    - holiday_name: 节假日名称 (全中文翻译，非节假日为 None)
    """
    exchange_upper = exchange.upper()
    cal_name = EXCHANGE_MAPPING.get(exchange_upper)
    if not cal_name:
        raise ValueError(f"Unsupported exchange '{exchange}'. Supported ones: {list(EXCHANGE_MAPPING.keys())}")
    
    try:
        cal = mcal.get_calendar(cal_name)
        # 获取在这个范围内的真实交易日表
        schedule = cal.schedule(start_date=start_date, end_date=end_date)
        # 提取真实交易日日期（格式为 YYYY-MM-DD 的 set）
        trading_days = set()
        if not schedule.empty:
            trading_days = {d.strftime("%Y-%m-%d") for d in schedule.index}
        
        # 构造完整的时间范围（包含工作日、周末和节假日）
        all_dates = pd.date_range(start=start_date, end=end_date)
        
        # 初始化对应国家/地区的节假日日历
        h_cal = None
        if exchange_upper == "CN":
            h_cal = holidays.CN(language="zh_CN")
        elif exchange_upper == "HK":
            h_cal = holidays.HK(language="zh_CN")
        elif exchange_upper == "US":
            h_cal = holidays.US()
        elif exchange_upper == "JP":
            h_cal = holidays.JP(language="ja")
        elif exchange_upper in ("GB", "UK"):
            h_cal = holidays.GB()

        result = []
        for d in all_dates:
            day_str = d.strftime("%Y-%m-%d")
            is_trade = day_str in trading_days
            
            # 使用 d.date() 传参，确保 holidays 库能够正确解析并激活延迟加载（lazy loading）
            holiday_name = None
            if h_cal is not None:
                raw_holiday = h_cal.get(d.date())
                holiday_name = translate_holiday(raw_holiday)

            result.append({
                "date": day_str,
                "exchange": exchange_upper,
                "is_trading": is_trade,
                "holiday_name": holiday_name
            })
            
        return result
    except Exception as e:
        logger.error(f"Error fetching trading days from pandas_market_calendars for {exchange} ({start_date} ~ {end_date}): {e}")
        raise
