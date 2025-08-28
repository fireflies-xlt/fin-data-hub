import datetime

def get_stock_start_date() -> str:
    """
    获取表的开始日期
    """
    return '19900101'

def future_year_end(years: int = 0) -> str:
    """
    获取未来几年的年底日期
    """
    return f"{datetime.datetime.now().year + years}1231"

def current_date_ymd() -> str:
    """
    获取当前日期
    """
    return datetime.datetime.now().strftime('%Y%m%d')

def future_year_start(years: int = 0) -> str:
    """
    获取未来几年的年初日期
    """
    return f"{datetime.datetime.now().year + years}0101"

def next_day(date_str: str) -> str:
    """
    获取下一天的日期
    
    Args:
        date_str: 日期字符串，格式为 'YYYYMMDD'
    
    Returns:
        下一天的日期字符串，格式为 'YYYYMMDD'
    """
    date_obj = datetime.datetime.strptime(date_str, '%Y%m%d')
    next_date = date_obj + datetime.timedelta(days=1)
    return next_date.strftime('%Y%m%d')

