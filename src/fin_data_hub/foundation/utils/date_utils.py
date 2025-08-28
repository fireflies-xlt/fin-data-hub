import datetime

def get_stock_start_date() -> str:
    """
    获取表的开始日期
    """
    return '19900101'

def future_year_start(years: int = 0) -> str:
    """
    获取未来几年的年初日期
    """
    return f"{datetime.datetime.now().year + years}0101"

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

def next_day(date_str: str) -> str:
    """
    获取下一天的日期
    
    Args:
        date_str: 日期字符串，格式为 'YYYYMMDD'
    
    Returns:
        下一天的日期字符串，格式为 'YYYYMMDD'
    """
    return add_days(date_str, 1)    

def add_year(date_str: str, years: int) -> str:
    """
    添加年份

    Args:
        date_str: 日期字符串，格式为 'YYYYMMDD'
        years: 年数
    
    Returns:
        添加年份后的日期字符串，格式为 'YYYYMMDD'
    """
    date_obj = datetime.datetime.strptime(date_str, '%Y%m%d')
    new_date = date_obj.replace(year=date_obj.year + years)
    return new_date.strftime('%Y%m%d')

def add_days(date_str: str, days: int) -> str:
    """
    添加天数

    Args:
        date_str: 日期字符串，格式为 'YYYYMMDD'
        days: 天数
    
    Returns:
        添加天数后的日期字符串，格式为 'YYYYMMDD'
    """
    date_obj = datetime.datetime.strptime(date_str, '%Y%m%d')
    new_date = date_obj + datetime.timedelta(days=days)
    return new_date.strftime('%Y%m%d')