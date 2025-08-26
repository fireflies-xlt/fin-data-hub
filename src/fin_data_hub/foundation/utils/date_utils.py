import datetime

def future_year_end(years: int = 0) -> str:
    """
    获取未来几年的年底日期
    """
    return f"{get_future_years(years)}1231"


def future_year_start(years: int = 0) -> str:
    """
    获取未来几年的年初日期
    """
    return f"{get_future_years(years)}0101"


def get_future_years(years: int = 0) -> str:
    """
    获取未来几年的日期
    """
    return f"{datetime.datetime.now().year + years}"


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

