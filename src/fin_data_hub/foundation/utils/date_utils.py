import datetime
import calendar

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

def month_end(date_str: str) -> str:
    """
    获取月份的最后一天

    Args:
        date_str: 日期字符串，格式为 'YYYYMMDD'
    
    Returns:
        月份的最后一天，格式为 'YYYYMMDD'
    """
    date_obj = datetime.datetime.strptime(date_str, '%Y%m%d')
    return date_obj.replace(day=calendar.monthrange(date_obj.year, date_obj.month)[1]).strftime('%Y%m%d')


def get_all_month_end(start_date: str, end_date: str) -> list[str]:
    """
    获取所有月份的最后一天

    Args:
        start_date: 开始日期，格式为 'YYYYMMDD'
        end_date: 结束日期，格式为 'YYYYMMDD'

    Returns:
        所有月份的最后一天，格式为 'YYYYMMDD'
    """
    start_obj = datetime.datetime.strptime(start_date, '%Y%m%d')
    end_obj = datetime.datetime.strptime(end_date, '%Y%m%d')
    
    month_end_dates: list[str] = []
    current = start_obj.replace(day=1)  # 从月初开始
    
    while current <= end_obj:
        # 获取当前月份的最后一天
        month_end_date = current.replace(day=calendar.monthrange(current.year, current.month)[1])
        month_end_dates.append(month_end_date.strftime('%Y%m%d'))
        
        # 移动到下一个月
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return month_end_dates


def get_all_quarter_end(start_date: str, end_date: str) -> list[str]:
    """
    获取所有季度的最后一天
    """
    start_obj = datetime.datetime.strptime(start_date, '%Y%m%d')
    end_obj = datetime.datetime.strptime(end_date, '%Y%m%d')
    
    quarter_end_dates: list[str] = []
    
    # 季度最后一天：0331, 0630, 0930, 1231
    quarter_end_patterns = ['0331', '0630', '0930', '1231']
    
    # 从开始年份开始遍历
    current_year = start_obj.year
    
    while current_year <= end_obj.year:
        for pattern in quarter_end_patterns:
            quarter_end_date = f"{current_year}{pattern}"
            quarter_end_obj = datetime.datetime.strptime(quarter_end_date, '%Y%m%d')
            
            # 如果日期在范围内，添加到结果中
            if start_obj <= quarter_end_obj <= end_obj:
                quarter_end_dates.append(quarter_end_date)
        
        current_year += 1
    
    return quarter_end_dates