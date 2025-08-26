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