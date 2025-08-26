import logging
from typing import Any, Callable

import pandas as pd
import tushare as ts
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from fin_data_hub.config import config
from fin_data_hub.foundation.mysql.mysql_engine import mysql_engine, table_exists
from fin_data_hub.foundation.utils.date_utils import future_year_end

logger = logging.getLogger(__name__)

_tushare_client: Any = None
_trade_calendar_table = 'trade_calendar'

def wrap_tushare(func: Callable) -> Callable:
    """
    包装 tushare 函数, 标记为 tushare 函数
    1. 每次调用时都重新初始化 tushare 客户端
    """
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

def get_tushare_client() -> Any:
    """获取 Tushare 客户端"""
    global _tushare_client
    if _tushare_client is None:
        assert config.tushare_token, "Tushare token is not set"
        ts.set_token(config.tushare_token)  
        _tushare_client = ts.pro_api()
    return _tushare_client

def sync_trade_calendar_data() -> pd.DataFrame | None:
    """同步交易日历数据 - 可测试的业务逻辑"""
    # 查询数据库中最新的日期

    if table_exists(_trade_calendar_table):
        query = f"SELECT MAX(cal_date) as last_date FROM {_trade_calendar_table}"
        result = pd.read_sql(query, mysql_engine())
        last_date = result['last_date'].iloc[0]
    else:
        last_date = None
    
    # 获取未来3年的结束日期
    year_end = future_year_end(3)
    
    # 如果最新日期已经是未来3年后，说明数据已是最新
    if last_date and str(last_date).replace('-', '') >= year_end:
        logger.info("交易日历数据已是最新，无需更新")
        return None
    
    if last_date is None:
        # 如果数据库为空，从1990年开始拉取
        start_date = '19900101'
    else:
        # 从最新日期后一天开始拉取
        start_date = str(last_date + 1).replace('-', '')
    
    df = get_tushare_client().trade_cal(
        start_date=start_date,
        end_date=year_end,
    )
    df.to_sql(_trade_calendar_table, con=mysql_engine(), if_exists='append', index=False)
    logger.info(f"获取到 {len(df)} 条交易日历数据")
    return df

# 创建后台调度器
scheduler = BackgroundScheduler()

@wrap_tushare
@scheduler.scheduled_job(CronTrigger(day=1, hour=1, minute=0))  # 每月1号凌晨1点
def sync_trade_calendar():
    """定时同步交易日历数据"""
    return sync_trade_calendar_data()

def start_scheduler():
    """启动调度器"""
    scheduler.start()
    logger.info("异步调度器已启动")

def stop_scheduler():
    """停止调度器"""
    scheduler.shutdown()
    logger.info("异步调度器已停止")