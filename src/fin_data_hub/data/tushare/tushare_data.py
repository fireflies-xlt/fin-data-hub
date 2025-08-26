import logging
import time
from typing import Any, Callable

import pandas as pd
import tushare as ts
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from fin_data_hub.config import config
from fin_data_hub.foundation.monitoring.telemetry import get_service_meter
from fin_data_hub.foundation.mysql.mysql_engine import mysql_engine, table_exists
from fin_data_hub.foundation.utils.date_utils import future_year_end, get_stock_start_date, next_day

logger = logging.getLogger(__name__)

# 初始化metrics
meter = get_service_meter()
tushare_function_duration = meter.create_histogram(
    name="tushare_function_duration",
    description="Tushare函数执行时间",
    unit="ms"
)

_tushare_client: Any = None
_trade_calendar_table = 'trade_calendar'

def wrap_tushare(func: Callable) -> Callable:
    """
    包装 tushare 函数, 标记为 tushare 函数
    1. 每次调用时都重新初始化 tushare 客户端
    2. 使用 OpenTelemetry metrics 统计调用耗时
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            success = True
            return result
        except Exception as e:
            success = False
            logger.error(f"Tushare函数 {func.__name__} 执行失败: {e}")
        finally:
            # 计算执行时间（毫秒）
            duration_ms = (time.time() - start_time) * 1000
            tushare_function_duration.record(
                duration_ms,
                attributes={
                    "name": f"tushare.{func.__name__}",
                    "success": str(success)
                }
            )
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
    
    # 获取未来今年的结束日期
    year_end = future_year_end(0)
    
    # 如果最新日期已经是今年后，说明数据已是最新
    if last_date and str(last_date) >= year_end:
        logger.info("交易日历数据已是最新，无需更新")
        return None
    
    start_date = get_stock_start_date() if last_date is None else next_day(str(last_date))
    
    df = get_tushare_client().trade_cal(
        start_date=start_date,
        end_date=year_end,
    )
    if df is not None and not df.empty:
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