import logging
import time
import threading
from typing import Any, Callable

import pandas as pd
import tushare as ts
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from fin_data_hub.config import config
from fin_data_hub.foundation.monitoring.telemetry import get_service_meter
from fin_data_hub.foundation.mysql.mysql_engine import mysql_engine
from fin_data_hub.foundation.utils.date_utils import (
    future_year_end, 
    get_stock_start_date, 
    current_date_ymd, 
    next_day
)
from fin_data_hub.data.tushare.constants import (
    STOCK_BASIC_TABLE, 
    TRADE_CALENDAR_TABLE, 
    DAILY_TABLE,
    STOCK_ST_TABLE
)
logger = logging.getLogger(__name__)

# 初始化metrics
meter = get_service_meter()
tushare_function_duration = meter.create_histogram(
    name="tushare_function_duration",
    description="Tushare函数执行时间",
    unit="ms"
)

_tushare_client: Any = None

# 简单的运行状态跟踪（线程安全）
_running_functions = set()
_running_lock = threading.Lock()

def wrap_tushare(func: Callable) -> Callable:
    """
    包装 tushare 函数, 标记为 tushare 函数
    1. 使用 OpenTelemetry metrics 统计调用耗时
    2. 防重复调用
    """
    def wrapper(*args, **kwargs):
        # 线程安全地检查是否正在运行
        with _running_lock:
            if func.__name__ in _running_functions:
                logger.warning(f"函数 {func.__name__} 正在运行中，跳过本次执行")
                return None
            
            # 标记为运行中
            _running_functions.add(func.__name__)
        
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            success = True
            return result
        except Exception as e:
            success = False
            logger.error(f"Tushare函数 {func.__name__} 执行失败: {e}")
        finally:
            # 线程安全地移除运行标记
            with _running_lock:
                _running_functions.discard(func.__name__)
            
            logger.info(f"Tushare函数 {func.__name__} 执行完成")
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

def is_trade_day(date_str: str) -> bool:
    """判断是否为交易日"""
    from fin_data_hub.data.tushare.tushare_data_cache import get_trade_calendar_cache
    trade_calendar = get_trade_calendar_cache()
    return trade_calendar[trade_calendar['cal_date'] == date_str]['is_open'].iloc[0]


# =============================================================================
# 股票数据 - 基础数据
# =============================================================================

@wrap_tushare
def sync_stock_basic_data() -> pd.DataFrame | None:
    """股票列表
    
    字段：ts_code symbol name area industry cnspell market list_date act_name act_ent_type
    """
    client = get_tushare_client()

    l_data = client.stock_basic(exchange='', list_status='L')
    l_data['status'] = 'L'

    d_data = client.stock_basic(exchange='', list_status='D')
    d_data['status'] = 'D'

    data = pd.concat([l_data, d_data], ignore_index=True)

    if data is not None and not data.empty:
        data.to_sql(STOCK_BASIC_TABLE, con=mysql_engine(), if_exists='replace', index=False)
        from fin_data_hub.data.tushare.tushare_data_cache import update_stock_basic_cache
        update_stock_basic_cache(data)
    logger.info(f"【股票列表】获取到 {len(data)} 条股票数据")
    return data

@wrap_tushare
def sync_trade_calendar_data() -> pd.DataFrame | None:
    """交易日历
    
    字段：exchange cal_date is_open pretrade_date
    """
    client = get_tushare_client()
    start_date = get_stock_start_date()
    end_date = future_year_end(0)
    df = client.trade_cal(exchange='', start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        df.to_sql(TRADE_CALENDAR_TABLE, con=mysql_engine(), if_exists='replace', index=False)
        from fin_data_hub.data.tushare.tushare_data_cache import update_trade_calendar_cache
        update_trade_calendar_cache(df)
    logger.info(f"【交易日历】获取到 {len(df)} 条交易日历数据")
    return df

@wrap_tushare
def sync_stock_st_data() -> pd.DataFrame | None:
    """ST股票列表

    每天上午9:20更新
    
    字段：ts_code name trade_date type type_name
    """

    query = f"SELECT MAX(trade_date) as last_date FROM {STOCK_ST_TABLE}"
    result = pd.read_sql(query, mysql_engine())
    start_date = result['last_date'].iloc[0]
    if not start_date:
        start_date = '20160101'

    current_date = current_date_ymd()
    if start_date >= current_date:
        logger.info(f"【ST股票列表】数据已是最新（{start_date}），无需更新")
        return None

    client = get_tushare_client()

    while start_date <= current_date:
        time.sleep(0.5)
        df = client.stock_st(trade_date=start_date)
        if df is None or df.empty:
            logger.info(f"【ST股票列表】从 {start_date} 获取到 0 条ST股票列表数据")
            start_date = next_day(start_date)
            continue
        df.to_sql(STOCK_ST_TABLE, con=mysql_engine(), if_exists='append', index=False)
        logger.info(f"【ST股票列表】从 {start_date} 获取到 {len(df)} 条ST股票列表数据")
        start_date = next_day(start_date)
    return

# =============================================================================
# 股票数据 - 行情数据
# =============================================================================

@wrap_tushare
def sync_daily_data() -> None:
    """日线行情

    交易日每天15点～16点之间入库
    
    字段：ts_code trade_date open high low close pre_close change pct_chg vol amount
    """
    from fin_data_hub.data.tushare.tushare_data_cache import get_stock_basic_cache
    stock_list = get_stock_basic_cache()   

    client = get_tushare_client()

    for index, row in stock_list.iterrows():
        ts_code = row['ts_code']
        query = f"SELECT MAX(trade_date) as last_date FROM {DAILY_TABLE} WHERE ts_code = '{ts_code}'"
        df = pd.read_sql(query, mysql_engine())
        start_date = row['list_date']
        if not df.empty:
            last_date = df['last_date'].iloc[0]
            start_date = next_day(last_date)

        end_date = current_date_ymd()
        if end_date < start_date:
            logger.info(f"[日线行情] {ts_code} 当前日期 {end_date} 小于开始日期 {start_date}，跳过")
            continue

        time.sleep(0.5)
        df = client.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            df.to_sql(DAILY_TABLE, con=mysql_engine(), if_exists='append', index=False)
        logger.info(f"[日线行情] 从 {start_date} 到 {end_date} 获取到 {len(df)} 条 {ts_code} 的日线行情数据")

# 创建后台调度器，配置线程池大小
scheduler = BackgroundScheduler(
    executors={
        'default': {
            'type': 'threadpool',
            'max_workers': 20  
        }
    }
)

# 添加调度任务
@scheduler.scheduled_job(CronTrigger(day=1, hour=1, minute=0))  # 每月1号凌晨1点
def scheduled_sync_trade_calendar():
    return sync_trade_calendar_data()

@scheduler.scheduled_job(CronTrigger(hour=8, minute=15))  # 每天早上8点15分
def scheduled_sync_stock_basic():
    return sync_stock_basic_data()

@scheduler.scheduled_job(CronTrigger(hour=17, minute=3))  # 每天下午5点3分
def scheduled_sync_daily():
    return sync_daily_data()

@scheduler.scheduled_job(CronTrigger(hour=9, minute=21))  # 每天上午9点21分
def scheduled_sync_stock_st():
    return sync_stock_st_data()

def start_scheduler():
    """启动调度器"""
    scheduler.start()
    logger.info("异步调度器已启动")

def stop_scheduler():
    """停止调度器"""
    scheduler.shutdown()
    logger.info("异步调度器已停止")