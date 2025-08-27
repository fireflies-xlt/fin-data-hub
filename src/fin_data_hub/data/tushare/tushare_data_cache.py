import logging
from typing import Optional

import pandas as pd

from fin_data_hub.foundation.mysql.mysql_engine import mysql_engine, table_exists
from fin_data_hub.data.tushare.constants import STOCK_BASIC_TABLE, TRADE_CALENDAR_TABLE

logger = logging.getLogger(__name__)


_stock_basic_cache: pd.DataFrame = pd.DataFrame()
_trade_calendar_cache: pd.DataFrame = pd.DataFrame()
def init_cache():
    """
    初始化股票数据缓存
    """
    global _stock_basic_cache
    _stock_basic_cache = get_stock_basic_from_db()
    global _trade_calendar_cache
    _trade_calendar_cache = get_trade_calendar_from_db()

def get_stock_basic_cache() -> pd.DataFrame:
    """
    获取股票数据缓存
    """
    global _stock_basic_cache
    return _stock_basic_cache
def get_trade_calendar_cache() -> pd.DataFrame:
    """
    获取交易日历数据缓存
    """
    global _trade_calendar_cache
    return _trade_calendar_cache
def update_stock_basic_cache(df: pd.DataFrame):
    """
    更新股票数据缓存
    """
    if df is None or df.empty:
        return

    df = df[['ts_code', 'symbol', 'name', 'area', 'industry', 'market', 'list_date', 'status']].copy()

    global _stock_basic_cache
    _stock_basic_cache = df
    logger.info(f"更新股票数据缓存，共 {len(df)} 条数据")


def update_trade_calendar_cache(df: pd.DataFrame):
    """
    更新交易日历数据缓存
    """
    if df is None or df.empty:
        return

    df = df[['cal_date', 'is_open']].copy()

    global _trade_calendar_cache
    _trade_calendar_cache = df
    logger.info(f"更新交易日历数据缓存，共 {len(df)} 条数据")


def get_stock_basic_from_db() -> pd.DataFrame:
    """
    获取股票数据
    """
    if not table_exists(STOCK_BASIC_TABLE):
        logger.warning(f"股票基础数据表 {STOCK_BASIC_TABLE} 不存在")
        return pd.DataFrame()
    
    query = f"SELECT ts_code, symbol, name, area, industry, market, list_date, status FROM {STOCK_BASIC_TABLE}"
    df = pd.read_sql(query, mysql_engine())
    
    if df.empty:
        logger.warning("股票基础数据表为空")
        return pd.DataFrame()
    
    logger.info(f"从缓存获取到 {len(df)} 条股票数据")
    
    return df
        

def get_trade_calendar_from_db() -> pd.DataFrame:
    """
    获取交易日历数据
    """
    if not table_exists(TRADE_CALENDAR_TABLE):
        logger.warning(f"交易日历数据表 {TRADE_CALENDAR_TABLE} 不存在")
        return pd.DataFrame()
    
    query = f"SELECT cal_date, is_open FROM {TRADE_CALENDAR_TABLE}"
    df = pd.read_sql(query, mysql_engine())
    
    if df.empty:
        logger.warning("交易日历数据表为空")
        return pd.DataFrame()
    
    logger.info(f"从缓存获取到 {len(df)} 条交易日历数据")
    return df
