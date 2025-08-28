import logging
import argparse
import time
import pandas as pd
import sys

from fin_data_hub.data.tushare.tushare_data import get_tushare_client
from fin_data_hub.data.tushare.constants import (
    STOCK_BASIC_TABLE, 
    TRADE_CALENDAR_TABLE, 
    STOCK_ST_TABLE
)
from fin_data_hub.foundation.mysql.mysql_engine import mysql_engine, table_exists_and_not_empty
from fin_data_hub.foundation.utils.date_utils import get_stock_start_date, future_year_end, current_date_ymd, next_day

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

def backfill_stock_basic_data():
    '''
    补全股票基础数据

    字段：ts_code symbol name area industry cnspell market list_date act_name act_ent_type
    '''
    client = get_tushare_client()

    if  table_exists_and_not_empty(STOCK_BASIC_TABLE):
        logger.info(f"股票基础数据表 {STOCK_BASIC_TABLE} 已存在，跳过补全")
        return
    
    l_data = client.stock_basic(exchange='', list_status='L')
    l_data['status'] = 'L'

    d_data = client.stock_basic(exchange='', list_status='D')
    d_data['status'] = 'D'

    data = pd.concat([l_data, d_data], ignore_index=True)

    if data is not None and not data.empty:
        data.to_sql(STOCK_BASIC_TABLE, con=mysql_engine(), if_exists='replace', index=False)
    logger.info(f"获取到 {len(data)} 条股票数据")
    return

def backfill_trade_calendar_data():
    '''
    补全交易日历数据
    
    字段：exchange cal_date is_open pretrade_date
    '''
    client = get_tushare_client()
    
    if  table_exists_and_not_empty(TRADE_CALENDAR_TABLE):
        logger.info(f"交易日历数据表 {TRADE_CALENDAR_TABLE} 已存在，跳过补全")
        return
    
    start_date = get_stock_start_date()
    end_date = future_year_end(0)
    df = client.trade_cal(exchange='', start_date=start_date, end_date=end_date)

    if df is not None and not df.empty:
        df.to_sql(TRADE_CALENDAR_TABLE, con=mysql_engine(), if_exists='replace', index=False)
    logger.info(f"获取到 {len(df)} 条交易日历数据")
    return


def backfill_stock_st_data():
    '''
    补全ST股票列表数据

    字段：ts_code name trade_date type type_name
    '''
    client = get_tushare_client()

    if table_exists_and_not_empty(STOCK_ST_TABLE):
        query = f"SELECT MAX(trade_date) as last_date FROM {STOCK_ST_TABLE}"
        result = pd.read_sql(query, mysql_engine())
        start_date = result['last_date'].iloc[0]
    else:
        start_date = '20160101'
    
    current_date = current_date_ymd()
    
    if start_date and str(start_date) >= current_date:
        logger.info(f"ST股票列表数据已是最新（{start_date}），无需更新")
        return None
    
    while start_date <= current_date:
        time.sleep(0.5)
        df = client.stock_st(trade_date=start_date)
        if df is None or df.empty:
            logger.info(f"从 {start_date} 获取到 0 条ST股票列表数据")
            start_date = next_day(start_date)
            continue
        
        df.to_sql(STOCK_ST_TABLE, con=mysql_engine(), if_exists='append', index=False)
        logger.info(f"从 {start_date} 获取到 {len(df)} 条ST股票列表数据")

        start_date = next_day(start_date)

    return

def main():
    """命令行入口点"""
    parser = argparse.ArgumentParser(description="Tushare数据补全")
    parser.add_argument("--stock_basic", action="store_true", help="补全股票基础数据")
    parser.add_argument("--trade_calendar", action="store_true", help="补全交易日历数据")
    parser.add_argument("--stock_st", action="store_true", help="补全ST股票列表数据")
    args = parser.parse_args()
    
    if args.stock_basic:
        backfill_stock_basic_data()
    if args.trade_calendar:
        backfill_trade_calendar_data()
    if args.stock_st:
        backfill_stock_st_data()

if __name__ == "__main__":
    main()
