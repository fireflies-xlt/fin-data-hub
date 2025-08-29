import logging
import argparse
import time
import pandas as pd
import sys

from fin_data_hub.data.tushare.tushare_data import get_tushare_client
from fin_data_hub.data.tushare.constants import (
    STOCK_BASIC_TABLE, 
    TRADE_CALENDAR_TABLE, 
    STOCK_ST_TABLE,
    DAILY_TABLE,
    DAILY_BASIC_TABLE,
    WEEKLY_TABLE,
    MONTHLY_TABLE
)
from fin_data_hub.foundation.mysql.mysql_engine import (
    mysql_engine, 
    table_exists_and_not_empty
)
from fin_data_hub.foundation.utils.date_utils import (
    add_year, 
    get_stock_start_date, 
    future_year_end, 
    current_date_ymd, 
    next_day,
    get_all_month_end
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

def backfill_stock_basic_data():
    """
    补全股票基础数据

    字段：ts_code symbol name area industry cnspell market list_date act_name act_ent_type
    """
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
    """
    补全交易日历数据

    字段：exchange cal_date is_open pretrade_date
    """
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
    """
    补全ST股票列表数据

    字段：ts_code name trade_date type type_name
    """
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

def backfill_bak_basic_data():
    """
    股票历史列表
    """
    # todo: 补全股票历史列表
    return

def backfill_daily_data():
    """
    A股日线行情

    字段：ts_code trade_date open high low close pre_close change pct_chg vol amount
    """
    stock_list = get_stock_list()
    client = get_tushare_client()

    end_date = current_date_ymd()

    for index, row in stock_list.iterrows():
        ts_code = row['ts_code']
        list_date = row['list_date']
        
        # 分批拉取数据，每20年拉一次
        all_data = []
        current_start = list_date

        if table_exists_and_not_empty(DAILY_TABLE):
            query = f"SELECT MAX(trade_date) as last_date FROM {DAILY_TABLE} WHERE ts_code = '{ts_code}'"
            df = pd.read_sql(query, mysql_engine())
            if df is not None and not df.empty:
                last_date = df['last_date'].iloc[0]
                current_start = next_day(last_date)
        
        while current_start < end_date:
            time.sleep(0.5)

            # 计算20年后的日期
            current_end = min(add_year(current_start, 20), end_date)
            df_batch = client.daily(ts_code=ts_code, start_date=current_start, end_date=current_end)
            logger.info(f"拉取 {ts_code} 从 {current_start} 到 {current_end} 的 {len(df_batch)} 条数据")
            if df_batch is not None and not df_batch.empty:
                all_data.append(df_batch)
            
            current_start = next_day(current_end)
           
        
        # 合并并排序
        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            df_combined = df_combined.sort_values('trade_date', ascending=True)
            df_combined.to_sql(DAILY_TABLE, con=mysql_engine(), if_exists='append', index=False)
            logger.info(f"保存 {ts_code} 的 {len(df_combined)} 条数据")
    
    return


def backfill_daily_basic_data():
    """
    补全每日指标

    字段：ts_code trade_date close turnover_rate turnover_rate_f volume_ratio pe pe_ttm pb ps ps_ttm dv_ratio dv_ttm total_share float_share free_share total_mv circ_mv
    """
    start_date = get_stock_start_date()

    if table_exists_and_not_empty(DAILY_BASIC_TABLE):
        query = f"SELECT MAX(trade_date) as last_date FROM {DAILY_BASIC_TABLE}"
        df = pd.read_sql(query, mysql_engine())
        last_date = df['last_date'].iloc[0]
        if last_date:
            start_date = next_day(last_date)

    current_date = current_date_ymd()

    client = get_tushare_client()
    while start_date < current_date:
        time.sleep(1)
        df = client.daily_basic(ts_code='', trade_date=start_date)
        if df is not None and not df.empty:
            df.to_sql(DAILY_BASIC_TABLE, con=mysql_engine(), if_exists='append', index=False)
        logger.info(f"获取到 {len(df)} 条 {start_date} 的每日指标数据")
        start_date = next_day(start_date)

    return


def backfill_weekly_data():
    """
    补全周线行情

    字段：ts_code trade_date open high low close vol amount
    """
    start_date = get_stock_start_date()

    if table_exists_and_not_empty(WEEKLY_TABLE):
        query = f"SELECT MAX(trade_date) as last_date FROM {WEEKLY_TABLE}"
        df = pd.read_sql(query, mysql_engine())
        last_date = df['last_date'].iloc[0]
        if last_date:
            start_date = next_day(last_date)
    
    current_date = current_date_ymd()

    client = get_tushare_client() 
    while start_date < current_date:
        time.sleep(1)
        df = client.weekly(trade_date=start_date)
        if df is not None and not df.empty:
            df.to_sql(WEEKLY_TABLE, con=mysql_engine(), if_exists='append', index=False)
        logger.info(f"获取到 {len(df)} 条 {start_date} 的周线行情数据")
        start_date = next_day(start_date)
    return

def backfill_monthly_data():
    """
    补全月线行情

    字段：ts_code trade_date close open high low pre_close change pct_chg vol amount
    """
    start_date = get_stock_start_date()

    if table_exists_and_not_empty(MONTHLY_TABLE):
        query = f"SELECT MAX(trade_date) as last_date FROM {MONTHLY_TABLE}"
        df = pd.read_sql(query, mysql_engine())
        last_date = df['last_date'].iloc[0]
        if last_date:
            start_date = next_day(last_date)

    month_end_dates = get_all_month_end(start_date, current_date_ymd())

    client = get_tushare_client()
    for date in month_end_dates:
        time.sleep(1)
        df = client.monthly(trade_date=date)
        if df is not None and not df.empty:
            df.to_sql(MONTHLY_TABLE, con=mysql_engine(), if_exists='append', index=False)
        else:
            # 如果获取不到数据，则调用该月每天的数据
            for day in range(1, 31):
                date_str = f"{date[:4]}{date[4:6]}{day:02d}"
                time.sleep(1)
                df = client.monthly(trade_date=date_str)
                if df is not None and not df.empty:
                    df.to_sql(MONTHLY_TABLE, con=mysql_engine(), if_exists='append', index=False)

        logger.info(f"获取到 {len(df)} 条 {date} 的月线行情数据")
    return

def get_stock_list() -> pd.DataFrame:
    """
    获取股票列表
    """
    query = f"SELECT * FROM {STOCK_BASIC_TABLE}"
    df = pd.read_sql(query, mysql_engine())
    return df


def get_trade_calendar_list() -> pd.DataFrame:
    """
    获取交易日历列表
    """
    query = f"SELECT * FROM {TRADE_CALENDAR_TABLE}"
    df = pd.read_sql(query, mysql_engine())
    return df

def main():
    """命令行入口点"""
    parser = argparse.ArgumentParser(description="Tushare数据补全")
    parser.add_argument("--stock_basic", action="store_true", help="补全股票基础数据")
    parser.add_argument("--trade_calendar", action="store_true", help="补全交易日历数据")
    parser.add_argument("--stock_st", action="store_true", help="补全ST股票列表数据")
    parser.add_argument("--bak_basic", action="store_true", help="补全股票历史列表")
    parser.add_argument("--daily", action="store_true", help="补全日线数据")
    parser.add_argument("--daily_basic", action="store_true", help="补全每日指标")
    parser.add_argument("--weekly", action="store_true", help="补全周线行情")
    parser.add_argument("--monthly", action="store_true", help="补全月线行情")
    args = parser.parse_args()
    
    if args.stock_basic:
        backfill_stock_basic_data()
    if args.trade_calendar:
        backfill_trade_calendar_data()
    if args.stock_st:
        backfill_stock_st_data()
    if args.bak_basic:
        backfill_bak_basic_data()
    if args.daily:
        backfill_daily_data()
    if args.daily_basic:
        backfill_daily_basic_data()
    if args.weekly:
        backfill_weekly_data()
    if args.monthly:
        backfill_monthly_data()

    backfill_monthly_data()
        
if __name__ == "__main__":
    main()
