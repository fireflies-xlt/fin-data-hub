import os

from dotenv import load_dotenv
# 加载 .env 文件
load_dotenv()

import tushare as ts
from fin_data_hub.foundation.utils.date_utils import (
    get_all_month_end, 
    get_stock_start_date, 
    current_date_ymd
)

# 从 .env 文件读取 token
token = os.getenv("TUSHARE_TOKEN")
assert token, "TUSHARE_TOKEN 环境变量未设置"

ts.set_token(token)
client = ts.pro_api()
# df = client.trade_cal(exchange='', start_date='20250801', end_date='20250831', is_open='0')
# print(df)

# df = client.stock_basic(exchange='', list_status='L')
# print(df)


# df = client.stock_basic(exchange='', list_status='D')
# print(df)

# df = client.stock_st(trade_date='20160705')
# print(df)


# df = client.daily(ts_code='000001.SZ', start_date='19910403', end_date='20250828')

# # 按 trade_date 升序排序（从早到晚）
# df_sorted_asc = df.sort_values('trade_date', ascending=True)
# print("按交易日期升序排序（从早到晚）:")
# print(df_sorted_asc.head())

# # 获取 trade_date 的最大值和最小值
# min_date = df['trade_date'].min()
# max_date = df['trade_date'].max()
# print(f"\n最早交易日期: {min_date}")
# print(f"最晚交易日期: {max_date}")



df = client.monthly(trade_date='19910430')
print(df)


start_date = 19910301
end_date = 19910331

for date in range(start_date, end_date):
    date_str = str(date)
    df = client.monthly(trade_date=date_str)
    print(df)


# month_end_dates = get_all_month_end(get_stock_start_date(), current_date_ymd())
# for date in month_end_dates:
#     print(date)

