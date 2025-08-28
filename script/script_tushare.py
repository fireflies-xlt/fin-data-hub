import os

from dotenv import load_dotenv
# 加载 .env 文件
load_dotenv()

import tushare as ts

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

df = client.stock_st(trade_date='20160705')
print(df)