import logging
from typing import Optional

import pandas as pd
import tushare as ts

from fin_data_hub.config import config

from tushare.pro import client

logger = logging.getLogger(__name__)

_tushare_client: Optional[client.DataApi] = None

def get_tushare_client() -> client.DataApi:
    global _tushare_client
    if _tushare_client is None:
         assert config.tushare_token, "Tushare token is not set"
         ts.set_token(config.tushare_token)  
         _tushare_client = ts.pro_api()
    return _tushare_client


def get_trade_calendar(
    exchange: str = '', 
    start_date: str = '',
    end_date: str = '',
    fields: str = 'exchange,cal_date,is_open,pretrade_date',
    is_open: str = '',
) -> pd.DataFrame:
    """
    获取交易日历
    
    """
    client = get_tushare_client()
    return client.trade_cal(
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
        is_open=is_open
    )
