'''
管理所有数据相关的模块
'''

import logging

logger = logging.getLogger(__name__)


def start_scheduler():
    from fin_data_hub.data.tushare.tushare_data import start_scheduler
    start_scheduler()
    logger.info("数据调度器已启动")

def stop_scheduler():
    from fin_data_hub.data.tushare.tushare_data import stop_scheduler
    stop_scheduler()
    logger.info("数据调度器已停止")

