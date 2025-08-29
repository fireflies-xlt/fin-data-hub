from apscheduler.schedulers.background import BackgroundScheduler
import logging
from typing import Optional
import threading

logger = logging.getLogger(__name__)

_scheduler: Optional[BackgroundScheduler] = None
_lock = threading.Lock()
def start_scheduler():
    """启动调度器"""
    global _scheduler
    _init_scheduler()
    assert _scheduler is not None 
    _scheduler.start()
    logger.info("异步调度器已启动")

def stop_scheduler():
    """停止调度器"""
    global _scheduler
    if _scheduler is None:
        return
    _scheduler.shutdown()
    logger.info("异步调度器已停止")


def get_scheduler() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is None:
        _init_scheduler()
    assert _scheduler is not None 
    return _scheduler


def _init_scheduler():
    """初始化调度器"""
    global _scheduler

    if _scheduler is None:
        with _lock:
            if _scheduler is None:
                _scheduler =  BackgroundScheduler(
                    executors={
                        'default': {
                            'type': 'threadpool',
                            'max_workers': 20  
                        }
                    }
                )
                logger.info("初始化调度器")