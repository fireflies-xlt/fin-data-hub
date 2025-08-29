import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI

logger = logging.getLogger(__name__)


# 使用 lifespan 管理应用启动和关闭事件
@asynccontextmanager
async def lifespan(app: FastAPI):
    # -- Startup --
    logger.info("FastAPI Lifespan: 初始化中...")

    from fin_data_hub.foundation.scheduler import start_scheduler, stop_scheduler

    start_scheduler()

    from fin_data_hub.foundation.monitoring import setup_telemetry
    setup_telemetry(app)
    
    include_routers(app)

    logger.info("FastAPI Lifespan: 初始化完成")

    yield  # 应用运行中

    stop_scheduler()

    logger.info("FastAPI Lifespan: 关闭完成")


def include_routers(app):
    """加入路由"""
    from fin_data_hub.interfaces.fin_data_query import router
    app.include_router(router)
    pass


# 创建 FastAPI 应用实例
app = FastAPI(
    title="Fin Data Hub API",
    description="金融数据平台 API",
    version="0.1.0",
    lifespan=lifespan,
)

@app.get("/")
async def root():
    return {"message": "欢迎使用金融数据 API"}

# 添加简单的健康检查端点
@app.get("/health")
async def health_check():
    server_time = int(time.time() * 1000)
    return {"status": "ok", "server_time": server_time, "components": {"api": "up"}}
