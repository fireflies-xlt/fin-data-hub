import logging

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

class Configurations(BaseSettings):
    """
    应用配置模型。
    自动从 .env 文件或环境变量加载值。
    """

    # --- 日志配置 ---
    log_level: str = Field(default="INFO", description="日志级别")

    # --- Tushare 配置 ---
    tushare_token: str = Field(default="", description="Tushare API Token")

    # --- 监控配置 ---
    # telemetry_metrics_port: int = Field(
    #     default=8003, description="Prometheus监控指标服务端口"
    # )

    # Pydantic-settings 配置
    model_config = SettingsConfigDict(
        env_file=None,  # 自动查找 .env 文件
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

# 导出单例实例
config = Configurations()
