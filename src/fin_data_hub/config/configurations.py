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


    # --- MySQL 配置 ---
    mysql_host: str = Field(default="localhost", description="MySQL 主机")
    mysql_port: int = Field(default=3306, description="MySQL 端口")
    mysql_user: str = Field(default="root", description="MySQL 用户")
    mysql_password: str = Field(default="123456", description="MySQL 密码")
    mysql_database: str = Field(default="fin_data_hub", description="MySQL 数据库")

    # Pydantic-settings 配置
    model_config = SettingsConfigDict(
        env_file=".env",  # 明确指定 .env 文件路径
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def mysql_url(self):
        return f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"

# 导出单例实例
config = Configurations()