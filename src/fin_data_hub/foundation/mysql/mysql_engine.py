from sqlalchemy import create_engine, text    

from fin_data_hub.config import config

_mysql_engine = None

def _get_mysql_engine():
    """创建带连接池的 MySQL 引擎"""
    global _mysql_engine
    if _mysql_engine is None:
        _mysql_engine = create_engine(
            config.mysql_url,
            pool_size=10,           # 连接池大小
            max_overflow=20,        # 最大溢出连接数
            pool_timeout=30,        # 连接超时时间
            pool_recycle=3600,      # 连接回收时间（1小时）
            pool_pre_ping=True,     # 连接前检查
            echo=False              # 不显示 SQL 语句
        )
    return _mysql_engine


# 为了向后兼容，提供一个函数
def mysql_engine():
    return _get_mysql_engine()

def table_exists(table_name: str) -> bool:
    """检查表是否存在"""
    query = f"""
    SELECT COUNT(*) as table_exists 
    FROM information_schema.tables 
    WHERE table_schema = DATABASE() AND table_name = '{table_name}'
    """
    with mysql_engine().connect() as conn:
        result = conn.execute(text(query))
        return result.fetchone()[0] > 0