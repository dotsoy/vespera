"""
启明星项目配置文件
"""
import os
from pathlib import Path
from typing import Dict, Any
from pydantic_settings import BaseSettings
from pydantic import Field


class DatabaseSettings(BaseSettings):
    """数据库配置"""
    
    # PostgreSQL 配置
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "qiming_star"
    postgres_user: str = "qiming_user"
    postgres_password: str = "qiming_pass_2024"
    
    # ClickHouse 配置
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 9000
    clickhouse_db: str = "qiming_timeseries"
    clickhouse_user: str = "qiming_user"
    clickhouse_password: str = "qiming_pass_2024"
    
    # Redis 配置
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str = "qiming_redis_2024"
    redis_db: int = 0
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "allow",
        "env_prefix": "",
        "env_nested_delimiter": "_"
    }
    
    @property
    def postgres_url(self) -> str:
        return f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    
    @property
    def clickhouse_url(self) -> str:
        return f"clickhouse://{self.clickhouse_user}:{self.clickhouse_password}@{self.clickhouse_host}:{self.clickhouse_port}/{self.clickhouse_db}"
    
    @property
    def redis_url(self) -> str:
        return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"


class DataSourceSettings(BaseSettings):
    """数据源配置"""
    akshare_enabled: bool = True
    akshare_timeout: int = 30
    yahoo_finance_enabled: bool = True
    yahoo_finance_timeout: int = 30
    alpha_vantage_api_key: str = ""
    alpha_vantage_timeout: int = 30
    enable_multi_source: bool = True
    default_fusion_strategy: str = "quality_based"
    enable_data_cache: bool = True
    cache_ttl_minutes: int = 60
    max_concurrent_sources: int = 3
    market_open_time: str = "09:30"
    market_close_time: str = "15:00"
    data_update_interval: int = 300
    min_market_cap: float = 50.0
    exclude_st_stocks: bool = True
    exclude_new_stocks_days: int = 60
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "allow",
        "env_prefix": "",
        "env_nested_delimiter": "_"
    }


class AnalysisSettings(BaseSettings):
    """分析配置"""
    ma_periods: list = [5, 10, 20, 60]
    ema_periods: list = [12, 26]
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    volume_ma_period: int = 20
    money_flow_threshold: float = 0.6
    min_technical_score: float = 0.7
    min_capital_score: float = 0.8
    signal_confidence_threshold: float = 0.75
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "allow",
        "env_prefix": "",
        "env_nested_delimiter": "_"
    }


class NotificationSettings(BaseSettings):
    """通知配置"""
    sendgrid_api_key: str = ""
    notification_email: str = ""
    enable_signal_alerts: bool = True
    enable_error_alerts: bool = True
    min_signal_score_for_alert: float = 0.85
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "allow",
        "env_prefix": "",
        "env_nested_delimiter": "_"
    }


class AppSettings(BaseSettings):
    """应用配置"""
    app_name: str = "启明星股票分析系统"
    version: str = "1.0.0"
    debug: bool = False
    
    # 路径配置
    project_root: Path = Path(__file__).parent.parent
    data_dir: Path = project_root / "data"
    logs_dir: Path = project_root / "logs"
    
    # Web 服务配置
    streamlit_host: str = "0.0.0.0"
    streamlit_port: int = 8501
    
    # 日志配置
    log_level: str = "INFO"
    log_rotation: str = "1 day"
    log_retention: str = "30 days"
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "allow",
        "env_prefix": "",
        "env_nested_delimiter": "_"
    }


# 全局配置实例
db_settings = DatabaseSettings()
data_settings = DataSourceSettings()
analysis_settings = AnalysisSettings()
notification_settings = NotificationSettings()
app_settings = AppSettings()


def get_all_settings() -> Dict[str, Any]:
    """获取所有配置"""
    return {
        "database": db_settings.dict(),
        "data_source": data_settings.dict(),
        "analysis": analysis_settings.dict(),
        "notification": notification_settings.dict(),
        "app": app_settings.dict(),
    }


if __name__ == "__main__":
    # 测试配置
    import json
    print(json.dumps(get_all_settings(), indent=2, default=str))
