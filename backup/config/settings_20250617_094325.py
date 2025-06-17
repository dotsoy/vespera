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
    postgres_host: str = Field(default="localhost", env="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, env="POSTGRES_PORT")
    postgres_db: str = Field(default="qiming_star", env="POSTGRES_DB")
    postgres_user: str = Field(default="qiming_user", env="POSTGRES_USER")
    postgres_password: str = Field(default="qiming_pass_2024", env="POSTGRES_PASSWORD")
    
    # ClickHouse 配置
    clickhouse_host: str = Field(default="localhost", env="CLICKHOUSE_HOST")
    clickhouse_port: int = Field(default=9000, env="CLICKHOUSE_PORT")
    clickhouse_db: str = Field(default="qiming_timeseries", env="CLICKHOUSE_DB")
    clickhouse_user: str = Field(default="qiming_user", env="CLICKHOUSE_USER")
    clickhouse_password: str = Field(default="qiming_pass_2024", env="CLICKHOUSE_PASSWORD")
    
    # Redis 配置
    redis_host: str = Field(default="localhost", env="REDIS_HOST")
    redis_port: int = Field(default=6379, env="REDIS_PORT")
    redis_password: str = Field(default="qiming_redis_2024", env="REDIS_PASSWORD")
    redis_db: int = Field(default=0, env="REDIS_DB")
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "allow"
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

    # AkShare 配置（免费，无需API key）
    akshare_enabled: bool = Field(default=True, env="AKSHARE_ENABLED")
    akshare_timeout: int = Field(default=30, env="AKSHARE_TIMEOUT")

    # Yahoo Finance 配置（免费，无需API key）
    yahoo_finance_enabled: bool = Field(default=True, env="YAHOO_FINANCE_ENABLED")
    yahoo_finance_timeout: int = Field(default=30, env="YAHOO_FINANCE_TIMEOUT")

    # Alpha Vantage 配置
    alpha_vantage_api_key: str = Field(default="", env="ALPHA_VANTAGE_API_KEY")
    alpha_vantage_timeout: int = Field(default=30, env="ALPHA_VANTAGE_TIMEOUT")

    # 多数据源配置
    enable_multi_source: bool = Field(default=True, env="ENABLE_MULTI_SOURCE")
    default_fusion_strategy: str = Field(default="quality_based", env="DEFAULT_FUSION_STRATEGY")
    enable_data_cache: bool = Field(default=True, env="ENABLE_DATA_CACHE")
    cache_ttl_minutes: int = Field(default=60, env="CACHE_TTL_MINUTES")
    max_concurrent_sources: int = Field(default=3, env="MAX_CONCURRENT_SOURCES")

    # 数据更新配置
    market_open_time: str = Field(default="09:30", env="MARKET_OPEN_TIME")
    market_close_time: str = Field(default="15:00", env="MARKET_CLOSE_TIME")
    data_update_interval: int = Field(default=300, env="DATA_UPDATE_INTERVAL")  # 秒

    # 股票池配置
    min_market_cap: float = Field(default=50.0, env="MIN_MARKET_CAP")  # 亿元
    exclude_st_stocks: bool = Field(default=True, env="EXCLUDE_ST_STOCKS")
    exclude_new_stocks_days: int = Field(default=60, env="EXCLUDE_NEW_STOCKS_DAYS")

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "allow"
    }


class AnalysisSettings(BaseSettings):
    """分析配置"""
    
    # 技术分析参数
    ma_periods: list = Field(default=[5, 10, 20, 60], env="MA_PERIODS")
    ema_periods: list = Field(default=[12, 26], env="EMA_PERIODS")
    rsi_period: int = Field(default=14, env="RSI_PERIOD")
    macd_fast: int = Field(default=12, env="MACD_FAST")
    macd_slow: int = Field(default=26, env="MACD_SLOW")
    macd_signal: int = Field(default=9, env="MACD_SIGNAL")
    
    # 资金流分析参数
    volume_ma_period: int = Field(default=20, env="VOLUME_MA_PERIOD")
    money_flow_threshold: float = Field(default=0.6, env="MONEY_FLOW_THRESHOLD")
    
    # 信号融合参数
    min_technical_score: float = Field(default=0.7, env="MIN_TECHNICAL_SCORE")
    min_capital_score: float = Field(default=0.8, env="MIN_CAPITAL_SCORE")
    signal_confidence_threshold: float = Field(default=0.75, env="SIGNAL_CONFIDENCE_THRESHOLD")
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "allow"
    }


class NotificationSettings(BaseSettings):
    """通知配置"""
    
    # 邮件通知
    sendgrid_api_key: str = Field(default="", env="SENDGRID_API_KEY")
    notification_email: str = Field(default="", env="NOTIFICATION_EMAIL")
    
    # 通知条件
    enable_signal_alerts: bool = Field(default=True, env="ENABLE_SIGNAL_ALERTS")
    enable_error_alerts: bool = Field(default=True, env="ENABLE_ERROR_ALERTS")
    min_signal_score_for_alert: float = Field(default=0.85, env="MIN_SIGNAL_SCORE_FOR_ALERT")
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "allow"
    }


class AppSettings(BaseSettings):
    """应用配置"""
    
    # 基础配置
    app_name: str = Field(default="启明星股票分析系统", env="APP_NAME")
    version: str = Field(default="1.0.0", env="APP_VERSION")
    debug: bool = Field(default=False, env="DEBUG")
    
    # 路径配置
    project_root: Path = Path(__file__).parent.parent
    data_dir: Path = project_root / "data"
    logs_dir: Path = project_root / "logs"
    
    # Streamlit 配置
    streamlit_host: str = Field(default="0.0.0.0", env="STREAMLIT_HOST")
    streamlit_port: int = Field(default=8501, env="STREAMLIT_PORT")
    
    # 日志配置
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_rotation: str = Field(default="1 day", env="LOG_ROTATION")
    log_retention: str = Field(default="30 days", env="LOG_RETENTION")
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "allow"
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
