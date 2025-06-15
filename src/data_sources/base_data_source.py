"""
数据源抽象基类
定义统一的数据源接口规范
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, date
import pandas as pd
from enum import Enum
from dataclasses import dataclass
from loguru import logger


class DataSourceType(Enum):
    """数据源类型枚举"""
    TUSHARE = "tushare"
    YAHOO_FINANCE = "yahoo_finance"
    ALPHA_VANTAGE = "alpha_vantage"
    ALLTICK = "alltick"
    AKSHARE = "akshare"
    WIND = "wind"
    EASTMONEY = "eastmoney"
    LOCAL_FILE = "local_file"
    DATABASE = "database"


class DataType(Enum):
    """数据类型枚举"""
    STOCK_BASIC = "stock_basic"           # 股票基础信息
    DAILY_QUOTES = "daily_quotes"         # 日线行情
    MINUTE_QUOTES = "minute_quotes"       # 分钟级行情
    TICK_DATA = "tick_data"              # 逐笔数据
    FINANCIAL_DATA = "financial_data"     # 财务数据
    INDEX_DATA = "index_data"            # 指数数据
    FUND_DATA = "fund_data"              # 基金数据
    BOND_DATA = "bond_data"              # 债券数据
    FUTURES_DATA = "futures_data"        # 期货数据
    OPTIONS_DATA = "options_data"        # 期权数据
    NEWS_DATA = "news_data"              # 新闻数据
    SENTIMENT_DATA = "sentiment_data"    # 情感数据


@dataclass
class DataRequest:
    """数据请求参数"""
    data_type: DataType
    symbol: Optional[str] = None
    symbols: Optional[List[str]] = None
    start_date: Optional[Union[str, date, datetime]] = None
    end_date: Optional[Union[str, date, datetime]] = None
    fields: Optional[List[str]] = None
    extra_params: Optional[Dict[str, Any]] = None


@dataclass
class DataResponse:
    """数据响应结果"""
    data: pd.DataFrame
    source: str
    data_type: DataType
    timestamp: datetime
    success: bool
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class DataSourceStatus(Enum):
    """数据源状态"""
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    LIMITED = "limited"
    ERROR = "error"


@dataclass
class DataSourceInfo:
    """数据源信息"""
    name: str
    type: DataSourceType
    status: DataSourceStatus
    supported_data_types: List[DataType]
    rate_limit: Optional[int] = None  # 每分钟请求限制
    cost_per_request: Optional[float] = None  # 每次请求成本
    priority: int = 1  # 优先级，数字越小优先级越高
    description: Optional[str] = None


class BaseDataSource(ABC):
    """数据源抽象基类"""
    
    def __init__(self, name: str, source_type: DataSourceType, config: Optional[Dict[str, Any]] = None):
        self.name = name
        self.source_type = source_type
        self.config = config or {}
        self.status = DataSourceStatus.UNAVAILABLE
        self._last_request_time = None
        self._request_count = 0
        
    @abstractmethod
    def initialize(self) -> bool:
        """
        初始化数据源连接
        
        Returns:
            bool: 初始化是否成功
        """
        pass
    
    @abstractmethod
    def get_supported_data_types(self) -> List[DataType]:
        """
        获取支持的数据类型
        
        Returns:
            List[DataType]: 支持的数据类型列表
        """
        pass
    
    @abstractmethod
    def check_availability(self) -> DataSourceStatus:
        """
        检查数据源可用性
        
        Returns:
            DataSourceStatus: 数据源状态
        """
        pass
    
    @abstractmethod
    def fetch_data(self, request: DataRequest) -> DataResponse:
        """
        获取数据
        
        Args:
            request: 数据请求参数
            
        Returns:
            DataResponse: 数据响应结果
        """
        pass
    
    def get_info(self) -> DataSourceInfo:
        """
        获取数据源信息
        
        Returns:
            DataSourceInfo: 数据源信息
        """
        return DataSourceInfo(
            name=self.name,
            type=self.source_type,
            status=self.status,
            supported_data_types=self.get_supported_data_types(),
            rate_limit=self.config.get('rate_limit'),
            cost_per_request=self.config.get('cost_per_request'),
            priority=self.config.get('priority', 1),
            description=self.config.get('description')
        )
    
    def validate_request(self, request: DataRequest) -> bool:
        """
        验证请求参数
        
        Args:
            request: 数据请求参数
            
        Returns:
            bool: 验证是否通过
        """
        # 检查数据类型是否支持
        if request.data_type not in self.get_supported_data_types():
            logger.warning(f"数据源 {self.name} 不支持数据类型 {request.data_type}")
            return False
        
        # 检查必要参数
        if request.data_type in [DataType.DAILY_QUOTES, DataType.MINUTE_QUOTES, DataType.TICK_DATA]:
            if not request.symbol and not request.symbols:
                logger.warning("股票行情数据请求必须提供股票代码")
                return False
        
        return True
    
    def _check_rate_limit(self) -> bool:
        """
        检查请求频率限制
        
        Returns:
            bool: 是否在限制范围内
        """
        rate_limit = self.config.get('rate_limit')
        if not rate_limit:
            return True
        
        now = datetime.now()
        if self._last_request_time:
            time_diff = (now - self._last_request_time).total_seconds()
            if time_diff < 60:  # 一分钟内
                if self._request_count >= rate_limit:
                    logger.warning(f"数据源 {self.name} 请求频率超限")
                    return False
            else:
                self._request_count = 0
        
        return True
    
    def _update_request_stats(self):
        """更新请求统计"""
        self._last_request_time = datetime.now()
        self._request_count += 1
    
    def close(self):
        """关闭数据源连接"""
        logger.info(f"关闭数据源 {self.name}")


class DataSourceError(Exception):
    """数据源异常"""
    
    def __init__(self, message: str, source: str, error_code: Optional[str] = None):
        self.message = message
        self.source = source
        self.error_code = error_code
        super().__init__(f"[{source}] {message}")


class RateLimitError(DataSourceError):
    """请求频率限制异常"""
    pass


class DataNotAvailableError(DataSourceError):
    """数据不可用异常"""
    pass


class AuthenticationError(DataSourceError):
    """认证失败异常"""
    pass


class NetworkError(DataSourceError):
    """网络连接异常"""
    pass
