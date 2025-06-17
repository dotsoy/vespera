# 数据源模块

from .base_data_source import (
    BaseDataSource, DataRequest, DataResponse, DataType,
    DataSourceType, DataSourceStatus, DataSourceError,
    RateLimitError, AuthenticationError, NetworkError
)
from .data_source_factory import DataSourceFactory
from .data_source_manager import DataSourceManager

# 尝试导入各种数据源
try:
    from .akshare_data_source import AkShareDataSource
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False

__all__ = [
    'BaseDataSource', 'DataRequest', 'DataResponse', 'DataType',
    'DataSourceType', 'DataSourceStatus', 'DataSourceError',
    'RateLimitError', 'AuthenticationError', 'NetworkError',
    'DataSourceFactory', 'DataSourceManager',
    'AkShareDataSource', 'AKSHARE_AVAILABLE'
]
