# 数据源模块 - 简化版本，只支持AkShare

from .base_data_source import (
    BaseDataSource, DataRequest, DataResponse, DataType,
    DataSourceType, DataSourceStatus, DataSourceError,
    RateLimitError, AuthenticationError, NetworkError
)
from .data_source_factory import DataSourceFactory, get_data_service
from .data_source_manager import DataSourceManager

# 导入AkShare数据源
try:
    from .akshare_data_source import AkShareDataSource
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False

# 导入简化客户端
from .simple_data_client import SimpleDataClient, get_simple_client

__all__ = [
    'BaseDataSource', 'DataRequest', 'DataResponse', 'DataType',
    'DataSourceType', 'DataSourceStatus', 'DataSourceError',
    'RateLimitError', 'AuthenticationError', 'NetworkError',
    'DataSourceFactory', 'DataSourceManager', 'get_data_service',
    'AkShareDataSource', 'AKSHARE_AVAILABLE',
    'SimpleDataClient', 'get_simple_client'
]
