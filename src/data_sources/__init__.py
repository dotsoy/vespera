"""
数据源模块 - 提供统一的数据访问接口

此包包含了数据源系统的核心组件，包括基础数据源类、数据模型和工具函数。

主要组件:
    - base: 基础数据源抽象基类
    - enums: 数据源相关的枚举类型
    - exceptions: 自定义异常类
    - models: 数据模型类
    - data_source_factory: 数据源工厂
    - data_source_manager: 数据源管理器
"""

# 导入枚举类型
from .enums import DataType, DataSourceType, DataSourceStatus

# 导入异常类
from .exceptions.base import (
    DataSourceError, DataSourceRateLimitError, DataSourceAuthenticationError, 
    DataSourceConnectionError, DataSourceValidationError, DataSourceConfigurationError
)

# 导入基础数据源类
from .base import BaseDataSource

# 导入数据模型
from .models.request import DataRequest
from .models.response import DataResponse
from .models.info import DataSourceInfo

# 导入工厂和管理器
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
    'DataSourceRateLimitError', 'DataSourceAuthenticationError', 'DataSourceConnectionError',
    'DataSourceFactory', 'DataSourceManager', 'get_data_service',
    'AkShareDataSource', 'AKSHARE_AVAILABLE',
    'SimpleDataClient', 'get_simple_client'
]
