"""
基础数据源模块

该模块定义了数据源系统的抽象基类 BaseDataSource，所有具体的数据源实现都应该继承此类。
"""
from __future__ import annotations

import abc
import logging
import threading
import time
from datetime import datetime, date, timedelta
from typing import (
    Any, Dict, List, Optional, Type, TypeVar, Union, 
    Generic, ClassVar, Set, Tuple, cast, Callable, overload
)

import numpy as np
import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field, validator

from .enums import DataSourceStatus, DataSourceType, DataType
from .exceptions.base import (
    DataSourceError, DataSourceRateLimitError, DataSourceAuthenticationError,
    DataSourceConnectionError, DataSourceValidationError, DataSourceConfigurationError
)

# 类型变量和类型别名
T = TypeVar('T')
TimestampType = Union[str, datetime, date, int, float]
SymbolType = Union[str, Tuple[str, str]]  # (symbol, exchange) 或 symbol
DataFrameOrDict = Union[pd.DataFrame, Dict[str, Any]]

class BaseDataSource(abc.ABC):
    """
    数据源抽象基类
    
    所有数据源实现都应该继承此类并实现抽象方法。
    这个类提供了数据缓存、请求限流、错误处理等通用功能。
    
    属性:
        name: 数据源名称
        source_type: 数据源类型
        status: 当前状态
        config: 配置字典
        last_request_time: 最后请求时间
        request_count: 请求计数器
        rate_limit: 请求速率限制 (requests/second)
        cache_enabled: 是否启用缓存
        cache: 缓存字典
        cache_ttl: 缓存过期时间 (秒)
        logger: 日志记录器
    """
    
    def __init__(
        self,
        name: str,
        source_type: Union[DataSourceType, str],
        config: Optional[Dict[str, Any]] = None,
        rate_limit: float = 1.0,
        cache_enabled: bool = True,
        cache_ttl: int = 300,
        **kwargs
    ) -> None:
        """
        初始化数据源
        
        Args:
            name: 数据源名称
            source_type: 数据源类型
            config: 配置字典
            rate_limit: 请求速率限制 (requests/second)
            cache_enabled: 是否启用缓存
            cache_ttl: 缓存过期时间 (秒)
            **kwargs: 其他关键字参数
        """
        self.name = name
        self.source_type = (
            DataSourceType(source_type) 
            if isinstance(source_type, str) 
            else source_type
        )
        self.config = config or {}
        self.status = DataSourceStatus.UNINITIALIZED
        self.rate_limit = max(0.1, min(rate_limit, 1000.0))  # 限制在合理范围内
        self.cache_enabled = cache_enabled
        self.cache_ttl = max(1, cache_ttl)  # 最小1秒
        self._cache: Dict[str, Tuple[float, Any]] = {}  # (timestamp, data)
        self._lock = threading.RLock()
        self._last_request_time: float = 0.0
        self._request_count: int = 0
        self._logger = logger.bind(component=f"datasource.{name}")
        
        # 初始化数据源
        self.initialize()
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', type='{self.source_type.value}')"
    
    def __str__(self) -> str:
        return f"{self.name} ({self.source_type.value})"
    
    def initialize(self) -> None:
        """初始化数据源"""
        if self.status == DataSourceStatus.UNINITIALIZED:
            self.status = DataSourceStatus.INITIALIZING
            try:
                self._initialize()
                self.status = DataSourceStatus.READY
                self._logger.info(f"Data source '{self.name}' initialized successfully")
            except Exception as e:
                self.status = DataSourceStatus.ERROR
                self._logger.error(f"Failed to initialize data source '{self.name}': {e}")
                raise
    
    def _initialize(self) -> None:
        """子类应该实现此方法以执行特定的初始化逻辑"""
        pass
    
    def close(self) -> None:
        """关闭数据源并释放资源"""
        self.status = DataSourceStatus.OFFLINE
        self._logger.info(f"Data source '{self.name}' closed")
    
    def __enter__(self) -> 'BaseDataSource':
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
    
    def _rate_limit(self) -> None:
        """实现请求速率限制"""
        if self.rate_limit <= 0:
            return
            
        now = time.time()
        min_interval = 1.0 / self.rate_limit
        elapsed = now - self._last_request_time
        
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        
        self._last_request_time = time.time()
    
    def _get_from_cache(self, key: str) -> Optional[Any]:
        """从缓存中获取数据"""
        if not self.cache_enabled or key not in self._cache:
            return None
            
        timestamp, data = self._cache[key]
        if time.time() - timestamp > self.cache_ttl:
            del self._cache[key]
            return None
            
        return data
    
    def _set_to_cache(self, key: str, data: Any) -> None:
        """将数据保存到缓存"""
        if self.cache_enabled:
            with self._lock:
                self._cache[key] = (time.time(), data)
    
    def clear_cache(self) -> None:
        """清除所有缓存数据"""
        with self._lock:
            self._cache.clear()
    
    @abc.abstractmethod
    async def fetch_data(self, request: 'DataRequest') -> 'DataResponse':
        """
        异步获取数据
        
        Args:
            request: 数据请求对象
            
        Returns:
            DataResponse: 数据响应对象
            
        Raises:
            DataSourceError: 数据源错误
            RateLimitError: 请求频率超限
            AuthenticationError: 认证失败
            NetworkError: 网络错误
            ValidationError: 请求参数验证失败
        """
        pass
    
    def get_status(self) -> Dict[str, Any]:
        """获取数据源状态信息"""
        return {
            'name': self.name,
            'type': self.source_type.value,
            'status': self.status.value,
            'cache_enabled': self.cache_enabled,
            'cache_size': len(self._cache),
            'rate_limit': self.rate_limit,
            'request_count': self._request_count,
            'last_request_time': datetime.fromtimestamp(self._last_request_time).isoformat() 
                              if self._last_request_time > 0 else None
        }
