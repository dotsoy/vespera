"""
数据源工厂模块
用于创建和管理数据源实例
"""
import importlib
from typing import Dict, Optional, Type, Union
from loguru import logger
import pandas as pd

from .base_data_source import (
    BaseDataSource,
    DataType,
    DataSourceType,
    DataRequest,
    DataSourceError
)
from .alltick_data_source import AllTickDataSource

class DataSourceFactory:
    """数据源工厂类"""
    
    def __init__(self):
        """初始化数据源工厂"""
        self._sources: Dict[str, BaseDataSource] = {}
        self._initialize_sources()
        
    def _initialize_sources(self):
        """初始化所有数据源"""
        # 初始化 AllTick 数据源
        try:
            alltick_source = AllTickDataSource(api_token='5d77b3af30d6b74b6bad3340996cb399-c-app')
            if alltick_source.initialize():
                self._register_source('alltick', alltick_source)
                logger.info("AllTick 数据源初始化成功")
            else:
                logger.warning("AllTick 数据源初始化失败")
        except Exception as e:
            logger.error(f"AllTick 数据源初始化失败: {e}")
        
    def _register_source(self, name: str, source: Union[str, BaseDataSource], class_name: Optional[str] = None) -> bool:
        """注册数据源
        
        Args:
            name: 数据源名称
            source: 数据源实例或模块路径
            class_name: 数据源类名（如果source是模块路径）
            
        Returns:
            bool: 是否注册成功
        """
        try:
            if isinstance(source, str):
                module = importlib.import_module(f'.{source}', package='src.data_sources')
                source_class = getattr(module, class_name)
                source = source_class()
                
            if not isinstance(source, BaseDataSource):
                raise ValueError(f"数据源 {name} 必须是 BaseDataSource 的实例")
                
            self._sources[name] = source
            logger.info(f"数据源 {name} 注册成功")
            return True
            
        except Exception as e:
            logger.error(f"注册数据源 {name} 失败: {e}")
            return False
            
    def get_source(self, name: str) -> Optional[BaseDataSource]:
        """获取数据源实例
        
        Args:
            name: 数据源名称
            
        Returns:
            Optional[BaseDataSource]: 数据源实例
        """
        return self._sources.get(name)
        
    def get_available_sources(self) -> Dict[str, BaseDataSource]:
        """获取所有可用的数据源
        
        Returns:
            Dict[str, BaseDataSource]: 数据源字典
        """
        return {name: source for name, source in self._sources.items() 
                if source.is_available()}
                
    def get_data(self, request: DataRequest) -> pd.DataFrame:
        """获取数据
        
        Args:
            request: 数据请求对象
            
        Returns:
            pd.DataFrame: 数据DataFrame
        """
        # 尝试从所有可用的数据源获取数据
        for source in self.get_available_sources().values():
            try:
                df = source.fetch_data(request)
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                logger.error(f"从数据源 {source.__class__.__name__} 获取数据失败: {e}")
                continue
                
        # 如果没有数据源返回数据，返回空DataFrame
        return pd.DataFrame()
                
    def close_all(self):
        """关闭所有数据源"""
        for source in self._sources.values():
            try:
                source.close()
            except Exception as e:
                logger.error(f"关闭数据源失败: {e}")
        self._sources.clear()
        logger.info("所有数据源已关闭")

# 创建全局数据源工厂实例
_factory = None

def get_data_service() -> DataSourceFactory:
    """获取数据服务实例
    
    Returns:
        DataSourceFactory: 数据源工厂实例
    """
    global _factory
    if _factory is None:
        _factory = DataSourceFactory()
    return _factory
