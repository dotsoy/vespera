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
from .akshare_data_source import AkShareDataSource
from .adata_data_source import AdataDataSource

class DataSourceFactory:
    """数据源工厂类"""
    
    def __init__(self):
        """初始化数据源工厂"""
        self._sources: Dict[str, BaseDataSource] = {}
        self._initialize_sources()
        
    def _initialize_sources(self):
        """初始化所有数据源"""
        # 初始化 AData 数据源 (免费、专注A股，优先使用)
        try:
            adata_source = AdataDataSource()
            self._register_source('adata', adata_source)
            logger.info("AData 数据源初始化成功")
        except Exception as e:
            logger.error(f"AData 数据源初始化失败: {e}")
            logger.error(f"错误详情: {str(e)}")
            import traceback
            logger.error(f"堆栈跟踪: {traceback.format_exc()}")
        
        # 初始化 AkShare 数据源 (免费、稳定，备用)
        try:
            akshare_source = AkShareDataSource()
            self._register_source('akshare', akshare_source)
            logger.info("AkShare 数据源初始化成功")
        except Exception as e:
            logger.error(f"AkShare 数据源初始化失败: {e}")
            logger.error(f"错误详情: {str(e)}")
            import traceback
            logger.error(f"堆栈跟踪: {traceback.format_exc()}")
        
        # 初始化 Tushare 数据源 (需要token，作为备用)
        try:
            from .tushare_data_source import TushareDataSource
            # 从环境变量或配置文件获取token
            import os
            tushare_token = os.getenv('TUSHARE_TOKEN', '')
            if tushare_token:
                tushare_source = TushareDataSource(token=tushare_token)
                self._register_source('tushare', tushare_source)
                logger.info("Tushare 数据源初始化成功")
            else:
                logger.info("Tushare token未配置，跳过初始化")
        except Exception as e:
            logger.error(f"Tushare 数据源初始化失败: {e}")
        
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
            
            # 自动初始化数据源
            if not source.is_available():
                logger.info(f"正在初始化数据源 {name}...")
                if not source.initialize():
                    logger.error(f"数据源 {name} 初始化失败")
                    return False
                
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
        # 定义数据源优先级顺序
        priority_order = ['adata', 'akshare', 'tushare']
        
        # 按优先级顺序尝试从数据源获取数据
        for source_name in priority_order:
            source = self._sources.get(source_name)
            if source and source.is_available():
                try:
                    logger.info(f"尝试从 {source_name} 数据源获取数据: {request.symbol}")
                    result = source.fetch_data(request)
                    # 兼容DataResponse对象
                    if hasattr(result, 'success') and hasattr(result, 'data'):
                        if result.success and result.data is not None and not result.data.empty:
                            logger.info(f"成功从 {source_name} 获取到 {len(result.data)} 条数据")
                            return result.data
                        else:
                            logger.warning(f"{source_name} 数据源返回空数据或失败: {getattr(result, 'error_message', '')}")
                    else:
                        # 兼容直接返回DataFrame的老数据源
                        if result is not None and not result.empty:
                            logger.info(f"成功从 {source_name} 获取到 {len(result)} 条数据")
                            return result
                        else:
                            logger.warning(f"{source_name} 数据源返回空数据")
                except Exception as e:
                    logger.error(f"从数据源 {source_name} 获取数据失败: {e}")
                    continue
            else:
                logger.warning(f"数据源 {source_name} 不可用或未初始化")
                
        # 如果没有数据源返回数据，返回空DataFrame
        logger.error(f"所有数据源都无法获取 {request.symbol} 的数据")
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
