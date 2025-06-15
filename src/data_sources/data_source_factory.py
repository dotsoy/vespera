"""
数据源工厂
负责创建和配置各种数据源
"""
from typing import Dict, Any, Optional, List
from loguru import logger

from .base_data_source import BaseDataSource, DataSourceType
from .tushare_data_source import TushareDataSource
from .alltick_data_source import AllTickDataSource

# 可选导入，避免依赖问题
try:
    from .yahoo_finance_data_source import YahooFinanceDataSource
    YAHOO_FINANCE_AVAILABLE = True
except ImportError:
    YahooFinanceDataSource = None
    YAHOO_FINANCE_AVAILABLE = False

try:
    from .alpha_vantage_data_source import AlphaVantageDataSource
    ALPHA_VANTAGE_AVAILABLE = True
except ImportError:
    AlphaVantageDataSource = None
    ALPHA_VANTAGE_AVAILABLE = False
from .data_source_manager import DataSourceManager
from .cache_manager import CacheManager
from config.settings import data_settings


class DataSourceFactory:
    """数据源工厂"""
    
    def __init__(self):
        self.data_source_classes = {
            DataSourceType.TUSHARE: TushareDataSource,
            DataSourceType.ALLTICK: AllTickDataSource
        }

        # 添加可选数据源
        if YAHOO_FINANCE_AVAILABLE:
            self.data_source_classes[DataSourceType.YAHOO_FINANCE] = YahooFinanceDataSource
        if ALPHA_VANTAGE_AVAILABLE:
            self.data_source_classes[DataSourceType.ALPHA_VANTAGE] = AlphaVantageDataSource
        
        self.default_configs = {
            DataSourceType.TUSHARE: {
                'priority': 1,
                'rate_limit': 200,
                'description': 'Tushare专业金融数据接口'
            },
            DataSourceType.ALLTICK: {
                'priority': 2,
                'rate_limit': 100,
                'description': 'AllTick专业金融数据API'
            }
        }

        # 添加可选数据源配置
        if YAHOO_FINANCE_AVAILABLE:
            self.default_configs[DataSourceType.YAHOO_FINANCE] = {
                'priority': 3,
                'rate_limit': 2000,
                'description': 'Yahoo Finance免费金融数据接口'
            }
        if ALPHA_VANTAGE_AVAILABLE:
            self.default_configs[DataSourceType.ALPHA_VANTAGE] = {
                'priority': 4,
                'rate_limit': 5,
                'description': 'Alpha Vantage金融数据API'
            }
    
    def create_data_source(self, source_type: DataSourceType, 
                          config: Optional[Dict[str, Any]] = None) -> Optional[BaseDataSource]:
        """
        创建数据源实例
        
        Args:
            source_type: 数据源类型
            config: 配置参数
            
        Returns:
            BaseDataSource: 数据源实例
        """
        if source_type not in self.data_source_classes:
            logger.error(f"不支持的数据源类型: {source_type}")
            return None
        
        # 合并默认配置和用户配置
        final_config = self.default_configs.get(source_type, {}).copy()
        if config:
            final_config.update(config)
        
        try:
            source_class = self.data_source_classes[source_type]
            
            if source_type == DataSourceType.TUSHARE:
                return source_class(final_config)
            elif source_type == DataSourceType.ALLTICK:
                token = final_config.get('token') or getattr(data_settings, 'alltick_token', None)
                if not token:
                    logger.error("AllTick token 未配置")
                    return None
                return source_class(token, final_config)
            elif source_type == DataSourceType.YAHOO_FINANCE and YAHOO_FINANCE_AVAILABLE:
                return source_class(final_config)
            elif source_type == DataSourceType.ALPHA_VANTAGE and ALPHA_VANTAGE_AVAILABLE:
                api_key = final_config.get('api_key') or getattr(data_settings, 'alpha_vantage_api_key', None)
                if not api_key:
                    logger.error("Alpha Vantage API key 未配置")
                    return None
                return source_class(api_key, final_config)
            
        except Exception as e:
            logger.error(f"创建数据源 {source_type} 失败: {e}")
            return None
    
    def create_manager_with_sources(self, source_configs: Optional[List[Dict[str, Any]]] = None) -> DataSourceManager:
        """
        创建配置好的数据源管理器
        
        Args:
            source_configs: 数据源配置列表
            
        Returns:
            DataSourceManager: 配置好的数据源管理器
        """
        manager = DataSourceManager()
        
        # 如果没有提供配置，使用默认配置
        if not source_configs:
            source_configs = self._get_default_source_configs()
        
        # 创建并注册数据源
        for config in source_configs:
            source_type = DataSourceType(config.get('type'))
            source_config = config.get('config', {})
            
            data_source = self.create_data_source(source_type, source_config)
            if data_source:
                success = manager.register_data_source(data_source)
                if success:
                    logger.info(f"数据源 {data_source.name} 注册成功")
                else:
                    logger.warning(f"数据源 {data_source.name} 注册失败")
        
        return manager
    
    def _get_default_source_configs(self) -> List[Dict[str, Any]]:
        """获取默认数据源配置"""
        configs = []
        
        # Tushare配置
        if hasattr(data_settings, 'tushare_token') and data_settings.tushare_token:
            configs.append({
                'type': DataSourceType.TUSHARE.value,
                'config': {
                    'priority': 1,
                    'rate_limit': 200
                }
            })

        # AllTick配置
        if hasattr(data_settings, 'alltick_token') and data_settings.alltick_token:
            configs.append({
                'type': DataSourceType.ALLTICK.value,
                'config': {
                    'priority': 2,
                    'rate_limit': 100,
                    'token': data_settings.alltick_token
                }
            })

        # Yahoo Finance配置（免费，如果可用）
        if YAHOO_FINANCE_AVAILABLE:
            configs.append({
                'type': DataSourceType.YAHOO_FINANCE.value,
                'config': {
                    'priority': 3,
                    'rate_limit': 2000
                }
            })

        # Alpha Vantage配置
        if ALPHA_VANTAGE_AVAILABLE and hasattr(data_settings, 'alpha_vantage_api_key') and data_settings.alpha_vantage_api_key:
            configs.append({
                'type': DataSourceType.ALPHA_VANTAGE.value,
                'config': {
                    'priority': 4,
                    'rate_limit': 5,
                    'api_key': data_settings.alpha_vantage_api_key
                }
            })
        
        return configs


class UnifiedDataService:
    """统一数据服务"""
    
    def __init__(self, source_configs: Optional[List[Dict[str, Any]]] = None,
                 enable_cache: bool = True, cache_dir: str = "cache"):
        self.factory = DataSourceFactory()
        self.manager = self.factory.create_manager_with_sources(source_configs)
        
        # 缓存管理器
        self.cache_manager = CacheManager(cache_dir) if enable_cache else None
        
        logger.info("统一数据服务初始化完成")
    
    def get_data(self, request, preferred_sources: Optional[List[str]] = None,
                 fallback: bool = True, merge_strategy: str = 'quality_based',
                 use_cache: bool = True):
        """
        获取数据（带缓存支持）
        
        Args:
            request: 数据请求
            preferred_sources: 首选数据源
            fallback: 是否启用故障转移
            merge_strategy: 合并策略
            use_cache: 是否使用缓存
            
        Returns:
            DataResponse: 数据响应
        """
        # 尝试从缓存获取
        if use_cache and self.cache_manager:
            cached_response = self.cache_manager.get(request)
            if cached_response:
                logger.info("从缓存获取数据成功")
                return cached_response
        
        # 从数据源获取
        response = self.manager.get_data(
            request=request,
            preferred_sources=preferred_sources,
            fallback=fallback,
            merge_strategy=merge_strategy
        )
        
        # 存储到缓存
        if use_cache and self.cache_manager and response.success:
            self.cache_manager.put(request, response)
        
        return response
    
    def get_available_sources(self, data_type):
        """获取可用数据源"""
        return self.manager.get_available_sources(data_type)
    
    def health_check(self):
        """健康检查"""
        health_status = self.manager.health_check()
        
        if self.cache_manager:
            cache_stats = self.cache_manager.get_cache_stats()
            health_status['cache'] = cache_stats
        
        return health_status
    
    def clear_cache(self, data_type=None):
        """清空缓存"""
        if self.cache_manager:
            self.cache_manager.clear_cache(data_type)
    
    def close(self):
        """关闭服务"""
        self.manager.close_all()
        logger.info("统一数据服务已关闭")


# 全局数据服务实例
_global_data_service: Optional[UnifiedDataService] = None


def get_data_service(source_configs: Optional[List[Dict[str, Any]]] = None,
                    enable_cache: bool = True, cache_dir: str = "cache") -> UnifiedDataService:
    """
    获取全局数据服务实例
    
    Args:
        source_configs: 数据源配置
        enable_cache: 是否启用缓存
        cache_dir: 缓存目录
        
    Returns:
        UnifiedDataService: 数据服务实例
    """
    global _global_data_service
    
    if _global_data_service is None:
        _global_data_service = UnifiedDataService(
            source_configs=source_configs,
            enable_cache=enable_cache,
            cache_dir=cache_dir
        )
    
    return _global_data_service


def close_data_service():
    """关闭全局数据服务"""
    global _global_data_service
    
    if _global_data_service:
        _global_data_service.close()
        _global_data_service = None
