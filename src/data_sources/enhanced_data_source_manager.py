"""
增强版数据源管理器
实现多数据源备份、故障转移、质量监控等功能
"""
import asyncio
import time
from typing import Dict, List, Optional, Union, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
from collections import defaultdict, deque
import json
import hashlib

try:
    from .base_data_source import BaseDataSource, DataRequest, DataResponse
except ImportError:
    # 创建模拟类用于测试
    class BaseDataSource:
        async def fetch_data(self, request):
            pass
    
    class DataRequest:
        pass
    
    class DataResponse:
        pass
try:
    from src.utils.logger import get_logger
except ImportError:
    import logging
    def get_logger(name):
        return logging.getLogger(name)

logger = get_logger("enhanced_data_source_manager")


class DataSourcePriority(Enum):
    """数据源优先级"""
    PRIMARY = 1
    SECONDARY = 2
    BACKUP = 3
    CACHE = 4


@dataclass
class DataSourceConfig:
    """数据源配置"""
    name: str
    priority: DataSourcePriority
    weight: float = 1.0
    max_retries: int = 3
    timeout: float = 30.0
    rate_limit: int = 100  # 每分钟请求数
    enabled: bool = True
    health_check_interval: int = 300  # 健康检查间隔(秒)


@dataclass
class DataSourceMetrics:
    """数据源指标"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    avg_response_time: float = 0.0
    last_success_time: Optional[datetime] = None
    last_failure_time: Optional[datetime] = None
    consecutive_failures: int = 0
    health_score: float = 1.0
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests
    
    def update_success(self, response_time: float):
        """更新成功指标"""
        self.total_requests += 1
        self.successful_requests += 1
        self.consecutive_failures = 0
        self.last_success_time = datetime.now()
        
        # 更新平均响应时间
        if self.avg_response_time == 0:
            self.avg_response_time = response_time
        else:
            self.avg_response_time = (self.avg_response_time * 0.9 + response_time * 0.1)
        
        # 更新健康分数
        self._update_health_score()
    
    def update_failure(self):
        """更新失败指标"""
        self.total_requests += 1
        self.failed_requests += 1
        self.consecutive_failures += 1
        self.last_failure_time = datetime.now()
        self._update_health_score()
    
    def _update_health_score(self):
        """更新健康分数"""
        # 基于成功率和连续失败次数计算健康分数
        base_score = self.success_rate
        failure_penalty = min(self.consecutive_failures * 0.1, 0.5)
        self.health_score = max(0.0, base_score - failure_penalty)


class EnhancedDataSourceManager:
    """增强版数据源管理器"""
    
    def __init__(self):
        self.data_sources: Dict[str, BaseDataSource] = {}
        self.configs: Dict[str, DataSourceConfig] = {}
        self.metrics: Dict[str, DataSourceMetrics] = {}
        self.cache: Dict[str, Any] = {}
        self.cache_ttl: Dict[str, datetime] = {}
        
        # 请求历史记录
        self.request_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # 故障转移配置
        self.fallback_enabled = True
        self.circuit_breaker_threshold = 5  # 连续失败阈值
        self.circuit_breaker_timeout = 300  # 熔断器超时时间(秒)
        
        # 数据质量检查器
        self.quality_checkers: List[Callable] = []
        
        logger.info("增强版数据源管理器初始化完成")
    
    def register_data_source(self, source: BaseDataSource, config: DataSourceConfig):
        """注册数据源"""
        self.data_sources[config.name] = source
        self.configs[config.name] = config
        self.metrics[config.name] = DataSourceMetrics()
        
        logger.info(f"注册数据源: {config.name}, 优先级: {config.priority.name}")
    
    def add_quality_checker(self, checker: Callable[[pd.DataFrame], bool]):
        """添加数据质量检查器"""
        self.quality_checkers.append(checker)
    
    async def get_data(self, request: DataRequest) -> pd.DataFrame:
        """获取数据 - 支持故障转移"""
        cache_key = self._generate_cache_key(request)
        
        # 检查缓存
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"从缓存获取数据: {cache_key}")
            return cached_data
        
        # 获取可用数据源列表
        available_sources = self._get_available_sources()
        
        if not available_sources:
            raise Exception("没有可用的数据源")
        
        # 按优先级排序
        sorted_sources = sorted(available_sources, 
                              key=lambda x: (self.configs[x].priority.value, -self.metrics[x].health_score))
        
        last_exception = None
        
        # 尝试从每个数据源获取数据
        for source_name in sorted_sources:
            try:
                data = await self._fetch_from_source(source_name, request)
                
                # 数据质量检查
                if self._validate_data_quality(data):
                    # 缓存数据
                    self._cache_data(cache_key, data)
                    return data
                else:
                    logger.warning(f"数据源 {source_name} 数据质量检查失败")
                    continue
                    
            except Exception as e:
                last_exception = e
                logger.warning(f"数据源 {source_name} 获取数据失败: {e}")
                self.metrics[source_name].update_failure()
                continue
        
        # 所有数据源都失败
        raise Exception(f"所有数据源均不可用，最后错误: {last_exception}")
    
    async def _fetch_from_source(self, source_name: str, request: DataRequest) -> pd.DataFrame:
        """从指定数据源获取数据"""
        source = self.data_sources[source_name]
        config = self.configs[source_name]
        
        # 检查速率限制
        if not self._check_rate_limit(source_name):
            raise Exception(f"数据源 {source_name} 达到速率限制")
        
        # 检查熔断器
        if self._is_circuit_breaker_open(source_name):
            raise Exception(f"数据源 {source_name} 熔断器开启")
        
        start_time = time.time()
        
        try:
            # 记录请求
            self.request_history[source_name].append(datetime.now())
            
            # 获取数据
            data = await asyncio.wait_for(
                source.fetch_data(request),
                timeout=config.timeout
            )
            
            response_time = time.time() - start_time
            self.metrics[source_name].update_success(response_time)
            
            logger.debug(f"从 {source_name} 成功获取数据，耗时: {response_time:.2f}s")
            return data
            
        except Exception as e:
            self.metrics[source_name].update_failure()
            raise e
    
    def _get_available_sources(self) -> List[str]:
        """获取可用数据源列表"""
        available = []
        
        for name, config in self.configs.items():
            if not config.enabled:
                continue
                
            metrics = self.metrics[name]
            
            # 检查健康分数
            if metrics.health_score < 0.3:
                continue
                
            # 检查熔断器
            if self._is_circuit_breaker_open(name):
                continue
                
            available.append(name)
        
        return available
    
    def _check_rate_limit(self, source_name: str) -> bool:
        """检查速率限制"""
        config = self.configs[source_name]
        history = self.request_history[source_name]
        
        if not history:
            return True
        
        # 计算最近一分钟的请求数
        now = datetime.now()
        recent_requests = sum(1 for req_time in history 
                            if (now - req_time).total_seconds() < 60)
        
        return recent_requests < config.rate_limit
    
    def _is_circuit_breaker_open(self, source_name: str) -> bool:
        """检查熔断器状态"""
        metrics = self.metrics[source_name]
        
        # 如果连续失败次数超过阈值，开启熔断器
        if metrics.consecutive_failures >= self.circuit_breaker_threshold:
            if metrics.last_failure_time:
                time_since_failure = (datetime.now() - metrics.last_failure_time).total_seconds()
                return time_since_failure < self.circuit_breaker_timeout
        
        return False
    
    def _validate_data_quality(self, data: pd.DataFrame) -> bool:
        """验证数据质量"""
        if data is None or data.empty:
            return False
        
        # 运行所有质量检查器
        for checker in self.quality_checkers:
            try:
                if not checker(data):
                    return False
            except Exception as e:
                logger.warning(f"数据质量检查器执行失败: {e}")
                return False
        
        return True
    
    def _generate_cache_key(self, request: DataRequest) -> str:
        """生成缓存键"""
        request_str = json.dumps({
            "symbol": getattr(request, 'symbol', ''),
            "start_date": getattr(request, 'start_date', ''),
            "end_date": getattr(request, 'end_date', ''),
            "data_type": getattr(request, 'data_type', '')
        }, sort_keys=True)
        
        return hashlib.md5(request_str.encode()).hexdigest()
    
    def _get_from_cache(self, cache_key: str) -> Optional[pd.DataFrame]:
        """从缓存获取数据"""
        if cache_key not in self.cache:
            return None
        
        # 检查缓存是否过期
        if cache_key in self.cache_ttl:
            if datetime.now() > self.cache_ttl[cache_key]:
                del self.cache[cache_key]
                del self.cache_ttl[cache_key]
                return None
        
        return self.cache[cache_key]
    
    def _cache_data(self, cache_key: str, data: pd.DataFrame, ttl_minutes: int = 5):
        """缓存数据"""
        self.cache[cache_key] = data.copy()
        self.cache_ttl[cache_key] = datetime.now() + timedelta(minutes=ttl_minutes)
    
    def get_health_report(self) -> Dict[str, Any]:
        """获取健康报告"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "sources": {}
        }
        
        for name, metrics in self.metrics.items():
            config = self.configs[name]
            report["sources"][name] = {
                "enabled": config.enabled,
                "priority": config.priority.name,
                "health_score": metrics.health_score,
                "success_rate": metrics.success_rate,
                "total_requests": metrics.total_requests,
                "avg_response_time": metrics.avg_response_time,
                "consecutive_failures": metrics.consecutive_failures,
                "last_success": metrics.last_success_time.isoformat() if metrics.last_success_time else None,
                "circuit_breaker_open": self._is_circuit_breaker_open(name)
            }
        
        return report
    
    def reset_metrics(self, source_name: str = None):
        """重置指标"""
        if source_name:
            if source_name in self.metrics:
                self.metrics[source_name] = DataSourceMetrics()
                logger.info(f"重置数据源 {source_name} 的指标")
        else:
            for name in self.metrics:
                self.metrics[name] = DataSourceMetrics()
            logger.info("重置所有数据源指标")
    
    def disable_source(self, source_name: str):
        """禁用数据源"""
        if source_name in self.configs:
            self.configs[source_name].enabled = False
            logger.info(f"禁用数据源: {source_name}")
    
    def enable_source(self, source_name: str):
        """启用数据源"""
        if source_name in self.configs:
            self.configs[source_name].enabled = True
            logger.info(f"启用数据源: {source_name}")