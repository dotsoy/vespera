"""
数据源管理器
负责管理多个数据源，提供统一的数据获取接口
"""
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from loguru import logger

from .base_data_source import (
    BaseDataSource, DataRequest, DataResponse, DataType,
    DataSourceType, DataSourceStatus, DataSourceInfo,
    DataSourceError, RateLimitError, DataNotAvailableError
)
from .data_fusion_engine import DataFusionEngine, FusionStrategy, ValidationLevel


class DataSourceManager:
    """数据源管理器"""
    
    def __init__(self, max_workers: int = 3):
        self.data_sources: Dict[str, BaseDataSource] = {}
        self.max_workers = max_workers
        self._cache: Dict[str, DataResponse] = {}
        self._cache_ttl = 300  # 缓存5分钟
        
    def register_data_source(self, data_source: BaseDataSource) -> bool:
        """
        注册数据源
        
        Args:
            data_source: 数据源实例
            
        Returns:
            bool: 注册是否成功
        """
        try:
            if data_source.initialize():
                self.data_sources[data_source.name] = data_source
                logger.info(f"数据源 {data_source.name} 注册成功")
                return True
            else:
                logger.error(f"数据源 {data_source.name} 初始化失败")
                return False
        except Exception as e:
            logger.error(f"注册数据源 {data_source.name} 失败: {e}")
            return False
    
    def unregister_data_source(self, name: str) -> bool:
        """
        注销数据源
        
        Args:
            name: 数据源名称
            
        Returns:
            bool: 注销是否成功
        """
        if name in self.data_sources:
            try:
                self.data_sources[name].close()
                del self.data_sources[name]
                logger.info(f"数据源 {name} 注销成功")
                return True
            except Exception as e:
                logger.error(f"注销数据源 {name} 失败: {e}")
                return False
        else:
            logger.warning(f"数据源 {name} 不存在")
            return False
    
    def get_available_sources(self, data_type: DataType) -> List[str]:
        """
        获取支持指定数据类型的可用数据源
        
        Args:
            data_type: 数据类型
            
        Returns:
            List[str]: 可用数据源名称列表
        """
        available_sources = []
        
        for name, source in self.data_sources.items():
            try:
                # 检查数据源状态
                status = source.check_availability()
                if status in [DataSourceStatus.AVAILABLE, DataSourceStatus.LIMITED]:
                    # 检查是否支持该数据类型
                    if data_type in source.get_supported_data_types():
                        available_sources.append(name)
            except Exception as e:
                logger.warning(f"检查数据源 {name} 可用性失败: {e}")
        
        # 按优先级排序
        available_sources.sort(key=lambda x: self.data_sources[x].config.get('priority', 1))
        
        return available_sources
    
    def get_data(self, request: DataRequest, 
                 preferred_sources: Optional[List[str]] = None,
                 fallback: bool = True,
                 merge_strategy: str = 'first_success') -> DataResponse:
        """
        获取数据
        
        Args:
            request: 数据请求参数
            preferred_sources: 首选数据源列表
            fallback: 是否启用故障转移
            merge_strategy: 多源数据合并策略 ('first_success', 'merge', 'validate')
            
        Returns:
            DataResponse: 数据响应结果
        """
        # 检查缓存
        cache_key = self._generate_cache_key(request)
        cached_response = self._get_from_cache(cache_key)
        if cached_response:
            logger.info(f"从缓存获取数据: {cache_key}")
            return cached_response
        
        # 确定数据源优先级
        available_sources = self.get_available_sources(request.data_type)
        
        if preferred_sources:
            # 使用首选数据源，但保持可用性检查
            sources_to_try = [s for s in preferred_sources if s in available_sources]
            if fallback:
                # 添加其他可用数据源作为备选
                sources_to_try.extend([s for s in available_sources if s not in sources_to_try])
        else:
            sources_to_try = available_sources
        
        if not sources_to_try:
            return DataResponse(
                data=pd.DataFrame(),
                source="none",
                data_type=request.data_type,
                timestamp=datetime.now(),
                success=False,
                error_message=f"没有可用的数据源支持 {request.data_type}"
            )
        
        # 根据合并策略获取数据
        if merge_strategy == 'first_success':
            return self._get_data_first_success(request, sources_to_try, cache_key)
        elif merge_strategy == 'merge':
            return self._get_data_merge(request, sources_to_try, cache_key)
        elif merge_strategy == 'validate':
            return self._get_data_validate(request, sources_to_try, cache_key)
        else:
            raise ValueError(f"不支持的合并策略: {merge_strategy}")
    
    def _get_data_first_success(self, request: DataRequest, sources: List[str], cache_key: str) -> DataResponse:
        """第一个成功策略：使用第一个成功返回数据的数据源"""
        for source_name in sources:
            try:
                source = self.data_sources[source_name]
                logger.info(f"尝试从数据源 {source_name} 获取数据")
                
                response = source.fetch_data(request)
                
                if response.success and not response.data.empty:
                    logger.info(f"从数据源 {source_name} 成功获取数据")
                    self._save_to_cache(cache_key, response)
                    return response
                else:
                    logger.warning(f"数据源 {source_name} 返回空数据或失败")
                    
            except RateLimitError as e:
                logger.warning(f"数据源 {source_name} 请求频率超限: {e}")
                continue
            except Exception as e:
                logger.error(f"从数据源 {source_name} 获取数据失败: {e}")
                continue
        
        return DataResponse(
            data=pd.DataFrame(),
            source="failed",
            data_type=request.data_type,
            timestamp=datetime.now(),
            success=False,
            error_message="所有数据源都获取失败"
        )
    
    def _get_data_merge(self, request: DataRequest, sources: List[str], cache_key: str) -> DataResponse:
        """合并策略：从多个数据源获取数据并合并"""
        responses = []
        
        # 并行获取数据
        with ThreadPoolExecutor(max_workers=min(len(sources), self.max_workers)) as executor:
            future_to_source = {
                executor.submit(self._fetch_from_source, source_name, request): source_name
                for source_name in sources
            }
            
            for future in as_completed(future_to_source):
                source_name = future_to_source[future]
                try:
                    response = future.result(timeout=30)
                    if response.success and not response.data.empty:
                        responses.append(response)
                        logger.info(f"从数据源 {source_name} 获取数据成功")
                except Exception as e:
                    logger.error(f"从数据源 {source_name} 获取数据失败: {e}")
        
        if not responses:
            return DataResponse(
                data=pd.DataFrame(),
                source="failed",
                data_type=request.data_type,
                timestamp=datetime.now(),
                success=False,
                error_message="所有数据源都获取失败"
            )
        
        # 合并数据
        merged_data = self._merge_responses(responses)
        merged_response = DataResponse(
            data=merged_data,
            source=",".join([r.source for r in responses]),
            data_type=request.data_type,
            timestamp=datetime.now(),
            success=True,
            metadata={"source_count": len(responses)}
        )
        
        self._save_to_cache(cache_key, merged_response)
        return merged_response
    
    def _get_data_validate(self, request: DataRequest, sources: List[str], cache_key: str) -> DataResponse:
        """验证策略：从多个数据源获取数据并交叉验证"""
        if len(sources) < 2:
            return self._get_data_first_success(request, sources, cache_key)
        
        # 获取前两个数据源的数据进行验证
        primary_sources = sources[:2]
        responses = []
        
        for source_name in primary_sources:
            try:
                response = self._fetch_from_source(source_name, request)
                if response.success and not response.data.empty:
                    responses.append(response)
            except Exception as e:
                logger.error(f"从数据源 {source_name} 获取数据失败: {e}")
        
        if len(responses) >= 2:
            # 验证数据一致性
            if self._validate_responses(responses):
                logger.info("数据验证通过")
                validated_response = responses[0]  # 使用第一个数据源的数据
                validated_response.metadata = {"validated": True, "source_count": len(responses)}
                self._save_to_cache(cache_key, validated_response)
                return validated_response
            else:
                logger.warning("数据验证失败，使用第一个数据源的数据")
        
        # 验证失败或数据不足，回退到第一个成功策略
        return self._get_data_first_success(request, sources, cache_key)
    
    def _fetch_from_source(self, source_name: str, request: DataRequest) -> DataResponse:
        """从指定数据源获取数据"""
        source = self.data_sources[source_name]
        return source.fetch_data(request)
    
    def _merge_responses(self, responses: List[DataResponse]) -> pd.DataFrame:
        """合并多个数据响应"""
        if not responses:
            return pd.DataFrame()
        
        if len(responses) == 1:
            return responses[0].data
        
        # 简单合并策略：使用第一个数据源的数据作为基础，用其他数据源填补缺失值
        base_data = responses[0].data.copy()
        
        for response in responses[1:]:
            # 这里可以实现更复杂的合并逻辑
            # 目前只是简单地用后续数据填补缺失值
            base_data = base_data.combine_first(response.data)
        
        return base_data
    
    def _validate_responses(self, responses: List[DataResponse]) -> bool:
        """验证多个数据响应的一致性"""
        if len(responses) < 2:
            return True
        
        # 简单验证：检查数据形状和关键字段
        base_data = responses[0].data
        
        for response in responses[1:]:
            data = response.data
            
            # 检查数据形状
            if data.shape != base_data.shape:
                logger.warning("数据形状不一致")
                return False
            
            # 检查关键字段的差异（这里可以根据具体需求调整）
            if 'close_price' in base_data.columns and 'close_price' in data.columns:
                price_diff = abs(base_data['close_price'] - data['close_price']).mean()
                if price_diff > base_data['close_price'].mean() * 0.01:  # 1%的差异阈值
                    logger.warning(f"价格数据差异过大: {price_diff}")
                    return False
        
        return True
    
    def _generate_cache_key(self, request: DataRequest) -> str:
        """生成缓存键"""
        key_parts = [
            str(request.data_type.value),
            str(request.symbol or ""),
            str(request.symbols or ""),
            str(request.start_date or ""),
            str(request.end_date or ""),
            str(sorted(request.fields or [])),
            str(sorted((request.extra_params or {}).items()))
        ]
        return "|".join(key_parts)
    
    def _get_from_cache(self, cache_key: str) -> Optional[DataResponse]:
        """从缓存获取数据"""
        if cache_key in self._cache:
            cached_response = self._cache[cache_key]
            # 检查缓存是否过期
            if (datetime.now() - cached_response.timestamp).total_seconds() < self._cache_ttl:
                return cached_response
            else:
                del self._cache[cache_key]
        return None
    
    def _save_to_cache(self, cache_key: str, response: DataResponse):
        """保存数据到缓存"""
        self._cache[cache_key] = response
    
    def clear_cache(self):
        """清空缓存"""
        self._cache.clear()
        logger.info("数据缓存已清空")
    
    def get_source_info(self) -> List[DataSourceInfo]:
        """获取所有数据源信息"""
        return [source.get_info() for source in self.data_sources.values()]
    
    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        health_status = {
            "total_sources": len(self.data_sources),
            "available_sources": 0,
            "unavailable_sources": 0,
            "sources": {}
        }
        
        for name, source in self.data_sources.items():
            try:
                status = source.check_availability()
                health_status["sources"][name] = {
                    "status": status.value,
                    "type": source.source_type.value
                }
                
                if status == DataSourceStatus.AVAILABLE:
                    health_status["available_sources"] += 1
                else:
                    health_status["unavailable_sources"] += 1
                    
            except Exception as e:
                health_status["sources"][name] = {
                    "status": "error",
                    "error": str(e)
                }
                health_status["unavailable_sources"] += 1
        
        return health_status
    
    def close_all(self):
        """关闭所有数据源"""
        for source in self.data_sources.values():
            try:
                source.close()
            except Exception as e:
                logger.error(f"关闭数据源失败: {e}")
        
        self.data_sources.clear()
        self.clear_cache()
        logger.info("所有数据源已关闭")
