"""
数据源管理器 - 增强版本
支持多数据源负载均衡、频率限制管理、智能轮换等功能
"""
from typing import Dict, List, Optional, Union, Any
from datetime import datetime, timedelta
import pandas as pd
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict, deque
from loguru import logger

from .base_data_source import (
    BaseDataSource, DataRequest, DataResponse, DataType,
    DataSourceType, DataSourceStatus, DataSourceInfo,
    DataSourceError, RateLimitError
)


class DataSourceManager:
    """数据源管理器 - 增强版本"""

    def __init__(self, max_workers: int = 3):
        self.data_sources: Dict[str, BaseDataSource] = {}
        self._cache: Dict[str, DataResponse] = {}
        self._cache_ttl = 300  # 缓存5分钟
        self.max_workers = max_workers

        # 频率限制管理
        self._request_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._source_cooldown: Dict[str, datetime] = {}
        self._source_weights: Dict[str, float] = {}  # 数据源权重
        self._source_success_rate: Dict[str, float] = {}  # 成功率统计

        # 负载均衡
        self._round_robin_index: Dict[str, int] = defaultdict(int)
        self._source_usage_count: Dict[str, int] = defaultdict(int)

        # 配置参数
        self.rate_limit_window = 60  # 频率限制窗口（秒）
        self.max_requests_per_window = 10  # 每个窗口最大请求数
        self.cooldown_duration = 30  # 冷却时间（秒）
        self.min_success_rate = 0.7  # 最小成功率阈值
        
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
                logger.info(f"数据源 {name} 状态: {status}")
                if status in [DataSourceStatus.AVAILABLE, DataSourceStatus.LIMITED]:
                    supported_types = source.get_supported_data_types()
                    logger.info(f"数据源 {name} 支持的数据类型: {supported_types}")
                    # 检查是否支持该数据类型
                    if data_type in supported_types:
                        available_sources.append(name)
                    else:
                        logger.warning(f"数据源 {name} 不支持数据类型 {data_type}")
                else:
                    logger.warning(f"数据源 {name} 不可用，状态: {status}")
            except Exception as e:
                logger.warning(f"检查数据源 {name} 可用性失败: {e}")
        
        # 智能排序：综合考虑优先级、成功率、使用频率
        available_sources.sort(key=lambda x: self._calculate_source_score(x), reverse=True)
        logger.info(f"可用数据源列表 for {data_type}: {available_sources}")

        return available_sources

    def _calculate_source_score(self, source_name: str) -> float:
        """计算数据源评分，用于智能选择"""
        source = self.data_sources[source_name]

        # 基础优先级
        priority_score = source.config.get('priority', 1) * 10

        # 成功率评分
        success_rate = self._source_success_rate.get(source_name, 1.0)
        success_score = success_rate * 20

        # 使用频率惩罚（避免过度使用单一数据源）
        usage_count = self._source_usage_count.get(source_name, 0)
        total_usage = sum(self._source_usage_count.values()) or 1
        usage_ratio = usage_count / total_usage
        usage_penalty = usage_ratio * 10  # 使用越多，惩罚越大

        # 冷却时间惩罚
        cooldown_penalty = 0
        if source_name in self._source_cooldown:
            cooldown_remaining = (self._source_cooldown[source_name] - datetime.now()).total_seconds()
            if cooldown_remaining > 0:
                cooldown_penalty = 50  # 冷却期间大幅降低评分

        # 频率限制检查
        rate_limit_penalty = 0
        if self._is_rate_limited(source_name):
            rate_limit_penalty = 30

        final_score = priority_score + success_score - usage_penalty - cooldown_penalty - rate_limit_penalty

        logger.debug(f"数据源 {source_name} 评分: {final_score:.2f} "
                    f"(优先级:{priority_score}, 成功率:{success_score:.2f}, "
                    f"使用惩罚:{usage_penalty:.2f}, 冷却惩罚:{cooldown_penalty}, "
                    f"频率惩罚:{rate_limit_penalty})")

        return final_score

    def _is_rate_limited(self, source_name: str) -> bool:
        """检查数据源是否达到频率限制"""
        now = datetime.now()
        history = self._request_history[source_name]

        # 清理过期记录
        cutoff_time = now - timedelta(seconds=self.rate_limit_window)
        while history and history[0] < cutoff_time:
            history.popleft()

        return len(history) >= self.max_requests_per_window

    def _record_request(self, source_name: str, success: bool):
        """记录请求历史和成功率"""
        now = datetime.now()

        # 记录请求时间
        self._request_history[source_name].append(now)

        # 更新使用计数
        self._source_usage_count[source_name] += 1

        # 更新成功率
        if source_name not in self._source_success_rate:
            self._source_success_rate[source_name] = 1.0 if success else 0.0
        else:
            # 使用指数移动平均更新成功率
            alpha = 0.1  # 平滑因子
            current_rate = self._source_success_rate[source_name]
            new_rate = alpha * (1.0 if success else 0.0) + (1 - alpha) * current_rate
            self._source_success_rate[source_name] = new_rate

        # 如果失败且成功率过低，设置冷却时间
        if not success and self._source_success_rate[source_name] < self.min_success_rate:
            self._source_cooldown[source_name] = now + timedelta(seconds=self.cooldown_duration)
            logger.warning(f"数据源 {source_name} 成功率过低 ({self._source_success_rate[source_name]:.2f})，"
                          f"进入冷却期 {self.cooldown_duration} 秒")
    
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
        # 记录请求日志
        request_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        request_details = {
            'data_type': request.data_type,
            'symbol': getattr(request, 'symbol', None),
            'symbols': getattr(request, 'symbols', None),
            'start_date': getattr(request, 'start_date', None),
            'end_date': getattr(request, 'end_date', None),
            'fields': getattr(request, 'fields', None),
            'extra_params': getattr(request, 'extra_params', None)
        }
        logger.info(f"接口请求 - 时间: {request_time}, 详情: {request_details}")
        
        # 首先从 ClickHouse 数据库中获取数据
        try:
            from src.utils.database import DatabaseManager
            db_manager = DatabaseManager()
            df = db_manager.query_dataframe(f"SELECT * FROM daily_quotes WHERE symbol = '{request.symbol}' AND trade_date BETWEEN '{request.start_date}' AND '{request.end_date}'")
            if not df.empty:
                logger.info(f"从 ClickHouse 成功获取数据，记录数: {len(df)}")
                return df
        except Exception as e:
            logger.warning(f"从 ClickHouse 获取数据失败: {e}")

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
            logger.error(f"没有可用的数据源支持 {request.data_type}")
            return DataResponse(
                data=pd.DataFrame(),
                source="none",
                data_type=request.data_type,
                timestamp=datetime.now(),
                success=False,
                error_message=f"没有可用的数据源支持 {request.data_type}"
            )
        
        logger.info(f"尝试数据源: {sources_to_try}")
        
        # 根据合并策略获取数据
        if merge_strategy == 'first_success':
            return self._get_data_first_success(request, sources_to_try, cache_key)
        elif merge_strategy == 'merge':
            return self._get_data_merge(request, sources_to_try, cache_key)
        elif merge_strategy == 'validate':
            return self._get_data_validate(request, sources_to_try, cache_key)
        else:
            logger.error(f"不支持的合并策略: {merge_strategy}")
            raise ValueError(f"不支持的合并策略: {merge_strategy}")
    
    def _get_data_first_success(self, request: DataRequest, sources: List[str], cache_key: str) -> DataResponse:
        """第一个成功策略：使用第一个成功返回数据的数据源（带智能选择）"""
        attempted_sources = []

        for source_name in sources:
            # 检查是否在冷却期或频率限制
            if self._is_source_available(source_name):
                attempted_sources.append(source_name)

                try:
                    source = self.data_sources[source_name]
                    logger.info(f"尝试从数据源 {source_name} 获取数据 (评分: {self._calculate_source_score(source_name):.2f})")

                    start_time = time.time()
                    response = source.fetch_data(request)
                    end_time = time.time()

                    # 记录请求结果
                    success = response.success and not response.data.empty
                    self._record_request(source_name, success)

                    if success:
                        logger.info(f"从数据源 {source_name} 成功获取数据 (耗时: {end_time - start_time:.2f}s)")
                        self._save_to_cache(cache_key, response)
                        return response
                    else:
                        logger.warning(f"数据源 {source_name} 返回空数据或失败")

                except RateLimitError as e:
                    logger.warning(f"数据源 {source_name} 请求频率超限: {e}")
                    self._record_request(source_name, False)
                    # 设置临时冷却
                    self._source_cooldown[source_name] = datetime.now() + timedelta(seconds=60)
                    continue
                except Exception as e:
                    logger.error(f"从数据源 {source_name} 获取数据失败: {e}")
                    self._record_request(source_name, False)
                    continue
            else:
                logger.debug(f"跳过数据源 {source_name} (不可用)")

        logger.error(f"所有数据源都获取失败，尝试过的数据源: {attempted_sources}")
        return DataResponse(
            data=pd.DataFrame(),
            source="failed",
            data_type=request.data_type,
            timestamp=datetime.now(),
            success=False,
            error_message=f"所有数据源都获取失败，尝试过: {', '.join(attempted_sources)}"
        )

    def _is_source_available(self, source_name: str) -> bool:
        """检查数据源是否可用（不在冷却期且未达到频率限制）"""
        # 检查冷却期
        if source_name in self._source_cooldown:
            if datetime.now() < self._source_cooldown[source_name]:
                return False
            else:
                # 冷却期结束，移除记录
                del self._source_cooldown[source_name]

        # 检查频率限制
        if self._is_rate_limited(source_name):
            return False

        return True
    
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
