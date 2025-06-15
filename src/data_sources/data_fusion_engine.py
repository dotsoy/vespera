"""
数据融合引擎
负责多数据源数据的融合、验证和质量控制
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from loguru import logger
from dataclasses import dataclass
from enum import Enum

from .base_data_source import DataResponse, DataType


class FusionStrategy(Enum):
    """数据融合策略"""
    FIRST_SUCCESS = "first_success"      # 第一个成功
    WEIGHTED_AVERAGE = "weighted_average"  # 加权平均
    MEDIAN = "median"                    # 中位数
    CONSENSUS = "consensus"              # 一致性投票
    PRIORITY_BASED = "priority_based"    # 基于优先级
    QUALITY_BASED = "quality_based"      # 基于质量评分


class ValidationLevel(Enum):
    """验证级别"""
    NONE = "none"           # 无验证
    BASIC = "basic"         # 基础验证
    STRICT = "strict"       # 严格验证
    COMPREHENSIVE = "comprehensive"  # 全面验证


@dataclass
class DataQualityMetrics:
    """数据质量指标"""
    completeness: float      # 完整性 (0-1)
    consistency: float       # 一致性 (0-1)
    accuracy: float         # 准确性 (0-1)
    timeliness: float       # 及时性 (0-1)
    overall_score: float    # 总体评分 (0-1)
    issues: List[str]       # 质量问题列表


@dataclass
class FusionResult:
    """融合结果"""
    data: pd.DataFrame
    strategy: FusionStrategy
    source_weights: Dict[str, float]
    quality_metrics: DataQualityMetrics
    validation_passed: bool
    metadata: Dict[str, Any]


class DataFusionEngine:
    """数据融合引擎"""
    
    def __init__(self):
        self.fusion_strategies = {
            FusionStrategy.FIRST_SUCCESS: self._first_success_fusion,
            FusionStrategy.WEIGHTED_AVERAGE: self._weighted_average_fusion,
            FusionStrategy.MEDIAN: self._median_fusion,
            FusionStrategy.CONSENSUS: self._consensus_fusion,
            FusionStrategy.PRIORITY_BASED: self._priority_based_fusion,
            FusionStrategy.QUALITY_BASED: self._quality_based_fusion
        }
        
        self.validation_functions = {
            ValidationLevel.BASIC: self._basic_validation,
            ValidationLevel.STRICT: self._strict_validation,
            ValidationLevel.COMPREHENSIVE: self._comprehensive_validation
        }
    
    def fuse_data(self, responses: List[DataResponse], 
                  strategy: FusionStrategy = FusionStrategy.QUALITY_BASED,
                  validation_level: ValidationLevel = ValidationLevel.BASIC,
                  source_weights: Optional[Dict[str, float]] = None) -> FusionResult:
        """
        融合多个数据源的数据
        
        Args:
            responses: 数据响应列表
            strategy: 融合策略
            validation_level: 验证级别
            source_weights: 数据源权重
            
        Returns:
            FusionResult: 融合结果
        """
        if not responses:
            return self._create_empty_result(strategy)
        
        # 过滤成功的响应
        valid_responses = [r for r in responses if r.success and not r.data.empty]
        
        if not valid_responses:
            return self._create_empty_result(strategy)
        
        if len(valid_responses) == 1:
            # 只有一个有效数据源
            return self._create_single_source_result(valid_responses[0], strategy)
        
        # 数据质量评估
        quality_scores = self._assess_data_quality(valid_responses)
        
        # 执行融合策略
        fusion_func = self.fusion_strategies.get(strategy, self._quality_based_fusion)
        fused_data, weights = fusion_func(valid_responses, source_weights, quality_scores)
        
        # 数据验证
        validation_func = self.validation_functions.get(validation_level, self._basic_validation)
        validation_passed, quality_metrics = validation_func(fused_data, valid_responses)
        
        return FusionResult(
            data=fused_data,
            strategy=strategy,
            source_weights=weights,
            quality_metrics=quality_metrics,
            validation_passed=validation_passed,
            metadata={
                'source_count': len(valid_responses),
                'fusion_timestamp': datetime.now(),
                'data_type': valid_responses[0].data_type
            }
        )
    
    def _assess_data_quality(self, responses: List[DataResponse]) -> Dict[str, DataQualityMetrics]:
        """评估数据质量"""
        quality_scores = {}
        
        for response in responses:
            data = response.data
            source = response.source
            
            # 计算完整性
            completeness = self._calculate_completeness(data)
            
            # 计算一致性（与其他数据源比较）
            consistency = self._calculate_consistency(data, responses, response)
            
            # 计算准确性（基于数据合理性）
            accuracy = self._calculate_accuracy(data)
            
            # 计算及时性
            timeliness = self._calculate_timeliness(response)
            
            # 总体评分
            overall_score = (completeness * 0.3 + consistency * 0.3 + 
                           accuracy * 0.3 + timeliness * 0.1)
            
            quality_scores[source] = DataQualityMetrics(
                completeness=completeness,
                consistency=consistency,
                accuracy=accuracy,
                timeliness=timeliness,
                overall_score=overall_score,
                issues=[]
            )
        
        return quality_scores
    
    def _calculate_completeness(self, data: pd.DataFrame) -> float:
        """计算数据完整性"""
        if data.empty:
            return 0.0
        
        total_cells = data.size
        missing_cells = data.isnull().sum().sum()
        
        return max(0.0, 1.0 - (missing_cells / total_cells))
    
    def _calculate_consistency(self, data: pd.DataFrame, all_responses: List[DataResponse], 
                             current_response: DataResponse) -> float:
        """计算数据一致性"""
        if len(all_responses) <= 1:
            return 1.0
        
        consistency_scores = []
        
        for other_response in all_responses:
            if other_response.source == current_response.source:
                continue
            
            other_data = other_response.data
            
            # 比较关键字段的一致性
            if 'close_price' in data.columns and 'close_price' in other_data.columns:
                # 合并数据进行比较
                merged = pd.merge(data, other_data, on=['ts_code', 'trade_date'], 
                                suffixes=('_1', '_2'), how='inner')
                
                if not merged.empty:
                    price_diff = abs(merged['close_price_1'] - merged['close_price_2'])
                    avg_price = (merged['close_price_1'] + merged['close_price_2']) / 2
                    relative_diff = price_diff / avg_price
                    
                    # 一致性评分：差异小于1%认为一致
                    consistency = (relative_diff < 0.01).mean()
                    consistency_scores.append(consistency)
        
        return np.mean(consistency_scores) if consistency_scores else 1.0
    
    def _calculate_accuracy(self, data: pd.DataFrame) -> float:
        """计算数据准确性"""
        if data.empty:
            return 0.0
        
        accuracy_score = 1.0
        
        # 检查价格数据的合理性
        if 'close_price' in data.columns:
            prices = data['close_price']
            
            # 检查是否有负价格
            if (prices < 0).any():
                accuracy_score -= 0.3
            
            # 检查是否有异常大的价格变动
            if 'pct_chg' in data.columns:
                pct_changes = data['pct_chg'].abs()
                # 单日涨跌幅超过50%认为异常
                if (pct_changes > 50).any():
                    accuracy_score -= 0.2
        
        # 检查成交量的合理性
        if 'vol' in data.columns:
            volumes = data['vol']
            
            # 检查是否有负成交量
            if (volumes < 0).any():
                accuracy_score -= 0.2
        
        return max(0.0, accuracy_score)
    
    def _calculate_timeliness(self, response: DataResponse) -> float:
        """计算数据及时性"""
        # 基于数据获取时间计算及时性
        time_diff = (datetime.now() - response.timestamp).total_seconds()
        
        # 5分钟内的数据认为是及时的
        if time_diff <= 300:
            return 1.0
        # 1小时内的数据认为是较及时的
        elif time_diff <= 3600:
            return 0.8
        # 1天内的数据认为是一般及时的
        elif time_diff <= 86400:
            return 0.6
        else:
            return 0.3
    
    def _first_success_fusion(self, responses: List[DataResponse], 
                            source_weights: Optional[Dict[str, float]], 
                            quality_scores: Dict[str, DataQualityMetrics]) -> Tuple[pd.DataFrame, Dict[str, float]]:
        """第一个成功策略"""
        first_response = responses[0]
        weights = {first_response.source: 1.0}
        return first_response.data, weights
    
    def _weighted_average_fusion(self, responses: List[DataResponse], 
                               source_weights: Optional[Dict[str, float]], 
                               quality_scores: Dict[str, DataQualityMetrics]) -> Tuple[pd.DataFrame, Dict[str, float]]:
        """加权平均融合"""
        if not source_weights:
            # 使用质量评分作为权重
            source_weights = {r.source: quality_scores[r.source].overall_score for r in responses}
        
        # 标准化权重
        total_weight = sum(source_weights.values())
        normalized_weights = {k: v/total_weight for k, v in source_weights.items()}
        
        # 对数值列进行加权平均
        base_data = responses[0].data.copy()
        numeric_columns = base_data.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            if col in ['close_price', 'open_price', 'high_price', 'low_price', 'vol']:
                weighted_values = pd.Series(0.0, index=base_data.index)
                
                for response in responses:
                    weight = normalized_weights.get(response.source, 0)
                    if col in response.data.columns:
                        # 需要对齐数据
                        aligned_data = pd.merge(base_data[['ts_code', 'trade_date']], 
                                              response.data[['ts_code', 'trade_date', col]], 
                                              on=['ts_code', 'trade_date'], how='left')
                        weighted_values += aligned_data[col].fillna(0) * weight
                
                base_data[col] = weighted_values
        
        return base_data, normalized_weights
    
    def _median_fusion(self, responses: List[DataResponse], 
                      source_weights: Optional[Dict[str, float]], 
                      quality_scores: Dict[str, DataQualityMetrics]) -> Tuple[pd.DataFrame, Dict[str, float]]:
        """中位数融合"""
        base_data = responses[0].data.copy()
        numeric_columns = base_data.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            if col in ['close_price', 'open_price', 'high_price', 'low_price', 'vol']:
                values_list = []
                
                for response in responses:
                    if col in response.data.columns:
                        aligned_data = pd.merge(base_data[['ts_code', 'trade_date']], 
                                              response.data[['ts_code', 'trade_date', col]], 
                                              on=['ts_code', 'trade_date'], how='left')
                        values_list.append(aligned_data[col])
                
                if values_list:
                    # 计算中位数
                    values_df = pd.concat(values_list, axis=1)
                    base_data[col] = values_df.median(axis=1)
        
        weights = {r.source: 1.0/len(responses) for r in responses}
        return base_data, weights
    
    def _consensus_fusion(self, responses: List[DataResponse], 
                         source_weights: Optional[Dict[str, float]], 
                         quality_scores: Dict[str, DataQualityMetrics]) -> Tuple[pd.DataFrame, Dict[str, float]]:
        """一致性投票融合"""
        # 简化实现：使用质量评分最高的数据源
        best_source = max(quality_scores.keys(), key=lambda x: quality_scores[x].overall_score)
        best_response = next(r for r in responses if r.source == best_source)
        
        weights = {best_source: 1.0}
        return best_response.data, weights
    
    def _priority_based_fusion(self, responses: List[DataResponse], 
                             source_weights: Optional[Dict[str, float]], 
                             quality_scores: Dict[str, DataQualityMetrics]) -> Tuple[pd.DataFrame, Dict[str, float]]:
        """基于优先级的融合"""
        # 使用第一个响应（假设已按优先级排序）
        return self._first_success_fusion(responses, source_weights, quality_scores)
    
    def _quality_based_fusion(self, responses: List[DataResponse], 
                            source_weights: Optional[Dict[str, float]], 
                            quality_scores: Dict[str, DataQualityMetrics]) -> Tuple[pd.DataFrame, Dict[str, float]]:
        """基于质量的融合"""
        # 使用质量评分作为权重进行加权平均
        quality_weights = {r.source: quality_scores[r.source].overall_score for r in responses}
        return self._weighted_average_fusion(responses, quality_weights, quality_scores)
    
    def _basic_validation(self, data: pd.DataFrame, responses: List[DataResponse]) -> Tuple[bool, DataQualityMetrics]:
        """基础验证"""
        issues = []
        
        # 检查数据是否为空
        if data.empty:
            issues.append("融合后数据为空")
            return False, DataQualityMetrics(0, 0, 0, 0, 0, issues)
        
        # 检查必要列是否存在
        required_columns = ['ts_code', 'trade_date']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            issues.append(f"缺少必要列: {missing_columns}")
        
        validation_passed = len(issues) == 0
        
        # 计算基础质量指标
        completeness = self._calculate_completeness(data)
        
        return validation_passed, DataQualityMetrics(
            completeness=completeness,
            consistency=1.0,  # 基础验证不检查一致性
            accuracy=1.0,     # 基础验证不检查准确性
            timeliness=1.0,   # 基础验证不检查及时性
            overall_score=completeness,
            issues=issues
        )
    
    def _strict_validation(self, data: pd.DataFrame, responses: List[DataResponse]) -> Tuple[bool, DataQualityMetrics]:
        """严格验证"""
        validation_passed, quality_metrics = self._basic_validation(data, responses)
        
        # 添加更严格的检查
        if 'close_price' in data.columns:
            # 检查价格数据的合理性
            if (data['close_price'] <= 0).any():
                quality_metrics.issues.append("存在非正价格数据")
                validation_passed = False
        
        return validation_passed, quality_metrics
    
    def _comprehensive_validation(self, data: pd.DataFrame, responses: List[DataResponse]) -> Tuple[bool, DataQualityMetrics]:
        """全面验证"""
        validation_passed, quality_metrics = self._strict_validation(data, responses)
        
        # 添加全面的数据质量检查
        # 这里可以添加更多复杂的验证逻辑
        
        return validation_passed, quality_metrics
    
    def _create_empty_result(self, strategy: FusionStrategy) -> FusionResult:
        """创建空结果"""
        return FusionResult(
            data=pd.DataFrame(),
            strategy=strategy,
            source_weights={},
            quality_metrics=DataQualityMetrics(0, 0, 0, 0, 0, ["无有效数据源"]),
            validation_passed=False,
            metadata={}
        )
    
    def _create_single_source_result(self, response: DataResponse, strategy: FusionStrategy) -> FusionResult:
        """创建单数据源结果"""
        quality_metrics = DataQualityMetrics(
            completeness=self._calculate_completeness(response.data),
            consistency=1.0,
            accuracy=self._calculate_accuracy(response.data),
            timeliness=self._calculate_timeliness(response),
            overall_score=1.0,
            issues=[]
        )
        
        return FusionResult(
            data=response.data,
            strategy=strategy,
            source_weights={response.source: 1.0},
            quality_metrics=quality_metrics,
            validation_passed=True,
            metadata={'source_count': 1}
        )
