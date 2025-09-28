"""
增强版数据源管理器测试用例
"""
import pytest
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch

from src.data_sources.enhanced_data_source_manager import (
    EnhancedDataSourceManager,
    DataSourceConfig,
    DataSourcePriority,
    DataSourceMetrics
)
from src.data_sources.base_data_source import BaseDataSource, DataRequest
from src.utils.exceptions import VesperaException


class MockDataSource(BaseDataSource):
    """模拟数据源"""
    
    def __init__(self, name: str, should_fail: bool = False, delay: float = 0.1):
        self.name = name
        self.should_fail = should_fail
        self.delay = delay
        self.call_count = 0
    
    async def fetch_data(self, request: DataRequest) -> pd.DataFrame:
        """模拟获取数据"""
        self.call_count += 1
        
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        
        if self.should_fail:
            raise Exception(f"数据源 {self.name} 模拟失败")
        
        # 返回模拟数据
        return pd.DataFrame({
            'ts_code': ['000001.SZ'] * 5,
            'trade_date': pd.date_range('2024-01-01', periods=5),
            'open': [10.0, 10.1, 10.2, 10.3, 10.4],
            'high': [10.5, 10.6, 10.7, 10.8, 10.9],
            'low': [9.5, 9.6, 9.7, 9.8, 9.9],
            'close': [10.2, 10.3, 10.4, 10.5, 10.6],
            'volume': [1000, 1100, 1200, 1300, 1400]
        })


@pytest.fixture
def manager():
    """创建数据源管理器实例"""
    return EnhancedDataSourceManager()


@pytest.fixture
def sample_request():
    """创建示例请求"""
    request = Mock(spec=DataRequest)
    request.symbol = "000001.SZ"
    request.start_date = "2024-01-01"
    request.end_date = "2024-01-05"
    request.data_type = "daily"
    return request


@pytest.fixture
def quality_checker():
    """创建数据质量检查器"""
    def checker(data: pd.DataFrame) -> bool:
        # 简单的质量检查：数据不为空且有必要的列
        if data is None or data.empty:
            return False
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        return all(col in data.columns for col in required_columns)
    
    return checker


class TestEnhancedDataSourceManager:
    """增强版数据源管理器测试"""
    
    def test_register_data_source(self, manager):
        """测试注册数据源"""
        source = MockDataSource("test_source")
        config = DataSourceConfig(
            name="test_source",
            priority=DataSourcePriority.PRIMARY
        )
        
        manager.register_data_source(source, config)
        
        assert "test_source" in manager.data_sources
        assert "test_source" in manager.configs
        assert "test_source" in manager.metrics
        assert manager.configs["test_source"].priority == DataSourcePriority.PRIMARY
    
    def test_add_quality_checker(self, manager, quality_checker):
        """测试添加质量检查器"""
        manager.add_quality_checker(quality_checker)
        assert len(manager.quality_checkers) == 1
    
    @pytest.mark.asyncio
    async def test_get_data_success(self, manager, sample_request, quality_checker):
        """测试成功获取数据"""
        # 注册数据源和质量检查器
        source = MockDataSource("primary_source")
        config = DataSourceConfig(
            name="primary_source",
            priority=DataSourcePriority.PRIMARY
        )
        manager.register_data_source(source, config)
        manager.add_quality_checker(quality_checker)
        
        # 获取数据
        data = await manager.get_data(sample_request)
        
        assert not data.empty
        assert len(data) == 5
        assert 'ts_code' in data.columns
        assert source.call_count == 1
    
    @pytest.mark.asyncio
    async def test_get_data_with_fallback(self, manager, sample_request, quality_checker):
        """测试故障转移"""
        # 注册主数据源（会失败）
        primary_source = MockDataSource("primary_source", should_fail=True)
        primary_config = DataSourceConfig(
            name="primary_source",
            priority=DataSourcePriority.PRIMARY
        )
        manager.register_data_source(primary_source, primary_config)
        
        # 注册备用数据源（会成功）
        backup_source = MockDataSource("backup_source", should_fail=False)
        backup_config = DataSourceConfig(
            name="backup_source",
            priority=DataSourcePriority.BACKUP
        )
        manager.register_data_source(backup_source, backup_config)
        
        manager.add_quality_checker(quality_checker)
        
        # 获取数据
        data = await manager.get_data(sample_request)
        
        assert not data.empty
        assert primary_source.call_count == 1  # 主数据源被调用
        assert backup_source.call_count == 1   # 备用数据源被调用
    
    @pytest.mark.asyncio
    async def test_get_data_all_sources_fail(self, manager, sample_request, quality_checker):
        """测试所有数据源都失败"""
        # 注册两个都会失败的数据源
        source1 = MockDataSource("source1", should_fail=True)
        config1 = DataSourceConfig(name="source1", priority=DataSourcePriority.PRIMARY)
        manager.register_data_source(source1, config1)
        
        source2 = MockDataSource("source2", should_fail=True)
        config2 = DataSourceConfig(name="source2", priority=DataSourcePriority.BACKUP)
        manager.register_data_source(source2, config2)
        
        manager.add_quality_checker(quality_checker)
        
        # 应该抛出异常
        with pytest.raises(Exception, match="所有数据源均不可用"):
            await manager.get_data(sample_request)
    
    @pytest.mark.asyncio
    async def test_data_quality_check_failure(self, manager, sample_request):
        """测试数据质量检查失败"""
        # 注册数据源
        source = MockDataSource("test_source")
        config = DataSourceConfig(name="test_source", priority=DataSourcePriority.PRIMARY)
        manager.register_data_source(source, config)
        
        # 添加总是失败的质量检查器
        def failing_checker(data: pd.DataFrame) -> bool:
            return False
        
        manager.add_quality_checker(failing_checker)
        
        # 应该抛出异常
        with pytest.raises(Exception, match="所有数据源均不可用"):
            await manager.get_data(sample_request)
    
    @pytest.mark.asyncio
    async def test_cache_functionality(self, manager, sample_request, quality_checker):
        """测试缓存功能"""
        source = MockDataSource("test_source")
        config = DataSourceConfig(name="test_source", priority=DataSourcePriority.PRIMARY)
        manager.register_data_source(source, config)
        manager.add_quality_checker(quality_checker)
        
        # 第一次获取数据
        data1 = await manager.get_data(sample_request)
        assert source.call_count == 1
        
        # 第二次获取数据（应该从缓存获取）
        data2 = await manager.get_data(sample_request)
        assert source.call_count == 1  # 调用次数不应该增加
        
        # 验证数据相同
        pd.testing.assert_frame_equal(data1, data2)
    
    def test_rate_limit_check(self, manager):
        """测试速率限制检查"""
        config = DataSourceConfig(
            name="test_source",
            priority=DataSourcePriority.PRIMARY,
            rate_limit=2  # 每分钟2次请求
        )
        manager.configs["test_source"] = config
        
        # 模拟请求历史
        now = datetime.now()
        manager.request_history["test_source"].extend([
            now - timedelta(seconds=30),  # 30秒前
            now - timedelta(seconds=10),  # 10秒前
        ])
        
        # 应该达到速率限制
        assert not manager._check_rate_limit("test_source")
        
        # 清空历史，应该可以请求
        manager.request_history["test_source"].clear()
        assert manager._check_rate_limit("test_source")
    
    def test_circuit_breaker_functionality(self, manager):
        """测试熔断器功能"""
        metrics = DataSourceMetrics()
        manager.metrics["test_source"] = metrics
        
        # 模拟连续失败
        for _ in range(6):  # 超过阈值(5)
            metrics.update_failure()
        
        # 应该触发熔断器
        assert manager._is_circuit_breaker_open("test_source")
        
        # 重置指标
        manager.reset_metrics("test_source")
        assert not manager._is_circuit_breaker_open("test_source")
    
    def test_get_available_sources(self, manager):
        """测试获取可用数据源"""
        # 注册正常数据源
        config1 = DataSourceConfig(name="source1", priority=DataSourcePriority.PRIMARY)
        manager.configs["source1"] = config1
        manager.metrics["source1"] = DataSourceMetrics()
        
        # 注册禁用的数据源
        config2 = DataSourceConfig(name="source2", priority=DataSourcePriority.BACKUP, enabled=False)
        manager.configs["source2"] = config2
        manager.metrics["source2"] = DataSourceMetrics()
        
        # 注册健康分数低的数据源
        config3 = DataSourceConfig(name="source3", priority=DataSourcePriority.BACKUP)
        manager.configs["source3"] = config3
        metrics3 = DataSourceMetrics()
        metrics3.health_score = 0.2  # 低于阈值
        manager.metrics["source3"] = metrics3
        
        available = manager._get_available_sources()
        assert "source1" in available
        assert "source2" not in available  # 被禁用
        assert "source3" not in available  # 健康分数太低
    
    def test_health_report(self, manager):
        """测试健康报告"""
        # 注册数据源
        config = DataSourceConfig(name="test_source", priority=DataSourcePriority.PRIMARY)
        manager.configs["test_source"] = config
        
        metrics = DataSourceMetrics()
        metrics.total_requests = 10
        metrics.successful_requests = 8
        metrics.avg_response_time = 1.5
        manager.metrics["test_source"] = metrics
        
        report = manager.get_health_report()
        
        assert "timestamp" in report
        assert "sources" in report
        assert "test_source" in report["sources"]
        
        source_info = report["sources"]["test_source"]
        assert source_info["success_rate"] == 0.8
        assert source_info["total_requests"] == 10
        assert source_info["avg_response_time"] == 1.5
    
    def test_disable_enable_source(self, manager):
        """测试禁用和启用数据源"""
        config = DataSourceConfig(name="test_source", priority=DataSourcePriority.PRIMARY)
        manager.configs["test_source"] = config
        
        # 禁用数据源
        manager.disable_source("test_source")
        assert not manager.configs["test_source"].enabled
        
        # 启用数据源
        manager.enable_source("test_source")
        assert manager.configs["test_source"].enabled


class TestDataSourceMetrics:
    """数据源指标测试"""
    
    def test_initial_state(self):
        """测试初始状态"""
        metrics = DataSourceMetrics()
        assert metrics.total_requests == 0
        assert metrics.successful_requests == 0
        assert metrics.failed_requests == 0
        assert metrics.success_rate == 1.0
        assert metrics.health_score == 1.0
    
    def test_update_success(self):
        """测试更新成功指标"""
        metrics = DataSourceMetrics()
        metrics.update_success(1.5)
        
        assert metrics.total_requests == 1
        assert metrics.successful_requests == 1
        assert metrics.consecutive_failures == 0
        assert metrics.avg_response_time == 1.5
        assert metrics.last_success_time is not None
    
    def test_update_failure(self):
        """测试更新失败指标"""
        metrics = DataSourceMetrics()
        metrics.update_failure()
        
        assert metrics.total_requests == 1
        assert metrics.failed_requests == 1
        assert metrics.consecutive_failures == 1
        assert metrics.last_failure_time is not None
    
    def test_success_rate_calculation(self):
        """测试成功率计算"""
        metrics = DataSourceMetrics()
        
        # 3次成功，2次失败
        for _ in range(3):
            metrics.update_success(1.0)
        for _ in range(2):
            metrics.update_failure()
        
        assert metrics.success_rate == 0.6
        assert metrics.total_requests == 5
    
    def test_health_score_degradation(self):
        """测试健康分数降级"""
        metrics = DataSourceMetrics()
        
        # 连续失败会降低健康分数
        for _ in range(5):
            metrics.update_failure()
        
        assert metrics.health_score < 1.0
        assert metrics.consecutive_failures == 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])