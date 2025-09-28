"""
错误处理和恢复机制测试用例
"""
import pytest
import asyncio
import time
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from src.utils.exceptions import (
    VesperaException,
    DataSourceException,
    StrategyException,
    ErrorSeverity,
    ErrorCategory,
    ErrorCollector,
    handle_exceptions,
    handle_async_exceptions
)
from src.utils.error_recovery import (
    RetryExecutor,
    RetryConfig,
    RetryStrategy,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
    FallbackHandler,
    ErrorRecoveryManager,
    with_retry,
    with_circuit_breaker,
    with_fallback
)


class TestVesperaException:
    """Vespera异常测试"""
    
    def test_basic_exception_creation(self):
        """测试基本异常创建"""
        exc = VesperaException(
            message="测试异常",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.DATA_SOURCE,
            component="test_component",
            operation="test_operation"
        )
        
        assert exc.message == "测试异常"
        assert exc.context.severity == ErrorSeverity.HIGH
        assert exc.context.category == ErrorCategory.DATA_SOURCE
        assert exc.context.component == "test_component"
        assert exc.context.operation == "test_operation"
        assert exc.context.timestamp is not None
    
    def test_exception_with_metadata(self):
        """测试带元数据的异常"""
        metadata = {"key1": "value1", "key2": 123}
        exc = VesperaException(
            message="测试异常",
            metadata=metadata
        )
        
        assert exc.context.metadata == metadata
    
    def test_exception_with_cause(self):
        """测试带原因的异常"""
        original_error = ValueError("原始错误")
        exc = VesperaException(
            message="包装异常",
            cause=original_error
        )
        
        assert exc.cause == original_error
    
    def test_exception_to_dict(self):
        """测试异常转换为字典"""
        exc = VesperaException(
            message="测试异常",
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.STRATEGY
        )
        
        exc_dict = exc.to_dict()
        
        assert exc_dict["error_type"] == "VesperaException"
        assert exc_dict["message"] == "测试异常"
        assert exc_dict["context"]["severity"] == "MEDIUM"
        assert exc_dict["context"]["category"] == "STRATEGY"


class TestSpecificExceptions:
    """特定异常类型测试"""
    
    def test_data_source_exception(self):
        """测试数据源异常"""
        exc = DataSourceException(
            message="数据源错误",
            source_name="test_source"
        )
        
        assert exc.context.category == ErrorCategory.DATA_SOURCE
        assert exc.context.metadata["source_name"] == "test_source"
    
    def test_strategy_exception(self):
        """测试策略异常"""
        exc = StrategyException(
            message="策略错误",
            strategy_name="test_strategy"
        )
        
        assert exc.context.category == ErrorCategory.STRATEGY
        assert exc.context.metadata["strategy_name"] == "test_strategy"


class TestErrorCollector:
    """错误收集器测试"""
    
    def test_add_error(self):
        """测试添加错误"""
        collector = ErrorCollector(max_errors=10)
        
        exc = VesperaException("测试错误")
        collector.add_error(exc)
        
        assert len(collector.errors) == 1
        assert collector.errors[0] == exc
    
    def test_max_errors_limit(self):
        """测试最大错误数限制"""
        collector = ErrorCollector(max_errors=3)
        
        # 添加5个错误
        for i in range(5):
            exc = VesperaException(f"错误 {i}")
            collector.add_error(exc)
        
        # 应该只保留最后3个
        assert len(collector.errors) == 3
        assert collector.errors[0].message == "错误 2"
        assert collector.errors[-1].message == "错误 4"
    
    def test_get_errors_by_severity(self):
        """测试按严重程度获取错误"""
        collector = ErrorCollector()
        
        high_exc = VesperaException("高严重性", severity=ErrorSeverity.HIGH)
        low_exc = VesperaException("低严重性", severity=ErrorSeverity.LOW)
        
        collector.add_error(high_exc)
        collector.add_error(low_exc)
        
        high_errors = collector.get_errors_by_severity(ErrorSeverity.HIGH)
        assert len(high_errors) == 1
        assert high_errors[0] == high_exc
    
    def test_get_errors_by_category(self):
        """测试按分类获取错误"""
        collector = ErrorCollector()
        
        data_exc = VesperaException("数据错误", category=ErrorCategory.DATA_SOURCE)
        strategy_exc = VesperaException("策略错误", category=ErrorCategory.STRATEGY)
        
        collector.add_error(data_exc)
        collector.add_error(strategy_exc)
        
        data_errors = collector.get_errors_by_category(ErrorCategory.DATA_SOURCE)
        assert len(data_errors) == 1
        assert data_errors[0] == data_exc
    
    def test_get_recent_errors(self):
        """测试获取最近错误"""
        collector = ErrorCollector()
        
        # 创建一个旧错误（手动设置时间戳）
        old_exc = VesperaException("旧错误")
        old_exc.context.timestamp = datetime.now() - timedelta(hours=2)
        
        # 创建一个新错误
        new_exc = VesperaException("新错误")
        
        collector.add_error(old_exc)
        collector.add_error(new_exc)
        
        recent_errors = collector.get_recent_errors(minutes=60)
        assert len(recent_errors) == 1
        assert recent_errors[0] == new_exc
    
    def test_error_summary(self):
        """测试错误摘要"""
        collector = ErrorCollector()
        
        collector.add_error(VesperaException("错误1", severity=ErrorSeverity.HIGH))
        collector.add_error(VesperaException("错误2", severity=ErrorSeverity.LOW))
        collector.add_error(VesperaException("错误3", category=ErrorCategory.DATA_SOURCE))
        
        summary = collector.get_error_summary()
        
        assert summary["total_errors"] == 3
        assert "HIGH" in summary["severity_breakdown"]
        assert "LOW" in summary["severity_breakdown"]
        assert "DATA_SOURCE" in summary["category_breakdown"]
        assert summary["latest_error"] is not None


class TestExceptionDecorators:
    """异常装饰器测试"""
    
    def test_handle_exceptions_decorator(self):
        """测试异常处理装饰器"""
        @handle_exceptions(default_return="默认值")
        def failing_function():
            raise ValueError("函数失败")
        
        result = failing_function()
        assert result == "默认值"
    
    def test_handle_exceptions_reraise(self):
        """测试异常重新抛出"""
        @handle_exceptions(reraise=True)
        def failing_function():
            raise ValueError("函数失败")
        
        with pytest.raises(VesperaException):
            failing_function()
    
    @pytest.mark.asyncio
    async def test_handle_async_exceptions_decorator(self):
        """测试异步异常处理装饰器"""
        @handle_async_exceptions(default_return="异步默认值")
        async def failing_async_function():
            raise ValueError("异步函数失败")
        
        result = await failing_async_function()
        assert result == "异步默认值"


class TestRetryExecutor:
    """重试执行器测试"""
    
    def test_successful_execution(self):
        """测试成功执行"""
        executor = RetryExecutor()
        
        def successful_function():
            return "成功"
        
        result = executor.execute(successful_function)
        assert result == "成功"
    
    def test_retry_on_failure(self):
        """测试失败重试"""
        config = RetryConfig(max_attempts=3, base_delay=0.01)
        executor = RetryExecutor(config)
        
        call_count = 0
        
        def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError(f"失败 {call_count}")
            return "最终成功"
        
        result = executor.execute(failing_function)
        assert result == "最终成功"
        assert call_count == 3
    
    def test_max_attempts_exceeded(self):
        """测试超过最大重试次数"""
        config = RetryConfig(max_attempts=2, base_delay=0.01)
        executor = RetryExecutor(config)
        
        def always_failing_function():
            raise ValueError("总是失败")
        
        with pytest.raises(ValueError, match="总是失败"):
            executor.execute(always_failing_function)
    
    @pytest.mark.asyncio
    async def test_async_retry(self):
        """测试异步重试"""
        config = RetryConfig(max_attempts=3, base_delay=0.01)
        executor = RetryExecutor(config)
        
        call_count = 0
        
        async def failing_async_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError(f"异步失败 {call_count}")
            return "异步成功"
        
        result = await executor.execute_async(failing_async_function)
        assert result == "异步成功"
        assert call_count == 3
    
    def test_retry_strategy_exponential_backoff(self):
        """测试指数退避策略"""
        config = RetryConfig(
            max_attempts=3,
            base_delay=0.1,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            backoff_multiplier=2.0,
            jitter=False
        )
        executor = RetryExecutor(config)
        
        # 测试延迟计算
        delay1 = executor._calculate_delay(0)
        delay2 = executor._calculate_delay(1)
        delay3 = executor._calculate_delay(2)
        
        assert delay1 == 0.1
        assert delay2 == 0.2
        assert delay3 == 0.4


class TestCircuitBreaker:
    """熔断器测试"""
    
    def test_initial_state(self):
        """测试初始状态"""
        config = CircuitBreakerConfig()
        cb = CircuitBreaker("test", config)
        
        assert cb.state == CircuitBreakerState.CLOSED
        assert cb.can_execute() == True
    
    def test_failure_threshold(self):
        """测试失败阈值"""
        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker("test", config)
        
        # 记录失败，但未达到阈值
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitBreakerState.CLOSED
        assert cb.can_execute() == True
        
        # 达到阈值，应该开启熔断器
        cb.record_failure()
        assert cb.state == CircuitBreakerState.OPEN
        assert cb.can_execute() == False
    
    def test_half_open_transition(self):
        """测试半开状态转换"""
        config = CircuitBreakerConfig(failure_threshold=2, timeout=0.1)
        cb = CircuitBreaker("test", config)
        
        # 触发熔断
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitBreakerState.OPEN
        
        # 等待超时
        time.sleep(0.2)
        
        # 应该可以转为半开状态
        assert cb.can_execute() == True
        assert cb.state == CircuitBreakerState.HALF_OPEN
    
    def test_recovery_from_half_open(self):
        """测试从半开状态恢复"""
        config = CircuitBreakerConfig(failure_threshold=2, success_threshold=2)
        cb = CircuitBreaker("test", config)
        
        # 触发熔断并转为半开状态
        cb.record_failure()
        cb.record_failure()
        cb.state = CircuitBreakerState.HALF_OPEN
        
        # 记录成功，但未达到成功阈值
        cb.record_success()
        assert cb.state == CircuitBreakerState.HALF_OPEN
        
        # 达到成功阈值，应该恢复正常
        cb.record_success()
        assert cb.state == CircuitBreakerState.CLOSED
    
    def test_failure_in_half_open(self):
        """测试半开状态下的失败"""
        config = CircuitBreakerConfig(failure_threshold=2)
        cb = CircuitBreaker("test", config)
        
        # 设置为半开状态
        cb.state = CircuitBreakerState.HALF_OPEN
        
        # 记录失败，应该重新熔断
        cb.record_failure()
        assert cb.state == CircuitBreakerState.OPEN


class TestFallbackHandler:
    """降级处理器测试"""
    
    def test_register_fallback(self):
        """测试注册降级函数"""
        handler = FallbackHandler()
        
        def fallback_func():
            return "降级结果"
        
        handler.register_fallback("test_operation", fallback_func)
        assert "test_operation" in handler.fallback_functions
        assert len(handler.fallback_functions["test_operation"]) == 1
    
    def test_execute_with_fallback_success(self):
        """测试主函数成功执行"""
        handler = FallbackHandler()
        
        def primary_func():
            return "主函数结果"
        
        def fallback_func():
            return "降级结果"
        
        handler.register_fallback("test_operation", fallback_func)
        
        result = handler.execute_with_fallback("test_operation", primary_func)
        assert result == "主函数结果"
    
    def test_execute_with_fallback_failure(self):
        """测试主函数失败，使用降级函数"""
        handler = FallbackHandler()
        
        def primary_func():
            raise ValueError("主函数失败")
        
        def fallback_func():
            return "降级结果"
        
        handler.register_fallback("test_operation", fallback_func)
        
        result = handler.execute_with_fallback("test_operation", primary_func)
        assert result == "降级结果"
    
    def test_multiple_fallbacks_priority(self):
        """测试多个降级函数的优先级"""
        handler = FallbackHandler()
        
        def primary_func():
            raise ValueError("主函数失败")
        
        def fallback1():
            raise ValueError("降级1失败")
        
        def fallback2():
            return "降级2成功"
        
        # 注册降级函数，优先级不同
        handler.register_fallback("test_operation", fallback1, priority=1)
        handler.register_fallback("test_operation", fallback2, priority=2)
        
        result = handler.execute_with_fallback("test_operation", primary_func)
        assert result == "降级2成功"


class TestErrorRecoveryManager:
    """错误恢复管理器测试"""
    
    def test_create_circuit_breaker(self):
        """测试创建熔断器"""
        manager = ErrorRecoveryManager()
        
        config = CircuitBreakerConfig(failure_threshold=3)
        cb = manager.create_circuit_breaker("test_cb", config)
        
        assert "test_cb" in manager.circuit_breakers
        assert cb.config.failure_threshold == 3
    
    def test_execute_with_circuit_breaker(self):
        """测试使用熔断器执行函数"""
        manager = ErrorRecoveryManager()
        manager.create_circuit_breaker("test_cb")
        
        def test_function():
            return "成功"
        
        result = manager.execute_with_circuit_breaker("test_cb", test_function)
        assert result == "成功"
    
    def test_circuit_breaker_blocks_execution(self):
        """测试熔断器阻止执行"""
        manager = ErrorRecoveryManager()
        cb = manager.create_circuit_breaker("test_cb", CircuitBreakerConfig(failure_threshold=1))
        
        # 触发熔断
        cb.record_failure()
        
        def test_function():
            return "不应该执行"
        
        with pytest.raises(VesperaException, match="熔断器.*处于开启状态"):
            manager.execute_with_circuit_breaker("test_cb", test_function)
    
    def test_get_system_health(self):
        """测试获取系统健康状态"""
        manager = ErrorRecoveryManager()
        manager.create_circuit_breaker("cb1")
        manager.create_circuit_breaker("cb2")
        
        health = manager.get_system_health()
        
        assert "timestamp" in health
        assert "circuit_breakers" in health
        assert "total_circuit_breakers" in health
        assert "open_circuit_breakers" in health
        
        assert health["total_circuit_breakers"] == 2
        assert "cb1" in health["circuit_breakers"]
        assert "cb2" in health["circuit_breakers"]


class TestDecorators:
    """装饰器测试"""
    
    def test_with_retry_decorator(self):
        """测试重试装饰器"""
        call_count = 0
        
        @with_retry(RetryConfig(max_attempts=3, base_delay=0.01))
        def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("失败")
            return "成功"
        
        result = failing_function()
        assert result == "成功"
        assert call_count == 3
    
    def test_with_circuit_breaker_decorator(self):
        """测试熔断器装饰器"""
        @with_circuit_breaker("test_decorator_cb")
        def test_function():
            return "成功"
        
        result = test_function()
        assert result == "成功"
        
        # 验证熔断器已创建
        assert "test_decorator_cb" in global_recovery_manager.circuit_breakers
    
    def test_with_fallback_decorator(self):
        """测试降级装饰器"""
        def fallback_function():
            return "降级结果"
        
        @with_fallback("test_fallback_op", fallback_function)
        def primary_function():
            raise ValueError("主函数失败")
        
        result = primary_function()
        assert result == "降级结果"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])