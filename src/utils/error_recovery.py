"""
错误恢复机制
实现自动重试、降级处理、故障转移等功能
"""
import asyncio
import time
from typing import Callable, Any, Optional, Dict, List, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import random

from .exceptions import VesperaException, ErrorSeverity, ErrorCategory
try:
    from .logger import get_logger
except ImportError:
    import logging
    def get_logger(name):
        return logging.getLogger(name)

logger = get_logger("error_recovery")


class RetryStrategy(Enum):
    """重试策略"""
    FIXED_DELAY = "fixed_delay"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    RANDOM_JITTER = "random_jitter"


@dataclass
class RetryConfig:
    """重试配置"""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    jitter: bool = True
    backoff_multiplier: float = 2.0
    
    # 可重试的异常类型
    retryable_exceptions: List[type] = None
    
    # 不可重试的异常类型
    non_retryable_exceptions: List[type] = None


class CircuitBreakerState(Enum):
    """熔断器状态"""
    CLOSED = "closed"      # 正常状态
    OPEN = "open"          # 熔断状态
    HALF_OPEN = "half_open"  # 半开状态


@dataclass
class CircuitBreakerConfig:
    """熔断器配置"""
    failure_threshold: int = 5      # 失败阈值
    success_threshold: int = 3      # 成功阈值（半开状态）
    timeout: float = 60.0          # 熔断超时时间
    monitor_window: float = 300.0   # 监控窗口时间


class CircuitBreaker:
    """熔断器"""
    
    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.failure_history: List[datetime] = []
    
    def can_execute(self) -> bool:
        """检查是否可以执行"""
        now = datetime.now()
        
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            # 检查是否可以转为半开状态
            if (self.last_failure_time and 
                (now - self.last_failure_time).total_seconds() > self.config.timeout):
                self.state = CircuitBreakerState.HALF_OPEN
                self.success_count = 0
                logger.info(f"熔断器 {self.name} 转为半开状态")
                return True
            return False
        else:  # HALF_OPEN
            return True
    
    def record_success(self):
        """记录成功"""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.failure_history.clear()
                logger.info(f"熔断器 {self.name} 恢复正常状态")
        elif self.state == CircuitBreakerState.CLOSED:
            # 清理过期的失败记录
            self._cleanup_failure_history()
    
    def record_failure(self):
        """记录失败"""
        now = datetime.now()
        self.failure_history.append(now)
        self.last_failure_time = now
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            logger.warning(f"熔断器 {self.name} 重新熔断")
        elif self.state == CircuitBreakerState.CLOSED:
            self._cleanup_failure_history()
            recent_failures = len(self.failure_history)
            
            if recent_failures >= self.config.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                logger.error(f"熔断器 {self.name} 触发熔断，失败次数: {recent_failures}")
    
    def _cleanup_failure_history(self):
        """清理过期的失败记录"""
        now = datetime.now()
        cutoff_time = now - timedelta(seconds=self.config.monitor_window)
        self.failure_history = [t for t in self.failure_history if t > cutoff_time]
    
    def get_state_info(self) -> Dict[str, Any]:
        """获取状态信息"""
        self._cleanup_failure_history()
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": len(self.failure_history),
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None
        }


class RetryExecutor:
    """重试执行器"""
    
    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """执行函数并重试"""
        last_exception = None
        
        for attempt in range(self.config.max_attempts):
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    logger.info(f"函数 {func.__name__} 在第 {attempt + 1} 次尝试后成功")
                return result
                
            except Exception as e:
                last_exception = e
                
                # 检查是否应该重试
                if not self._should_retry(e, attempt):
                    break
                
                # 计算延迟时间
                delay = self._calculate_delay(attempt)
                
                logger.warning(
                    f"函数 {func.__name__} 第 {attempt + 1} 次尝试失败: {e}, "
                    f"将在 {delay:.2f}s 后重试"
                )
                
                time.sleep(delay)
        
        # 所有重试都失败
        logger.error(f"函数 {func.__name__} 在 {self.config.max_attempts} 次尝试后仍然失败")
        raise last_exception
    
    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        """异步执行函数并重试"""
        last_exception = None
        
        for attempt in range(self.config.max_attempts):
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                    
                if attempt > 0:
                    logger.info(f"异步函数 {func.__name__} 在第 {attempt + 1} 次尝试后成功")
                return result
                
            except Exception as e:
                last_exception = e
                
                if not self._should_retry(e, attempt):
                    break
                
                delay = self._calculate_delay(attempt)
                
                logger.warning(
                    f"异步函数 {func.__name__} 第 {attempt + 1} 次尝试失败: {e}, "
                    f"将在 {delay:.2f}s 后重试"
                )
                
                await asyncio.sleep(delay)
        
        logger.error(f"异步函数 {func.__name__} 在 {self.config.max_attempts} 次尝试后仍然失败")
        raise last_exception
    
    def _should_retry(self, exception: Exception, attempt: int) -> bool:
        """判断是否应该重试"""
        # 检查是否还有重试机会
        if attempt >= self.config.max_attempts - 1:
            return False
        
        # 检查不可重试的异常
        if self.config.non_retryable_exceptions:
            for exc_type in self.config.non_retryable_exceptions:
                if isinstance(exception, exc_type):
                    return False
        
        # 检查可重试的异常
        if self.config.retryable_exceptions:
            for exc_type in self.config.retryable_exceptions:
                if isinstance(exception, exc_type):
                    return True
            return False  # 如果指定了可重试异常但不匹配，则不重试
        
        # 默认情况下，除了某些特定异常外都可以重试
        if isinstance(exception, (KeyboardInterrupt, SystemExit)):
            return False
        
        return True
    
    def _calculate_delay(self, attempt: int) -> float:
        """计算延迟时间"""
        if self.config.strategy == RetryStrategy.FIXED_DELAY:
            delay = self.config.base_delay
        elif self.config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.config.base_delay * (self.config.backoff_multiplier ** attempt)
        elif self.config.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.config.base_delay * (attempt + 1)
        elif self.config.strategy == RetryStrategy.RANDOM_JITTER:
            delay = self.config.base_delay + random.uniform(0, self.config.base_delay)
        else:
            delay = self.config.base_delay
        
        # 应用抖动
        if self.config.jitter and self.config.strategy != RetryStrategy.RANDOM_JITTER:
            jitter = random.uniform(0.8, 1.2)
            delay *= jitter
        
        # 限制最大延迟
        return min(delay, self.config.max_delay)


class FallbackHandler:
    """降级处理器"""
    
    def __init__(self):
        self.fallback_functions: Dict[str, List[Callable]] = {}
    
    def register_fallback(self, operation_name: str, fallback_func: Callable, priority: int = 0):
        """注册降级函数"""
        if operation_name not in self.fallback_functions:
            self.fallback_functions[operation_name] = []
        
        self.fallback_functions[operation_name].append((priority, fallback_func))
        # 按优先级排序
        self.fallback_functions[operation_name].sort(key=lambda x: x[0])
    
    def execute_with_fallback(self, operation_name: str, primary_func: Callable, *args, **kwargs) -> Any:
        """执行主函数，失败时使用降级函数"""
        try:
            return primary_func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"主函数 {primary_func.__name__} 执行失败: {e}，尝试降级处理")
            
            if operation_name in self.fallback_functions:
                for priority, fallback_func in self.fallback_functions[operation_name]:
                    try:
                        result = fallback_func(*args, **kwargs)
                        logger.info(f"降级函数 {fallback_func.__name__} 执行成功")
                        return result
                    except Exception as fallback_error:
                        logger.warning(f"降级函数 {fallback_func.__name__} 也失败: {fallback_error}")
                        continue
            
            # 所有降级函数都失败
            logger.error(f"操作 {operation_name} 的所有降级函数都失败")
            raise e


class ErrorRecoveryManager:
    """错误恢复管理器"""
    
    def __init__(self):
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.retry_executor = RetryExecutor()
        self.fallback_handler = FallbackHandler()
    
    def create_circuit_breaker(self, name: str, config: CircuitBreakerConfig = None) -> CircuitBreaker:
        """创建熔断器"""
        config = config or CircuitBreakerConfig()
        circuit_breaker = CircuitBreaker(name, config)
        self.circuit_breakers[name] = circuit_breaker
        return circuit_breaker
    
    def execute_with_circuit_breaker(
        self, 
        circuit_breaker_name: str, 
        func: Callable, 
        *args, 
        **kwargs
    ) -> Any:
        """使用熔断器执行函数"""
        if circuit_breaker_name not in self.circuit_breakers:
            raise ValueError(f"熔断器 {circuit_breaker_name} 不存在")
        
        circuit_breaker = self.circuit_breakers[circuit_breaker_name]
        
        if not circuit_breaker.can_execute():
            raise VesperaException(
                f"熔断器 {circuit_breaker_name} 处于开启状态，拒绝执行",
                severity=ErrorSeverity.HIGH,
                category=ErrorCategory.SYSTEM,
                component="circuit_breaker",
                operation="execute"
            )
        
        try:
            result = func(*args, **kwargs)
            circuit_breaker.record_success()
            return result
        except Exception as e:
            circuit_breaker.record_failure()
            raise e
    
    def execute_with_full_recovery(
        self,
        operation_name: str,
        primary_func: Callable,
        circuit_breaker_name: str = None,
        retry_config: RetryConfig = None,
        *args,
        **kwargs
    ) -> Any:
        """使用完整的错误恢复机制执行函数"""
        
        def wrapped_func(*args, **kwargs):
            if circuit_breaker_name:
                return self.execute_with_circuit_breaker(
                    circuit_breaker_name, primary_func, *args, **kwargs
                )
            else:
                return primary_func(*args, **kwargs)
        
        # 使用重试执行器
        if retry_config:
            executor = RetryExecutor(retry_config)
        else:
            executor = self.retry_executor
        
        try:
            return executor.execute(wrapped_func, *args, **kwargs)
        except Exception as e:
            # 尝试降级处理
            return self.fallback_handler.execute_with_fallback(
                operation_name, lambda *a, **k: None, *args, **kwargs
            )
    
    def get_system_health(self) -> Dict[str, Any]:
        """获取系统健康状态"""
        circuit_breaker_states = {}
        for name, cb in self.circuit_breakers.items():
            circuit_breaker_states[name] = cb.get_state_info()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "circuit_breakers": circuit_breaker_states,
            "total_circuit_breakers": len(self.circuit_breakers),
            "open_circuit_breakers": sum(
                1 for cb in self.circuit_breakers.values() 
                if cb.state == CircuitBreakerState.OPEN
            )
        }


# 全局错误恢复管理器实例
global_recovery_manager = ErrorRecoveryManager()


# 装饰器
def with_retry(config: RetryConfig = None):
    """重试装饰器"""
    def decorator(func):
        executor = RetryExecutor(config or RetryConfig())
        
        def wrapper(*args, **kwargs):
            return executor.execute(func, *args, **kwargs)
        
        async def async_wrapper(*args, **kwargs):
            return await executor.execute_async(func, *args, **kwargs)
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return wrapper
    
    return decorator


def with_circuit_breaker(circuit_breaker_name: str, config: CircuitBreakerConfig = None):
    """熔断器装饰器"""
    def decorator(func):
        # 确保熔断器存在
        if circuit_breaker_name not in global_recovery_manager.circuit_breakers:
            global_recovery_manager.create_circuit_breaker(circuit_breaker_name, config)
        
        def wrapper(*args, **kwargs):
            return global_recovery_manager.execute_with_circuit_breaker(
                circuit_breaker_name, func, *args, **kwargs
            )
        
        return wrapper
    
    return decorator


def with_fallback(operation_name: str, fallback_func: Callable = None):
    """降级装饰器"""
    def decorator(func):
        if fallback_func:
            global_recovery_manager.fallback_handler.register_fallback(
                operation_name, fallback_func
            )
        
        def wrapper(*args, **kwargs):
            return global_recovery_manager.fallback_handler.execute_with_fallback(
                operation_name, func, *args, **kwargs
            )
        
        return wrapper
    
    return decorator