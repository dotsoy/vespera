"""
Vespera 统一异常处理框架
定义系统中所有异常类型和错误处理机制
"""
import traceback
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
from dataclasses import dataclass

try:
    from .logger import get_logger
except ImportError:
    # 如果logger模块不存在，使用标准logging
    import logging
    def get_logger(name):
        return logging.getLogger(name)

logger = get_logger("exceptions")


class ErrorSeverity(Enum):
    """错误严重程度"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ErrorCategory(Enum):
    """错误分类"""
    DATA_SOURCE = "DATA_SOURCE"
    STRATEGY = "STRATEGY"
    ANALYSIS = "ANALYSIS"
    DATABASE = "DATABASE"
    NETWORK = "NETWORK"
    VALIDATION = "VALIDATION"
    SYSTEM = "SYSTEM"
    USER_INPUT = "USER_INPUT"


@dataclass
class ErrorContext:
    """错误上下文信息"""
    timestamp: datetime
    severity: ErrorSeverity
    category: ErrorCategory
    component: str
    operation: str
    metadata: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "severity": self.severity.value,
            "category": self.category.value,
            "component": self.component,
            "operation": self.operation,
            "metadata": self.metadata or {}
        }


class VesperaException(Exception):
    """Vespera 系统基础异常类"""
    
    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        category: ErrorCategory = ErrorCategory.SYSTEM,
        component: str = "unknown",
        operation: str = "unknown",
        metadata: Dict[str, Any] = None,
        cause: Exception = None
    ):
        super().__init__(message)
        self.message = message
        self.context = ErrorContext(
            timestamp=datetime.now(),
            severity=severity,
            category=category,
            component=component,
            operation=operation,
            metadata=metadata
        )
        self.cause = cause
        
        # 记录异常
        self._log_exception()
    
    def _log_exception(self):
        """记录异常信息"""
        log_level = {
            ErrorSeverity.LOW: logger.debug,
            ErrorSeverity.MEDIUM: logger.warning,
            ErrorSeverity.HIGH: logger.error,
            ErrorSeverity.CRITICAL: logger.critical
        }.get(self.context.severity, logger.error)
        
        log_level(
            f"[{self.context.category.value}] {self.message}",
            extra={
                "component": self.context.component,
                "operation": self.context.operation,
                "severity": self.context.severity.value,
                "metadata": self.context.metadata,
                "traceback": traceback.format_exc() if self.cause else None
            }
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "context": self.context.to_dict(),
            "cause": str(self.cause) if self.cause else None
        }


# 数据源相关异常
class DataSourceException(VesperaException):
    """数据源异常"""
    
    def __init__(self, message: str, source_name: str = None, **kwargs):
        kwargs.setdefault("category", ErrorCategory.DATA_SOURCE)
        kwargs.setdefault("component", "data_source")
        if source_name:
            kwargs.setdefault("metadata", {}).update({"source_name": source_name})
        super().__init__(message, **kwargs)


class DataSourceUnavailableException(DataSourceException):
    """数据源不可用异常"""
    
    def __init__(self, source_name: str, **kwargs):
        message = f"数据源 {source_name} 不可用"
        kwargs.setdefault("severity", ErrorSeverity.HIGH)
        super().__init__(message, source_name=source_name, **kwargs)


class DataSourceTimeoutException(DataSourceException):
    """数据源超时异常"""
    
    def __init__(self, source_name: str, timeout: float, **kwargs):
        message = f"数据源 {source_name} 请求超时 ({timeout}s)"
        kwargs.setdefault("severity", ErrorSeverity.MEDIUM)
        kwargs.setdefault("metadata", {}).update({"timeout": timeout})
        super().__init__(message, source_name=source_name, **kwargs)


class DataSourceRateLimitException(DataSourceException):
    """数据源速率限制异常"""
    
    def __init__(self, source_name: str, retry_after: int = None, **kwargs):
        message = f"数据源 {source_name} 达到速率限制"
        if retry_after:
            message += f"，请在 {retry_after} 秒后重试"
        kwargs.setdefault("severity", ErrorSeverity.MEDIUM)
        kwargs.setdefault("metadata", {}).update({"retry_after": retry_after})
        super().__init__(message, source_name=source_name, **kwargs)


# 数据质量相关异常
class DataQualityException(VesperaException):
    """数据质量异常"""
    
    def __init__(self, message: str, quality_score: float = None, **kwargs):
        kwargs.setdefault("category", ErrorCategory.VALIDATION)
        kwargs.setdefault("component", "data_quality")
        if quality_score is not None:
            kwargs.setdefault("metadata", {}).update({"quality_score": quality_score})
        super().__init__(message, **kwargs)


class DataValidationException(DataQualityException):
    """数据验证异常"""
    
    def __init__(self, message: str, validation_errors: List[str] = None, **kwargs):
        kwargs.setdefault("severity", ErrorSeverity.HIGH)
        if validation_errors:
            kwargs.setdefault("metadata", {}).update({"validation_errors": validation_errors})
        super().__init__(message, **kwargs)


# 策略相关异常
class StrategyException(VesperaException):
    """策略异常"""
    
    def __init__(self, message: str, strategy_name: str = None, **kwargs):
        kwargs.setdefault("category", ErrorCategory.STRATEGY)
        kwargs.setdefault("component", "strategy")
        if strategy_name:
            kwargs.setdefault("metadata", {}).update({"strategy_name": strategy_name})
        super().__init__(message, **kwargs)


class StrategyConfigurationException(StrategyException):
    """策略配置异常"""
    
    def __init__(self, message: str, config_key: str = None, **kwargs):
        kwargs.setdefault("severity", ErrorSeverity.HIGH)
        if config_key:
            kwargs.setdefault("metadata", {}).update({"config_key": config_key})
        super().__init__(message, **kwargs)


class StrategyExecutionException(StrategyException):
    """策略执行异常"""
    
    def __init__(self, message: str, execution_step: str = None, **kwargs):
        kwargs.setdefault("severity", ErrorSeverity.MEDIUM)
        if execution_step:
            kwargs.setdefault("metadata", {}).update({"execution_step": execution_step})
        super().__init__(message, **kwargs)


# 分析相关异常
class AnalysisException(VesperaException):
    """分析异常"""
    
    def __init__(self, message: str, analyzer_name: str = None, **kwargs):
        kwargs.setdefault("category", ErrorCategory.ANALYSIS)
        kwargs.setdefault("component", "analyzer")
        if analyzer_name:
            kwargs.setdefault("metadata", {}).update({"analyzer_name": analyzer_name})
        super().__init__(message, **kwargs)


class TechnicalAnalysisException(AnalysisException):
    """技术分析异常"""
    
    def __init__(self, message: str, indicator_name: str = None, **kwargs):
        kwargs.setdefault("component", "technical_analyzer")
        if indicator_name:
            kwargs.setdefault("metadata", {}).update({"indicator_name": indicator_name})
        super().__init__(message, **kwargs)


class FundamentalAnalysisException(AnalysisException):
    """基本面分析异常"""
    
    def __init__(self, message: str, **kwargs):
        kwargs.setdefault("component", "fundamental_analyzer")
        super().__init__(message, **kwargs)


# 数据库相关异常
class DatabaseException(VesperaException):
    """数据库异常"""
    
    def __init__(self, message: str, database_name: str = None, **kwargs):
        kwargs.setdefault("category", ErrorCategory.DATABASE)
        kwargs.setdefault("component", "database")
        if database_name:
            kwargs.setdefault("metadata", {}).update({"database_name": database_name})
        super().__init__(message, **kwargs)


class DatabaseConnectionException(DatabaseException):
    """数据库连接异常"""
    
    def __init__(self, message: str, **kwargs):
        kwargs.setdefault("severity", ErrorSeverity.HIGH)
        super().__init__(message, **kwargs)


class DatabaseQueryException(DatabaseException):
    """数据库查询异常"""
    
    def __init__(self, message: str, query: str = None, **kwargs):
        kwargs.setdefault("severity", ErrorSeverity.MEDIUM)
        if query:
            kwargs.setdefault("metadata", {}).update({"query": query[:500]})  # 限制查询长度
        super().__init__(message, **kwargs)


# 网络相关异常
class NetworkException(VesperaException):
    """网络异常"""
    
    def __init__(self, message: str, url: str = None, **kwargs):
        kwargs.setdefault("category", ErrorCategory.NETWORK)
        kwargs.setdefault("component", "network")
        if url:
            kwargs.setdefault("metadata", {}).update({"url": url})
        super().__init__(message, **kwargs)


class NetworkTimeoutException(NetworkException):
    """网络超时异常"""
    
    def __init__(self, message: str, timeout: float = None, **kwargs):
        kwargs.setdefault("severity", ErrorSeverity.MEDIUM)
        if timeout:
            kwargs.setdefault("metadata", {}).update({"timeout": timeout})
        super().__init__(message, **kwargs)


# 用户输入相关异常
class UserInputException(VesperaException):
    """用户输入异常"""
    
    def __init__(self, message: str, input_field: str = None, **kwargs):
        kwargs.setdefault("category", ErrorCategory.USER_INPUT)
        kwargs.setdefault("component", "user_interface")
        kwargs.setdefault("severity", ErrorSeverity.LOW)
        if input_field:
            kwargs.setdefault("metadata", {}).update({"input_field": input_field})
        super().__init__(message, **kwargs)


class InvalidParameterException(UserInputException):
    """无效参数异常"""
    
    def __init__(self, parameter_name: str, parameter_value: Any, expected_type: str = None, **kwargs):
        message = f"参数 {parameter_name} 的值 {parameter_value} 无效"
        if expected_type:
            message += f"，期望类型: {expected_type}"
        kwargs.setdefault("metadata", {}).update({
            "parameter_name": parameter_name,
            "parameter_value": str(parameter_value),
            "expected_type": expected_type
        })
        super().__init__(message, input_field=parameter_name, **kwargs)


# 错误处理装饰器
def handle_exceptions(
    default_return=None,
    reraise: bool = False,
    log_level: str = "error"
):
    """异常处理装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except VesperaException:
                # Vespera异常已经被记录，直接重新抛出或返回默认值
                if reraise:
                    raise
                return default_return
            except Exception as e:
                # 包装未知异常
                vespera_exception = VesperaException(
                    message=f"未处理的异常: {str(e)}",
                    severity=ErrorSeverity.HIGH,
                    component=func.__module__,
                    operation=func.__name__,
                    cause=e
                )
                if reraise:
                    raise vespera_exception
                return default_return
        return wrapper
    return decorator


# 异步异常处理装饰器
def handle_async_exceptions(
    default_return=None,
    reraise: bool = False
):
    """异步异常处理装饰器"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except VesperaException:
                if reraise:
                    raise
                return default_return
            except Exception as e:
                vespera_exception = VesperaException(
                    message=f"未处理的异步异常: {str(e)}",
                    severity=ErrorSeverity.HIGH,
                    component=func.__module__,
                    operation=func.__name__,
                    cause=e
                )
                if reraise:
                    raise vespera_exception
                return default_return
        return wrapper
    return decorator


class ErrorCollector:
    """错误收集器"""
    
    def __init__(self, max_errors: int = 1000):
        self.errors: List[VesperaException] = []
        self.max_errors = max_errors
    
    def add_error(self, error: VesperaException):
        """添加错误"""
        self.errors.append(error)
        
        # 保持错误数量在限制内
        if len(self.errors) > self.max_errors:
            self.errors = self.errors[-self.max_errors:]
    
    def get_errors_by_severity(self, severity: ErrorSeverity) -> List[VesperaException]:
        """按严重程度获取错误"""
        return [e for e in self.errors if e.context.severity == severity]
    
    def get_errors_by_category(self, category: ErrorCategory) -> List[VesperaException]:
        """按分类获取错误"""
        return [e for e in self.errors if e.context.category == category]
    
    def get_recent_errors(self, minutes: int = 60) -> List[VesperaException]:
        """获取最近的错误"""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        return [e for e in self.errors if e.context.timestamp > cutoff_time]
    
    def clear_errors(self):
        """清空错误"""
        self.errors.clear()
    
    def get_error_summary(self) -> Dict[str, Any]:
        """获取错误摘要"""
        if not self.errors:
            return {"total_errors": 0}
        
        severity_counts = {}
        category_counts = {}
        
        for error in self.errors:
            severity = error.context.severity.value
            category = error.context.category.value
            
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
            category_counts[category] = category_counts.get(category, 0) + 1
        
        return {
            "total_errors": len(self.errors),
            "severity_breakdown": severity_counts,
            "category_breakdown": category_counts,
            "latest_error": self.errors[-1].to_dict() if self.errors else None
        }


# 全局错误收集器实例
global_error_collector = ErrorCollector()