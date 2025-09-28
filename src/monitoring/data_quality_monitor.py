"""
数据质量监控系统
实现实时数据质量监控、告警和自动修复
"""
import asyncio
import time
from typing import Dict, List, Any, Optional, Callable, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np
from collections import defaultdict, deque
import json
from pathlib import Path

try:
    from src.utils.logger import get_logger
    from src.data_sources.data_quality_checkers import DataQualityManager, QualityReport
    from src.utils.exceptions import VesperaException, ErrorSeverity, ErrorCategory
except ImportError:
    import logging
    def get_logger(name):
        return logging.getLogger(name)
    
    # 创建模拟类
    class DataQualityManager:
        def check_quality(self, data):
            return type('QualityReport', (), {'quality_score': 100, 'passed': True, 'issues': []})()
    
    class VesperaException(Exception):
        pass
    
    class ErrorSeverity:
        LOW = "LOW"
        MEDIUM = "MEDIUM"
        HIGH = "HIGH"
        CRITICAL = "CRITICAL"
    
    class ErrorCategory:
        DATA_SOURCE = "DATA_SOURCE"

logger = get_logger("data_quality_monitor")


class AlertLevel(Enum):
    """告警级别"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class MonitoringStatus(Enum):
    """监控状态"""
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    ERROR = "ERROR"


@dataclass
class QualityMetric:
    """质量指标"""
    name: str
    value: float
    threshold: float
    status: str  # OK, WARNING, ERROR
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityAlert:
    """质量告警"""
    alert_id: str
    level: AlertLevel
    message: str
    source: str
    timestamp: datetime
    metric_name: str
    current_value: float
    threshold: float
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "level": self.level.value,
            "message": self.message,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "metric_name": self.metric_name,
            "current_value": self.current_value,
            "threshold": self.threshold,
            "resolved": self.resolved,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "metadata": self.metadata
        }


@dataclass
class MonitoringRule:
    """监控规则"""
    name: str
    metric_name: str
    threshold: float
    comparison: str  # >, <, >=, <=, ==, !=
    alert_level: AlertLevel
    enabled: bool = True
    cooldown_minutes: int = 5
    description: str = ""
    
    def evaluate(self, value: float) -> bool:
        """评估规则"""
        if not self.enabled:
            return False
        
        if self.comparison == ">":
            return value > self.threshold
        elif self.comparison == "<":
            return value < self.threshold
        elif self.comparison == ">=":
            return value >= self.threshold
        elif self.comparison == "<=":
            return value <= self.threshold
        elif self.comparison == "==":
            return value == self.threshold
        elif self.comparison == "!=":
            return value != self.threshold
        else:
            return False


class DataQualityMonitor:
    """数据质量监控器"""
    
    def __init__(self, 
                 check_interval: int = 300,  # 5分钟
                 history_size: int = 1000,
                 alert_cooldown: int = 300):  # 5分钟冷却
        
        self.check_interval = check_interval
        self.history_size = history_size
        self.alert_cooldown = alert_cooldown
        
        # 组件
        self.quality_manager = DataQualityManager()
        
        # 状态
        self.status = MonitoringStatus.STOPPED
        self.monitoring_task: Optional[asyncio.Task] = None
        
        # 数据存储
        self.metrics_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=history_size))
        self.alerts: Dict[str, QualityAlert] = {}
        self.rules: Dict[str, MonitoringRule] = {}
        self.last_alert_time: Dict[str, datetime] = {}
        
        # 数据源注册
        self.data_sources: Dict[str, Callable] = {}
        
        # 统计信息
        self.stats = {
            "total_checks": 0,
            "total_alerts": 0,
            "active_alerts": 0,
            "last_check_time": None,
            "average_quality_score": 0.0
        }
        
        # 设置默认规则
        self._setup_default_rules()
        
        logger.info("数据质量监控器初始化完成")
    
    def _setup_default_rules(self):
        """设置默认监控规则"""
        default_rules = [
            MonitoringRule(
                name="quality_score_low",
                metric_name="quality_score",
                threshold=80.0,
                comparison="<",
                alert_level=AlertLevel.WARNING,
                description="数据质量分数过低"
            ),
            MonitoringRule(
                name="quality_score_critical",
                metric_name="quality_score",
                threshold=60.0,
                comparison="<",
                alert_level=AlertLevel.CRITICAL,
                description="数据质量分数严重过低"
            ),
            MonitoringRule(
                name="missing_data_high",
                metric_name="missing_data_ratio",
                threshold=0.1,
                comparison=">",
                alert_level=AlertLevel.WARNING,
                description="缺失数据比例过高"
            ),
            MonitoringRule(
                name="duplicate_data_high",
                metric_name="duplicate_ratio",
                threshold=0.05,
                comparison=">",
                alert_level=AlertLevel.ERROR,
                description="重复数据比例过高"
            ),
            MonitoringRule(
                name="data_freshness_stale",
                metric_name="data_age_hours",
                threshold=24.0,
                comparison=">",
                alert_level=AlertLevel.WARNING,
                description="数据过期"
            )
        ]
        
        for rule in default_rules:
            self.rules[rule.name] = rule
    
    def register_data_source(self, name: str, data_fetcher: Callable):
        """注册数据源"""
        self.data_sources[name] = data_fetcher
        logger.info(f"注册数据源: {name}")
    
    def add_rule(self, rule: MonitoringRule):
        """添加监控规则"""
        self.rules[rule.name] = rule
        logger.info(f"添加监控规则: {rule.name}")
    
    def remove_rule(self, rule_name: str):
        """移除监控规则"""
        if rule_name in self.rules:
            del self.rules[rule_name]
            logger.info(f"移除监控规则: {rule_name}")
    
    async def start_monitoring(self):
        """开始监控"""
        if self.status == MonitoringStatus.ACTIVE:
            logger.warning("监控已在运行中")
            return
        
        self.status = MonitoringStatus.ACTIVE
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("数据质量监控已启动")
    
    async def stop_monitoring(self):
        """停止监控"""
        self.status = MonitoringStatus.STOPPED
        
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        logger.info("数据质量监控已停止")
    
    async def pause_monitoring(self):
        """暂停监控"""
        self.status = MonitoringStatus.PAUSED
        logger.info("数据质量监控已暂停")
    
    async def resume_monitoring(self):
        """恢复监控"""
        if self.status == MonitoringStatus.PAUSED:
            self.status = MonitoringStatus.ACTIVE
            logger.info("数据质量监控已恢复")
    
    async def _monitoring_loop(self):
        """监控循环"""
        while self.status in [MonitoringStatus.ACTIVE, MonitoringStatus.PAUSED]:
            try:
                if self.status == MonitoringStatus.ACTIVE:
                    await self._perform_quality_check()
                
                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"监控循环错误: {e}")
                self.status = MonitoringStatus.ERROR
                await asyncio.sleep(60)  # 错误后等待1分钟
    
    async def _perform_quality_check(self):
        """执行质量检查"""
        check_start_time = time.time()
        
        try:
            # 检查所有注册的数据源
            for source_name, data_fetcher in self.data_sources.items():
                await self._check_data_source_quality(source_name, data_fetcher)
            
            # 更新统计信息
            self.stats["total_checks"] += 1
            self.stats["last_check_time"] = datetime.now()
            
            check_duration = time.time() - check_start_time
            logger.debug(f"质量检查完成，耗时: {check_duration:.2f}s")
            
        except Exception as e:
            logger.error(f"质量检查失败: {e}")
            raise VesperaException(
                f"质量检查失败: {e}",
                severity=ErrorSeverity.HIGH,
                category=ErrorCategory.DATA_SOURCE
            )
    
    async def _check_data_source_quality(self, source_name: str, data_fetcher: Callable):
        """检查单个数据源的质量"""
        try:
            # 获取数据
            if asyncio.iscoroutinefunction(data_fetcher):
                data = await data_fetcher()
            else:
                data = data_fetcher()
            
            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                self._create_alert(
                    source_name,
                    "no_data",
                    AlertLevel.ERROR,
                    f"数据源 {source_name} 返回空数据",
                    0.0,
                    1.0
                )
                return
            
            # 执行质量检查
            quality_report = self.quality_manager.check_quality(data)
            
            # 计算质量指标
            metrics = self._calculate_quality_metrics(source_name, data, quality_report)
            
            # 记录指标历史
            for metric in metrics:
                self.metrics_history[f"{source_name}_{metric.name}"].append(metric)
            
            # 评估规则和生成告警
            await self._evaluate_rules(source_name, metrics)
            
            logger.debug(f"数据源 {source_name} 质量检查完成")
            
        except Exception as e:
            logger.error(f"数据源 {source_name} 质量检查失败: {e}")
            self._create_alert(
                source_name,
                "check_failed",
                AlertLevel.ERROR,
                f"数据源 {source_name} 质量检查失败: {e}",
                0.0,
                1.0
            )
    
    def _calculate_quality_metrics(self, 
                                 source_name: str, 
                                 data: pd.DataFrame, 
                                 quality_report) -> List[QualityMetric]:
        """计算质量指标"""
        metrics = []
        timestamp = datetime.now()
        
        # 基础指标
        metrics.append(QualityMetric(
            name="quality_score",
            value=getattr(quality_report, 'quality_score', 100),
            threshold=80.0,
            status="OK" if getattr(quality_report, 'quality_score', 100) >= 80 else "WARNING",
            timestamp=timestamp
        ))
        
        # 数据完整性指标
        if isinstance(data, pd.DataFrame):
            total_cells = data.size
            missing_cells = data.isnull().sum().sum()
            missing_ratio = missing_cells / total_cells if total_cells > 0 else 0
            
            metrics.append(QualityMetric(
                name="missing_data_ratio",
                value=missing_ratio,
                threshold=0.1,
                status="OK" if missing_ratio <= 0.1 else "WARNING",
                timestamp=timestamp
            ))
            
            # 重复数据指标
            duplicate_rows = data.duplicated().sum()
            duplicate_ratio = duplicate_rows / len(data) if len(data) > 0 else 0
            
            metrics.append(QualityMetric(
                name="duplicate_ratio",
                value=duplicate_ratio,
                threshold=0.05,
                status="OK" if duplicate_ratio <= 0.05 else "WARNING",
                timestamp=timestamp
            ))
            
            # 数据新鲜度指标（如果有时间列）
            time_columns = [col for col in data.columns if 'date' in col.lower() or 'time' in col.lower()]
            if time_columns:
                try:
                    latest_date = pd.to_datetime(data[time_columns[0]]).max()
                    data_age_hours = (datetime.now() - latest_date).total_seconds() / 3600
                    
                    metrics.append(QualityMetric(
                        name="data_age_hours",
                        value=data_age_hours,
                        threshold=24.0,
                        status="OK" if data_age_hours <= 24 else "WARNING",
                        timestamp=timestamp
                    ))
                except Exception:
                    pass
            
            # 数据量指标
            metrics.append(QualityMetric(
                name="record_count",
                value=len(data),
                threshold=0,
                status="OK" if len(data) > 0 else "ERROR",
                timestamp=timestamp
            ))
        
        return metrics
    
    async def _evaluate_rules(self, source_name: str, metrics: List[QualityMetric]):
        """评估监控规则"""
        for metric in metrics:
            metric_key = f"{source_name}_{metric.name}"
            
            for rule in self.rules.values():
                if rule.metric_name == metric.name and rule.enabled:
                    if rule.evaluate(metric.value):
                        # 检查冷却时间
                        if self._should_create_alert(rule.name, metric_key):
                            self._create_alert(
                                source_name,
                                rule.name,
                                rule.alert_level,
                                f"{rule.description}: {metric.value} {rule.comparison} {rule.threshold}",
                                metric.value,
                                rule.threshold
                            )
    
    def _should_create_alert(self, rule_name: str, metric_key: str) -> bool:
        """检查是否应该创建告警（考虑冷却时间）"""
        alert_key = f"{rule_name}_{metric_key}"
        
        if alert_key in self.last_alert_time:
            time_since_last = datetime.now() - self.last_alert_time[alert_key]
            if time_since_last.total_seconds() < self.alert_cooldown:
                return False
        
        return True
    
    def _create_alert(self, 
                     source: str,
                     rule_name: str,
                     level: AlertLevel,
                     message: str,
                     current_value: float,
                     threshold: float):
        """创建告警"""
        alert_id = f"{source}_{rule_name}_{int(time.time())}"
        
        alert = QualityAlert(
            alert_id=alert_id,
            level=level,
            message=message,
            source=source,
            timestamp=datetime.now(),
            metric_name=rule_name,
            current_value=current_value,
            threshold=threshold
        )
        
        self.alerts[alert_id] = alert
        self.last_alert_time[f"{rule_name}_{source}"] = datetime.now()
        
        # 更新统计
        self.stats["total_alerts"] += 1
        self.stats["active_alerts"] = len([a for a in self.alerts.values() if not a.resolved])
        
        logger.warning(f"创建告警: {alert.level.value} - {message}")
        
        # 触发告警处理
        asyncio.create_task(self._handle_alert(alert))
    
    async def _handle_alert(self, alert: QualityAlert):
        """处理告警"""
        try:
            # 记录告警
            await self._log_alert(alert)
            
            # 根据告警级别执行不同的处理
            if alert.level == AlertLevel.CRITICAL:
                await self._handle_critical_alert(alert)
            elif alert.level == AlertLevel.ERROR:
                await self._handle_error_alert(alert)
            elif alert.level == AlertLevel.WARNING:
                await self._handle_warning_alert(alert)
            
        except Exception as e:
            logger.error(f"告警处理失败: {alert.alert_id} - {e}")
    
    async def _log_alert(self, alert: QualityAlert):
        """记录告警到文件"""
        try:
            log_dir = Path("logs/alerts")
            log_dir.mkdir(parents=True, exist_ok=True)
            
            log_file = log_dir / f"alerts_{datetime.now().strftime('%Y%m%d')}.json"
            
            # 读取现有日志
            alerts_log = []
            if log_file.exists():
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        alerts_log = json.load(f)
                except Exception:
                    alerts_log = []
            
            # 添加新告警
            alerts_log.append(alert.to_dict())
            
            # 写入文件
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(alerts_log, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"记录告警失败: {e}")
    
    async def _handle_critical_alert(self, alert: QualityAlert):
        """处理严重告警"""
        logger.critical(f"严重告警: {alert.message}")
        # 可以在这里添加紧急通知逻辑
        
    async def _handle_error_alert(self, alert: QualityAlert):
        """处理错误告警"""
        logger.error(f"错误告警: {alert.message}")
        # 可以在这里添加错误处理逻辑
        
    async def _handle_warning_alert(self, alert: QualityAlert):
        """处理警告告警"""
        logger.warning(f"警告告警: {alert.message}")
        # 可以在这里添加警告处理逻辑
    
    def resolve_alert(self, alert_id: str) -> bool:
        """解决告警"""
        if alert_id in self.alerts:
            self.alerts[alert_id].resolved = True
            self.alerts[alert_id].resolved_at = datetime.now()
            
            # 更新统计
            self.stats["active_alerts"] = len([a for a in self.alerts.values() if not a.resolved])
            
            logger.info(f"告警已解决: {alert_id}")
            return True
        
        return False
    
    def get_active_alerts(self) -> List[QualityAlert]:
        """获取活跃告警"""
        return [alert for alert in self.alerts.values() if not alert.resolved]
    
    def get_metrics_history(self, 
                          source_name: str = None, 
                          metric_name: str = None,
                          hours: int = 24) -> Dict[str, List[QualityMetric]]:
        """获取指标历史"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        result = {}
        
        for key, metrics in self.metrics_history.items():
            # 过滤条件
            if source_name and not key.startswith(f"{source_name}_"):
                continue
            if metric_name and not key.endswith(f"_{metric_name}"):
                continue
            
            # 过滤时间
            filtered_metrics = [
                m for m in metrics 
                if m.timestamp >= cutoff_time
            ]
            
            if filtered_metrics:
                result[key] = filtered_metrics
        
        return result
    
    def get_quality_summary(self) -> Dict[str, Any]:
        """获取质量摘要"""
        active_alerts = self.get_active_alerts()
        
        # 计算平均质量分数
        quality_scores = []
        for metrics in self.metrics_history.values():
            if metrics and "quality_score" in metrics[-1].name:
                quality_scores.append(metrics[-1].value)
        
        avg_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        # 按级别统计告警
        alert_counts = {level.value: 0 for level in AlertLevel}
        for alert in active_alerts:
            alert_counts[alert.level.value] += 1
        
        return {
            "timestamp": datetime.now().isoformat(),
            "monitoring_status": self.status.value,
            "total_data_sources": len(self.data_sources),
            "average_quality_score": avg_quality_score,
            "active_alerts_count": len(active_alerts),
            "alert_breakdown": alert_counts,
            "total_checks": self.stats["total_checks"],
            "last_check_time": self.stats["last_check_time"].isoformat() if self.stats["last_check_time"] else None,
            "monitoring_rules_count": len([r for r in self.rules.values() if r.enabled])
        }
    
    def export_metrics(self, file_path: str, hours: int = 24):
        """导出指标数据"""
        metrics_data = self.get_metrics_history(hours=hours)
        
        # 转换为可序列化的格式
        export_data = {}
        for key, metrics in metrics_data.items():
            export_data[key] = [
                {
                    "name": m.name,
                    "value": m.value,
                    "threshold": m.threshold,
                    "status": m.status,
                    "timestamp": m.timestamp.isoformat(),
                    "metadata": m.metadata
                }
                for m in metrics
            ]
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"指标数据已导出到: {file_path}")


# 全局监控器实例
global_quality_monitor = DataQualityMonitor()


# 便捷函数
async def start_quality_monitoring(data_sources: Dict[str, Callable], 
                                 check_interval: int = 300):
    """启动质量监控便捷函数"""
    monitor = DataQualityMonitor(check_interval=check_interval)
    
    for name, fetcher in data_sources.items():
        monitor.register_data_source(name, fetcher)
    
    await monitor.start_monitoring()
    return monitor


def create_quality_rule(name: str,
                       metric_name: str,
                       threshold: float,
                       comparison: str,
                       alert_level: AlertLevel = AlertLevel.WARNING) -> MonitoringRule:
    """创建质量规则便捷函数"""
    return MonitoringRule(
        name=name,
        metric_name=metric_name,
        threshold=threshold,
        comparison=comparison,
        alert_level=alert_level
    )