"""
数据质量检查器
实现各种数据质量验证规则
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from abc import ABC, abstractmethod

try:
    from src.utils.logger import get_logger
except ImportError:
    import logging
    def get_logger(name):
        return logging.getLogger(name)

logger = get_logger("data_quality_checkers")


@dataclass
class QualityIssue:
    """数据质量问题"""
    type: str
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    description: str
    affected_rows: List[int] = None
    affected_columns: List[str] = None
    metadata: Dict[str, Any] = None


@dataclass
class QualityReport:
    """数据质量报告"""
    passed: bool
    issues: List[QualityIssue]
    total_rows: int
    total_columns: int
    quality_score: float  # 0-100
    
    def add_issue(self, issue: QualityIssue):
        """添加质量问题"""
        self.issues.append(issue)
        if issue.severity in ['HIGH', 'CRITICAL']:
            self.passed = False


class BaseQualityChecker(ABC):
    """数据质量检查器基类"""
    
    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def check(self, data: pd.DataFrame) -> QualityReport:
        """执行质量检查"""
        pass


class PriceRangeChecker(BaseQualityChecker):
    """价格范围检查器"""
    
    def __init__(self, min_price: float = 0.01, max_price: float = 10000.0):
        super().__init__("PriceRangeChecker")
        self.min_price = min_price
        self.max_price = max_price
    
    def check(self, data: pd.DataFrame) -> QualityReport:
        """检查价格是否在合理范围内"""
        report = QualityReport(
            passed=True,
            issues=[],
            total_rows=len(data),
            total_columns=len(data.columns),
            quality_score=100.0
        )
        
        price_columns = ['open', 'high', 'low', 'close', 'adj_close']
        existing_price_cols = [col for col in price_columns if col in data.columns]
        
        for col in existing_price_cols:
            # 检查负价格
            negative_mask = data[col] < 0
            if negative_mask.any():
                issue = QualityIssue(
                    type="negative_price",
                    severity="CRITICAL",
                    description=f"发现负价格在列 {col}",
                    affected_rows=data[negative_mask].index.tolist(),
                    affected_columns=[col]
                )
                report.add_issue(issue)
            
            # 检查异常高价格
            high_price_mask = data[col] > self.max_price
            if high_price_mask.any():
                issue = QualityIssue(
                    type="abnormal_high_price",
                    severity="HIGH",
                    description=f"发现异常高价格在列 {col} (>{self.max_price})",
                    affected_rows=data[high_price_mask].index.tolist(),
                    affected_columns=[col]
                )
                report.add_issue(issue)
            
            # 检查异常低价格
            low_price_mask = (data[col] > 0) & (data[col] < self.min_price)
            if low_price_mask.any():
                issue = QualityIssue(
                    type="abnormal_low_price",
                    severity="MEDIUM",
                    description=f"发现异常低价格在列 {col} (<{self.min_price})",
                    affected_rows=data[low_price_mask].index.tolist(),
                    affected_columns=[col]
                )
                report.add_issue(issue)
        
        # 计算质量分数
        if report.issues:
            penalty = sum(10 if issue.severity == 'CRITICAL' else 
                         5 if issue.severity == 'HIGH' else 
                         2 if issue.severity == 'MEDIUM' else 1 
                         for issue in report.issues)
            report.quality_score = max(0, 100 - penalty)
        
        return report


class VolumeConsistencyChecker(BaseQualityChecker):
    """成交量一致性检查器"""
    
    def __init__(self):
        super().__init__("VolumeConsistencyChecker")
    
    def check(self, data: pd.DataFrame) -> QualityReport:
        """检查成交量数据一致性"""
        report = QualityReport(
            passed=True,
            issues=[],
            total_rows=len(data),
            total_columns=len(data.columns),
            quality_score=100.0
        )
        
        if 'volume' not in data.columns:
            return report
        
        # 检查负成交量
        negative_volume_mask = data['volume'] < 0
        if negative_volume_mask.any():
            issue = QualityIssue(
                type="negative_volume",
                severity="CRITICAL",
                description="发现负成交量",
                affected_rows=data[negative_volume_mask].index.tolist(),
                affected_columns=['volume']
            )
            report.add_issue(issue)
        
        # 检查异常大成交量（超过历史平均值的10倍）
        if len(data) > 20:
            avg_volume = data['volume'].rolling(20).mean()
            abnormal_volume_mask = data['volume'] > (avg_volume * 10)
            if abnormal_volume_mask.any():
                issue = QualityIssue(
                    type="abnormal_high_volume",
                    severity="MEDIUM",
                    description="发现异常大成交量（超过20日均值10倍）",
                    affected_rows=data[abnormal_volume_mask].index.tolist(),
                    affected_columns=['volume']
                )
                report.add_issue(issue)
        
        # 检查连续零成交量
        zero_volume_mask = data['volume'] == 0
        if zero_volume_mask.any():
            # 查找连续零成交量的区间
            consecutive_zeros = []
            current_start = None
            
            for i, is_zero in enumerate(zero_volume_mask):
                if is_zero and current_start is None:
                    current_start = i
                elif not is_zero and current_start is not None:
                    if i - current_start >= 3:  # 连续3天或以上
                        consecutive_zeros.extend(range(current_start, i))
                    current_start = None
            
            if consecutive_zeros:
                issue = QualityIssue(
                    type="consecutive_zero_volume",
                    severity="HIGH",
                    description="发现连续零成交量（3天或以上）",
                    affected_rows=consecutive_zeros,
                    affected_columns=['volume']
                )
                report.add_issue(issue)
        
        # 计算质量分数
        if report.issues:
            penalty = sum(10 if issue.severity == 'CRITICAL' else 
                         5 if issue.severity == 'HIGH' else 
                         2 if issue.severity == 'MEDIUM' else 1 
                         for issue in report.issues)
            report.quality_score = max(0, 100 - penalty)
        
        return report


class TimeSeriesChecker(BaseQualityChecker):
    """时间序列完整性检查器"""
    
    def __init__(self):
        super().__init__("TimeSeriesChecker")
    
    def check(self, data: pd.DataFrame) -> QualityReport:
        """检查时间序列完整性"""
        report = QualityReport(
            passed=True,
            issues=[],
            total_rows=len(data),
            total_columns=len(data.columns),
            quality_score=100.0
        )
        
        if 'trade_date' not in data.columns and data.index.name != 'trade_date':
            return report
        
        # 获取日期列
        if 'trade_date' in data.columns:
            dates = pd.to_datetime(data['trade_date'])
        else:
            dates = pd.to_datetime(data.index)
        
        if len(dates) < 2:
            return report
        
        # 检查日期排序
        if not dates.is_monotonic_increasing:
            issue = QualityIssue(
                type="unsorted_dates",
                severity="HIGH",
                description="日期序列未按时间顺序排列",
                affected_columns=['trade_date']
            )
            report.add_issue(issue)
        
        # 检查重复日期
        duplicate_dates = dates.duplicated()
        if duplicate_dates.any():
            issue = QualityIssue(
                type="duplicate_dates",
                severity="HIGH",
                description="发现重复日期",
                affected_rows=data[duplicate_dates].index.tolist(),
                affected_columns=['trade_date']
            )
            report.add_issue(issue)
        
        # 检查日期间隔（工作日）
        date_diffs = dates.diff().dt.days
        # 正常工作日间隔应该是1-3天（考虑周末）
        abnormal_gaps = date_diffs > 7  # 超过一周的间隔
        if abnormal_gaps.any():
            issue = QualityIssue(
                type="large_date_gaps",
                severity="MEDIUM",
                description="发现异常大的日期间隔（超过7天）",
                affected_rows=data[abnormal_gaps].index.tolist(),
                affected_columns=['trade_date']
            )
            report.add_issue(issue)
        
        # 计算质量分数
        if report.issues:
            penalty = sum(10 if issue.severity == 'CRITICAL' else 
                         5 if issue.severity == 'HIGH' else 
                         2 if issue.severity == 'MEDIUM' else 1 
                         for issue in report.issues)
            report.quality_score = max(0, 100 - penalty)
        
        return report


class DuplicateChecker(BaseQualityChecker):
    """重复数据检查器"""
    
    def __init__(self, key_columns: List[str] = None):
        super().__init__("DuplicateChecker")
        self.key_columns = key_columns or ['ts_code', 'trade_date']
    
    def check(self, data: pd.DataFrame) -> QualityReport:
        """检查重复数据"""
        report = QualityReport(
            passed=True,
            issues=[],
            total_rows=len(data),
            total_columns=len(data.columns),
            quality_score=100.0
        )
        
        # 检查可用的键列
        available_key_cols = [col for col in self.key_columns if col in data.columns]
        
        if not available_key_cols:
            return report
        
        # 检查基于键列的重复
        duplicates = data.duplicated(subset=available_key_cols, keep=False)
        if duplicates.any():
            issue = QualityIssue(
                type="duplicate_records",
                severity="HIGH",
                description=f"发现基于 {available_key_cols} 的重复记录",
                affected_rows=data[duplicates].index.tolist(),
                affected_columns=available_key_cols
            )
            report.add_issue(issue)
        
        # 检查完全重复的行
        full_duplicates = data.duplicated(keep=False)
        if full_duplicates.any():
            issue = QualityIssue(
                type="full_duplicate_records",
                severity="CRITICAL",
                description="发现完全重复的记录",
                affected_rows=data[full_duplicates].index.tolist()
            )
            report.add_issue(issue)
        
        # 计算质量分数
        if report.issues:
            penalty = sum(10 if issue.severity == 'CRITICAL' else 
                         5 if issue.severity == 'HIGH' else 
                         2 if issue.severity == 'MEDIUM' else 1 
                         for issue in report.issues)
            report.quality_score = max(0, 100 - penalty)
        
        return report


class MissingDataChecker(BaseQualityChecker):
    """缺失数据检查器"""
    
    def __init__(self, critical_columns: List[str] = None):
        super().__init__("MissingDataChecker")
        self.critical_columns = critical_columns or ['open', 'high', 'low', 'close', 'volume']
    
    def check(self, data: pd.DataFrame) -> QualityReport:
        """检查缺失数据"""
        report = QualityReport(
            passed=True,
            issues=[],
            total_rows=len(data),
            total_columns=len(data.columns),
            quality_score=100.0
        )
        
        # 检查关键列的缺失值
        for col in self.critical_columns:
            if col not in data.columns:
                continue
                
            missing_mask = data[col].isna()
            if missing_mask.any():
                missing_count = missing_mask.sum()
                missing_rate = missing_count / len(data)
                
                severity = "CRITICAL" if missing_rate > 0.1 else \
                          "HIGH" if missing_rate > 0.05 else \
                          "MEDIUM" if missing_rate > 0.01 else "LOW"
                
                issue = QualityIssue(
                    type="missing_data",
                    severity=severity,
                    description=f"列 {col} 缺失 {missing_count} 个值 ({missing_rate:.2%})",
                    affected_rows=data[missing_mask].index.tolist(),
                    affected_columns=[col],
                    metadata={"missing_count": missing_count, "missing_rate": missing_rate}
                )
                report.add_issue(issue)
        
        # 检查完全空行
        empty_rows = data.isna().all(axis=1)
        if empty_rows.any():
            issue = QualityIssue(
                type="empty_rows",
                severity="HIGH",
                description=f"发现 {empty_rows.sum()} 个完全空行",
                affected_rows=data[empty_rows].index.tolist()
            )
            report.add_issue(issue)
        
        # 计算质量分数
        if report.issues:
            penalty = sum(10 if issue.severity == 'CRITICAL' else 
                         5 if issue.severity == 'HIGH' else 
                         2 if issue.severity == 'MEDIUM' else 1 
                         for issue in report.issues)
            report.quality_score = max(0, 100 - penalty)
        
        return report


class DataQualityManager:
    """数据质量管理器"""
    
    def __init__(self):
        self.checkers: List[BaseQualityChecker] = []
        self.setup_default_checkers()
    
    def setup_default_checkers(self):
        """设置默认检查器"""
        self.checkers = [
            PriceRangeChecker(),
            VolumeConsistencyChecker(),
            TimeSeriesChecker(),
            DuplicateChecker(),
            MissingDataChecker()
        ]
    
    def add_checker(self, checker: BaseQualityChecker):
        """添加检查器"""
        self.checkers.append(checker)
    
    def remove_checker(self, checker_name: str):
        """移除检查器"""
        self.checkers = [c for c in self.checkers if c.name != checker_name]
    
    def check_quality(self, data: pd.DataFrame) -> QualityReport:
        """执行完整的数据质量检查"""
        overall_report = QualityReport(
            passed=True,
            issues=[],
            total_rows=len(data),
            total_columns=len(data.columns),
            quality_score=100.0
        )
        
        all_scores = []
        
        for checker in self.checkers:
            try:
                report = checker.check(data)
                overall_report.issues.extend(report.issues)
                all_scores.append(report.quality_score)
                
                # 如果有严重问题，整体检查失败
                if not report.passed:
                    overall_report.passed = False
                    
            except Exception as e:
                logger.error(f"质量检查器 {checker.name} 执行失败: {e}")
                issue = QualityIssue(
                    type="checker_error",
                    severity="HIGH",
                    description=f"质量检查器 {checker.name} 执行失败: {e}"
                )
                overall_report.add_issue(issue)
                all_scores.append(50.0)  # 检查器失败时给予中等分数
        
        # 计算综合质量分数
        if all_scores:
            overall_report.quality_score = sum(all_scores) / len(all_scores)
        
        logger.info(f"数据质量检查完成，总分: {overall_report.quality_score:.1f}, "
                   f"问题数: {len(overall_report.issues)}")
        
        return overall_report
    
    def get_quality_summary(self, report: QualityReport) -> Dict[str, Any]:
        """获取质量摘要"""
        issue_counts = {}
        for issue in report.issues:
            issue_counts[issue.severity] = issue_counts.get(issue.severity, 0) + 1
        
        return {
            "overall_passed": report.passed,
            "quality_score": report.quality_score,
            "total_issues": len(report.issues),
            "issue_breakdown": issue_counts,
            "data_shape": (report.total_rows, report.total_columns)
        }