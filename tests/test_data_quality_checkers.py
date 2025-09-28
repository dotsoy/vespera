"""
数据质量检查器测试用例
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from src.data_sources.data_quality_checkers import (
    PriceRangeChecker,
    VolumeConsistencyChecker,
    TimeSeriesChecker,
    DuplicateChecker,
    MissingDataChecker,
    DataQualityManager,
    QualityIssue,
    QualityReport
)


@pytest.fixture
def good_data():
    """创建高质量的测试数据"""
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
def bad_price_data():
    """创建价格异常的测试数据"""
    return pd.DataFrame({
        'ts_code': ['000001.SZ'] * 5,
        'trade_date': pd.date_range('2024-01-01', periods=5),
        'open': [-1.0, 10.1, 15000.0, 10.3, 0.001],  # 负价格、异常高价格、异常低价格
        'high': [10.5, 10.6, 15500.0, 10.8, 10.9],
        'low': [9.5, 9.6, 14500.0, 9.8, 9.9],
        'close': [10.2, 10.3, 15200.0, 10.5, 10.6],
        'volume': [1000, 1100, 1200, 1300, 1400]
    })


@pytest.fixture
def bad_volume_data():
    """创建成交量异常的测试数据"""
    return pd.DataFrame({
        'ts_code': ['000001.SZ'] * 10,
        'trade_date': pd.date_range('2024-01-01', periods=10),
        'open': [10.0] * 10,
        'high': [10.5] * 10,
        'low': [9.5] * 10,
        'close': [10.2] * 10,
        'volume': [-100, 1000, 0, 0, 0, 50000, 1200, 1300, 1400, 1500]  # 负成交量、连续零成交量、异常大成交量
    })


@pytest.fixture
def bad_time_series_data():
    """创建时间序列异常的测试数据"""
    dates = [
        '2024-01-01',
        '2024-01-01',  # 重复日期
        '2024-01-03',
        '2024-01-02',  # 乱序
        '2024-01-15'   # 大间隔
    ]
    return pd.DataFrame({
        'ts_code': ['000001.SZ'] * 5,
        'trade_date': pd.to_datetime(dates),
        'open': [10.0, 10.1, 10.2, 10.3, 10.4],
        'high': [10.5, 10.6, 10.7, 10.8, 10.9],
        'low': [9.5, 9.6, 9.7, 9.8, 9.9],
        'close': [10.2, 10.3, 10.4, 10.5, 10.6],
        'volume': [1000, 1100, 1200, 1300, 1400]
    })


@pytest.fixture
def duplicate_data():
    """创建重复数据"""
    data = pd.DataFrame({
        'ts_code': ['000001.SZ'] * 6,
        'trade_date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-03', '2024-01-04', '2024-01-04'],
        'open': [10.0, 10.1, 10.0, 10.3, 10.4, 10.4],
        'high': [10.5, 10.6, 10.5, 10.8, 10.9, 10.9],
        'low': [9.5, 9.6, 9.5, 9.8, 9.9, 9.9],
        'close': [10.2, 10.3, 10.2, 10.5, 10.6, 10.6],
        'volume': [1000, 1100, 1000, 1300, 1400, 1400]
    })
    return data


@pytest.fixture
def missing_data():
    """创建缺失数据"""
    return pd.DataFrame({
        'ts_code': ['000001.SZ'] * 5,
        'trade_date': pd.date_range('2024-01-01', periods=5),
        'open': [10.0, np.nan, 10.2, 10.3, np.nan],
        'high': [10.5, 10.6, np.nan, 10.8, 10.9],
        'low': [9.5, 9.6, 9.7, np.nan, 9.9],
        'close': [10.2, 10.3, 10.4, 10.5, np.nan],
        'volume': [1000, np.nan, 1200, 1300, 1400]
    })


class TestPriceRangeChecker:
    """价格范围检查器测试"""
    
    def test_good_data(self, good_data):
        """测试正常数据"""
        checker = PriceRangeChecker()
        report = checker.check(good_data)
        
        assert report.passed
        assert len(report.issues) == 0
        assert report.quality_score == 100.0
    
    def test_bad_price_data(self, bad_price_data):
        """测试异常价格数据"""
        checker = PriceRangeChecker()
        report = checker.check(bad_price_data)
        
        assert not report.passed
        assert len(report.issues) > 0
        assert report.quality_score < 100.0
        
        # 检查是否检测到负价格
        negative_price_issues = [issue for issue in report.issues if issue.type == "negative_price"]
        assert len(negative_price_issues) > 0
        
        # 检查是否检测到异常高价格
        high_price_issues = [issue for issue in report.issues if issue.type == "abnormal_high_price"]
        assert len(high_price_issues) > 0
    
    def test_custom_price_range(self):
        """测试自定义价格范围"""
        checker = PriceRangeChecker(min_price=5.0, max_price=50.0)
        
        data = pd.DataFrame({
            'open': [1.0, 10.0, 100.0],  # 低于最小值、正常、高于最大值
            'close': [2.0, 20.0, 200.0]
        })
        
        report = checker.check(data)
        assert not report.passed
        assert len(report.issues) >= 2  # 至少有低价格和高价格问题


class TestVolumeConsistencyChecker:
    """成交量一致性检查器测试"""
    
    def test_good_data(self, good_data):
        """测试正常数据"""
        checker = VolumeConsistencyChecker()
        report = checker.check(good_data)
        
        assert report.passed
        assert len(report.issues) == 0
        assert report.quality_score == 100.0
    
    def test_bad_volume_data(self, bad_volume_data):
        """测试异常成交量数据"""
        checker = VolumeConsistencyChecker()
        report = checker.check(bad_volume_data)
        
        assert not report.passed
        assert len(report.issues) > 0
        
        # 检查是否检测到负成交量
        negative_volume_issues = [issue for issue in report.issues if issue.type == "negative_volume"]
        assert len(negative_volume_issues) > 0
        
        # 检查是否检测到连续零成交量
        zero_volume_issues = [issue for issue in report.issues if issue.type == "consecutive_zero_volume"]
        assert len(zero_volume_issues) > 0
    
    def test_no_volume_column(self):
        """测试没有成交量列的数据"""
        data = pd.DataFrame({
            'open': [10.0, 10.1],
            'close': [10.2, 10.3]
        })
        
        checker = VolumeConsistencyChecker()
        report = checker.check(data)
        
        assert report.passed  # 没有成交量列时应该通过
        assert len(report.issues) == 0


class TestTimeSeriesChecker:
    """时间序列检查器测试"""
    
    def test_good_data(self, good_data):
        """测试正常数据"""
        checker = TimeSeriesChecker()
        report = checker.check(good_data)
        
        assert report.passed
        assert len(report.issues) == 0
        assert report.quality_score == 100.0
    
    def test_bad_time_series_data(self, bad_time_series_data):
        """测试异常时间序列数据"""
        checker = TimeSeriesChecker()
        report = checker.check(bad_time_series_data)
        
        assert not report.passed
        assert len(report.issues) > 0
        
        # 检查问题类型
        issue_types = [issue.type for issue in report.issues]
        assert "duplicate_dates" in issue_types
        assert "unsorted_dates" in issue_types
        assert "large_date_gaps" in issue_types
    
    def test_no_date_column(self):
        """测试没有日期列的数据"""
        data = pd.DataFrame({
            'open': [10.0, 10.1],
            'close': [10.2, 10.3]
        })
        
        checker = TimeSeriesChecker()
        report = checker.check(data)
        
        assert report.passed  # 没有日期列时应该通过
        assert len(report.issues) == 0


class TestDuplicateChecker:
    """重复数据检查器测试"""
    
    def test_good_data(self, good_data):
        """测试正常数据"""
        checker = DuplicateChecker()
        report = checker.check(good_data)
        
        assert report.passed
        assert len(report.issues) == 0
        assert report.quality_score == 100.0
    
    def test_duplicate_data(self, duplicate_data):
        """测试重复数据"""
        checker = DuplicateChecker()
        report = checker.check(duplicate_data)
        
        assert not report.passed
        assert len(report.issues) > 0
        
        # 检查是否检测到重复记录
        duplicate_issues = [issue for issue in report.issues if issue.type == "duplicate_records"]
        assert len(duplicate_issues) > 0
    
    def test_custom_key_columns(self):
        """测试自定义键列"""
        checker = DuplicateChecker(key_columns=['ts_code'])
        
        data = pd.DataFrame({
            'ts_code': ['000001.SZ', '000001.SZ'],  # 重复的股票代码
            'trade_date': ['2024-01-01', '2024-01-02'],  # 不同的日期
            'close': [10.0, 10.1]
        })
        
        report = checker.check(data)
        assert not report.passed


class TestMissingDataChecker:
    """缺失数据检查器测试"""
    
    def test_good_data(self, good_data):
        """测试正常数据"""
        checker = MissingDataChecker()
        report = checker.check(good_data)
        
        assert report.passed
        assert len(report.issues) == 0
        assert report.quality_score == 100.0
    
    def test_missing_data(self, missing_data):
        """测试缺失数据"""
        checker = MissingDataChecker()
        report = checker.check(missing_data)
        
        assert not report.passed
        assert len(report.issues) > 0
        
        # 检查是否检测到缺失数据
        missing_issues = [issue for issue in report.issues if issue.type == "missing_data"]
        assert len(missing_issues) > 0
    
    def test_empty_rows(self):
        """测试完全空行"""
        data = pd.DataFrame({
            'open': [10.0, np.nan, 10.2],
            'high': [10.5, np.nan, 10.7],
            'low': [9.5, np.nan, 9.7],
            'close': [10.2, np.nan, 10.4],
            'volume': [1000, np.nan, 1200]
        })
        
        checker = MissingDataChecker()
        report = checker.check(data)
        
        assert not report.passed
        empty_row_issues = [issue for issue in report.issues if issue.type == "empty_rows"]
        assert len(empty_row_issues) > 0


class TestDataQualityManager:
    """数据质量管理器测试"""
    
    def test_default_checkers(self):
        """测试默认检查器"""
        manager = DataQualityManager()
        assert len(manager.checkers) == 5  # 默认有5个检查器
    
    def test_add_remove_checker(self):
        """测试添加和移除检查器"""
        manager = DataQualityManager()
        initial_count = len(manager.checkers)
        
        # 添加检查器
        custom_checker = PriceRangeChecker()
        custom_checker.name = "custom_price_checker"
        manager.add_checker(custom_checker)
        assert len(manager.checkers) == initial_count + 1
        
        # 移除检查器
        manager.remove_checker("custom_price_checker")
        assert len(manager.checkers) == initial_count
    
    def test_check_quality_good_data(self, good_data):
        """测试高质量数据的完整检查"""
        manager = DataQualityManager()
        report = manager.check_quality(good_data)
        
        assert report.passed
        assert len(report.issues) == 0
        assert report.quality_score == 100.0
    
    def test_check_quality_bad_data(self, bad_price_data):
        """测试低质量数据的完整检查"""
        manager = DataQualityManager()
        report = manager.check_quality(bad_price_data)
        
        assert not report.passed
        assert len(report.issues) > 0
        assert report.quality_score < 100.0
    
    def test_get_quality_summary(self, good_data):
        """测试质量摘要"""
        manager = DataQualityManager()
        report = manager.check_quality(good_data)
        summary = manager.get_quality_summary(report)
        
        assert "overall_passed" in summary
        assert "quality_score" in summary
        assert "total_issues" in summary
        assert "issue_breakdown" in summary
        assert "data_shape" in summary
        
        assert summary["overall_passed"] == True
        assert summary["quality_score"] == 100.0
        assert summary["total_issues"] == 0
    
    def test_checker_exception_handling(self):
        """测试检查器异常处理"""
        manager = DataQualityManager()
        
        # 添加一个会抛出异常的检查器
        class FailingChecker:
            def __init__(self):
                self.name = "failing_checker"
            
            def check(self, data):
                raise Exception("检查器故意失败")
        
        manager.add_checker(FailingChecker())
        
        data = pd.DataFrame({'test': [1, 2, 3]})
        report = manager.check_quality(data)
        
        # 应该有一个检查器错误
        checker_error_issues = [issue for issue in report.issues if issue.type == "checker_error"]
        assert len(checker_error_issues) > 0


class TestQualityIssue:
    """质量问题测试"""
    
    def test_quality_issue_creation(self):
        """测试质量问题创建"""
        issue = QualityIssue(
            type="test_issue",
            severity="HIGH",
            description="测试问题",
            affected_rows=[0, 1, 2],
            affected_columns=["col1", "col2"],
            metadata={"key": "value"}
        )
        
        assert issue.type == "test_issue"
        assert issue.severity == "HIGH"
        assert issue.description == "测试问题"
        assert issue.affected_rows == [0, 1, 2]
        assert issue.affected_columns == ["col1", "col2"]
        assert issue.metadata == {"key": "value"}


class TestQualityReport:
    """质量报告测试"""
    
    def test_quality_report_creation(self):
        """测试质量报告创建"""
        report = QualityReport(
            passed=True,
            issues=[],
            total_rows=100,
            total_columns=5,
            quality_score=95.0
        )
        
        assert report.passed == True
        assert len(report.issues) == 0
        assert report.total_rows == 100
        assert report.total_columns == 5
        assert report.quality_score == 95.0
    
    def test_add_issue(self):
        """测试添加问题"""
        report = QualityReport(
            passed=True,
            issues=[],
            total_rows=100,
            total_columns=5,
            quality_score=95.0
        )
        
        # 添加高严重性问题
        high_issue = QualityIssue(
            type="test_issue",
            severity="HIGH",
            description="高严重性问题"
        )
        report.add_issue(high_issue)
        
        assert not report.passed  # 应该变为失败
        assert len(report.issues) == 1
        
        # 添加低严重性问题
        low_issue = QualityIssue(
            type="test_issue",
            severity="LOW",
            description="低严重性问题"
        )
        report.add_issue(low_issue)
        
        assert len(report.issues) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])