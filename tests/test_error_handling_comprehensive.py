"""
全面错误处理测试
测试各种异常情况、边界条件和容错机制
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path
import logging

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.analyzers.technical_analyzer import TechnicalAnalyzer
from src.analyzers.capital_flow_analyzer import CapitalFlowAnalyzer
from src.data_sources.akshare_data_source import AkShareDataSource
from src.strategies.qiming_star.qiming_star_strategy import QimingStarStrategy


class TestTechnicalAnalyzerErrorHandling:
    """技术分析器错误处理测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = TechnicalAnalyzer()
    
    def test_database_connection_failure(self):
        """测试数据库连接失败时的处理"""
        with patch('src.utils.database.get_db_manager') as mock_db:
            mock_db.side_effect = Exception("数据库连接失败")
            
            # 应该能够优雅处理数据库连接失败
            analyzer = TechnicalAnalyzer()
            assert analyzer is not None
    
    def test_invalid_data_types(self):
        """测试无效数据类型处理"""
        # 测试字符串数据
        string_data = pd.DataFrame({
            'close_price': ['invalid', 'data', 'here'],
            'volume': ['also', 'invalid', 'data']
        })
        
        with pytest.raises(Exception):
            self.analyzer.calculate_indicators(string_data)
    
    def test_extreme_values(self):
        """测试极端值处理"""
        # 测试极大值
        extreme_data = pd.DataFrame({
            'close_price': [1e10, 1e20, 1e30],
            'volume': [1e15, 1e25, 1e35],
            'high_price': [1e10, 1e20, 1e30],
            'low_price': [1e9, 1e19, 1e29],
            'open_price': [1e9, 1e19, 1e29]
        })
        
        # 应该能够处理极端值而不崩溃
        result = self.analyzer.calculate_indicators(extreme_data)
        assert isinstance(result, pd.DataFrame)
    
    def test_negative_values(self):
        """测试负值处理"""
        negative_data = pd.DataFrame({
            'close_price': [-10, -20, -30],
            'volume': [-1000, -2000, -3000],
            'high_price': [-5, -15, -25],
            'low_price': [-15, -25, -35],
            'open_price': [-12, -22, -32]
        })
        
        # 应该能够处理负值
        result = self.analyzer.calculate_indicators(negative_data)
        assert isinstance(result, pd.DataFrame)
    
    def test_zero_values(self):
        """测试零值处理"""
        zero_data = pd.DataFrame({
            'close_price': [0, 0, 0],
            'volume': [0, 0, 0],
            'high_price': [0, 0, 0],
            'low_price': [0, 0, 0],
            'open_price': [0, 0, 0]
        })
        
        # 应该能够处理零值
        result = self.analyzer.calculate_indicators(zero_data)
        assert isinstance(result, pd.DataFrame)
    
    def test_missing_required_columns(self):
        """测试缺失必需列的处理"""
        incomplete_data = pd.DataFrame({
            'close_price': [10, 11, 12]
            # 缺少其他必需列
        })
        
        with pytest.raises(Exception):
            self.analyzer.calculate_indicators(incomplete_data)
    
    def test_duplicate_dates(self):
        """测试重复日期处理"""
        duplicate_data = pd.DataFrame({
            'trade_date': ['2024-01-01', '2024-01-01', '2024-01-01'],
            'close_price': [10, 11, 12],
            'volume': [1000, 1100, 1200],
            'high_price': [12, 13, 14],
            'low_price': [8, 9, 10],
            'open_price': [9, 10, 11]
        })
        
        # 应该能够处理重复日期
        result = self.analyzer.calculate_indicators(duplicate_data)
        assert isinstance(result, pd.DataFrame)
    
    def test_non_chronological_dates(self):
        """测试非按时间顺序的日期处理"""
        mixed_dates = pd.DataFrame({
            'trade_date': ['2024-01-03', '2024-01-01', '2024-01-02'],
            'close_price': [12, 10, 11],
            'volume': [1200, 1000, 1100],
            'high_price': [14, 12, 13],
            'low_price': [10, 8, 9],
            'open_price': [11, 9, 10]
        })
        
        # 应该能够处理非按时间顺序的日期
        result = self.analyzer.calculate_indicators(mixed_dates)
        assert isinstance(result, pd.DataFrame)


class TestCapitalFlowAnalyzerErrorHandling:
    """资金流分析器错误处理测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = CapitalFlowAnalyzer()
    
    def test_insufficient_data_for_analysis(self):
        """测试数据不足时的处理"""
        minimal_data = pd.DataFrame({
            'main_net_inflow': [1000],
            'total_amount': [10000],
            'super_large_net_inflow': [500],
            'retail_net_inflow': [200]
        })
        
        # 应该能够处理最小数据集
        score = self.analyzer.calculate_main_force_score(minimal_data)
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_zero_total_amount(self):
        """测试总金额为零时的处理"""
        zero_amount_data = pd.DataFrame({
            'main_net_inflow': [1000, 2000, 3000],
            'total_amount': [0, 0, 0],
            'super_large_net_inflow': [500, 1000, 1500],
            'retail_net_inflow': [200, 400, 600]
        })
        
        # 应该能够处理总金额为零的情况
        score = self.analyzer.calculate_main_force_score(zero_amount_data)
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_negative_flow_values(self):
        """测试负资金流值处理"""
        negative_flow_data = pd.DataFrame({
            'main_net_inflow': [-1000, -2000, -3000],
            'total_amount': [10000, 20000, 30000],
            'super_large_net_inflow': [-500, -1000, -1500],
            'retail_net_inflow': [-200, -400, -600]
        })
        
        # 应该能够处理负资金流
        score = self.analyzer.calculate_main_force_score(negative_flow_data)
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_extremely_large_flow_values(self):
        """测试极大资金流值处理"""
        large_flow_data = pd.DataFrame({
            'main_net_inflow': [1e15, 1e16, 1e17],
            'total_amount': [1e16, 1e17, 1e18],
            'super_large_net_inflow': [1e14, 1e15, 1e16],
            'retail_net_inflow': [1e13, 1e14, 1e15]
        })
        
        # 应该能够处理极大值
        score = self.analyzer.calculate_main_force_score(large_flow_data)
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0


class TestDataSourceErrorHandling:
    """数据源错误处理测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.data_source = AkShareDataSource()
    
    def test_network_timeout(self):
        """测试网络超时处理"""
        with patch('akshare.stock_zh_a_hist') as mock_akshare:
            mock_akshare.side_effect = Exception("网络超时")
            
            # 应该能够处理网络超时
            result = self.data_source.get_stock_data("000001.SZ", "2024-01-01", "2024-01-31")
            assert result is None or isinstance(result, pd.DataFrame)
    
    def test_invalid_stock_code(self):
        """测试无效股票代码处理"""
        # 测试不存在的股票代码
        result = self.data_source.get_stock_data("INVALID.CODE", "2024-01-01", "2024-01-31")
        assert result is None or isinstance(result, pd.DataFrame)
    
    def test_invalid_date_range(self):
        """测试无效日期范围处理"""
        # 测试结束日期早于开始日期
        result = self.data_source.get_stock_data("000001.SZ", "2024-01-31", "2024-01-01")
        assert result is None or isinstance(result, pd.DataFrame)
    
    def test_future_dates(self):
        """测试未来日期处理"""
        from datetime import datetime, timedelta
        
        future_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        result = self.data_source.get_stock_data("000001.SZ", "2024-01-01", future_date)
        assert result is None or isinstance(result, pd.DataFrame)


class TestStrategyErrorHandling:
    """策略错误处理测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.strategy = QimingStarStrategy()
    
    def test_empty_stock_data(self):
        """测试空股票数据处理"""
        empty_data = pd.DataFrame()
        
        result = self.strategy.analyze_stock("000001.SZ", empty_data)
        assert result is None or isinstance(result, dict)
    
    def test_invalid_stock_code_format(self):
        """测试无效股票代码格式处理"""
        valid_data = pd.DataFrame({
            'close': [10, 11, 12],
            'open': [9, 10, 11],
            'high': [12, 13, 14],
            'low': [8, 9, 10],
            'volume': [1000, 1100, 1200]
        })
        
        # 测试各种无效格式
        invalid_codes = ["", "INVALID", "000001", "000001.SZ.INVALID"]
        
        for code in invalid_codes:
            result = self.strategy.analyze_stock(code, valid_data)
            assert result is None or isinstance(result, dict)
    
    def test_missing_required_columns_in_stock_data(self):
        """测试股票数据缺少必需列的处理"""
        incomplete_data = pd.DataFrame({
            'close': [10, 11, 12]
            # 缺少其他必需列
        })
        
        result = self.strategy.analyze_stock("000001.SZ", incomplete_data)
        assert result is None or isinstance(result, dict)
    
    def test_strategy_config_validation(self):
        """测试策略配置验证"""
        # 测试无效配置
        invalid_configs = [
            {"weights": {"invalid": 0.5}},
            {"thresholds": {"invalid": 100}},
            {"initial_capital": -1000},
            {"weights": {"technical": 2.0}}  # 权重超过1.0
        ]
        
        for config in invalid_configs:
            try:
                strategy = QimingStarStrategy(config)
                assert strategy is not None
            except Exception:
                # 配置验证失败是预期的
                pass


class TestBoundaryConditions:
    """边界条件测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.technical_analyzer = TechnicalAnalyzer()
        self.capital_flow_analyzer = CapitalFlowAnalyzer()
    
    def test_single_record_analysis(self):
        """测试单条记录分析"""
        single_record = pd.DataFrame({
            'close_price': [15.0],
            'volume': [1000],
            'high_price': [16.0],
            'low_price': [14.0],
            'open_price': [14.5]
        })
        
        # 技术分析应该能够处理单条记录
        result = self.technical_analyzer.calculate_indicators(single_record)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
    
    def test_maximum_data_size(self):
        """测试最大数据量处理"""
        # 创建大量数据
        large_data = pd.DataFrame({
            'close_price': np.random.uniform(10, 20, 10000),
            'volume': np.random.uniform(1000000, 5000000, 10000),
            'high_price': np.random.uniform(15, 25, 10000),
            'low_price': np.random.uniform(5, 15, 10000),
            'open_price': np.random.uniform(10, 20, 10000)
        })
        
        # 应该能够处理大量数据
        result = self.technical_analyzer.calculate_indicators(large_data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 10000
    
    def test_memory_efficiency(self):
        """测试内存效率"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # 创建中等大小的数据
        medium_data = pd.DataFrame({
            'close_price': np.random.uniform(10, 20, 1000),
            'volume': np.random.uniform(1000000, 5000000, 1000),
            'high_price': np.random.uniform(15, 25, 1000),
            'low_price': np.random.uniform(5, 15, 1000),
            'open_price': np.random.uniform(10, 20, 1000)
        })
        
        # 执行多次计算
        for _ in range(10):
            result = self.technical_analyzer.calculate_indicators(medium_data)
            del result  # 显式删除结果
        
        final_memory = process.memory_info().rss
        memory_increase = (final_memory - initial_memory) / 1024 / 1024  # MB
        
        # 内存增长应该合理（小于100MB）
        assert memory_increase < 100


class TestPerformanceUnderStress:
    """压力测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.technical_analyzer = TechnicalAnalyzer()
    
    def test_concurrent_analysis(self):
        """测试并发分析"""
        import threading
        import time
        
        test_data = pd.DataFrame({
            'close_price': np.random.uniform(10, 20, 100),
            'volume': np.random.uniform(1000000, 5000000, 100),
            'high_price': np.random.uniform(15, 25, 100),
            'low_price': np.random.uniform(5, 15, 100),
            'open_price': np.random.uniform(10, 20, 100)
        })
        
        results = []
        errors = []
        
        def analyze_stock():
            try:
                result = self.technical_analyzer.calculate_indicators(test_data)
                results.append(result)
            except Exception as e:
                errors.append(e)
        
        # 创建多个线程并发执行
        threads = []
        start_time = time.time()
        
        for _ in range(5):
            thread = threading.Thread(target=analyze_stock)
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # 验证结果
        assert len(results) == 5  # 所有线程都应该成功
        assert len(errors) == 0   # 不应该有错误
        assert execution_time < 10  # 执行时间应该合理
    
    def test_large_batch_processing(self):
        """测试大批量处理"""
        # 创建多个股票的数据
        stock_data_dict = {}
        
        for i in range(100):
            stock_code = f"00000{i:03d}.SZ"
            stock_data_dict[stock_code] = pd.DataFrame({
                'close_price': np.random.uniform(10, 20, 50),
                'volume': np.random.uniform(1000000, 5000000, 50),
                'high_price': np.random.uniform(15, 25, 50),
                'low_price': np.random.uniform(5, 15, 50),
                'open_price': np.random.uniform(10, 20, 50)
            })
        
        # 批量处理
        start_time = time.time()
        
        for stock_code, data in stock_data_dict.items():
            try:
                result = self.technical_analyzer.calculate_indicators(data)
                assert isinstance(result, pd.DataFrame)
            except Exception as e:
                pytest.fail(f"处理股票 {stock_code} 时失败: {e}")
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # 验证性能
        assert execution_time < 30  # 100只股票应该在30秒内完成


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 