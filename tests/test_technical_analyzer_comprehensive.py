"""
技术分析器全面单元测试
测试所有功能模块、边界条件和错误处理
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.analyzers.technical_analyzer import TechnicalAnalyzer
from src.utils.technical_indicators import add_all_indicators


class TestTechnicalAnalyzerInitialization:
    """技术分析器初始化测试"""
    
    def test_analyzer_initialization(self):
        """测试分析器正常初始化"""
        analyzer = TechnicalAnalyzer()
        assert analyzer is not None
        assert hasattr(analyzer, 'db_manager')
        assert hasattr(analyzer, 'ma_periods')
        assert hasattr(analyzer, 'ema_periods')
        assert hasattr(analyzer, 'rsi_period')
    
    def test_analyzer_with_custom_config(self):
        """测试自定义配置初始化"""
        custom_config = {
            'ma_periods': [5, 10, 20],
            'ema_periods': [12, 26],
            'rsi_period': 14
        }
        
        with patch('config.settings.analysis_settings') as mock_settings:
            mock_settings.ma_periods = custom_config['ma_periods']
            mock_settings.ema_periods = custom_config['ema_periods']
            mock_settings.rsi_period = custom_config['rsi_period']
            
            analyzer = TechnicalAnalyzer()
            assert analyzer.ma_periods == custom_config['ma_periods']
            assert analyzer.ema_periods == custom_config['ema_periods']
            assert analyzer.rsi_period == custom_config['rsi_period']


class TestTechnicalIndicatorCalculation:
    """技术指标计算测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = TechnicalAnalyzer()
        
        # 创建标准测试数据
        self.standard_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=50, freq='D'),
            'open_price': np.random.uniform(10, 20, 50),
            'high_price': np.random.uniform(15, 25, 50),
            'low_price': np.random.uniform(5, 15, 50),
            'close_price': np.random.uniform(10, 20, 50),
            'volume': np.random.uniform(1000000, 5000000, 50),
        })
    
    def test_calculate_indicators_success(self):
        """测试技术指标计算成功"""
        result = self.analyzer.calculate_indicators(self.standard_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(self.standard_data)
        assert 'ma_5' in result.columns
        assert 'ema_12' in result.columns
        assert 'rsi' in result.columns
        assert 'macd' in result.columns
    
    def test_calculate_indicators_empty_data(self):
        """测试空数据处理"""
        empty_df = pd.DataFrame()
        result = self.analyzer.calculate_indicators(empty_df)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_calculate_indicators_single_record(self):
        """测试单条记录处理"""
        single_record = self.standard_data.iloc[:1]
        result = self.analyzer.calculate_indicators(single_record)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
    
    def test_calculate_indicators_missing_columns(self):
        """测试缺失列处理"""
        incomplete_data = self.standard_data[['trade_date', 'close_price']]
        
        with pytest.raises(Exception):
            self.analyzer.calculate_indicators(incomplete_data)
    
    def test_calculate_indicators_with_nan_values(self):
        """测试包含NaN值的数据"""
        data_with_nan = self.standard_data.copy()
        data_with_nan.loc[5:10, 'close_price'] = np.nan
        
        result = self.analyzer.calculate_indicators(data_with_nan)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(data_with_nan)


class TestTrendScoreCalculation:
    """趋势评分计算测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = TechnicalAnalyzer()
        
        # 创建包含技术指标的测试数据
        self.test_data = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
            'ema_12': [14.5, 15.5, 16.5, 17.5, 18.5],
            'ema_26': [14.0, 15.0, 16.0, 17.0, 18.0],
            'ma_5': [14.8, 15.8, 16.8, 17.8, 18.8],
            'ma_10': [14.2, 15.2, 16.2, 17.2, 18.2],
            'ma_20': [13.5, 14.5, 15.5, 16.5, 17.5],
            'bb_upper': [20.0, 21.0, 22.0, 23.0, 24.0],
            'bb_middle': [15.0, 16.0, 17.0, 18.0, 19.0],
            'macd': [0.5, 0.6, 0.7, 0.8, 0.9],
            'macd_signal': [0.3, 0.4, 0.5, 0.6, 0.7],
        })
    
    def test_trend_score_ideal_conditions(self):
        """测试理想条件下的趋势评分"""
        score = self.analyzer.calculate_trend_score(self.test_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 理想条件下应该得到较高分数
        assert score > 0.5
    
    def test_trend_score_empty_data(self):
        """测试空数据趋势评分"""
        empty_df = pd.DataFrame()
        score = self.analyzer.calculate_trend_score(empty_df)
        
        assert score == 0.0
    
    def test_trend_score_missing_indicators(self):
        """测试缺失指标的趋势评分"""
        data_missing_indicators = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0],
        })
        
        score = self.analyzer.calculate_trend_score(data_missing_indicators)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_trend_score_with_nan_values(self):
        """测试包含NaN值的趋势评分"""
        data_with_nan = self.test_data.copy()
        data_with_nan.loc[2, 'ema_12'] = np.nan
        data_with_nan.loc[3, 'ma_5'] = np.nan
        
        score = self.analyzer.calculate_trend_score(data_with_nan)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_trend_score_price_fallback(self):
        """测试价格趋势备用计算"""
        data_only_price = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
        })
        
        score = self.analyzer.calculate_trend_score(data_only_price)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0


class TestMomentumScoreCalculation:
    """动量评分计算测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = TechnicalAnalyzer()
        
        # 创建包含动量指标的测试数据
        self.test_data = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
            'rsi': [55.0, 60.0, 65.0, 70.0, 75.0],
            'k': [60.0, 65.0, 70.0, 75.0, 80.0],
            'd': [55.0, 60.0, 65.0, 70.0, 75.0],
            'j': [65.0, 70.0, 75.0, 80.0, 85.0],
            'williams_r': [-30.0, -25.0, -20.0, -15.0, -10.0],
        })
    
    def test_momentum_score_ideal_conditions(self):
        """测试理想条件下的动量评分"""
        score = self.analyzer.calculate_momentum_score(self.test_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 理想条件下应该得到较高分数
        assert score > 0.3
    
    def test_momentum_score_empty_data(self):
        """测试空数据动量评分"""
        empty_df = pd.DataFrame()
        score = self.analyzer.calculate_momentum_score(empty_df)
        
        assert score == 0.0
    
    def test_momentum_score_missing_indicators(self):
        """测试缺失指标的动量评分"""
        data_missing_indicators = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0],
        })
        
        score = self.analyzer.calculate_momentum_score(data_missing_indicators)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_momentum_score_rsi_only(self):
        """测试只有RSI指标的动量评分"""
        data_rsi_only = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
            'rsi': [55.0, 60.0, 65.0, 70.0, 75.0],
        })
        
        score = self.analyzer.calculate_momentum_score(data_rsi_only)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_momentum_score_extreme_values(self):
        """测试极值条件下的动量评分"""
        data_extreme = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
            'rsi': [10.0, 90.0, 5.0, 95.0, 50.0],
            'k': [10.0, 90.0, 5.0, 95.0, 50.0],
            'd': [10.0, 90.0, 5.0, 95.0, 50.0],
            'j': [10.0, 90.0, 5.0, 95.0, 50.0],
            'williams_r': [-90.0, -10.0, -95.0, -5.0, -50.0],
        })
        
        score = self.analyzer.calculate_momentum_score(data_extreme)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0


class TestVolumeHealthScoreCalculation:
    """量能健康度评分测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = TechnicalAnalyzer()
        
        # 创建包含量能指标的测试数据
        self.test_data = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
            'volume': [1000000, 1200000, 1400000, 1600000, 1800000],
            'volume_ma': [1100000, 1150000, 1200000, 1250000, 1300000],
            'volume_ratio': [1.1, 1.2, 1.3, 1.4, 1.5],
            'obv': [1000000, 2200000, 3600000, 5200000, 7000000],
            'obv_ma': [1100000, 2200000, 3300000, 4400000, 5500000],
        })
    
    def test_volume_health_score_ideal_conditions(self):
        """测试理想条件下的量能健康度评分"""
        score = self.analyzer.calculate_volume_health_score(self.test_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_volume_health_score_empty_data(self):
        """测试空数据量能健康度评分"""
        empty_df = pd.DataFrame()
        score = self.analyzer.calculate_volume_health_score(empty_df)
        
        assert score == 0.0
    
    def test_volume_health_score_zero_volume(self):
        """测试零成交量的量能健康度评分"""
        data_zero_volume = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0],
            'volume': [0, 0, 0],
        })
        
        score = self.analyzer.calculate_volume_health_score(data_zero_volume)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0


class TestPatternRecognition:
    """形态识别测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = TechnicalAnalyzer()
    
    def test_identify_patterns_bullish_engulfing(self):
        """测试看涨吞没形态识别"""
        # 创建看涨吞没形态数据
        data = pd.DataFrame({
            'open_price': [16.0, 15.0],  # 前一天开盘价高，后一天开盘价低
            'high_price': [16.5, 16.5],  # 最高价相同
            'low_price': [15.0, 14.5],   # 前一天最低价高，后一天最低价低
            'close_price': [15.0, 16.0], # 前一天收盘价低，后一天收盘价高
            'volume': [1000000, 1200000],
        })
        
        patterns = self.analyzer.identify_patterns(data)
        
        assert isinstance(patterns, dict)
        assert 'patterns' in patterns
        assert 'confidence' in patterns
    
    def test_identify_patterns_insufficient_data(self):
        """测试数据不足的形态识别"""
        data = pd.DataFrame({
            'open_price': [15.0],
            'high_price': [16.0],
            'low_price': [14.0],
            'close_price': [15.5],
            'volume': [1000000],
        })
        
        patterns = self.analyzer.identify_patterns(data)
        
        assert isinstance(patterns, dict)
        assert 'patterns' in patterns
        assert 'confidence' in patterns
    
    def test_identify_patterns_empty_data(self):
        """测试空数据的形态识别"""
        empty_df = pd.DataFrame()
        
        patterns = self.analyzer.identify_patterns(empty_df)
        
        assert isinstance(patterns, dict)
        assert 'patterns' in patterns
        assert 'confidence' in patterns


class TestSupportResistanceCalculation:
    """支撑阻力位计算测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = TechnicalAnalyzer()
    
    def test_support_resistance_normal_data(self):
        """测试正常数据的支撑阻力位计算"""
        data = pd.DataFrame({
            'low_price': [14.0, 15.0, 16.0, 17.0, 18.0],
            'high_price': [16.0, 17.0, 18.0, 19.0, 20.0],
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
        })
        
        support, resistance = self.analyzer.calculate_support_resistance(data)
        
        assert support is None or isinstance(support, float)
        assert resistance is None or isinstance(resistance, float)
    
    def test_support_resistance_minimal_data(self):
        """测试最小数据的支撑阻力位计算"""
        data = pd.DataFrame({
            'low_price': [14.0, 15.0],
            'high_price': [16.0, 17.0],
            'close_price': [15.0, 16.0],
        })
        
        support, resistance = self.analyzer.calculate_support_resistance(data)
        
        assert support is None or isinstance(support, float)
        assert resistance is None or isinstance(resistance, float)
    
    def test_support_resistance_empty_data(self):
        """测试空数据的支撑阻力位计算"""
        empty_df = pd.DataFrame()
        
        support, resistance = self.analyzer.calculate_support_resistance(empty_df)
        
        assert support is None
        assert resistance is None


class TestCompleteStockAnalysis:
    """完整股票分析测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = TechnicalAnalyzer()
        
        # 创建完整的测试数据
        self.complete_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
            'open_price': np.random.uniform(10, 20, 30),
            'high_price': np.random.uniform(15, 25, 30),
            'low_price': np.random.uniform(5, 15, 30),
            'close_price': np.random.uniform(10, 20, 30),
            'volume': np.random.uniform(1000000, 5000000, 30),
        })
    
    def test_analyze_stock_success(self):
        """测试完整股票分析成功"""
        with patch.object(self.analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.return_value = self.complete_data
            
            result = self.analyzer.analyze_stock('000001.SZ', '2024-01-30')
            
            assert isinstance(result, dict)
            assert 'trend_score' in result
            assert 'momentum_score' in result
            assert 'volume_health_score' in result
            assert 'patterns' in result
            assert 'support_resistance' in result
            assert 'analysis_time' in result
    
    def test_analyze_stock_database_error(self):
        """测试数据库错误处理"""
        with patch.object(self.analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.side_effect = Exception("Database error")
            
            result = self.analyzer.analyze_stock('000001.SZ', '2024-01-30')
            
            assert result is None
    
    def test_analyze_stock_empty_data(self):
        """测试空数据分析"""
        with patch.object(self.analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.return_value = pd.DataFrame()
            
            result = self.analyzer.analyze_stock('000001.SZ', '2024-01-30')
            
            assert result is None


class TestPerformanceOptimization:
    """性能优化测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = TechnicalAnalyzer()
    
    def test_large_dataset_performance(self):
        """测试大数据集性能"""
        # 创建大数据集
        large_data = pd.DataFrame({
            'trade_date': pd.date_range('2020-01-01', periods=1000, freq='D'),
            'open_price': np.random.uniform(10, 20, 1000),
            'high_price': np.random.uniform(15, 25, 1000),
            'low_price': np.random.uniform(5, 15, 1000),
            'close_price': np.random.uniform(10, 20, 1000),
            'volume': np.random.uniform(1000000, 5000000, 1000),
        })
        
        import time
        start_time = time.time()
        
        result = self.analyzer.calculate_indicators(large_data)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1000
        # 确保处理时间在合理范围内（小于5秒）
        assert processing_time < 5.0
    
    def test_memory_efficiency(self):
        """测试内存效率"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # 创建中等大小数据集
        medium_data = pd.DataFrame({
            'trade_date': pd.date_range('2023-01-01', periods=500, freq='D'),
            'open_price': np.random.uniform(10, 20, 500),
            'high_price': np.random.uniform(15, 25, 500),
            'low_price': np.random.uniform(5, 15, 500),
            'close_price': np.random.uniform(10, 20, 500),
            'volume': np.random.uniform(1000000, 5000000, 500),
        })
        
        result = self.analyzer.calculate_indicators(medium_data)
        
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        assert isinstance(result, pd.DataFrame)
        # 确保内存增长在合理范围内（小于100MB）
        assert memory_increase < 100 * 1024 * 1024


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 