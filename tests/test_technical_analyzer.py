"""
技术分析器单元测试
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.analyzers.technical_analyzer import TechnicalAnalyzer


class TestTechnicalAnalyzer:
    """技术分析器测试类"""
    
    @pytest.fixture
    def analyzer(self):
        """创建技术分析器实例"""
        return TechnicalAnalyzer()
    
    @pytest.fixture
    def sample_data(self):
        """创建样本数据"""
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        data = {
            'trade_date': dates,
            'open_price': np.random.uniform(10, 20, 30),
            'high_price': np.random.uniform(15, 25, 30),
            'low_price': np.random.uniform(5, 15, 30),
            'close_price': np.random.uniform(10, 20, 30),
            'volume': np.random.uniform(1000000, 5000000, 30),
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def minimal_data(self):
        """创建最小数据集"""
        data = {
            'trade_date': ['2024-01-01'],
            'open_price': [15.0],
            'high_price': [16.0],
            'low_price': [14.0],
            'close_price': [15.5],
            'volume': [2000000],
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def empty_data(self):
        """创建空数据集"""
        return pd.DataFrame()
    
    def test_initialization(self, analyzer):
        """测试初始化"""
        assert analyzer is not None
        assert hasattr(analyzer, 'ma_periods')
        assert hasattr(analyzer, 'ema_periods')
        assert hasattr(analyzer, 'rsi_period')
    
    def test_calculate_indicators_with_valid_data(self, analyzer, sample_data):
        """测试有效数据的技术指标计算"""
        with patch('src.utils.technical_indicators.add_all_indicators') as mock_add:
            mock_add.return_value = sample_data
            result = analyzer.calculate_indicators(sample_data)
            
            assert result is not None
            assert len(result) == 30
            mock_add.assert_called_once()
    
    def test_calculate_indicators_with_empty_data(self, analyzer, empty_data):
        """测试空数据的技术指标计算"""
        with patch('src.utils.technical_indicators.add_all_indicators') as mock_add:
            mock_add.return_value = empty_data
            result = analyzer.calculate_indicators(empty_data)
            
            assert result is not None
            assert len(result) == 0
            mock_add.assert_called_once()
    
    def test_calculate_indicators_with_exception(self, analyzer, sample_data):
        """测试技术指标计算异常处理"""
        with patch('src.utils.technical_indicators.add_all_indicators') as mock_add:
            mock_add.side_effect = Exception("计算失败")
            
            with pytest.raises(Exception):
                analyzer.calculate_indicators(sample_data)
    
    def test_calculate_trend_score_with_valid_data(self, analyzer):
        """测试有效数据的趋势评分计算"""
        # 创建包含技术指标的测试数据
        data = {
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
            'ema_12': [14.5, 15.5, 16.5, 17.5, 18.5],
            'ema_26': [14.0, 15.0, 16.0, 17.0, 18.0],
            'ma_5': [14.8, 15.8, 16.8, 17.8, 18.8],
            'ma_10': [14.3, 15.3, 16.3, 17.3, 18.3],
            'ma_20': [14.0, 15.0, 16.0, 17.0, 18.0],
            'bb_upper': [20.0, 21.0, 22.0, 23.0, 24.0],
            'bb_middle': [15.0, 16.0, 17.0, 18.0, 19.0],
            'macd': [0.5, 0.6, 0.7, 0.8, 0.9],
            'macd_signal': [0.3, 0.4, 0.5, 0.6, 0.7],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_trend_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        assert score > 0.0  # 应该有正分数
    
    def test_calculate_trend_score_with_minimal_data(self, analyzer, minimal_data):
        """测试最小数据的趋势评分计算"""
        score = analyzer.calculate_trend_score(minimal_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_calculate_trend_score_with_empty_data(self, analyzer, empty_data):
        """测试空数据的趋势评分计算"""
        score = analyzer.calculate_trend_score(empty_data)
        
        assert score == 0.0
    
    def test_calculate_trend_score_with_missing_indicators(self, analyzer):
        """测试缺失技术指标的趋势评分计算"""
        # 创建缺少技术指标的数据
        data = {
            'close_price': [15.0, 16.0, 17.0],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_trend_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_calculate_momentum_score_with_valid_data(self, analyzer):
        """测试有效数据的动量评分计算"""
        data = {
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
            'rsi': [55.0, 60.0, 65.0, 70.0, 75.0],
            'k': [30.0, 35.0, 40.0, 45.0, 50.0],
            'd': [25.0, 30.0, 35.0, 40.0, 45.0],
            'j': [35.0, 40.0, 45.0, 50.0, 55.0],
            'williams_r': [-30.0, -35.0, -40.0, -45.0, -50.0],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_momentum_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_calculate_momentum_score_with_missing_indicators(self, analyzer):
        """测试缺失技术指标的动量评分计算"""
        data = {
            'close_price': [15.0, 16.0, 17.0],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_momentum_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_calculate_volume_health_score_with_valid_data(self, analyzer):
        """测试有效数据的量能健康度评分计算"""
        data = {
            'volume': [1000000, 1200000, 1100000, 1300000, 1400000],
            'volume_ma_5': [1100000, 1150000, 1200000, 1250000, 1300000],
            'volume_ma_10': [1050000, 1100000, 1150000, 1200000, 1250000],
            'obv': [10000000, 11000000, 12000000, 13000000, 14000000],
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_volume_health_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_identify_patterns_with_valid_data(self, analyzer):
        """测试形态识别功能"""
        data = {
            'open_price': [15.0, 16.0, 17.0, 18.0, 19.0],
            'high_price': [16.0, 17.0, 18.0, 19.0, 20.0],
            'low_price': [14.0, 15.0, 16.0, 17.0, 18.0],
            'close_price': [15.5, 16.5, 17.5, 18.5, 19.5],
            'volume': [1000000, 1200000, 1100000, 1300000, 1400000],
        }
        df = pd.DataFrame(data)
        
        patterns = analyzer.identify_patterns(df)
        
        assert isinstance(patterns, dict)
        assert 'patterns' in patterns
        assert 'confidence' in patterns
    
    def test_calculate_support_resistance_with_valid_data(self, analyzer):
        """测试支撑阻力位计算"""
        data = {
            'low_price': [14.0, 15.0, 16.0, 17.0, 18.0],
            'high_price': [16.0, 17.0, 18.0, 19.0, 20.0],
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
        }
        df = pd.DataFrame(data)
        
        support, resistance = analyzer.calculate_support_resistance(df)
        
        assert support is None or isinstance(support, float)
        assert resistance is None or isinstance(resistance, float)
    
    def test_analyze_stock_with_valid_data(self, analyzer):
        """测试股票分析功能"""
        with patch.object(analyzer.db_manager, 'execute_postgres_query') as mock_query:
            # 模拟数据库返回数据
            mock_data = pd.DataFrame({
                'trade_date': ['2024-01-01', '2024-01-02'],
                'open_price': [15.0, 16.0],
                'high_price': [16.0, 17.0],
                'low_price': [14.0, 15.0],
                'close_price': [15.5, 16.5],
                'volume': [1000000, 1200000],
            })
            mock_query.return_value = mock_data
            
            result = analyzer.analyze_stock('000001.SZ', '2024-01-02')
            
            assert isinstance(result, dict)
            assert 'trend_score' in result
            assert 'momentum_score' in result
            assert 'volume_health_score' in result
            assert 'patterns' in result
            assert 'support' in result
            assert 'resistance' in result
    
    def test_analyze_stock_with_no_data(self, analyzer):
        """测试无数据时的股票分析"""
        with patch.object(analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.return_value = pd.DataFrame()
            
            result = analyzer.analyze_stock('000001.SZ', '2024-01-02')
            
            assert isinstance(result, dict)
            assert result['trend_score'] == 0.0
            assert result['momentum_score'] == 0.0
            assert result['volume_health_score'] == 0.0
    
    def test_analyze_stock_with_database_error(self, analyzer):
        """测试数据库错误时的股票分析"""
        with patch.object(analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.side_effect = Exception("数据库连接失败")
            
            result = analyzer.analyze_stock('000001.SZ', '2024-01-02')
            
            assert isinstance(result, dict)
            assert result['trend_score'] == 0.0
            assert result['momentum_score'] == 0.0
            assert result['volume_health_score'] == 0.0
    
    @pytest.mark.parametrize("field_name", [
        'ema_12', 'ema_26', 'ma_5', 'ma_10', 'ma_20', 
        'bb_upper', 'bb_middle', 'macd', 'macd_signal'
    ])
    def test_trend_score_with_missing_field(self, analyzer, field_name):
        """测试缺失特定字段时的趋势评分"""
        data = {
            'close_price': [15.0, 16.0, 17.0],
        }
        # 添加缺失的字段
        data[field_name] = [np.nan, np.nan, np.nan]
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_trend_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    @pytest.mark.parametrize("field_name", [
        'rsi', 'k', 'd', 'j', 'williams_r'
    ])
    def test_momentum_score_with_missing_field(self, analyzer, field_name):
        """测试缺失特定字段时的动量评分"""
        data = {
            'close_price': [15.0, 16.0, 17.0],
        }
        # 添加缺失的字段
        data[field_name] = [np.nan, np.nan, np.nan]
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_momentum_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_performance_with_large_dataset(self, analyzer):
        """测试大数据集的性能"""
        # 创建大数据集
        dates = pd.date_range('2024-01-01', periods=1000, freq='D')
        data = {
            'trade_date': dates,
            'open_price': np.random.uniform(10, 20, 1000),
            'high_price': np.random.uniform(15, 25, 1000),
            'low_price': np.random.uniform(5, 15, 1000),
            'close_price': np.random.uniform(10, 20, 1000),
            'volume': np.random.uniform(1000000, 5000000, 1000),
        }
        df = pd.DataFrame(data)
        
        import time
        start_time = time.time()
        
        score = analyzer.calculate_trend_score(df)
        
        end_time = time.time()
        duration = end_time - start_time
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        assert duration < 1.0  # 应该在1秒内完成


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 