"""
资金流分析器单元测试
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

from src.analyzers.capital_flow_analyzer import CapitalFlowAnalyzer


class TestCapitalFlowAnalyzer:
    """资金流分析器测试类"""
    
    @pytest.fixture
    def analyzer(self):
        """创建资金流分析器实例"""
        return CapitalFlowAnalyzer()
    
    @pytest.fixture
    def sample_flow_data(self):
        """创建样本资金流数据"""
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        data = {
            'trade_date': dates,
            'main_net_inflow': np.random.uniform(-10000000, 10000000, 30),
            'super_large_net_inflow': np.random.uniform(-5000000, 5000000, 30),
            'retail_net_inflow': np.random.uniform(-2000000, 2000000, 30),
            'total_amount': np.random.uniform(50000000, 200000000, 30),
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def minimal_flow_data(self):
        """创建最小资金流数据集"""
        data = {
            'trade_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
            'main_net_inflow': [1000000, 2000000, 1500000, 3000000, 2500000],
            'super_large_net_inflow': [500000, 1000000, 750000, 1500000, 1250000],
            'retail_net_inflow': [200000, 400000, 300000, 600000, 500000],
            'total_amount': [100000000, 120000000, 110000000, 140000000, 130000000],
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def empty_flow_data(self):
        """创建空资金流数据集"""
        return pd.DataFrame()
    
    def test_initialization(self, analyzer):
        """测试初始化"""
        assert analyzer is not None
        assert hasattr(analyzer, 'volume_ma_period')
        assert hasattr(analyzer, 'money_flow_threshold')
    
    def test_calculate_main_force_score_with_valid_data(self, analyzer, minimal_flow_data):
        """测试有效数据的主力资金评分计算"""
        score = analyzer.calculate_main_force_score(minimal_flow_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_calculate_main_force_score_with_empty_data(self, analyzer, empty_flow_data):
        """测试空数据的主力资金评分计算"""
        score = analyzer.calculate_main_force_score(empty_flow_data)
        
        assert score == 0.0
    
    def test_calculate_main_force_score_with_insufficient_data(self, analyzer):
        """测试数据不足的主力资金评分计算"""
        data = {
            'main_net_inflow': [1000000],
            'super_large_net_inflow': [500000],
            'total_amount': [100000000],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_main_force_score(df)
        
        assert score == 0.0
    
    def test_calculate_main_force_score_with_zero_total_amount(self, analyzer):
        """测试总金额为零的主力资金评分计算"""
        data = {
            'main_net_inflow': [1000000, 2000000, 1500000, 3000000, 2500000],
            'super_large_net_inflow': [500000, 1000000, 750000, 1500000, 1250000],
            'total_amount': [0, 0, 0, 0, 0],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_main_force_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_calculate_retail_sentiment_score_with_valid_data(self, analyzer, minimal_flow_data):
        """测试有效数据的散户情绪评分计算"""
        score = analyzer.calculate_retail_sentiment_score(minimal_flow_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_calculate_retail_sentiment_score_with_empty_data(self, analyzer, empty_flow_data):
        """测试空数据的散户情绪评分计算"""
        score = analyzer.calculate_retail_sentiment_score(empty_flow_data)
        
        assert score == 0.0
    
    def test_calculate_retail_sentiment_score_with_insufficient_data(self, analyzer):
        """测试数据不足的散户情绪评分计算"""
        data = {
            'retail_net_inflow': [200000],
            'total_amount': [100000000],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_retail_sentiment_score(df)
        
        assert score == 0.0
    
    def test_calculate_institutional_activity_with_valid_data(self, analyzer, sample_flow_data):
        """测试有效数据的机构活跃度评分计算"""
        score = analyzer.calculate_institutional_activity(sample_flow_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_calculate_institutional_activity_with_insufficient_data(self, analyzer):
        """测试数据不足的机构活跃度评分计算"""
        data = {
            'main_net_inflow': [1000000, 2000000, 1500000, 3000000, 2500000],
            'super_large_net_inflow': [500000, 1000000, 750000, 1500000, 1250000],
            'total_amount': [100000000, 120000000, 110000000, 140000000, 130000000],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_institutional_activity(df)
        
        assert score == 0.0
    
    def test_calculate_flow_consistency_with_valid_data(self, analyzer, minimal_flow_data):
        """测试有效数据的资金流一致性评分计算"""
        score = analyzer.calculate_flow_consistency(minimal_flow_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_calculate_flow_consistency_with_insufficient_data(self, analyzer):
        """测试数据不足的资金流一致性评分计算"""
        data = {
            'main_net_inflow': [1000000, 2000000],
            'super_large_net_inflow': [500000, 1000000],
            'total_amount': [100000000, 120000000],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_flow_consistency(df)
        
        assert score == 0.0
    
    def test_calculate_volume_price_correlation_with_valid_data(self, analyzer):
        """测试有效数据的量价相关性评分计算"""
        # 创建资金流数据
        flow_data = pd.DataFrame({
            'volume': [1000000, 1200000, 1100000, 1300000, 1400000],
            'total_amount': [100000000, 120000000, 110000000, 140000000, 130000000],
        })
        
        # 创建价格数据
        price_data = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
            'volume': [1000000, 1200000, 1100000, 1300000, 1400000],
        })
        
        score = analyzer.calculate_volume_price_correlation(flow_data, price_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_calculate_volume_price_correlation_with_empty_data(self, analyzer, empty_flow_data):
        """测试空数据的量价相关性评分计算"""
        price_data = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0],
            'volume': [1000000, 1200000, 1100000],
        })
        
        score = analyzer.calculate_volume_price_correlation(empty_flow_data, price_data)
        
        assert score == 0.0
    
    def test_analyze_stock_with_valid_data(self, analyzer):
        """测试股票分析功能"""
        with patch.object(analyzer.db_manager, 'execute_postgres_query') as mock_query:
            # 模拟数据库返回资金流数据
            mock_data = pd.DataFrame({
                'trade_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
                'main_net_inflow': [1000000, 2000000, 1500000, 3000000, 2500000],
                'super_large_net_inflow': [500000, 1000000, 750000, 1500000, 1250000],
                'retail_net_inflow': [200000, 400000, 300000, 600000, 500000],
                'total_amount': [100000000, 120000000, 110000000, 140000000, 130000000],
            })
            mock_query.return_value = mock_data
            
            result = analyzer.analyze_stock('000001.SZ', '2024-01-05')
            
            assert isinstance(result, dict)
            assert 'main_force_score' in result
            assert 'retail_sentiment_score' in result
            assert 'institutional_activity' in result
            assert 'flow_consistency' in result
            assert 'volume_price_correlation' in result
            assert 'flow_analysis' in result
    
    def test_analyze_stock_with_no_data(self, analyzer):
        """测试无数据时的股票分析"""
        with patch.object(analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.return_value = pd.DataFrame()
            
            result = analyzer.analyze_stock('000001.SZ', '2024-01-05')
            
            assert isinstance(result, dict)
            assert result['main_force_score'] == 0.0
            assert result['retail_sentiment_score'] == 0.0
            assert result['institutional_activity'] == 0.0
            assert result['flow_consistency'] == 0.0
            assert result['volume_price_correlation'] == 0.0
    
    def test_analyze_stock_with_database_error(self, analyzer):
        """测试数据库错误时的股票分析"""
        with patch.object(analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.side_effect = Exception("数据库连接失败")
            
            result = analyzer.analyze_stock('000001.SZ', '2024-01-05')
            
            assert isinstance(result, dict)
            assert result['main_force_score'] == 0.0
            assert result['retail_sentiment_score'] == 0.0
            assert result['institutional_activity'] == 0.0
            assert result['flow_consistency'] == 0.0
            assert result['volume_price_correlation'] == 0.0
    
    def test_analyze_stock_with_missing_price_data(self, analyzer):
        """测试缺失价格数据的股票分析"""
        with patch.object(analyzer.db_manager, 'execute_postgres_query') as mock_query:
            # 只返回资金流数据，没有价格数据
            mock_data = pd.DataFrame({
                'trade_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
                'main_net_inflow': [1000000, 2000000, 1500000, 3000000, 2500000],
                'super_large_net_inflow': [500000, 1000000, 750000, 1500000, 1250000],
                'retail_net_inflow': [200000, 400000, 300000, 600000, 500000],
                'total_amount': [100000000, 120000000, 110000000, 140000000, 130000000],
            })
            mock_query.return_value = mock_data
            
            result = analyzer.analyze_stock('000001.SZ', '2024-01-05')
            
            assert isinstance(result, dict)
            assert 'main_force_score' in result
            assert 'retail_sentiment_score' in result
            assert 'institutional_activity' in result
            assert 'flow_consistency' in result
            assert 'volume_price_correlation' in result
    
    @pytest.mark.parametrize("field_name", [
        'main_net_inflow', 'super_large_net_inflow', 'retail_net_inflow', 'total_amount'
    ])
    def test_main_force_score_with_missing_field(self, analyzer, field_name):
        """测试缺失特定字段时的主力资金评分"""
        data = {
            'main_net_inflow': [1000000, 2000000, 1500000, 3000000, 2500000],
            'super_large_net_inflow': [500000, 1000000, 750000, 1500000, 1250000],
            'retail_net_inflow': [200000, 400000, 300000, 600000, 500000],
            'total_amount': [100000000, 120000000, 110000000, 140000000, 130000000],
        }
        # 删除指定字段
        del data[field_name]
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_main_force_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_negative_flow_values(self, analyzer):
        """测试负值资金流"""
        data = {
            'main_net_inflow': [-1000000, -2000000, -1500000, -3000000, -2500000],
            'super_large_net_inflow': [-500000, -1000000, -750000, -1500000, -1250000],
            'retail_net_inflow': [-200000, -400000, -300000, -600000, -500000],
            'total_amount': [100000000, 120000000, 110000000, 140000000, 130000000],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_main_force_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_extreme_flow_values(self, analyzer):
        """测试极值资金流"""
        data = {
            'main_net_inflow': [1000000000, 2000000000, 1500000000, 3000000000, 2500000000],
            'super_large_net_inflow': [500000000, 1000000000, 750000000, 1500000000, 1250000000],
            'retail_net_inflow': [200000000, 400000000, 300000000, 600000000, 500000000],
            'total_amount': [10000000000, 12000000000, 11000000000, 14000000000, 13000000000],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_main_force_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_performance_with_large_dataset(self, analyzer):
        """测试大数据集的性能"""
        # 创建大数据集
        dates = pd.date_range('2024-01-01', periods=1000, freq='D')
        data = {
            'trade_date': dates,
            'main_net_inflow': np.random.uniform(-10000000, 10000000, 1000),
            'super_large_net_inflow': np.random.uniform(-5000000, 5000000, 1000),
            'retail_net_inflow': np.random.uniform(-2000000, 2000000, 1000),
            'total_amount': np.random.uniform(50000000, 200000000, 1000),
        }
        df = pd.DataFrame(data)
        
        import time
        start_time = time.time()
        
        score = analyzer.calculate_main_force_score(df)
        
        end_time = time.time()
        duration = end_time - start_time
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        assert duration < 1.0  # 应该在1秒内完成
    
    def test_data_serialization(self, analyzer):
        """测试数据序列化（修复字典类型问题）"""
        with patch.object(analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_data = pd.DataFrame({
                'trade_date': ['2024-01-01', '2024-01-02'],
                'main_net_inflow': [1000000, 2000000],
                'super_large_net_inflow': [500000, 1000000],
                'retail_net_inflow': [200000, 400000],
                'total_amount': [100000000, 120000000],
            })
            mock_query.return_value = mock_data
            
            result = analyzer.analyze_stock('000001.SZ', '2024-01-02')
            
            # 检查flow_analysis字段是否为字符串类型
            assert isinstance(result['flow_analysis'], str)
    
    def test_nan_values_handling(self, analyzer):
        """测试NaN值处理"""
        data = {
            'main_net_inflow': [1000000, np.nan, 1500000, 3000000, 2500000],
            'super_large_net_inflow': [500000, 1000000, np.nan, 1500000, 1250000],
            'retail_net_inflow': [200000, 400000, 300000, np.nan, 500000],
            'total_amount': [100000000, 120000000, 110000000, 140000000, np.nan],
        }
        df = pd.DataFrame(data)
        
        score = analyzer.calculate_main_force_score(df)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 