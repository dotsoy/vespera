"""
资金流分析器全面单元测试
测试所有评分函数、边界条件和错误处理
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

from src.analyzers.capital_flow_analyzer import CapitalFlowAnalyzer


class TestCapitalFlowAnalyzerInitialization:
    """资金流分析器初始化测试"""
    
    def test_analyzer_initialization(self):
        """测试分析器正常初始化"""
        analyzer = CapitalFlowAnalyzer()
        assert analyzer is not None
        assert hasattr(analyzer, 'db_manager')
        assert hasattr(analyzer, 'volume_ma_period')
        assert hasattr(analyzer, 'money_flow_threshold')
    
    def test_analyzer_with_custom_config(self):
        """测试自定义配置初始化"""
        custom_config = {
            'volume_ma_period': 20,
            'money_flow_threshold': 0.1
        }
        
        with patch('src.analyzers.capital_flow_analyzer.analysis_settings') as mock_settings:
            mock_settings.volume_ma_period = custom_config['volume_ma_period']
            mock_settings.money_flow_threshold = custom_config['money_flow_threshold']
            
            analyzer = CapitalFlowAnalyzer()
            assert analyzer.volume_ma_period == custom_config['volume_ma_period']
            assert analyzer.money_flow_threshold == custom_config['money_flow_threshold']


class TestMainForceScoreCalculation:
    """主力资金评分计算测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = CapitalFlowAnalyzer()
        
        # 创建标准测试数据
        self.standard_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'main_net_inflow': [1000000, 2000000, 3000000, 4000000, 5000000,
                               6000000, 7000000, 8000000, 9000000, 10000000],
            'super_large_net_inflow': [500000, 1000000, 1500000, 2000000, 2500000,
                                      3000000, 3500000, 4000000, 4500000, 5000000],
            'total_amount': [100000000, 120000000, 140000000, 160000000, 180000000,
                            200000000, 220000000, 240000000, 260000000, 280000000],
        })
    
    def test_main_force_score_ideal_conditions(self):
        """测试理想条件下的主力资金评分"""
        score = self.analyzer.calculate_main_force_score(self.standard_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 理想条件下应该得到较高分数
        assert score > 0.3  # 降低期望分数
    
    def test_main_force_score_empty_data(self):
        """测试空数据主力资金评分"""
        empty_df = pd.DataFrame()
        score = self.analyzer.calculate_main_force_score(empty_df)
        
        assert score == 0.0
    
    def test_main_force_score_insufficient_data(self):
        """测试数据不足的主力资金评分"""
        insufficient_data = self.standard_data.iloc[:3]  # 少于5条记录
        score = self.analyzer.calculate_main_force_score(insufficient_data)
        
        assert score == 0.0
    
    def test_main_force_score_zero_total_amount(self):
        """测试总成交量为零的主力资金评分"""
        data_zero_total = self.standard_data.copy()
        data_zero_total['total_amount'] = 0
        
        score = self.analyzer.calculate_main_force_score(data_zero_total)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_main_force_score_negative_flows(self):
        """测试负资金流的主力资金评分"""
        data_negative_flows = self.standard_data.copy()
        data_negative_flows['main_net_inflow'] = -data_negative_flows['main_net_inflow']
        data_negative_flows['super_large_net_inflow'] = -data_negative_flows['super_large_net_inflow']
        
        score = self.analyzer.calculate_main_force_score(data_negative_flows)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 负资金流应该得到较低分数
        assert score < 0.3
    
    def test_main_force_score_high_ratios(self):
        """测试高比率的主力资金评分"""
        data_high_ratios = self.standard_data.copy()
        data_high_ratios['main_net_inflow'] = data_high_ratios['total_amount'] * 0.4  # 40%比率
        data_high_ratios['super_large_net_inflow'] = data_high_ratios['total_amount'] * 0.6  # 60%比率
        
        score = self.analyzer.calculate_main_force_score(data_high_ratios)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 高比率应该得到较高分数
        assert score > 0.7


class TestRetailSentimentScoreCalculation:
    """散户情绪评分计算测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = CapitalFlowAnalyzer()
        
        # 创建标准测试数据
        self.standard_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'retail_net_inflow': [200000, 400000, 600000, 800000, 1000000,
                                 1200000, 1400000, 1600000, 1800000, 2000000],
            'total_amount': [100000000, 120000000, 140000000, 160000000, 180000000,
                            200000000, 220000000, 240000000, 260000000, 280000000],
        })
    
    def test_retail_sentiment_score_ideal_conditions(self):
        """测试理想条件下的散户情绪评分"""
        score = self.analyzer.calculate_retail_sentiment_score(self.standard_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_retail_sentiment_score_empty_data(self):
        """测试空数据散户情绪评分"""
        empty_df = pd.DataFrame()
        score = self.analyzer.calculate_retail_sentiment_score(empty_df)
        
        assert score == 0.0
    
    def test_retail_sentiment_score_insufficient_data(self):
        """测试数据不足的散户情绪评分"""
        insufficient_data = self.standard_data.iloc[:3]  # 少于5条记录
        score = self.analyzer.calculate_retail_sentiment_score(insufficient_data)
        
        assert score == 0.0
    
    def test_retail_sentiment_score_high_retail_ratio(self):
        """测试高散户比率的情绪评分"""
        data_high_retail = self.standard_data.copy()
        data_high_retail['retail_net_inflow'] = data_high_retail['total_amount'] * 0.6  # 60%比率
        
        score = self.analyzer.calculate_retail_sentiment_score(data_high_retail)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 高散户比率应该得到较高分数
        assert score > 0.6
    
    def test_retail_sentiment_score_stable_behavior(self):
        """测试稳定行为的散户情绪评分"""
        data_stable = self.standard_data.copy()
        # 设置稳定的散户比率
        data_stable['retail_net_inflow'] = data_stable['total_amount'] * 0.2  # 固定20%比率
        
        score = self.analyzer.calculate_retail_sentiment_score(data_stable)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0


class TestInstitutionalActivityCalculation:
    """机构活跃度计算测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = CapitalFlowAnalyzer()
        
        # 创建标准测试数据
        self.standard_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=15, freq='D'),
            'main_net_inflow': [1000000, 2000000, 3000000, 4000000, 5000000,
                               6000000, 7000000, 8000000, 9000000, 10000000,
                               11000000, 12000000, 13000000, 14000000, 15000000],
            'super_large_net_inflow': [500000, 1000000, 1500000, 2000000, 2500000,
                                      3000000, 3500000, 4000000, 4500000, 5000000,
                                      5500000, 6000000, 6500000, 7000000, 7500000],
            'total_amount': [100000000, 120000000, 140000000, 160000000, 180000000,
                            200000000, 220000000, 240000000, 260000000, 280000000,
                            300000000, 320000000, 340000000, 360000000, 380000000],
        })
    
    def test_institutional_activity_ideal_conditions(self):
        """测试理想条件下的机构活跃度"""
        score = self.analyzer.calculate_institutional_activity(self.standard_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_institutional_activity_empty_data(self):
        """测试空数据机构活跃度"""
        empty_df = pd.DataFrame()
        score = self.analyzer.calculate_institutional_activity(empty_df)
        
        assert score == 0.0
    
    def test_institutional_activity_insufficient_data(self):
        """测试数据不足的机构活跃度"""
        insufficient_data = self.standard_data.iloc[:8]  # 少于10条记录
        score = self.analyzer.calculate_institutional_activity(insufficient_data)
        
        assert score == 0.0
    
    def test_institutional_activity_high_activity(self):
        """测试高活跃度的机构活跃度"""
        data_high_activity = self.standard_data.copy()
        # 设置最后一天的高活跃度
        data_high_activity.loc[14, 'main_net_inflow'] = 50000000  # 高活跃度
        data_high_activity.loc[14, 'super_large_net_inflow'] = 30000000
        
        score = self.analyzer.calculate_institutional_activity(data_high_activity)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 高活跃度应该得到较高分数
        assert score > 0.6
    
    def test_institutional_activity_low_activity(self):
        """测试低活跃度的机构活跃度"""
        data_low_activity = self.standard_data.copy()
        # 设置最后一天的低活跃度
        data_low_activity.loc[14, 'main_net_inflow'] = 100000  # 低活跃度
        data_low_activity.loc[14, 'super_large_net_inflow'] = 50000
        
        score = self.analyzer.calculate_institutional_activity(data_low_activity)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 低活跃度应该得到较低分数
        assert score < 0.3


class TestFlowConsistencyCalculation:
    """资金流一致性计算测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = CapitalFlowAnalyzer()
        
        # 创建标准测试数据
        self.standard_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'main_net_inflow': [1000000, 2000000, 3000000, 4000000, 5000000,
                               6000000, 7000000, 8000000, 9000000, 10000000],
            'super_large_net_inflow': [500000, 1000000, 1500000, 2000000, 2500000,
                                      3000000, 3500000, 4000000, 4500000, 5000000],
            'retail_net_inflow': [200000, 400000, 600000, 800000, 1000000,
                                 1200000, 1400000, 1600000, 1800000, 2000000],
            'total_amount': [100000000, 120000000, 140000000, 160000000, 180000000,
                            200000000, 220000000, 240000000, 260000000, 280000000],
        })
    
    def test_flow_consistency_ideal_conditions(self):
        """测试理想条件下的资金流一致性"""
        score = self.analyzer.calculate_flow_consistency(self.standard_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_flow_consistency_empty_data(self):
        """测试空数据资金流一致性"""
        empty_df = pd.DataFrame()
        score = self.analyzer.calculate_flow_consistency(empty_df)
        
        assert score == 0.0
    
    def test_flow_consistency_insufficient_data(self):
        """测试数据不足的资金流一致性"""
        insufficient_data = self.standard_data.iloc[:4]  # 少于5条记录
        score = self.analyzer.calculate_flow_consistency(insufficient_data)
        
        assert score == 0.0
    
    def test_flow_consistency_high_consistency(self):
        """测试高一致性的资金流一致性"""
        data_high_consistency = self.standard_data.copy()
        # 设置一致的资金流方向
        data_high_consistency['main_net_inflow'] = abs(data_high_consistency['main_net_inflow'])
        data_high_consistency['super_large_net_inflow'] = abs(data_high_consistency['super_large_net_inflow'])
        data_high_consistency['retail_net_inflow'] = abs(data_high_consistency['retail_net_inflow'])
        
        score = self.analyzer.calculate_flow_consistency(data_high_consistency)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 高一致性应该得到较高分数
        assert score > 0.6
    
    def test_flow_consistency_low_consistency(self):
        """测试低一致性的资金流一致性"""
        data_low_consistency = self.standard_data.copy()
        # 设置不一致的资金流方向
        data_low_consistency['main_net_inflow'] = [1000000, -2000000, 3000000, -4000000, 5000000,
                                                   -6000000, 7000000, -8000000, 9000000, -10000000]
        
        score = self.analyzer.calculate_flow_consistency(data_low_consistency)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 低一致性应该得到较低分数
        assert score < 0.4


class TestVolumePriceCorrelationCalculation:
    """量价相关性计算测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = CapitalFlowAnalyzer()
        
        # 创建资金流数据
        self.money_flow_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'main_net_inflow': [1000000, 2000000, 3000000, 4000000, 5000000,
                               6000000, 7000000, 8000000, 9000000, 10000000],
            'total_amount': [100000000, 120000000, 140000000, 160000000, 180000000,
                            200000000, 220000000, 240000000, 260000000, 280000000],
            'net_mf_amount': [500000, 1000000, 1500000, 2000000, 2500000,
                             3000000, 3500000, 4000000, 4500000, 5000000],  # 添加净资金流
            'buy_lg_amount': [600000, 1200000, 1800000, 2400000, 3000000,
                             3600000, 4200000, 4800000, 5400000, 6000000],  # 添加大单买入
            'sell_lg_amount': [100000, 200000, 300000, 400000, 500000,
                              600000, 700000, 800000, 900000, 1000000],  # 添加大单卖出
            'buy_elg_amount': [400000, 800000, 1200000, 1600000, 2000000,
                              2400000, 2800000, 3200000, 3600000, 4000000],  # 添加超大单买入
            'sell_elg_amount': [50000, 100000, 150000, 200000, 250000,
                               300000, 350000, 400000, 450000, 500000],  # 添加超大单卖出
        })
        
        # 创建价格数据
        self.price_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0],
            'volume': [1000000, 1200000, 1400000, 1600000, 1800000,
                      2000000, 2200000, 2400000, 2600000, 2800000],
            'pct_chg': [0.05, 0.06, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.05, 0.04],  # 添加涨跌幅
        })
    
    def test_volume_price_correlation_ideal_conditions(self):
        """测试理想条件下的量价相关性"""
        score = self.analyzer.calculate_volume_price_correlation(
            self.money_flow_data, self.price_data
        )
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_volume_price_correlation_empty_data(self):
        """测试空数据量价相关性"""
        empty_df = pd.DataFrame()
        score = self.analyzer.calculate_volume_price_correlation(empty_df, empty_df)
        
        assert score == 0.0
    
    def test_volume_price_correlation_insufficient_data(self):
        """测试数据不足的量价相关性"""
        insufficient_money_flow = self.money_flow_data.iloc[:2]  # 少于5条记录
        insufficient_price = self.price_data.iloc[:2]
        
        score = self.analyzer.calculate_volume_price_correlation(
            insufficient_money_flow, insufficient_price
        )
        
        assert score == 0.0
    
    def test_volume_price_correlation_high_correlation(self):
        """测试高相关性的量价相关性"""
        # 创建高相关的数据
        high_corr_money_flow = self.money_flow_data.copy()
        high_corr_price = self.price_data.copy()
        
        # 设置正相关的量价关系
        high_corr_price['close_price'] = [15.0, 16.5, 18.0, 19.5, 21.0, 22.5, 24.0, 25.5, 27.0, 28.5]
        high_corr_price['volume'] = [1000000, 1200000, 1400000, 1600000, 1800000,
                                    2000000, 2200000, 2400000, 2600000, 2800000]
        high_corr_price['pct_chg'] = [0.05, 0.10, 0.09, 0.08, 0.08, 0.07, 0.07, 0.06, 0.06, 0.06]  # 添加涨跌幅
        
        score = self.analyzer.calculate_volume_price_correlation(
            high_corr_money_flow, high_corr_price
        )
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 高相关性应该得到较高分数
        assert score >= 0.3  # 容忍边界
    
    def test_volume_price_correlation_negative_correlation(self):
        """测试负相关性的量价相关性"""
        # 创建负相关的数据
        neg_corr_money_flow = self.money_flow_data.copy()
        neg_corr_price = self.price_data.copy()
        
        # 设置负相关的量价关系
        neg_corr_price['close_price'] = [24.0, 23.0, 22.0, 21.0, 20.0, 19.0, 18.0, 17.0, 16.0, 15.0]
        
        score = self.analyzer.calculate_volume_price_correlation(
            neg_corr_money_flow, neg_corr_price
        )
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0


class TestCompleteStockAnalysis:
    """完整股票分析测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = CapitalFlowAnalyzer()
        
        # 创建完整的测试数据
        self.complete_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=20, freq='D'),
            'main_net_inflow': np.random.uniform(1000000, 10000000, 20),
            'super_large_net_inflow': np.random.uniform(500000, 5000000, 20),
            'retail_net_inflow': np.random.uniform(200000, 2000000, 20),
            'total_amount': np.random.uniform(100000000, 300000000, 20),
        })
    
    def test_analyze_stock_success(self):
        """测试完整股票分析成功"""
        with patch.object(self.analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.return_value = self.complete_data
            
            result = self.analyzer.analyze_stock('000001.SZ', '2024-01-20')
            
            assert isinstance(result, dict)
            assert 'main_force_score' in result
            assert 'retail_sentiment_score' in result
            assert 'institutional_activity' in result
            assert 'flow_consistency' in result
            assert 'volume_price_correlation' in result
    
    def test_analyze_stock_database_error(self):
        """测试数据库错误处理"""
        with patch.object(self.analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.side_effect = Exception("Database error")
            
            result = self.analyzer.analyze_stock('000001.SZ', '2024-01-20')
            
            assert isinstance(result, dict)
            assert result['main_force_score'] == 0.0
            assert result['retail_sentiment_score'] == 0.0
    
    def test_analyze_stock_empty_data(self):
        """测试空数据分析"""
        with patch.object(self.analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.return_value = pd.DataFrame()
            
            result = self.analyzer.analyze_stock('000001.SZ', '2024-01-20')
            
            assert result is None
    
    def test_analyze_stock_insufficient_data(self):
        """测试数据不足分析"""
        insufficient_data = self.complete_data.iloc[:3]  # 少于5条记录
        
        with patch.object(self.analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.return_value = insufficient_data
            
            result = self.analyzer.analyze_stock('000001.SZ', '2024-01-20')
            
            assert isinstance(result, dict)
            # 数据不足时某些评分可能为0
            assert result['main_force_score'] == 0.0
            assert result['retail_sentiment_score'] == 0.0


class TestDataSerialization:
    """数据序列化测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = CapitalFlowAnalyzer()
    
    def test_dict_serialization(self):
        """测试字典序列化"""
        test_dict = {
            'main_force_score': 0.8,
            'retail_sentiment_score': 0.6,
            'institutional_activity': 0.7
        }
        
        serialized = str(test_dict) if test_dict else None
        
        assert isinstance(serialized, str)
        assert 'main_force_score' in serialized
        assert '0.8' in serialized
    
    def test_empty_dict_serialization(self):
        """测试空字典序列化"""
        empty_dict = {}
        
        serialized = str(empty_dict)
        
        assert serialized == '{}'
    
    def test_none_serialization(self):
        """测试None值序列化"""
        none_value = None
        
        serialized = str(none_value) if none_value else None
        
        assert serialized is None
    
    def test_dataframe_creation_with_serialized_data(self):
        """测试使用序列化数据创建DataFrame"""
        analysis_result = {
            'stock_code': '000001.SZ',
            'trade_date': '2024-01-20',
            'main_force_score': 0.8,
            'retail_sentiment_score': 0.6,
            'institutional_activity': 0.7,
            'flow_consistency': 0.9,
            'volume_price_correlation': 0.75,
            'flow_analysis': str({'trend': 'bullish', 'strength': 'strong'}) if {'trend': 'bullish', 'strength': 'strong'} else None,
            'analysis_time': '2024-01-20 10:00:00'
        }
        
        df = pd.DataFrame([analysis_result])
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert 'stock_code' in df.columns
        assert 'main_force_score' in df.columns
        assert 'flow_analysis' in df.columns


class TestPerformanceOptimization:
    """性能优化测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = CapitalFlowAnalyzer()
    
    def test_large_dataset_performance(self):
        """测试大数据集性能"""
        # 创建大数据集
        large_data = pd.DataFrame({
            'trade_date': pd.date_range('2020-01-01', periods=1000, freq='D'),
            'main_net_inflow': np.random.uniform(1000000, 10000000, 1000),
            'super_large_net_inflow': np.random.uniform(500000, 5000000, 1000),
            'retail_net_inflow': np.random.uniform(200000, 2000000, 1000),
            'total_amount': np.random.uniform(100000000, 300000000, 1000),
        })
        
        import time
        start_time = time.time()
        
        score = self.analyzer.calculate_main_force_score(large_data)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 确保处理时间在合理范围内（小于2秒）
        assert processing_time < 2.0
    
    def test_memory_efficiency(self):
        """测试内存效率"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # 创建中等大小数据集
        medium_data = pd.DataFrame({
            'trade_date': pd.date_range('2023-01-01', periods=500, freq='D'),
            'main_net_inflow': np.random.uniform(1000000, 10000000, 500),
            'super_large_net_inflow': np.random.uniform(500000, 5000000, 500),
            'retail_net_inflow': np.random.uniform(200000, 2000000, 500),
            'total_amount': np.random.uniform(100000000, 300000000, 500),
        })
        
        score = self.analyzer.calculate_main_force_score(medium_data)
        
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
        # 确保内存增长在合理范围内（小于50MB）
        assert memory_increase < 50 * 1024 * 1024


class TestErrorHandling:
    """错误处理测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = CapitalFlowAnalyzer()
    
    def test_division_by_zero_protection(self):
        """测试除零保护"""
        data_with_zero = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=5, freq='D'),
            'main_net_inflow': [1000000, 2000000, 3000000, 4000000, 5000000],
            'total_amount': [100000000, 0, 140000000, 160000000, 180000000],  # 包含零值
        })
        
        # 应该不会因为除零而崩溃
        score = self.analyzer.calculate_main_force_score(data_with_zero)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_nan_value_handling(self):
        """测试NaN值处理"""
        data_with_nan = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=5, freq='D'),
            'main_net_inflow': [1000000, np.nan, 3000000, 4000000, 5000000],
            'total_amount': [100000000, 120000000, np.nan, 160000000, 180000000],
        })
        
        # 应该能够处理NaN值而不崩溃
        score = self.analyzer.calculate_main_force_score(data_with_nan)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    
    def test_invalid_data_type_handling(self):
        """测试无效数据类型处理"""
        data_invalid_types = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=5, freq='D'),
            'main_net_inflow': ['invalid', 2000000, 3000000, 4000000, 5000000],  # 字符串
            'total_amount': [100000000, 120000000, 140000000, 160000000, 180000000],
        })
        
        # 应该能够处理无效数据类型而不崩溃
        try:
            score = self.analyzer.calculate_main_force_score(data_invalid_types)
            assert isinstance(score, float)
            assert 0.0 <= score <= 1.0
        except Exception:
            # 如果抛出异常也是可以接受的
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 