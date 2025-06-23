"""
错误修复验证测试
专门测试文档中提到的错误修复是否有效
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
from src.analyzers.capital_flow_analyzer import CapitalFlowAnalyzer
from src.utils.technical_indicators import add_all_indicators


class TestTechnicalIndicatorFixes:
    """技术指标错误修复测试"""
    
    def test_minimal_data_calculation(self):
        """测试最小数据计算（修复：从30条降低到1条）"""
        # 创建最小数据集
        data = {
            'trade_date': ['2024-01-01'],
            'open_price': [15.0],
            'high_price': [16.0],
            'low_price': [14.0],
            'close_price': [15.5],
            'volume': [2000000],
        }
        df = pd.DataFrame(data)
        
        # 应该能够计算技术指标而不报错
        result = add_all_indicators(df, use_polars=False)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        # 即使数据不足，也应该返回DataFrame，只是很多指标会是NaN
    
    def test_empty_data_handling(self):
        """测试空数据处理（修复：空数据检查）"""
        empty_df = pd.DataFrame()
        
        # 应该优雅处理空数据
        result = add_all_indicators(empty_df, use_polars=False)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_single_record_calculation(self):
        """测试单条记录计算（修复：最小数据要求）"""
        data = {
            'trade_date': ['2024-01-01'],
            'open_price': [15.0],
            'high_price': [16.0],
            'low_price': [14.0],
            'close_price': [15.5],
            'volume': [2000000],
        }
        df = pd.DataFrame(data)
        
        analyzer = TechnicalAnalyzer()
        
        # 测试趋势评分计算
        trend_score = analyzer.calculate_trend_score(df)
        assert isinstance(trend_score, float)
        assert 0.0 <= trend_score <= 1.0
        
        # 测试动量评分计算
        momentum_score = analyzer.calculate_momentum_score(df)
        assert isinstance(momentum_score, float)
        assert 0.0 <= momentum_score <= 1.0
        
        # 测试量能健康度计算
        volume_score = analyzer.calculate_volume_health_score(df)
        assert isinstance(volume_score, float)
        assert 0.0 <= volume_score <= 1.0


class TestTechnicalAnalyzerFixes:
    """技术分析器错误修复测试"""
    
    def test_safe_field_access(self):
        """测试安全字段访问（修复：使用.get()方法）"""
        analyzer = TechnicalAnalyzer()
        
        # 创建缺少技术指标的数据
        data = {
            'close_price': [15.0, 16.0, 17.0],
            # 故意不包含技术指标字段
        }
        df = pd.DataFrame(data)
        
        # 应该不会因为缺少字段而崩溃
        trend_score = analyzer.calculate_trend_score(df)
        assert isinstance(trend_score, float)
        assert 0.0 <= trend_score <= 1.0
        
        momentum_score = analyzer.calculate_momentum_score(df)
        assert isinstance(momentum_score, float)
        assert 0.0 <= momentum_score <= 1.0
    
    def test_nan_value_handling(self):
        """测试NaN值处理（修复：pd.notna()检查）"""
        analyzer = TechnicalAnalyzer()
        
        # 创建包含NaN值的数据
        data = {
            'close_price': [15.0, 16.0, 17.0],
            'rsi': [np.nan, 60.0, np.nan],
            'ema_12': [np.nan, np.nan, 15.5],
            'ma_5': [14.8, np.nan, np.nan],
        }
        df = pd.DataFrame(data)
        
        # 应该能够处理NaN值而不崩溃
        trend_score = analyzer.calculate_trend_score(df)
        assert isinstance(trend_score, float)
        assert 0.0 <= trend_score <= 1.0
        
        momentum_score = analyzer.calculate_momentum_score(df)
        assert isinstance(momentum_score, float)
        assert 0.0 <= momentum_score <= 1.0
    
    def test_missing_indicators_fallback(self):
        """测试缺失指标降级处理（修复：备用计算）"""
        analyzer = TechnicalAnalyzer()
        
        # 创建只有价格数据的数据
        data = {
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
        }
        df = pd.DataFrame(data)
        
        # 应该使用价格趋势作为备用计算
        trend_score = analyzer.calculate_trend_score(df)
        assert isinstance(trend_score, float)
        assert 0.0 <= trend_score <= 1.0
        
        # 应该能够计算价格动量
        momentum_score = analyzer.calculate_momentum_score(df)
        assert isinstance(momentum_score, float)
        assert 0.0 <= momentum_score <= 1.0
    
    def test_support_resistance_minimal_data(self):
        """测试支撑阻力位最小数据（修复：从20条降低到5条）"""
        analyzer = TechnicalAnalyzer()
        
        # 创建5条记录的数据
        data = {
            'low_price': [14.0, 15.0, 16.0, 17.0, 18.0],
            'high_price': [16.0, 17.0, 18.0, 19.0, 20.0],
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
        }
        df = pd.DataFrame(data)
        
        support, resistance = analyzer.calculate_support_resistance(df)
        
        assert support is None or isinstance(support, float)
        assert resistance is None or isinstance(resistance, float)
    
    def test_pattern_recognition_minimal_data(self):
        """测试形态识别最小数据（修复：从10条降低到2条）"""
        analyzer = TechnicalAnalyzer()
        
        # 创建2条记录的数据
        data = {
            'open_price': [15.0, 16.0],
            'high_price': [16.0, 17.0],
            'low_price': [14.0, 15.0],
            'close_price': [15.5, 16.5],
            'volume': [1000000, 1200000],
        }
        df = pd.DataFrame(data)
        
        patterns = analyzer.identify_patterns(df)
        
        assert isinstance(patterns, dict)
        assert 'patterns' in patterns
        assert 'confidence' in patterns


class TestCapitalFlowAnalyzerFixes:
    """资金流分析器错误修复测试"""
    
    def test_data_serialization_fix(self):
        """测试数据序列化修复（修复：字典转字符串）"""
        analyzer = CapitalFlowAnalyzer()
        
        with patch.object(analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.return_value = pd.DataFrame({
                'trade_date': ['2024-01-01', '2024-01-02'],
                'main_net_inflow': [1000000, 2000000],
                'super_large_net_inflow': [500000, 1000000],
                'retail_net_inflow': [200000, 400000],
                'total_amount': [100000000, 120000000],
            })
            
            result = analyzer.analyze_stock('000001.SZ', '2024-01-02')
            
            # 检查flow_analysis字段是否为字符串类型
            assert isinstance(result['flow_analysis'], str)
    
    def test_empty_dict_serialization(self):
        """测试空字典序列化（修复：空值检查）"""
        analyzer = CapitalFlowAnalyzer()
        
        # 测试空字典序列化
        empty_dict = {}
        serialized = str(empty_dict) if empty_dict else None
        
        assert serialized == "{}"
    
    def test_none_value_serialization(self):
        """测试None值序列化（修复：空值检查）"""
        analyzer = CapitalFlowAnalyzer()
        
        # 测试None值序列化
        none_value = None
        serialized = str(none_value) if none_value else None
        
        assert serialized is None


class TestDatabaseCompatibilityFixes:
    """数据库兼容性修复测试"""
    
    def test_dataframe_creation_with_serialized_data(self):
        """测试序列化数据的DataFrame创建"""
        # 模拟序列化后的数据
        serialized_data = {
            'ts_code': '000001.SZ',
            'trade_date': '2024-01-01',
            'trend_score': 0.8,
            'momentum_score': 0.7,
            'volume_health_score': 0.6,
            'flow_analysis': '{"main_force": 0.9, "retail_sentiment": 0.6}',  # 序列化的字典
        }
        
        # 应该能够成功创建DataFrame
        df = pd.DataFrame([serialized_data])
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert 'flow_analysis' in df.columns
        assert isinstance(df['flow_analysis'].iloc[0], str)
    
    def test_database_insert_compatibility(self):
        """测试数据库插入兼容性"""
        # 创建包含序列化数据的DataFrame
        data = {
            'ts_code': ['000001.SZ', '000002.SZ'],
            'trade_date': ['2024-01-01', '2024-01-02'],
            'trend_score': [0.8, 0.7],
            'momentum_score': [0.7, 0.6],
            'volume_health_score': [0.6, 0.5],
            'flow_analysis': [
                '{"main_force": 0.9, "retail_sentiment": 0.6}',
                '{"main_force": 0.8, "retail_sentiment": 0.5}'
            ],
        }
        
        df = pd.DataFrame(data)
        
        # 验证所有字段都是数据库兼容类型
        for column in df.columns:
            for value in df[column]:
                # 检查是否为基本数据类型
                assert isinstance(value, (str, int, float, bool)) or pd.isna(value)


class TestErrorHandlingImprovements:
    """错误处理改进测试"""
    
    def test_graceful_degradation_with_insufficient_data(self):
        """测试数据不足时的优雅降级"""
        analyzer = TechnicalAnalyzer()
        
        # 创建数据不足的情况
        minimal_data = pd.DataFrame({
            'close_price': [15.0],
        })
        
        # 应该能够优雅处理
        trend_score = analyzer.calculate_trend_score(minimal_data)
        momentum_score = analyzer.calculate_momentum_score(minimal_data)
        volume_score = analyzer.calculate_volume_health_score(minimal_data)
        
        # 所有评分都应该在有效范围内
        for score in [trend_score, momentum_score, volume_score]:
            assert isinstance(score, float)
            assert 0.0 <= score <= 1.0
    
    def test_exception_handling_in_analysis(self):
        """测试分析过程中的异常处理"""
        analyzer = TechnicalAnalyzer()
        
        with patch.object(analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.side_effect = Exception("数据库连接失败")
            
            # 应该能够处理数据库错误
            result = analyzer.analyze_stock('000001.SZ', '2024-01-01')
            
            assert isinstance(result, dict)
            assert result['trend_score'] == 0.0
            assert result['momentum_score'] == 0.0
            assert result['volume_health_score'] == 0.0
    
    def test_field_missing_handling(self):
        """测试字段缺失处理"""
        analyzer = TechnicalAnalyzer()
        
        # 创建缺少各种字段的数据
        incomplete_data = pd.DataFrame({
            'close_price': [15.0, 16.0, 17.0],
            # 故意缺少技术指标字段
        })
        
        # 应该能够处理字段缺失
        trend_score = analyzer.calculate_trend_score(incomplete_data)
        momentum_score = analyzer.calculate_momentum_score(incomplete_data)
        
        assert isinstance(trend_score, float)
        assert isinstance(momentum_score, float)
    
    def test_division_by_zero_protection(self):
        """测试除零保护"""
        analyzer = CapitalFlowAnalyzer()
        
        # 创建总金额为零的数据
        zero_amount_data = pd.DataFrame({
            'main_net_inflow': [1000000, 2000000],
            'super_large_net_inflow': [500000, 1000000],
            'total_amount': [0, 0],  # 总金额为零
        })
        
        # 应该能够处理除零情况
        score = analyzer.calculate_main_force_score(zero_amount_data)
        
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0


class TestPerformanceImprovements:
    """性能改进测试"""
    
    def test_large_dataset_performance(self):
        """测试大数据集性能"""
        analyzer = TechnicalAnalyzer()
        
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
        
        # 测试趋势评分计算性能
        trend_score = analyzer.calculate_trend_score(df)
        
        end_time = time.time()
        duration = end_time - start_time
        
        assert isinstance(trend_score, float)
        assert 0.0 <= trend_score <= 1.0
        assert duration < 1.0  # 应该在1秒内完成
    
    def test_memory_efficiency(self):
        """测试内存效率"""
        analyzer = TechnicalAnalyzer()
        
        # 创建中等大小的数据集
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        data = {
            'trade_date': dates,
            'open_price': np.random.uniform(10, 20, 100),
            'high_price': np.random.uniform(15, 25, 100),
            'low_price': np.random.uniform(5, 15, 100),
            'close_price': np.random.uniform(10, 20, 100),
            'volume': np.random.uniform(1000000, 5000000, 100),
        }
        df = pd.DataFrame(data)
        
        # 测试多次计算的内存使用
        for _ in range(10):
            trend_score = analyzer.calculate_trend_score(df)
            momentum_score = analyzer.calculate_momentum_score(df)
            volume_score = analyzer.calculate_volume_health_score(df)
            
            assert isinstance(trend_score, float)
            assert isinstance(momentum_score, float)
            assert isinstance(volume_score, float)


class TestIntegrationFixes:
    """集成修复测试"""
    
    def test_end_to_end_analysis_with_minimal_data(self):
        """测试端到端分析（最小数据）"""
        analyzer = TechnicalAnalyzer()
        
        # 创建最小数据集
        minimal_data = pd.DataFrame({
            'trade_date': ['2024-01-01'],
            'open_price': [15.0],
            'high_price': [16.0],
            'low_price': [14.0],
            'close_price': [15.5],
            'volume': [2000000],
        })
        
        # 测试完整分析流程
        with patch.object(analyzer.db_manager, 'execute_postgres_query') as mock_query:
            mock_query.return_value = minimal_data
            
            result = analyzer.analyze_stock('000001.SZ', '2024-01-01')
            
            assert isinstance(result, dict)
            assert 'trend_score' in result
            assert 'momentum_score' in result
            assert 'volume_health_score' in result
            assert 'patterns' in result
            assert 'support' in result
            assert 'resistance' in result
    
    def test_error_fix_verification(self):
        """验证错误修复是否有效"""
        # 测试所有修复的问题
        test_cases = [
            # 最小数据测试
            pd.DataFrame({'close_price': [15.0]}),
            # 空数据测试
            pd.DataFrame(),
            # 包含NaN的数据测试
            pd.DataFrame({
                'close_price': [15.0, 16.0],
                'rsi': [np.nan, 60.0],
                'ema_12': [np.nan, np.nan],
            }),
            # 缺少字段的数据测试
            pd.DataFrame({
                'close_price': [15.0, 16.0, 17.0],
            }),
        ]
        
        analyzer = TechnicalAnalyzer()
        
        for test_data in test_cases:
            # 所有测试都应该通过而不崩溃
            trend_score = analyzer.calculate_trend_score(test_data)
            momentum_score = analyzer.calculate_momentum_score(test_data)
            volume_score = analyzer.calculate_volume_health_score(test_data)
            
            # 验证结果
            for score in [trend_score, momentum_score, volume_score]:
                assert isinstance(score, float)
                assert 0.0 <= score <= 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 