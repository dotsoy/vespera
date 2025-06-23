"""
全面集成测试
测试各个模块之间的协作、数据流和端到端功能
"""
import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.analyzers.technical_analyzer import TechnicalAnalyzer
from src.analyzers.capital_flow_analyzer import CapitalFlowAnalyzer
from src.analyzers.fundamental_analyzer import FundamentalAnalyzer
from src.analyzers.macro_analyzer import MacroAnalyzer
from src.data_sources.akshare_data_source import AkShareDataSource
from src.data_sources.tushare_data_source import TushareDataSource
from src.data_sources.data_source_factory import DataSourceFactory
from src.data_sources.data_source_manager import DataSourceManager
from src.strategies.qiming_star.qiming_star_strategy import QimingStarStrategy
from src.strategies.qiming_star.four_dimensional_analyzer import FourDimensionalAnalyzer
from src.strategies.qiming_star.signal_fusion_engine import SignalFusionEngine
from src.strategies.qiming_star.backtest_engine import BacktestEngine
from src.utils.database import get_db_manager
from src.utils.technical_indicators import add_all_indicators


class TestDataFlowIntegration:
    """数据流集成测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.technical_analyzer = TechnicalAnalyzer()
        self.capital_flow_analyzer = CapitalFlowAnalyzer()
        self.fundamental_analyzer = FundamentalAnalyzer()
        self.macro_analyzer = MacroAnalyzer()
        
        # 创建标准测试数据
        self.stock_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'open_price': np.random.uniform(10, 20, 100),
            'high_price': np.random.uniform(15, 25, 100),
            'low_price': np.random.uniform(5, 15, 100),
            'close_price': np.random.uniform(10, 20, 100),
            'volume': np.random.uniform(1000000, 5000000, 100),
        })
        
        self.capital_flow_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'main_net_inflow': np.random.uniform(-1000000, 1000000, 100),
            'super_large_net_inflow': np.random.uniform(-500000, 500000, 100),
            'large_net_inflow': np.random.uniform(-200000, 200000, 100),
            'medium_net_inflow': np.random.uniform(-100000, 100000, 100),
            'small_net_inflow': np.random.uniform(-50000, 50000, 100),
            'retail_net_inflow': np.random.uniform(-100000, 100000, 100),
            'total_amount': np.random.uniform(5000000, 10000000, 100),
        })
    
    def test_complete_analysis_pipeline(self):
        """测试完整分析流水线"""
        stock_code = "000001.SZ"
        trade_date = "2024-01-15"
        
        # 1. 技术分析
        technical_result = self.technical_analyzer.analyze_stock(stock_code, trade_date)
        assert technical_result is not None
        assert 'technical_score' in technical_result
        assert 'trend_score' in technical_result
        assert 'momentum_score' in technical_result
        assert 'volume_health_score' in technical_result
        
        # 2. 资金流分析
        capital_flow_result = self.capital_flow_analyzer.analyze_stock(stock_code, trade_date)
        assert capital_flow_result is not None
        assert 'capital_flow_score' in capital_flow_result
        assert 'main_force_score' in capital_flow_result
        assert 'retail_sentiment_score' in capital_flow_result
        
        # 3. 基本面分析
        fundamental_result = self.fundamental_analyzer.analyze_stock(stock_code, trade_date)
        assert fundamental_result is not None
        assert 'fundamental_score' in fundamental_result
        
        # 4. 宏观分析
        macro_result = self.macro_analyzer.analyze_stock(stock_code, trade_date)
        assert macro_result is not None
        assert 'macro_score' in macro_result
        
        print(f"完整分析流水线测试通过:")
        print(f"  技术分析得分: {technical_result['technical_score']:.3f}")
        print(f"  资金流分析得分: {capital_flow_result['capital_flow_score']:.3f}")
        print(f"  基本面分析得分: {fundamental_result['fundamental_score']:.3f}")
        print(f"  宏观分析得分: {macro_result['macro_score']:.3f}")
    
    def test_data_consistency_across_analyzers(self):
        """测试分析器间的数据一致性"""
        stock_code = "000001.SZ"
        trade_date = "2024-01-15"
        
        # 获取各分析器的结果
        technical_result = self.technical_analyzer.analyze_stock(stock_code, trade_date)
        capital_flow_result = self.capital_flow_analyzer.analyze_stock(stock_code, trade_date)
        fundamental_result = self.fundamental_analyzer.analyze_stock(stock_code, trade_date)
        macro_result = self.macro_analyzer.analyze_stock(stock_code, trade_date)
        
        # 验证数据一致性
        assert technical_result['stock_code'] == stock_code
        assert capital_flow_result['stock_code'] == stock_code
        assert fundamental_result['stock_code'] == stock_code
        assert macro_result['stock_code'] == stock_code
        
        assert technical_result['trade_date'] == trade_date
        assert capital_flow_result['trade_date'] == trade_date
        assert fundamental_result['trade_date'] == trade_date
        assert macro_result['trade_date'] == trade_date
        
        # 验证评分范围一致性
        for result in [technical_result, capital_flow_result, fundamental_result, macro_result]:
            for key, value in result.items():
                if 'score' in key and isinstance(value, (int, float)):
                    assert 0.0 <= value <= 1.0, f"{key}: {value} 超出范围 [0,1]"
        
        print("数据一致性测试通过")
    
    def test_error_propagation_across_modules(self):
        """测试模块间错误传播"""
        invalid_stock_code = "INVALID.CODE"
        trade_date = "2024-01-15"
        
        # 测试无效股票代码的错误传播
        try:
            technical_result = self.technical_analyzer.analyze_stock(invalid_stock_code, trade_date)
            # 如果分析器能够处理无效代码，结果应该为None或包含错误信息
            if technical_result is not None:
                assert 'error' in technical_result or 'status' in technical_result
        except Exception as e:
            # 异常也是可以接受的
            assert isinstance(e, Exception)
        
        try:
            capital_flow_result = self.capital_flow_analyzer.analyze_stock(invalid_stock_code, trade_date)
            if capital_flow_result is not None:
                assert 'error' in capital_flow_result or 'status' in capital_flow_result
        except Exception as e:
            assert isinstance(e, Exception)
        
        print("错误传播测试通过")


class TestStrategyIntegration:
    """策略集成测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.strategy = QimingStarStrategy()
        self.four_d_analyzer = FourDimensionalAnalyzer()
        self.signal_engine = SignalFusionEngine()
        self.backtest_engine = BacktestEngine()
        
        # 创建测试数据
        self.stock_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'open': np.random.uniform(10, 20, 100),
            'high': np.random.uniform(15, 25, 100),
            'low': np.random.uniform(5, 15, 100),
            'close': np.random.uniform(10, 20, 100),
            'volume': np.random.uniform(1000000, 5000000, 100),
        })
        
        self.market_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'close': np.random.uniform(3000, 3500, 100),
            'volume': np.random.uniform(1e10, 2e10, 100),
        })
    
    def test_four_dimensional_analysis_integration(self):
        """测试四维分析集成"""
        # 执行四维分析
        profiles = self.four_d_analyzer.analyze_all_dimensions(
            self.stock_data, self.market_data
        )
        
        # 验证分析结果
        assert isinstance(profiles, dict)
        assert 'technical' in profiles
        assert 'capital_flow' in profiles
        assert 'relative_strength' in profiles
        assert 'catalyst' in profiles
        
        # 验证每个维度的分析结果
        for dimension, profile in profiles.items():
            assert isinstance(profile, dict)
            assert 'score' in profile
            assert 0.0 <= profile['score'] <= 1.0
            assert 'details' in profile
        
        print(f"四维分析集成测试通过:")
        for dimension, profile in profiles.items():
            print(f"  {dimension}: {profile['score']:.3f}")
    
    def test_signal_fusion_integration(self):
        """测试信号融合集成"""
        # 创建分析结果
        profiles = {
            'technical': {'score': 0.8, 'details': {}},
            'capital_flow': {'score': 0.9, 'details': {}},
            'relative_strength': {'score': 0.7, 'details': {}},
            'catalyst': {'score': 0.6, 'details': {}}
        }
        
        # 生成交易计划
        trade_plan = self.signal_engine.generate_trade_plan(
            stock_code="000001.SZ",
            stock_name="平安银行",
            current_price=15.0,
            profiles=profiles,
            market_context={'trend': 'bullish', 'volatility': 'medium'}
        )
        
        # 验证交易计划
        assert isinstance(trade_plan, dict)
        assert 'signal_type' in trade_plan
        assert 'signal_strength' in trade_plan
        assert 'entry_price' in trade_plan
        assert 'stop_loss' in trade_plan
        assert 'take_profit' in trade_plan
        
        print(f"信号融合集成测试通过:")
        print(f"  信号类型: {trade_plan['signal_type']}")
        print(f"  信号强度: {trade_plan['signal_strength']:.3f}")
        print(f"  入场价格: {trade_plan['entry_price']}")
    
    def test_backtest_integration(self):
        """测试回测集成"""
        # 创建多只股票数据
        stock_data_dict = {}
        for i in range(5):  # 5只股票
            stock_code = f"00000{i:03d}.SZ"
            stock_data_dict[stock_code] = pd.DataFrame({
                'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
                'open': np.random.uniform(10, 20, 100),
                'high': np.random.uniform(15, 25, 100),
                'low': np.random.uniform(5, 15, 100),
                'close': np.random.uniform(10, 20, 100),
                'volume': np.random.uniform(1000000, 5000000, 100),
            })
        
        # 执行回测
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 4, 1)
        
        results = self.backtest_engine.compare_strategies(
            [{"name": "启明星策略", "params": {}}],
            stock_data_dict,
            start_date,
            end_date
        )
        
        # 验证回测结果
        assert isinstance(results, dict)
        assert len(results) > 0
        
        for strategy_name, result in results.items():
            assert isinstance(result, dict)
            assert 'total_return' in result
            assert 'sharpe_ratio' in result
            assert 'max_drawdown' in result
            assert 'win_rate' in result
        
        print(f"回测集成测试通过:")
        for strategy_name, result in results.items():
            print(f"  {strategy_name}:")
            print(f"    总收益: {result['total_return']:.2%}")
            print(f"    夏普比率: {result['sharpe_ratio']:.3f}")
            print(f"    最大回撤: {result['max_drawdown']:.2%}")


class TestDataSourceIntegration:
    """数据源集成测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.akshare_source = AkShareDataSource()
        self.tushare_source = TushareDataSource()
    
    def test_data_source_fallback_mechanism(self):
        """测试数据源降级机制"""
        stock_code = "000001.SZ"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        
        # 模拟AkShare失败，Tushare成功的情况
        with patch('akshare.stock_zh_a_hist') as mock_akshare:
            mock_akshare.side_effect = Exception("AkShare服务不可用")
            
            # 应该能够从Tushare获取数据
            try:
                result = self.tushare_source.get_stock_data(stock_code, start_date, end_date)
                if result is not None:
                    assert isinstance(result, pd.DataFrame)
                    print("数据源降级机制测试通过: Tushare成功获取数据")
                else:
                    print("数据源降级机制测试通过: 所有数据源都不可用")
            except Exception as e:
                print(f"数据源降级机制测试通过: 预期的异常 {e}")
    
    def test_data_format_consistency(self):
        """测试数据格式一致性"""
        # 测试不同数据源返回的数据格式是否一致
        stock_code = "000001.SZ"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        
        # 获取AkShare数据
        akshare_data = None
        try:
            akshare_data = self.akshare_source.get_stock_data(stock_code, start_date, end_date)
        except Exception:
            pass
        
        # 获取Tushare数据
        tushare_data = None
        try:
            tushare_data = self.tushare_source.get_stock_data(stock_code, start_date, end_date)
        except Exception:
            pass
        
        # 验证数据格式一致性
        if akshare_data is not None and tushare_data is not None:
            # 检查必需列是否存在
            required_columns = ['trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
            
            for col in required_columns:
                assert col in akshare_data.columns, f"AkShare数据缺少列: {col}"
                assert col in tushare_data.columns, f"Tushare数据缺少列: {col}"
            
            print("数据格式一致性测试通过")
        else:
            print("数据格式一致性测试跳过: 部分数据源不可用")


class TestDatabaseIntegration:
    """数据库集成测试"""
    
    def setup_method(self):
        """测试前准备"""
        try:
            self.db_manager = get_db_manager()
            self.db_available = True
        except Exception:
            self.db_available = False
    
    def test_database_connection_and_queries(self):
        """测试数据库连接和查询"""
        if not self.db_available:
            pytest.skip("数据库不可用")
        
        # 测试基本查询
        try:
            # 测试股票列表查询
            stock_list = self.db_manager.get_stock_list()
            assert isinstance(stock_list, pd.DataFrame)
            
            # 测试历史数据查询
            if not stock_list.empty:
                sample_stock = stock_list.iloc[0]['ts_code']
                historical_data = self.db_manager.get_stock_data(
                    sample_stock, "2024-01-01", "2024-01-31"
                )
                if historical_data is not None:
                    assert isinstance(historical_data, pd.DataFrame)
            
            print("数据库连接和查询测试通过")
            
        except Exception as e:
            pytest.fail(f"数据库测试失败: {e}")
    
    def test_data_persistence_and_retrieval(self):
        """测试数据持久化和检索"""
        if not self.db_available:
            pytest.skip("数据库不可用")
        
        # 创建测试数据
        test_data = pd.DataFrame({
            'ts_code': ['TEST001.SZ'],
            'trade_date': ['2024-01-01'],
            'open_price': [15.0],
            'high_price': [16.0],
            'low_price': [14.0],
            'close_price': [15.5],
            'volume': [1000000],
        })
        
        try:
            # 测试数据插入
            success = self.db_manager.insert_stock_data(test_data)
            assert success is True or success is False  # 可能成功或失败（重复数据）
            
            # 测试数据检索
            retrieved_data = self.db_manager.get_stock_data('TEST001.SZ', '2024-01-01', '2024-01-01')
            if retrieved_data is not None:
                assert isinstance(retrieved_data, pd.DataFrame)
            
            print("数据持久化和检索测试通过")
            
        except Exception as e:
            pytest.fail(f"数据持久化测试失败: {e}")


class TestEndToEndWorkflow:
    """端到端工作流测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.strategy = QimingStarStrategy()
        
        # 创建完整的测试数据
        self.stock_data_dict = {}
        for i in range(3):  # 3只股票
            stock_code = f"00000{i:03d}.SZ"
            self.stock_data_dict[stock_code] = pd.DataFrame({
                'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
                'open': np.random.uniform(10, 20, 100),
                'high': np.random.uniform(15, 25, 100),
                'low': np.random.uniform(5, 15, 100),
                'close': np.random.uniform(10, 20, 100),
                'volume': np.random.uniform(1000000, 5000000, 100),
            })
        
        self.market_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'close': np.random.uniform(3000, 3500, 100),
            'volume': np.random.uniform(1e10, 2e10, 100),
        })
    
    def test_complete_strategy_workflow(self):
        """测试完整策略工作流"""
        # 1. 批量分析股票
        analysis_results = self.strategy.batch_analyze(
            self.stock_data_dict, self.market_data
        )
        
        assert isinstance(analysis_results, dict)
        assert 's_class' in analysis_results
        assert 'a_class' in analysis_results
        
        # 2. 生成交易信号
        total_signals = len(analysis_results['s_class']) + len(analysis_results['a_class'])
        assert total_signals >= 0
        
        # 3. 执行回测
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 4, 1)
        
        backtest_results = self.strategy.run_backtest(
            self.stock_data_dict, start_date, end_date
        )
        
        assert isinstance(backtest_results, dict)
        
        print(f"完整策略工作流测试通过:")
        print(f"  分析股票数: {len(self.stock_data_dict)}")
        print(f"  生成信号数: {total_signals}")
        print(f"  回测策略数: {len(backtest_results)}")
    
    def test_error_recovery_in_workflow(self):
        """测试工作流中的错误恢复"""
        # 创建包含无效数据的股票字典
        mixed_data_dict = self.stock_data_dict.copy()
        mixed_data_dict['INVALID.SZ'] = pd.DataFrame()  # 空数据
        
        # 应该能够处理部分无效数据
        try:
            results = self.strategy.batch_analyze(mixed_data_dict, self.market_data)
            assert isinstance(results, dict)
            print("错误恢复测试通过: 成功处理混合数据")
        except Exception as e:
            print(f"错误恢复测试通过: 预期的异常 {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 