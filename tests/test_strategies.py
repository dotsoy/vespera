"""
策略模块单元测试
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path
from datetime import datetime, timedelta

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.strategies.qiming_star.qiming_star_strategy import QimingStarStrategy
from src.strategies.qiming_star.backtest_engine import BacktestEngine
from src.strategies.qiming_star.four_dimensional_analyzer import FourDimensionalAnalyzer
from src.strategies.qiming_star.signal_fusion_engine import SignalFusionEngine


class TestQimingStarStrategy:
    """启明星策略测试类"""
    
    @pytest.fixture
    def strategy(self):
        """创建启明星策略实例"""
        return QimingStarStrategy()
    
    @pytest.fixture
    def sample_stock_data(self):
        """创建样本股票数据"""
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        data = {
            'trade_date': dates,
            'open_price': np.random.uniform(10, 20, 30),
            'high_price': np.random.uniform(15, 25, 30),
            'low_price': np.random.uniform(5, 15, 30),
            'close_price': np.random.uniform(10, 20, 30),
            'volume': np.random.uniform(1000000, 5000000, 30),
            'main_net_inflow': np.random.uniform(-10000000, 10000000, 30),
            'super_large_net_inflow': np.random.uniform(-5000000, 5000000, 30),
            'retail_net_inflow': np.random.uniform(-2000000, 2000000, 30),
            'total_amount': np.random.uniform(50000000, 200000000, 30),
        }
        return pd.DataFrame(data)
    
    def test_initialization(self, strategy):
        """测试初始化"""
        assert strategy is not None
        assert hasattr(strategy, 'analyze_stock')
        assert hasattr(strategy, 'analyze_multiple_stocks')
        assert hasattr(strategy, 'generate_signals')
    
    def test_analyze_stock_with_valid_data(self, strategy):
        """测试有效数据的股票分析"""
        with patch.object(strategy, '_get_stock_data') as mock_get_data:
            mock_get_data.return_value = pd.DataFrame({
                'trade_date': ['2024-01-01', '2024-01-02'],
                'close_price': [15.0, 16.0],
                'volume': [1000000, 1200000],
                'main_net_inflow': [1000000, 2000000],
                'total_amount': [100000000, 120000000],
            })
            
            result = strategy.analyze_stock('000001.SZ')
            
            assert isinstance(result, dict)
            assert 'signal' in result
            assert 'score' in result
            assert 'analysis' in result
    
    def test_analyze_stock_with_no_data(self, strategy):
        """测试无数据时的股票分析"""
        with patch.object(strategy, '_get_stock_data') as mock_get_data:
            mock_get_data.return_value = pd.DataFrame()
            
            result = strategy.analyze_stock('000001.SZ')
            
            assert isinstance(result, dict)
            assert result['signal'] == 'HOLD'
            assert result['score'] == 0.0
    
    def test_analyze_stock_with_error(self, strategy):
        """测试错误时的股票分析"""
        with patch.object(strategy, '_get_stock_data') as mock_get_data:
            mock_get_data.side_effect = Exception("数据获取失败")
            
            result = strategy.analyze_stock('000001.SZ')
            
            assert isinstance(result, dict)
            assert result['signal'] == 'HOLD'
            assert result['score'] == 0.0
    
    def test_analyze_multiple_stocks(self, strategy):
        """测试多股票分析"""
        with patch.object(strategy, 'analyze_stock') as mock_analyze:
            mock_analyze.return_value = {
                'signal': 'BUY',
                'score': 85.0,
                'analysis': {'technical': 80, 'capital': 90}
            }
            
            symbols = ['000001.SZ', '000002.SZ', '600000.SH']
            results = strategy.analyze_multiple_stocks(symbols)
            
            assert isinstance(results, dict)
            assert len(results) == 3
            assert all(symbol in results for symbol in symbols)
    
    def test_generate_signals(self, strategy):
        """测试信号生成"""
        with patch.object(strategy, 'analyze_multiple_stocks') as mock_analyze:
            mock_analyze.return_value = {
                '000001.SZ': {'signal': 'BUY', 'score': 85.0},
                '000002.SZ': {'signal': 'SELL', 'score': 75.0},
                '600000.SH': {'signal': 'HOLD', 'score': 50.0},
            }
            
            signals = strategy.generate_signals(['000001.SZ', '000002.SZ', '600000.SH'])
            
            assert isinstance(signals, dict)
            assert 'buy_signals' in signals
            assert 'sell_signals' in signals
            assert 'hold_signals' in signals
    
    def test_signal_classification(self, strategy):
        """测试信号分类"""
        # 测试S级信号
        result = strategy._classify_signal(95.0)
        assert result == 'S'
        
        # 测试A级信号
        result = strategy._classify_signal(85.0)
        assert result == 'A'
        
        # 测试B级信号
        result = strategy._classify_signal(75.0)
        assert result == 'B'
        
        # 测试C级信号
        result = strategy._classify_signal(65.0)
        assert result == 'C'
        
        # 测试D级信号
        result = strategy._classify_signal(45.0)
        assert result == 'D'


class TestBacktestEngine:
    """回测引擎测试类"""
    
    @pytest.fixture
    def engine(self):
        """创建回测引擎实例"""
        return BacktestEngine()
    
    @pytest.fixture
    def sample_backtest_data(self):
        """创建样本回测数据"""
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        data = {
            'trade_date': dates,
            'open_price': np.random.uniform(10, 20, 100),
            'high_price': np.random.uniform(15, 25, 100),
            'low_price': np.random.uniform(5, 15, 100),
            'close_price': np.random.uniform(10, 20, 100),
            'volume': np.random.uniform(1000000, 5000000, 100),
        }
        return pd.DataFrame(data)
    
    def test_initialization(self, engine):
        """测试初始化"""
        assert engine is not None
        assert hasattr(engine, 'run_backtest')
        assert hasattr(engine, 'calculate_metrics')
        assert hasattr(engine, 'generate_report')
    
    def test_run_backtest_with_valid_data(self, engine, sample_backtest_data):
        """测试有效数据的回测"""
        signals = pd.DataFrame({
            'trade_date': sample_backtest_data['trade_date'][:10],
            'signal': ['BUY', 'HOLD', 'SELL', 'BUY', 'HOLD', 'SELL', 'BUY', 'HOLD', 'SELL', 'BUY'],
            'price': [15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0],
        })
        
        result = engine.run_backtest(sample_backtest_data, signals)
        
        assert isinstance(result, dict)
        assert 'trades' in result
        assert 'metrics' in result
        assert 'equity_curve' in result
    
    def test_run_backtest_with_no_signals(self, engine, sample_backtest_data):
        """测试无信号的回测"""
        signals = pd.DataFrame()
        
        result = engine.run_backtest(sample_backtest_data, signals)
        
        assert isinstance(result, dict)
        assert len(result['trades']) == 0
        assert result['metrics']['total_return'] == 0.0
    
    def test_calculate_metrics(self, engine):
        """测试指标计算"""
        trades = pd.DataFrame({
            'entry_date': ['2024-01-01', '2024-01-05', '2024-01-10'],
            'exit_date': ['2024-01-03', '2024-01-08', '2024-01-15'],
            'entry_price': [15.0, 16.0, 17.0],
            'exit_price': [16.0, 15.5, 18.0],
            'profit_loss': [1.0, -0.5, 1.0],
            'return_pct': [6.67, -3.13, 5.88],
        })
        
        metrics = engine.calculate_metrics(trades)
        
        assert isinstance(metrics, dict)
        assert 'total_return' in metrics
        assert 'sharpe_ratio' in metrics
        assert 'max_drawdown' in metrics
        assert 'win_rate' in metrics
    
    def test_generate_report(self, engine):
        """测试报告生成"""
        trades = pd.DataFrame({
            'entry_date': ['2024-01-01', '2024-01-05'],
            'exit_date': ['2024-01-03', '2024-01-08'],
            'entry_price': [15.0, 16.0],
            'exit_price': [16.0, 15.5],
            'profit_loss': [1.0, -0.5],
            'return_pct': [6.67, -3.13],
        })
        
        metrics = {
            'total_return': 0.5,
            'sharpe_ratio': 1.2,
            'max_drawdown': -0.1,
            'win_rate': 0.6,
        }
        
        report = engine.generate_report(trades, metrics)
        
        assert isinstance(report, dict)
        assert 'summary' in report
        assert 'trades' in report
        assert 'charts' in report
    
    def test_position_sizing(self, engine):
        """测试仓位管理"""
        # 测试固定仓位
        position = engine._calculate_position_size(10000, 0.1, 'fixed')
        assert position == 1000
        
        # 测试百分比仓位
        position = engine._calculate_position_size(10000, 0.1, 'percentage')
        assert position == 1000
        
        # 测试风险仓位
        position = engine._calculate_position_size(10000, 0.1, 'risk_based')
        assert position > 0
    
    def test_stop_loss_handling(self, engine):
        """测试止损处理"""
        entry_price = 15.0
        stop_loss_pct = 0.05
        
        stop_loss_price = engine._calculate_stop_loss(entry_price, stop_loss_pct)
        assert stop_loss_price == 14.25
        
        # 测试触发止损
        current_price = 14.0
        should_stop = engine._check_stop_loss(current_price, stop_loss_price)
        assert should_stop is True


class TestFourDimensionalAnalyzer:
    """四维分析器测试类"""
    
    @pytest.fixture
    def analyzer(self):
        """创建四维分析器实例"""
        return FourDimensionalAnalyzer()
    
    @pytest.fixture
    def sample_analysis_data(self):
        """创建样本分析数据"""
        data = {
            'close_price': [15.0, 16.0, 17.0, 18.0, 19.0],
            'volume': [1000000, 1200000, 1100000, 1300000, 1400000],
            'main_net_inflow': [1000000, 2000000, 1500000, 3000000, 2500000],
            'total_amount': [100000000, 120000000, 110000000, 140000000, 130000000],
        }
        return pd.DataFrame(data)
    
    def test_initialization(self, analyzer):
        """测试初始化"""
        assert analyzer is not None
        assert hasattr(analyzer, 'analyze_technical')
        assert hasattr(analyzer, 'analyze_capital')
        assert hasattr(analyzer, 'analyze_relative_strength')
        assert hasattr(analyzer, 'analyze_catalyst')
    
    def test_analyze_technical(self, analyzer, sample_analysis_data):
        """测试技术分析"""
        with patch('src.analyzers.technical_analyzer.TechnicalAnalyzer') as mock_tech:
            mock_analyzer = Mock()
            mock_analyzer.analyze_stock.return_value = {
                'trend_score': 0.8,
                'momentum_score': 0.7,
                'volume_health_score': 0.6,
            }
            mock_tech.return_value = mock_analyzer
            
            result = analyzer.analyze_technical('000001.SZ', '2024-01-05')
            
            assert isinstance(result, dict)
            assert 'trend_score' in result
            assert 'momentum_score' in result
            assert 'volume_health_score' in result
    
    def test_analyze_capital(self, analyzer, sample_analysis_data):
        """测试资金分析"""
        with patch('src.analyzers.capital_flow_analyzer.CapitalFlowAnalyzer') as mock_capital:
            mock_analyzer = Mock()
            mock_analyzer.analyze_stock.return_value = {
                'main_force_score': 0.9,
                'retail_sentiment_score': 0.6,
                'institutional_activity': 0.8,
            }
            mock_capital.return_value = mock_analyzer
            
            result = analyzer.analyze_capital('000001.SZ', '2024-01-05')
            
            assert isinstance(result, dict)
            assert 'main_force_score' in result
            assert 'retail_sentiment_score' in result
            assert 'institutional_activity' in result
    
    def test_analyze_relative_strength(self, analyzer):
        """测试相对强度分析"""
        result = analyzer.analyze_relative_strength('000001.SZ', '2024-01-05')
        
        assert isinstance(result, dict)
        assert 'vs_market' in result
        assert 'vs_sector' in result
        assert 'vs_peers' in result
    
    def test_analyze_catalyst(self, analyzer):
        """测试催化剂分析"""
        result = analyzer.analyze_catalyst('000001.SZ', '2024-01-05')
        
        assert isinstance(result, dict)
        assert 'events' in result
        assert 'sentiment' in result
        assert 'impact' in result
    
    def test_comprehensive_analysis(self, analyzer):
        """测试综合分析"""
        with patch.object(analyzer, 'analyze_technical') as mock_tech:
            with patch.object(analyzer, 'analyze_capital') as mock_capital:
                with patch.object(analyzer, 'analyze_relative_strength') as mock_rs:
                    with patch.object(analyzer, 'analyze_catalyst') as mock_cat:
                        
                        mock_tech.return_value = {'trend_score': 0.8}
                        mock_capital.return_value = {'main_force_score': 0.9}
                        mock_rs.return_value = {'vs_market': 0.7}
                        mock_cat.return_value = {'impact': 0.6}
                        
                        result = analyzer.analyze_stock('000001.SZ', '2024-01-05')
                        
                        assert isinstance(result, dict)
                        assert 'technical' in result
                        assert 'capital' in result
                        assert 'relative_strength' in result
                        assert 'catalyst' in result


class TestSignalFusionEngine:
    """信号融合引擎测试类"""
    
    @pytest.fixture
    def fusion_engine(self):
        """创建信号融合引擎实例"""
        return SignalFusionEngine()
    
    def test_initialization(self, fusion_engine):
        """测试初始化"""
        assert fusion_engine is not None
        assert hasattr(fusion_engine, 'fuse_signals')
        assert hasattr(fusion_engine, 'calculate_weighted_score')
        assert hasattr(fusion_engine, 'generate_final_signal')
    
    def test_fuse_signals(self, fusion_engine):
        """测试信号融合"""
        analysis_results = {
            'technical': {
                'trend_score': 0.8,
                'momentum_score': 0.7,
                'volume_health_score': 0.6,
            },
            'capital': {
                'main_force_score': 0.9,
                'retail_sentiment_score': 0.6,
                'institutional_activity': 0.8,
            },
            'relative_strength': {
                'vs_market': 0.7,
                'vs_sector': 0.8,
                'vs_peers': 0.6,
            },
            'catalyst': {
                'impact': 0.6,
                'sentiment': 0.5,
                'events': 0.4,
            }
        }
        
        result = fusion_engine.fuse_signals(analysis_results)
        
        assert isinstance(result, dict)
        assert 'final_score' in result
        assert 'signal' in result
        assert 'confidence' in result
        assert 'breakdown' in result
    
    def test_calculate_weighted_score(self, fusion_engine):
        """测试加权评分计算"""
        scores = {
            'technical': 0.8,
            'capital': 0.9,
            'relative_strength': 0.7,
            'catalyst': 0.6,
        }
        
        weights = {
            'technical': 0.35,
            'capital': 0.45,
            'relative_strength': 0.15,
            'catalyst': 0.05,
        }
        
        weighted_score = fusion_engine.calculate_weighted_score(scores, weights)
        
        assert isinstance(weighted_score, float)
        assert 0.0 <= weighted_score <= 1.0
        assert weighted_score > 0.0
    
    def test_generate_final_signal(self, fusion_engine):
        """测试最终信号生成"""
        # 测试买入信号
        signal = fusion_engine.generate_final_signal(0.85)
        assert signal == 'BUY'
        
        # 测试卖出信号
        signal = fusion_engine.generate_final_signal(0.25)
        assert signal == 'SELL'
        
        # 测试持有信号
        signal = fusion_engine.generate_final_signal(0.55)
        assert signal == 'HOLD'
    
    def test_signal_thresholds(self, fusion_engine):
        """测试信号阈值"""
        # 测试S级信号阈值
        assert fusion_engine.buy_threshold_s == 0.90
        assert fusion_engine.buy_threshold_a == 0.75
        
        # 测试卖出阈值
        assert fusion_engine.sell_threshold == 0.30
    
    def test_confidence_calculation(self, fusion_engine):
        """测试置信度计算"""
        scores = {
            'technical': 0.8,
            'capital': 0.9,
            'relative_strength': 0.7,
            'catalyst': 0.6,
        }
        
        confidence = fusion_engine.calculate_confidence(scores)
        
        assert isinstance(confidence, float)
        assert 0.0 <= confidence <= 1.0
    
    def test_analyze_and_generate_signal(self, fusion_engine):
        """测试分析和信号生成"""
        with patch('src.strategies.qiming_star.four_dimensional_analyzer.FourDimensionalAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.analyze_stock.return_value = {
                'technical': {'trend_score': 0.8},
                'capital': {'main_force_score': 0.9},
                'relative_strength': {'vs_market': 0.7},
                'catalyst': {'impact': 0.6},
            }
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = fusion_engine.analyze_and_generate_signal('000001.SZ', '2024-01-05')
            
            assert isinstance(result, dict)
            assert 'signal' in result
            assert 'score' in result
            assert 'confidence' in result


class TestIntegration:
    """集成测试类"""
    
    def test_strategy_backtest_integration(self):
        """测试策略和回测集成"""
        strategy = QimingStarStrategy()
        engine = BacktestEngine()
        
        # 模拟策略分析
        with patch.object(strategy, 'analyze_stock') as mock_analyze:
            mock_analyze.return_value = {
                'signal': 'BUY',
                'score': 85.0,
                'analysis': {'technical': 80, 'capital': 90}
            }
            
            # 生成信号
            signals = strategy.generate_signals(['000001.SZ'])
            
            # 运行回测
            sample_data = pd.DataFrame({
                'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
                'close_price': np.random.uniform(10, 20, 30),
                'volume': np.random.uniform(1000000, 5000000, 30),
            })
            
            signal_df = pd.DataFrame({
                'trade_date': sample_data['trade_date'][:5],
                'signal': ['BUY', 'HOLD', 'SELL', 'BUY', 'HOLD'],
                'price': [15.0, 16.0, 17.0, 18.0, 19.0],
            })
            
            result = engine.run_backtest(sample_data, signal_df)
            
            assert isinstance(result, dict)
            assert 'trades' in result
            assert 'metrics' in result
    
    def test_analyzer_fusion_integration(self):
        """测试分析器和融合引擎集成"""
        analyzer = FourDimensionalAnalyzer()
        fusion_engine = SignalFusionEngine()
        
        # 模拟四维分析
        with patch.object(analyzer, 'analyze_technical') as mock_tech:
            with patch.object(analyzer, 'analyze_capital') as mock_capital:
                with patch.object(analyzer, 'analyze_relative_strength') as mock_rs:
                    with patch.object(analyzer, 'analyze_catalyst') as mock_cat:
                        
                        mock_tech.return_value = {'trend_score': 0.8}
                        mock_capital.return_value = {'main_force_score': 0.9}
                        mock_rs.return_value = {'vs_market': 0.7}
                        mock_cat.return_value = {'impact': 0.6}
                        
                        # 执行分析
                        analysis_result = analyzer.analyze_stock('000001.SZ', '2024-01-05')
                        
                        # 融合信号
                        fusion_result = fusion_engine.fuse_signals(analysis_result)
                        
                        assert isinstance(fusion_result, dict)
                        assert 'final_score' in fusion_result
                        assert 'signal' in fusion_result


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 