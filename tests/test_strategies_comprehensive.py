"""
策略模块全面单元测试
测试启明星策略、回测引擎、四维分析器等核心策略组件
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
from src.strategies.qiming_star.signal_fusion_engine import SignalFusionEngine, TradePlan, MarketContext


class TestQimingStarStrategy:
    """启明星策略主类测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.strategy = QimingStarStrategy()
        
        # 创建测试数据
        self.test_stock_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
            'open_price': np.random.uniform(10, 20, 30),
            'high_price': np.random.uniform(15, 25, 30),
            'low_price': np.random.uniform(5, 15, 30),
            'close_price': np.random.uniform(10, 20, 30),
            'volume': np.random.uniform(1000000, 5000000, 30),
        })
        
        self.test_market_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
            'close_price': np.random.uniform(3000, 3500, 30),
            'volume': np.random.uniform(100000000, 200000000, 30),
        })
    
    def test_initialization(self):
        """测试策略初始化"""
        assert self.strategy is not None
        assert hasattr(self.strategy, 'analyzer')
        assert hasattr(self.strategy, 'signal_engine')
        assert hasattr(self.strategy, 'backtest_engine')
        assert hasattr(self.strategy, 'config')
    
    def test_analyze_stock_success(self):
        """测试单只股票分析成功"""
        result = self.strategy.analyze_stock(
            '000001.SZ',
            self.test_stock_data,
            self.test_market_data
        )
        
        assert isinstance(result, dict)
        assert 'stock_code' in result
        assert 'current_price' in result
        assert 'profiles' in result
        assert 'trade_plan' in result
        assert 'market_context' in result
        assert 'analysis_time' in result
    
    def test_analyze_stock_without_market_data(self):
        """测试无市场数据的股票分析"""
        result = self.strategy.analyze_stock(
            '000001.SZ',
            self.test_stock_data
        )
        
        assert isinstance(result, dict)
        assert 'stock_code' in result
        assert 'current_price' in result
        assert 'profiles' in result
        assert 'trade_plan' in result
    
    def test_batch_analyze_success(self):
        """测试批量分析成功"""
        stock_data_dict = {
            '000001.SZ': self.test_stock_data,
            '000002.SZ': self.test_stock_data.copy()
        }
        
        result = self.strategy.batch_analyze(stock_data_dict, self.test_market_data)
        
        assert isinstance(result, dict)
        assert 's_class' in result
        assert 'a_class' in result
        assert isinstance(result['s_class'], list)
        assert isinstance(result['a_class'], list)
    
    def test_batch_analyze_empty_data(self):
        """测试空数据批量分析"""
        result = self.strategy.batch_analyze({}, self.test_market_data)
        
        assert isinstance(result, dict)
        assert result['s_class'] == []
        assert result['a_class'] == []
    
    def test_run_backtest_success(self):
        """测试回测运行成功"""
        stock_data_dict = {
            '000001.SZ': self.test_stock_data,
            '000002.SZ': self.test_stock_data.copy()
        }
        
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 30)
        
        result = self.strategy.run_backtest(
            stock_data_dict,
            start_date,
            end_date
        )
        
        assert isinstance(result, dict)
        # 应该包含启明星策略的回测结果
        assert '启明星策略' in result
    
    def test_run_backtest_with_benchmarks(self):
        """测试带基准策略的回测"""
        stock_data_dict = {
            '000001.SZ': self.test_stock_data,
            '000002.SZ': self.test_stock_data.copy()
        }
        
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 30)
        benchmark_strategies = ['简单移动平均', 'RSI策略']
        
        result = self.strategy.run_backtest(
            stock_data_dict,
            start_date,
            end_date,
            benchmark_strategies
        )
        
        assert isinstance(result, dict)
        assert '启明星策略' in result
        assert '简单移动平均' in result
        assert 'RSI策略' in result
    
    def test_generate_daily_signals(self):
        """测试生成每日信号"""
        result = self.strategy.generate_daily_signals()
        
        assert isinstance(result, dict)
        assert 's_class' in result
        assert 'a_class' in result
        assert isinstance(result['s_class'], list)
        assert isinstance(result['a_class'], list)
    
    def test_update_config(self):
        """测试配置更新"""
        new_config = {
            'weights': {
                'technical': 0.4,
                'capital_flow': 0.4,
                'relative_strength': 0.15,
                'catalyst': 0.05
            },
            'thresholds': {
                's_class': 90,
                'a_class': 75
            }
        }
        
        self.strategy.update_config(new_config)
        
        # 验证配置已更新
        assert self.strategy.config['weights']['technical'] == 0.4
        assert self.strategy.config['thresholds']['s_class'] == 90
    
    def test_get_strategy_summary(self):
        """测试获取策略摘要"""
        summary = self.strategy.get_strategy_summary()
        
        assert isinstance(summary, dict)
        assert 'strategy_name' in summary
        assert 'version' in summary
        assert 'description' in summary
        assert 'weights' in summary
        assert 'thresholds' in summary


class TestBacktestEngine:
    """回测引擎测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.engine = BacktestEngine(initial_capital=1000000)
        
        # 创建测试数据
        self.test_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'open_price': np.random.uniform(10, 20, 100),
            'high_price': np.random.uniform(15, 25, 100),
            'low_price': np.random.uniform(5, 15, 100),
            'close_price': np.random.uniform(10, 20, 100),
            'volume': np.random.uniform(1000000, 5000000, 100),
        })
    
    def test_initialization(self):
        """测试回测引擎初始化"""
        assert self.engine is not None
        assert self.engine.initial_capital == 1000000
        assert hasattr(self.engine, 'commission_rate')
        assert hasattr(self.engine, 'slippage')
    
    def test_simple_moving_average_strategy(self):
        """测试简单移动平均策略"""
        result = self.engine.simple_moving_average_strategy(
            self.test_data,
            short_window=5,
            long_window=20
        )
        
        assert isinstance(result, dict)
        assert 'trades' in result
        assert 'equity_curve' in result
        assert 'performance_metrics' in result
        assert isinstance(result['trades'], list)
        assert isinstance(result['equity_curve'], pd.DataFrame)
        assert isinstance(result['performance_metrics'], dict)
    
    def test_rsi_strategy(self):
        """测试RSI策略"""
        result = self.engine.rsi_strategy(
            self.test_data,
            rsi_period=14,
            oversold=30,
            overbought=70
        )
        
        assert isinstance(result, dict)
        assert 'trades' in result
        assert 'equity_curve' in result
        assert 'performance_metrics' in result
    
    def test_buy_and_hold_strategy(self):
        """测试买入持有策略"""
        result = self.engine.buy_and_hold_strategy(self.test_data)
        
        assert isinstance(result, dict)
        assert 'trades' in result
        assert 'equity_curve' in result
        assert 'performance_metrics' in result
    
    def test_qiming_star_strategy(self):
        """测试启明星策略"""
        result = self.engine.qiming_star_strategy(
            self.test_data,
            weights={
                'technical': 0.35,
                'capital_flow': 0.45,
                'relative_strength': 0.15,
                'catalyst': 0.05
            },
            thresholds={
                's_class': 90,
                'a_class': 75
            }
        )
        
        assert isinstance(result, dict)
        assert 'trades' in result
        assert 'equity_curve' in result
        assert 'performance_metrics' in result
    
    def test_compare_strategies(self):
        """测试策略对比"""
        strategies_config = [
            {
                'name': '启明星策略',
                'params': {
                    'weights': {
                        'technical': 0.35,
                        'capital_flow': 0.45,
                        'relative_strength': 0.15,
                        'catalyst': 0.05
                    }
                }
            },
            {
                'name': '简单移动平均',
                'params': {
                    'short_window': 5,
                    'long_window': 20
                }
            },
            {
                'name': 'RSI策略',
                'params': {
                    'rsi_period': 14,
                    'oversold': 30,
                    'overbought': 70
                }
            }
        ]
        
        stock_data_dict = {
            '000001.SZ': self.test_data,
            '000002.SZ': self.test_data.copy()
        }
        
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 30)
        
        results = self.engine.compare_strategies(
            strategies_config,
            stock_data_dict,
            start_date,
            end_date
        )
        
        assert isinstance(results, dict)
        assert '启明星策略' in results
        assert '简单移动平均' in results
        assert 'RSI策略' in results
    
    def test_calculate_performance_metrics(self):
        """测试性能指标计算"""
        # 创建模拟交易记录
        trades = [
            {
                'entry_date': datetime(2024, 1, 1),
                'exit_date': datetime(2024, 1, 10),
                'entry_price': 15.0,
                'exit_price': 16.0,
                'quantity': 1000,
                'pnl': 1000,
                'return': 0.067
            },
            {
                'entry_date': datetime(2024, 1, 15),
                'exit_date': datetime(2024, 1, 25),
                'entry_price': 16.0,
                'exit_price': 15.5,
                'quantity': 1000,
                'pnl': -500,
                'return': -0.031
            }
        ]
        
        equity_curve = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=30, freq='D'),
            'equity': np.linspace(1000000, 1100000, 30)
        })
        
        metrics = self.engine.calculate_performance_metrics(trades, equity_curve)
        
        assert isinstance(metrics, dict)
        assert 'total_return' in metrics
        assert 'annualized_return' in metrics
        assert 'sharpe_ratio' in metrics
        assert 'max_drawdown' in metrics
        assert 'win_rate' in metrics
        assert 'profit_factor' in metrics
    
    def test_risk_management(self):
        """测试风险管理"""
        # 测试止损
        stop_loss_result = self.engine.apply_stop_loss(
            self.test_data,
            stop_loss_pct=0.05
        )
        
        assert isinstance(stop_loss_result, pd.DataFrame)
        assert len(stop_loss_result) <= len(self.test_data)
        
        # 测试止盈
        take_profit_result = self.engine.apply_take_profit(
            self.test_data,
            take_profit_pct=0.10
        )
        
        assert isinstance(take_profit_result, pd.DataFrame)
        assert len(take_profit_result) <= len(self.test_data)


class TestFourDimensionalAnalyzer:
    """四维分析器测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.analyzer = FourDimensionalAnalyzer()
        
        # 创建测试数据
        self.test_stock_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
            'open_price': np.random.uniform(10, 20, 30),
            'high_price': np.random.uniform(15, 25, 30),
            'low_price': np.random.uniform(5, 15, 30),
            'close_price': np.random.uniform(10, 20, 30),
            'volume': np.random.uniform(1000000, 5000000, 30),
        })
        
        self.test_market_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
            'close_price': np.random.uniform(3000, 3500, 30),
            'volume': np.random.uniform(100000000, 200000000, 30),
        })
        
        self.test_sector_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
            'close_price': np.random.uniform(1000, 1200, 30),
            'volume': np.random.uniform(50000000, 100000000, 30),
        })
    
    def test_initialization(self):
        """测试分析器初始化"""
        assert self.analyzer is not None
        assert hasattr(self.analyzer, 'technical_analyzer')
        assert hasattr(self.analyzer, 'capital_flow_analyzer')
        assert hasattr(self.analyzer, 'fundamental_analyzer')
        assert hasattr(self.analyzer, 'macro_analyzer')
    
    def test_analyze_all_dimensions(self):
        """测试四维分析"""
        profiles = self.analyzer.analyze_all_dimensions(
            self.test_stock_data,
            self.test_market_data,
            self.test_sector_data
        )
        
        assert isinstance(profiles, dict)
        assert 'technical' in profiles
        assert 'capital_flow' in profiles
        assert 'relative_strength' in profiles
        assert 'catalyst' in profiles
    
    def test_analyze_technical_dimension(self):
        """测试技术分析维度"""
        profile = self.analyzer.analyze_technical_dimension(self.test_stock_data)
        
        assert isinstance(profile, dict)
        assert 'trend_score' in profile
        assert 'momentum_score' in profile
        assert 'volume_health_score' in profile
        assert 'patterns' in profile
        assert 'support_resistance' in profile
        assert 'overall_score' in profile
    
    def test_analyze_capital_flow_dimension(self):
        """测试资金流分析维度"""
        profile = self.analyzer.analyze_capital_flow_dimension(self.test_stock_data)
        
        assert isinstance(profile, dict)
        assert 'main_force_score' in profile
        assert 'retail_sentiment_score' in profile
        assert 'institutional_activity' in profile
        assert 'flow_consistency' in profile
        assert 'volume_price_correlation' in profile
        assert 'overall_score' in profile
    
    def test_analyze_relative_strength_dimension(self):
        """测试相对强度分析维度"""
        profile = self.analyzer.analyze_relative_strength_dimension(
            self.test_stock_data,
            self.test_market_data,
            self.test_sector_data
        )
        
        assert isinstance(profile, dict)
        assert 'vs_market_score' in profile
        assert 'vs_sector_score' in profile
        assert 'momentum_rank' in profile
        assert 'overall_score' in profile
    
    def test_analyze_catalyst_dimension(self):
        """测试催化剂分析维度"""
        profile = self.analyzer.analyze_catalyst_dimension(self.test_stock_data)
        
        assert isinstance(profile, dict)
        assert 'news_sentiment' in profile
        assert 'earnings_events' in profile
        assert 'policy_impact' in profile
        assert 'overall_score' in profile
    
    def test_calculate_composite_score(self):
        """测试综合评分计算"""
        profiles = {
            'technical': {'overall_score': 0.8},
            'capital_flow': {'overall_score': 0.9},
            'relative_strength': {'overall_score': 0.7},
            'catalyst': {'overall_score': 0.6}
        }
        
        weights = {
            'technical': 0.35,
            'capital_flow': 0.45,
            'relative_strength': 0.15,
            'catalyst': 0.05
        }
        
        composite_score = self.analyzer.calculate_composite_score(profiles, weights)
        
        assert isinstance(composite_score, float)
        assert 0.0 <= composite_score <= 1.0
    
    def test_analyze_with_missing_data(self):
        """测试缺失数据分析"""
        # 只有股票数据，没有市场和行业数据
        profiles = self.analyzer.analyze_all_dimensions(self.test_stock_data)
        
        assert isinstance(profiles, dict)
        assert 'technical' in profiles
        assert 'capital_flow' in profiles
        assert 'relative_strength' in profiles
        assert 'catalyst' in profiles
    
    def test_analyze_empty_data(self):
        """测试空数据分析"""
        empty_df = pd.DataFrame()
        
        profiles = self.analyzer.analyze_all_dimensions(empty_df)
        
        assert isinstance(profiles, dict)
        # 空数据时所有维度都应该返回默认值
        for dimension in ['technical', 'capital_flow', 'relative_strength', 'catalyst']:
            assert dimension in profiles
            assert profiles[dimension]['overall_score'] == 0.0


class TestSignalFusionEngine:
    """信号融合引擎测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.engine = SignalFusionEngine()
        
        # 创建测试数据
        self.test_profiles = {
            'technical': {
                'overall_score': 0.8,
                'trend_score': 0.9,
                'momentum_score': 0.7,
                'volume_health_score': 0.8
            },
            'capital_flow': {
                'overall_score': 0.9,
                'main_force_score': 0.95,
                'retail_sentiment_score': 0.8,
                'institutional_activity': 0.9
            },
            'relative_strength': {
                'overall_score': 0.7,
                'vs_market_score': 0.8,
                'vs_sector_score': 0.6
            },
            'catalyst': {
                'overall_score': 0.6,
                'news_sentiment': 0.7,
                'earnings_events': 0.5
            }
        }
        
        self.test_market_context = MarketContext(
            market_trend='bullish',
            volatility='medium',
            sector_performance='positive',
            market_sentiment='positive'
        )
    
    def test_initialization(self):
        """测试引擎初始化"""
        assert self.engine is not None
        assert hasattr(self.engine, 'weights')
        assert hasattr(self.engine, 'quality_thresholds')
        assert hasattr(self.engine, 'signal_rules')
    
    def test_generate_trade_plan_success(self):
        """测试生成交易计划成功"""
        trade_plan = self.engine.generate_trade_plan(
            stock_code='000001.SZ',
            stock_name='平安银行',
            current_price=15.5,
            profiles=self.test_profiles,
            market_context=self.test_market_context
        )
        
        assert isinstance(trade_plan, TradePlan)
        assert trade_plan.stock_code == '000001.SZ'
        assert trade_plan.stock_name == '平安银行'
        assert trade_plan.current_price == 15.5
        assert trade_plan.signal_type in ['buy', 'sell', 'hold']
        assert trade_plan.confidence_score >= 0.0
        assert trade_plan.confidence_score <= 1.0
    
    def test_generate_trade_plan_s_class_signal(self):
        """测试生成S级信号"""
        # 创建高分数据
        high_score_profiles = {
            'technical': {'overall_score': 0.95},
            'capital_flow': {'overall_score': 0.95},
            'relative_strength': {'overall_score': 0.9},
            'catalyst': {'overall_score': 0.8}
        }
        
        trade_plan = self.engine.generate_trade_plan(
            stock_code='000001.SZ',
            stock_name='平安银行',
            current_price=15.5,
            profiles=high_score_profiles,
            market_context=self.test_market_context
        )
        
        assert trade_plan.signal_type == 'buy'
        assert trade_plan.confidence_score >= 0.9
    
    def test_generate_trade_plan_a_class_signal(self):
        """测试生成A级信号"""
        # 创建中等分数数据
        medium_score_profiles = {
            'technical': {'overall_score': 0.8},
            'capital_flow': {'overall_score': 0.8},
            'relative_strength': {'overall_score': 0.7},
            'catalyst': {'overall_score': 0.6}
        }
        
        trade_plan = self.engine.generate_trade_plan(
            stock_code='000001.SZ',
            stock_name='平安银行',
            current_price=15.5,
            profiles=medium_score_profiles,
            market_context=self.test_market_context
        )
        
        assert trade_plan.signal_type in ['buy', 'hold']
        assert trade_plan.confidence_score >= 0.75
    
    def test_generate_trade_plan_hold_signal(self):
        """测试生成持有信号"""
        # 创建低分数据
        low_score_profiles = {
            'technical': {'overall_score': 0.3},
            'capital_flow': {'overall_score': 0.4},
            'relative_strength': {'overall_score': 0.3},
            'catalyst': {'overall_score': 0.2}
        }
        
        trade_plan = self.engine.generate_trade_plan(
            stock_code='000001.SZ',
            stock_name='平安银行',
            current_price=15.5,
            profiles=low_score_profiles,
            market_context=self.test_market_context
        )
        
        assert trade_plan.signal_type == 'hold'
        assert trade_plan.confidence_score < 0.75
    
    def test_batch_generate_signals(self):
        """测试批量生成信号"""
        formatted_data = {
            '000001.SZ': {
                'data': pd.DataFrame({'close': [15.5]}),
                'name': '平安银行',
                'price': 15.5
            },
            '000002.SZ': {
                'data': pd.DataFrame({'close': [20.0]}),
                'name': '万科A',
                'price': 20.0
            }
        }
        
        signals = self.engine.batch_generate_signals(
            formatted_data,
            self.test_market_context
        )
        
        assert isinstance(signals, dict)
        assert 's_class' in signals
        assert 'a_class' in signals
        assert isinstance(signals['s_class'], list)
        assert isinstance(signals['a_class'], list)
    
    def test_get_market_context(self):
        """测试获取市场环境"""
        market_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
            'close_price': np.random.uniform(3000, 3500, 30),
            'volume': np.random.uniform(100000000, 200000000, 30),
        })
        
        context = self.engine.get_market_context(market_data)
        
        assert isinstance(context, MarketContext)
        assert context.market_trend in ['bullish', 'bearish', 'sideways']
        assert context.volatility in ['low', 'medium', 'high']
        assert context.market_sentiment in ['positive', 'neutral', 'negative']
    
    def test_apply_market_adjustments(self):
        """测试市场环境调整"""
        # 牛市环境
        bullish_context = MarketContext(
            market_trend='bullish',
            volatility='low',
            sector_performance='positive',
            market_sentiment='positive'
        )
        
        adjusted_score = self.engine.apply_market_adjustments(
            0.8,
            bullish_context
        )
        
        assert isinstance(adjusted_score, float)
        assert adjusted_score >= 0.8  # 牛市应该提升分数
    
    def test_validate_signal_quality(self):
        """测试信号质量验证"""
        # 高质量信号
        high_quality_signal = TradePlan(
            stock_code='000001.SZ',
            stock_name='平安银行',
            current_price=15.5,
            signal_type='buy',
            confidence_score=0.95,
            entry_price=15.5,
            stop_loss=14.7,
            take_profit=17.0,
            reasoning='Strong technical and capital flow signals'
        )
        
        is_valid = self.engine.validate_signal_quality(high_quality_signal)
        assert is_valid == True
        
        # 低质量信号
        low_quality_signal = TradePlan(
            stock_code='000001.SZ',
            stock_name='平安银行',
            current_price=15.5,
            signal_type='buy',
            confidence_score=0.3,
            entry_price=15.5,
            stop_loss=14.7,
            take_profit=17.0,
            reasoning='Weak signals across all dimensions'
        )
        
        is_valid = self.engine.validate_signal_quality(low_quality_signal)
        assert is_valid == False


class TestTradePlan:
    """交易计划测试"""
    
    def test_trade_plan_creation(self):
        """测试交易计划创建"""
        trade_plan = TradePlan(
            stock_code='000001.SZ',
            stock_name='平安银行',
            current_price=15.5,
            signal_type='buy',
            confidence_score=0.9,
            entry_price=15.5,
            stop_loss=14.7,
            take_profit=17.0,
            reasoning='Strong technical and capital flow signals'
        )
        
        assert trade_plan.stock_code == '000001.SZ'
        assert trade_plan.stock_name == '平安银行'
        assert trade_plan.current_price == 15.5
        assert trade_plan.signal_type == 'buy'
        assert trade_plan.confidence_score == 0.9
        assert trade_plan.entry_price == 15.5
        assert trade_plan.stop_loss == 14.7
        assert trade_plan.take_profit == 17.0
        assert trade_plan.reasoning == 'Strong technical and capital flow signals'
    
    def test_trade_plan_validation(self):
        """测试交易计划验证"""
        # 有效交易计划
        valid_plan = TradePlan(
            stock_code='000001.SZ',
            stock_name='平安银行',
            current_price=15.5,
            signal_type='buy',
            confidence_score=0.9,
            entry_price=15.5,
            stop_loss=14.7,
            take_profit=17.0,
            reasoning='Valid plan'
        )
        
        assert valid_plan.is_valid() == True
        
        # 无效交易计划（止损高于入场价）
        invalid_plan = TradePlan(
            stock_code='000001.SZ',
            stock_name='平安银行',
            current_price=15.5,
            signal_type='buy',
            confidence_score=0.9,
            entry_price=15.5,
            stop_loss=16.0,  # 止损高于入场价
            take_profit=17.0,
            reasoning='Invalid plan'
        )
        
        assert invalid_plan.is_valid() == False
    
    def test_trade_plan_risk_reward_ratio(self):
        """测试风险收益比计算"""
        trade_plan = TradePlan(
            stock_code='000001.SZ',
            stock_name='平安银行',
            current_price=15.5,
            signal_type='buy',
            confidence_score=0.9,
            entry_price=15.5,
            stop_loss=14.7,
            take_profit=17.0,
            reasoning='Test plan'
        )
        
        risk_reward_ratio = trade_plan.calculate_risk_reward_ratio()
        
        assert isinstance(risk_reward_ratio, float)
        assert risk_reward_ratio > 0


class TestMarketContext:
    """市场环境测试"""
    
    def test_market_context_creation(self):
        """测试市场环境创建"""
        context = MarketContext(
            market_trend='bullish',
            volatility='medium',
            sector_performance='positive',
            market_sentiment='positive'
        )
        
        assert context.market_trend == 'bullish'
        assert context.volatility == 'medium'
        assert context.sector_performance == 'positive'
        assert context.market_sentiment == 'positive'
    
    def test_market_context_validation(self):
        """测试市场环境验证"""
        # 有效市场环境
        valid_context = MarketContext(
            market_trend='bullish',
            volatility='medium',
            sector_performance='positive',
            market_sentiment='positive'
        )
        
        assert valid_context.is_valid() == True
        
        # 无效市场环境
        invalid_context = MarketContext(
            market_trend='invalid',
            volatility='medium',
            sector_performance='positive',
            market_sentiment='positive'
        )
        
        assert invalid_context.is_valid() == False


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 