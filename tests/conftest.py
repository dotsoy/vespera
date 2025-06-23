"""
Pytest配置文件
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


@pytest.fixture(scope="session")
def project_root_path():
    """项目根目录路径"""
    return project_root


@pytest.fixture(scope="session")
def sample_stock_data():
    """样本股票数据"""
    return pd.DataFrame({
        'trade_date': pd.date_range('2024-01-01', periods=50, freq='D'),
        'open_price': np.random.uniform(10, 20, 50),
        'high_price': np.random.uniform(15, 25, 50),
        'low_price': np.random.uniform(5, 15, 50),
        'close_price': np.random.uniform(10, 20, 50),
        'volume': np.random.uniform(1000000, 5000000, 50),
    })


@pytest.fixture(scope="session")
def sample_market_data():
    """样本市场数据"""
    return pd.DataFrame({
        'trade_date': pd.date_range('2024-01-01', periods=50, freq='D'),
        'close_price': np.random.uniform(3000, 3500, 50),
        'volume': np.random.uniform(100000000, 200000000, 50),
    })


@pytest.fixture(scope="session")
def sample_sector_data():
    """样本行业数据"""
    return pd.DataFrame({
        'trade_date': pd.date_range('2024-01-01', periods=50, freq='D'),
        'close_price': np.random.uniform(1000, 1200, 50),
        'volume': np.random.uniform(50000000, 100000000, 50),
    })


@pytest.fixture(scope="session")
def sample_capital_flow_data():
    """样本资金流数据"""
    return pd.DataFrame({
        'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
        'main_net_inflow': np.random.uniform(-10000000, 10000000, 30),
        'super_large_net_inflow': np.random.uniform(-5000000, 5000000, 30),
        'retail_net_inflow': np.random.uniform(-2000000, 2000000, 30),
        'total_amount': np.random.uniform(100000000, 300000000, 30),
    })


@pytest.fixture(scope="session")
def sample_stock_list():
    """样本股票列表"""
    return pd.DataFrame({
        'code': ['000001.SZ', '000002.SZ', '000003.SZ', '000004.SZ', '000005.SZ'],
        'name': ['平安银行', '万科A', '中国神华', '中国平安', '招商银行'],
        'industry': ['银行', '房地产', '能源', '保险', '银行']
    })


@pytest.fixture(scope="session")
def sample_technical_indicators():
    """样本技术指标数据"""
    return pd.DataFrame({
        'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
        'close_price': np.random.uniform(10, 20, 30),
        'ema_12': np.random.uniform(10, 20, 30),
        'ema_26': np.random.uniform(10, 20, 30),
        'ma_5': np.random.uniform(10, 20, 30),
        'ma_10': np.random.uniform(10, 20, 30),
        'ma_20': np.random.uniform(10, 20, 30),
        'rsi': np.random.uniform(20, 80, 30),
        'k': np.random.uniform(20, 80, 30),
        'd': np.random.uniform(20, 80, 30),
        'j': np.random.uniform(20, 80, 30),
        'williams_r': np.random.uniform(-80, -20, 30),
        'macd': np.random.uniform(-1, 1, 30),
        'macd_signal': np.random.uniform(-1, 1, 30),
        'bb_upper': np.random.uniform(20, 25, 30),
        'bb_middle': np.random.uniform(15, 20, 30),
        'bb_lower': np.random.uniform(10, 15, 30),
        'volume_ma': np.random.uniform(1000000, 5000000, 30),
        'volume_ratio': np.random.uniform(0.5, 2.0, 30),
        'obv': np.random.uniform(1000000, 10000000, 30),
        'obv_ma': np.random.uniform(1000000, 10000000, 30),
    })


@pytest.fixture(scope="session")
def sample_analysis_profiles():
    """样本分析维度数据"""
    return {
        'technical': {
            'overall_score': 0.8,
            'trend_score': 0.9,
            'momentum_score': 0.7,
            'volume_health_score': 0.8,
            'patterns': {
                'patterns': ['bullish_engulfing'],
                'confidence': 0.8
            },
            'support_resistance': (14.5, 16.5)
        },
        'capital_flow': {
            'overall_score': 0.9,
            'main_force_score': 0.95,
            'retail_sentiment_score': 0.8,
            'institutional_activity': 0.9,
            'flow_consistency': 0.85,
            'volume_price_correlation': 0.75
        },
        'relative_strength': {
            'overall_score': 0.7,
            'vs_market_score': 0.8,
            'vs_sector_score': 0.6,
            'momentum_rank': 0.75
        },
        'catalyst': {
            'overall_score': 0.6,
            'news_sentiment': 0.7,
            'earnings_events': 0.5,
            'policy_impact': 0.6
        }
    }


@pytest.fixture(scope="session")
def sample_trade_plan():
    """样本交易计划"""
    from src.strategies.qiming_star.signal_fusion_engine import TradePlan
    
    return TradePlan(
        stock_code='000001.SZ',
        stock_name='平安银行',
        current_price=15.5,
        signal_type='buy',
        confidence_score=0.85,
        entry_price=15.5,
        stop_loss=14.7,
        take_profit=17.0,
        reasoning='Strong technical and capital flow signals'
    )


@pytest.fixture(scope="session")
def sample_market_context():
    """样本市场环境"""
    from src.strategies.qiming_star.signal_fusion_engine import MarketContext
    
    return MarketContext(
        market_trend='bullish',
        volatility='medium',
        sector_performance='positive',
        market_sentiment='positive'
    )


@pytest.fixture(scope="session")
def sample_backtest_result():
    """样本回测结果"""
    return {
        'trades': [
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
        ],
        'equity_curve': pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=30, freq='D'),
            'equity': np.linspace(1000000, 1100000, 30),
            'benchmark': np.linspace(1000000, 1050000, 30)
        }),
        'performance_metrics': {
            'total_return': 0.10,
            'annualized_return': 0.15,
            'sharpe_ratio': 1.2,
            'max_drawdown': -0.05,
            'win_rate': 0.6,
            'profit_factor': 1.5
        }
    }


@pytest.fixture(scope="session")
def mock_database_manager():
    """模拟数据库管理器"""
    mock_manager = Mock()
    mock_manager.execute_postgres_query.return_value = pd.DataFrame({
        'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
        'close_price': np.random.uniform(10, 20, 30),
        'volume': np.random.uniform(1000000, 5000000, 30),
    })
    mock_manager.test_connection.return_value = True
    mock_manager.get_table_count.return_value = 10
    return mock_manager


@pytest.fixture(scope="session")
def mock_akshare_data():
    """模拟AkShare数据"""
    return pd.DataFrame({
        '日期': ['2024-01-01', '2024-01-02', '2024-01-03'],
        '开盘': [15.0, 15.5, 16.0],
        '最高': [15.8, 16.2, 16.5],
        '最低': [14.8, 15.2, 15.8],
        '收盘': [15.5, 16.0, 16.3],
        '成交量': [1000000, 1200000, 1400000],
        '成交额': [15000000, 19200000, 22820000],
        '振幅': [6.67, 6.45, 4.38],
        '涨跌幅': [3.33, 3.23, 1.88],
        '涨跌额': [0.5, 0.5, 0.3],
        '换手率': [0.5, 0.6, 0.7]
    })


@pytest.fixture(scope="session")
def mock_tushare_data():
    """模拟Tushare数据"""
    return {
        'success': True,
        'data': [
            {
                'date': '2024-01-01',
                'open': 15.0,
                'high': 15.8,
                'low': 14.8,
                'close': 15.5,
                'volume': 1000000
            },
            {
                'date': '2024-01-02',
                'open': 15.5,
                'high': 16.2,
                'low': 15.2,
                'close': 16.0,
                'volume': 1200000
            }
        ]
    }


@pytest.fixture(scope="session")
def mock_strategy_result():
    """模拟策略分析结果"""
    return {
        'stock_code': '000001.SZ',
        'current_price': 15.5,
        'profiles': {
            'technical': {'overall_score': 0.8},
            'capital_flow': {'overall_score': 0.9},
            'relative_strength': {'overall_score': 0.7},
            'catalyst': {'overall_score': 0.6}
        },
        'trade_plan': Mock(
            signal_type='buy',
            confidence_score=0.85,
            entry_price=15.5,
            stop_loss=14.7,
            take_profit=17.0,
            reasoning='Strong signals'
        ),
        'market_context': Mock(
            market_trend='bullish',
            volatility='medium',
            sector_performance='positive',
            market_sentiment='positive'
        ),
        'analysis_time': datetime.now()
    }


@pytest.fixture(scope="session")
def mock_system_info():
    """模拟系统信息"""
    return {
        'cpu_usage': 25.5,
        'memory_usage': 30.0,
        'disk_usage': 45.0,
        'uptime': '2 days, 5 hours'
    }


@pytest.fixture(scope="session")
def mock_database_status():
    """模拟数据库状态"""
    return {
        'connected': True,
        'table_count': 10,
        'last_update': '2024-01-20 10:00:00'
    }


@pytest.fixture(scope="session")
def mock_strategy_status():
    """模拟策略状态"""
    return {
        'strategy_name': '启明星策略',
        'version': '2.0',
        'status': 'active',
        'last_update': '2024-01-20 10:00:00'
    }


@pytest.fixture(scope="session")
def large_dataset():
    """大数据集用于性能测试"""
    return pd.DataFrame({
        'trade_date': pd.date_range('2020-01-01', periods=1000, freq='D'),
        'open_price': np.random.uniform(10, 20, 1000),
        'high_price': np.random.uniform(15, 25, 1000),
        'low_price': np.random.uniform(5, 15, 1000),
        'close_price': np.random.uniform(10, 20, 1000),
        'volume': np.random.uniform(1000000, 5000000, 1000),
    })


@pytest.fixture(scope="session")
def dirty_dataset():
    """包含数据质量问题的数据集"""
    clean_data = pd.DataFrame({
        'trade_date': pd.date_range('2024-01-01', periods=30, freq='D'),
        'open_price': np.random.uniform(10, 20, 30),
        'high_price': np.random.uniform(15, 25, 30),
        'low_price': np.random.uniform(5, 15, 30),
        'close_price': np.random.uniform(10, 20, 30),
        'volume': np.random.uniform(1000000, 5000000, 30),
    })
    
    # 添加数据质量问题
    dirty_data = clean_data.copy()
    dirty_data.loc[5:10, 'close_price'] = np.nan  # NaN值
    dirty_data.loc[15:20, 'volume'] = np.nan  # NaN值
    dirty_data.loc[25, 'close_price'] = 1000  # 异常高价
    dirty_data.loc[26, 'volume'] = 0  # 零成交量
    dirty_data.loc[27, 'high_price'] = 10  # 最高价低于最低价
    dirty_data.loc[27, 'low_price'] = 20
    
    return dirty_data


@pytest.fixture(scope="session")
def minimal_dataset():
    """最小数据集"""
    return pd.DataFrame({
        'trade_date': ['2024-01-01'],
        'open_price': [15.0],
        'high_price': [16.0],
        'low_price': [14.0],
        'close_price': [15.5],
        'volume': [1000000],
    })


@pytest.fixture(scope="session")
def empty_dataset():
    """空数据集"""
    return pd.DataFrame()


@pytest.fixture(scope="session")
def test_config():
    """测试配置"""
    return {
        'weights': {
            'technical': 0.35,
            'capital_flow': 0.45,
            'relative_strength': 0.15,
            'catalyst': 0.05
        },
        'thresholds': {
            's_class': 90,
            'a_class': 75
        },
        'initial_capital': 1000000,
        'commission_rate': 0.0003,
        'slippage': 0.0001
    }


@pytest.fixture(scope="session")
def error_test_cases():
    """错误测试用例"""
    return {
        'database_connection_error': Exception("Database connection failed"),
        'data_source_error': Exception("Data source unavailable"),
        'strategy_error': Exception("Strategy analysis failed"),
        'invalid_data_error': ValueError("Invalid data format"),
        'network_error': ConnectionError("Network timeout"),
        'memory_error': MemoryError("Insufficient memory"),
        'timeout_error': TimeoutError("Operation timed out")
    }


@pytest.fixture(scope="session")
def performance_thresholds():
    """性能测试阈值"""
    return {
        'large_dataset_time': 10.0,  # 秒
        'memory_increase_mb': 500,  # MB
        'concurrent_analysis_time': 30.0,  # 秒
        'database_query_time': 2.0,  # 秒
        'strategy_analysis_time': 5.0,  # 秒
        'data_processing_time': 3.0  # 秒
    }


# 测试标记
def pytest_configure(config):
    """配置测试标记"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as performance tests"
    )
    config.addinivalue_line(
        "markers", "error_handling: marks tests as error handling tests"
    )
    config.addinivalue_line(
        "markers", "data_quality: marks tests as data quality tests"
    )


# 测试收集钩子
def pytest_collection_modifyitems(config, items):
    """修改测试收集"""
    for item in items:
        # 为集成测试添加标记
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        
        # 为性能测试添加标记
        if "performance" in item.nodeid:
            item.add_marker(pytest.mark.performance)
        
        # 为错误处理测试添加标记
        if "error" in item.nodeid:
            item.add_marker(pytest.mark.error_handling)
        
        # 为数据质量测试添加标记
        if "quality" in item.nodeid or "dirty" in item.nodeid:
            item.add_marker(pytest.mark.data_quality)


# 测试报告钩子
def pytest_html_report_title(report):
    """设置HTML报告标题"""
    report.title = "启明星量化投资分析平台测试报告"


def pytest_html_results_table_header(cells):
    """自定义HTML报告表头"""
    cells.pop()  # 移除默认的Links列


def pytest_html_results_table_row(report, cells):
    """自定义HTML报告行"""
    cells.pop()  # 移除默认的Links列


# 测试会话钩子
def pytest_sessionstart(session):
    """测试会话开始"""
    print(f"\n开始测试会话: {session.name}")
    print(f"测试文件数量: {len(session.items)}")


# 测试结果收集（兼容pytest 8.x）
test_results = []


def pytest_runtest_logreport(report):
    """测试运行日志报告和结果收集"""
    if report.when == "call":
        test_results.append(report.outcome)
        if report.passed:
            print(f"✓ {report.nodeid}")
        elif report.failed:
            print(f"✗ {report.nodeid}")
            if report.longrepr:
                print(f"  错误: {report.longrepr}")
        elif report.skipped:
            print(f"- {report.nodeid}")


def pytest_sessionfinish(session, exitstatus):
    """测试会话结束"""
    print(f"\n测试会话结束: {session.name}")
    print(f"退出状态: {exitstatus}")
    # 统计测试结果（兼容pytest 8.x）
    passed = test_results.count('passed')
    failed = test_results.count('failed')
    skipped = test_results.count('skipped')
    print(f"通过: {passed}, 失败: {failed}, 跳过: {skipped}")


# 性能测试装饰器
def performance_test(threshold_seconds=5.0):
    """性能测试装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            
            assert execution_time < threshold_seconds, \
                f"性能测试失败: 执行时间 {execution_time:.2f}秒 超过阈值 {threshold_seconds}秒"
            
            return result
        return wrapper
    return decorator


# 内存测试装饰器
def memory_test(threshold_mb=100):
    """内存测试装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            import psutil
            import os
            
            process = psutil.Process(os.getpid())
            initial_memory = process.memory_info().rss
            
            result = func(*args, **kwargs)
            
            final_memory = process.memory_info().rss
            memory_increase = (final_memory - initial_memory) / (1024 * 1024)  # MB
            
            assert memory_increase < threshold_mb, \
                f"内存测试失败: 内存增长 {memory_increase:.2f}MB 超过阈值 {threshold_mb}MB"
            
            return result
        return wrapper
    return decorator 