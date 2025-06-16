#!/usr/bin/env python3
"""
启明星策略测试脚本
测试策略的完整功能和性能
"""
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.strategies.qiming_star import QimingStarStrategy
from src.utils.logger import get_logger

logger = get_logger("qiming_star_test")


def generate_mock_stock_data(stock_code: str, days: int = 200) -> pd.DataFrame:
    """生成模拟股票数据"""
    np.random.seed(hash(stock_code) % 2**32)
    
    dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
    
    # 生成价格数据
    base_price = 10 + (hash(stock_code) % 50)
    returns = np.random.randn(days) * 0.02
    prices = base_price * np.exp(np.cumsum(returns))
    
    # 生成OHLC数据
    opens = prices * (1 + np.random.randn(days) * 0.005)
    highs = np.maximum(opens, prices) * (1 + np.abs(np.random.randn(days)) * 0.01)
    lows = np.minimum(opens, prices) * (1 - np.abs(np.random.randn(days)) * 0.01)
    volumes = np.random.lognormal(15, 0.5, days)
    
    data = pd.DataFrame({
        'open': opens,
        'high': highs,
        'low': lows,
        'close': prices,
        'volume': volumes
    }, index=dates)
    
    return data


def test_single_stock_analysis():
    """测试单股分析功能"""
    logger.info("🔍 测试单股分析功能")
    
    try:
        # 创建策略实例
        strategy = QimingStarStrategy()
        
        # 生成测试数据
        stock_data = generate_mock_stock_data("000001.SZ", 120)
        market_data = generate_mock_stock_data("INDEX", 120)
        
        # 执行分析
        result = strategy.analyze_stock(
            stock_code="000001.SZ",
            stock_data=stock_data,
            market_data=market_data
        )
        
        if result:
            logger.success("✅ 单股分析成功")
            logger.info(f"当前价格: {result['current_price']:.2f}")
            
            # 显示四维分析结果
            profiles = result['profiles']
            logger.info("四维分析结果:")
            for dimension, profile in profiles.items():
                logger.info(f"  {dimension}: {profile.score:.1f}分 - {profile.labels}")
            
            # 显示交易计划
            trade_plan = result['trade_plan']
            if trade_plan:
                logger.info(f"交易信号: {trade_plan.signal_class}")
                logger.info(f"确定性得分: {trade_plan.conviction_score:.1f}")
                logger.info(f"核心逻辑: {trade_plan.rationale}")
                logger.info(f"买入区间: {trade_plan.entry_zone}")
                logger.info(f"止损价: {trade_plan.stop_loss_price:.2f}")
                logger.info(f"目标价: {trade_plan.target_price:.2f}")
            else:
                logger.info("未产生交易信号")
            
            return True
        else:
            logger.error("❌ 单股分析失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 单股分析异常: {e}")
        return False


def test_batch_analysis():
    """测试批量分析功能"""
    logger.info("🔍 测试批量分析功能")
    
    try:
        # 创建策略实例
        strategy = QimingStarStrategy()
        
        # 生成测试股票池
        stock_codes = ["000001.SZ", "000002.SZ", "600000.SH", "600036.SH", "000858.SZ"]
        stock_data_dict = {}
        
        for code in stock_codes:
            stock_data_dict[code] = generate_mock_stock_data(code, 120)
        
        # 生成市场数据
        market_data = generate_mock_stock_data("INDEX", 120)
        
        # 执行批量分析
        signals = strategy.batch_analyze(stock_data_dict, market_data)
        
        logger.success("✅ 批量分析成功")
        logger.info(f"S级信号: {len(signals['s_class'])} 个")
        logger.info(f"A级信号: {len(signals['a_class'])} 个")
        
        # 显示信号详情
        all_signals = signals['s_class'] + signals['a_class']
        if all_signals:
            logger.info("信号详情:")
            for signal in all_signals[:3]:  # 显示前3个
                logger.info(f"  {signal.stock_code}: {signal.signal_class} - {signal.conviction_score:.1f}分")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 批量分析异常: {e}")
        return False


def test_backtest_functionality():
    """测试回测功能"""
    logger.info("🔍 测试回测功能")
    
    try:
        # 创建策略实例
        strategy = QimingStarStrategy({
            "initial_capital": 1000000,
            "max_position_size": 0.2
        })
        
        # 生成测试数据
        stock_codes = ["000001.SZ", "000002.SZ", "600000.SH", "600036.SH", "000858.SZ"]
        stock_data_dict = {}
        
        for code in stock_codes:
            data = generate_mock_stock_data(code, 150)
            stock_data_dict[code] = data
        
        # 设置回测期间
        start_date = datetime.now() - timedelta(days=100)
        end_date = datetime.now() - timedelta(days=10)
        
        # 执行回测
        results = strategy.run_backtest(
            stock_data_dict=stock_data_dict,
            start_date=start_date,
            end_date=end_date,
            benchmark_strategies=["简单移动平均", "RSI策略"]
        )
        
        if results:
            logger.success("✅ 回测执行成功")
            
            # 显示回测结果
            for strategy_name, result in results.items():
                logger.info(f"\n{strategy_name} 回测结果:")
                logger.info(f"  总收益率: {result.total_return_pct:.2f}%")
                logger.info(f"  年化收益率: {result.annualized_return:.2f}%")
                logger.info(f"  最大回撤: {result.max_drawdown:.2f}%")
                logger.info(f"  夏普比率: {result.sharpe_ratio:.2f}")
                logger.info(f"  胜率: {result.win_rate:.1f}%")
                logger.info(f"  交易次数: {result.total_trades}")
            
            # 比较启明星策略与基准
            qiming_result = results.get("启明星策略")
            if qiming_result:
                other_results = [r for name, r in results.items() if name != "启明星策略"]
                if other_results:
                    avg_benchmark_return = np.mean([r.total_return_pct for r in other_results])
                    excess_return = qiming_result.total_return_pct - avg_benchmark_return
                    logger.info(f"\n启明星策略超额收益: {excess_return:.2f}%")
            
            return True
        else:
            logger.error("❌ 回测执行失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 回测异常: {e}")
        return False


def test_strategy_configuration():
    """测试策略配置功能"""
    logger.info("🔍 测试策略配置功能")
    
    try:
        # 创建默认策略
        strategy = QimingStarStrategy()
        
        # 获取策略摘要
        summary = strategy.get_strategy_summary()
        logger.info(f"策略名称: {summary['strategy_name']}")
        logger.info(f"策略版本: {summary['version']}")
        logger.info(f"策略描述: {summary['description']}")
        
        # 显示当前配置
        logger.info("当前权重配置:")
        for dimension, weight in summary['weights'].items():
            logger.info(f"  {dimension}: {weight:.2f}")
        
        logger.info("当前阈值配置:")
        for threshold, value in summary['thresholds'].items():
            logger.info(f"  {threshold}: {value}")
        
        # 测试配置更新
        new_config = {
            "weights": {
                "capital": 0.5,
                "technical": 0.3,
                "relative_strength": 0.15,
                "catalyst": 0.05
            },
            "thresholds": {
                "capital_min": 85,
                "technical_min": 80
            }
        }
        
        strategy.update_config(new_config)
        logger.success("✅ 配置更新成功")
        
        # 验证配置更新
        updated_summary = strategy.get_strategy_summary()
        logger.info("更新后权重:")
        for dimension, weight in updated_summary['weights'].items():
            logger.info(f"  {dimension}: {weight:.2f}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 配置测试异常: {e}")
        return False


def performance_benchmark():
    """性能基准测试"""
    logger.info("🔍 性能基准测试")
    
    try:
        import time
        
        # 创建策略实例
        strategy = QimingStarStrategy()
        
        # 生成大量测试数据
        stock_codes = [f"00000{i:02d}.SZ" for i in range(1, 51)]  # 50只股票
        stock_data_dict = {}
        
        logger.info(f"生成 {len(stock_codes)} 只股票的测试数据...")
        for code in stock_codes:
            stock_data_dict[code] = generate_mock_stock_data(code, 120)
        
        # 测试批量分析性能
        start_time = time.time()
        signals = strategy.batch_analyze(stock_data_dict)
        analysis_time = time.time() - start_time
        
        logger.success(f"✅ 批量分析完成")
        logger.info(f"分析时间: {analysis_time:.2f}秒")
        logger.info(f"处理速度: {len(stock_codes)/analysis_time:.1f} 股票/秒")
        logger.info(f"生成信号: S级 {len(signals['s_class'])} 个, A级 {len(signals['a_class'])} 个")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 性能测试异常: {e}")
        return False


def main():
    """主测试函数"""
    logger.info("🚀 启明星策略完整功能测试")
    logger.info("=" * 80)
    
    test_results = []
    
    # 执行各项测试
    tests = [
        ("单股分析功能", test_single_stock_analysis),
        ("批量分析功能", test_batch_analysis),
        ("回测功能", test_backtest_functionality),
        ("策略配置功能", test_strategy_configuration),
        ("性能基准测试", performance_benchmark)
    ]
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*20} {test_name} {'='*20}")
        try:
            result = test_func()
            test_results.append((test_name, result))
        except Exception as e:
            logger.error(f"测试 {test_name} 发生异常: {e}")
            test_results.append((test_name, False))
    
    # 测试结果汇总
    logger.info("\n" + "=" * 80)
    logger.info("🎯 测试结果汇总")
    logger.info("=" * 80)
    
    passed_tests = 0
    total_tests = len(test_results)
    
    for test_name, result in test_results:
        status = "✅ 通过" if result else "❌ 失败"
        logger.info(f"{test_name}: {status}")
        if result:
            passed_tests += 1
    
    success_rate = passed_tests / total_tests * 100
    logger.info(f"\n📊 测试通过率: {passed_tests}/{total_tests} ({success_rate:.1f}%)")
    
    if success_rate == 100:
        logger.success("🎉 所有测试通过！启明星策略系统运行正常")
        
        logger.info("\n🚀 下一步操作:")
        logger.info("1. 运行 Marimo 笔记本进行交互式分析:")
        logger.info("   python scripts/launch_marimo.py launch qiming_star_strategy_analysis.py")
        logger.info("2. 连接真实数据源进行实盘测试")
        logger.info("3. 根据市场情况调整策略参数")
        
        return True
    else:
        logger.warning(f"⚠️ 部分测试失败，请检查相关功能")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
