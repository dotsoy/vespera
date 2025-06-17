#!/usr/bin/env python3
"""
统一系统测试脚本 - 简化版本
整合了原有的多个测试脚本功能，只保留核心测试
"""
import sys
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("unified_system_test")


def test_database_connection():
    """测试数据库连接"""
    logger.info("🔍 测试数据库连接...")
    
    try:
        from src.utils.database import get_db_manager
        db_manager = get_db_manager()
        connections = db_manager.test_connections()
        
        postgres_ok = connections.get('postgres', False)
        clickhouse_ok = connections.get('clickhouse', False)
        
        if postgres_ok:
            logger.success("✅ PostgreSQL连接正常")
        else:
            logger.error("❌ PostgreSQL连接失败")
            
        if clickhouse_ok:
            logger.success("✅ ClickHouse连接正常")
        else:
            logger.warning("⚠️ ClickHouse连接失败（可选）")
            
        return postgres_ok
        
    except Exception as e:
        logger.error(f"❌ 数据库测试失败: {e}")
        return False


def test_akshare_data_source():
    """测试AkShare数据源"""
    logger.info("🔍 测试AkShare数据源...")
    
    try:
        from src.data_sources.akshare_data_source import AkShareDataSource
        from src.data_sources.base_data_source import DataRequest, DataType
        
        # 初始化客户端
        client = AkShareDataSource()
        if not client.initialize():
            logger.error("❌ AkShare初始化失败")
            return False
            
        # 测试获取股票基础信息
        request = DataRequest(data_type=DataType.STOCK_BASIC)
        response = client.fetch_data(request)
        
        if response.success and not response.data.empty:
            logger.success(f"✅ AkShare测试成功，获取到 {len(response.data)} 只股票信息")
            return True
        else:
            logger.error("❌ AkShare数据获取失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ AkShare测试失败: {e}")
        return False


def test_qiming_star_strategy():
    """测试启明星策略"""
    logger.info("🔍 测试启明星策略...")
    
    try:
        from src.strategies.qiming_star_strategy import QimingStarStrategy
        
        # 创建策略实例
        strategy = QimingStarStrategy()
        
        # 测试单股分析
        test_symbol = "000001.SZ"
        result = strategy.analyze_stock(test_symbol)
        
        if result and 'signal' in result:
            logger.success(f"✅ 启明星策略测试成功，{test_symbol} 信号: {result['signal']}")
            return True
        else:
            logger.error("❌ 启明星策略分析失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 启明星策略测试失败: {e}")
        return False


def test_dashboard_components():
    """测试Dashboard组件"""
    logger.info("🔍 测试Dashboard组件...")
    
    try:
        # 测试主要组件导入
        from dashboard.components.system_status import render_system_status_main
        from dashboard.components.data_management import render_data_management_main
        from dashboard.components.strategy_analysis import render_strategy_analysis_main
        from dashboard.components.backtest_visualization import render_backtest_visualization_main
        
        logger.success("✅ Dashboard组件导入成功")
        return True
        
    except Exception as e:
        logger.error(f"❌ Dashboard组件测试失败: {e}")
        return False


def test_marimo_integration():
    """测试Marimo集成"""
    logger.info("🔍 测试Marimo集成...")
    
    try:
        from scripts.launch_marimo import MarimoLauncher
        
        launcher = MarimoLauncher()
        marimo_installed = launcher.check_marimo_installed()
        
        if marimo_installed:
            logger.success("✅ Marimo已安装并可用")
            return True
        else:
            logger.warning("⚠️ Marimo未安装（可选功能）")
            return True  # 不是必需的，所以返回True
            
    except Exception as e:
        logger.warning(f"⚠️ Marimo测试失败: {e}")
        return True  # 不是必需的，所以返回True


def run_performance_test():
    """运行性能测试"""
    logger.info("🔍 运行性能测试...")
    
    try:
        from src.strategies.qiming_star_strategy import QimingStarStrategy
        import time
        
        strategy = QimingStarStrategy()
        test_symbols = ["000001.SZ", "000002.SZ", "600000.SH"]
        
        start_time = time.time()
        
        for symbol in test_symbols:
            result = strategy.analyze_stock(symbol)
            if not result:
                logger.warning(f"⚠️ {symbol} 分析失败")
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.success(f"✅ 性能测试完成，分析 {len(test_symbols)} 只股票耗时 {duration:.2f} 秒")
        return True
        
    except Exception as e:
        logger.error(f"❌ 性能测试失败: {e}")
        return False


def main():
    """主测试函数"""
    logger.info("🚀 启明星系统统一测试")
    logger.info("=" * 60)
    
    tests = [
        ("数据库连接", test_database_connection),
        ("AkShare数据源", test_akshare_data_source),
        ("启明星策略", test_qiming_star_strategy),
        ("Dashboard组件", test_dashboard_components),
        ("Marimo集成", test_marimo_integration),
        ("性能测试", run_performance_test),
    ]
    
    results = {}
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n📋 {test_name}测试")
        logger.info("-" * 30)
        
        try:
            result = test_func()
            results[test_name] = result
            
            if result:
                logger.success(f"✅ {test_name}测试通过")
                passed += 1
            else:
                logger.error(f"❌ {test_name}测试失败")
                
        except Exception as e:
            logger.error(f"❌ {test_name}测试异常: {e}")
            results[test_name] = False
    
    # 测试总结
    logger.info("\n" + "=" * 60)
    logger.info("📊 测试总结")
    logger.info("=" * 60)
    
    for test_name, result in results.items():
        status = "✅ 通过" if result else "❌ 失败"
        logger.info(f"{test_name}: {status}")
    
    success_rate = (passed / total) * 100
    logger.info(f"\n📈 测试通过率: {passed}/{total} ({success_rate:.1f}%)")
    
    if success_rate >= 80:
        logger.success("🎉 系统测试基本通过！")
        return True
    else:
        logger.error("❌ 系统测试失败，请检查错误信息")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
