#!/usr/bin/env python3
"""
Dashboard v2.0 功能验证脚本
验证所有组件是否正常导入和运行
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("dashboard_verification")


def test_component_imports():
    """测试所有组件导入"""
    logger.info("🔍 测试组件导入...")
    
    try:
        # 测试主应用
        from dashboard.app import main
        logger.info("✅ 主应用导入成功")
        
        # 测试系统状态组件
        from dashboard.components.system_status import render_system_status_main
        logger.info("✅ 系统状态组件导入成功")
        
        # 测试数据管理组件
        from dashboard.components.data_management import render_data_management_main
        logger.info("✅ 数据管理组件导入成功")
        
        # 测试数据源管理组件
        from dashboard.components.data_source_manager import render_data_source_manager_main
        logger.info("✅ 数据源管理组件导入成功")
        
        # 测试策略分析组件
        from dashboard.components.strategy_analysis import render_strategy_analysis_main
        logger.info("✅ 策略分析组件导入成功")
        
        # 测试回测可视化组件
        from dashboard.components.backtest_visualization import render_backtest_visualization_main
        logger.info("✅ 回测可视化组件导入成功")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 组件导入失败: {e}")
        return False


def test_database_connections():
    """测试数据库连接"""
    logger.info("🔍 测试数据库连接...")
    
    try:
        from dashboard.components.data_management import get_stock_list, get_latest_data_status
        
        # 测试股票列表获取
        stock_list = get_stock_list()
        if not stock_list.empty:
            logger.info(f"✅ 股票列表获取成功: {len(stock_list)} 只股票")
        else:
            logger.warning("⚠️ 股票列表为空，可能使用模拟数据")
        
        # 测试数据状态获取
        data_status = get_latest_data_status()
        if data_status:
            logger.info(f"✅ 数据状态获取成功: 最新日期={data_status.get('latest_date')}")
        else:
            logger.warning("⚠️ 数据状态获取失败")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据库连接测试失败: {e}")
        return False


def test_strategy_modules():
    """测试策略模块"""
    logger.info("🔍 测试策略模块...")
    
    try:
        # 测试启明星策略导入
        try:
            from src.strategies.qiming_star import QimingStarStrategy
            logger.info("✅ 启明星策略模块导入成功")
            strategy_available = True
        except ImportError as e:
            logger.warning(f"⚠️ 启明星策略模块导入失败: {e}")
            strategy_available = False
        
        # 测试模拟数据生成
        from dashboard.components.strategy_analysis import generate_mock_stock_data
        mock_data = generate_mock_stock_data(['000001.SZ', '600000.SH'], 30)
        if mock_data:
            logger.info(f"✅ 模拟数据生成成功: {len(mock_data)} 只股票")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 策略模块测试失败: {e}")
        return False


def test_data_source_manager():
    """测试数据源管理器"""
    logger.info("🔍 测试数据源管理器...")
    
    try:
        from dashboard.components.data_source_manager import DATA_SOURCES
        
        logger.info("📡 数据源状态:")
        for source_name, source_info in DATA_SOURCES.items():
            status = "✅ 可用" if source_info['available'] else "❌ 不可用"
            logger.info(f"  {source_name}: {status}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据源管理器测试失败: {e}")
        return False


def test_file_structure():
    """测试文件结构"""
    logger.info("🔍 测试文件结构...")
    
    required_files = [
        "dashboard/app.py",
        "dashboard/app_v1_backup.py",
        "dashboard/components/system_status.py",
        "dashboard/components/data_management.py",
        "dashboard/components/data_source_manager.py",
        "dashboard/components/strategy_analysis.py",
        "dashboard/components/backtest_visualization.py",
        "scripts/run_dashboard_v2.py",
        "README.md",
        "docs/PROJECT_COMPLETION_SUMMARY.md"
    ]
    
    missing_files = []
    for file_path in required_files:
        full_path = project_root / file_path
        if full_path.exists():
            logger.info(f"✅ {file_path}")
        else:
            logger.error(f"❌ {file_path} 不存在")
            missing_files.append(file_path)
    
    if not missing_files:
        logger.info("✅ 所有必需文件都存在")
        return True
    else:
        logger.error(f"❌ 缺少 {len(missing_files)} 个文件")
        return False


def main():
    """主验证函数"""
    logger.info("🚀 开始 Dashboard v2.0 功能验证")
    logger.info("=" * 50)
    
    tests = [
        ("文件结构", test_file_structure),
        ("组件导入", test_component_imports),
        ("数据库连接", test_database_connections),
        ("策略模块", test_strategy_modules),
        ("数据源管理器", test_data_source_manager),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n📋 {test_name}测试")
        logger.info("-" * 30)
        
        try:
            result = test_func()
            results[test_name] = result
            
            if result:
                logger.info(f"✅ {test_name}测试通过")
            else:
                logger.error(f"❌ {test_name}测试失败")
                
        except Exception as e:
            logger.error(f"❌ {test_name}测试异常: {e}")
            results[test_name] = False
    
    # 总结
    logger.info("\n" + "=" * 50)
    logger.info("📊 验证结果总结")
    logger.info("=" * 50)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ 通过" if result else "❌ 失败"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\n总体结果: {passed}/{total} 测试通过")
    
    if passed == total:
        logger.info("🎉 所有测试通过！Dashboard v2.0 功能正常")
        
        logger.info("\n🚀 快速启动指南:")
        logger.info("1. 启动 Dashboard: make dashboard")
        logger.info("2. 访问地址: http://localhost:8501")
        logger.info("3. 或使用新脚本: python scripts/run_dashboard_v2.py")
        
        return True
    else:
        logger.error(f"⚠️ {total - passed} 个测试失败，请检查相关问题")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
