#!/usr/bin/env python3
"""
测试不同数据源的可用性
"""
import sys
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("test_data_sources")


def test_tushare():
    """测试Tushare数据源"""
    logger.info("🔍 测试Tushare数据源...")
    
    try:
        from src.data_sources.tushare_client import TushareClient
        logger.info("✅ Tushare模块导入成功")
        
        # 初始化客户端
        client = TushareClient()
        logger.info("✅ Tushare客户端初始化成功")
        
        # 测试获取股票基础信息
        try:
            stock_basic = client.get_stock_basic()
            if not stock_basic.empty:
                logger.info(f"✅ Tushare股票基础信息获取成功: {len(stock_basic)} 只股票")
                return True, "成功"
            else:
                logger.warning("⚠️ Tushare股票基础信息为空")
                return False, "数据为空"
        except Exception as api_e:
            error_msg = str(api_e)
            if "权限" in error_msg or "permission" in error_msg.lower():
                logger.error(f"❌ Tushare API权限不足: {error_msg}")
                return False, f"API权限不足: {error_msg}"
            elif "token" in error_msg.lower():
                logger.error(f"❌ Tushare Token配置错误: {error_msg}")
                return False, f"Token配置错误: {error_msg}"
            else:
                logger.error(f"❌ Tushare API调用失败: {error_msg}")
                return False, f"API调用失败: {error_msg}"
        
    except ImportError as e:
        logger.error(f"❌ Tushare模块导入失败: {e}")
        return False, f"模块导入失败: {e}"
    except Exception as e:
        logger.error(f"❌ Tushare测试失败: {e}")
        return False, f"测试失败: {e}"


def test_alltick():
    """测试AllTick数据源"""
    logger.info("🔍 测试AllTick数据源...")
    
    try:
        from src.data_sources.alltick_data_source import AllTickDataSource
        logger.info("✅ AllTick模块导入成功")
        
        # 初始化客户端
        try:
            client = AllTickDataSource()
            logger.info("✅ AllTick客户端初始化成功")
            
            # 测试连接
            # 这里应该有具体的测试逻辑
            logger.info("⚠️ AllTick连接测试暂未实现")
            return False, "连接测试暂未实现"
            
        except Exception as init_e:
            error_msg = str(init_e)
            if "token" in error_msg.lower() or "key" in error_msg.lower():
                logger.error(f"❌ AllTick Token配置错误: {error_msg}")
                return False, f"Token配置错误: {error_msg}"
            else:
                logger.error(f"❌ AllTick初始化失败: {error_msg}")
                return False, f"初始化失败: {error_msg}"
        
    except ImportError as e:
        logger.error(f"❌ AllTick模块导入失败: {e}")
        return False, f"模块导入失败: {e}"
    except Exception as e:
        logger.error(f"❌ AllTick测试失败: {e}")
        return False, f"测试失败: {e}"


def test_alpha_vantage():
    """测试Alpha Vantage数据源"""
    logger.info("🔍 测试Alpha Vantage数据源...")
    
    try:
        from src.data_sources.alpha_vantage_data_source import AlphaVantageDataSource
        logger.info("✅ Alpha Vantage模块导入成功")
        
        # 初始化客户端
        try:
            client = AlphaVantageDataSource()
            logger.info("✅ Alpha Vantage客户端初始化成功")
            
            # 测试连接
            # 这里应该有具体的测试逻辑
            logger.info("⚠️ Alpha Vantage连接测试暂未实现")
            return False, "连接测试暂未实现，且主要用于美股数据"
            
        except Exception as init_e:
            error_msg = str(init_e)
            if "api" in error_msg.lower() or "key" in error_msg.lower():
                logger.error(f"❌ Alpha Vantage API Key配置错误: {error_msg}")
                return False, f"API Key配置错误: {error_msg}"
            else:
                logger.error(f"❌ Alpha Vantage初始化失败: {error_msg}")
                return False, f"初始化失败: {error_msg}"
        
    except ImportError as e:
        logger.error(f"❌ Alpha Vantage模块导入失败: {e}")
        return False, f"模块导入失败: {e}"
    except Exception as e:
        logger.error(f"❌ Alpha Vantage测试失败: {e}")
        return False, f"测试失败: {e}"


def test_database():
    """测试数据库连接"""
    logger.info("🔍 测试数据库连接...")
    
    try:
        from src.utils.database import get_db_manager
        logger.info("✅ 数据库模块导入成功")
        
        db_manager = get_db_manager()
        logger.info("✅ 数据库管理器初始化成功")
        
        # 测试连接
        connections = db_manager.test_connections()
        logger.info(f"数据库连接状态: {connections}")
        
        if connections.get('postgres', False):
            logger.info("✅ PostgreSQL连接成功")
            return True, "PostgreSQL连接正常"
        else:
            logger.error("❌ PostgreSQL连接失败")
            return False, "PostgreSQL连接失败"
        
    except ImportError as e:
        logger.error(f"❌ 数据库模块导入失败: {e}")
        return False, f"模块导入失败: {e}"
    except Exception as e:
        logger.error(f"❌ 数据库测试失败: {e}")
        return False, f"测试失败: {e}"


def main():
    """主函数"""
    logger.info("🚀 数据源可用性测试")
    logger.info("=" * 50)
    
    # 测试结果
    results = {}
    
    # 1. 测试Tushare
    logger.info("\n📋 测试 1/4: Tushare数据源")
    logger.info("-" * 30)
    success, message = test_tushare()
    results['Tushare'] = {'success': success, 'message': message}
    
    # 2. 测试AllTick
    logger.info("\n📋 测试 2/4: AllTick数据源")
    logger.info("-" * 30)
    success, message = test_alltick()
    results['AllTick'] = {'success': success, 'message': message}
    
    # 3. 测试Alpha Vantage
    logger.info("\n📋 测试 3/4: Alpha Vantage数据源")
    logger.info("-" * 30)
    success, message = test_alpha_vantage()
    results['Alpha Vantage'] = {'success': success, 'message': message}
    
    # 4. 测试数据库
    logger.info("\n📋 测试 4/4: 数据库连接")
    logger.info("-" * 30)
    success, message = test_database()
    results['Database'] = {'success': success, 'message': message}
    
    # 总结
    logger.info("\n" + "=" * 50)
    logger.info("📊 数据源测试总结")
    logger.info("=" * 50)
    
    available_sources = []
    unavailable_sources = []
    
    for source, result in results.items():
        status = "✅ 可用" if result['success'] else "❌ 不可用"
        logger.info(f"{source}: {status} - {result['message']}")
        
        if result['success']:
            available_sources.append(source)
        else:
            unavailable_sources.append(source)
    
    logger.info(f"\n📈 可用数据源 ({len(available_sources)}): {', '.join(available_sources) if available_sources else '无'}")
    logger.info(f"❌ 不可用数据源 ({len(unavailable_sources)}): {', '.join(unavailable_sources) if unavailable_sources else '无'}")
    
    # 建议
    logger.info("\n💡 建议:")
    if 'Tushare' in available_sources:
        logger.info("  - 推荐使用Tushare获取A股数据（股票基础信息 + 日线行情）")
    elif 'AllTick' in available_sources:
        logger.info("  - 可以使用AllTick获取A股数据（需要实现具体接口）")
    else:
        logger.info("  - 当前无可用的A股数据源，请检查配置")
        logger.info("  - Tushare: 检查Token配置和API权限")
        logger.info("  - AllTick: 检查Token配置和接口实现")
    
    if 'Database' in available_sources:
        logger.info("  - 数据库连接正常，可以保存数据")
    else:
        logger.info("  - 数据库连接异常，请检查配置")
    
    logger.info("\n🚀 下一步操作:")
    logger.info("1. 访问Dashboard: http://localhost:8506")
    logger.info("2. 进入'数据管理'页面测试数据更新")
    logger.info("3. 选择可用的数据源进行数据拉取")
    
    return len(available_sources) > 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
