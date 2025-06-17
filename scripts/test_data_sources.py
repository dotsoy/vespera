#!/usr/bin/env python3
"""
测试AkShare数据源的可用性
"""
import sys
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("test_data_sources")


def test_akshare():
    """测试AkShare数据源"""
    logger.info("🔍 测试AkShare数据源...")

    try:
        from src.data_sources.akshare_data_source import AkShareDataSource
        from src.data_sources.base_data_source import DataRequest, DataType
        logger.info("✅ AkShare模块导入成功")

        # 初始化客户端
        client = AkShareDataSource()
        logger.info("✅ AkShare数据源初始化成功")

        # 初始化数据源
        if not client.initialize():
            logger.error("❌ AkShare数据源初始化失败")
            return False, "数据源初始化失败"

        # 测试获取股票基础信息
        logger.info("📡 测试获取股票基础信息...")
        request = DataRequest(data_type=DataType.STOCK_BASIC)
        response = client.fetch_data(request)

        if response.success and not response.data.empty:
            stock_basic = response.data
            logger.info(f"✅ 获取到 {len(stock_basic)} 只股票基础信息")
            logger.info("📋 样本数据:")
            for _, row in stock_basic.head(3).iterrows():
                logger.info(f"  {row['ts_code']} - {row['name']} - {row['market']}")
            return True, f"成功获取 {len(stock_basic)} 只股票"
        else:
            logger.error("❌ 未获取到股票基础信息")
            return False, "未获取到股票基础信息"

    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ AkShare测试失败: {error_msg}")
        return False, f"测试失败: {error_msg}"


# AllTick数据源已移除


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

    # 1. 测试AkShare
    logger.info("\n📋 测试 1/2: AkShare数据源")
    logger.info("-" * 30)
    success, message = test_akshare()
    results['AkShare'] = {'success': success, 'message': message}

    # 2. 测试数据库
    logger.info("\n📋 测试 2/2: 数据库连接")
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
    if 'AkShare' in available_sources:
        logger.info("  - 推荐使用AkShare获取A股数据（免费，无需API key）")
        logger.info("  - AkShare支持股票基础信息、日线行情、指数数据等")
    else:
        logger.info("  - 当前AkShare数据源不可用，请检查网络连接")
        logger.info("  - AkShare是免费数据源，无需配置API key")
    
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
