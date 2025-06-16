#!/usr/bin/env python3
"""
测试数据源可用性（加载.env文件）
"""
import sys
from pathlib import Path
import os

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 加载.env文件
from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from src.utils.logger import get_logger

logger = get_logger("test_data_sources_with_env")


def test_alltick():
    """测试AllTick数据源"""
    logger.info("🔍 测试AllTick数据源...")
    
    try:
        from src.data_sources.alltick_client import AllTickClient
        logger.info("✅ AllTick模块导入成功")
        
        # 检查环境变量
        token = os.getenv('ALLTICK_TOKEN')
        logger.info(f"AllTick Token: {token[:10]}...{token[-10:] if token else 'None'}")
        
        if not token:
            logger.error("❌ ALLTICK_TOKEN环境变量未设置")
            return False, "ALLTICK_TOKEN环境变量未设置"
        
        # 初始化客户端
        client = AllTickClient()
        logger.info("✅ AllTick客户端初始化成功")
        
        # 测试连接
        test_result = client.test_connection()
        if test_result:
            logger.info("✅ AllTick连接测试成功")
            
            # 测试获取股票列表
            try:
                stock_list = client.get_stock_list('cn')
                if not stock_list.empty:
                    logger.info(f"✅ AllTick获取到 {len(stock_list)} 只股票信息")
                    return True, f"成功获取 {len(stock_list)} 只股票信息"
                else:
                    logger.warning("⚠️ AllTick股票列表为空")
                    return False, "股票列表为空"
            except Exception as list_e:
                logger.error(f"❌ AllTick获取股票列表失败: {list_e}")
                return False, f"获取股票列表失败: {list_e}"
        else:
            logger.error("❌ AllTick连接测试失败")
            return False, "连接测试失败"
        
    except Exception as e:
        logger.error(f"❌ AllTick测试失败: {e}")
        return False, f"测试失败: {e}"


def test_alpha_vantage():
    """测试Alpha Vantage数据源"""
    logger.info("🔍 测试Alpha Vantage数据源...")
    
    try:
        from src.data_sources.alpha_vantage_client import AlphaVantageClient
        logger.info("✅ Alpha Vantage模块导入成功")
        
        # 检查环境变量
        api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        logger.info(f"Alpha Vantage API Key: {api_key[:5]}...{api_key[-5:] if api_key else 'None'}")
        
        if not api_key:
            logger.error("❌ ALPHA_VANTAGE_API_KEY环境变量未设置")
            return False, "ALPHA_VANTAGE_API_KEY环境变量未设置"
        
        # 初始化客户端
        client = AlphaVantageClient()
        logger.info("✅ Alpha Vantage客户端初始化成功")
        
        # 测试连接
        test_result = client.test_connection()
        if test_result:
            logger.info("✅ Alpha Vantage连接测试成功")
            
            # 测试获取股票基础信息
            try:
                basic_info = client.get_stock_basic('AAPL')
                if not basic_info.empty:
                    logger.info(f"✅ Alpha Vantage获取到AAPL基础信息")
                    return True, "成功获取AAPL基础信息"
                else:
                    logger.warning("⚠️ Alpha Vantage基础信息为空")
                    return False, "基础信息为空"
            except Exception as basic_e:
                logger.error(f"❌ Alpha Vantage获取基础信息失败: {basic_e}")
                return False, f"获取基础信息失败: {basic_e}"
        else:
            logger.error("❌ Alpha Vantage连接测试失败")
            return False, "连接测试失败"
        
    except Exception as e:
        logger.error(f"❌ Alpha Vantage测试失败: {e}")
        return False, f"测试失败: {e}"


def test_stock_list_fetch():
    """测试股票列表拉取"""
    logger.info("🔍 测试股票列表拉取...")
    
    results = {}
    
    # 测试AllTick股票列表
    if os.getenv('ALLTICK_TOKEN'):
        try:
            from src.data_sources.alltick_client import AllTickClient
            client = AllTickClient()
            
            logger.info("📡 从AllTick获取中国股票列表...")
            stock_list = client.get_stock_list('cn')
            
            if not stock_list.empty:
                logger.info(f"✅ AllTick: 获取到 {len(stock_list)} 只股票")
                logger.info("📋 样本数据:")
                for _, row in stock_list.head(5).iterrows():
                    logger.info(f"  {row['ts_code']} - {row['name']}")
                results['AllTick'] = len(stock_list)
            else:
                logger.warning("⚠️ AllTick: 股票列表为空")
                results['AllTick'] = 0
                
        except Exception as e:
            logger.error(f"❌ AllTick股票列表获取失败: {e}")
            results['AllTick'] = f"失败: {e}"
    
    # 测试Alpha Vantage美股列表
    if os.getenv('ALPHA_VANTAGE_API_KEY'):
        try:
            from src.data_sources.alpha_vantage_client import AlphaVantageClient
            client = AlphaVantageClient()
            
            logger.info("📡 从Alpha Vantage获取美股列表...")
            us_stocks = client.get_us_stock_list()
            
            logger.info(f"✅ Alpha Vantage: 获取到 {len(us_stocks)} 只常见美股")
            logger.info("📋 美股列表:")
            for stock in us_stocks[:10]:
                logger.info(f"  {stock}")
            results['Alpha Vantage'] = len(us_stocks)
                
        except Exception as e:
            logger.error(f"❌ Alpha Vantage美股列表获取失败: {e}")
            results['Alpha Vantage'] = f"失败: {e}"
    
    return results


def main():
    """主函数"""
    logger.info("🚀 数据源测试（加载环境变量）")
    logger.info("=" * 60)
    
    # 显示环境变量状态
    logger.info("🔧 环境变量检查:")
    alltick_token = os.getenv('ALLTICK_TOKEN')
    alpha_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    logger.info(f"  ALLTICK_TOKEN: {'✅ 已设置' if alltick_token else '❌ 未设置'}")
    logger.info(f"  ALPHA_VANTAGE_API_KEY: {'✅ 已设置' if alpha_key else '❌ 未设置'}")
    
    if alltick_token:
        logger.info(f"  AllTick Token: {alltick_token[:10]}...{alltick_token[-10:]}")
    if alpha_key:
        logger.info(f"  Alpha Vantage Key: {alpha_key[:5]}...{alpha_key[-5:]}")
    
    results = {}
    
    # 1. 测试AllTick
    if alltick_token:
        logger.info("\n📋 测试 1/2: AllTick数据源")
        logger.info("-" * 40)
        success, message = test_alltick()
        results['AllTick'] = {'success': success, 'message': message}
    else:
        logger.info("\n📋 跳过AllTick测试: Token未配置")
        results['AllTick'] = {'success': False, 'message': 'Token未配置'}
    
    # 2. 测试Alpha Vantage
    if alpha_key:
        logger.info("\n📋 测试 2/2: Alpha Vantage数据源")
        logger.info("-" * 40)
        success, message = test_alpha_vantage()
        results['Alpha Vantage'] = {'success': success, 'message': message}
    else:
        logger.info("\n📋 跳过Alpha Vantage测试: API Key未配置")
        results['Alpha Vantage'] = {'success': False, 'message': 'API Key未配置'}
    
    # 3. 测试股票列表拉取
    logger.info("\n📋 测试股票列表拉取")
    logger.info("-" * 40)
    stock_results = test_stock_list_fetch()
    
    # 总结
    logger.info("\n" + "=" * 60)
    logger.info("📊 测试结果总结")
    logger.info("=" * 60)
    
    available_sources = []
    for source, result in results.items():
        status = "✅ 可用" if result['success'] else "❌ 不可用"
        logger.info(f"{source}: {status} - {result['message']}")
        
        if result['success']:
            available_sources.append(source)
    
    logger.info(f"\n📈 可用数据源 ({len(available_sources)}): {', '.join(available_sources) if available_sources else '无'}")
    
    # 股票列表结果
    logger.info("\n📋 股票列表拉取结果:")
    for source, count in stock_results.items():
        logger.info(f"  {source}: {count}")
    
    # 建议
    logger.info("\n💡 建议:")
    if available_sources:
        logger.info(f"  - 可以使用 {', '.join(available_sources)} 进行数据拉取")
        logger.info("  - 建议在Dashboard中测试数据更新功能")
    else:
        logger.info("  - 当前无可用数据源，请检查API配置")
    
    logger.info("\n🚀 下一步操作:")
    logger.info("1. 启动Dashboard: python -m streamlit run dashboard/app.py --server.port 8507")
    logger.info("2. 进入'数据管理'页面测试数据更新")
    logger.info("3. 选择可用的数据源进行数据拉取")
    
    return len(available_sources) > 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
