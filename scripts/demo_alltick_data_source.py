#!/usr/bin/env python3
"""
AllTick数据源演示脚本
展示AllTick API的功能和使用方法
"""
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.data_sources.base_data_source import DataRequest, DataType
from src.data_sources.alltick_data_source import AllTickDataSource
from src.data_sources.data_source_manager import DataSourceManager

logger = get_logger("alltick_demo")


def demo_alltick_basic():
    """演示AllTick基本功能"""
    logger.info("🚀 演示AllTick数据源基本功能")
    logger.info("=" * 60)
    
    try:
        # 注意：需要配置ALLTICK_TOKEN环境变量
        import os
        token = os.getenv('ALLTICK_TOKEN', '')
        
        if not token:
            logger.warning("⚠️ 未配置ALLTICK_TOKEN环境变量")
            logger.info("请设置环境变量: export ALLTICK_TOKEN='your_token_here'")
            return False
        
        # 创建AllTick数据源
        alltick_source = AllTickDataSource(token)
        
        # 初始化
        if not alltick_source.initialize():
            logger.error("❌ AllTick数据源初始化失败")
            return False
        
        logger.success("✅ AllTick数据源初始化成功")
        
        # 检查可用性
        status = alltick_source.check_availability()
        logger.info(f"数据源状态: {status}")
        
        # 获取支持的数据类型
        supported_types = alltick_source.get_supported_data_types()
        logger.info(f"支持的数据类型: {[dt.value for dt in supported_types]}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ AllTick基本功能演示失败: {e}")
        return False


def demo_stock_basic():
    """演示获取股票基础信息"""
    logger.info("\n📊 演示获取股票基础信息")
    logger.info("=" * 60)
    
    try:
        import os
        token = os.getenv('ALLTICK_TOKEN', '')
        
        if not token:
            logger.warning("⚠️ 未配置ALLTICK_TOKEN")
            return False
        
        alltick_source = AllTickDataSource(token)
        
        if not alltick_source.initialize():
            logger.error("❌ 初始化失败")
            return False
        
        # 获取中国股票基础信息
        logger.info("获取中国股票基础信息...")
        basic_request = DataRequest(
            data_type=DataType.STOCK_BASIC,
            extra_params={
                'market': 'cn',
                'type': 'stock'
            }
        )
        
        response = alltick_source.fetch_data(basic_request)
        
        if response.success:
            logger.success(f"✅ 获取股票基础信息成功，共 {len(response.data)} 只股票")
            if not response.data.empty:
                logger.info("前10只股票:")
                print(response.data.head(10).to_string())
        else:
            logger.error(f"❌ 获取失败: {response.error_message}")
        
        return response.success
        
    except Exception as e:
        logger.error(f"❌ 股票基础信息演示失败: {e}")
        return False


def demo_daily_quotes():
    """演示获取日线数据"""
    logger.info("\n📈 演示获取日线数据")
    logger.info("=" * 60)
    
    try:
        import os
        token = os.getenv('ALLTICK_TOKEN', '')
        
        if not token:
            logger.warning("⚠️ 未配置ALLTICK_TOKEN")
            return False
        
        alltick_source = AllTickDataSource(token)
        
        if not alltick_source.initialize():
            logger.error("❌ 初始化失败")
            return False
        
        # 获取平安银行日线数据
        symbol = "000001.SZ"  # 平安银行
        logger.info(f"获取 {symbol} 的日线数据...")
        
        daily_request = DataRequest(
            data_type=DataType.DAILY_QUOTES,
            symbol=symbol,
            start_date="2024-06-01",
            end_date="2024-06-15",
            extra_params={'adjust': 'qfq'}  # 前复权
        )
        
        response = alltick_source.fetch_data(daily_request)
        
        if response.success:
            logger.success(f"✅ 获取日线数据成功，共 {len(response.data)} 条记录")
            if not response.data.empty:
                logger.info("数据样例:")
                print(response.data.head().to_string())
                
                # 显示数据统计
                data = response.data
                logger.info(f"\n📊 数据统计:")
                logger.info(f"  日期范围: {data['trade_date'].min()} - {data['trade_date'].max()}")
                logger.info(f"  价格范围: ¥{data['close_price'].min():.2f} - ¥{data['close_price'].max():.2f}")
                logger.info(f"  平均成交量: {data['vol'].mean():,.0f}")
                
                if 'pct_chg' in data.columns:
                    logger.info(f"  平均涨跌幅: {data['pct_chg'].mean():.2f}%")
        else:
            logger.error(f"❌ 获取失败: {response.error_message}")
        
        return response.success
        
    except Exception as e:
        logger.error(f"❌ 日线数据演示失败: {e}")
        return False


def demo_minute_quotes():
    """演示获取分钟数据"""
    logger.info("\n⏱️ 演示获取分钟数据")
    logger.info("=" * 60)
    
    try:
        import os
        token = os.getenv('ALLTICK_TOKEN', '')
        
        if not token:
            logger.warning("⚠️ 未配置ALLTICK_TOKEN")
            return False
        
        alltick_source = AllTickDataSource(token)
        
        if not alltick_source.initialize():
            logger.error("❌ 初始化失败")
            return False
        
        # 获取5分钟数据
        symbol = "000001.SZ"
        logger.info(f"获取 {symbol} 的5分钟数据...")
        
        # 获取今天的数据
        today = datetime.now().strftime('%Y-%m-%d')
        
        minute_request = DataRequest(
            data_type=DataType.MINUTE_QUOTES,
            symbol=symbol,
            start_date=today,
            end_date=today,
            extra_params={
                'period': '5m',
                'adjust': 'qfq'
            }
        )
        
        response = alltick_source.fetch_data(minute_request)
        
        if response.success:
            logger.success(f"✅ 获取分钟数据成功，共 {len(response.data)} 条记录")
            if not response.data.empty:
                logger.info("数据样例:")
                print(response.data.head().to_string())
        else:
            logger.error(f"❌ 获取失败: {response.error_message}")
        
        return response.success
        
    except Exception as e:
        logger.error(f"❌ 分钟数据演示失败: {e}")
        return False


def demo_rate_limiting():
    """演示频率限制功能"""
    logger.info("\n🚦 演示频率限制功能")
    logger.info("=" * 60)
    
    try:
        import os
        token = os.getenv('ALLTICK_TOKEN', '')
        
        if not token:
            logger.warning("⚠️ 未配置ALLTICK_TOKEN")
            return False
        
        alltick_source = AllTickDataSource(token)
        
        if not alltick_source.initialize():
            logger.error("❌ 初始化失败")
            return False
        
        # 连续发送多个请求测试频率限制
        symbols = ["000001.SZ", "000002.SZ", "600000.SH"]
        
        logger.info("连续发送多个请求测试频率限制...")
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"第 {i} 个请求: {symbol}")
            
            start_time = time.time()
            
            request = DataRequest(
                data_type=DataType.DAILY_QUOTES,
                symbol=symbol,
                start_date="2024-06-10",
                end_date="2024-06-14"
            )
            
            response = alltick_source.fetch_data(request)
            
            elapsed_time = time.time() - start_time
            
            if response.success:
                logger.success(f"✅ 请求成功，耗时: {elapsed_time:.2f}秒，数据: {len(response.data)} 条")
            else:
                logger.error(f"❌ 请求失败: {response.error_message}")
        
        logger.info("✅ 频率限制测试完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 频率限制演示失败: {e}")
        return False


def demo_with_manager():
    """演示通过数据源管理器使用AllTick"""
    logger.info("\n🔄 演示通过数据源管理器使用AllTick")
    logger.info("=" * 60)
    
    try:
        import os
        token = os.getenv('ALLTICK_TOKEN', '')
        
        if not token:
            logger.warning("⚠️ 未配置ALLTICK_TOKEN")
            return False
        
        # 创建数据源管理器
        manager = DataSourceManager()
        
        # 创建并注册AllTick数据源
        alltick_source = AllTickDataSource(token)
        success = manager.register_data_source(alltick_source)
        
        if not success:
            logger.error("❌ 数据源注册失败")
            return False
        
        logger.success("✅ AllTick数据源注册成功")
        
        # 获取可用数据源
        available_sources = manager.get_available_sources(DataType.DAILY_QUOTES)
        logger.info(f"可用数据源: {available_sources}")
        
        # 通过管理器获取数据
        request = DataRequest(
            data_type=DataType.DAILY_QUOTES,
            symbol="000001.SZ",
            start_date="2024-06-01",
            end_date="2024-06-07"
        )
        
        response = manager.get_data(request)
        
        if response.success:
            logger.success(f"✅ 通过管理器获取数据成功")
            logger.info(f"数据源: {response.source}")
            logger.info(f"记录数: {len(response.data)}")
        else:
            logger.error(f"❌ 获取失败: {response.error_message}")
        
        return response.success
        
    except Exception as e:
        logger.error(f"❌ 数据源管理器演示失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🌟 AllTick数据源功能演示")
    logger.info("=" * 80)
    
    # 检查环境变量
    import os
    if not os.getenv('ALLTICK_TOKEN'):
        logger.error("❌ 未配置ALLTICK_TOKEN环境变量")
        logger.info("请先设置环境变量:")
        logger.info("export ALLTICK_TOKEN='your_alltick_token_here'")
        logger.info("\n获取AllTick Token:")
        logger.info("1. 访问 https://apis.alltick.co/")
        logger.info("2. 注册账号并获取API Token")
        logger.info("3. 设置环境变量后重新运行此脚本")
        return False
    
    demos = [
        ("AllTick基本功能", demo_alltick_basic),
        ("股票基础信息", demo_stock_basic),
        ("日线数据获取", demo_daily_quotes),
        ("分钟数据获取", demo_minute_quotes),
        ("频率限制测试", demo_rate_limiting),
        ("数据源管理器", demo_with_manager)
    ]
    
    success_count = 0
    total_count = len(demos)
    
    for demo_name, demo_func in demos:
        try:
            logger.info(f"\n🎯 开始演示: {demo_name}")
            success = demo_func()
            if success:
                success_count += 1
                logger.success(f"✅ {demo_name} 演示成功")
            else:
                logger.error(f"❌ {demo_name} 演示失败")
        except Exception as e:
            logger.error(f"❌ {demo_name} 演示异常: {e}")
    
    # 总结
    logger.info("\n" + "=" * 80)
    logger.info("📊 演示总结")
    logger.info("=" * 80)
    logger.info(f"总演示数: {total_count}")
    logger.info(f"成功演示: {success_count}")
    logger.info(f"失败演示: {total_count - success_count}")
    logger.info(f"成功率: {success_count/total_count*100:.1f}%")
    
    if success_count == total_count:
        logger.success("🎉 所有演示都成功完成！")
        logger.info("\n💡 AllTick数据源的主要特点:")
        logger.info("  ✅ 专业金融数据API")
        logger.info("  ✅ 支持多种数据类型")
        logger.info("  ✅ 智能频率限制控制")
        logger.info("  ✅ 高质量实时数据")
        logger.info("  ✅ 完善的错误处理")
    else:
        logger.warning("⚠️ 部分演示失败，请检查网络连接和API配置")
    
    return success_count == total_count


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
