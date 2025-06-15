#!/usr/bin/env python3
"""
简化版多数据源演示
只使用Tushare数据源进行演示
"""
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.data_sources.base_data_source import DataRequest, DataType
from src.data_sources.tushare_data_source import TushareDataSource
from src.data_sources.data_source_manager import DataSourceManager
from src.data_sources.cache_manager import CacheManager

logger = get_logger("simple_multi_source_demo")


def demo_tushare_data_source():
    """演示Tushare数据源"""
    logger.info("🚀 演示Tushare数据源")
    logger.info("=" * 60)
    
    try:
        # 创建Tushare数据源
        tushare_source = TushareDataSource()
        
        # 初始化
        if not tushare_source.initialize():
            logger.error("❌ Tushare数据源初始化失败")
            return False
        
        logger.success("✅ Tushare数据源初始化成功")
        
        # 检查可用性
        status = tushare_source.check_availability()
        logger.info(f"数据源状态: {status}")
        
        # 获取支持的数据类型
        supported_types = tushare_source.get_supported_data_types()
        logger.info(f"支持的数据类型: {[dt.value for dt in supported_types]}")
        
        # 测试获取股票基础信息
        logger.info("\n📊 测试获取股票基础信息")
        basic_request = DataRequest(
            data_type=DataType.STOCK_BASIC,
            extra_params={'list_status': 'L'}
        )
        
        basic_response = tushare_source.fetch_data(basic_request)
        
        if basic_response.success:
            logger.success(f"✅ 获取股票基础信息成功，共 {len(basic_response.data)} 只股票")
            if not basic_response.data.empty:
                logger.info("前5只股票:")
                print(basic_response.data.head().to_string())
        else:
            logger.error(f"❌ 获取股票基础信息失败: {basic_response.error_message}")
        
        # 测试获取日线数据
        if basic_response.success and not basic_response.data.empty:
            sample_code = basic_response.data.iloc[0]['ts_code']
            
            logger.info(f"\n📈 测试获取日线数据: {sample_code}")
            daily_request = DataRequest(
                data_type=DataType.DAILY_QUOTES,
                symbol=sample_code,
                start_date="2024-06-01",
                end_date="2024-06-07"
            )
            
            daily_response = tushare_source.fetch_data(daily_request)
            
            if daily_response.success:
                logger.success(f"✅ 获取日线数据成功，共 {len(daily_response.data)} 条记录")
                if not daily_response.data.empty:
                    logger.info("数据样例:")
                    print(daily_response.data.head().to_string())
            else:
                logger.error(f"❌ 获取日线数据失败: {daily_response.error_message}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Tushare数据源演示失败: {e}")
        return False


def demo_data_source_manager():
    """演示数据源管理器"""
    logger.info("\n🔄 演示数据源管理器")
    logger.info("=" * 60)
    
    try:
        # 创建数据源管理器
        manager = DataSourceManager()
        
        # 创建并注册Tushare数据源
        tushare_source = TushareDataSource()
        success = manager.register_data_source(tushare_source)
        
        if not success:
            logger.error("❌ 数据源注册失败")
            return False
        
        logger.success("✅ 数据源注册成功")
        
        # 获取可用数据源
        available_sources = manager.get_available_sources(DataType.DAILY_QUOTES)
        logger.info(f"可用数据源: {available_sources}")
        
        # 健康检查
        health_status = manager.health_check()
        logger.info("健康检查结果:")
        for source_name, status in health_status.get('sources', {}).items():
            status_emoji = "✅" if status.get('status') == 'available' else "❌"
            logger.info(f"  {status_emoji} {source_name}: {status.get('status', 'unknown')}")
        
        # 测试数据获取
        logger.info("\n📊 测试通过管理器获取数据")
        request = DataRequest(
            data_type=DataType.DAILY_QUOTES,
            symbol="000001.SZ",
            start_date="2024-06-01",
            end_date="2024-06-07"
        )
        
        response = manager.get_data(request)
        
        if response.success:
            logger.success(f"✅ 数据获取成功")
            logger.info(f"数据源: {response.source}")
            logger.info(f"记录数: {len(response.data)}")
            
            if not response.data.empty:
                logger.info("数据样例:")
                print(response.data.head(3).to_string())
        else:
            logger.error(f"❌ 数据获取失败: {response.error_message}")
        
        return response.success
        
    except Exception as e:
        logger.error(f"❌ 数据源管理器演示失败: {e}")
        return False


def demo_cache_manager():
    """演示缓存管理器"""
    logger.info("\n💾 演示缓存管理器")
    logger.info("=" * 60)
    
    try:
        # 创建缓存管理器
        cache_manager = CacheManager(cache_dir="demo_cache")
        
        # 创建测试请求
        request = DataRequest(
            data_type=DataType.DAILY_QUOTES,
            symbol="000001.SZ",
            start_date="2024-06-01",
            end_date="2024-06-07"
        )
        
        # 第一次获取（应该没有缓存）
        logger.info("第一次获取数据（无缓存）")
        cached_response = cache_manager.get(request)
        
        if cached_response:
            logger.info("从缓存获取到数据")
        else:
            logger.info("缓存中无数据，需要从数据源获取")
        
        # 模拟从数据源获取数据
        tushare_source = TushareDataSource()
        if tushare_source.initialize():
            response = tushare_source.fetch_data(request)
            
            if response.success:
                # 存储到缓存
                cache_manager.put(request, response)
                logger.success("✅ 数据已存储到缓存")
                
                # 第二次获取（应该从缓存获取）
                logger.info("\n第二次获取相同数据（从缓存）")
                cached_response = cache_manager.get(request)
                
                if cached_response:
                    logger.success("✅ 从缓存成功获取数据")
                    logger.info(f"缓存数据记录数: {len(cached_response.data)}")
                else:
                    logger.warning("⚠️ 缓存获取失败")
        
        # 获取缓存统计
        cache_stats = cache_manager.get_cache_stats()
        logger.info(f"\n📊 缓存统计: {cache_stats}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 缓存管理器演示失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🌟 简化版多数据源中间层演示")
    logger.info("=" * 80)
    
    demos = [
        ("Tushare数据源", demo_tushare_data_source),
        ("数据源管理器", demo_data_source_manager),
        ("缓存管理器", demo_cache_manager)
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
        logger.info("\n💡 多数据源中间层的核心功能:")
        logger.info("  ✅ 统一的数据源接口")
        logger.info("  ✅ 数据源管理和调度")
        logger.info("  ✅ 智能缓存机制")
        logger.info("  ✅ 故障转移能力")
        logger.info("  ✅ 数据质量控制")
    else:
        logger.warning("⚠️ 部分演示失败，请检查配置")
    
    return success_count == total_count


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
