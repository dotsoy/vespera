#!/usr/bin/env python3
"""
多数据源中间层演示脚本
展示统一数据接口的功能和优势
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
from src.data_sources.data_source_factory import get_data_service
from src.data_sources.data_fusion_engine import FusionStrategy, ValidationLevel

logger = get_logger("multi_data_sources_demo")


def demo_basic_usage():
    """演示基本用法"""
    logger.info("🚀 演示1: 基本数据获取")
    logger.info("=" * 60)
    
    try:
        # 获取数据服务
        data_service = get_data_service(enable_cache=True)
        
        # 创建数据请求
        request = DataRequest(
            data_type=DataType.DAILY_QUOTES,
            symbol="AAPL",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        # 获取数据
        logger.info("📊 请求苹果公司(AAPL)2024年1月的日线数据")
        response = data_service.get_data(request)
        
        if response.success:
            logger.success(f"✅ 数据获取成功！")
            logger.info(f"数据源: {response.source}")
            logger.info(f"数据记录数: {len(response.data)}")
            logger.info(f"数据列: {list(response.data.columns)}")
            
            # 显示前几行数据
            if not response.data.empty:
                logger.info("前5行数据:")
                print(response.data.head().to_string())
        else:
            logger.error(f"❌ 数据获取失败: {response.error_message}")
        
        return response.success
        
    except Exception as e:
        logger.error(f"❌ 演示1失败: {e}")
        return False


def demo_multiple_sources():
    """演示多数据源获取和融合"""
    logger.info("\n🔄 演示2: 多数据源融合")
    logger.info("=" * 60)
    
    try:
        data_service = get_data_service(enable_cache=True)
        
        # 获取可用数据源
        available_sources = data_service.get_available_sources(DataType.DAILY_QUOTES)
        logger.info(f"可用数据源: {available_sources}")
        
        if len(available_sources) < 2:
            logger.warning("⚠️ 可用数据源不足，无法演示多源融合")
            return True
        
        # 创建数据请求
        request = DataRequest(
            data_type=DataType.DAILY_QUOTES,
            symbol="AAPL",
            start_date="2024-06-01",
            end_date="2024-06-07"
        )
        
        # 使用不同的融合策略
        strategies = [
            ('first_success', '第一个成功'),
            ('quality_based', '基于质量'),
            ('weighted_average', '加权平均')
        ]
        
        for strategy, description in strategies:
            logger.info(f"\n📈 使用 {description} 策略获取数据")
            
            response = data_service.get_data(
                request=request,
                merge_strategy=strategy,
                use_cache=False  # 禁用缓存以测试不同策略
            )
            
            if response.success:
                logger.success(f"✅ {description} 策略成功")
                logger.info(f"数据源: {response.source}")
                logger.info(f"记录数: {len(response.data)}")
                
                if hasattr(response, 'metadata') and response.metadata:
                    logger.info(f"元数据: {response.metadata}")
            else:
                logger.error(f"❌ {description} 策略失败: {response.error_message}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 演示2失败: {e}")
        return False


def demo_caching():
    """演示缓存功能"""
    logger.info("\n💾 演示3: 缓存功能")
    logger.info("=" * 60)
    
    try:
        data_service = get_data_service(enable_cache=True)
        
        request = DataRequest(
            data_type=DataType.DAILY_QUOTES,
            symbol="AAPL",
            start_date="2024-05-01",
            end_date="2024-05-31"
        )
        
        # 第一次请求（从数据源获取）
        logger.info("📊 第一次请求数据（从数据源）")
        start_time = time.time()
        response1 = data_service.get_data(request, use_cache=True)
        time1 = time.time() - start_time
        
        if response1.success:
            logger.success(f"✅ 第一次请求成功，耗时: {time1:.2f}秒")
            logger.info(f"数据源: {response1.source}")
            logger.info(f"记录数: {len(response1.data)}")
        
        # 第二次请求（从缓存获取）
        logger.info("\n📊 第二次请求相同数据（从缓存）")
        start_time = time.time()
        response2 = data_service.get_data(request, use_cache=True)
        time2 = time.time() - start_time
        
        if response2.success:
            logger.success(f"✅ 第二次请求成功，耗时: {time2:.2f}秒")
            logger.info(f"数据源: {response2.source}")
            logger.info(f"记录数: {len(response2.data)}")
            
            # 计算性能提升
            if time1 > 0:
                speedup = time1 / time2 if time2 > 0 else float('inf')
                logger.info(f"🚀 缓存性能提升: {speedup:.1f}倍")
        
        # 显示缓存统计
        health_status = data_service.health_check()
        if 'cache' in health_status:
            cache_stats = health_status['cache']
            logger.info(f"\n📊 缓存统计:")
            logger.info(f"  内存缓存: {cache_stats.get('memory_cache_size', 0)} 项")
            logger.info(f"  磁盘缓存: {cache_stats.get('disk_cache_files', 0)} 文件")
            logger.info(f"  数据库缓存: {cache_stats.get('database_cache_records', 0)} 记录")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 演示3失败: {e}")
        return False


def demo_fault_tolerance():
    """演示故障容错"""
    logger.info("\n🛡️ 演示4: 故障容错")
    logger.info("=" * 60)
    
    try:
        data_service = get_data_service(enable_cache=True)
        
        # 获取健康状态
        health_status = data_service.health_check()
        logger.info("📊 数据源健康状态:")
        
        for source_name, status in health_status.get('sources', {}).items():
            status_emoji = "✅" if status.get('status') == 'available' else "❌"
            logger.info(f"  {status_emoji} {source_name}: {status.get('status', 'unknown')}")
        
        # 测试故障转移
        request = DataRequest(
            data_type=DataType.DAILY_QUOTES,
            symbol="AAPL",
            start_date="2024-04-01",
            end_date="2024-04-07"
        )
        
        logger.info("\n🔄 测试故障转移机制")
        
        # 指定一个可能不可用的数据源作为首选
        preferred_sources = ["NonExistentSource", "Tushare", "Yahoo Finance"]
        
        response = data_service.get_data(
            request=request,
            preferred_sources=preferred_sources,
            fallback=True
        )
        
        if response.success:
            logger.success("✅ 故障转移成功")
            logger.info(f"实际使用的数据源: {response.source}")
            logger.info(f"获取到 {len(response.data)} 条记录")
        else:
            logger.error(f"❌ 故障转移失败: {response.error_message}")
        
        return response.success
        
    except Exception as e:
        logger.error(f"❌ 演示4失败: {e}")
        return False


def demo_data_quality():
    """演示数据质量评估"""
    logger.info("\n🔍 演示5: 数据质量评估")
    logger.info("=" * 60)
    
    try:
        data_service = get_data_service(enable_cache=True)
        
        request = DataRequest(
            data_type=DataType.DAILY_QUOTES,
            symbol="AAPL",
            start_date="2024-03-01",
            end_date="2024-03-07"
        )
        
        # 使用质量验证
        response = data_service.get_data(
            request=request,
            merge_strategy='quality_based',
            use_cache=False
        )
        
        if response.success:
            logger.success("✅ 数据获取成功")
            logger.info(f"数据源: {response.source}")
            logger.info(f"记录数: {len(response.data)}")
            
            # 显示数据质量信息
            if hasattr(response, 'metadata') and response.metadata:
                metadata = response.metadata
                logger.info("\n📊 数据质量信息:")
                
                if 'source_count' in metadata:
                    logger.info(f"  参与融合的数据源数量: {metadata['source_count']}")
                
                if 'fusion_timestamp' in metadata:
                    logger.info(f"  融合时间: {metadata['fusion_timestamp']}")
            
            # 基本数据质量检查
            data = response.data
            if not data.empty:
                logger.info("\n🔍 数据质量检查:")
                
                # 检查缺失值
                missing_count = data.isnull().sum().sum()
                total_cells = data.size
                completeness = 1 - (missing_count / total_cells) if total_cells > 0 else 0
                logger.info(f"  数据完整性: {completeness:.2%}")
                
                # 检查价格数据合理性
                if 'close_price' in data.columns:
                    prices = data['close_price']
                    logger.info(f"  价格范围: ${prices.min():.2f} - ${prices.max():.2f}")
                    
                    if (prices <= 0).any():
                        logger.warning("  ⚠️ 发现非正价格数据")
                    else:
                        logger.info("  ✅ 价格数据合理")
                
                # 检查成交量
                if 'vol' in data.columns:
                    volumes = data['vol']
                    avg_volume = volumes.mean()
                    logger.info(f"  平均成交量: {avg_volume:,.0f}")
        else:
            logger.error(f"❌ 数据获取失败: {response.error_message}")
        
        return response.success
        
    except Exception as e:
        logger.error(f"❌ 演示5失败: {e}")
        return False


def demo_performance_comparison():
    """演示性能对比"""
    logger.info("\n⚡ 演示6: 性能对比")
    logger.info("=" * 60)
    
    try:
        data_service = get_data_service(enable_cache=True)
        
        # 清空缓存以确保公平比较
        data_service.clear_cache()
        
        request = DataRequest(
            data_type=DataType.DAILY_QUOTES,
            symbol="AAPL",
            start_date="2024-02-01",
            end_date="2024-02-29"
        )
        
        # 测试不同策略的性能
        strategies = [
            ('first_success', '第一个成功'),
            ('quality_based', '基于质量')
        ]
        
        performance_results = []
        
        for strategy, description in strategies:
            logger.info(f"\n📊 测试 {description} 策略性能")
            
            start_time = time.time()
            response = data_service.get_data(
                request=request,
                merge_strategy=strategy,
                use_cache=False
            )
            elapsed_time = time.time() - start_time
            
            if response.success:
                performance_results.append({
                    'strategy': description,
                    'time': elapsed_time,
                    'records': len(response.data),
                    'source': response.source
                })
                
                logger.success(f"✅ {description} 完成")
                logger.info(f"耗时: {elapsed_time:.2f}秒")
                logger.info(f"记录数: {len(response.data)}")
                logger.info(f"数据源: {response.source}")
        
        # 显示性能对比
        if len(performance_results) > 1:
            logger.info("\n📊 性能对比结果:")
            for result in performance_results:
                logger.info(f"  {result['strategy']}: {result['time']:.2f}秒 ({result['records']} 记录)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 演示6失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🌟 多数据源中间层功能演示")
    logger.info("=" * 80)
    
    demos = [
        ("基本数据获取", demo_basic_usage),
        ("多数据源融合", demo_multiple_sources),
        ("缓存功能", demo_caching),
        ("故障容错", demo_fault_tolerance),
        ("数据质量评估", demo_data_quality),
        ("性能对比", demo_performance_comparison)
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
        logger.info("\n💡 多数据源中间层的主要优势:")
        logger.info("  ✅ 统一的数据接口，简化开发")
        logger.info("  ✅ 多数据源支持，提高可靠性")
        logger.info("  ✅ 智能缓存机制，提升性能")
        logger.info("  ✅ 故障转移能力，保证可用性")
        logger.info("  ✅ 数据质量评估，确保准确性")
        logger.info("  ✅ 灵活的融合策略，满足不同需求")
    else:
        logger.warning("⚠️ 部分演示失败，请检查配置和网络连接")
    
    # 清理资源
    try:
        from src.data_sources.data_source_factory import close_data_service
        close_data_service()
        logger.info("🔧 资源清理完成")
    except Exception as e:
        logger.warning(f"资源清理失败: {e}")
    
    return success_count == total_count


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
