#!/usr/bin/env python3
"""
Vespera 简化工作流程测试
基于现有模块进行实际功能测试
"""

import sys
import os
import asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_core_modules():
    """测试核心模块导入"""
    print("🔧 测试核心模块导入")
    print("=" * 60)
    
    results = {}
    
    # 测试缓存管理器
    try:
        from src.utils.cache_manager import global_cache_manager
        results['cache_manager'] = True
        print("✅ 缓存管理器导入成功")
    except Exception as e:
        results['cache_manager'] = False
        print(f"❌ 缓存管理器导入失败: {e}")
    
    # 测试异步任务管理器
    try:
        from src.utils.async_processor import AsyncTaskManager
        results['async_task_manager'] = True
        print("✅ 异步任务管理器导入成功")
    except Exception as e:
        results['async_task_manager'] = False
        print(f"❌ 异步任务管理器导入失败: {e}")
    
    # 测试数据质量监控
    try:
        from src.monitoring.data_quality_monitor import DataQualityMonitor
        results['data_quality_monitor'] = True
        print("✅ 数据质量监控器导入成功")
    except Exception as e:
        results['data_quality_monitor'] = False
        print(f"❌ 数据质量监控器导入失败: {e}")
    
    # 测试启明星策略
    try:
        from src.strategies.qiming_star.qiming_star_strategy import QimingStarStrategy
        results['qiming_star_strategy'] = True
        print("✅ 启明星策略导入成功")
    except Exception as e:
        results['qiming_star_strategy'] = False
        print(f"❌ 启明星策略导入失败: {e}")
    
    # 测试四维分析器
    try:
        from src.strategies.qiming_star.four_dimensional_analyzer import FourDimensionalAnalyzer
        results['four_dimensional_analyzer'] = True
        print("✅ 四维分析器导入成功")
    except Exception as e:
        results['four_dimensional_analyzer'] = False
        print(f"❌ 四维分析器导入失败: {e}")
    
    # 测试信号融合引擎
    try:
        from src.strategies.qiming_star.signal_fusion_engine import SignalFusionEngine
        results['signal_fusion_engine'] = True
        print("✅ 信号融合引擎导入成功")
    except Exception as e:
        results['signal_fusion_engine'] = False
        print(f"❌ 信号融合引擎导入失败: {e}")
    
    return results

async def test_async_processing():
    """测试异步处理功能"""
    print("\n⚡ 测试异步处理功能")
    print("=" * 60)
    
    results = {}
    
    try:
        from src.utils.async_processor import AsyncTaskManager
        
        # 初始化异步任务管理器
        task_manager = AsyncTaskManager(max_concurrent_tasks=4)
        
        # 定义测试任务
        async def test_task(data):
            await asyncio.sleep(0.1)  # 模拟异步操作
            return data * 2
        
        # 提交多个任务
        task_ids = []
        for i in range(5):
            task_id = await task_manager.submit_task(test_task, i, mode="parallel")
            task_ids.append(task_id)
        
        # 等待所有任务完成
        await task_manager.wait_for_completion(task_ids)
        
        # 获取结果
        all_completed = True
        for task_id in task_ids:
            task_result = task_manager.get_task_result(task_id)
            if not task_result or task_result.status.value != "COMPLETED":
                all_completed = False
                break
        
        if all_completed:
            results['async_tasks'] = True
            print(f"✅ 异步任务处理成功: {len(task_ids)} 个任务")
        else:
            results['async_tasks'] = False
            print("❌ 异步任务处理失败")
        
        # 获取统计信息
        stats = task_manager.get_stats()
        if stats:
            results['async_stats'] = True
            print(f"✅ 异步处理统计: {stats}")
        else:
            results['async_stats'] = False
            print("❌ 异步处理统计获取失败")
        
    except Exception as e:
        results['async_processing'] = False
        print(f"❌ 异步处理测试失败: {e}")
    
    return results

def test_cache_functionality():
    """测试缓存功能"""
    print("\n💾 测试缓存功能")
    print("=" * 60)
    
    results = {}
    
    try:
        from src.utils.cache_manager import global_cache_manager
        
        # 测试基本缓存操作
        test_key = "test_workflow_cache"
        test_data = {"timestamp": datetime.now().isoformat(), "data": [1, 2, 3, 4, 5]}
        
        # 写入缓存
        global_cache_manager.set(test_key, test_data)
        print("✅ 缓存写入成功")
        
        # 读取缓存
        cached_data = global_cache_manager.get(test_key)
        if cached_data == test_data:
            results['cache_operations'] = True
            print("✅ 缓存读取成功")
        else:
            results['cache_operations'] = False
            print("❌ 缓存数据不匹配")
        
        # 测试缓存统计
        try:
            cache_stats = global_cache_manager.get_cache_stats()
            if cache_stats:
                results['cache_stats'] = True
                print(f"✅ 缓存统计获取成功")
            else:
                results['cache_stats'] = False
                print("❌ 缓存统计为空")
        except Exception as e:
            results['cache_stats'] = False
            print(f"❌ 缓存统计获取失败: {e}")
        
        # 测试缓存清理
        global_cache_manager.delete(test_key)
        cleared_data = global_cache_manager.get(test_key)
        if cleared_data is None:
            results['cache_cleanup'] = True
            print("✅ 缓存清理成功")
        else:
            results['cache_cleanup'] = False
            print("❌ 缓存清理失败")
        
    except Exception as e:
        results['cache_functionality'] = False
        print(f"❌ 缓存功能测试失败: {e}")
    
    return results

def test_data_quality_monitoring():
    """测试数据质量监控"""
    print("\n📊 测试数据质量监控")
    print("=" * 60)
    
    results = {}
    
    try:
        from src.monitoring.data_quality_monitor import DataQualityMonitor
        
        monitor = DataQualityMonitor()
        
        # 创建测试数据
        test_df = pd.DataFrame({
            'symbol': ['000001.SZ'] * 100,
            'date': pd.date_range('2024-01-01', periods=100),
            'open': [10.0 + i * 0.1 for i in range(100)],
            'high': [10.5 + i * 0.1 for i in range(100)],
            'low': [9.5 + i * 0.1 for i in range(100)],
            'close': [10.2 + i * 0.1 for i in range(100)],
            'volume': [1000000 + i * 10000 for i in range(100)]
        })
        
        # 测试数据验证
        try:
            validation_result = monitor.validate_data(test_df)
            if validation_result:
                results['data_validation'] = True
                print("✅ 数据验证成功")
            else:
                results['data_validation'] = False
                print("❌ 数据验证失败")
        except Exception as e:
            results['data_validation'] = False
            print(f"❌ 数据验证失败: {e}")
        
        # 测试数据监控
        try:
            monitor.monitor_data_quality(test_df, "test_dataset")
            results['data_monitoring'] = True
            print("✅ 数据监控成功")
        except Exception as e:
            results['data_monitoring'] = False
            print(f"❌ 数据监控失败: {e}")
        
    except Exception as e:
        results['data_quality_monitoring'] = False
        print(f"❌ 数据质量监控测试失败: {e}")
    
    return results

def test_strategy_components():
    """测试策略组件"""
    print("\n🎯 测试策略组件")
    print("=" * 60)
    
    results = {}
    
    # 测试四维分析器
    try:
        from src.strategies.qiming_star.four_dimensional_analyzer import FourDimensionalAnalyzer
        
        analyzer = FourDimensionalAnalyzer()
        
        # 创建测试数据
        test_data = {
            'symbol': '000001.SZ',
            'price_data': pd.DataFrame({
                'close': [10.0 + i * 0.1 for i in range(50)],
                'high': [10.5 + i * 0.1 for i in range(50)],
                'low': [9.5 + i * 0.1 for i in range(50)],
                'volume': [1000000 + i * 10000 for i in range(50)]
            }),
            'capital_data': {
                'net_inflow': 1000000,
                'main_inflow': 500000,
                'retail_inflow': 500000
            }
        }
        
        # 执行四维分析
        try:
            analysis_result = analyzer.analyze(test_data)
            if analysis_result and 'technical_score' in analysis_result:
                results['four_dimensional_analysis'] = True
                print("✅ 四维分析成功")
            else:
                results['four_dimensional_analysis'] = False
                print("❌ 四维分析结果异常")
        except Exception as e:
            results['four_dimensional_analysis'] = False
            print(f"❌ 四维分析失败: {e}")
        
    except Exception as e:
        results['four_dimensional_analyzer'] = False
        print(f"❌ 四维分析器测试失败: {e}")
    
    # 测试信号融合引擎
    try:
        from src.strategies.qiming_star.signal_fusion_engine import SignalFusionEngine
        
        fusion_engine = SignalFusionEngine()
        
        # 创建测试信号
        test_signals = {
            'technical_signal': {'action': 'BUY', 'confidence': 0.75},
            'capital_signal': {'action': 'BUY', 'confidence': 0.80},
            'relative_strength_signal': {'action': 'HOLD', 'confidence': 0.60},
            'catalyst_signal': {'action': 'BUY', 'confidence': 0.70}
        }
        
        # 执行信号融合
        try:
            fused_signal = fusion_engine.fuse_signals(test_signals)
            if fused_signal and 'final_action' in fused_signal:
                results['signal_fusion'] = True
                print(f"✅ 信号融合成功: {fused_signal['final_action']}")
            else:
                results['signal_fusion'] = False
                print("❌ 信号融合结果异常")
        except Exception as e:
            results['signal_fusion'] = False
            print(f"❌ 信号融合失败: {e}")
        
    except Exception as e:
        results['signal_fusion_engine'] = False
        print(f"❌ 信号融合引擎测试失败: {e}")
    
    return results

async def main():
    """主测试函数"""
    print("🔄 Vespera 简化工作流程测试")
    print(f"测试时间: {datetime.now()}")
    print("=" * 80)
    
    # 执行所有测试
    start_time = time.time()
    
    core_results = test_core_modules()
    async_results = await test_async_processing()
    cache_results = test_cache_functionality()
    quality_results = test_data_quality_monitoring()
    strategy_results = test_strategy_components()
    
    end_time = time.time()
    
    # 汇总所有结果
    all_results = {}
    all_results.update(core_results)
    all_results.update(async_results)
    all_results.update(cache_results)
    all_results.update(quality_results)
    all_results.update(strategy_results)
    
    # 计算总体成功率
    total_tests = len(all_results)
    passed_tests = sum(all_results.values())
    success_rate = passed_tests / total_tests * 100 if total_tests > 0 else 0
    
    print("\n📊 简化工作流程测试结果汇总")
    print("=" * 80)
    print(f"总测试数: {total_tests}")
    print(f"通过测试: {passed_tests}")
    print(f"成功率: {success_rate:.1f}%")
    print(f"测试耗时: {end_time - start_time:.2f} 秒")
    
    # 详细结果分析
    print("\n📋 详细测试结果:")
    for test_name, result in all_results.items():
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name}: {status}")
    
    # 给出总体评估
    if success_rate >= 90:
        print("\n🎉 工作流程测试优秀! 项目核心功能完美运行!")
    elif success_rate >= 80:
        print("\n✅ 工作流程测试良好! 项目核心功能基本正常!")
    elif success_rate >= 70:
        print("\n⚠️ 工作流程测试一般，部分功能需要完善")
    else:
        print("\n❌ 工作流程测试较差，核心功能存在问题")
    
    # 生成运行建议
    print("\n💡 运行建议:")
    if all_results.get('cache_operations', False):
        print("1. ✅ 缓存系统正常，性能优化已启用")
    else:
        print("1. ⚠️ 缓存系统异常，可能影响性能")
    
    if all_results.get('async_tasks', False):
        print("2. ✅ 异步处理正常，支持并发操作")
    else:
        print("2. ⚠️ 异步处理异常，并发性能受限")
    
    if all_results.get('signal_fusion', False):
        print("3. ✅ 信号融合正常，策略决策功能可用")
    else:
        print("3. ⚠️ 信号融合异常，策略决策需要修复")
    
    print("\n🚀 启动命令:")
    print("   streamlit run dashboard/app.py  # 启动Dashboard")
    print("   python test_dashboard_functionality.py  # 测试Dashboard功能")

if __name__ == "__main__":
    asyncio.run(main())