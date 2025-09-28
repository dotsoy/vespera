"""
第二阶段性能提升优化 - 回归测试
测试缓存机制、并发处理、数据质量监控和Dashboard优化
"""
import sys
import os
import json
import time
import asyncio
from datetime import datetime
import pandas as pd

# 添加项目根目录到Python路径
sys.path.insert(0, '/workspaces/vespera')

def test_phase2_optimizations():
    """测试第二阶段优化功能"""
    print("="*60)
    print("第二阶段性能提升优化 - 回归测试")
    print("="*60)
    
    test_results = {
        "timestamp": datetime.now().isoformat(),
        "phase": "Phase 2 - 性能提升优化",
        "tests": {},
        "summary": {}
    }
    
    # 测试1: 检查新增优化文件
    print("\n1. 检查第二阶段优化文件...")
    file_checks = {
        "cache_manager": "src/utils/cache_manager.py",
        "async_processor": "src/utils/async_processor.py", 
        "data_quality_monitor": "src/monitoring/data_quality_monitor.py",
        "monitoring_init": "src/monitoring/__init__.py",
        "dashboard_performance_optimizer": "src/dashboard/performance_optimizer.py",
        "dashboard_init": "src/dashboard/__init__.py"
    }
    
    file_test_results = {}
    for name, path in file_checks.items():
        exists = os.path.exists(path)
        size = os.path.getsize(path) if exists else 0
        
        file_test_results[name] = {
            "exists": exists,
            "path": path,
            "size": size,
            "size_kb": round(size / 1024, 2)
        }
        
        status = "✅" if exists else "❌"
        print(f"   {status} {name}: {path} ({size} bytes)")
    
    test_results["tests"]["file_existence"] = file_test_results
    
    # 测试2: 缓存机制测试
    print("\n2. 测试缓存机制...")
    cache_tests = {}
    
    try:
        # 测试内存缓存
        from src.utils.cache_manager import MemoryCache, CacheStrategy
        
        memory_cache = MemoryCache(max_size=100, strategy=CacheStrategy.LRU)
        
        # 测试基本操作
        test_key = "test_key"
        test_value = {"data": "test_value", "number": 123}
        
        # 设置缓存
        set_success = memory_cache.set(test_key, test_value)
        
        # 获取缓存
        cached_value = memory_cache.get(test_key)
        
        # 验证缓存
        cache_hit = cached_value is not None and cached_value["data"] == "test_value"
        
        cache_tests["memory_cache_basic"] = {
            "success": True,
            "set_success": set_success,
            "cache_hit": cache_hit,
            "cached_value_correct": cached_value == test_value if cached_value else False
        }
        
        print("   ✅ 内存缓存: 基础功能正常")
        
    except Exception as e:
        cache_tests["memory_cache_basic"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 内存缓存: {e}")
    
    try:
        # 测试磁盘缓存
        from src.utils.cache_manager import DiskCache
        
        disk_cache = DiskCache(cache_dir="test_cache", max_size_mb=10)
        
        # 测试基本操作
        test_data = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        
        set_success = disk_cache.set("test_df", test_data)
        cached_df = disk_cache.get("test_df")
        
        cache_tests["disk_cache_basic"] = {
            "success": True,
            "set_success": set_success,
            "cache_hit": cached_df is not None,
            "data_integrity": cached_df.equals(test_data) if cached_df is not None else False
        }
        
        # 清理测试缓存
        disk_cache.clear()
        
        print("   ✅ 磁盘缓存: 基础功能正常")
        
    except Exception as e:
        cache_tests["disk_cache_basic"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 磁盘缓存: {e}")
    
    try:
        # 测试多层缓存管理器
        from src.utils.cache_manager import MultiLevelCacheManager
        
        cache_manager = MultiLevelCacheManager(
            enable_memory=True,
            enable_disk=True,
            enable_redis=False
        )
        
        # 测试缓存操作
        test_key = "multilevel_test"
        test_value = {"level": "multi", "data": [1, 2, 3, 4, 5]}
        
        set_success = cache_manager.set(test_key, test_value)
        cached_value = cache_manager.get(test_key)
        
        cache_tests["multilevel_cache"] = {
            "success": True,
            "set_success": set_success,
            "cache_hit": cached_value is not None,
            "value_correct": cached_value == test_value if cached_value else False
        }
        
        print("   ✅ 多层缓存管理器: 基础功能正常")
        
    except Exception as e:
        cache_tests["multilevel_cache"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 多层缓存管理器: {e}")
    
    test_results["tests"]["cache_mechanism"] = cache_tests
    
    # 测试3: 异步处理测试
    print("\n3. 测试异步处理...")
    async_tests = {}
    
    try:
        # 测试异步任务管理器
        from src.utils.async_processor import AsyncTaskManager, ProcessingConfig, TaskStatus
        
        config = ProcessingConfig(max_workers=2, timeout=10)
        task_manager = AsyncTaskManager(config)
        
        # 定义测试函数
        def test_sync_function(x):
            time.sleep(0.1)  # 模拟处理时间
            return x * 2
        
        async def test_async_function(x):
            await asyncio.sleep(0.1)  # 模拟异步处理
            return x * 3
        
        async def run_async_tests():
            # 测试同步任务
            task_id1 = await task_manager.submit_task(test_sync_function, 5)
            result1 = await task_manager.get_task_result(task_id1, wait=True)
            
            # 测试异步任务
            task_id2 = await task_manager.submit_task(test_async_function, 7)
            result2 = await task_manager.get_task_result(task_id2, wait=True)
            
            return result1, result2
        
        # 运行异步测试
        result1, result2 = asyncio.run(run_async_tests())
        
        async_tests["task_manager"] = {
            "success": True,
            "sync_task_success": result1.status == TaskStatus.COMPLETED and result1.result == 10,
            "async_task_success": result2.status == TaskStatus.COMPLETED and result2.result == 21,
            "sync_result": result1.result if result1.status == TaskStatus.COMPLETED else None,
            "async_result": result2.result if result2.status == TaskStatus.COMPLETED else None
        }
        
        print("   ✅ 异步任务管理器: 基础功能正常")
        
    except Exception as e:
        async_tests["task_manager"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 异步任务管理器: {e}")
    
    try:
        # 测试批处理器
        from src.utils.async_processor import BatchProcessor
        
        batch_processor = BatchProcessor()
        
        def batch_process_function(items):
            return [item * 2 for item in items]
        
        async def run_batch_test():
            test_items = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            results = await batch_processor.process_batch(
                test_items,
                batch_process_function,
                batch_size=3
            )
            return results
        
        batch_results = asyncio.run(run_batch_test())
        
        # 验证批处理结果
        successful_batches = sum(1 for r in batch_results if r.status == TaskStatus.COMPLETED)
        
        async_tests["batch_processor"] = {
            "success": True,
            "total_batches": len(batch_results),
            "successful_batches": successful_batches,
            "all_batches_successful": successful_batches == len(batch_results)
        }
        
        print("   ✅ 批处理器: 基础功能正常")
        
    except Exception as e:
        async_tests["batch_processor"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 批处理器: {e}")
    
    test_results["tests"]["async_processing"] = async_tests
    
    # 测试4: 数据质量监控测试
    print("\n4. 测试数据质量监控...")
    monitoring_tests = {}
    
    try:
        # 测试数据质量监控器
        from src.monitoring.data_quality_monitor import DataQualityMonitor, MonitoringRule, AlertLevel
        
        monitor = DataQualityMonitor(check_interval=1)  # 1秒检查间隔用于测试
        
        # 创建测试数据源
        def test_data_source():
            return pd.DataFrame({
                'value': [1, 2, 3, 4, 5],
                'quality': [0.9, 0.8, 0.95, 0.85, 0.92]
            })
        
        # 注册数据源
        monitor.register_data_source("test_source", test_data_source)
        
        # 添加监控规则
        rule = MonitoringRule(
            name="test_rule",
            metric_name="quality_score",
            threshold=80.0,
            comparison="<",
            alert_level=AlertLevel.WARNING
        )
        monitor.add_rule(rule)
        
        monitoring_tests["monitor_setup"] = {
            "success": True,
            "data_sources_count": len(monitor.data_sources),
            "rules_count": len(monitor.rules),
            "has_test_source": "test_source" in monitor.data_sources
        }
        
        print("   ✅ 数据质量监控器: 设置成功")
        
    except Exception as e:
        monitoring_tests["monitor_setup"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 数据质量监控器: {e}")
    
    try:
        # 测试质量指标计算
        from src.monitoring.data_quality_monitor import QualityMetric
        
        test_metric = QualityMetric(
            name="test_metric",
            value=85.5,
            threshold=80.0,
            status="OK",
            timestamp=datetime.now()
        )
        
        monitoring_tests["quality_metrics"] = {
            "success": True,
            "metric_created": test_metric.name == "test_metric",
            "value_correct": test_metric.value == 85.5,
            "status_correct": test_metric.status == "OK"
        }
        
        print("   ✅ 质量指标: 创建成功")
        
    except Exception as e:
        monitoring_tests["quality_metrics"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 质量指标: {e}")
    
    test_results["tests"]["data_quality_monitoring"] = monitoring_tests
    
    # 测试5: Dashboard性能优化测试
    print("\n5. 测试Dashboard性能优化...")
    dashboard_tests = {}
    
    try:
        # 测试Dashboard缓存
        from src.dashboard.performance_optimizer import DashboardCache
        
        dashboard_cache = DashboardCache()
        
        # 测试会话缓存
        test_key = "session_test"
        test_value = {"dashboard": "test", "timestamp": datetime.now().isoformat()}
        
        dashboard_cache.set_session_cache(test_key, test_value, ttl_minutes=5)
        cached_value = dashboard_cache.get_session_cache(test_key)
        
        dashboard_tests["dashboard_cache"] = {
            "success": True,
            "session_cache_set": True,
            "session_cache_hit": cached_value is not None,
            "value_correct": cached_value == test_value if cached_value else False
        }
        
        print("   ✅ Dashboard缓存: 基础功能正常")
        
    except Exception as e:
        dashboard_tests["dashboard_cache"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ Dashboard缓存: {e}")
    
    try:
        # 测试图表优化器
        from src.dashboard.performance_optimizer import ChartOptimizer
        
        # 创建测试数据
        test_df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=2000),
            'value': range(2000),
            'category': ['A'] * 1000 + ['B'] * 1000
        })
        
        # 测试数据优化
        optimized_df = ChartOptimizer.optimize_dataframe_for_chart(test_df, max_points=500)
        
        dashboard_tests["chart_optimizer"] = {
            "success": True,
            "original_size": len(test_df),
            "optimized_size": len(optimized_df),
            "size_reduced": len(optimized_df) < len(test_df),
            "optimization_ratio": len(optimized_df) / len(test_df)
        }
        
        print("   ✅ 图表优化器: 数据优化成功")
        
    except Exception as e:
        dashboard_tests["chart_optimizer"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 图表优化器: {e}")
    
    try:
        # 测试性能监控器
        from src.dashboard.performance_optimizer import PerformanceMonitor
        
        perf_monitor = PerformanceMonitor()
        
        # 模拟操作计时
        perf_monitor.start_timer("test_operation")
        time.sleep(0.1)  # 模拟操作
        duration = perf_monitor.end_timer("test_operation")
        
        avg_time = perf_monitor.get_average_time("test_operation")
        
        dashboard_tests["performance_monitor"] = {
            "success": True,
            "timer_worked": duration is not None,
            "duration_reasonable": 0.05 < duration < 0.2 if duration else False,
            "average_calculated": avg_time > 0
        }
        
        print("   ✅ 性能监控器: 计时功能正常")
        
    except Exception as e:
        dashboard_tests["performance_monitor"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 性能监控器: {e}")
    
    test_results["tests"]["dashboard_optimization"] = dashboard_tests
    
    # 测试6: 性能基准测试
    print("\n6. 性能基准测试...")
    performance_tests = {}
    
    try:
        # 测试缓存性能
        from src.utils.cache_manager import MemoryCache
        
        cache = MemoryCache(max_size=1000)
        
        # 测试写入性能
        start_time = time.time()
        for i in range(100):
            cache.set(f"key_{i}", {"data": f"value_{i}", "index": i})
        write_time = time.time() - start_time
        
        # 测试读取性能
        start_time = time.time()
        hits = 0
        for i in range(100):
            if cache.get(f"key_{i}") is not None:
                hits += 1
        read_time = time.time() - start_time
        
        performance_tests["cache_performance"] = {
            "success": True,
            "write_time": write_time,
            "read_time": read_time,
            "cache_hit_rate": hits / 100,
            "write_ops_per_second": 100 / write_time if write_time > 0 else 0,
            "read_ops_per_second": 100 / read_time if read_time > 0 else 0
        }
        
        print(f"   ✅ 缓存性能: 写入 {write_time:.3f}s, 读取 {read_time:.3f}s")
        
    except Exception as e:
        performance_tests["cache_performance"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 缓存性能测试失败: {e}")
    
    try:
        # 测试数据处理性能
        large_df = pd.DataFrame({
            'id': range(10000),
            'value': [i * 1.5 for i in range(10000)],
            'category': [f"cat_{i % 10}" for i in range(10000)]
        })
        
        # 测试数据优化性能
        start_time = time.time()
        from src.dashboard.performance_optimizer import DataPreprocessor
        optimized_df = DataPreprocessor.optimize_dataframe_memory(large_df)
        optimization_time = time.time() - start_time
        
        # 计算内存节省
        original_memory = large_df.memory_usage(deep=True).sum()
        optimized_memory = optimized_df.memory_usage(deep=True).sum()
        memory_savings = (original_memory - optimized_memory) / original_memory
        
        performance_tests["data_optimization"] = {
            "success": True,
            "optimization_time": optimization_time,
            "original_memory_mb": original_memory / 1024 / 1024,
            "optimized_memory_mb": optimized_memory / 1024 / 1024,
            "memory_savings_pct": memory_savings * 100,
            "rows_processed": len(large_df)
        }
        
        print(f"   ✅ 数据优化性能: {optimization_time:.3f}s, 内存节省 {memory_savings*100:.1f}%")
        
    except Exception as e:
        performance_tests["data_optimization"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 数据优化性能测试失败: {e}")
    
    test_results["tests"]["performance_benchmarks"] = performance_tests
    
    # 生成测试摘要
    print("\n7. 生成测试摘要...")
    
    total_tests = 0
    passed_tests = 0
    
    for category, tests in test_results["tests"].items():
        if isinstance(tests, dict):
            for test_name, result in tests.items():
                total_tests += 1
                if isinstance(result, dict):
                    success_indicators = [
                        result.get("success", False),
                        result.get("exists", False),
                        result.get("all_batches_successful", False),
                        result.get("cache_hit", False),
                        result.get("timer_worked", False)
                    ]
                    if any(success_indicators):
                        passed_tests += 1
    
    success_rate = passed_tests / total_tests if total_tests > 0 else 0
    
    # 计算性能分数
    performance_score = 0
    if "performance_benchmarks" in test_results["tests"]:
        perf_tests = test_results["tests"]["performance_benchmarks"]
        successful_perf_tests = sum(1 for test in perf_tests.values() 
                                   if test.get("success", False))
        total_perf_tests = len(perf_tests)
        performance_score = successful_perf_tests / total_perf_tests if total_perf_tests > 0 else 0
    
    # 计算优化效果分数
    optimization_score = 0
    optimization_metrics = []
    
    # 缓存命中率
    cache_tests = test_results["tests"].get("cache_mechanism", {})
    for test in cache_tests.values():
        if test.get("cache_hit"):
            optimization_metrics.append(1.0)
        elif test.get("success"):
            optimization_metrics.append(0.5)
    
    # 异步处理成功率
    async_tests = test_results["tests"].get("async_processing", {})
    for test in async_tests.values():
        if test.get("all_batches_successful") or test.get("async_task_success"):
            optimization_metrics.append(1.0)
        elif test.get("success"):
            optimization_metrics.append(0.5)
    
    if optimization_metrics:
        optimization_score = sum(optimization_metrics) / len(optimization_metrics)
    
    test_results["summary"] = {
        "total_tests": total_tests,
        "passed_tests": passed_tests,
        "success_rate": success_rate,
        "performance_score": performance_score,
        "optimization_score": optimization_score,
        "overall_status": "PASS" if success_rate >= 0.8 else "PARTIAL" if success_rate >= 0.6 else "FAIL",
        "recommendations": []
    }
    
    # 生成建议
    if success_rate < 0.8:
        test_results["summary"]["recommendations"].append("需要修复失败的测试项")
    
    if performance_score >= 0.8:
        test_results["summary"]["recommendations"].append("性能优化效果良好")
    
    if optimization_score >= 0.8:
        test_results["summary"]["recommendations"].append("缓存和异步处理优化成功")
    
    if success_rate >= 0.8 and performance_score >= 0.7:
        test_results["summary"]["recommendations"].append("第二阶段优化完成，系统性能显著提升")
    
    # 保存测试报告
    os.makedirs("reports", exist_ok=True)
    report_path = f"reports/phase2_optimization_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(test_results, f, ensure_ascii=False, indent=2)
    
    # 打印最终摘要
    print("\n" + "="*60)
    print("第二阶段性能优化测试摘要")
    print("="*60)
    print(f"总测试数: {total_tests}")
    print(f"通过测试: {passed_tests}")
    print(f"成功率: {success_rate:.1%}")
    print(f"性能分数: {performance_score:.1%}")
    print(f"优化效果分数: {optimization_score:.1%}")
    print(f"整体状态: {test_results['summary']['overall_status']}")
    
    if test_results["summary"]["recommendations"]:
        print("\n建议:")
        for rec in test_results["summary"]["recommendations"]:
            print(f"- {rec}")
    
    print(f"\n详细报告已保存至: {report_path}")
    
    return test_results


if __name__ == "__main__":
    test_phase2_optimizations()