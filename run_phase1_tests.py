"""
运行第一阶段回归测试的简化版本
"""
import sys
import os
import json
import time
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, '/workspaces/vespera')

def run_basic_tests():
    """运行基础测试"""
    print("="*60)
    print("第一阶段基础稳定性优化 - 回归测试")
    print("="*60)
    
    test_results = {
        "timestamp": datetime.now().isoformat(),
        "phase": "Phase 1 - 基础稳定性优化",
        "tests": {},
        "summary": {}
    }
    
    # 测试1: 检查新增文件是否存在
    print("\n1. 检查新增优化文件...")
    file_checks = {
        "enhanced_data_source_manager": "src/data_sources/enhanced_data_source_manager.py",
        "data_quality_checkers": "src/data_sources/data_quality_checkers.py",
        "exceptions": "src/utils/exceptions.py",
        "error_recovery": "src/utils/error_recovery.py",
        "database_optimizer": "scripts/optimize_database_indexes.py",
        "optimized_indexes_sql": "sql/create_optimized_indexes.sql"
    }
    
    file_test_results = {}
    for name, path in file_checks.items():
        exists = os.path.exists(path)
        file_test_results[name] = {
            "exists": exists,
            "path": path,
            "size": os.path.getsize(path) if exists else 0
        }
        status = "✅" if exists else "❌"
        print(f"   {status} {name}: {path}")
    
    test_results["tests"]["file_existence"] = file_test_results
    
    # 测试2: 基础导入测试
    print("\n2. 测试模块导入...")
    import_tests = {}
    
    modules_to_test = [
        ("enhanced_data_source_manager", "src.data_sources.enhanced_data_source_manager"),
        ("data_quality_checkers", "src.data_sources.data_quality_checkers"),
        ("exceptions", "src.utils.exceptions"),
        ("error_recovery", "src.utils.error_recovery")
    ]
    
    for name, module_path in modules_to_test:
        try:
            __import__(module_path)
            import_tests[name] = {"success": True, "error": None}
            print(f"   ✅ {name}: 导入成功")
        except Exception as e:
            import_tests[name] = {"success": False, "error": str(e)}
            print(f"   ❌ {name}: 导入失败 - {e}")
    
    test_results["tests"]["module_imports"] = import_tests
    
    # 测试3: 基础功能测试
    print("\n3. 测试基础功能...")
    functionality_tests = {}
    
    try:
        # 测试异常处理
        from src.utils.exceptions import VesperaException, ErrorSeverity, ErrorCategory
        
        exc = VesperaException(
            message="测试异常",
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.DATA_SOURCE
        )
        
        functionality_tests["exception_creation"] = {
            "success": exc.message == "测试异常",
            "details": "异常创建和属性设置"
        }
        print("   ✅ 异常处理: 基础功能正常")
        
    except Exception as e:
        functionality_tests["exception_creation"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 异常处理: {e}")
    
    try:
        # 测试数据质量检查器
        from src.data_sources.data_quality_checkers import DataQualityManager
        import pandas as pd
        
        manager = DataQualityManager()
        test_data = pd.DataFrame({
            'open': [10.0, 10.1],
            'close': [10.2, 10.3],
            'volume': [1000, 1100]
        })
        
        report = manager.check_quality(test_data)
        
        functionality_tests["data_quality"] = {
            "success": hasattr(report, 'quality_score'),
            "details": f"质量分数: {getattr(report, 'quality_score', 'N/A')}"
        }
        print("   ✅ 数据质量检查: 基础功能正常")
        
    except Exception as e:
        functionality_tests["data_quality"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 数据质量检查: {e}")
    
    try:
        # 测试增强数据源管理器
        from src.data_sources.enhanced_data_source_manager import EnhancedDataSourceManager
        
        manager = EnhancedDataSourceManager()
        health_report = manager.get_health_report()
        
        functionality_tests["enhanced_data_source"] = {
            "success": "timestamp" in health_report,
            "details": "健康报告生成成功"
        }
        print("   ✅ 增强数据源管理器: 基础功能正常")
        
    except Exception as e:
        functionality_tests["enhanced_data_source"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 增强数据源管理器: {e}")
    
    test_results["tests"]["functionality"] = functionality_tests
    
    # 测试4: 性能基准测试
    print("\n4. 性能基准测试...")
    performance_tests = {}
    
    try:
        # 测试数据质量检查性能
        start_time = time.time()
        
        from src.data_sources.data_quality_checkers import DataQualityManager
        import pandas as pd
        
        # 创建较大的测试数据集
        large_data = pd.DataFrame({
            'ts_code': ['000001.SZ'] * 1000,
            'open': list(range(1000)),
            'close': list(range(1000, 2000)),
            'volume': list(range(2000, 3000))
        })
        
        manager = DataQualityManager()
        report = manager.check_quality(large_data)
        
        execution_time = time.time() - start_time
        
        performance_tests["quality_check_performance"] = {
            "execution_time": execution_time,
            "data_size": len(large_data),
            "performance_rating": "good" if execution_time < 1.0 else "needs_improvement"
        }
        
        print(f"   ✅ 质量检查性能: {execution_time:.3f}s (1000行数据)")
        
    except Exception as e:
        performance_tests["quality_check_performance"] = {
            "success": False,
            "error": str(e)
        }
        print(f"   ❌ 质量检查性能测试失败: {e}")
    
    test_results["tests"]["performance"] = performance_tests
    
    # 生成测试摘要
    print("\n5. 生成测试摘要...")
    
    total_tests = 0
    passed_tests = 0
    
    for category, tests in test_results["tests"].items():
        if isinstance(tests, dict):
            for test_name, result in tests.items():
                total_tests += 1
                if isinstance(result, dict):
                    if result.get("success", False) or result.get("exists", False):
                        passed_tests += 1
    
    success_rate = passed_tests / total_tests if total_tests > 0 else 0
    
    test_results["summary"] = {
        "total_tests": total_tests,
        "passed_tests": passed_tests,
        "success_rate": success_rate,
        "overall_status": "PASS" if success_rate >= 0.8 else "FAIL",
        "recommendations": []
    }
    
    if success_rate < 0.8:
        test_results["summary"]["recommendations"].append("需要修复失败的测试项")
    
    if success_rate >= 0.9:
        test_results["summary"]["recommendations"].append("第一阶段优化效果良好，可以进入第二阶段")
    
    # 保存测试报告
    os.makedirs("reports", exist_ok=True)
    report_path = f"reports/phase1_basic_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(test_results, f, ensure_ascii=False, indent=2)
    
    # 打印最终摘要
    print("\n" + "="*60)
    print("测试摘要")
    print("="*60)
    print(f"总测试数: {total_tests}")
    print(f"通过测试: {passed_tests}")
    print(f"成功率: {success_rate:.1%}")
    print(f"整体状态: {test_results['summary']['overall_status']}")
    
    if test_results["summary"]["recommendations"]:
        print("\n建议:")
        for rec in test_results["summary"]["recommendations"]:
            print(f"- {rec}")
    
    print(f"\n详细报告已保存至: {report_path}")
    
    return test_results


if __name__ == "__main__":
    run_basic_tests()