#!/usr/bin/env python3
"""
测试运行脚本
运行所有测试并生成报告
"""
import os
import sys
import subprocess
import argparse
import time
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_command(command, description):
    """运行命令并显示结果"""
    print(f"\n{'='*60}")
    print(f"🔄 {description}")
    print(f"{'='*60}")
    print(f"执行命令: {command}")
    print("-" * 60)
    
    start_time = time.time()
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            cwd=project_root
        )
        end_time = time.time()
        
        print(f"返回码: {result.returncode}")
        print(f"执行时间: {end_time - start_time:.2f}秒")
        
        if result.stdout:
            print("标准输出:")
            print(result.stdout)
        
        if result.stderr:
            print("错误输出:")
            print(result.stderr)
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"执行命令时出错: {e}")
        return False


def create_test_report(test_results, output_file="test_report.md"):
    """创建测试报告"""
    report_content = f"""# 启明星量化投资分析平台测试报告

## 测试概览

**测试时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**测试环境**: Python {sys.version}
**项目路径**: {project_root}

## 测试结果汇总

"""
    
    for test_name, result in test_results.items():
        status = "✅ 通过" if result else "❌ 失败"
        report_content += f"- **{test_name}**: {status}\n"
    
    report_content += """

## 测试详情

### 1. 基础功能测试
测试项目的基本功能，包括模块导入、配置加载等。

### 2. 技术分析器测试
测试技术分析模块的所有功能，包括：
- 技术指标计算
- 趋势评分计算
- 动量评分计算
- 量能健康度计算
- 形态识别
- 支撑阻力位计算

### 3. 资金流分析器测试
测试资金流分析模块的所有功能，包括：
- 主力资金评分
- 散户情绪评分
- 机构活跃度计算
- 资金流一致性
- 量价相关性

### 4. 数据源测试
测试各种数据源的功能，包括：
- AkShare数据源
- Tushare数据源
- 数据源工厂
- 数据源管理器
- 数据兼容性层

### 5. 策略模块测试
测试策略相关的所有功能，包括：
- 启明星策略
- 回测引擎
- 四维分析器
- 信号融合引擎

### 6. Dashboard组件测试
测试Dashboard的所有组件，包括：
- 系统状态组件
- 数据管理组件
- 策略分析组件
- 回测可视化组件
- 数据源管理组件

### 7. 错误修复验证测试
验证之前修复的错误是否已经解决：
- 技术指标计算错误
- 字段缺失错误
- 数据库插入错误
- 数据序列化错误

### 8. 集成测试
测试整个系统的端到端功能：
- 数据流集成
- Dashboard集成
- 性能集成
- 数据质量集成
- 配置集成
- 错误恢复集成

## 测试覆盖率

运行 `pytest --cov=src --cov-report=html` 查看详细的覆盖率报告。

## 性能测试结果

运行 `pytest -m performance` 查看性能测试结果。

## 建议

1. **定期运行测试**: 建议在每次代码修改后运行测试
2. **关注覆盖率**: 保持测试覆盖率在80%以上
3. **性能监控**: 定期运行性能测试，确保系统性能
4. **错误处理**: 重点关注错误处理测试的结果

## 联系方式

如有问题，请联系开发团队。
"""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"\n📄 测试报告已生成: {output_file}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='运行启明星项目测试')
    parser.add_argument(
        '--test-type',
        choices=['all', 'basic', 'technical', 'capital-flow', 'data-sources', 
                'strategies', 'dashboard', 'error-fixes', 'integration', 'performance'],
        default='all',
        help='测试类型'
    )
    parser.add_argument(
        '--coverage',
        action='store_true',
        help='生成覆盖率报告'
    )
    parser.add_argument(
        '--html-report',
        action='store_true',
        help='生成HTML测试报告'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='详细输出'
    )
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='并行运行测试'
    )
    
    args = parser.parse_args()
    
    print("🚀 启明星量化投资分析平台测试套件")
    print("=" * 60)
    print(f"项目路径: {project_root}")
    print(f"Python版本: {sys.version}")
    print(f"测试类型: {args.test_type}")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 检查测试环境
    print("\n🔍 检查测试环境...")
    if not (project_root / "tests").exists():
        print("❌ 错误: 未找到tests目录")
        return 1
    
    if not (project_root / "src").exists():
        print("❌ 错误: 未找到src目录")
        return 1
    
    print("✅ 测试环境检查通过")
    
    # 构建pytest命令
    pytest_args = ["pytest"]
    
    if args.verbose:
        pytest_args.append("-v")
    
    if args.parallel:
        pytest_args.extend(["-n", "auto"])
    
    if args.coverage:
        pytest_args.extend([
            "--cov=src",
            "--cov-report=term-missing",
            "--cov-report=html:htmlcov"
        ])
    
    if args.html_report:
        pytest_args.extend([
            "--html=reports/test_report.html",
            "--self-contained-html"
        ])
    
    # 根据测试类型选择测试文件
    test_results = {}
    
    if args.test_type in ['all', 'basic']:
        print("\n📋 运行基础功能测试...")
        basic_cmd = " ".join(pytest_args + ["tests/test_basic.py"])
        test_results['基础功能测试'] = run_command(basic_cmd, "基础功能测试")
    
    if args.test_type in ['all', 'technical']:
        print("\n📊 运行技术分析器测试...")
        technical_cmd = " ".join(pytest_args + ["tests/test_technical_analyzer_comprehensive.py"])
        test_results['技术分析器测试'] = run_command(technical_cmd, "技术分析器测试")
    
    if args.test_type in ['all', 'capital-flow']:
        print("\n💰 运行资金流分析器测试...")
        capital_flow_cmd = " ".join(pytest_args + ["tests/test_capital_flow_analyzer_comprehensive.py"])
        test_results['资金流分析器测试'] = run_command(capital_flow_cmd, "资金流分析器测试")
    
    if args.test_type in ['all', 'data-sources']:
        print("\n🧪 开始运行数据源测试...")
        print("- AkShare数据源")
        print("- Tushare数据源")
        print("- 数据源工厂")
        print("- 数据源管理器")
        print("- 数据兼容性层")
        data_sources_cmd = " ".join(pytest_args + ["tests/test_data_sources_comprehensive.py"])
        test_results['数据源测试'] = run_command(data_sources_cmd, "数据源测试")
    
    if args.test_type in ['all', 'strategies']:
        print("\n🎯 运行策略模块测试...")
        strategies_cmd = " ".join(pytest_args + ["tests/test_strategies_comprehensive.py"])
        test_results['策略模块测试'] = run_command(strategies_cmd, "策略模块测试")
    
    if args.test_type in ['all', 'dashboard']:
        print("\n🖥️ 运行Dashboard组件测试...")
        dashboard_cmd = " ".join(pytest_args + ["tests/test_dashboard_comprehensive.py"])
        test_results['Dashboard组件测试'] = run_command(dashboard_cmd, "Dashboard组件测试")
    
    if args.test_type in ['all', 'error-fixes']:
        print("\n🔧 运行错误修复验证测试...")
        error_fixes_cmd = " ".join(pytest_args + ["tests/test_error_fixes.py"])
        test_results['错误修复验证测试'] = run_command(error_fixes_cmd, "错误修复验证测试")
    
    if args.test_type in ['all', 'integration']:
        print("\n🔗 运行集成测试...")
        integration_cmd = " ".join(pytest_args + ["tests/test_integration_comprehensive.py"])
        test_results['集成测试'] = run_command(integration_cmd, "集成测试")
    
    if args.test_type in ['all', 'performance']:
        print("\n⚡ 运行性能测试...")
        performance_cmd = " ".join(pytest_args + ["-m", "performance"])
        test_results['性能测试'] = run_command(performance_cmd, "性能测试")
    
    # 运行所有测试（如果选择了all）
    if args.test_type == 'all':
        print("\n🎯 运行所有测试...")
        all_tests_cmd = " ".join(pytest_args + ["tests/"])
        test_results['所有测试'] = run_command(all_tests_cmd, "所有测试")
    
    # 生成测试报告
    print("\n📄 生成测试报告...")
    create_test_report(test_results)
    
    # 显示测试结果摘要
    print("\n" + "="*60)
    print("📊 测试结果摘要")
    print("="*60)
    
    passed = sum(1 for result in test_results.values() if result)
    total = len(test_results)
    
    for test_name, result in test_results.items():
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{test_name}: {status}")
    
    print(f"\n总计: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有测试通过！")
        return 0
    else:
        print("⚠️ 部分测试失败，请检查详细输出")
        return 1


def run_specific_test():
    """运行特定测试的辅助函数"""
    print("\n🔧 运行特定测试...")
    
    # 可以在这里添加特定测试的逻辑
    specific_tests = [
        "tests/test_basic.py::test_imports",
        "tests/test_technical_analyzer_comprehensive.py::TestTechnicalAnalyzerInitialization::test_analyzer_initialization",
        "tests/test_capital_flow_analyzer_comprehensive.py::TestCapitalFlowAnalyzerInitialization::test_analyzer_initialization"
    ]
    
    for test in specific_tests:
        cmd = f"pytest {test} -v"
        print(f"\n运行测试: {test}")
        run_command(cmd, f"特定测试: {test}")


def run_coverage_analysis():
    """运行覆盖率分析"""
    print("\n📊 运行覆盖率分析...")
    
    # 生成覆盖率报告
    coverage_cmd = "pytest --cov=src --cov-report=term-missing --cov-report=html:htmlcov --cov-report=xml:coverage.xml"
    success = run_command(coverage_cmd, "覆盖率分析")
    
    if success:
        print("\n📈 覆盖率报告已生成:")
        print("- HTML报告: htmlcov/index.html")
        print("- XML报告: coverage.xml")
        print("- 终端报告: 见上方输出")
    
    return success


def run_performance_benchmark():
    """运行性能基准测试"""
    print("\n⚡ 运行性能基准测试...")
    
    # 运行性能测试
    perf_cmd = "pytest -m performance -v"
    success = run_command(perf_cmd, "性能基准测试")
    
    if success:
        print("\n📊 性能测试完成")
    
    return success


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⏹️ 测试被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ 测试运行出错: {e}")
        sys.exit(1) 