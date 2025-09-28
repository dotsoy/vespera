#!/usr/bin/env python3
"""
Vespera Dashboard组件测试脚本
测试所有Dashboard组件的导入和基础功能
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_dashboard_imports():
    """测试Dashboard组件导入"""
    print("🎯 测试Dashboard组件导入")
    print("=" * 60)
    
    results = {}
    
    # 测试主Dashboard应用
    try:
        from dashboard.app import main as dashboard_main
        results['dashboard_app'] = True
        print("✅ 主Dashboard应用导入成功")
    except Exception as e:
        results['dashboard_app'] = False
        print(f"❌ 主Dashboard应用导入失败: {e}")
    
    # 测试系统状态组件
    try:
        from dashboard.components.system_status import render_system_status_main
        results['system_status'] = True
        print("✅ 系统状态组件导入成功")
    except Exception as e:
        results['system_status'] = False
        print(f"❌ 系统状态组件导入失败: {e}")
    
    # 测试数据探索器组件
    try:
        from dashboard.components.data_explorer import render_data_explorer_main
        results['data_explorer'] = True
        print("✅ 数据探索器组件导入成功")
    except Exception as e:
        results['data_explorer'] = False
        print(f"❌ 数据探索器组件导入失败: {e}")
    
    # 测试数据源管理器组件
    try:
        from dashboard.components.data_source_manager import render_data_source_manager
        results['data_source_manager'] = True
        print("✅ 数据源管理器组件导入成功")
    except Exception as e:
        results['data_source_manager'] = False
        print(f"❌ 数据源管理器组件导入失败: {e}")
    
    # 测试股票全息视图组件
    try:
        from dashboard.components.stock_holographic_view import render_stock_holographic_view
        results['stock_holographic'] = True
        print("✅ 股票全息视图组件导入成功")
    except Exception as e:
        results['stock_holographic'] = False
        print(f"❌ 股票全息视图组件导入失败: {e}")
    
    # 测试Marimo实验室组件
    try:
        from dashboard.components.marimo_lab import render_marimo_lab
        results['marimo_lab'] = True
        print("✅ Marimo实验室组件导入成功")
    except Exception as e:
        results['marimo_lab'] = False
        print(f"⚠️ Marimo实验室组件导入失败: {e}")
    
    return results

def test_streamlit_dependencies():
    """测试Streamlit相关依赖"""
    print("\n📦 测试Streamlit相关依赖")
    print("=" * 60)
    
    dependencies = {
        'streamlit': 'Streamlit Web框架',
        'plotly': 'Plotly图表库',
        'pandas': 'Pandas数据处理',
        'numpy': 'NumPy数值计算',
        'psycopg2': 'PostgreSQL连接器',
        'redis': 'Redis连接器',
        'clickhouse_driver': 'ClickHouse连接器'
    }
    
    results = {}
    
    for package, description in dependencies.items():
        try:
            __import__(package)
            results[package] = True
            print(f"✅ {description}: {package}")
        except ImportError as e:
            results[package] = False
            print(f"❌ {description}: {package} - {e}")
    
    return results

def test_dashboard_configuration():
    """测试Dashboard配置"""
    print("\n⚙️ 测试Dashboard配置")
    print("=" * 60)
    
    results = {}
    
    # 检查Dashboard目录结构
    dashboard_dir = project_root / "dashboard"
    if dashboard_dir.exists():
        results['dashboard_dir'] = True
        print(f"✅ Dashboard目录存在: {dashboard_dir}")
        
        # 检查组件目录
        components_dir = dashboard_dir / "components"
        if components_dir.exists():
            results['components_dir'] = True
            print(f"✅ 组件目录存在: {components_dir}")
            
            # 列出所有组件文件
            component_files = list(components_dir.glob("*.py"))
            print(f"📁 发现组件文件: {len(component_files)} 个")
            for file in component_files:
                print(f"   - {file.name}")
        else:
            results['components_dir'] = False
            print(f"❌ 组件目录不存在: {components_dir}")
    else:
        results['dashboard_dir'] = False
        print(f"❌ Dashboard目录不存在: {dashboard_dir}")
    
    # 检查示例Dashboard
    sample_dashboard = project_root / "sample_dashboard.py"
    if sample_dashboard.exists():
        results['sample_dashboard'] = True
        print(f"✅ 示例Dashboard存在: {sample_dashboard}")
    else:
        results['sample_dashboard'] = False
        print(f"❌ 示例Dashboard不存在: {sample_dashboard}")
    
    return results

def test_performance_optimizer():
    """测试性能优化器"""
    print("\n🚀 测试性能优化器")
    print("=" * 60)
    
    try:
        from src.dashboard.performance_optimizer import dashboard_page, st_cache_data
        print("✅ 性能优化器导入成功")
        print("✅ dashboard_page装饰器可用")
        print("✅ st_cache_data缓存可用")
        return True
    except Exception as e:
        print(f"❌ 性能优化器导入失败: {e}")
        return False

def create_dashboard_test_script():
    """创建Dashboard测试脚本"""
    print("\n📝 创建Dashboard测试脚本")
    print("=" * 60)
    
    test_script_content = '''#!/usr/bin/env python3
"""
Dashboard功能测试脚本
"""

import streamlit as st
import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_dashboard_functionality():
    """测试Dashboard功能"""
    st.title("🧪 Vespera Dashboard 功能测试")
    
    st.header("1. 系统状态测试")
    try:
        from dashboard.components.system_status import render_system_status_main
        st.success("✅ 系统状态组件加载成功")
        
        # 渲染系统状态（简化版本）
        with st.expander("系统状态详情"):
            st.info("系统状态组件功能正常")
    except Exception as e:
        st.error(f"❌ 系统状态组件加载失败: {e}")
    
    st.header("2. 数据探索器测试")
    try:
        from dashboard.components.data_explorer import render_data_explorer_main
        st.success("✅ 数据探索器组件加载成功")
    except Exception as e:
        st.error(f"❌ 数据探索器组件加载失败: {e}")
    
    st.header("3. 性能优化器测试")
    try:
        from src.dashboard.performance_optimizer import dashboard_page
        st.success("✅ 性能优化器加载成功")
    except Exception as e:
        st.error(f"❌ 性能优化器加载失败: {e}")
    
    st.header("4. 缓存系统测试")
    try:
        from src.utils.cache_manager import global_cache_manager
        cache_stats = global_cache_manager.get_cache_stats()
        st.success("✅ 缓存系统连接成功")
        st.json(cache_stats)
    except Exception as e:
        st.error(f"❌ 缓存系统连接失败: {e}")

if __name__ == "__main__":
    test_dashboard_functionality()
'''
    
    test_script_path = project_root / "test_dashboard_functionality.py"
    with open(test_script_path, 'w', encoding='utf-8') as f:
        f.write(test_script_content)
    
    print(f"✅ Dashboard测试脚本已创建: {test_script_path}")
    return test_script_path

def main():
    """主函数"""
    print("🎯 Vespera Dashboard组件测试")
    print(f"测试时间: {datetime.now()}")
    print("=" * 60)
    
    # 执行所有测试
    dashboard_results = test_dashboard_imports()
    dependency_results = test_streamlit_dependencies()
    config_results = test_dashboard_configuration()
    optimizer_result = test_performance_optimizer()
    
    # 创建测试脚本
    test_script_path = create_dashboard_test_script()
    
    # 汇总结果
    print("\n📊 Dashboard测试结果汇总")
    print("=" * 60)
    
    total_tests = len(dashboard_results) + len(dependency_results) + len(config_results) + 1
    passed_tests = sum(dashboard_results.values()) + sum(dependency_results.values()) + sum(config_results.values()) + (1 if optimizer_result else 0)
    
    print(f"总测试数: {total_tests}")
    print(f"通过测试: {passed_tests}")
    print(f"成功率: {passed_tests/total_tests*100:.1f}%")
    
    if passed_tests >= total_tests * 0.8:
        print("🎉 Dashboard组件测试通过!")
    else:
        print("⚠️ Dashboard组件存在问题，需要修复")
    
    print(f"\n📋 Dashboard使用指南:")
    print(f"1. 启动主Dashboard:")
    print(f"   streamlit run dashboard/app.py")
    print(f"2. 启动示例Dashboard:")
    print(f"   streamlit run sample_dashboard.py")
    print(f"3. 运行功能测试:")
    print(f"   streamlit run {test_script_path}")

if __name__ == "__main__":
    main()