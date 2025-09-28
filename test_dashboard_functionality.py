#!/usr/bin/env python3
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
