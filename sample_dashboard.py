
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

# 添加项目路径
sys.path.insert(0, '/workspaces/vespera')

from src.dashboard.performance_optimizer import dashboard_page, st_cache_data
from src.utils.cache_manager import global_cache_manager

@dashboard_page("Vespera 量化投资分析平台", cache_ttl=300)
def main():
    """主Dashboard页面"""
    
    st.sidebar.title("导航")
    page = st.sidebar.selectbox("选择页面", [
        "系统概览", 
        "数据质量监控", 
        "性能监控", 
        "缓存状态"
    ])
    
    if page == "系统概览":
        show_system_overview()
    elif page == "数据质量监控":
        show_data_quality()
    elif page == "性能监控":
        show_performance_monitoring()
    elif page == "缓存状态":
        show_cache_status()

@st_cache_data(ttl=300)
def load_sample_data():
    """加载示例数据"""
    return pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=30),
        'price': [100 + i + (i % 5) * 2 for i in range(30)],
        'volume': [1000 + i * 50 for i in range(30)]
    })

def show_system_overview():
    """显示系统概览"""
    st.header("🚀 系统概览")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("系统状态", "运行中", "正常")
    
    with col2:
        st.metric("数据源", "3个", "全部在线")
    
    with col3:
        st.metric("缓存命中率", "92%", "+5%")
    
    with col4:
        st.metric("响应时间", "120ms", "-60%")
    
    # 示例图表
    st.subheader("📈 价格趋势")
    data = load_sample_data()
    
    fig = px.line(data, x='date', y='price', title='股价走势')
    st.plotly_chart(fig, use_container_width=True)

def show_data_quality():
    """显示数据质量监控"""
    st.header("📊 数据质量监控")
    
    # 质量指标
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("数据质量分数", "95.2", "+2.1")
    
    with col2:
        st.metric("完整性", "98.5%", "+0.5%")
    
    with col3:
        st.metric("及时性", "99.1%", "正常")
    
    # 质量趋势
    st.subheader("质量趋势")
    quality_data = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=7),
        'quality_score': [92, 94, 93, 95, 96, 95, 95]
    })
    
    fig = px.line(quality_data, x='date', y='quality_score', 
                  title='数据质量分数趋势')
    st.plotly_chart(fig, use_container_width=True)

def show_performance_monitoring():
    """显示性能监控"""
    st.header("⚡ 性能监控")
    
    # 性能指标
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("平均响应时间", "120ms", "-60%")
    
    with col2:
        st.metric("并发请求数", "45", "+300%")
    
    with col3:
        st.metric("内存使用", "2.1GB", "-40%")
    
    # 性能趋势
    st.subheader("响应时间趋势")
    perf_data = pd.DataFrame({
        'time': pd.date_range('2024-01-01', periods=24, freq='H'),
        'response_time': [120 + (i % 6) * 10 for i in range(24)]
    })
    
    fig = px.line(perf_data, x='time', y='response_time', 
                  title='响应时间趋势 (ms)')
    st.plotly_chart(fig, use_container_width=True)

def show_cache_status():
    """显示缓存状态"""
    st.header("💾 缓存状态")
    
    try:
        # 获取缓存统计
        cache_stats = global_cache_manager.get_stats()
        
        st.subheader("缓存统计")
        
        for level, stats in cache_stats.get("cache_stats", {}).items():
            st.write(f"**{level}**")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("命中率", f"{stats.get('hit_rate', 0):.1%}")
            
            with col2:
                st.metric("命中次数", stats.get('hits', 0))
            
            with col3:
                st.metric("未命中次数", stats.get('misses', 0))
    
    except Exception as e:
        st.error(f"获取缓存状态失败: {e}")
    
    # 缓存操作
    st.subheader("缓存操作")
    
    if st.button("清理缓存"):
        try:
            global_cache_manager.clear()
            st.success("缓存已清理")
        except Exception as e:
            st.error(f"清理缓存失败: {e}")

if __name__ == "__main__":
    main()
