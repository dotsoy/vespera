"""
量化投资分析平台 v2.0 - 重新设计的主仪表盘
"""
import streamlit as st
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 导入新的组件
from dashboard.components.system_status import render_system_status_main
from dashboard.components.data_explorer import render_data_explorer_main
from dashboard.components.strategy_analysis import render_strategy_analysis_main
from dashboard.components.backtest_visualization import render_backtest_visualization_main

# 尝试导入Marimo组件
try:
    from dashboard.components.marimo_lab import render_marimo_lab
    MARIMO_AVAILABLE = True
except ImportError:
    MARIMO_AVAILABLE = False

def main():
    """主函数"""
    st.set_page_config(
        page_title="量化投资分析平台 v2.0",
        page_icon="🚀",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # 主标题
    st.title("🚀 量化投资分析平台 v2.0")
    st.markdown("**基于启明星策略的智能量化投资决策系统**")
    st.markdown("---")
    
    # 侧边栏导航
    st.sidebar.title("🎯 功能导航")
    st.sidebar.markdown("---")
    
    # 主要功能页面
    pages = {
        "🖥️ 系统状态": {
            "key": "system_status",
            "description": "监控系统运行状态、数据库连接、可用策略等",
            "icon": "🖥️"
        },
        "🔍 数据探索": {
            "key": "data_explorer",
            "description": "多维度数据分析和可视化",
            "icon": "🔍"
        },
        "🎯 策略分析": {
            "key": "strategy_analysis",
            "description": "执行策略分析、生成交易信号、查看分析报告",
            "icon": "🎯"
        },
        "📈 回测可视化": {
            "key": "backtest_visualization",
            "description": "展示回测结果、买卖点、权益曲线等",
            "icon": "📈"
        }
    }
    
    # 添加Marimo研究室（如果可用）
    if MARIMO_AVAILABLE:
        pages["🔬 Marimo 研究室"] = {
            "key": "marimo_lab",
            "description": "交互式量化研究环境",
            "icon": "🔬"
        }
    
    # 页面选择
    st.sidebar.markdown("### 📋 功能模块")
    
    # 创建页面选择按钮
    selected_page_key = None
    for page_name, page_info in pages.items():
        if st.sidebar.button(
            f"{page_info['icon']} {page_name.split(' ', 1)[1]}",  # 移除emoji前缀
            key=f"nav_{page_info['key']}",
            help=page_info['description'],
            use_container_width=True
        ):
            selected_page_key = page_info['key']
            st.session_state['current_page'] = selected_page_key
    
    # 如果没有选择页面，使用session state中的页面或默认页面
    if selected_page_key is None:
        selected_page_key = st.session_state.get('current_page', 'system_status')
    
    # 显示当前页面信息
    current_page_info = None
    for page_name, page_info in pages.items():
        if page_info['key'] == selected_page_key:
            current_page_info = page_info
            current_page_name = page_name
            break
    
    if current_page_info:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📍 当前页面")
        st.sidebar.info(f"**{current_page_name}**\n\n{current_page_info['description']}")
    
    # 系统状态快速预览
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ⚡ 快速状态")
    
    # 这里可以添加一些快速状态指标
    try:
        from src.utils.database import get_db_manager
        db_manager = get_db_manager()
        connection_status = db_manager.test_connections()
        
        postgres_status = "🟢" if connection_status.get('postgres', False) else "🔴"
        clickhouse_status = "🟢" if connection_status.get('clickhouse', False) else "🔴"
        
        st.sidebar.markdown(f"**数据库状态**")
        st.sidebar.markdown(f"PostgreSQL: {postgres_status}")
        st.sidebar.markdown(f"ClickHouse: {clickhouse_status}")
        
    except Exception:
        st.sidebar.markdown("**数据库状态**: ⚠️ 检查中...")
    
    # 添加版本信息
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ 版本信息")
    st.sidebar.markdown("**平台版本**: v2.0.0")
    st.sidebar.markdown("**启明星策略**: v1.0.0")
    st.sidebar.markdown("**更新时间**: 2025-06-16")
    
    # 渲染选中的页面
    st.markdown("---")
    
    if selected_page_key == "system_status":
        render_system_status_main()
    elif selected_page_key == "data_explorer":
        render_data_explorer_main()
    elif selected_page_key == "strategy_analysis":
        render_strategy_analysis_main()
    elif selected_page_key == "backtest_visualization":
        render_backtest_visualization_main()
    elif selected_page_key == "marimo_lab" and MARIMO_AVAILABLE:
        render_marimo_lab()
    else:
        st.error(f"页面 '{selected_page_key}' 未找到")
        st.info("请从侧边栏选择一个有效的功能模块")
    
    # 页脚
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray; font-size: 12px;'>
        🚀 量化投资分析平台 v2.0 | 基于启明星策略 | 
        <a href='#' style='color: gray;'>使用文档</a> | 
        <a href='#' style='color: gray;'>技术支持</a>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
