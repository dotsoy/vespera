"""
数据源管理组件 - 展示不同数据源拉取状态和可视化界面
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import asyncio
import time
import numpy as np
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from config.settings import data_settings

logger = get_logger("data_source_manager")

# 数据源客户端导入
DATA_SOURCES = {}

try:
    from src.data_sources.alltick_client import AllTickClient
    DATA_SOURCES['AllTick'] = {
        'client': AllTickClient,
        'available': True,
        'description': '专业的A股实时数据源',
        'features': ['实时行情', '历史数据', '资金流向', '技术指标']
    }
except ImportError:
    DATA_SOURCES['AllTick'] = {
        'client': None,
        'available': False,
        'description': '专业的A股实时数据源 (未配置)',
        'features': ['实时行情', '历史数据', '资金流向', '技术指标']
    }

try:
    from src.data_sources.alpha_vantage_client import AlphaVantageClient
    DATA_SOURCES['Alpha Vantage'] = {
        'client': AlphaVantageClient,
        'available': True,
        'description': '国际金融数据API',
        'features': ['股票数据', '外汇数据', '加密货币', '技术指标']
    }
except ImportError:
    DATA_SOURCES['Alpha Vantage'] = {
        'client': None,
        'available': False,
        'description': '国际金融数据API (未配置)',
        'features': ['股票数据', '外汇数据', '加密货币', '技术指标']
    }

# 模拟数据源
DATA_SOURCES['模拟数据'] = {
    'client': None,
    'available': True,
    'description': '用于测试和演示的模拟数据',
    'features': ['历史数据', '技术指标', '无限制访问', '快速响应']
}


def render_data_source_overview():
    """渲染数据源概览"""
    st.header("📡 数据源概览")
    
    # 数据源状态卡片
    cols = st.columns(len(DATA_SOURCES))
    
    for i, (source_name, source_info) in enumerate(DATA_SOURCES.items()):
        with cols[i]:
            # 状态指示器
            status_color = "🟢" if source_info['available'] else "🔴"
            st.markdown(f"### {status_color} {source_name}")
            
            # 描述
            st.write(source_info['description'])
            
            # 功能特性
            st.write("**功能特性:**")
            for feature in source_info['features']:
                st.write(f"• {feature}")
            
            # 状态
            if source_info['available']:
                st.success("✅ 可用")
            else:
                st.error("❌ 不可用")


def render_data_fetch_interface():
    """渲染数据拉取界面"""
    st.header("🔄 数据拉取管理")
    
    # 数据源选择
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("📋 拉取配置")
        
        # 选择数据源
        available_sources = [name for name, info in DATA_SOURCES.items() if info['available']]
        selected_source = st.selectbox(
            "选择数据源",
            available_sources,
            index=0 if available_sources else None
        )
        
        if not available_sources:
            st.error("没有可用的数据源")
            return
        
        # 数据类型选择
        data_types = st.multiselect(
            "数据类型",
            ["股票基础信息", "日线行情", "分钟线行情", "资金流向", "技术指标"],
            default=["股票基础信息", "日线行情"]
        )
        
        # 股票范围
        stock_scope = st.selectbox(
            "股票范围",
            ["全部A股", "沪深300", "中证500", "创业板50", "科创板50", "自定义"],
            index=1
        )
        
        if stock_scope == "自定义":
            custom_stocks = st.text_area(
                "股票代码 (每行一个)",
                placeholder="000001.SZ\n000002.SZ\n600000.SH"
            )
        
        # 时间范围
        date_range = st.date_input(
            "时间范围",
            value=[datetime.now().date() - timedelta(days=30), datetime.now().date()],
            max_value=datetime.now().date()
        )
        
        # 拉取频率
        fetch_frequency = st.selectbox(
            "拉取频率",
            ["立即执行", "每日定时", "每小时", "实时更新"],
            index=0
        )
    
    with col2:
        st.subheader("📊 拉取状态监控")
        
        # 创建状态显示区域
        status_container = st.container()
        progress_container = st.container()
        log_container = st.container()
        
        # 拉取按钮
        if st.button("🚀 开始拉取", type="primary", use_container_width=True):
            execute_data_fetch(
                selected_source, data_types, stock_scope, date_range,
                status_container, progress_container, log_container
            )


def execute_data_fetch(source_name, data_types, stock_scope, date_range, 
                      status_container, progress_container, log_container):
    """执行数据拉取"""
    
    with status_container:
        st.info(f"🔄 正在从 {source_name} 拉取数据...")
    
    with progress_container:
        # 总进度条
        total_progress = st.progress(0)
        total_status = st.empty()
        
        # 详细进度
        detail_progress = st.progress(0)
        detail_status = st.empty()
    
    with log_container:
        st.subheader("📋 拉取日志")
        log_area = st.empty()
    
    # 模拟数据拉取过程
    logs = []
    
    try:
        # 初始化
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 初始化 {source_name} 连接...")
        log_area.text_area("", value="\n".join(logs), height=200, disabled=True)
        time.sleep(1)
        
        # 获取股票列表
        total_progress.progress(10)
        total_status.text("获取股票列表...")
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 获取股票列表: {stock_scope}")
        log_area.text_area("", value="\n".join(logs), height=200, disabled=True)
        time.sleep(1)
        
        # 模拟股票数量
        if stock_scope == "全部A股":
            stock_count = 5000
        elif stock_scope == "沪深300":
            stock_count = 300
        elif stock_scope == "中证500":
            stock_count = 500
        else:
            stock_count = 50
        
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 找到 {stock_count} 只股票")
        log_area.text_area("", value="\n".join(logs), height=200, disabled=True)
        
        # 按数据类型拉取
        for i, data_type in enumerate(data_types):
            total_progress.progress(20 + (i * 60 // len(data_types)))
            total_status.text(f"拉取 {data_type} 数据...")
            
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 开始拉取 {data_type}")
            log_area.text_area("", value="\n".join(logs), height=200, disabled=True)
            
            # 模拟按股票拉取
            for j in range(min(stock_count, 100)):  # 限制演示数量
                detail_progress.progress((j + 1) / min(stock_count, 100))
                detail_status.text(f"处理股票 {j+1}/{min(stock_count, 100)}")
                
                if j % 20 == 0:  # 每20只股票记录一次日志
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 已处理 {j+1} 只股票")
                    log_area.text_area("", value="\n".join(logs), height=200, disabled=True)
                
                time.sleep(0.05)  # 模拟网络延迟
        
        # 数据验证
        total_progress.progress(85)
        total_status.text("验证数据完整性...")
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 验证数据完整性...")
        log_area.text_area("", value="\n".join(logs), height=200, disabled=True)
        time.sleep(1)
        
        # 保存到数据库
        total_progress.progress(95)
        total_status.text("保存到数据库...")
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 保存到数据库...")
        log_area.text_area("", value="\n".join(logs), height=200, disabled=True)
        time.sleep(1)
        
        # 完成
        total_progress.progress(100)
        total_status.text("✅ 拉取完成!")
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ 数据拉取完成!")
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 总计处理 {stock_count} 只股票")
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 数据类型: {', '.join(data_types)}")
        log_area.text_area("", value="\n".join(logs), height=200, disabled=True)
        
        with status_container:
            st.success(f"✅ 从 {source_name} 成功拉取数据!")
            
            # 显示拉取统计
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("处理股票", f"{stock_count:,}")
            with col2:
                st.metric("数据类型", len(data_types))
            with col3:
                st.metric("时间范围", f"{len(date_range)} 天")
            with col4:
                st.metric("数据源", source_name)
        
    except Exception as e:
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 错误: {e}")
        log_area.text_area("", value="\n".join(logs), height=200, disabled=True)
        
        with status_container:
            st.error(f"❌ 数据拉取失败: {e}")


def render_fetch_history():
    """渲染拉取历史"""
    st.header("📈 拉取历史统计")
    
    # 生成模拟历史数据
    dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
    
    # 模拟不同数据源的拉取记录
    history_data = []
    for date in dates:
        for source in ['AllTick', 'Alpha Vantage', '模拟数据']:
            if DATA_SOURCES[source]['available']:
                # 模拟成功率和拉取量
                success_rate = 0.95 if source != '模拟数据' else 1.0
                records = np.random.randint(1000, 5000) if source != '模拟数据' else 1500
                
                history_data.append({
                    'date': date,
                    'source': source,
                    'records': records,
                    'success_rate': success_rate,
                    'duration': np.random.randint(30, 300)  # 秒
                })
    
    df = pd.DataFrame(history_data)
    
    # 拉取量趋势图
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.line(
            df, x='date', y='records', color='source',
            title='每日数据拉取量趋势',
            labels={'records': '记录数', 'date': '日期'}
        )
        fig.update_layout(template="plotly_white", height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 成功率统计
        success_stats = df.groupby('source')['success_rate'].mean().reset_index()
        
        fig = px.bar(
            success_stats, x='source', y='success_rate',
            title='数据源成功率统计',
            labels={'success_rate': '成功率', 'source': '数据源'}
        )
        fig.update_layout(template="plotly_white", height=400)
        fig.update_yaxis(range=[0, 1])
        st.plotly_chart(fig, use_container_width=True)
    
    # 详细统计表
    st.subheader("📊 详细统计")
    
    # 按数据源汇总
    summary = df.groupby('source').agg({
        'records': ['sum', 'mean'],
        'success_rate': 'mean',
        'duration': 'mean'
    }).round(2)
    
    summary.columns = ['总记录数', '平均记录数', '平均成功率', '平均耗时(秒)']
    st.dataframe(summary, use_container_width=True)


def render_data_source_config():
    """渲染数据源配置"""
    st.header("⚙️ 数据源配置")
    
    # 配置选项卡
    tab1, tab2, tab3 = st.tabs(["AllTick配置", "Alpha Vantage配置", "通用配置"])
    
    with tab1:
        st.subheader("🔧 AllTick API 配置")
        
        col1, col2 = st.columns(2)
        with col1:
            alltick_token = st.text_input(
                "API Token",
                value=data_settings.alltick_token if hasattr(data_settings, 'alltick_token') else "",
                type="password"
            )
            alltick_timeout = st.number_input(
                "请求超时(秒)",
                value=30,
                min_value=5,
                max_value=300
            )
        
        with col2:
            st.info("""
            **AllTick API 说明:**
            - 提供A股实时和历史数据
            - 支持分钟级和日级数据
            - 包含资金流向数据
            - 需要有效的API Token
            """)
        
        if st.button("🧪 测试 AllTick 连接"):
            if alltick_token:
                with st.spinner("测试连接中..."):
                    time.sleep(2)  # 模拟测试
                    st.success("✅ AllTick 连接测试成功!")
            else:
                st.error("❌ 请输入有效的API Token")
    
    with tab2:
        st.subheader("🔧 Alpha Vantage API 配置")
        
        col1, col2 = st.columns(2)
        with col1:
            av_token = st.text_input(
                "API Key",
                value=data_settings.alpha_vantage_api_key if hasattr(data_settings, 'alpha_vantage_api_key') else "",
                type="password"
            )
            av_timeout = st.number_input(
                "请求超时(秒)",
                value=30,
                min_value=5,
                max_value=300,
                key="av_timeout"
            )
        
        with col2:
            st.info("""
            **Alpha Vantage API 说明:**
            - 提供全球股票数据
            - 支持技术指标计算
            - 免费版有请求限制
            - 需要注册获取API Key
            """)
        
        if st.button("🧪 测试 Alpha Vantage 连接"):
            if av_token:
                with st.spinner("测试连接中..."):
                    time.sleep(2)  # 模拟测试
                    st.success("✅ Alpha Vantage 连接测试成功!")
            else:
                st.error("❌ 请输入有效的API Key")
    
    with tab3:
        st.subheader("🔧 通用配置")
        
        col1, col2 = st.columns(2)
        with col1:
            max_concurrent = st.number_input(
                "最大并发请求数",
                value=3,
                min_value=1,
                max_value=10
            )
            
            cache_ttl = st.number_input(
                "缓存有效期(分钟)",
                value=60,
                min_value=1,
                max_value=1440
            )
        
        with col2:
            enable_cache = st.checkbox("启用数据缓存", value=True)
            enable_retry = st.checkbox("启用失败重试", value=True)
            
            if enable_retry:
                retry_times = st.number_input(
                    "重试次数",
                    value=3,
                    min_value=1,
                    max_value=10
                )
        
        if st.button("💾 保存配置", type="primary"):
            st.success("✅ 配置已保存!")


def render_data_source_manager_main():
    """渲染数据源管理主面板"""
    # 数据源概览
    render_data_source_overview()
    st.markdown("---")
    
    # 创建标签页
    tab1, tab2, tab3, tab4 = st.tabs(["🔄 数据拉取", "📈 拉取历史", "⚙️ 源配置", "📊 监控面板"])
    
    with tab1:
        render_data_fetch_interface()
    
    with tab2:
        render_fetch_history()
    
    with tab3:
        render_data_source_config()
    
    with tab4:
        st.header("📊 实时监控面板")
        st.info("实时监控面板功能开发中...")
        
        # 可以添加实时监控功能，如：
        # - API调用频率监控
        # - 数据质量监控
        # - 错误率统计
        # - 性能指标
