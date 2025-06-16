"""
系统状态监控组件
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import psutil
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

# 暂时注释掉有问题的导入
# from src.utils.database import get_db_manager
# from src.strategies.qiming_star import QimingStarStrategy

logger = get_logger("system_status")


def get_system_info():
    """获取系统信息"""
    try:
        # CPU信息
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()
        
        # 内存信息
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        memory_total = memory.total / (1024**3)  # GB
        memory_used = memory.used / (1024**3)   # GB
        
        # 磁盘信息
        disk = psutil.disk_usage('/')
        disk_percent = (disk.used / disk.total) * 100
        disk_total = disk.total / (1024**3)     # GB
        disk_used = disk.used / (1024**3)       # GB
        
        return {
            "cpu_percent": cpu_percent,
            "cpu_count": cpu_count,
            "memory_percent": memory_percent,
            "memory_total": memory_total,
            "memory_used": memory_used,
            "disk_percent": disk_percent,
            "disk_total": disk_total,
            "disk_used": disk_used
        }
    except Exception as e:
        logger.error(f"获取系统信息失败: {e}")
        return {}


def get_database_status():
    """获取数据库状态"""
    try:
        # 暂时使用模拟数据库状态
        # TODO: 修复数据库连接后替换为真实状态检查

        # 模拟连接状态
        connection_status = {
            'postgres': False,  # 模拟连接失败
            'clickhouse': False,
            'redis': False
        }

        # 模拟表信息
        table_info = {
            'stock_basic': 15,
            'stock_daily_quotes': 1500,
            'trading_signals': 0,
            'technical_daily_profiles': 0,
            'capital_flow_daily': 0
        }

        return {
            "connections": connection_status,
            "table_info": table_info
        }

    except Exception as e:
        logger.error(f"获取数据库状态失败: {e}")
        return {"connections": {}, "table_info": {}}


def get_available_strategies():
    """获取可用策略列表"""
    try:
        strategies = [
            {
                "name": "启明星策略",
                "version": "1.0.0",
                "description": "基于'资金为王，技术触发'理念的T+1交易策略",
                "status": "active",
                "last_run": datetime.now() - timedelta(hours=2)
            },
            {
                "name": "简单移动平均策略",
                "version": "1.0.0", 
                "description": "基于移动平均线的经典策略",
                "status": "active",
                "last_run": datetime.now() - timedelta(days=1)
            },
            {
                "name": "RSI策略",
                "version": "1.0.0",
                "description": "基于相对强弱指数的反转策略",
                "status": "active", 
                "last_run": datetime.now() - timedelta(days=1)
            }
        ]
        
        return strategies
        
    except Exception as e:
        logger.error(f"获取策略列表失败: {e}")
        return []


def render_system_overview():
    """渲染系统概览"""
    st.header("🖥️ 系统状态概览")
    
    # 获取系统信息
    system_info = get_system_info()
    db_status = get_database_status()
    strategies = get_available_strategies()
    
    # 系统资源状态
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if system_info:
            cpu_color = "normal" if system_info["cpu_percent"] < 80 else "inverse"
            st.metric(
                "CPU使用率",
                f"{system_info['cpu_percent']:.1f}%",
                delta=f"{system_info['cpu_count']} 核心",
                delta_color=cpu_color
            )
        else:
            st.metric("CPU使用率", "N/A")
    
    with col2:
        if system_info:
            memory_color = "normal" if system_info["memory_percent"] < 80 else "inverse"
            st.metric(
                "内存使用率",
                f"{system_info['memory_percent']:.1f}%",
                delta=f"{system_info['memory_used']:.1f}GB / {system_info['memory_total']:.1f}GB",
                delta_color=memory_color
            )
        else:
            st.metric("内存使用率", "N/A")
    
    with col3:
        if system_info:
            disk_color = "normal" if system_info["disk_percent"] < 80 else "inverse"
            st.metric(
                "磁盘使用率",
                f"{system_info['disk_percent']:.1f}%",
                delta=f"{system_info['disk_used']:.1f}GB / {system_info['disk_total']:.1f}GB",
                delta_color=disk_color
            )
        else:
            st.metric("磁盘使用率", "N/A")
    
    with col4:
        active_strategies = len([s for s in strategies if s["status"] == "active"])
        st.metric(
            "可用策略",
            f"{active_strategies}",
            delta="个策略就绪"
        )


def render_database_status():
    """渲染数据库状态"""
    st.header("🗄️ 数据库状态")
    
    db_status = get_database_status()
    connections = db_status.get("connections", {})
    table_info = db_status.get("table_info", {})
    
    # 数据库连接状态
    col1, col2, col3 = st.columns(3)
    
    with col1:
        postgres_status = connections.get('postgres', False)
        status_text = "✅ 已连接" if postgres_status else "❌ 连接失败"
        st.metric("PostgreSQL", status_text)
    
    with col2:
        clickhouse_status = connections.get('clickhouse', False)
        status_text = "✅ 已连接" if clickhouse_status else "❌ 连接失败"
        st.metric("ClickHouse", status_text)
    
    with col3:
        redis_status = connections.get('redis', False)
        status_text = "✅ 已连接" if redis_status else "❌ 连接失败"
        st.metric("Redis", status_text)
    
    # 数据表状态
    if table_info:
        st.subheader("📊 数据表状态")
        
        # 创建表格显示
        table_data = []
        for table_name, count in table_info.items():
            table_data.append({
                "表名": table_name,
                "记录数": f"{count:,}",
                "状态": "✅ 正常" if count > 0 else "⚠️ 无数据"
            })
        
        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True)
        
        # 数据分布图
        if any(count > 0 for count in table_info.values()):
            fig = px.bar(
                x=list(table_info.keys()),
                y=list(table_info.values()),
                title="数据表记录数分布",
                labels={'x': '数据表', 'y': '记录数'}
            )
            fig.update_layout(template="plotly_white", height=400)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("无法获取数据表信息，请检查数据库连接")


def render_strategy_status():
    """渲染策略状态"""
    st.header("🎯 策略状态")
    
    strategies = get_available_strategies()
    
    if strategies:
        for strategy in strategies:
            with st.expander(f"📈 {strategy['name']} v{strategy['version']}", expanded=True):
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    st.write(f"**描述**: {strategy['description']}")
                    st.write(f"**最后运行**: {strategy['last_run'].strftime('%Y-%m-%d %H:%M:%S')}")
                
                with col2:
                    status_color = "🟢" if strategy['status'] == 'active' else "🔴"
                    st.write(f"**状态**: {status_color} {strategy['status']}")
                
                with col3:
                    if st.button(f"🚀 运行", key=f"run_{strategy['name']}"):
                        st.success(f"正在运行 {strategy['name']}...")
                        # 这里可以添加实际的策略运行逻辑
    else:
        st.warning("未找到可用策略")


def render_system_logs():
    """渲染系统日志"""
    st.header("📋 系统日志")
    
    try:
        # 读取最近的日志
        log_file = project_root / "logs" / "app.log"
        
        if log_file.exists():
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # 显示最近50行日志
            recent_logs = lines[-50:] if len(lines) > 50 else lines
            
            # 日志级别筛选
            log_level = st.selectbox(
                "日志级别筛选",
                ["ALL", "ERROR", "WARNING", "INFO", "DEBUG"],
                index=0
            )
            
            # 筛选日志
            if log_level != "ALL":
                filtered_logs = [line for line in recent_logs if log_level in line]
            else:
                filtered_logs = recent_logs
            
            # 显示日志
            if filtered_logs:
                log_text = "".join(filtered_logs)
                st.text_area(
                    "最近日志",
                    value=log_text,
                    height=400,
                    disabled=True
                )
            else:
                st.info(f"没有找到 {log_level} 级别的日志")
        else:
            st.warning("日志文件不存在")
            
    except Exception as e:
        st.error(f"读取日志失败: {e}")


def render_system_actions():
    """渲染系统操作"""
    st.header("⚙️ 系统操作")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 刷新系统状态", type="primary"):
            st.cache_data.clear()
            st.rerun()
    
    with col2:
        if st.button("🧹 清理缓存"):
            st.cache_data.clear()
            st.success("缓存已清理")
    
    with col3:
        if st.button("📊 生成系统报告"):
            # 生成系统状态报告
            system_info = get_system_info()
            db_status = get_database_status()
            strategies = get_available_strategies()
            
            report = {
                "生成时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "系统资源": system_info,
                "数据库状态": db_status,
                "策略数量": len(strategies)
            }
            
            st.json(report)


def render_system_status_main():
    """渲染系统状态主面板"""
    # 系统概览
    render_system_overview()
    st.markdown("---")
    
    # 数据库状态
    render_database_status()
    st.markdown("---")
    
    # 策略状态
    render_strategy_status()
    st.markdown("---")
    
    # 系统操作
    render_system_actions()
    
    # 可选：系统日志（折叠显示）
    with st.expander("📋 查看系统日志"):
        render_system_logs()
