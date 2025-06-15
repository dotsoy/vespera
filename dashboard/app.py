"""
启明星股票分析系统 - Streamlit 仪表盘
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.database import get_db_manager
from src.utils.logger import get_logger
from config.settings import app_settings

# 配置页面
st.set_page_config(
    page_title="启明星股票分析系统",
    page_icon="⭐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 初始化
logger = get_logger("dashboard")
db_manager = get_db_manager()


@st.cache_data(ttl=300)  # 缓存5分钟
def load_trading_signals(date_filter: str = None):
    """加载交易信号数据"""
    try:
        query = """
        SELECT 
            ts.ts_code,
            sb.name,
            sb.industry,
            ts.trade_date,
            ts.signal_type,
            ts.confidence_score,
            ts.technical_score,
            ts.capital_score,
            ts.entry_price,
            ts.stop_loss,
            ts.target_price,
            ts.risk_reward_ratio,
            ts.signal_reason
        FROM trading_signals ts
        LEFT JOIN stock_basic sb ON ts.ts_code = sb.ts_code
        WHERE ts.is_active = true
        """
        
        params = {}
        if date_filter:
            query += " AND ts.trade_date >= :date_filter"
            params['date_filter'] = date_filter
        
        query += " ORDER BY ts.confidence_score DESC, ts.trade_date DESC"
        
        df = db_manager.execute_postgres_query(query, params)
        return df
    except Exception as e:
        logger.error(f"加载交易信号失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_stock_quotes(ts_code: str, days: int = 60):
    """加载股票行情数据"""
    try:
        query = """
        SELECT * FROM stock_daily_quotes 
        WHERE ts_code = :ts_code 
        ORDER BY trade_date DESC 
        LIMIT :days
        """
        
        df = db_manager.execute_postgres_query(
            query, {'ts_code': ts_code, 'days': days}
        )
        return df.sort_values('trade_date')
    except Exception as e:
        logger.error(f"加载股票行情失败: {e}")
        return pd.DataFrame()


def create_candlestick_chart(df: pd.DataFrame, title: str):
    """创建K线图"""
    if df.empty:
        return None
    
    fig = go.Figure(data=go.Candlestick(
        x=df['trade_date'],
        open=df['open_price'],
        high=df['high_price'],
        low=df['low_price'],
        close=df['close_price'],
        name="K线"
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title="日期",
        yaxis_title="价格",
        template="plotly_white",
        height=500
    )
    
    return fig


def create_volume_chart(df: pd.DataFrame):
    """创建成交量图"""
    if df.empty:
        return None
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['trade_date'],
        y=df['vol'],
        name="成交量",
        marker_color='lightblue'
    ))
    
    fig.update_layout(
        title="成交量",
        xaxis_title="日期",
        yaxis_title="成交量",
        template="plotly_white",
        height=200
    )
    
    return fig


def create_radar_chart(signal_row):
    """创建四维雷达图"""
    import plotly.graph_objects as go

    categories = ['技术面', '资金流', '基本面', '宏观面']
    values = [
        signal_row.get('technical_score', 0) * 100,
        signal_row.get('capital_score', 0) * 100,
        signal_row.get('fundamental_score', 0) * 100,
        signal_row.get('macro_score', 0) * 100
    ]

    fig = go.Figure()

    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name='评分',
        line_color='rgb(0, 123, 255)',
        fillcolor='rgba(0, 123, 255, 0.3)'
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )),
        showlegend=False,
        height=300,
        margin=dict(l=20, r=20, t=20, b=20)
    )

    return fig


def display_signal_card(signal_row):
    """显示增强的信号卡片"""
    with st.container():
        # 主要信息行
        col1, col2, col3 = st.columns([3, 2, 2])

        with col1:
            st.subheader(f"{signal_row['name']} ({signal_row['ts_code']})")
            st.write(f"**行业**: {signal_row['industry']}")
            st.write(f"**信号原因**: {signal_row['signal_reason']}")

            # 置信度进度条
            confidence = signal_row['confidence_score']
            st.write("**综合置信度**")
            st.progress(confidence)
            st.write(f"{confidence:.1%}")

        with col2:
            # 四维评分雷达图
            st.write("**四维分析雷达图**")
            radar_fig = create_radar_chart(signal_row)
            st.plotly_chart(radar_fig, use_container_width=True)

        with col3:
            # 交易参数
            st.write("**交易参数**")
            st.metric("入场价", f"¥{signal_row['entry_price']:.2f}")
            st.metric("止损价", f"¥{signal_row['stop_loss']:.2f}")
            st.metric("目标价", f"¥{signal_row['target_price']:.2f}")
            st.metric("风险收益比", f"1:{signal_row['risk_reward_ratio']:.1f}")
            st.metric("建议仓位", f"{signal_row.get('position_size', 0.05):.1%}")

        # 详细评分展示
        st.write("**详细评分**")
        score_col1, score_col2, score_col3, score_col4 = st.columns(4)

        with score_col1:
            tech_score = signal_row.get('technical_score', 0)
            st.metric("技术面", f"{tech_score:.1%}",
                     delta=f"{'强势' if tech_score > 0.7 else '一般' if tech_score > 0.5 else '偏弱'}")

        with score_col2:
            capital_score = signal_row.get('capital_score', 0)
            st.metric("资金流", f"{capital_score:.1%}",
                     delta=f"{'流入' if capital_score > 0.7 else '平衡' if capital_score > 0.5 else '流出'}")

        with score_col3:
            fund_score = signal_row.get('fundamental_score', 0)
            st.metric("基本面", f"{fund_score:.1%}",
                     delta=f"{'利好' if fund_score > 0.7 else '中性' if fund_score > 0.5 else '利空'}")

        with score_col4:
            macro_score = signal_row.get('macro_score', 0)
            st.metric("宏观面", f"{macro_score:.1%}",
                     delta=f"{'有利' if macro_score > 0.7 else '中性' if macro_score > 0.5 else '不利'}")


def create_signal_analysis_chart(ts_code: str):
    """创建信号分析图表"""
    try:
        # 获取历史分析数据
        query = """
        SELECT
            trade_date,
            trend_score,
            momentum_score,
            volume_health_score,
            pattern_score
        FROM technical_daily_profiles
        WHERE ts_code = :ts_code
        ORDER BY trade_date DESC
        LIMIT 30
        """

        df = db_manager.execute_postgres_query(query, {'ts_code': ts_code})

        if df.empty:
            return None

        df = df.sort_values('trade_date')

        fig = go.Figure()

        # 添加各项技术指标
        fig.add_trace(go.Scatter(
            x=df['trade_date'],
            y=df['trend_score'],
            mode='lines+markers',
            name='趋势评分',
            line=dict(color='blue')
        ))

        fig.add_trace(go.Scatter(
            x=df['trade_date'],
            y=df['momentum_score'],
            mode='lines+markers',
            name='动量评分',
            line=dict(color='green')
        ))

        fig.add_trace(go.Scatter(
            x=df['trade_date'],
            y=df['volume_health_score'],
            mode='lines+markers',
            name='量能评分',
            line=dict(color='orange')
        ))

        fig.update_layout(
            title="技术指标历史趋势",
            xaxis_title="日期",
            yaxis_title="评分",
            yaxis=dict(range=[0, 1]),
            template="plotly_white",
            height=400
        )

        return fig

    except Exception as e:
        logger.error(f"创建分析图表失败: {e}")
        return None


def main():
    """主函数"""
    # 标题
    st.title("⭐ 启明星股票分析系统")
    st.markdown("---")
    
    # 侧边栏
    with st.sidebar:
        st.header("📊 控制面板")
        
        # 日期筛选
        date_filter = st.date_input(
            "信号日期筛选",
            value=datetime.now() - timedelta(days=7),
            max_value=datetime.now()
        )
        
        # 置信度筛选
        min_confidence = st.slider(
            "最低置信度",
            min_value=0.0,
            max_value=1.0,
            value=0.7,
            step=0.05
        )
        
        # 刷新按钮
        if st.button("🔄 刷新数据"):
            st.cache_data.clear()
            st.rerun()
    
    # 主要内容区域
    tab1, tab2, tab3, tab4 = st.tabs(["🎯 交易信号", "📊 市场概览", "📈 分析详情", "⚙️ 系统状态"])

    with tab1:
        st.header("🎯 今日交易信号")

        # 加载信号数据
        signals_df = load_trading_signals(date_filter.strftime('%Y-%m-%d'))

        if signals_df.empty:
            st.warning("暂无交易信号数据")
            st.info("💡 提示：请确保已运行数据分析任务，或检查数据库连接")
            return

        # 按置信度筛选
        filtered_signals = signals_df[signals_df['confidence_score'] >= min_confidence]

        if filtered_signals.empty:
            st.warning(f"没有置信度大于 {min_confidence:.0%} 的信号")
            st.info(f"当前共有 {len(signals_df)} 个信号，建议降低置信度阈值")
            return

        # 信号统计
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("总信号数", len(filtered_signals))
        with col2:
            high_quality = len(filtered_signals[filtered_signals['confidence_score'] > 0.85])
            st.metric("高质量信号", high_quality)
        with col3:
            avg_confidence = filtered_signals['confidence_score'].mean()
            st.metric("平均置信度", f"{avg_confidence:.1%}")
        with col4:
            avg_risk_reward = filtered_signals['risk_reward_ratio'].mean()
            st.metric("平均风险收益比", f"1:{avg_risk_reward:.1f}")

        st.markdown("---")

        # 显示信号列表
        for idx, signal in filtered_signals.iterrows():
            with st.expander(
                f"🔥 {signal['name']} - 置信度: {signal['confidence_score']:.1%} - 风险收益比: 1:{signal['risk_reward_ratio']:.1f}",
                expanded=idx < 2  # 默认展开前2个
            ):
                display_signal_card(signal)

                # 图表选项
                chart_col1, chart_col2, chart_col3 = st.columns(3)

                with chart_col1:
                    if st.button(f"📈 K线图", key=f"kline_{idx}"):
                        quotes_df = load_stock_quotes(signal['ts_code'])
                        if not quotes_df.empty:
                            fig = create_candlestick_chart(
                                quotes_df,
                                f"{signal['name']} ({signal['ts_code']}) K线图"
                            )
                            st.plotly_chart(fig, use_container_width=True)

                            vol_fig = create_volume_chart(quotes_df)
                            st.plotly_chart(vol_fig, use_container_width=True)

                with chart_col2:
                    if st.button(f"📊 分析趋势", key=f"analysis_{idx}"):
                        analysis_fig = create_signal_analysis_chart(signal['ts_code'])
                        if analysis_fig:
                            st.plotly_chart(analysis_fig, use_container_width=True)
                        else:
                            st.warning("暂无分析数据")

                with chart_col3:
                    if st.button(f"💾 导出信号", key=f"export_{idx}"):
                        signal_data = {
                            '股票代码': signal['ts_code'],
                            '股票名称': signal['name'],
                            '信号类型': signal['signal_type'],
                            '置信度': f"{signal['confidence_score']:.1%}",
                            '入场价': signal['entry_price'],
                            '止损价': signal['stop_loss'],
                            '目标价': signal['target_price'],
                            '风险收益比': f"1:{signal['risk_reward_ratio']:.1f}",
                            '建议仓位': f"{signal.get('position_size', 0.05):.1%}",
                            '信号原因': signal['signal_reason']
                        }
                        st.json(signal_data)

                st.markdown("---")
    
    with tab2:
        st.header("📊 市场概览")

        if not signals_df.empty:
            # 市场统计概览
            col1, col2 = st.columns(2)

            with col1:
                # 信号类型分布
                signal_counts = signals_df['signal_type'].value_counts()
                fig_pie = px.pie(
                    values=signal_counts.values,
                    names=signal_counts.index,
                    title="信号类型分布"
                )
                st.plotly_chart(fig_pie, use_container_width=True)

                # 置信度分布
                fig_hist = px.histogram(
                    signals_df,
                    x='confidence_score',
                    nbins=20,
                    title="置信度分布",
                    labels={'confidence_score': '置信度', 'count': '数量'}
                )
                st.plotly_chart(fig_hist, use_container_width=True)

            with col2:
                # 行业分布
                industry_counts = signals_df['industry'].value_counts().head(10)
                fig_bar = px.bar(
                    x=industry_counts.values,
                    y=industry_counts.index,
                    orientation='h',
                    title="热门行业 TOP 10",
                    labels={'x': '信号数量', 'y': '行业'}
                )
                st.plotly_chart(fig_bar, use_container_width=True)

                # 风险收益比分布
                fig_risk_reward = px.histogram(
                    signals_df,
                    x='risk_reward_ratio',
                    nbins=15,
                    title="风险收益比分布",
                    labels={'risk_reward_ratio': '风险收益比', 'count': '数量'}
                )
                st.plotly_chart(fig_risk_reward, use_container_width=True)

            # 四维评分对比
            st.subheader("📈 四维评分分析")

            # 计算平均评分
            avg_scores = {
                '技术面': signals_df['technical_score'].mean(),
                '资金流': signals_df['capital_score'].mean(),
                '基本面': signals_df['fundamental_score'].mean(),
                '宏观面': signals_df['macro_score'].mean()
            }

            # 创建评分对比图
            fig_scores = px.bar(
                x=list(avg_scores.keys()),
                y=list(avg_scores.values()),
                title="各维度平均评分",
                labels={'x': '分析维度', 'y': '平均评分'},
                color=list(avg_scores.values()),
                color_continuous_scale='viridis'
            )
            fig_scores.update_layout(showlegend=False)
            st.plotly_chart(fig_scores, use_container_width=True)

        else:
            st.warning("暂无数据用于市场概览")

    with tab3:
        st.header("📈 深度分析")

        # 选择股票进行详细分析
        if not signals_df.empty:
            selected_stock = st.selectbox(
                "选择股票进行详细分析",
                options=signals_df['ts_code'].tolist(),
                format_func=lambda x: f"{signals_df[signals_df['ts_code']==x]['name'].iloc[0]} ({x})"
            )

            if selected_stock:
                # 获取选中股票的信号信息
                stock_signal = signals_df[signals_df['ts_code'] == selected_stock].iloc[0]

                st.subheader(f"📊 {stock_signal['name']} ({selected_stock}) 详细分析")

                # 显示详细的信号卡片
                display_signal_card(stock_signal)

                # 分析图表
                col1, col2 = st.columns(2)

                with col1:
                    st.subheader("📈 K线图")
                    quotes_df = load_stock_quotes(selected_stock)
                    if not quotes_df.empty:
                        fig = create_candlestick_chart(
                            quotes_df,
                            f"{stock_signal['name']} K线图"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("暂无K线数据")

                with col2:
                    st.subheader("📊 技术指标趋势")
                    analysis_fig = create_signal_analysis_chart(selected_stock)
                    if analysis_fig:
                        st.plotly_chart(analysis_fig, use_container_width=True)
                    else:
                        st.warning("暂无技术分析数据")

                # 成交量图
                st.subheader("📊 成交量分析")
                if not quotes_df.empty:
                    vol_fig = create_volume_chart(quotes_df)
                    st.plotly_chart(vol_fig, use_container_width=True)
        else:
            st.warning("暂无信号数据进行分析")
    
    with tab4:
        st.header("⚙️ 系统状态")

        # 数据库连接状态
        st.subheader("🔗 数据库连接状态")
        try:
            connection_status = db_manager.test_connections()

            col1, col2, col3 = st.columns(3)

            with col1:
                postgres_status = connection_status.get('postgres', False)
                status_text = "🟢 正常" if postgres_status else "🔴 异常"
                st.metric("PostgreSQL", status_text)
                if postgres_status:
                    st.success("主数据库连接正常")
                else:
                    st.error("主数据库连接异常")

            with col2:
                clickhouse_status = connection_status.get('clickhouse', False)
                status_text = "🟢 正常" if clickhouse_status else "🔴 异常"
                st.metric("ClickHouse", status_text)
                if clickhouse_status:
                    st.success("时序数据库连接正常")
                else:
                    st.error("时序数据库连接异常")

            with col3:
                redis_status = connection_status.get('redis', False)
                status_text = "🟢 正常" if redis_status else "🔴 异常"
                st.metric("Redis", status_text)
                if redis_status:
                    st.success("缓存服务连接正常")
                else:
                    st.error("缓存服务连接异常")

        except Exception as e:
            st.error(f"检查数据库状态失败: {e}")

        # 数据统计
        st.subheader("📊 数据统计")
        try:
            # 获取各表数据量
            tables_stats = {}

            # 股票基础信息
            stock_count = db_manager.execute_postgres_query("SELECT COUNT(*) as count FROM stock_basic")
            tables_stats['股票数量'] = stock_count.iloc[0]['count'] if not stock_count.empty else 0

            # 今日行情数据
            today_quotes = db_manager.execute_postgres_query(
                "SELECT COUNT(*) as count FROM stock_daily_quotes WHERE trade_date = CURRENT_DATE"
            )
            tables_stats['今日行情'] = today_quotes.iloc[0]['count'] if not today_quotes.empty else 0

            # 技术分析数据
            tech_analysis = db_manager.execute_postgres_query(
                "SELECT COUNT(*) as count FROM technical_daily_profiles WHERE trade_date = CURRENT_DATE"
            )
            tables_stats['技术分析'] = tech_analysis.iloc[0]['count'] if not tech_analysis.empty else 0

            # 交易信号数据
            signals_count = db_manager.execute_postgres_query(
                "SELECT COUNT(*) as count FROM trading_signals WHERE trade_date = CURRENT_DATE AND is_active = true"
            )
            tables_stats['活跃信号'] = signals_count.iloc[0]['count'] if not signals_count.empty else 0

            # 显示统计信息
            stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)

            with stat_col1:
                st.metric("股票总数", f"{tables_stats['股票数量']:,}")

            with stat_col2:
                st.metric("今日行情", f"{tables_stats['今日行情']:,}")

            with stat_col3:
                st.metric("技术分析", f"{tables_stats['技术分析']:,}")

            with stat_col4:
                st.metric("活跃信号", f"{tables_stats['活跃信号']:,}")

        except Exception as e:
            st.error(f"获取数据统计失败: {e}")

        # 系统信息
        st.subheader("ℹ️ 系统信息")
        info_col1, info_col2 = st.columns(2)

        with info_col1:
            st.write(f"**应用名称**: {app_settings.app_name}")
            st.write(f"**版本**: {app_settings.version}")
            st.write(f"**调试模式**: {'开启' if app_settings.debug else '关闭'}")
            st.write(f"**当前时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        with info_col2:
            st.write(f"**日志级别**: {app_settings.log_level}")
            st.write(f"**数据目录**: {app_settings.data_dir}")
            st.write(f"**日志目录**: {app_settings.logs_dir}")
            st.write(f"**项目根目录**: {app_settings.project_root}")

        # 快速操作
        st.subheader("🚀 快速操作")
        op_col1, op_col2, op_col3 = st.columns(3)

        with op_col1:
            if st.button("🔄 刷新缓存"):
                st.cache_data.clear()
                st.success("缓存已清理")

        with op_col2:
            if st.button("📊 Airflow 管理"):
                st.info("请访问: http://localhost:8080")

        with op_col3:
            if st.button("📝 查看日志"):
                st.info(f"日志位置: {app_settings.logs_dir}")

        # 系统健康检查
        st.subheader("🏥 系统健康检查")

        health_checks = []

        # 检查数据库连接
        all_db_connected = all(connection_status.values()) if 'connection_status' in locals() else False
        health_checks.append(("数据库连接", all_db_connected))

        # 检查数据完整性
        data_complete = (tables_stats.get('股票数量', 0) > 0 and
                        tables_stats.get('今日行情', 0) > 0)
        health_checks.append(("数据完整性", data_complete))

        # 检查分析任务
        analysis_complete = tables_stats.get('技术分析', 0) > 0
        health_checks.append(("分析任务", analysis_complete))

        # 检查信号生成
        signals_generated = tables_stats.get('活跃信号', 0) > 0
        health_checks.append(("信号生成", signals_generated))

        for check_name, status in health_checks:
            if status:
                st.success(f"✅ {check_name}: 正常")
            else:
                st.error(f"❌ {check_name}: 异常")

        # 整体健康状态
        overall_health = sum(1 for _, status in health_checks if status) / len(health_checks)

        if overall_health >= 0.8:
            st.success(f"🎉 系统整体健康状态: 优秀 ({overall_health:.0%})")
        elif overall_health >= 0.6:
            st.warning(f"⚠️ 系统整体健康状态: 良好 ({overall_health:.0%})")
        else:
            st.error(f"🚨 系统整体健康状态: 需要关注 ({overall_health:.0%})")


if __name__ == "__main__":
    main()
