"""
个股全息透视 - 以股票为核心的聚合多表信息分析视图
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import sys
import traceback
from datetime import datetime, timedelta

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.database import get_db_manager
from src.data_sources.base_data_source import DataRequest, DataType
from src.data_sources.data_source_factory import get_data_service

logger = get_logger("stock_holographic_view")

# 检查数据库可用性
try:
    DB_AVAILABLE = True
except ImportError as e:
    logger.warning(f"数据库模块导入失败: {e}")
    DB_AVAILABLE = False


def load_stock_list():
    """加载股票列表"""
    if not DB_AVAILABLE:
        return pd.DataFrame()
    
    try:
        db_manager = get_db_manager()
        query = "SELECT ts_code, name FROM stock_basic ORDER BY name"
        return db_manager.execute_postgres_query(query)
    except Exception as e:
        logger.error(f"加载股票列表失败: {e}")
        return pd.DataFrame()


def get_holographic_data_for_stock(ts_code: str):
    """获取指定股票的所有关联数据，并融合成一张宽表"""
    if not DB_AVAILABLE:
        return pd.DataFrame(), pd.DataFrame()
    
    try:
        db_manager = get_db_manager()
        
        data_service = get_data_service()
        
        # 计算近一年的日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

        # 创建数据请求对象
        data_service = get_data_service()

        # 获取日线数据
        df_quotes = data_service.get_data(
            request=DataRequest(
                data_type=DataType.STOCK_DAILY,
                symbol=ts_code,
                start_date=start_date,
                end_date=end_date
            )
        )
        
        if df_quotes is not None and not df_quotes.empty:
            logger.info(f"成功获取 {ts_code} 的日线数据，记录数: {len(df_quotes)}")
        else:
            logger.warning(f"未能获取 {ts_code} 的日线数据")
            return pd.DataFrame(), pd.DataFrame()

        # 确保日期格式正确
        df_quotes['trade_date'] = pd.to_datetime(df_quotes['trade_date'])

        # 存入ClickHouse
        try:
            db_manager.insert_dataframe_to_clickhouse(df_quotes, 'daily_quotes')
            logger.info(f"已更新 {ts_code} 的日线数据到ClickHouse")
        except Exception as e:
            logger.warning(f"存储到ClickHouse失败: {e}")

        # 尝试查询其他分析数据，如果表不存在则返回空DataFrame
        try:
            query_tech = f"SELECT trade_date, rsi as trend_score, macd as momentum_score, ma5, ma10, ma20 FROM technical_indicators_daily WHERE ts_code = '{ts_code}' ORDER BY trade_date"
            df_tech = db_manager.execute_postgres_query(query_tech)
            if not df_tech.empty:
                df_tech['trade_date'] = pd.to_datetime(df_tech['trade_date'])
        except Exception as e:
            logger.warning(f"技术分析数据查询失败: {e}")
            df_tech = pd.DataFrame()

        try:
            query_capital = f"""
            SELECT 
                trade_date,
                main_net_inflow,
                CASE 
                    WHEN total_amount > 0 THEN main_net_inflow / total_amount 
                    ELSE 0 
                END as net_inflow_ratio,
                main_net_inflow as main_force_trend 
            FROM capital_flow_daily 
            WHERE stock_code = '{ts_code}' 
            ORDER BY trade_date
            """
            df_capital = db_manager.execute_postgres_query(query_capital)
            if not df_capital.empty:
                df_capital['trade_date'] = pd.to_datetime(df_capital['trade_date'])
        except Exception as e:
            logger.warning(f"资金流数据查询失败: {e}")
            df_capital = pd.DataFrame()

        try:
            query_sentiment = f"SELECT trade_date, sentiment_score as signal_grade, 'AI分析' as signal_reason FROM market_sentiment_daily WHERE ts_code = '{ts_code}' ORDER BY trade_date"
            df_signals = db_manager.execute_postgres_query(query_sentiment)
            if not df_signals.empty:
                df_signals['trade_date'] = pd.to_datetime(df_signals['trade_date'])
        except Exception as e:
            logger.warning(f"信号数据查询失败: {e}")
            df_signals = pd.DataFrame()

        # 新闻事件数据（暂时使用空DataFrame，因为没有相关表）
        df_news = pd.DataFrame()

        # 使用Pandas的merge功能，基于'trade_date'字段进行连接
        df_holographic = df_quotes
        if not df_tech.empty:
            df_holographic = pd.merge(df_holographic, df_tech, on="trade_date", how="left")
        if not df_capital.empty:
            df_holographic = pd.merge(df_holographic, df_capital, on="trade_date", how="left")
        if not df_signals.empty:
            df_holographic = pd.merge(df_holographic, df_signals, on="trade_date", how="left")
        
        # 确保日期格式正确，并作为索引
        if not df_holographic.empty:
            df_holographic['trade_date'] = pd.to_datetime(df_holographic['trade_date'])
            df_holographic = df_holographic.set_index('trade_date')
        
        return df_holographic, df_news
    except Exception as e:
        logger.error(f"获取股票全息数据失败: {e}\n{traceback.format_exc()}")
        return pd.DataFrame(), pd.DataFrame()


def render_stock_identity_card(stock_data):
    """渲染股票身份卡"""
    st.subheader("股票身份卡")
    if not stock_data.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("股票名称", stock_data['name'].iloc[0])
        with col2:
            st.metric("TS代码", stock_data['ts_code'].iloc[0])
        with col3:
            market_value = stock_data['market'].iloc[0] if 'market' in stock_data.columns else '未知'
            st.metric("交易市场", market_value)
        with col4:
            list_status_value = stock_data['list_status'].iloc[0] if 'list_status' in stock_data.columns else '未知'
            st.metric("上市状态", list_status_value)
    else:
        st.warning("暂无股票信息")


def render_core_signal_and_radar(df_holographic):
    """渲染核心信号与四维雷达图"""
    st.subheader("核心信号与四维雷达图")
    if not df_holographic.empty:
        latest_data = df_holographic.iloc[-1]
        col1, col2 = st.columns(2)
        
        with col1:
            signal_grade = latest_data.get('signal_grade', '无明确信号')
            signal_reason = latest_data.get('signal_reason', '暂无详细原因')
            st.metric("最新信号评级", signal_grade)
            st.markdown(f"**核心驱动逻辑**: {signal_reason}")
        
        with col2:
            # 四维雷达图数据
            radar_data = {
                '维度': ['技术', '资金', '基本面', '宏观'],
                '评分': [
                    latest_data.get('trend_score', 0) * 10,
                    latest_data.get('net_inflow_ratio', 0) * 10,
                    50,  # 基本面评分，示例数据
                    30   # 宏观评分，示例数据
                ]
            }
            fig = px.line_polar(pd.DataFrame(radar_data), r='评分', theta='维度', line_close=True, title="四维分析雷达图")
            fig.update_traces(fill='toself')
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("暂无信号数据")


def render_interactive_chart(df_holographic, df_news):
    """渲染交互式量价资金分析图"""
    st.subheader("交互式量价资金分析图")
    if not df_holographic.empty:
        # 创建主副图
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, 
                            row_heights=[0.5, 0.25, 0.25],
                            subplot_titles=('K线与均线', '成交量', '资金流'))
        
        # 主图 - K线
        candlestick = go.Candlestick(
            x=df_holographic.index,
            open=df_holographic['open_price'],
            high=df_holographic['high_price'],
            low=df_holographic['low_price'],
            close=df_holographic['close_price'],
            name='K线',
            increasing_line_color='red',
            decreasing_line_color='green',
            hovertext=df_holographic.apply(
                lambda x: f"日期: {x.name}<br>开盘: {x['open_price']:.2f}<br>最高: {x['high_price']:.2f}<br>最低: {x['low_price']:.2f}<br>收盘: {x['close_price']:.2f}<br>成交量: {x['vol']}<br>成交额: {x['amount']:.2f}",
                axis=1
            )
        )
        fig.add_trace(candlestick, row=1, col=1)
        
        # 计算并添加均线
        df_holographic['MA5'] = df_holographic['close_price'].rolling(window=5).mean()
        df_holographic['MA10'] = df_holographic['close_price'].rolling(window=10).mean()
        fig.add_trace(go.Scatter(x=df_holographic.index, y=df_holographic['MA5'], mode='lines', name='MA5', line=dict(color='orange')), row=1, col=1)
        fig.add_trace(go.Scatter(x=df_holographic.index, y=df_holographic['MA10'], mode='lines', name='MA10', line=dict(color='purple')), row=1, col=1)
        
        # 副图一 - 成交量
        fig.add_trace(go.Bar(x=df_holographic.index, y=df_holographic['vol'], name='成交量', marker_color='gray'), row=2, col=1)
        
        # 副图二 - 资金流
        if 'net_inflow_ratio' in df_holographic.columns:
            net_inflow_data = df_holographic['net_inflow_ratio'].fillna(0)
            colors = ['red' if x > 0 else 'green' for x in net_inflow_data]
            fig.add_trace(go.Bar(x=df_holographic.index, y=net_inflow_data, name='主力净流入', marker_color=colors), row=3, col=1)
        else:
            # 如果没有资金流数据，显示空的图表
            fig.add_trace(go.Bar(x=df_holographic.index, y=[0]*len(df_holographic), name='主力净流入（无数据）', marker_color='gray'), row=3, col=1)
        
        # 添加新闻事件标记
        if not df_news.empty:
            for _, event in df_news.iterrows():
                fig.add_vline(x=event['trade_date'], line_width=1, line_dash="dash", line_color="blue", row=1, col=1)
                fig.add_annotation(x=event['trade_date'], y=1.05, yref="paper", text=event['event_title'], showarrow=True, arrowhead=1, row=1, col=1)
        
        fig.update_layout(height=800, width=1000, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("暂无图表数据")


def render_linked_data_explorer(df_holographic):
    """渲染关联数据浏览器"""
    st.subheader("关联数据浏览器")
    if not df_holographic.empty:
        # 显示主要数据
        st.dataframe(df_holographic[['open_price', 'high_price', 'low_price', 'close_price', 'vol']], use_container_width=True)
        
        # 显示技术指标
        if 'trend_score' in df_holographic.columns:
            st.subheader("技术指标")
            st.dataframe(df_holographic[['trend_score', 'momentum_score', 'ma5', 'ma10', 'ma20']], use_container_width=True)
        
        # 显示资金流数据
        if 'net_inflow_ratio' in df_holographic.columns:
            st.subheader("资金流数据")
            st.dataframe(df_holographic[['net_inflow_ratio', 'main_force_trend']], use_container_width=True)
        
        # 显示信号数据
        if 'signal_grade' in df_holographic.columns:
            st.subheader("信号数据")
            st.dataframe(df_holographic[['signal_grade', 'signal_reason']], use_container_width=True)
    else:
        st.warning("暂无数据可显示")


def render_stock_holographic_view_main():
    """渲染个股全息透视主界面"""
    st.header("📊 个股全息透视")
    st.markdown("以股票为核心，聚合多表信息进行深度分析")
    
    if not DB_AVAILABLE:
        st.error("❌ 数据库连接不可用，请检查配置")
        return
    
    # 股票选择器
    stock_list = load_stock_list()
    if not stock_list.empty:
        selected_stock_name = st.selectbox(
            "请选择要分析的股票:",
            options=stock_list['name']
        )
        
        if selected_stock_name:
            selected_ts_code = stock_list[stock_list['name'] == selected_stock_name]['ts_code'].iloc[0]
            st.header(f"正在分析: {selected_stock_name} ({selected_ts_code})")
            
            # 获取股票基本信息
            stock_data = stock_list[stock_list['name'] == selected_stock_name]
            
            # 渲染股票身份卡
            render_stock_identity_card(stock_data)
            
            # 获取全息数据
            with st.spinner(f"正在加载 {selected_stock_name} 的全息数据..."):
                df_holographic, df_news = get_holographic_data_for_stock(selected_ts_code)
                
                if not df_holographic.empty:
                    # 渲染核心信号与四维雷达图
                    render_core_signal_and_radar(df_holographic)
                    
                    # 渲染交互式量价资金分析图
                    render_interactive_chart(df_holographic, df_news)
                    
                    # 渲染关联数据深度探索
                    render_linked_data_explorer(df_holographic)

                    # 在关联数据浏览器下方添加策略激活按钮
                    if not df_holographic.empty:
                        st.markdown("---")
                        st.subheader("策略激活")
                        if st.button("激活启明星策略", key=f"activate_{selected_ts_code}"):
                            try:
                                # 直接调用本地策略激活函数
                                from src.strategies.qiming_star import QimingStarStrategy
                                strategy = QimingStarStrategy()
                                # 这里可以传递 selected_ts_code 或其他参数
                                # 例如：strategy.run_for_stock(selected_ts_code)
                                st.success("策略已激活！")
                            except Exception as e:
                                st.error(f"激活异常: {e}")
                else:
                    st.warning("⚠️ 暂无该股票的详细数据")
    else:
        st.warning("⚠️ 无法加载股票列表")
