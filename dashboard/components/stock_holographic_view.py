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
from src.utils.backup_manager import get_backup_manager

logger = get_logger("stock_holographic_view")

# 检查数据库可用性
try:
    DB_AVAILABLE = True
    db_manager = get_db_manager()
    db_manager.test_connections()
except ImportError as e:
    logger.warning(f"数据库模块导入失败: {e}")
    DB_AVAILABLE = False


def load_stock_list():
    """加载股票列表"""
    if not DB_AVAILABLE:
        return pd.DataFrame()
    
    try:
        query = "SELECT ts_code, name FROM stock_basic ORDER BY name"
        return db_manager.execute_postgres_query(query)
    except Exception as e:
        logger.error(f"加载股票列表失败: {e}")
        return pd.DataFrame()


def get_holographic_data_for_stock(ts_code: str):
    """获取指定股票的所有关联数据，并融合成一张宽表"""
    logger.info(f"get_holographic_data_for_stock: ts_code={ts_code}")
    if not DB_AVAILABLE:
        return pd.DataFrame(), pd.DataFrame()
    
    try:
        db_manager = get_db_manager()
        
        # 计算近一年的日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

        # 1. 优先查本地ClickHouse日线数据
        try:
            query = f"SELECT * FROM daily_quotes WHERE ts_code = '{ts_code}' AND trade_date >= '{start_date}' AND trade_date <= '{end_date}' ORDER BY trade_date"
            df_quotes = db_manager.execute_clickhouse_query(query)
            if df_quotes is not None and not df_quotes.empty:
                logger.info(f"本地ClickHouse获取到 {ts_code} 的日线数据，记录数: {len(df_quotes)}")
            else:
                df_quotes = None
        except Exception as e:
            logger.warning(f"本地ClickHouse查询失败: {e}")
            df_quotes = None

        # 2. 本地无数据时，尝试外部数据源
        if df_quotes is None or df_quotes.empty:
            logger.info(f"本地无数据，尝试外部数据源拉取 {ts_code} 日线数据")
            from src.data_sources.data_source_factory import get_data_service
            from src.data_sources.base_data_source import DataRequest, DataType
            data_service = get_data_service()
            df_quotes = data_service.get_data(
                request=DataRequest(
                    data_type=DataType.STOCK_DAILY,
                    symbol=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
            )
            if df_quotes is not None and not df_quotes.empty:
                logger.info(f"外部数据源获取到 {ts_code} 的日线数据，记录数: {len(df_quotes)}")
                # 存入ClickHouse
                try:
                    db_manager.insert_dataframe_to_clickhouse(df_quotes, 'daily_quotes')
                    logger.info(f"已更新 {ts_code} 的日线数据到ClickHouse")
                except Exception as e:
                    logger.warning(f"存储到ClickHouse失败: {e}")
            else:
                logger.warning(f"未能获取 {ts_code} 的日线数据")
                return pd.DataFrame(), pd.DataFrame()

        # 确保日期格式正确
        df_quotes['trade_date'] = pd.to_datetime(df_quotes['trade_date'])

        # 技术分析、资金流、信号等数据照常查本地
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
            WHERE ts_code = '{ts_code}' 
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


def generate_strategy_recommendation(df_holographic: pd.DataFrame, stock_name: str, ts_code: str) -> dict:
    """根据全息数据生成策略建议"""
    if df_holographic.empty:
        return {
            "recommendation": "数据不足",
            "confidence": 0,
            "reason": "无法获取足够的历史数据进行分析",
            "risk_level": "未知",
            "actions": []
        }
    
    try:
        # 获取最新数据
        latest_data = df_holographic.iloc[-1] if len(df_holographic) > 0 else None
        if latest_data is None:
            return {"recommendation": "数据不足", "confidence": 0, "reason": "无最新数据", "risk_level": "未知", "actions": []}
        
        # 获取最近5个交易日的数据
        recent_data = df_holographic.tail(5)
        
        # 分析技术指标
        technical_score = 0
        technical_reasons = []
        
        # RSI分析
        if 'trend_score' in latest_data and pd.notna(latest_data['trend_score']):
            rsi = latest_data['trend_score']
            if rsi < 30:
                technical_score += 2
                technical_reasons.append(f"RSI超卖({rsi:.1f})，存在反弹机会")
            elif rsi > 70:
                technical_score -= 2
                technical_reasons.append(f"RSI超买({rsi:.1f})，注意回调风险")
            elif 40 <= rsi <= 60:
                technical_score += 1
                technical_reasons.append(f"RSI中性({rsi:.1f})，趋势稳定")
        
        # MACD分析
        if 'momentum_score' in latest_data and pd.notna(latest_data['momentum_score']):
            macd = latest_data['momentum_score']
            if macd > 0:
                technical_score += 1
                technical_reasons.append("MACD为正，动量向上")
            else:
                technical_score -= 1
                technical_reasons.append("MACD为负，动量向下")
        
        # 均线分析
        if all(col in latest_data for col in ['ma5', 'ma10', 'ma20']) and all(pd.notna(latest_data[col]) for col in ['ma5', 'ma10', 'ma20']):
            ma5, ma10, ma20 = latest_data['ma5'], latest_data['ma10'], latest_data['ma20']
            if ma5 > ma10 > ma20:
                technical_score += 2
                technical_reasons.append("均线多头排列，趋势向上")
            elif ma5 < ma10 < ma20:
                technical_score -= 2
                technical_reasons.append("均线空头排列，趋势向下")
        
        # 分析资金流
        capital_score = 0
        capital_reasons = []
        
        if 'net_inflow_ratio' in latest_data and pd.notna(latest_data['net_inflow_ratio']):
            net_ratio = latest_data['net_inflow_ratio']
            if net_ratio > 0.1:
                capital_score += 2
                capital_reasons.append(f"主力资金净流入({net_ratio:.2%})，看好")
            elif net_ratio < -0.1:
                capital_score -= 2
                capital_reasons.append(f"主力资金净流出({net_ratio:.2%})，谨慎")
            else:
                capital_score += 0
                capital_reasons.append("资金流向中性")
        
        # 分析价格趋势
        price_score = 0
        price_reasons = []
        
        if len(recent_data) >= 3:
            # 计算最近3日涨跌幅
            price_changes = []
            for i in range(1, min(4, len(recent_data))):
                if 'close_price' in recent_data.columns and 'open_price' in recent_data.columns:
                    close = recent_data.iloc[-i]['close_price']
                    open_price = recent_data.iloc[-i]['open_price']
                    if pd.notna(close) and pd.notna(open_price) and open_price > 0:
                        change = (close - open_price) / open_price
                        price_changes.append(change)
            
            if price_changes:
                avg_change = sum(price_changes) / len(price_changes)
                if avg_change > 0.02:  # 平均涨幅超过2%
                    price_score += 1
                    price_reasons.append(f"近期平均涨幅{avg_change:.2%}，表现强势")
                elif avg_change < -0.02:  # 平均跌幅超过2%
                    price_score -= 1
                    price_reasons.append(f"近期平均跌幅{abs(avg_change):.2%}，表现弱势")
        
        # 综合评分
        total_score = technical_score + capital_score + price_score
        
        # 计算数据质量分数（0-100）
        data_quality_score = 0
        data_indicators = 0
        total_indicators = 0
        
        # 检查技术指标数据质量
        if 'trend_score' in latest_data and pd.notna(latest_data['trend_score']):
            data_quality_score += 20
            data_indicators += 1
        total_indicators += 1
        
        if 'momentum_score' in latest_data and pd.notna(latest_data['momentum_score']):
            data_quality_score += 20
            data_indicators += 1
        total_indicators += 1
        
        if all(col in latest_data for col in ['ma5', 'ma10', 'ma20']) and all(pd.notna(latest_data[col]) for col in ['ma5', 'ma10', 'ma20']):
            data_quality_score += 20
            data_indicators += 1
        total_indicators += 1
        
        # 检查资金流数据质量
        if 'net_inflow_ratio' in latest_data and pd.notna(latest_data['net_inflow_ratio']):
            data_quality_score += 20
            data_indicators += 1
        total_indicators += 1
        
        # 检查价格数据质量
        if len(recent_data) >= 3:
            data_quality_score += 20
            data_indicators += 1
        total_indicators += 1
        
        # 计算指标一致性分数（0-100）
        consistency_score = 0
        if data_indicators >= 3:  # 至少3个指标才有意义
            # 检查指标方向一致性
            positive_indicators = 0
            negative_indicators = 0
            
            if technical_score > 0:
                positive_indicators += 1
            elif technical_score < 0:
                negative_indicators += 1
                
            if capital_score > 0:
                positive_indicators += 1
            elif capital_score < 0:
                negative_indicators += 1
                
            if price_score > 0:
                positive_indicators += 1
            elif price_score < 0:
                negative_indicators += 1
            
            # 计算一致性
            total_indicators_with_direction = positive_indicators + negative_indicators
            if total_indicators_with_direction > 0:
                max_direction = max(positive_indicators, negative_indicators)
                consistency_score = (max_direction / total_indicators_with_direction) * 100
        
        # 基于总分和建议类型计算基础置信度
        if total_score >= 3:
            recommendation = "买入"
            base_confidence = 75 + (total_score - 3) * 3  # 75%基础 + 每分3%
            risk_level = "中等"
            actions = [
                "分批建仓：首次建仓30%，回调时加仓20%",
                "止损位设置：以近期低点下浮3-5%作为止损",
                "目标位设置：以近期高点或阻力位作为止盈目标",
                "关注量能：确认放量突破时加仓，缩量回调时观望",
                "时间窗口：建议在开盘30分钟内或尾盘15分钟内操作"
            ]
        elif total_score >= 1:
            recommendation = "持有"
            base_confidence = 70 + (total_score - 1) * 5  # 70%基础 + 每分5%
            risk_level = "较低"
            actions = [
                "继续持有：当前趋势向好，保持现有仓位",
                "动态止盈：设置移动止盈，保护已有利润",
                "关注技术指标：RSI超过70时考虑部分止盈",
                "量能监控：放量上涨时持有，缩量回调时减仓",
                "定期评估：每周评估一次持仓，根据市场变化调整"
            ]
        elif total_score >= -1:
            recommendation = "观望"
            base_confidence = 65 + (total_score + 1) * 5  # 65%基础 + 每分5%
            risk_level = "中等"
            actions = [
                "等待明确信号：等待技术指标出现明确买入信号",
                "关注重要支撑位：以MA20或前期低点作为支撑参考",
                "控制仓位：如要试探性建仓，仓位不超过10%",
                "设置严格止损：试探性建仓的止损位不超过3%",
                "观察市场情绪：关注大盘走势和板块轮动情况"
            ]
        elif total_score >= -3:
            recommendation = "减仓"
            base_confidence = 70 + (total_score + 3) * 3  # 70%基础 + 每分3%
            risk_level = "较高"
            actions = [
                "分批减仓：先减仓50%，观察后续走势再决定",
                "设置严格止损：以MA10或近期支撑位作为止损",
                "等待企稳信号：等待RSI回到30以下或MACD金叉",
                "关注成交量：放量下跌时加速减仓，缩量时观望",
                "资金管理：减仓资金暂时观望，等待更好机会"
            ]
        else:
            recommendation = "卖出"
            base_confidence = 75 + (total_score + 5) * 2  # 75%基础 + 每分2%
            risk_level = "高"
            actions = [
                "立即卖出：建议在开盘或反弹时尽快卖出",
                "分批卖出：如仓位较重，可分2-3次卖出",
                "严格止损：以MA5或近期低点作为最后止损位",
                "等待底部确认：等待技术指标出现超卖信号",
                "关注基本面：检查是否有重大利空消息影响"
            ]
        
        # 根据数据质量调整操作建议
        if data_quality_score < 60:
            actions.append("⚠️ 数据质量较低，建议谨慎操作，等待更多数据确认")
        
        if consistency_score < 50:
            actions.append("⚠️ 指标信号矛盾，建议降低操作频率，等待信号明确")
        
        # 添加市场环境建议
        if 'pct_chg' in latest_data and pd.notna(latest_data['pct_chg']):
            pct_chg = latest_data['pct_chg']
            if abs(pct_chg) > 5:
                actions.append(f"📈 今日波动较大({pct_chg:+.2f}%)，建议控制仓位，避免追涨杀跌")
        
        # 添加时间建议
        current_hour = datetime.now().hour
        if 9 <= current_hour <= 11:
            actions.append("⏰ 当前为早盘时段，建议关注开盘30分钟内的走势确认")
        elif 13 <= current_hour <= 15:
            actions.append("⏰ 当前为午盘时段，建议关注尾盘15分钟的资金流向")
        else:
            actions.append("⏰ 当前为非交易时段，建议在下一个交易日开盘时执行操作")
        
        # 综合计算最终置信度
        # 权重：数据质量30% + 指标一致性30% + 基础置信度40%
        final_confidence = (
            data_quality_score * 0.3 + 
            consistency_score * 0.3 + 
            base_confidence * 0.4
        )
        
        # 确保置信度在合理范围内
        confidence = max(30, min(95, int(final_confidence)))
        
        # 合并原因
        all_reasons = technical_reasons + capital_reasons + price_reasons
        reason_text = "；".join(all_reasons) if all_reasons else "综合技术面、资金面分析"
        
        return {
            "recommendation": recommendation,
            "confidence": confidence,
            "reason": reason_text,
            "risk_level": risk_level,
            "actions": actions,
            "scores": {
                "technical": technical_score,
                "capital": capital_score,
                "price": price_score,
                "total": total_score
            },
            "confidence_details": {
                "data_quality": data_quality_score,
                "consistency": consistency_score,
                "base_confidence": base_confidence,
                "data_indicators": data_indicators,
                "total_indicators": total_indicators
            }
        }
        
    except Exception as e:
        logger.error(f"生成策略建议失败: {e}")
        return {
            "recommendation": "分析失败",
            "confidence": 0,
            "reason": f"分析过程中出现错误: {str(e)}",
            "risk_level": "未知",
            "actions": ["请检查数据完整性"]
        }


def render_strategy_recommendation(df_holographic: pd.DataFrame, stock_name: str, ts_code: str):
    """渲染策略建议模块"""
    st.markdown("---")
    st.header("🎯 投资策略建议")
    
    # 生成策略建议
    strategy = generate_strategy_recommendation(df_holographic, stock_name, ts_code)
    
    # 创建三列布局
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        # 主要建议
        st.subheader("📊 主要建议")
        recommendation_color = {
            "买入": "green",
            "持有": "blue", 
            "观望": "orange",
            "减仓": "red",
            "卖出": "darkred"
        }.get(strategy["recommendation"], "gray")
        
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; border: 2px solid {recommendation_color}; border-radius: 10px;">
            <h2 style="color: {recommendation_color}; margin: 0;">{strategy["recommendation"]}</h2>
            <p style="margin: 10px 0 0 0; font-size: 14px;">置信度: {strategy["confidence"]}%</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # 风险等级
        st.subheader("⚠️ 风险等级")
        risk_color = {
            "低": "green",
            "较低": "lightgreen", 
            "中等": "orange",
            "较高": "red",
            "高": "darkred"
        }.get(strategy["risk_level"], "gray")
        
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; border: 2px solid {risk_color}; border-radius: 10px;">
            <h3 style="color: {risk_color}; margin: 0;">{strategy["risk_level"]}</h3>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        # 评分详情
        st.subheader("📈 评分详情")
        scores = strategy.get("scores", {})
        if scores:
            st.write(f"技术面: {scores.get('technical', 0):+d}")
            st.write(f"资金面: {scores.get('capital', 0):+d}")
            st.write(f"价格面: {scores.get('price', 0):+d}")
            st.write(f"**总分: {scores.get('total', 0):+d}**")
        
        # 置信度详情
        st.subheader("🎯 置信度详情")
        confidence_details = strategy.get("confidence_details", {})
        if confidence_details:
            st.write(f"数据质量: {confidence_details.get('data_quality', 0)}%")
            st.write(f"指标一致性: {confidence_details.get('consistency', 0):.0f}%")
            st.write(f"基础置信度: {confidence_details.get('base_confidence', 0):.0f}%")
            st.write(f"数据指标: {confidence_details.get('data_indicators', 0)}/{confidence_details.get('total_indicators', 0)}")
    
    # 分析理由
    st.subheader("🔍 分析理由")
    st.info(strategy["reason"])
    
    # 操作建议
    st.subheader("💡 操作建议")
    
    # 将操作建议分类显示
    core_actions = []
    risk_actions = []
    timing_actions = []
    warning_actions = []
    
    for action in strategy["actions"]:
        if "⚠️" in action:
            warning_actions.append(action)
        elif "⏰" in action:
            timing_actions.append(action)
        elif any(keyword in action for keyword in ["止损", "止盈", "风险", "仓位"]):
            risk_actions.append(action)
        else:
            core_actions.append(action)
    
    # 核心操作建议
    if core_actions:
        st.write("**🎯 核心操作：**")
        for i, action in enumerate(core_actions, 1):
            st.write(f"  {i}. {action}")
    
    # 风险控制建议
    if risk_actions:
        st.write("**🛡️ 风险控制：**")
        for i, action in enumerate(risk_actions, 1):
            st.write(f"  {i}. {action}")
    
    # 时间建议
    if timing_actions:
        st.write("**⏰ 时间建议：**")
        for action in timing_actions:
            st.write(f"  • {action}")
    
    # 风险提示
    if warning_actions:
        st.write("**⚠️ 风险提示：**")
        for action in warning_actions:
            st.write(f"  • {action}")
    
    # 如果没有分类的建议，按原格式显示
    if not any([core_actions, risk_actions, timing_actions, warning_actions]):
        for i, action in enumerate(strategy["actions"], 1):
            st.write(f"{i}. {action}")
    
    # 免责声明
    st.markdown("---")
    st.caption("⚠️ **免责声明**: 本策略建议仅供参考，不构成投资建议。投资有风险，入市需谨慎。请结合自身风险承受能力和投资目标做出决策。")


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
        # 初始化session_state
        if 'last_selected_stock' not in st.session_state:
            st.session_state.last_selected_stock = None
        
        selected_stock_name = st.selectbox(
            "请选择要分析的股票:",
            options=stock_list['name'],
            key="stock_selector"
        )
        
        # 保存当前选择的股票
        if selected_stock_name != st.session_state.last_selected_stock:
            st.session_state.last_selected_stock = selected_stock_name
        
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
                    
                    # 渲染策略建议
                    render_strategy_recommendation(df_holographic, selected_stock_name, selected_ts_code)
                else:
                    st.warning("⚠️ 暂无该股票的详细数据")
    else:
        st.warning("⚠️ 无法加载股票列表")
