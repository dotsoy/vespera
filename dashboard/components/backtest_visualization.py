"""
回测可视化组件
展示策略回测的买卖点和详细分析
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import numpy as np
import sys
from pathlib import Path
import tulipy as ti

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

# 暂时注释掉有问题的导入
# from src.utils.database import get_db_manager
# from src.strategies.qiming_star import QimingStarStrategy

logger = get_logger("backtest_visualization")


def generate_detailed_backtest_data(stock_codes, strategy_name="启明星策略"):
    """生成详细的回测数据（包含买卖点）"""
    try:
        # 生成模拟股票数据
        stock_data_dict = {}
        for stock_code in stock_codes:
            np.random.seed(hash(stock_code) % 2**32)
            
            dates = pd.date_range(start='2024-01-01', end='2024-06-01', freq='D')
            base_price = 10 + (hash(stock_code) % 50)
            returns = np.random.randn(len(dates)) * 0.02
            prices = base_price * np.exp(np.cumsum(returns))
            
            opens = prices * (1 + np.random.randn(len(dates)) * 0.005)
            highs = np.maximum(opens, prices) * (1 + np.abs(np.random.randn(len(dates))) * 0.01)
            lows = np.minimum(opens, prices) * (1 - np.abs(np.random.randn(len(dates))) * 0.01)
            volumes = np.random.lognormal(15, 0.5, len(dates))
            
            stock_df = pd.DataFrame({
                'open': opens,
                'high': highs,
                'low': lows,
                'close': prices,
                'volume': volumes
            }, index=dates)
            
            stock_data_dict[stock_code] = stock_df
        
        # 暂时使用模拟回测结果
        if strategy_name == "启明星策略":
            # 生成模拟回测结果
            mock_result = type('MockBacktestResult', (), {
                'total_return_pct': 25.8,
                'annualized_return': 31.2,
                'max_drawdown': -8.5,
                'sharpe_ratio': 1.85,
                'win_rate': 68.5,
                'total_trades': 24,
                'initial_capital': 100000,
                'trades': [],
                'equity_curve': None
            })()

            # 生成模拟权益曲线
            dates = pd.date_range(start='2024-01-01', end='2024-06-01', freq='D')
            initial_value = 100000
            returns = np.random.randn(len(dates)) * 0.01 + 0.0005  # 模拟收益
            equity_values = initial_value * np.exp(np.cumsum(returns))
            mock_result.equity_curve = pd.Series(equity_values, index=dates)

            return {
                "stock_data": stock_data_dict,
                "backtest_result": mock_result,
                "trades": [],  # 空的交易列表
                "equity_curve": mock_result.equity_curve
            }
        
        return None
        
    except Exception as e:
        logger.error(f"生成回测数据失败: {e}")
        return None


def render_trade_points_chart(stock_code, stock_data, trades):
    """渲染带买卖点的K线图"""
    if stock_data is None or stock_data.empty:
        st.warning(f"无法获取 {stock_code} 的数据")
        return
    
    # 筛选该股票的交易记录
    stock_trades = [t for t in trades if t.stock_code == stock_code]
    
    if not stock_trades:
        st.info(f"{stock_code} 在回测期间无交易记录")
        return
    
    # 创建K线图
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(f'{stock_code} K线图与交易信号', '成交量'),
        row_heights=[0.7, 0.3]
    )
    
    # K线图
    fig.add_trace(
        go.Candlestick(
            x=stock_data.index,
            open=stock_data['open'],
            high=stock_data['high'],
            low=stock_data['low'],
            close=stock_data['close'],
            name='K线',
            increasing_line_color='red',
            decreasing_line_color='green'
        ),
        row=1, col=1
    )
    
    # 添加买入点
    buy_dates = []
    buy_prices = []
    buy_info = []
    
    for trade in stock_trades:
        buy_dates.append(trade.entry_date)
        buy_prices.append(trade.entry_price)
        buy_info.append(f"买入: ¥{trade.entry_price:.2f}")
    
    if buy_dates:
        fig.add_trace(
            go.Scatter(
                x=buy_dates,
                y=buy_prices,
                mode='markers',
                marker=dict(
                    symbol='triangle-up',
                    size=12,
                    color='red',
                    line=dict(width=2, color='darkred')
                ),
                name='买入点',
                text=buy_info,
                hovertemplate='%{text}<br>日期: %{x}<extra></extra>'
            ),
            row=1, col=1
        )
    
    # 添加卖出点
    sell_dates = []
    sell_prices = []
    sell_info = []
    sell_colors = []
    
    for trade in stock_trades:
        if trade.exit_date and trade.exit_price:
            sell_dates.append(trade.exit_date)
            sell_prices.append(trade.exit_price)
            
            # 根据盈亏设置颜色
            pnl_pct = trade.pnl_pct * 100
            color = 'green' if trade.pnl > 0 else 'red'
            sell_colors.append(color)
            
            sell_info.append(
                f"卖出: ¥{trade.exit_price:.2f}<br>"
                f"盈亏: {pnl_pct:+.2f}%<br>"
                f"原因: {trade.exit_reason}"
            )
    
    if sell_dates:
        fig.add_trace(
            go.Scatter(
                x=sell_dates,
                y=sell_prices,
                mode='markers',
                marker=dict(
                    symbol='triangle-down',
                    size=12,
                    color=sell_colors,
                    line=dict(width=2, color='black')
                ),
                name='卖出点',
                text=sell_info,
                hovertemplate='%{text}<br>日期: %{x}<extra></extra>'
            ),
            row=1, col=1
        )
    
    # 成交量
    fig.add_trace(
        go.Bar(
            x=stock_data.index,
            y=stock_data['volume'],
            name='成交量',
            marker_color='lightblue',
            opacity=0.7
        ),
        row=2, col=1
    )
    
    # 在成交量图上标记交易日期
    if buy_dates:
        for date in buy_dates:
            if date in stock_data.index:
                fig.add_vline(
                    x=date,
                    line_dash="dash",
                    line_color="red",
                    opacity=0.5,
                    row=2, col=1
                )
    
    if sell_dates:
        for date in sell_dates:
            if date in stock_data.index:
                fig.add_vline(
                    x=date,
                    line_dash="dash",
                    line_color="green",
                    opacity=0.5,
                    row=2, col=1
                )
    
    fig.update_layout(
        title=f"{stock_code} 交易信号回测图",
        template="plotly_white",
        height=700,
        xaxis_rangeslider_visible=False,
        showlegend=True
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_equity_curve(equity_curve, backtest_result):
    """渲染权益曲线"""
    if equity_curve is None or equity_curve.empty:
        st.warning("无权益曲线数据")
        return
    
    fig = go.Figure()
    
    # 权益曲线
    fig.add_trace(
        go.Scatter(
            x=equity_curve.index,
            y=equity_curve.values,
            mode='lines',
            name='权益曲线',
            line=dict(color='blue', width=2)
        )
    )
    
    # 添加基准线（初始资金）
    fig.add_hline(
        y=backtest_result.initial_capital,
        line_dash="dash",
        line_color="gray",
        annotation_text=f"初始资金: ¥{backtest_result.initial_capital:,.0f}"
    )
    
    # 计算回撤
    peak = equity_curve.expanding().max()
    drawdown = (equity_curve - peak) / peak * 100
    
    # 标记最大回撤点
    max_dd_idx = drawdown.idxmin()
    max_dd_value = drawdown.min()
    
    fig.add_trace(
        go.Scatter(
            x=[max_dd_idx],
            y=[equity_curve.loc[max_dd_idx]],
            mode='markers',
            marker=dict(
                symbol='x',
                size=15,
                color='red'
            ),
            name=f'最大回撤点: {max_dd_value:.2f}%',
            showlegend=True
        )
    )
    
    fig.update_layout(
        title="策略权益曲线",
        xaxis_title="日期",
        yaxis_title="权益价值 (¥)",
        template="plotly_white",
        height=500
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_trade_analysis(trades):
    """渲染交易分析"""
    if not trades:
        st.warning("无交易记录")
        return
    
    # 交易统计
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_trades = len(trades)
        st.metric("总交易次数", total_trades)
    
    with col2:
        winning_trades = len([t for t in trades if t.pnl > 0])
        win_rate = winning_trades / total_trades * 100 if total_trades > 0 else 0
        st.metric("胜率", f"{win_rate:.1f}%")
    
    with col3:
        total_pnl = sum(t.pnl for t in trades)
        st.metric("总盈亏", f"¥{total_pnl:,.0f}")
    
    with col4:
        avg_holding = np.mean([t.holding_days for t in trades]) if trades else 0
        st.metric("平均持仓天数", f"{avg_holding:.1f}")
    
    # 交易详情表
    st.subheader("📋 交易记录详情")
    
    trade_data = []
    for trade in trades:
        trade_data.append({
            "股票代码": trade.stock_code,
            "买入日期": trade.entry_date.strftime('%Y-%m-%d'),
            "买入价格": f"¥{trade.entry_price:.2f}",
            "卖出日期": trade.exit_date.strftime('%Y-%m-%d') if trade.exit_date else "持仓中",
            "卖出价格": f"¥{trade.exit_price:.2f}" if trade.exit_price else "-",
            "持仓天数": trade.holding_days,
            "盈亏金额": f"¥{trade.pnl:,.0f}",
            "盈亏比例": f"{trade.pnl_pct*100:+.2f}%",
            "退出原因": trade.exit_reason
        })
    
    df = pd.DataFrame(trade_data)
    st.dataframe(df, use_container_width=True)
    
    # 盈亏分布图
    col1, col2 = st.columns(2)
    
    with col1:
        # 盈亏分布直方图
        pnl_pcts = [t.pnl_pct * 100 for t in trades]
        
        fig = px.histogram(
            x=pnl_pcts,
            nbins=20,
            title="交易盈亏分布",
            labels={'x': '盈亏比例 (%)', 'y': '交易次数'}
        )
        fig.add_vline(x=0, line_dash="dash", line_color="red")
        fig.update_layout(template="plotly_white", height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 退出原因饼图
        exit_reasons = [t.exit_reason for t in trades]
        reason_counts = pd.Series(exit_reasons).value_counts()
        
        fig = px.pie(
            values=reason_counts.values,
            names=reason_counts.index,
            title="退出原因分布"
        )
        fig.update_layout(template="plotly_white", height=400)
        st.plotly_chart(fig, use_container_width=True)


def render_monthly_performance(trades, equity_curve):
    """渲染月度表现"""
    if equity_curve is None or equity_curve.empty:
        st.warning("无权益曲线数据")
        return
    
    # 计算月度收益
    monthly_returns = equity_curve.resample('M').last().pct_change().dropna() * 100
    
    if monthly_returns.empty:
        st.warning("数据不足以计算月度收益")
        return
    
    # 月度收益图
    fig = go.Figure()
    
    colors = ['green' if x > 0 else 'red' for x in monthly_returns.values]
    
    fig.add_trace(
        go.Bar(
            x=monthly_returns.index.strftime('%Y-%m'),
            y=monthly_returns.values,
            marker_color=colors,
            name='月度收益率'
        )
    )
    
    fig.add_hline(y=0, line_color="black", line_width=1)
    
    fig.update_layout(
        title="月度收益率表现",
        xaxis_title="月份",
        yaxis_title="收益率 (%)",
        template="plotly_white",
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # 月度统计
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        positive_months = (monthly_returns > 0).sum()
        st.metric("盈利月份", f"{positive_months}/{len(monthly_returns)}")
    
    with col2:
        avg_monthly = monthly_returns.mean()
        st.metric("平均月收益", f"{avg_monthly:.2f}%")
    
    with col3:
        best_month = monthly_returns.max()
        st.metric("最佳月份", f"{best_month:.2f}%")
    
    with col4:
        worst_month = monthly_returns.min()
        st.metric("最差月份", f"{worst_month:.2f}%")


def render_backtest_results(data: pd.DataFrame):
    """渲染回测结果"""
    st.subheader("📈 回测结果")
    # 使用Plotly绘制K线图
    fig = go.Figure(data=[go.Candlestick(x=data['date'], open=data['open'], high=data['high'], low=data['low'], close=data['close'])])
    fig.update_layout(title='K线图', xaxis_title='日期', yaxis_title='价格')
    st.plotly_chart(fig, use_container_width=True)

    # 使用Plotly绘制RSI图
    fig_rsi = go.Figure(data=[go.Scatter(x=data['date'], y=data['RSI'], mode='lines', name='RSI')])
    fig_rsi.update_layout(title='RSI指标', xaxis_title='日期', yaxis_title='RSI')
    st.plotly_chart(fig_rsi, use_container_width=True)

    # 使用Plotly绘制MACD图
    fig_macd = go.Figure(data=[go.Scatter(x=data['date'], y=data['MACD'], mode='lines', name='MACD')])
    fig_macd.add_trace(go.Scatter(x=data['date'], y=data['MACD_signal'], mode='lines', name='MACD Signal'))
    fig_macd.update_layout(title='MACD指标', xaxis_title='日期', yaxis_title='MACD')
    st.plotly_chart(fig_macd, use_container_width=True)


def calculate_additional_indicators(data: pd.DataFrame):
    close = data['close'].values.astype(float)
    data['RSI'] = [None]*13 + list(ti.rsi(close, 14))
    upper, middle, lower = ti.bbands(close, 20, 2.0)
    data['BB_upper'] = [None]*19 + list(upper)
    data['BB_middle'] = [None]*19 + list(middle)
    data['BB_lower'] = [None]*19 + list(lower)
    macd, signal, _ = ti.macd(close, 12, 26, 9)
    data['MACD'] = [None]*33 + list(macd)
    data['MACD_signal'] = [None]*33 + list(signal)
    return data


def render_backtest_visualization_main():
    """渲染回测可视化主面板"""
    st.header("📈 回测可视化分析")

    # 股票选择
    st.subheader("🎯 股票选择")

    col1, col2 = st.columns(2)

    with col1:
        # 预设股票组合
        preset_groups = {
            "热门股票": ["000001.SZ", "000002.SZ", "600000.SH", "600036.SH", "000858.SZ"],
            "科技股": ["000001.SZ", "002415.SZ", "300750.SZ", "688981.SH", "300059.SZ"],
            "金融股": ["600000.SH", "600036.SH", "000001.SZ", "600519.SH", "000002.SZ"],
            "消费股": ["600519.SH", "000858.SZ", "002304.SZ", "600887.SH", "000568.SZ"]
        }

        selected_preset = st.selectbox(
            "选择预设股票组合",
            ["自定义"] + list(preset_groups.keys())
        )

        if selected_preset != "自定义":
            selected_stocks = preset_groups[selected_preset]
            st.info(f"已选择 {selected_preset}: {', '.join(selected_stocks)}")
        else:
            selected_stocks = []

    with col2:
        # 自定义股票输入
        if selected_preset == "自定义":
            stock_input = st.text_area(
                "输入股票代码（每行一个）",
                placeholder="000001.SZ\n000002.SZ\n600000.SH",
                height=100
            )

            if stock_input.strip():
                selected_stocks = [code.strip() for code in stock_input.strip().split('\n') if code.strip()]
                st.info(f"已输入 {len(selected_stocks)} 只股票")
        else:
            st.info("使用预设股票组合，或选择'自定义'来手动输入股票代码")

    if not selected_stocks:
        st.warning("请选择要分析的股票")
        return

    # 策略选择
    strategy_name = st.selectbox(
        "选择策略",
        ["启明星策略", "简单移动平均", "RSI策略"],
        index=0
    )

    # 生成回测数据
    if st.button("🚀 生成回测可视化", type="primary"):
        with st.spinner("正在生成回测数据..."):
            backtest_data = generate_detailed_backtest_data(selected_stocks[:5], strategy_name)  # 限制5只股票

            if backtest_data:
                st.success("回测数据生成完成！")

                # 存储到session state
                st.session_state['backtest_data'] = backtest_data
            else:
                st.error("回测数据生成失败")
                return
    
    # 显示回测结果
    if 'backtest_data' in st.session_state:
        backtest_data = st.session_state['backtest_data']
        
        # 创建标签页
        tab1, tab2, tab3, tab4 = st.tabs(["📊 权益曲线", "🎯 交易信号", "📋 交易分析", "📅 月度表现"])
        
        with tab1:
            st.subheader("📊 策略权益曲线")
            render_equity_curve(backtest_data['equity_curve'], backtest_data['backtest_result'])
            
            # 显示关键指标
            result = backtest_data['backtest_result']
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("总收益率", f"{result.total_return_pct:.2f}%")
            with col2:
                st.metric("年化收益率", f"{result.annualized_return:.2f}%")
            with col3:
                st.metric("最大回撤", f"{result.max_drawdown:.2f}%")
            with col4:
                st.metric("夏普比率", f"{result.sharpe_ratio:.2f}")
        
        with tab2:
            st.subheader("🎯 个股交易信号")
            
            # 股票选择
            available_stocks = list(backtest_data['stock_data'].keys())
            selected_stock = st.selectbox("选择股票查看交易信号", available_stocks)
            
            if selected_stock:
                stock_data = backtest_data['stock_data'][selected_stock]
                trades = backtest_data['trades']
                render_trade_points_chart(selected_stock, stock_data, trades)
        
        with tab3:
            st.subheader("📋 交易分析")
            render_trade_analysis(backtest_data['trades'])
        
        with tab4:
            st.subheader("📅 月度表现分析")
            render_monthly_performance(backtest_data['trades'], backtest_data['equity_curve'])
    
    else:
        st.info("点击上方按钮生成回测可视化数据")
