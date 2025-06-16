"""
策略分析组件
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

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

# 暂时注释掉有问题的导入
# from src.utils.database import get_db_manager
# from src.strategies.qiming_star import QimingStarStrategy

logger = get_logger("strategy_analysis")


def get_stock_data_from_db(ts_codes, days=120):
    """从数据库获取股票数据"""
    try:
        # 暂时返回空字典，因为数据库连接有问题
        # TODO: 修复数据库连接后替换为真实数据获取
        logger.warning("数据库连接暂不可用，将使用模拟数据")
        return {}

    except Exception as e:
        logger.error(f"从数据库获取股票数据失败: {e}")
        return {}


def generate_mock_stock_data(ts_codes, days=120):
    """生成模拟股票数据（当数据库无数据时使用）"""
    stock_data_dict = {}
    
    for ts_code in ts_codes:
        np.random.seed(hash(ts_code) % 2**32)
        
        dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
        
        # 生成价格数据
        base_price = 10 + (hash(ts_code) % 50)
        returns = np.random.randn(days) * 0.02
        prices = base_price * np.exp(np.cumsum(returns))
        
        # 生成OHLC数据
        opens = prices * (1 + np.random.randn(days) * 0.005)
        highs = np.maximum(opens, prices) * (1 + np.abs(np.random.randn(days)) * 0.01)
        lows = np.minimum(opens, prices) * (1 - np.abs(np.random.randn(days)) * 0.01)
        volumes = np.random.lognormal(15, 0.5, days)
        
        stock_df = pd.DataFrame({
            'open': opens,
            'high': highs,
            'low': lows,
            'close': prices,
            'volume': volumes
        }, index=dates)
        
        stock_data_dict[ts_code] = stock_df
    
    return stock_data_dict


def render_strategy_selection():
    """渲染策略选择"""
    st.header("🎯 策略分析配置")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 策略选择")
        
        strategy_name = st.selectbox(
            "选择策略",
            ["启明星策略", "简单移动平均", "RSI策略", "买入持有"],
            index=0
        )
        
        # 策略参数配置
        if strategy_name == "启明星策略":
            st.write("**四维分析权重配置**")
            capital_weight = st.slider("资金面权重", 0.1, 0.8, 0.45, 0.05)
            technical_weight = st.slider("技术面权重", 0.1, 0.6, 0.35, 0.05)
            rs_weight = st.slider("相对强度权重", 0.05, 0.3, 0.15, 0.05)
            catalyst_weight = st.slider("催化剂权重", 0.01, 0.2, 0.05, 0.01)
            
            # 权重归一化检查
            total_weight = capital_weight + technical_weight + rs_weight + catalyst_weight
            if abs(total_weight - 1.0) > 0.01:
                st.warning(f"权重总和: {total_weight:.3f} (应为1.0)")
            
            strategy_params = {
                "weights": {
                    "capital": capital_weight,
                    "technical": technical_weight,
                    "relative_strength": rs_weight,
                    "catalyst": catalyst_weight
                }
            }
        else:
            strategy_params = {}
    
    with col2:
        st.subheader("📊 分析参数")
        
        # 数据源选择
        data_source = st.selectbox(
            "数据源",
            ["数据库数据", "模拟数据"],
            index=0
        )
        
        # 分析周期
        analysis_days = st.slider("分析周期（天）", 30, 250, 120, 10)
        
        # 回测参数
        if st.checkbox("启用回测分析"):
            backtest_start = st.date_input(
                "回测开始日期",
                value=datetime.now().date() - timedelta(days=90)
            )
            backtest_end = st.date_input(
                "回测结束日期",
                value=datetime.now().date() - timedelta(days=10)
            )
            enable_backtest = True
        else:
            enable_backtest = False
            backtest_start = None
            backtest_end = None
    
    return {
        "strategy_name": strategy_name,
        "strategy_params": strategy_params,
        "data_source": data_source,
        "analysis_days": analysis_days,
        "enable_backtest": enable_backtest,
        "backtest_start": backtest_start,
        "backtest_end": backtest_end
    }


def render_stock_analysis(selected_stocks, config):
    """渲染股票分析结果"""
    if not selected_stocks:
        st.warning("请先在数据管理页面选择要分析的股票")
        return
    
    st.header(f"📊 {config['strategy_name']} 分析结果")
    
    # 获取股票数据
    if config['data_source'] == "数据库数据":
        stock_data_dict = get_stock_data_from_db(selected_stocks, config['analysis_days'])
        if not stock_data_dict:
            st.warning("数据库中无相关数据，使用模拟数据进行演示")
            stock_data_dict = generate_mock_stock_data(selected_stocks, config['analysis_days'])
    else:
        stock_data_dict = generate_mock_stock_data(selected_stocks, config['analysis_days'])
    
    if not stock_data_dict:
        st.error("无法获取股票数据")
        return
    
    # 执行策略分析
    with st.spinner("正在执行策略分析..."):
        try:
            if config['strategy_name'] == "启明星策略":
                # 暂时使用模拟分析结果
                st.warning("启明星策略分析功能暂时不可用，显示模拟结果")

                # 模拟信号结果
                st.success(f"生成 3 个模拟交易信号")

                # 模拟信号统计
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("S级信号", 2)
                with col2:
                    st.metric("A级信号", 1)
                with col3:
                    st.metric("平均确定性", "85.5")
                with col4:
                    st.metric("平均风险收益比", "1:2.3")

                # 显示模拟信号详情
                for i, stock_code in enumerate(selected_stocks[:3]):
                    with st.expander(
                        f"🎯 {stock_code} - S级 - 确定性: 85.{i}",
                        expanded=i < 2
                    ):
                        st.write("**核心逻辑**: 资金流入+技术突破")
                        st.write("**入场价**: ¥15.20-15.50")
                        st.write("**止损价**: ¥14.50")
                        st.write("**目标价**: ¥18.00")
                        st.write("**风险收益比**: 1:2.5")
                        st.info("这是模拟数据，实际使用需要修复策略模块导入")

            else:
                st.info(f"{config['strategy_name']} 分析功能开发中...")
                
        except Exception as e:
            st.error(f"策略分析失败: {e}")
            logger.error(f"策略分析异常: {e}")


def render_signal_detail(signal, stock_data):
    """渲染信号详情"""
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.write(f"**核心逻辑**: {signal.rationale}")
        st.write(f"**生成时间**: {signal.generated_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 四维分析结果
        if hasattr(signal, 'all_profiles_data') and signal.all_profiles_data:
            profiles = signal.all_profiles_data
            st.write("**四维分析评分**:")
            
            score_col1, score_col2, score_col3, score_col4 = st.columns(4)
            
            with score_col1:
                tech_score = profiles.get('technical', {}).score if profiles.get('technical') else 0
                st.metric("技术面", f"{tech_score:.1f}")
            
            with score_col2:
                capital_score = profiles.get('capital', {}).score if profiles.get('capital') else 0
                st.metric("资金面", f"{capital_score:.1f}")
            
            with score_col3:
                rs_score = profiles.get('relative_strength', {}).score if profiles.get('relative_strength') else 0
                st.metric("相对强度", f"{rs_score:.1f}")
            
            with score_col4:
                catalyst_score = profiles.get('catalyst', {}).score if profiles.get('catalyst') else 0
                st.metric("催化剂", f"{catalyst_score:.1f}")
    
    with col2:
        st.write("**交易参数**")
        st.metric("入场价", f"¥{signal.entry_zone[0]:.2f}-{signal.entry_zone[1]:.2f}")
        st.metric("止损价", f"¥{signal.stop_loss_price:.2f}")
        st.metric("目标价", f"¥{signal.target_price:.2f}")
        st.metric("风险收益比", f"1:{signal.risk_reward_ratio:.1f}")
    
    with col3:
        st.write("**仓位建议**")
        st.metric("建议仓位", f"{signal.position_size_pct:.1%}")
        
        # 计算潜在盈亏
        entry_price = (signal.entry_zone[0] + signal.entry_zone[1]) / 2
        potential_profit = signal.target_price - entry_price
        potential_loss = entry_price - signal.stop_loss_price
        
        st.metric("潜在盈利", f"¥{potential_profit:.2f}")
        st.metric("潜在亏损", f"¥{potential_loss:.2f}")
    
    # K线图
    if stock_data is not None and not stock_data.empty:
        if st.button(f"📈 查看K线图", key=f"chart_{signal.stock_code}"):
            render_stock_chart(signal, stock_data)


def render_stock_chart(signal, stock_data):
    """渲染股票K线图"""
    # 创建K线图
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=('K线图', '成交量'),
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
            name='K线'
        ),
        row=1, col=1
    )
    
    # 添加交易信号标记
    current_price = stock_data['close'].iloc[-1]
    
    # 买入区间
    fig.add_hline(
        y=signal.entry_zone[0],
        line_dash="dash",
        line_color="green",
        annotation_text=f"买入下限: {signal.entry_zone[0]:.2f}",
        row=1, col=1
    )
    fig.add_hline(
        y=signal.entry_zone[1],
        line_dash="dash",
        line_color="green",
        annotation_text=f"买入上限: {signal.entry_zone[1]:.2f}",
        row=1, col=1
    )
    
    # 止损线
    fig.add_hline(
        y=signal.stop_loss_price,
        line_dash="dash",
        line_color="red",
        annotation_text=f"止损: {signal.stop_loss_price:.2f}",
        row=1, col=1
    )
    
    # 目标价
    fig.add_hline(
        y=signal.target_price,
        line_dash="dash",
        line_color="blue",
        annotation_text=f"目标: {signal.target_price:.2f}",
        row=1, col=1
    )
    
    # 成交量
    fig.add_trace(
        go.Bar(
            x=stock_data.index,
            y=stock_data['volume'],
            name='成交量',
            marker_color='lightblue'
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        title=f"{signal.stock_code} 交易信号图",
        template="plotly_white",
        height=600,
        xaxis_rangeslider_visible=False
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_backtest_results(config, selected_stocks):
    """渲染回测结果"""
    if not config['enable_backtest']:
        return
    
    st.header("📈 回测分析结果")
    
    # 获取股票数据
    if config['data_source'] == "数据库数据":
        stock_data_dict = get_stock_data_from_db(selected_stocks, 200)  # 更长的历史数据用于回测
        if not stock_data_dict:
            stock_data_dict = generate_mock_stock_data(selected_stocks, 200)
    else:
        stock_data_dict = generate_mock_stock_data(selected_stocks, 200)
    
    if not stock_data_dict:
        st.error("无法获取回测数据")
        return
    
    with st.spinner("正在执行回测..."):
        try:
            if config['strategy_name'] == "启明星策略":
                # 暂时使用模拟回测结果
                st.warning("启明星策略回测功能暂时不可用，显示模拟结果")

                # 模拟回测结果
                mock_results = {
                    "启明星策略": type('MockResult', (), {
                        'total_return_pct': 25.8,
                        'annualized_return': 31.2,
                        'max_drawdown': -8.5,
                        'sharpe_ratio': 1.85,
                        'win_rate': 68.5,
                        'total_trades': 24
                    })(),
                    "买入持有": type('MockResult', (), {
                        'total_return_pct': 12.3,
                        'annualized_return': 15.1,
                        'max_drawdown': -15.2,
                        'sharpe_ratio': 0.92,
                        'win_rate': 100.0,
                        'total_trades': 1
                    })()
                }

                # 显示模拟回测结果
                render_backtest_comparison(mock_results)
                st.info("这是模拟回测数据，实际使用需要修复策略模块导入")
            else:
                st.info(f"{config['strategy_name']} 回测功能开发中...")
                
        except Exception as e:
            st.error(f"回测执行失败: {e}")
            logger.error(f"回测异常: {e}")


def render_backtest_comparison(backtest_results):
    """渲染回测对比结果"""
    # 结果表格
    results_data = []
    for strategy_name, result in backtest_results.items():
        results_data.append({
            "策略": strategy_name,
            "总收益率": f"{result.total_return_pct:.2f}%",
            "年化收益率": f"{result.annualized_return:.2f}%",
            "最大回撤": f"{result.max_drawdown:.2f}%",
            "夏普比率": f"{result.sharpe_ratio:.2f}",
            "胜率": f"{result.win_rate:.1f}%",
            "交易次数": result.total_trades
        })
    
    df = pd.DataFrame(results_data)
    st.dataframe(df, use_container_width=True)
    
    # 收益率对比图
    strategies = list(backtest_results.keys())
    returns = [backtest_results[s].total_return_pct for s in strategies]
    
    fig = px.bar(
        x=strategies,
        y=returns,
        title="策略收益率对比",
        labels={'x': '策略', 'y': '总收益率 (%)'}
    )
    fig.update_layout(template="plotly_white", height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # 风险收益散点图
    max_drawdowns = [abs(backtest_results[s].max_drawdown) for s in strategies]
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=max_drawdowns,
        y=returns,
        mode='markers+text',
        text=strategies,
        textposition="top center",
        marker=dict(size=15),
        name="策略"
    ))
    
    fig.update_layout(
        title="风险收益分析",
        xaxis_title="最大回撤 (%)",
        yaxis_title="总收益率 (%)",
        template="plotly_white",
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)


def render_strategy_analysis_main():
    """渲染策略分析主面板"""
    # 策略配置
    config = render_strategy_selection()
    
    st.markdown("---")
    
    # 获取选中的股票
    selected_stocks = st.session_state.get('selected_stocks', [])
    
    if selected_stocks:
        st.info(f"当前选中 {len(selected_stocks)} 只股票: {', '.join(selected_stocks[:5])}{'...' if len(selected_stocks) > 5 else ''}")
        
        # 分析按钮
        if st.button("🚀 开始分析", type="primary"):
            # 股票分析
            render_stock_analysis(selected_stocks, config)
            
            st.markdown("---")
            
            # 回测分析
            render_backtest_results(config, selected_stocks)
    else:
        st.warning("请先在 '数据管理' 页面选择要分析的股票")
        st.info("💡 提示：在数据管理页面的股票选择器中选择股票后，再回到此页面进行分析")
