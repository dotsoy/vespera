import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import numpy as np
    import plotly.graph_objects as go
    from datetime import datetime, timedelta
    return go, mo, np, pd


@app.cell
def _(mo):
    mo.md(
        """
    # 📈 策略回测模板

    这是一个用于策略回测的Marimo笔记本模板。
    您可以在这里测试和验证各种交易策略。

    ## 功能特点
    - 📊 数据加载和预处理
    - 🎯 策略信号生成
    - 📈 回测结果分析
    - 💰 风险收益评估
    """
    )
    return


@app.cell
def _(mo):
    # 策略参数设置
    mo.md("## ⚙️ 策略参数")

    # 创建参数输入控件
    lookback_period = mo.ui.slider(5, 60, value=20, label="回看周期")
    threshold = mo.ui.slider(0.01, 0.1, value=0.05, step=0.01, label="信号阈值")

    return lookback_period, threshold


@app.cell
def _(lookback_period, mo, threshold):
    mo.md(
        f"""
    ### 当前参数设置：
    - **回看周期**: {lookback_period.value} 天
    - **信号阈值**: {threshold.value:.2%}

    这些参数将用于策略信号的生成和回测分析。
    """
    )
    return


@app.cell
def _(mo, np, pd):
    mo.md("""
    ## 📊 数据加载

    在这里加载股票数据进行回测分析。
    """)

    # 示例数据生成
    dates = pd.date_range(start='2024-01-01', end='2024-06-01', freq='D')
    np.random.seed(42)  # 确保结果可重现
    prices = 100 + np.cumsum(np.random.randn(len(dates)) * 0.02)

    data = pd.DataFrame({
        'date': dates,
        'price': prices,
        'returns': np.concatenate([[0], np.diff(prices) / prices[:-1]])
    })

    mo.md(f"**数据概览**: 共 {len(data)} 个交易日，价格范围 {data['price'].min():.2f} - {data['price'].max():.2f}")

    return (data,)


@app.cell
def _(data, go, mo):
    def _():
        # 创建价格图表
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=data['date'],
            y=data['price'],
            mode='lines',
            name='价格',
            line=dict(color='blue', width=2)
        ))

        fig.update_layout(
            title="股票价格走势",
            xaxis_title="日期",
            yaxis_title="价格",
            template="plotly_white",
            height=400
        )
        return mo.ui.plotly(fig)


    _()
    return


@app.cell
def _(data, lookback_period, mo, threshold):
    mo.md("## 🎯 策略信号")

    # 计算移动平均
    data_with_signals = data.copy()
    data_with_signals['ma'] = data_with_signals['price'].rolling(lookback_period.value).mean()

    # 生成交易信号
    data_with_signals['signal'] = 0
    data_with_signals.loc[data_with_signals['price'] > data_with_signals['ma'] * (1 + threshold.value), 'signal'] = 1
    data_with_signals.loc[data_with_signals['price'] < data_with_signals['ma'] * (1 - threshold.value), 'signal'] = -1

    # 统计信号
    buy_signals = (data_with_signals['signal'] == 1).sum()
    sell_signals = (data_with_signals['signal'] == -1).sum()

    mo.md(f"""
    ### 信号统计
    - **买入信号**: {buy_signals} 次
    - **卖出信号**: {sell_signals} 次
    - **信号频率**: {(buy_signals + sell_signals) / len(data_with_signals) * 100:.1f}%
    """)

    return (data_with_signals,)


@app.cell
def _(data_with_signals, go, mo):
    # 创建带信号的价格图
    fig = go.Figure()

    # 价格线
    fig.add_trace(go.Scatter(
        x=data_with_signals['date'],
        y=data_with_signals['price'],
        mode='lines',
        name='价格',
        line=dict(color='blue')
    ))

    # 移动平均线
    fig.add_trace(go.Scatter(
        x=data_with_signals['date'],
        y=data_with_signals['ma'],
        mode='lines',
        name=f'移动平均',
        line=dict(color='orange', dash='dash')
    ))

    # 买入信号
    buy_points = data_with_signals[data_with_signals['signal'] == 1]
    if not buy_points.empty:
        fig.add_trace(go.Scatter(
            x=buy_points['date'],
            y=buy_points['price'],
            mode='markers',
            name='买入信号',
            marker=dict(color='green', size=8, symbol='triangle-up')
        ))

    # 卖出信号
    sell_points = data_with_signals[data_with_signals['signal'] == -1]
    if not sell_points.empty:
        fig.add_trace(go.Scatter(
            x=sell_points['date'],
            y=sell_points['price'],
            mode='markers',
            name='卖出信号',
            marker=dict(color='red', size=8, symbol='triangle-down')
        ))

    fig.update_layout(
        title="策略信号图",
        xaxis_title="日期",
        yaxis_title="价格",
        template="plotly_white",
        height=500
    )

    mo.ui.plotly(fig)
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 💡 使用说明

    1. **调整参数**: 使用上方的滑块调整策略参数
    2. **观察信号**: 查看生成的买卖信号在价格图上的分布
    3. **分析结果**: 评估策略的收益率和风险指标
    4. **优化策略**: 根据回测结果调整参数或策略逻辑

    ## 🔧 扩展功能

    您可以在此基础上添加：
    - 更复杂的技术指标
    - 多因子策略模型
    - 风险管理规则
    - 交易成本考虑
    - 更详细的性能分析
    """
    )
    return


if __name__ == "__main__":
    app.run()
