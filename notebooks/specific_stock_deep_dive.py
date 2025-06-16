import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import numpy as np
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    return go, make_subplots, mo, np, pd


@app.cell
def _(mo):
    mo.md(
        """
    # 🔍 个股深度分析

    这个笔记本专门用于对单只股票进行深入的技术和基本面分析。

    ## 分析维度
    - 📈 技术分析：K线、技术指标、形态识别
    - 💰 基本面分析：财务指标、估值分析
    - 📊 量价分析：成交量、资金流向
    - 🎯 投资建议：综合评估和建议
    """
    )
    return


@app.cell
def _(mo):
    mo.md("## 📝 股票选择")

    # 股票代码输入
    stock_code = mo.ui.text(
        value="000001.SZ",
        label="股票代码",
        placeholder="请输入股票代码，如：000001.SZ"
    )

    # 分析周期选择
    period = mo.ui.dropdown(
        options=["30天", "60天", "90天", "180天", "1年"],
        value="90天",
        label="分析周期"
    )

    return period, stock_code


@app.cell
def _(mo, period, stock_code):
    mo.md(
        f"""
    ### 当前分析设置
    - **股票代码**: {stock_code.value}
    - **分析周期**: {period.value}

    *注：本示例使用模拟数据进行演示*
    """
    )
    return


@app.cell
def _(mo, np, pd, period, stock_code):
    mo.md("## 📊 技术分析")

    # 生成模拟数据
    period_days = {"30天": 30, "60天": 60, "90天": 90, "180天": 180, "1年": 365}
    days = period_days[period.value]

    dates = pd.date_range(end=pd.Timestamp.now(), periods=days, freq='D')
    np.random.seed(hash(stock_code.value) % 2**32)  # 基于股票代码生成一致的随机数

    # 生成OHLCV数据
    base_price = 10 + (hash(stock_code.value) % 100)
    returns = np.random.randn(days) * 0.02
    prices = base_price * np.exp(np.cumsum(returns))

    # 生成OHLC
    opens = prices * (1 + np.random.randn(days) * 0.005)
    highs = np.maximum(opens, prices) * (1 + np.abs(np.random.randn(days)) * 0.01)
    lows = np.minimum(opens, prices) * (1 - np.abs(np.random.randn(days)) * 0.01)
    volumes = np.random.lognormal(15, 0.5, days)

    stock_data = pd.DataFrame({
        'date': dates,
        'open': opens,
        'high': highs,
        'low': lows,
        'close': prices,
        'volume': volumes
    })

    # 计算技术指标
    stock_data['ma5'] = stock_data['close'].rolling(5).mean()
    stock_data['ma20'] = stock_data['close'].rolling(20).mean()

    # RSI
    delta = stock_data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    stock_data['rsi'] = 100 - (100 / (1 + rs))

    return (stock_data,)


@app.cell
def _(go, make_subplots, mo, stock_code, stock_data):
    # 创建K线图
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=('K线图', 'RSI'),
        row_heights=[0.7, 0.3]
    )

    # K线图
    fig.add_trace(go.Candlestick(
        x=stock_data['date'],
        open=stock_data['open'],
        high=stock_data['high'],
        low=stock_data['low'],
        close=stock_data['close'],
        name='K线'
    ), row=1, col=1)

    # 移动平均线
    fig.add_trace(go.Scatter(
        x=stock_data['date'], y=stock_data['ma5'],
        mode='lines', name='MA5', line=dict(color='orange', width=1)
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=stock_data['date'], y=stock_data['ma20'],
        mode='lines', name='MA20', line=dict(color='blue', width=1)
    ), row=1, col=1)

    # RSI
    fig.add_trace(go.Scatter(
        x=stock_data['date'], y=stock_data['rsi'],
        mode='lines', name='RSI', line=dict(color='purple')
    ), row=2, col=1)

    # RSI超买超卖线
    fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)

    fig.update_layout(
        title=f"{stock_code.value} 技术分析图表",
        height=600,
        template="plotly_white",
        xaxis_rangeslider_visible=False
    )

    mo.ui.plotly(fig)
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 💡 使用说明

    1. **输入股票代码**: 在顶部输入要分析的股票代码
    2. **选择分析周期**: 根据需要选择合适的分析时间范围
    3. **查看技术分析**: 观察K线图、技术指标和交易信号

    ## 🔧 扩展功能

    您可以在此基础上添加：
    - 真实数据接口连接
    - 更多技术指标分析
    - 行业对比分析
    - 机构评级和研报
    - 资金流向分析
    - 风险评估模型
    """
    )
    return


if __name__ == "__main__":
    app.run()
