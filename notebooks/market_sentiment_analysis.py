import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import numpy as np
    import plotly.graph_objects as go
    import plotly.express as px
    from datetime import datetime, timedelta
    return datetime, go, mo, np, pd


@app.cell
def _(mo):
    mo.md(
        """
    # 😊 市场情绪分析

    分析市场整体情绪、投资者行为和市场心理。

    ## 分析内容
    - 📊 情绪指标：恐慌贪婪指数、VIX等
    - 📈 市场广度：涨跌比、新高新低
    - 🔄 资金流向：主力资金、散户资金
    - 🎯 情绪信号：买卖点识别
    """
    )
    return


@app.cell
def _(datetime, mo, np, pd):
    mo.md("## 📊 情绪指标概览")

    # 生成模拟情绪数据
    np.random.seed(42)
    dates = pd.date_range(end=datetime.now(), periods=60, freq='D')

    # 恐慌贪婪指数 (0-100)
    fear_greed = 50 + np.cumsum(np.random.randn(60) * 2)
    fear_greed = np.clip(fear_greed, 0, 100)

    # VIX指数 (波动率指数)
    vix = 20 + np.abs(np.random.randn(60) * 5)

    # 涨跌比
    advance_decline = 1 + np.random.randn(60) * 0.3
    advance_decline = np.clip(advance_decline, 0.3, 3.0)

    sentiment_data = pd.DataFrame({
        'date': dates,
        'fear_greed': fear_greed,
        'vix': vix,
        'advance_decline': advance_decline
    })

    # 当前情绪状态
    current_fg = sentiment_data['fear_greed'].iloc[-1]
    current_vix = sentiment_data['vix'].iloc[-1]
    current_ad = sentiment_data['advance_decline'].iloc[-1]

    # 情绪判断
    if current_fg > 75:
        fg_status = "🔥 极度贪婪"
    elif current_fg > 55:
        fg_status = "😊 贪婪"
    elif current_fg > 45:
        fg_status = "😐 中性"
    elif current_fg > 25:
        fg_status = "😰 恐慌"
    else:
        fg_status = "😱 极度恐慌"

    mo.md(f"""
    ### 当前市场情绪

    | 指标 | 数值 | 状态 |
    |------|------|------|
    | 恐慌贪婪指数 | {current_fg:.0f} | {fg_status} |
    | 波动率指数(VIX) | {current_vix:.1f} | {'高波动' if current_vix > 25 else '中等波动' if current_vix > 15 else '低波动'} |
    | 涨跌比 | {current_ad:.2f} | {'多头强势' if current_ad > 1.5 else '多头' if current_ad > 1.1 else '平衡' if current_ad > 0.9 else '空头'} |
    """)

    return (sentiment_data,)


@app.cell
def _(go, mo, sentiment_data):
    # 创建情绪指标图表
    fig = go.Figure()

    # 恐慌贪婪指数
    fig.add_trace(go.Scatter(
        x=sentiment_data['date'],
        y=sentiment_data['fear_greed'],
        mode='lines+markers',
        name='恐慌贪婪指数',
        line=dict(color='blue', width=2)
    ))

    # 添加恐慌贪婪区域
    fig.add_hline(y=75, line_dash="dash", line_color="red", 
                  annotation_text="极度贪婪", annotation_position="right")
    fig.add_hline(y=25, line_dash="dash", line_color="green", 
                  annotation_text="极度恐慌", annotation_position="right")

    fig.update_layout(
        title="恐慌贪婪指数趋势",
        xaxis_title="日期",
        yaxis_title="恐慌贪婪指数",
        template="plotly_white",
        height=400,
        yaxis=dict(range=[0, 100])
    )

    mo.ui.plotly(fig)
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 💡 使用说明

    ### 情绪指标解读
    - **恐慌贪婪指数**: 0-25极度恐慌(买入机会), 75-100极度贪婪(卖出信号)
    - **VIX指数**: 高波动通常伴随市场恐慌，可能是买入机会
    - **涨跌比**: >1.5多头强势, <0.9空头占优

    ### 交易策略
    1. **逆向投资**: 极度恐慌时买入，极度贪婪时卖出
    2. **资金跟踪**: 跟随主力资金方向
    3. **情绪确认**: 多个指标共振时信号更可靠

    ## ⚠️ 风险提示
    - 情绪指标具有滞后性，需结合其他分析
    - 极端情绪可能持续较长时间
    - 注意T+1交易制度的影响
    - 本分析基于模拟数据，仅供学习参考
    """
    )
    return


if __name__ == "__main__":
    app.run()
