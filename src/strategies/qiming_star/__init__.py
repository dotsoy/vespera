"""
启明星策略模块
基于"资金为王，技术触发"理念的T+1交易策略

核心组件:
- FourDimensionalAnalyzer: 四维分析框架
- SignalFusionEngine: 信号融合引擎
- BacktestEngine: 回测分析引擎
- QimingStarStrategy: 策略主类
"""

from .four_dimensional_analyzer import (
    FourDimensionalAnalyzer,
    AnalysisProfile,
    TechnicalProfile,
    CapitalProfile,
    CatalystProfile,
    RelativeStrengthProfile
)

from .signal_fusion_engine import (
    SignalFusionEngine,
    TradePlan,
    MarketContext
)

from .backtest_engine import (
    BacktestEngine,
    BacktestResult,
    Trade
)

from .qiming_star_strategy import QimingStarStrategy

__all__ = [
    # 分析框架
    'FourDimensionalAnalyzer',
    'AnalysisProfile',
    'TechnicalProfile', 
    'CapitalProfile',
    'CatalystProfile',
    'RelativeStrengthProfile',
    
    # 信号引擎
    'SignalFusionEngine',
    'TradePlan',
    'MarketContext',
    
    # 回测引擎
    'BacktestEngine',
    'BacktestResult',
    'Trade',
    
    # 主策略类
    'QimingStarStrategy'
]

# 版本信息
__version__ = "1.0.0"
__author__ = "启明星量化团队"
__description__ = "基于四维分析的T+1交易策略系统"
