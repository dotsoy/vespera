"""
启明星策略主类
整合四维分析、信号融合、回测等功能的统一接口
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
import asyncio

from .four_dimensional_analyzer import FourDimensionalAnalyzer
from .signal_fusion_engine import SignalFusionEngine, TradePlan, MarketContext
from .backtest_engine import BacktestEngine, BacktestResult
from src.utils.logger import get_logger

logger = get_logger("qiming_star_strategy")


class QimingStarStrategy:
    """
    启明星策略主类
    
    实现"资金为王，技术触发"的核心策略逻辑
    提供策略分析、信号生成、回测等完整功能
    """
    
    def __init__(self, config: Dict = None):
        """
        初始化启明星策略
        
        Args:
            config: 策略配置参数
        """
        self.logger = logger
        self.config = self._load_default_config()
        
        if config:
            self.config.update(config)
        
        # 初始化核心组件
        self.analyzer = FourDimensionalAnalyzer()
        self.signal_engine = SignalFusionEngine()
        self.backtest_engine = BacktestEngine(
            initial_capital=self.config.get("initial_capital", 1000000)
        )
        
        # 更新信号引擎配置
        self._update_signal_engine_config()
        
        self.logger.info("启明星策略初始化完成")
    
    def analyze_stock(self, stock_code: str, stock_data: pd.DataFrame,
                     market_data: pd.DataFrame = None,
                     sector_data: pd.DataFrame = None) -> Dict:
        """
        分析单只股票
        
        Args:
            stock_code: 股票代码
            stock_data: 股票数据
            market_data: 市场数据
            sector_data: 行业数据
            
        Returns:
            分析结果字典
        """
        try:
            self.logger.info(f"开始分析股票: {stock_code}")
            
            # 执行四维分析
            profiles = self.analyzer.analyze_all_dimensions(
                stock_data, market_data, sector_data
            )
            
            # 生成交易计划
            current_price = stock_data['close'].iloc[-1]
            market_context = self.signal_engine.get_market_context(market_data)
            
            trade_plan = self.signal_engine.generate_trade_plan(
                stock_code=stock_code,
                stock_name=stock_code,  # 实际应从数据库获取
                current_price=current_price,
                profiles=profiles,
                market_context=market_context
            )
            
            return {
                "stock_code": stock_code,
                "current_price": current_price,
                "profiles": profiles,
                "trade_plan": trade_plan,
                "market_context": market_context,
                "analysis_time": datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"分析股票 {stock_code} 失败: {e}")
            return None
    
    def batch_analyze(self, stock_data_dict: Dict[str, pd.DataFrame],
                     market_data: pd.DataFrame = None) -> Dict[str, List[TradePlan]]:
        """
        批量分析股票池
        
        Args:
            stock_data_dict: {stock_code: DataFrame}
            market_data: 市场数据
            
        Returns:
            {"s_class": [TradePlan], "a_class": [TradePlan]}
        """
        try:
            self.logger.info(f"开始批量分析 {len(stock_data_dict)} 只股票")
            
            # 准备数据格式
            formatted_data = {}
            for stock_code, data in stock_data_dict.items():
                formatted_data[stock_code] = {
                    "data": data,
                    "name": stock_code,
                    "price": data['close'].iloc[-1]
                }
            
            # 获取市场环境
            market_context = self.signal_engine.get_market_context(market_data)
            
            # 批量生成信号
            signals = self.signal_engine.batch_generate_signals(
                formatted_data, market_context
            )
            
            self.logger.info(f"生成信号: S级 {len(signals['s_class'])} 个, A级 {len(signals['a_class'])} 个")
            
            return signals
            
        except Exception as e:
            self.logger.error(f"批量分析失败: {e}")
            return {"s_class": [], "a_class": []}
    
    def run_backtest(self, stock_data_dict: Dict[str, pd.DataFrame],
                    start_date: datetime, end_date: datetime,
                    benchmark_strategies: List[str] = None) -> Dict[str, BacktestResult]:
        """
        运行策略回测
        
        Args:
            stock_data_dict: 股票数据
            start_date: 开始日期
            end_date: 结束日期
            benchmark_strategies: 基准策略列表
            
        Returns:
            回测结果字典
        """
        try:
            self.logger.info("开始策略回测")
            
            # 准备策略配置
            strategies_config = [
                {
                    "name": "启明星策略",
                    "params": {
                        "weights": self.signal_engine.weights,
                        "thresholds": self.signal_engine.quality_thresholds
                    }
                }
            ]
            
            # 添加基准策略
            if benchmark_strategies:
                for strategy_name in benchmark_strategies:
                    strategies_config.append({
                        "name": strategy_name,
                        "params": {}
                    })
            else:
                # 默认基准策略
                strategies_config.extend([
                    {"name": "简单移动平均", "params": {}},
                    {"name": "RSI策略", "params": {}},
                    {"name": "买入持有", "params": {}}
                ])
            
            # 执行多策略回测
            results = self.backtest_engine.compare_strategies(
                strategies_config, stock_data_dict, start_date, end_date
            )
            
            self.logger.info("回测完成")
            return results
            
        except Exception as e:
            self.logger.error(f"回测失败: {e}")
            return {}
    
    def generate_daily_signals(self, target_date: datetime = None) -> Dict[str, List[TradePlan]]:
        """
        生成每日交易信号
        
        Args:
            target_date: 目标日期，默认为今天
            
        Returns:
            当日交易信号
        """
        if target_date is None:
            target_date = datetime.now().date()
        
        try:
            self.logger.info(f"生成 {target_date} 的交易信号")
            
            # 这里应该连接数据源获取最新数据
            # 暂时返回空结果
            self.logger.warning("需要连接实时数据源")
            
            return {"s_class": [], "a_class": []}
            
        except Exception as e:
            self.logger.error(f"生成每日信号失败: {e}")
            return {"s_class": [], "a_class": []}
    
    def update_config(self, new_config: Dict):
        """更新策略配置"""
        self.config.update(new_config)
        self._update_signal_engine_config()
        self.logger.info("策略配置已更新")
    
    def get_strategy_summary(self) -> Dict:
        """获取策略摘要信息"""
        return {
            "strategy_name": "启明星策略",
            "version": "1.0.0",
            "description": "基于'资金为王，技术触发'理念的T+1交易策略",
            "config": self.config,
            "weights": self.signal_engine.weights,
            "thresholds": self.signal_engine.quality_thresholds,
            "components": {
                "analyzer": "FourDimensionalAnalyzer",
                "signal_engine": "SignalFusionEngine", 
                "backtest_engine": "BacktestEngine"
            }
        }
    
    # ==================== 私有方法 ====================
    
    def _load_default_config(self) -> Dict:
        """加载默认配置"""
        return {
            # 基本参数
            "initial_capital": 1000000,  # 初始资金100万
            "max_position_size": 0.25,   # 单股最大仓位25%
            "max_holding_days": 30,      # 最大持仓30天
            
            # 交易成本
            "commission_rate": 0.0003,   # 万三手续费
            "slippage_rate": 0.001,      # 0.1%滑点
            
            # 风险管理
            "stop_loss_atr_multiplier": 2.0,  # 止损ATR倍数
            "profit_target_ratio": 2.0,       # 盈亏比目标
            
            # 市场环境
            "bull_market_threshold": 0.001,   # 牛市判断阈值
            "bear_market_threshold": -0.001,  # 熊市判断阈值
            
            # 数据要求
            "min_data_length": 60,       # 最少数据长度
            "min_volume": 10000000,      # 最小成交额1000万
            
            # 四维分析权重
            "weights": {
                "capital": 0.45,
                "technical": 0.35,
                "relative_strength": 0.15,
                "catalyst": 0.05
            },
            
            # 信号质量阈值
            "thresholds": {
                "capital_min": 80,
                "technical_min": 75,
                "rs_min": 60,
                "s_class_min": 90,
                "a_class_min": 75
            }
        }
    
    def _update_signal_engine_config(self):
        """更新信号引擎配置"""
        if "weights" in self.config:
            self.signal_engine.weights.update(self.config["weights"])
        
        if "thresholds" in self.config:
            self.signal_engine.quality_thresholds.update(self.config["thresholds"])
    
    async def async_batch_analyze(self, stock_data_dict: Dict[str, pd.DataFrame],
                                 market_data: pd.DataFrame = None,
                                 max_concurrent: int = 10) -> Dict[str, List[TradePlan]]:
        """
        异步批量分析 (用于大规模股票池)
        
        Args:
            stock_data_dict: 股票数据
            market_data: 市场数据
            max_concurrent: 最大并发数
            
        Returns:
            分析结果
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def analyze_single(stock_code: str, stock_data: pd.DataFrame):
            async with semaphore:
                return await asyncio.get_event_loop().run_in_executor(
                    None, self.analyze_stock, stock_code, stock_data, market_data
                )
        
        # 创建任务
        tasks = [
            analyze_single(code, data) 
            for code, data in stock_data_dict.items()
        ]
        
        # 执行分析
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        s_class_signals = []
        a_class_signals = []
        
        for result in results:
            if isinstance(result, dict) and result.get("trade_plan"):
                trade_plan = result["trade_plan"]
                if trade_plan.signal_class == "S级":
                    s_class_signals.append(trade_plan)
                else:
                    a_class_signals.append(trade_plan)
        
        return {"s_class": s_class_signals, "a_class": a_class_signals}


# 便捷函数
def create_qiming_star_strategy(config: Dict = None) -> QimingStarStrategy:
    """创建启明星策略实例"""
    return QimingStarStrategy(config)


def quick_backtest(stock_data_dict: Dict[str, pd.DataFrame],
                  start_date: datetime, end_date: datetime,
                  config: Dict = None) -> Dict[str, BacktestResult]:
    """快速回测函数"""
    strategy = create_qiming_star_strategy(config)
    return strategy.run_backtest(stock_data_dict, start_date, end_date)
