"""
启明星策略 - 回测引擎
支持多策略对比分析和性能评估
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

from .signal_fusion_engine import SignalFusionEngine, TradePlan, MarketContext
from .four_dimensional_analyzer import FourDimensionalAnalyzer
from src.utils.logger import get_logger

logger = get_logger("backtest_engine")


@dataclass
class Trade:
    """交易记录"""
    stock_code: str
    entry_date: datetime
    entry_price: float
    exit_date: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: str = "HOLDING"  # PROFIT_TARGET, STOP_LOSS, TIME_LIMIT, MANUAL
    quantity: int = 0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    holding_days: int = 0
    trade_plan: Optional[TradePlan] = None


@dataclass
class BacktestResult:
    """回测结果"""
    strategy_name: str
    start_date: datetime
    end_date: datetime
    initial_capital: float
    final_capital: float
    total_return: float
    total_return_pct: float
    annualized_return: float
    max_drawdown: float
    sharpe_ratio: float
    win_rate: float
    profit_factor: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    avg_win: float
    avg_loss: float
    max_win: float
    max_loss: float
    avg_holding_days: float
    trades: List[Trade]
    daily_returns: pd.Series
    equity_curve: pd.Series
    
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        result = asdict(self)
        # 转换日期格式
        result['start_date'] = self.start_date.isoformat()
        result['end_date'] = self.end_date.isoformat()
        # 移除复杂对象
        result.pop('trades', None)
        result.pop('daily_returns', None)
        result.pop('equity_curve', None)
        return result


class BacktestEngine:
    """回测引擎"""
    
    def __init__(self, initial_capital: float = 1000000):
        self.initial_capital = initial_capital
        self.logger = logger
        
        # 交易设置
        self.commission_rate = 0.0003  # 万三手续费
        self.slippage_rate = 0.001     # 0.1% 滑点
        self.max_position_size = 0.25  # 单只股票最大仓位25%
        self.max_holding_days = 30     # 最大持仓天数
        
    def run_backtest(self, strategy_name: str,
                    stock_data_dict: Dict[str, pd.DataFrame],
                    start_date: datetime,
                    end_date: datetime,
                    strategy_params: Dict = None) -> BacktestResult:
        """
        运行回测
        
        Args:
            strategy_name: 策略名称
            stock_data_dict: {stock_code: DataFrame} 股票数据
            start_date: 开始日期
            end_date: 结束日期
            strategy_params: 策略参数
            
        Returns:
            回测结果
        """
        self.logger.info(f"开始回测策略: {strategy_name}")
        self.logger.info(f"回测期间: {start_date} 到 {end_date}")
        self.logger.info(f"股票池大小: {len(stock_data_dict)}")
        
        # 初始化
        current_capital = self.initial_capital
        positions = {}  # {stock_code: Trade}
        closed_trades = []
        daily_equity = []
        daily_dates = []
        
        # 创建策略引擎
        if strategy_name == "启明星策略":
            strategy_engine = self._create_qiming_star_engine(strategy_params)
        else:
            strategy_engine = self._create_baseline_engine(strategy_name, strategy_params)
        
        # 按日期遍历
        current_date = start_date
        while current_date <= end_date:
            try:
                # 获取当日可交易股票数据
                daily_stock_data = self._get_daily_stock_data(
                    stock_data_dict, current_date
                )
                
                if not daily_stock_data:
                    current_date += timedelta(days=1)
                    continue
                
                # 检查止损和止盈
                positions, closed_trades_today = self._check_exit_conditions(
                    positions, daily_stock_data, current_date
                )
                closed_trades.extend(closed_trades_today)
                
                # 生成新信号
                if strategy_name == "启明星策略":
                    new_signals = self._generate_qiming_star_signals(
                        strategy_engine, daily_stock_data, current_date
                    )
                else:
                    new_signals = self._generate_baseline_signals(
                        strategy_engine, daily_stock_data, current_date, strategy_name
                    )
                
                # 执行交易
                positions, trades_today = self._execute_trades(
                    positions, new_signals, daily_stock_data, 
                    current_capital, current_date
                )
                
                # 计算当日权益
                daily_equity_value = self._calculate_daily_equity(
                    current_capital, positions, daily_stock_data
                )
                daily_equity.append(daily_equity_value)
                daily_dates.append(current_date)
                
                # 更新资金 (卖出时释放资金)
                for trade in closed_trades_today:
                    if trade.exit_reason != "HOLDING":
                        current_capital += trade.quantity * trade.exit_price * (1 - self.commission_rate)
                
                current_date += timedelta(days=1)
                
            except Exception as e:
                self.logger.error(f"回测日期 {current_date} 处理失败: {e}")
                current_date += timedelta(days=1)
                continue
        
        # 强制平仓所有持仓
        final_positions, final_trades = self._force_close_positions(
            positions, stock_data_dict, end_date
        )
        closed_trades.extend(final_trades)
        
        # 计算最终资金
        final_capital = current_capital
        for trade in final_trades:
            final_capital += trade.quantity * trade.exit_price * (1 - self.commission_rate)
        
        # 生成回测结果
        result = self._generate_backtest_result(
            strategy_name, start_date, end_date,
            self.initial_capital, final_capital,
            closed_trades, daily_equity, daily_dates
        )
        
        self.logger.info(f"回测完成: 总收益率 {result.total_return_pct:.2f}%")
        return result
    
    def compare_strategies(self, strategies_config: List[Dict],
                          stock_data_dict: Dict[str, pd.DataFrame],
                          start_date: datetime,
                          end_date: datetime) -> Dict[str, BacktestResult]:
        """
        多策略对比
        
        Args:
            strategies_config: [{"name": str, "params": dict}]
            stock_data_dict: 股票数据
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            {strategy_name: BacktestResult}
        """
        results = {}
        
        for config in strategies_config:
            strategy_name = config["name"]
            strategy_params = config.get("params", {})
            
            try:
                result = self.run_backtest(
                    strategy_name, stock_data_dict,
                    start_date, end_date, strategy_params
                )
                results[strategy_name] = result
                
            except Exception as e:
                self.logger.error(f"策略 {strategy_name} 回测失败: {e}")
                continue
        
        return results
    
    # ==================== 私有方法 ====================
    
    def _create_qiming_star_engine(self, params: Dict = None):
        """创建启明星策略引擎"""
        engine = SignalFusionEngine()
        
        if params:
            # 更新参数
            if "weights" in params:
                engine.weights.update(params["weights"])
            if "thresholds" in params:
                engine.quality_thresholds.update(params["thresholds"])
        
        return engine
    
    def _create_baseline_engine(self, strategy_name: str, params: Dict = None):
        """创建基准策略引擎"""
        return {
            "name": strategy_name,
            "params": params or {}
        }
    
    def _get_daily_stock_data(self, stock_data_dict: Dict[str, pd.DataFrame],
                            target_date: datetime) -> Dict[str, Dict]:
        """获取指定日期的股票数据"""
        daily_data = {}
        
        for stock_code, df in stock_data_dict.items():
            # 获取目标日期及之前的数据
            mask = df.index <= target_date
            if mask.sum() < 60:  # 至少需要60天数据
                continue
                
            stock_history = df[mask]
            if len(stock_history) == 0:
                continue
                
            current_price = stock_history['close'].iloc[-1]
            
            daily_data[stock_code] = {
                "data": stock_history,
                "price": current_price,
                "name": stock_code  # 实际应该从数据库获取股票名称
            }
        
        return daily_data
    
    def _generate_qiming_star_signals(self, engine: SignalFusionEngine,
                                    daily_stock_data: Dict,
                                    current_date: datetime) -> List[TradePlan]:
        """生成启明星策略信号"""
        try:
            # 获取市场环境
            market_context = engine.get_market_context()
            
            # 只在牛市背景下生成信号
            if market_context.status != "BULLISH":
                return []
            
            # 批量生成信号
            signals = engine.batch_generate_signals(daily_stock_data, market_context)
            
            # 返回所有信号 (S级 + A级)
            all_signals = signals["s_class"] + signals["a_class"]
            
            # 按确定性得分排序，取前10个
            all_signals.sort(key=lambda x: x.conviction_score, reverse=True)
            return all_signals[:10]
            
        except Exception as e:
            self.logger.error(f"生成启明星信号失败: {e}")
            return []
    
    def _generate_baseline_signals(self, engine: Dict, daily_stock_data: Dict,
                                 current_date: datetime, strategy_name: str) -> List[TradePlan]:
        """生成基准策略信号"""
        signals = []
        
        try:
            for stock_code, stock_info in daily_stock_data.items():
                data = stock_info["data"]
                current_price = stock_info["price"]
                
                # 简单移动平均策略
                if strategy_name == "简单移动平均":
                    signal = self._ma_strategy_signal(data, stock_code, current_price)
                # RSI策略
                elif strategy_name == "RSI策略":
                    signal = self._rsi_strategy_signal(data, stock_code, current_price)
                # 买入持有策略
                elif strategy_name == "买入持有":
                    signal = self._buy_hold_signal(data, stock_code, current_price)
                else:
                    continue
                
                if signal:
                    signals.append(signal)
            
            return signals[:10]  # 限制信号数量
            
        except Exception as e:
            self.logger.error(f"生成基准策略信号失败: {e}")
            return []
    
    def _ma_strategy_signal(self, data: pd.DataFrame, stock_code: str, 
                          current_price: float) -> Optional[TradePlan]:
        """移动平均策略信号"""
        if len(data) < 20:
            return None
        
        ma_5 = data['close'].rolling(5).mean().iloc[-1]
        ma_20 = data['close'].rolling(20).mean().iloc[-1]
        
        # 金叉买入信号
        if ma_5 > ma_20 and current_price > ma_5:
            return TradePlan(
                stock_code=stock_code,
                stock_name=stock_code,
                conviction_score=75.0,
                signal_class="A级",
                rationale="5日均线上穿20日均线，金叉买入",
                entry_zone=(current_price * 0.99, current_price * 1.01),
                stop_loss_price=current_price * 0.95,
                target_price=current_price * 1.10,
                risk_reward_ratio=2.0,
                position_size_pct=0.1,
                all_profiles_data={},
                generated_time=datetime.now()
            )
        
        return None
    
    def _rsi_strategy_signal(self, data: pd.DataFrame, stock_code: str,
                           current_price: float) -> Optional[TradePlan]:
        """RSI策略信号"""
        if len(data) < 14:
            return None
        
        # 计算RSI
        delta = data['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        current_rsi = rsi.iloc[-1]
        
        # RSI超卖买入信号
        if current_rsi < 30:
            return TradePlan(
                stock_code=stock_code,
                stock_name=stock_code,
                conviction_score=70.0,
                signal_class="A级",
                rationale=f"RSI超卖({current_rsi:.1f})，反弹买入",
                entry_zone=(current_price * 0.99, current_price * 1.01),
                stop_loss_price=current_price * 0.93,
                target_price=current_price * 1.15,
                risk_reward_ratio=2.1,
                position_size_pct=0.08,
                all_profiles_data={},
                generated_time=datetime.now()
            )
        
        return None
    
    def _buy_hold_signal(self, data: pd.DataFrame, stock_code: str,
                        current_price: float) -> Optional[TradePlan]:
        """买入持有策略信号"""
        # 简单的买入持有，只在第一次遇到时买入
        return TradePlan(
            stock_code=stock_code,
            stock_name=stock_code,
            conviction_score=60.0,
            signal_class="A级",
            rationale="买入持有策略",
            entry_zone=(current_price * 0.99, current_price * 1.01),
            stop_loss_price=current_price * 0.80,  # 20%止损
            target_price=current_price * 2.00,     # 100%目标
            risk_reward_ratio=4.0,
            position_size_pct=0.05,
            all_profiles_data={},
            generated_time=datetime.now()
        )
    
    def _check_exit_conditions(self, positions: Dict[str, Trade],
                             daily_stock_data: Dict,
                             current_date: datetime) -> Tuple[Dict, List[Trade]]:
        """检查退出条件"""
        remaining_positions = {}
        closed_trades = []
        
        for stock_code, trade in positions.items():
            if stock_code not in daily_stock_data:
                # 股票停牌或退市，强制平仓
                trade.exit_date = current_date
                trade.exit_price = trade.entry_price * 0.9  # 假设10%损失
                trade.exit_reason = "DELISTED"
                trade.holding_days = (current_date - trade.entry_date).days
                trade.pnl = trade.quantity * (trade.exit_price - trade.entry_price)
                trade.pnl_pct = (trade.exit_price - trade.entry_price) / trade.entry_price
                closed_trades.append(trade)
                continue
            
            current_price = daily_stock_data[stock_code]["price"]
            holding_days = (current_date - trade.entry_date).days
            
            # 检查止损
            if current_price <= trade.trade_plan.stop_loss_price:
                trade.exit_date = current_date
                trade.exit_price = current_price * (1 - self.slippage_rate)
                trade.exit_reason = "STOP_LOSS"
                trade.holding_days = holding_days
                trade.pnl = trade.quantity * (trade.exit_price - trade.entry_price)
                trade.pnl_pct = (trade.exit_price - trade.entry_price) / trade.entry_price
                closed_trades.append(trade)
                continue
            
            # 检查止盈
            if current_price >= trade.trade_plan.target_price:
                trade.exit_date = current_date
                trade.exit_price = current_price * (1 - self.slippage_rate)
                trade.exit_reason = "PROFIT_TARGET"
                trade.holding_days = holding_days
                trade.pnl = trade.quantity * (trade.exit_price - trade.entry_price)
                trade.pnl_pct = (trade.exit_price - trade.entry_price) / trade.entry_price
                closed_trades.append(trade)
                continue
            
            # 检查时间限制
            if holding_days >= self.max_holding_days:
                trade.exit_date = current_date
                trade.exit_price = current_price * (1 - self.slippage_rate)
                trade.exit_reason = "TIME_LIMIT"
                trade.holding_days = holding_days
                trade.pnl = trade.quantity * (trade.exit_price - trade.entry_price)
                trade.pnl_pct = (trade.exit_price - trade.entry_price) / trade.entry_price
                closed_trades.append(trade)
                continue
            
            # 继续持有
            remaining_positions[stock_code] = trade
        
        return remaining_positions, closed_trades

    def _execute_trades(self, positions: Dict[str, Trade], signals: List[TradePlan],
                       daily_stock_data: Dict, available_capital: float,
                       current_date: datetime) -> Tuple[Dict, List[Trade]]:
        """执行交易"""
        new_trades = []

        for signal in signals:
            stock_code = signal.stock_code

            # 检查是否已持仓
            if stock_code in positions:
                continue

            # 检查是否有数据
            if stock_code not in daily_stock_data:
                continue

            current_price = daily_stock_data[stock_code]["price"]

            # 检查是否在买入区间
            if not (signal.entry_zone[0] <= current_price <= signal.entry_zone[1]):
                continue

            # 计算仓位大小
            position_value = available_capital * signal.position_size_pct
            position_value = min(position_value, available_capital * self.max_position_size)

            if position_value < 10000:  # 最小交易金额1万元
                continue

            # 计算股数 (100股为一手)
            entry_price = current_price * (1 + self.slippage_rate)
            quantity = int(position_value / entry_price / 100) * 100

            if quantity <= 0:
                continue

            # 创建交易记录
            trade = Trade(
                stock_code=stock_code,
                entry_date=current_date,
                entry_price=entry_price,
                quantity=quantity,
                trade_plan=signal
            )

            positions[stock_code] = trade
            new_trades.append(trade)

            # 扣除资金
            available_capital -= quantity * entry_price * (1 + self.commission_rate)

        return positions, new_trades

    def _calculate_daily_equity(self, cash: float, positions: Dict[str, Trade],
                              daily_stock_data: Dict) -> float:
        """计算当日权益"""
        total_equity = cash

        for stock_code, trade in positions.items():
            if stock_code in daily_stock_data:
                current_price = daily_stock_data[stock_code]["price"]
                position_value = trade.quantity * current_price
                total_equity += position_value

        return total_equity

    def _force_close_positions(self, positions: Dict[str, Trade],
                             stock_data_dict: Dict[str, pd.DataFrame],
                             end_date: datetime) -> Tuple[Dict, List[Trade]]:
        """强制平仓所有持仓"""
        closed_trades = []

        for stock_code, trade in positions.items():
            if stock_code in stock_data_dict:
                # 获取最后一个交易日的价格
                stock_data = stock_data_dict[stock_code]
                mask = stock_data.index <= end_date
                if mask.sum() > 0:
                    final_price = stock_data[mask]['close'].iloc[-1]
                else:
                    final_price = trade.entry_price * 0.9  # 假设损失
            else:
                final_price = trade.entry_price * 0.9

            trade.exit_date = end_date
            trade.exit_price = final_price * (1 - self.slippage_rate)
            trade.exit_reason = "FORCE_CLOSE"
            trade.holding_days = (end_date - trade.entry_date).days
            trade.pnl = trade.quantity * (trade.exit_price - trade.entry_price)
            trade.pnl_pct = (trade.exit_price - trade.entry_price) / trade.entry_price
            closed_trades.append(trade)

        return {}, closed_trades

    def _generate_backtest_result(self, strategy_name: str,
                                start_date: datetime, end_date: datetime,
                                initial_capital: float, final_capital: float,
                                trades: List[Trade],
                                daily_equity: List[float],
                                daily_dates: List[datetime]) -> BacktestResult:
        """生成回测结果"""
        # 基本收益指标
        total_return = final_capital - initial_capital
        total_return_pct = (final_capital / initial_capital - 1) * 100

        # 年化收益率
        days = (end_date - start_date).days
        years = days / 365.25
        annualized_return = (final_capital / initial_capital) ** (1 / years) - 1 if years > 0 else 0
        annualized_return *= 100

        # 交易统计
        total_trades = len(trades)
        winning_trades = len([t for t in trades if t.pnl > 0])
        losing_trades = len([t for t in trades if t.pnl < 0])
        win_rate = winning_trades / total_trades * 100 if total_trades > 0 else 0

        # 盈亏统计
        wins = [t.pnl for t in trades if t.pnl > 0]
        losses = [t.pnl for t in trades if t.pnl < 0]

        avg_win = np.mean(wins) if wins else 0
        avg_loss = np.mean(losses) if losses else 0
        max_win = max(wins) if wins else 0
        max_loss = min(losses) if losses else 0

        # 盈亏比
        profit_factor = abs(sum(wins) / sum(losses)) if losses and sum(losses) != 0 else 0

        # 平均持仓天数
        avg_holding_days = np.mean([t.holding_days for t in trades]) if trades else 0

        # 计算最大回撤和夏普比率
        if daily_equity and len(daily_equity) > 1:
            equity_series = pd.Series(daily_equity, index=daily_dates)
            daily_returns = equity_series.pct_change().dropna()

            # 最大回撤
            peak = equity_series.expanding().max()
            drawdown = (equity_series - peak) / peak
            max_drawdown = drawdown.min() * 100

            # 夏普比率 (假设无风险利率为3%)
            excess_returns = daily_returns - 0.03 / 252  # 日化无风险利率
            sharpe_ratio = excess_returns.mean() / excess_returns.std() * np.sqrt(252) if excess_returns.std() > 0 else 0
        else:
            max_drawdown = 0
            sharpe_ratio = 0
            daily_returns = pd.Series()
            equity_series = pd.Series()

        return BacktestResult(
            strategy_name=strategy_name,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            final_capital=final_capital,
            total_return=total_return,
            total_return_pct=total_return_pct,
            annualized_return=annualized_return,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe_ratio,
            win_rate=win_rate,
            profit_factor=profit_factor,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            avg_win=avg_win,
            avg_loss=avg_loss,
            max_win=max_win,
            max_loss=max_loss,
            avg_holding_days=avg_holding_days,
            trades=trades,
            daily_returns=daily_returns,
            equity_curve=equity_series
        )
