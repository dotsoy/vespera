"""
启明星策略 - 四维分析框架
实现技术分析、资金分析、基本面催化剂分析、相对强弱分析四个维度
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

import tulipy as ti
from src.utils.logger import get_logger

logger = get_logger("four_dimensional_analyzer")


@dataclass
class AnalysisProfile:
    """分析结果配置文件"""
    score: float  # 0-100分
    labels: List[str]  # 描述性标签
    details: Dict  # 详细数据
    confidence: float = 0.0  # 置信度


@dataclass
class TechnicalProfile(AnalysisProfile):
    """技术分析配置文件"""
    is_spring: bool = False  # 是否出现震仓
    is_sos: bool = False  # 是否强势发力
    atr: float = 0.0  # 平均真实波幅
    trend_status: str = "NEUTRAL"  # 趋势状态


@dataclass
class CapitalProfile(AnalysisProfile):
    """资金分析配置文件"""
    net_inflow_ratio: float = 0.0  # 净流入比例
    consecutive_days: int = 0  # 连续流入天数
    main_force_intensity: str = "WEAK"  # 主力强度


@dataclass
class CatalystProfile(AnalysisProfile):
    """催化剂分析配置文件"""
    upcoming_events: List[Dict] = None  # 即将到来的事件
    event_impact: str = "NEUTRAL"  # 事件影响


@dataclass
class RelativeStrengthProfile(AnalysisProfile):
    """相对强弱分析配置文件"""
    rs_vs_market: float = 0.0  # 相对大盘强度
    rs_vs_sector: float = 0.0  # 相对行业强度
    rs_trend: str = "NEUTRAL"  # 相对强度趋势


class FourDimensionalAnalyzer:
    """四维分析器"""
    
    def __init__(self):
        self.logger = logger
        
    def analyze_all_dimensions(self, stock_data: pd.DataFrame, 
                             market_data: pd.DataFrame = None,
                             sector_data: pd.DataFrame = None) -> Dict[str, AnalysisProfile]:
        """
        执行四维分析
        
        Args:
            stock_data: 股票数据 (OHLCV + 资金流数据)
            market_data: 市场指数数据
            sector_data: 行业数据
            
        Returns:
            包含四个维度分析结果的字典
        """
        try:
            # 技术分析
            technical_profile = self.analyze_technical_profile(stock_data)
            
            # 资金分析
            capital_profile = self.analyze_capital_flow_profile(stock_data)
            
            # 催化剂分析
            catalyst_profile = self.analyze_catalyst_profile(stock_data)
            
            # 相对强弱分析
            rs_profile = self.analyze_relative_strength(
                stock_data, market_data, sector_data
            )
            
            return {
                "technical": technical_profile,
                "capital": capital_profile,
                "catalyst": catalyst_profile,
                "relative_strength": rs_profile
            }
            
        except Exception as e:
            self.logger.error(f"四维分析失败: {e}")
            return self._get_default_profiles()
    
    def analyze_technical_profile(self, stock_data: pd.DataFrame) -> TechnicalProfile:
        """
        技术分析维度
        基于威科夫量价分析和技术指标
        """
        try:
            # 确保数据足够
            if len(stock_data) < 60:
                return self._get_default_technical_profile()
            
            # 计算技术指标
            close = stock_data['close'].values
            high = stock_data['high'].values
            low = stock_data['low'].values
            volume = stock_data['volume'].values
            
            # EMA指标
            ema_10 = ti.ema(close, period=10)
            ema_20 = ti.ema(close, period=20)
            
            # RSI指标
            rsi = ti.rsi(close, period=14)
            
            # ATR指标
            atr = ti.atr(high, low, close, period=14)
            
            # 成交量比率
            volume_sma_20 = ti.sma(volume, period=20)
            volume_ratio = volume[-1] / volume_sma_20[-1] if volume_sma_20[-1] > 0 else 1.0
            
            # 评估趋势
            trend_score, trend_status = self._assess_trend(ema_10, ema_20, close)
            
            # 评估努力与结果 (威科夫核心概念)
            effort_result_score, effort_result_label = self._assess_effort_vs_result(
                stock_data.tail(20)
            )
            
            # 识别威科夫事件
            is_spring = self._detect_spring(stock_data.tail(30))
            is_sos = self._detect_sign_of_strength(stock_data.tail(20))
            
            # 计算综合技术得分
            technical_score = (
                trend_score * 0.4 +
                effort_result_score * 0.3 +
                (100 if is_sos else 0) * 0.2 +
                (80 if is_spring else 0) * 0.1
            )
            
            # 生成标签
            labels = [trend_status, effort_result_label]
            if is_spring:
                labels.append("检测到震仓信号")
            if is_sos:
                labels.append("检测到强势发力")
            
            return TechnicalProfile(
                score=min(technical_score, 100),
                labels=labels,
                details={
                    "ema_10": ema_10[-1],
                    "ema_20": ema_20[-1],
                    "rsi": rsi[-1],
                    "volume_ratio": volume_ratio,
                    "trend_score": trend_score,
                    "effort_result_score": effort_result_score
                },
                confidence=0.8,
                is_spring=is_spring,
                is_sos=is_sos,
                atr=atr[-1],
                trend_status=trend_status
            )
            
        except Exception as e:
            self.logger.error(f"技术分析失败: {e}")
            return self._get_default_technical_profile()
    
    def analyze_capital_flow_profile(self, stock_data: pd.DataFrame) -> CapitalProfile:
        """
        资金分析维度
        分析主力资金流入流出情况
        """
        try:
            # 模拟资金流数据计算 (实际应使用Level-2数据)
            # 这里使用价量关系来估算资金流向
            
            # 计算资金流指标
            net_inflow_stats = self._calculate_net_inflow(stock_data.tail(20))
            
            score = 50  # 默认中性
            status = "资金面平平"
            intensity = "WEAK"
            
            # 评估资金流入强度
            if (net_inflow_stats['daily_ratio'] > 0.1 and 
                net_inflow_stats['consecutive_days'] >= 3):
                score = 95
                status = "主力资金深度介入"
                intensity = "STRONG"
            elif net_inflow_stats['daily_ratio'] > 0.05:
                score = 80
                status = "主力资金显著流入"
                intensity = "MODERATE"
            elif net_inflow_stats['daily_ratio'] < -0.05:
                score = 30
                status = "主力资金流出"
                intensity = "OUTFLOW"
            
            return CapitalProfile(
                score=score,
                labels=[status],
                details=net_inflow_stats,
                confidence=0.7,
                net_inflow_ratio=net_inflow_stats['daily_ratio'],
                consecutive_days=net_inflow_stats['consecutive_days'],
                main_force_intensity=intensity
            )
            
        except Exception as e:
            self.logger.error(f"资金分析失败: {e}")
            return self._get_default_capital_profile()
    
    def analyze_catalyst_profile(self, stock_data: pd.DataFrame) -> CatalystProfile:
        """
        催化剂分析维度
        识别短期事件催化剂
        """
        try:
            # 模拟催化剂检测 (实际应连接新闻API和财报日历)
            upcoming_events = self._find_upcoming_events(stock_data)
            
            score = 50  # 默认中性
            labels = ["无近期催化剂"]
            impact = "NEUTRAL"
            
            if len(upcoming_events) > 0:
                score = 70
                labels = [f"临近事件: {upcoming_events[0]['type']}"]
                impact = "POSITIVE"
            
            return CatalystProfile(
                score=score,
                labels=labels,
                details={"events": upcoming_events},
                confidence=0.6,
                upcoming_events=upcoming_events,
                event_impact=impact
            )
            
        except Exception as e:
            self.logger.error(f"催化剂分析失败: {e}")
            return self._get_default_catalyst_profile()
    
    def analyze_relative_strength(self, stock_data: pd.DataFrame,
                                market_data: pd.DataFrame = None,
                                sector_data: pd.DataFrame = None) -> RelativeStrengthProfile:
        """
        相对强弱分析维度
        评估个股相对大盘和行业的强弱
        """
        try:
            if market_data is None:
                # 使用模拟市场数据
                market_data = self._generate_mock_market_data(len(stock_data))
            
            # 计算相对强度线
            rs_vs_market = self._calculate_rs_line(
                stock_data['close'].values,
                market_data['close'].values if 'close' in market_data.columns else market_data.values
            )
            
            rs_vs_sector = 0.0  # 默认值
            if sector_data is not None:
                rs_vs_sector = self._calculate_rs_line(
                    stock_data['close'].values,
                    sector_data['close'].values
                )
            
            # 评估相对强度
            score = 50  # 默认中性
            labels = ["强度与大盘持平"]
            rs_trend = "NEUTRAL"
            
            if rs_vs_market > 0.05:  # 相对强度明显上升
                score = 90
                labels = ["显著强于大盘"]
                rs_trend = "UP"
            elif rs_vs_market < -0.05:  # 相对强度明显下降
                score = 30
                labels = ["明显弱于大盘"]
                rs_trend = "DOWN"
            
            return RelativeStrengthProfile(
                score=score,
                labels=labels,
                details={
                    "rs_vs_market_value": rs_vs_market,
                    "rs_vs_sector_value": rs_vs_sector
                },
                confidence=0.8,
                rs_vs_market=rs_vs_market,
                rs_vs_sector=rs_vs_sector,
                rs_trend=rs_trend
            )
            
        except Exception as e:
            self.logger.error(f"相对强弱分析失败: {e}")
            return self._get_default_rs_profile()
    
    # ==================== 私有辅助方法 ====================
    
    def _assess_trend(self, ema_10: np.ndarray, ema_20: np.ndarray, 
                     close: np.ndarray) -> Tuple[float, str]:
        """评估趋势强度"""
        if len(ema_10) < 5 or len(ema_20) < 5:
            return 50.0, "NEUTRAL"
        
        # 当前价格相对于均线的位置
        current_close = close[-1]
        current_ema_10 = ema_10[-1]
        current_ema_20 = ema_20[-1]
        
        # 均线排列
        if current_close > current_ema_10 > current_ema_20:
            # 多头排列
            trend_strength = min(100, 70 + (current_close - current_ema_20) / current_ema_20 * 1000)
            return trend_strength, "BULLISH"
        elif current_close < current_ema_10 < current_ema_20:
            # 空头排列
            trend_strength = max(0, 30 - (current_ema_20 - current_close) / current_ema_20 * 1000)
            return trend_strength, "BEARISH"
        else:
            return 50.0, "NEUTRAL"
    
    def _assess_effort_vs_result(self, recent_data: pd.DataFrame) -> Tuple[float, str]:
        """评估努力与结果 (威科夫核心概念)"""
        if len(recent_data) < 5:
            return 50.0, "数据不足"
        
        # 计算价格变化和成交量变化
        price_change = (recent_data['close'].iloc[-1] - recent_data['close'].iloc[0]) / recent_data['close'].iloc[0]
        avg_volume = recent_data['volume'].mean()
        recent_volume = recent_data['volume'].iloc[-3:].mean()
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
        
        # 威科夫努力与结果分析
        if volume_ratio > 1.5 and price_change > 0.03:
            return 90.0, "量价齐升，健康上涨"
        elif volume_ratio > 1.5 and abs(price_change) < 0.01:
            return 20.0, "巨量滞涨，警惕风险"
        elif volume_ratio < 0.8 and price_change > 0.02:
            return 30.0, "无量上涨，缺乏支撑"
        else:
            return 50.0, "量价关系正常"
    
    def _detect_spring(self, recent_data: pd.DataFrame) -> bool:
        """检测震仓信号 (Spring)"""
        if len(recent_data) < 20:
            return False
        
        # 简化的震仓检测逻辑
        # 寻找低点后的快速反弹
        lows = recent_data['low'].values
        closes = recent_data['close'].values
        
        # 找到最近的最低点
        min_idx = np.argmin(lows[-15:])
        if min_idx < 5:  # 最低点太近，不算震仓
            return False
        
        # 检查是否有快速反弹
        low_price = lows[-(15-min_idx)]
        current_price = closes[-1]
        rebound_ratio = (current_price - low_price) / low_price
        
        return rebound_ratio > 0.05  # 5%以上的反弹
    
    def _detect_sign_of_strength(self, recent_data: pd.DataFrame) -> bool:
        """检测强势发力信号 (Sign of Strength)"""
        if len(recent_data) < 10:
            return False
        
        # 检查最近几天的表现
        recent_closes = recent_data['close'].values[-5:]
        recent_volumes = recent_data['volume'].values[-5:]
        
        # 价格上涨且成交量放大
        price_trend = (recent_closes[-1] - recent_closes[0]) / recent_closes[0]
        volume_trend = recent_volumes[-1] / np.mean(recent_volumes[:-1])
        
        return price_trend > 0.03 and volume_trend > 1.3
    
    def _calculate_net_inflow(self, stock_data: pd.DataFrame) -> Dict:
        """计算净流入统计"""
        # 简化的资金流计算 (实际应使用Level-2数据)
        price_changes = stock_data['close'].pct_change()
        volume_changes = stock_data['volume'].pct_change()
        
        # 估算净流入比例
        net_inflow_ratio = np.mean(price_changes * volume_changes)
        
        # 计算连续流入天数
        consecutive_days = 0
        for i in range(len(price_changes)-1, -1, -1):
            if price_changes.iloc[i] > 0 and volume_changes.iloc[i] > 0:
                consecutive_days += 1
            else:
                break
        
        return {
            "daily_ratio": net_inflow_ratio,
            "consecutive_days": consecutive_days,
            "total_inflow": np.sum(price_changes * volume_changes)
        }
    
    def _find_upcoming_events(self, stock_data: pd.DataFrame) -> List[Dict]:
        """查找即将到来的事件 (模拟)"""
        # 模拟事件检测
        events = []
        
        # 基于日期模拟一些事件
        current_date = datetime.now()
        
        # 模拟财报季
        if current_date.month in [1, 4, 7, 10]:
            events.append({
                "type": "财报发布",
                "date": current_date + timedelta(days=np.random.randint(1, 14)),
                "impact": "HIGH"
            })
        
        return events
    
    def _calculate_rs_line(self, stock_prices: np.ndarray, 
                          benchmark_prices: np.ndarray) -> float:
        """计算相对强度线"""
        if len(stock_prices) != len(benchmark_prices) or len(stock_prices) < 20:
            return 0.0
        
        # 计算相对强度
        stock_return = (stock_prices[-1] - stock_prices[-20]) / stock_prices[-20]
        benchmark_return = (benchmark_prices[-1] - benchmark_prices[-20]) / benchmark_prices[-20]
        
        return stock_return - benchmark_return
    
    def _generate_mock_market_data(self, length: int) -> pd.DataFrame:
        """生成模拟市场数据"""
        np.random.seed(42)
        dates = pd.date_range(end=datetime.now(), periods=length, freq='D')
        prices = 3000 + np.cumsum(np.random.randn(length) * 10)
        
        return pd.DataFrame({
            'date': dates,
            'close': prices
        })
    
    # ==================== 默认配置文件 ====================
    
    def _get_default_profiles(self) -> Dict[str, AnalysisProfile]:
        """获取默认分析配置文件"""
        return {
            "technical": self._get_default_technical_profile(),
            "capital": self._get_default_capital_profile(),
            "catalyst": self._get_default_catalyst_profile(),
            "relative_strength": self._get_default_rs_profile()
        }
    
    def _get_default_technical_profile(self) -> TechnicalProfile:
        return TechnicalProfile(
            score=50.0,
            labels=["数据不足"],
            details={},
            confidence=0.0
        )
    
    def _get_default_capital_profile(self) -> CapitalProfile:
        return CapitalProfile(
            score=50.0,
            labels=["数据不足"],
            details={},
            confidence=0.0
        )
    
    def _get_default_catalyst_profile(self) -> CatalystProfile:
        return CatalystProfile(
            score=50.0,
            labels=["数据不足"],
            details={},
            confidence=0.0
        )
    
    def _get_default_rs_profile(self) -> RelativeStrengthProfile:
        return RelativeStrengthProfile(
            score=50.0,
            labels=["数据不足"],
            details={},
            confidence=0.0
        )
