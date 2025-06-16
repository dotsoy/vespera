"""
启明星策略 - 信号融合引擎
实现"资金为王，技术触发"的核心决策逻辑
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime

from .four_dimensional_analyzer import (
    AnalysisProfile, TechnicalProfile, CapitalProfile, 
    CatalystProfile, RelativeStrengthProfile
)
from src.utils.logger import get_logger

logger = get_logger("signal_fusion_engine")


@dataclass
class TradePlan:
    """交易计划"""
    stock_code: str
    stock_name: str
    conviction_score: float  # 确定性得分 0-100
    signal_class: str  # S级或A级
    rationale: str  # 核心逻辑
    entry_zone: Tuple[float, float]  # 买入区间
    stop_loss_price: float  # 止损价
    target_price: float  # 目标价
    risk_reward_ratio: float  # 风险收益比
    position_size_pct: float  # 建议仓位比例
    all_profiles_data: Dict  # 所有分析数据
    generated_time: datetime
    
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return {
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "conviction_score": self.conviction_score,
            "signal_class": self.signal_class,
            "rationale": self.rationale,
            "entry_zone_start": self.entry_zone[0],
            "entry_zone_end": self.entry_zone[1],
            "stop_loss_price": self.stop_loss_price,
            "target_price": self.target_price,
            "risk_reward_ratio": self.risk_reward_ratio,
            "position_size_pct": self.position_size_pct,
            "generated_time": self.generated_time.isoformat()
        }


@dataclass
class MarketContext:
    """市场环境上下文"""
    status: str  # BULLISH, BEARISH, NEUTRAL
    trend_strength: float  # 趋势强度 0-100
    volatility_level: str  # HIGH, MEDIUM, LOW
    risk_appetite: str  # HIGH, MEDIUM, LOW


class SignalFusionEngine:
    """信号融合引擎"""
    
    def __init__(self):
        self.logger = logger
        
        # 权重配置 - 体现"资金为王，技术触发"
        self.weights = {
            "capital": 0.45,      # 资金面权重最高
            "technical": 0.35,    # 技术面权重次之
            "relative_strength": 0.15,  # 相对强度
            "catalyst": 0.05      # 催化剂权重最低
        }
        
        # 信号质量阈值
        self.quality_thresholds = {
            "capital_min": 80,     # 资金面最低要求
            "technical_min": 75,   # 技术面最低要求
            "rs_min": 60,         # 相对强度最低要求
            "s_class_min": 90,    # S级信号最低得分
            "a_class_min": 75     # A级信号最低得分
        }
    
    def check_signal_quality(self, profiles: Dict[str, AnalysisProfile]) -> bool:
        """
        检查信号质量
        实现"资金为王，技术触发"的核心过滤逻辑
        """
        try:
            capital_profile = profiles.get("capital")
            technical_profile = profiles.get("technical")
            rs_profile = profiles.get("relative_strength")
            
            # 资金面是第一道门槛
            if not capital_profile or capital_profile.score < self.quality_thresholds["capital_min"]:
                self.logger.debug("资金面不达标")
                return False
            
            # 技术面是第二道门槛，必须有明确的看涨结构
            if not technical_profile or technical_profile.score < self.quality_thresholds["technical_min"]:
                self.logger.debug("技术面不达标")
                return False
            
            # 相对强度不能太弱
            if not rs_profile or rs_profile.score < self.quality_thresholds["rs_min"]:
                self.logger.debug("相对强度不达标")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"信号质量检查失败: {e}")
            return False
    
    def generate_trade_plan(self, stock_code: str, stock_name: str,
                          current_price: float,
                          profiles: Dict[str, AnalysisProfile],
                          market_context: MarketContext = None) -> Optional[TradePlan]:
        """
        生成交易计划
        
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            current_price: 当前价格
            profiles: 四维分析结果
            market_context: 市场环境
            
        Returns:
            交易计划或None
        """
        try:
            # 检查信号质量
            if not self.check_signal_quality(profiles):
                return None
            
            # 计算综合确定性得分
            conviction_score = self._calculate_conviction_score(profiles)
            
            # 生成核心驱动逻辑描述
            rationale = self._generate_rationale(profiles)
            
            # 计算交易计划要素
            entry_zone = self._calculate_entry_zone(current_price, profiles)
            stop_loss_price = self._calculate_stop_loss(current_price, profiles)
            target_price = self._calculate_target_price(current_price, profiles)
            
            # 计算风险收益比
            risk_reward_ratio = self._calculate_risk_reward_ratio(
                current_price, stop_loss_price, target_price
            )
            
            # 确定信号等级
            signal_class = "S级" if conviction_score >= self.quality_thresholds["s_class_min"] else "A级"
            
            # 计算建议仓位
            position_size_pct = self._calculate_position_size(
                conviction_score, risk_reward_ratio, market_context
            )
            
            return TradePlan(
                stock_code=stock_code,
                stock_name=stock_name,
                conviction_score=round(conviction_score, 1),
                signal_class=signal_class,
                rationale=rationale,
                entry_zone=entry_zone,
                stop_loss_price=stop_loss_price,
                target_price=target_price,
                risk_reward_ratio=risk_reward_ratio,
                position_size_pct=position_size_pct,
                all_profiles_data=profiles,
                generated_time=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"生成交易计划失败: {e}")
            return None
    
    def batch_generate_signals(self, stock_data_dict: Dict[str, Dict],
                             market_context: MarketContext = None) -> Dict[str, List[TradePlan]]:
        """
        批量生成交易信号
        
        Args:
            stock_data_dict: {stock_code: {"data": DataFrame, "name": str, "price": float}}
            market_context: 市场环境
            
        Returns:
            {"s_class": [TradePlan], "a_class": [TradePlan]}
        """
        from .four_dimensional_analyzer import FourDimensionalAnalyzer
        
        analyzer = FourDimensionalAnalyzer()
        s_class_signals = []
        a_class_signals = []
        
        self.logger.info(f"开始批量分析 {len(stock_data_dict)} 只股票")
        
        for stock_code, stock_info in stock_data_dict.items():
            try:
                # 执行四维分析
                profiles = analyzer.analyze_all_dimensions(
                    stock_info["data"],
                    market_data=None,  # 可以传入市场数据
                    sector_data=None   # 可以传入行业数据
                )
                
                # 生成交易计划
                trade_plan = self.generate_trade_plan(
                    stock_code=stock_code,
                    stock_name=stock_info.get("name", stock_code),
                    current_price=stock_info.get("price", stock_info["data"]["close"].iloc[-1]),
                    profiles=profiles,
                    market_context=market_context
                )
                
                if trade_plan:
                    if trade_plan.signal_class == "S级":
                        s_class_signals.append(trade_plan)
                    else:
                        a_class_signals.append(trade_plan)
                        
            except Exception as e:
                self.logger.error(f"分析股票 {stock_code} 失败: {e}")
                continue
        
        # 按确定性得分排序
        s_class_signals.sort(key=lambda x: x.conviction_score, reverse=True)
        a_class_signals.sort(key=lambda x: x.conviction_score, reverse=True)
        
        self.logger.info(f"生成信号完成: S级 {len(s_class_signals)} 个, A级 {len(a_class_signals)} 个")
        
        return {
            "s_class": s_class_signals,
            "a_class": a_class_signals
        }
    
    # ==================== 私有方法 ====================
    
    def _calculate_conviction_score(self, profiles: Dict[str, AnalysisProfile]) -> float:
        """计算综合确定性得分"""
        total_score = 0.0
        
        for dimension, weight in self.weights.items():
            profile = profiles.get(dimension)
            if profile:
                # 考虑置信度的影响
                adjusted_score = profile.score * profile.confidence
                total_score += adjusted_score * weight
            else:
                # 缺失维度按50分计算
                total_score += 50 * weight
        
        return min(total_score, 100.0)
    
    def _generate_rationale(self, profiles: Dict[str, AnalysisProfile]) -> str:
        """生成核心驱动逻辑描述"""
        capital_profile = profiles.get("capital")
        technical_profile = profiles.get("technical")
        
        capital_desc = capital_profile.labels[0] if capital_profile and capital_profile.labels else "资金面正常"
        technical_desc = technical_profile.labels[0] if technical_profile and technical_profile.labels else "技术面正常"
        
        rationale = f"核心逻辑: {capital_desc}，技术面呈现{technical_desc}。"
        
        # 添加特殊信号描述
        if technical_profile:
            if hasattr(technical_profile, 'is_spring') and technical_profile.is_spring:
                rationale += " 检测到震仓信号，后市有望反弹。"
            if hasattr(technical_profile, 'is_sos') and technical_profile.is_sos:
                rationale += " 出现强势发力信号，动能充足。"
        
        return rationale
    
    def _calculate_entry_zone(self, current_price: float, 
                            profiles: Dict[str, AnalysisProfile]) -> Tuple[float, float]:
        """计算买入区间"""
        # 基于当前价格和技术分析确定买入区间
        technical_profile = profiles.get("technical")
        
        if technical_profile and hasattr(technical_profile, 'atr'):
            atr = technical_profile.atr
            # 买入区间为当前价格 ± 0.5 ATR
            entry_start = current_price - 0.5 * atr
            entry_end = current_price + 0.5 * atr
        else:
            # 默认买入区间为当前价格 ± 2%
            entry_start = current_price * 0.98
            entry_end = current_price * 1.02
        
        return (round(entry_start, 2), round(entry_end, 2))
    
    def _calculate_stop_loss(self, current_price: float,
                           profiles: Dict[str, AnalysisProfile]) -> float:
        """计算止损价"""
        technical_profile = profiles.get("technical")
        
        if technical_profile and hasattr(technical_profile, 'atr'):
            atr = technical_profile.atr
            # 止损设置为 2 ATR
            stop_loss = current_price - 2 * atr
        else:
            # 默认止损为 5%
            stop_loss = current_price * 0.95
        
        return round(stop_loss, 2)
    
    def _calculate_target_price(self, current_price: float,
                              profiles: Dict[str, AnalysisProfile]) -> float:
        """计算目标价"""
        # 基于风险收益比计算目标价
        stop_loss = self._calculate_stop_loss(current_price, profiles)
        risk = current_price - stop_loss
        
        # 目标风险收益比 2:1
        target_reward = risk * 2
        target_price = current_price + target_reward
        
        return round(target_price, 2)
    
    def _calculate_risk_reward_ratio(self, current_price: float,
                                   stop_loss: float, target_price: float) -> float:
        """计算风险收益比"""
        risk = current_price - stop_loss
        reward = target_price - current_price
        
        if risk <= 0:
            return 0.0
        
        return round(reward / risk, 2)
    
    def _calculate_position_size(self, conviction_score: float,
                               risk_reward_ratio: float,
                               market_context: MarketContext = None) -> float:
        """计算建议仓位比例"""
        # 基础仓位
        base_position = 0.1  # 10%
        
        # 根据确定性得分调整
        conviction_multiplier = conviction_score / 100
        
        # 根据风险收益比调整
        rr_multiplier = min(risk_reward_ratio / 2, 1.5)  # 最大1.5倍
        
        # 根据市场环境调整
        market_multiplier = 1.0
        if market_context:
            if market_context.status == "BULLISH":
                market_multiplier = 1.2
            elif market_context.status == "BEARISH":
                market_multiplier = 0.8
        
        position_size = base_position * conviction_multiplier * rr_multiplier * market_multiplier
        
        # 限制最大仓位
        return min(round(position_size, 3), 0.25)  # 最大25%
    
    def get_market_context(self, market_data: pd.DataFrame = None) -> MarketContext:
        """获取市场环境上下文"""
        # 简化的市场环境判断
        # 实际应该基于多个市场指标综合判断
        
        if market_data is None:
            # 默认牛市背景
            return MarketContext(
                status="BULLISH",
                trend_strength=75.0,
                volatility_level="MEDIUM",
                risk_appetite="MEDIUM"
            )
        
        # 基于市场数据判断
        # 这里可以添加更复杂的市场环境分析逻辑
        recent_returns = market_data['close'].pct_change().tail(20)
        avg_return = recent_returns.mean()
        volatility = recent_returns.std()
        
        if avg_return > 0.001:  # 日均涨幅 > 0.1%
            status = "BULLISH"
            trend_strength = min(100, 50 + avg_return * 10000)
        elif avg_return < -0.001:  # 日均跌幅 > 0.1%
            status = "BEARISH"
            trend_strength = max(0, 50 + avg_return * 10000)
        else:
            status = "NEUTRAL"
            trend_strength = 50.0
        
        volatility_level = "HIGH" if volatility > 0.02 else "MEDIUM" if volatility > 0.01 else "LOW"
        
        return MarketContext(
            status=status,
            trend_strength=trend_strength,
            volatility_level=volatility_level,
            risk_appetite="MEDIUM"
        )
