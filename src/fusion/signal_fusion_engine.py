"""
信号融合引擎 - "资金为王，技术触发"核心逻辑
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from loguru import logger

from config.settings import analysis_settings
from src.utils.database import get_db_manager
from src.analyzers.technical_analyzer import TechnicalAnalyzer
from src.analyzers.capital_flow_analyzer import CapitalFlowAnalyzer
from src.analyzers.fundamental_analyzer import FundamentalAnalyzer
from src.analyzers.macro_analyzer import MacroAnalyzer


class SignalFusionEngine:
    """信号融合引擎"""
    
    def __init__(self):
        self.db_manager = get_db_manager()
        
        # 初始化各分析器
        self.technical_analyzer = TechnicalAnalyzer()
        self.capital_analyzer = CapitalFlowAnalyzer()
        self.fundamental_analyzer = FundamentalAnalyzer()
        self.macro_analyzer = MacroAnalyzer()

        # 融合权重配置 (四维分析)
        self.fusion_weights = {
            'capital_flow': 0.40,      # 资金流权重最高
            'technical': 0.35,         # 技术面权重
            'fundamental': 0.20,       # 基本面权重
            'macro': 0.05             # 宏观面权重
        }
        
        # 阈值配置
        self.min_capital_score = analysis_settings.min_capital_score
        self.min_technical_score = analysis_settings.min_technical_score
        self.signal_confidence_threshold = analysis_settings.signal_confidence_threshold
        
        logger.info("信号融合引擎初始化完成")
    
    def calculate_capital_composite_score(self, capital_result: Dict[str, Any]) -> float:
        """
        计算资金流综合评分
        
        Args:
            capital_result: 资金流分析结果
            
        Returns:
            资金流综合评分 (0-1)
        """
        try:
            if not capital_result:
                return 0.0
            
            # 各项指标权重
            weights = {
                'main_force_score': 0.4,
                'institutional_activity': 0.25,
                'flow_consistency': 0.2,
                'volume_price_correlation': 0.15
            }
            
            composite_score = 0.0
            for indicator, weight in weights.items():
                score = capital_result.get(indicator, 0.0)
                composite_score += score * weight
            
            return composite_score
            
        except Exception as e:
            logger.error(f"计算资金流综合评分失败: {e}")
            return 0.0
    
    def calculate_technical_composite_score(self, technical_result: Dict[str, Any]) -> float:
        """
        计算技术面综合评分
        
        Args:
            technical_result: 技术分析结果
            
        Returns:
            技术面综合评分 (0-1)
        """
        try:
            if not technical_result:
                return 0.0
            
            # 各项指标权重
            weights = {
                'trend_score': 0.4,
                'momentum_score': 0.3,
                'volume_health_score': 0.2,
                'pattern_score': 0.1
            }
            
            composite_score = 0.0
            for indicator, weight in weights.items():
                score = technical_result.get(indicator, 0.0)
                composite_score += score * weight
            
            return composite_score
            
        except Exception as e:
            logger.error(f"计算技术面综合评分失败: {e}")
            return 0.0
    
    def calculate_fundamental_composite_score(self, fundamental_result: Dict[str, Any]) -> float:
        """
        计算基本面综合评分
        
        Args:
            fundamental_result: 基本面分析结果
            
        Returns:
            基本面综合评分 (0-1)
        """
        try:
            if not fundamental_result:
                return 0.0
            
            # 各项指标权重
            weights = {
                'catalyst_score': 0.4,
                'announcement_impact': 0.3,
                'industry_momentum': 0.2,
                'news_sentiment': 0.1
            }
            
            composite_score = 0.0
            for indicator, weight in weights.items():
                score = fundamental_result.get(indicator, 0.0)
                composite_score += score * weight
            
            return composite_score
            
        except Exception as e:
            logger.error(f"计算基本面综合评分失败: {e}")
            return 0.0
    

    
    def calculate_macro_composite_score(self, macro_result: Dict[str, Any]) -> float:
        """
        计算宏观面综合评分
        
        Args:
            macro_result: 宏观分析结果
            
        Returns:
            宏观面综合评分 (0-1)
        """
        try:
            if not macro_result:
                return 0.5  # 中性
            
            # 各项指标权重
            weights = {
                'market_regime': 0.4,
                'risk_appetite': 0.3,
                'liquidity_condition': 0.2,
                'sector_rotation': 0.1
            }
            
            composite_score = 0.0
            for indicator, weight in weights.items():
                score = macro_result.get(indicator, 0.5)
                composite_score += score * weight
            
            return composite_score
            
        except Exception as e:
            logger.error(f"计算宏观面综合评分失败: {e}")
            return 0.5
    
    def apply_fusion_logic(self, analysis_results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        应用"资金为王，技术触发"融合逻辑

        Args:
            analysis_results: 各维度分析结果

        Returns:
            融合后的信号，如果不满足条件则返回None
        """
        try:
            # 计算各维度综合评分
            capital_score = self.calculate_capital_composite_score(
                analysis_results.get('capital_flow', {})
            )
            technical_score = self.calculate_technical_composite_score(
                analysis_results.get('technical', {})
            )
            fundamental_score = self.calculate_fundamental_composite_score(
                analysis_results.get('fundamental', {})
            )
            macro_score = self.calculate_macro_composite_score(
                analysis_results.get('macro', {})
            )

            # 第一步：资金面过滤
            if capital_score < self.min_capital_score:
                logger.debug(f"资金面评分 {capital_score:.3f} 低于阈值 {self.min_capital_score}")
                return None

            # 第二步：技术面触发
            if technical_score < self.min_technical_score:
                logger.debug(f"技术面评分 {technical_score:.3f} 低于阈值 {self.min_technical_score}")
                return None

            # 第三步：计算综合置信度 (四维融合)
            confidence_score = (
                capital_score * self.fusion_weights['capital_flow'] +
                technical_score * self.fusion_weights['technical'] +
                fundamental_score * self.fusion_weights['fundamental'] +
                macro_score * self.fusion_weights['macro']
            )

            # 第四步：置信度过滤
            if confidence_score < self.signal_confidence_threshold:
                logger.debug(f"综合置信度 {confidence_score:.3f} 低于阈值 {self.signal_confidence_threshold}")
                return None

            # 生成信号
            signal = {
                'signal_type': 'BUY',
                'confidence_score': confidence_score,
                'technical_score': technical_score,
                'capital_score': capital_score,
                'fundamental_score': fundamental_score,
                'macro_score': macro_score,
                'analysis_results': analysis_results
            }

            return signal

        except Exception as e:
            logger.error(f"应用融合逻辑失败: {e}")
            return None
    
    def calculate_position_sizing(self, signal: Dict[str, Any], 
                                current_price: float) -> Dict[str, float]:
        """
        计算仓位管理参数
        
        Args:
            signal: 交易信号
            current_price: 当前价格
            
        Returns:
            仓位管理参数
        """
        try:
            confidence = signal['confidence_score']
            technical_score = signal['technical_score']
            
            # 基础仓位 (基于置信度)
            base_position = min(0.1, confidence * 0.15)  # 最大10%
            
            # 技术面调整
            if technical_score > 0.8:
                base_position *= 1.2
            elif technical_score < 0.6:
                base_position *= 0.8
            
            # 计算止损位 (基于技术分析结果)
            technical_result = signal['analysis_results'].get('technical', {})
            support_level = technical_result.get('support_level')
            
            if support_level and support_level > 0:
                stop_loss = support_level * 0.98  # 支撑位下方2%
            else:
                stop_loss = current_price * 0.92  # 默认8%止损
            
            # 计算目标价 (基于风险收益比)
            risk_amount = current_price - stop_loss
            target_price = current_price + risk_amount * 2.5  # 1:2.5风险收益比
            
            # 风险收益比
            risk_reward_ratio = (target_price - current_price) / risk_amount if risk_amount > 0 else 2.5
            
            return {
                'position_size': round(base_position, 3),
                'entry_price': current_price,
                'stop_loss': round(stop_loss, 2),
                'target_price': round(target_price, 2),
                'risk_reward_ratio': round(risk_reward_ratio, 2)
            }
            
        except Exception as e:
            logger.error(f"计算仓位管理参数失败: {e}")
            return {
                'position_size': 0.05,
                'entry_price': current_price,
                'stop_loss': current_price * 0.92,
                'target_price': current_price * 1.15,
                'risk_reward_ratio': 2.0
            }
    
    def generate_signal_reason(self, signal: Dict[str, Any]) -> str:
        """
        生成信号原因描述

        Args:
            signal: 交易信号

        Returns:
            信号原因文本
        """
        try:
            reasons = []

            # 资金流原因
            if signal['capital_score'] > 0.8:
                reasons.append("主力资金大幅流入")
            elif signal['capital_score'] > 0.7:
                reasons.append("资金面表现强势")

            # 技术面原因
            if signal['technical_score'] > 0.8:
                reasons.append("技术形态突破")
            elif signal['technical_score'] > 0.7:
                reasons.append("技术指标向好")

            # 基本面原因
            if signal['fundamental_score'] > 0.7:
                reasons.append("基本面催化剂明确")

            # 宏观面原因
            if signal['macro_score'] > 0.7:
                reasons.append("宏观环境有利")

            if not reasons:
                reasons.append("四维度指标综合向好")

            return "；".join(reasons)

        except Exception as e:
            logger.error(f"生成信号原因失败: {e}")
            return "综合分析结果积极"
    
    def analyze_and_generate_signal(self, ts_code: str, trade_date: str) -> Optional[Dict[str, Any]]:
        """
        分析股票并生成交易信号
        
        Args:
            ts_code: 股票代码
            trade_date: 分析日期
            
        Returns:
            交易信号或None
        """
        try:
            logger.info(f"开始分析股票 {ts_code}")
            
            # 获取当前价格
            price_query = """
            SELECT close_price FROM stock_daily_quotes 
            WHERE ts_code = :ts_code AND trade_date = :trade_date
            """
            
            price_data = self.db_manager.execute_postgres_query(
                price_query, {'ts_code': ts_code, 'trade_date': trade_date}
            )
            
            if price_data.empty:
                logger.warning(f"股票 {ts_code} 无价格数据")
                return None
            
            current_price = float(price_data.iloc[0]['close_price'])
            
            # 执行各维度分析
            analysis_results = {}
            
            # 技术分析
            technical_result = self.technical_analyzer.analyze_stock(ts_code, trade_date)
            if technical_result:
                analysis_results['technical'] = technical_result
            
            # 资金流分析
            capital_result = self.capital_analyzer.analyze_stock(ts_code, trade_date)
            if capital_result:
                analysis_results['capital_flow'] = capital_result
            
            # 基本面分析
            fundamental_result = self.fundamental_analyzer.analyze_stock(ts_code, trade_date)
            if fundamental_result:
                analysis_results['fundamental'] = fundamental_result
            

            
            # 应用融合逻辑
            signal = self.apply_fusion_logic(analysis_results)
            
            if signal is None:
                logger.debug(f"股票 {ts_code} 未通过融合逻辑筛选")
                return None
            
            # 计算仓位管理参数
            position_params = self.calculate_position_sizing(signal, current_price)
            
            # 生成信号原因
            signal_reason = self.generate_signal_reason(signal)
            
            # 构建最终信号
            final_signal = {
                'ts_code': ts_code,
                'trade_date': trade_date,
                'signal_type': signal['signal_type'],
                'confidence_score': signal['confidence_score'],
                'technical_score': signal['technical_score'],
                'capital_score': signal['capital_score'],
                'fundamental_score': signal['fundamental_score'],
                'macro_score': signal['macro_score'],
                'entry_price': position_params['entry_price'],
                'stop_loss': position_params['stop_loss'],
                'target_price': position_params['target_price'],
                'risk_reward_ratio': position_params['risk_reward_ratio'],
                'position_size': position_params['position_size'],
                'signal_reason': signal_reason,
                # 将字典转换为 JSON 字符串，避免数据库插入问题
                'signal_data': str(analysis_results) if analysis_results else None,
                'is_active': True
            }
            
            logger.info(f"股票 {ts_code} 生成交易信号，置信度: {signal['confidence_score']:.3f}")
            return final_signal
            
        except Exception as e:
            logger.error(f"分析股票 {ts_code} 并生成信号失败: {e}")
            return None


if __name__ == "__main__":
    # 测试信号融合引擎
    engine = SignalFusionEngine()
    
    # 测试生成信号
    signal = engine.analyze_and_generate_signal('000001.SZ', '2024-12-01')
    if signal:
        print("生成的交易信号:", signal)
    else:
        print("未生成交易信号")
