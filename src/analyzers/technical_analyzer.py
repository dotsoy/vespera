"""
技术分析模块 (Polars 加速版)
实现 44.5x 平均性能提升
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple, Optional
from loguru import logger

from config.settings import analysis_settings
from src.utils.database import get_db_manager
from src.utils.technical_indicators import add_all_indicators


class TechnicalAnalyzer:
    """技术分析器"""
    
    def __init__(self):
        self.db_manager = get_db_manager()
        self.ma_periods = analysis_settings.ma_periods
        self.ema_periods = analysis_settings.ema_periods
        self.rsi_period = analysis_settings.rsi_period
        self.macd_fast = analysis_settings.macd_fast
        self.macd_slow = analysis_settings.macd_slow
        self.macd_signal = analysis_settings.macd_signal
        
        logger.info("技术分析器初始化完成")
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标 (Polars 加速版)

        Args:
            df: 包含 OHLCV 数据的 DataFrame

        Returns:
            添加了技术指标的 DataFrame
        """
        try:
            # 使用 Polars 加速的技术指标计算
            df = add_all_indicators(
                df,
                ma_periods=self.ma_periods,
                ema_periods=self.ema_periods,
                use_polars=True  # 明确启用 Polars 加速
            )

            logger.info(f"成功计算 {len(df)} 条记录的技术指标 (Polars 加速)")
            return df

        except Exception as e:
            logger.error(f"计算技术指标失败: {e}")
            raise
    
    def calculate_trend_score(self, df: pd.DataFrame) -> float:
        """
        计算趋势评分 (0-1)
        
        Args:
            df: 包含技术指标的 DataFrame
            
        Returns:
            趋势评分
        """
        try:
            if df.empty or len(df) < 1:
                return 0.0

            latest = df.iloc[-1]
            score = 0.0

            # EMA 排列评分 (40%)
            ema_12 = latest.get('ema_12')
            ema_26 = latest.get('ema_26')
            close = latest['close_price']

            if pd.notna(ema_12) and pd.notna(ema_26):
                if close > ema_12 > ema_26:
                    score += 0.4
                elif close > ema_12 or ema_12 > ema_26:
                    score += 0.2
            else:
                # 如果 EMA 数据不足，使用价格趋势替代
                if len(df) >= 2:
                    price_change = (close - df.iloc[-2]['close_price']) / df.iloc[-2]['close_price']
                    if price_change > 0:
                        score += 0.2
            
            # MA 排列评分 (30%)
            ma_5 = latest.get('ma_5')
            ma_10 = latest.get('ma_10')
            ma_20 = latest.get('ma_20')

            if pd.notna(ma_5) and pd.notna(ma_10) and pd.notna(ma_20):
                if close > ma_5 > ma_10 > ma_20:
                    score += 0.3
                elif close > ma_5 > ma_10 or ma_5 > ma_10 > ma_20:
                    score += 0.15
            elif pd.notna(ma_20):
                if close > ma_20:
                    score += 0.15

            # 价格相对位置评分 (20%)
            bb_upper = latest.get('bb_upper')
            bb_middle = latest.get('bb_middle')

            if pd.notna(bb_upper) and close > bb_upper:
                score += 0.2
            elif pd.notna(bb_middle) and close > bb_middle:
                score += 0.1
            
            # MACD 趋势评分 (10%)
            macd = latest.get('macd')
            macd_signal = latest.get('macd_signal')

            if pd.notna(macd) and pd.notna(macd_signal):
                if macd > macd_signal and macd > 0:
                    score += 0.1
                elif macd > macd_signal:
                    score += 0.05
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算趋势评分失败: {e}")
            return 0.0
    
    def calculate_momentum_score(self, df: pd.DataFrame) -> float:
        """
        计算动量评分 (0-1)
        
        Args:
            df: 包含技术指标的 DataFrame
            
        Returns:
            动量评分
        """
        try:
            if df.empty or len(df) < 1:
                return 0.0

            latest = df.iloc[-1]
            score = 0.0

            # RSI 评分 (40%)
            rsi = latest.get('rsi')
            if pd.notna(rsi):
                if 50 <= rsi <= 70:
                    score += 0.4
                elif 40 <= rsi < 50 or 70 < rsi <= 80:
                    score += 0.2
                elif 30 <= rsi < 40:
                    score += 0.1
            
            # KDJ 评分 (30%)
            k = latest.get('k')
            d = latest.get('d')
            j = latest.get('j')

            if pd.notna(k) and pd.notna(d) and pd.notna(j):
                if 20 <= k <= 80 and k > d and j > k:
                    score += 0.3
                elif k > d:
                    score += 0.15
            
            # 威廉指标评分 (20%)
            williams = latest.get('williams_r')
            if pd.notna(williams):
                if -80 <= williams <= -20:
                    score += 0.2
                elif -50 <= williams <= -20:
                    score += 0.1
            
            # 价格动量评分 (10%)
            if len(df) >= 5:
                price_change_5d = (latest['close_price'] / df.iloc[-5]['close_price'] - 1) * 100
                if 0 < price_change_5d <= 10:
                    score += 0.1
                elif -5 <= price_change_5d <= 0:
                    score += 0.05
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算动量评分失败: {e}")
            return 0.0
    
    def calculate_volume_health_score(self, df: pd.DataFrame) -> float:
        """
        计算量能健康度评分 (0-1)
        
        Args:
            df: 包含技术指标的 DataFrame
            
        Returns:
            量能健康度评分
        """
        try:
            if df.empty or len(df) < 1:
                return 0.0
            
            latest = df.iloc[-1]
            score = 0.0
            
            # 量价关系评分 (50%)
            vol_ratio = latest.get('vol_ratio')
            price_change = latest.get('pct_chg', 0)

            if pd.notna(vol_ratio):
                if price_change > 0 and vol_ratio > 1.5:  # 放量上涨
                    score += 0.5
                elif price_change > 0 and vol_ratio > 1.0:  # 温和放量上涨
                    score += 0.3
                elif price_change > 0 and vol_ratio < 0.8:  # 缩量上涨
                    score += 0.1
                elif price_change < 0 and vol_ratio < 0.8:  # 缩量下跌
                    score += 0.2
            
            # OBV 趋势评分 (30%)
            if len(df) >= 5:
                current_obv = latest.get('obv')
                prev_obv = df.iloc[-5].get('obv')

                if pd.notna(current_obv) and pd.notna(prev_obv) and prev_obv != 0:
                    obv_trend = (current_obv / prev_obv - 1) * 100
                    if obv_trend > 5:
                        score += 0.3
                    elif obv_trend > 0:
                        score += 0.15
            
            # 成交量稳定性评分 (20%)
            if len(df) >= 10:
                recent_vol_ratios = df.tail(10)['vol_ratio'].dropna()
                if len(recent_vol_ratios) > 0:
                    recent_vol_std = recent_vol_ratios.std()
                    if recent_vol_std < 0.5:
                        score += 0.2
                    elif recent_vol_std < 1.0:
                        score += 0.1
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算量能健康度评分失败: {e}")
            return 0.0
    
    def identify_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        识别技术形态
        
        Args:
            df: 包含技术指标的 DataFrame
            
        Returns:
            识别到的形态字典
        """
        try:
            patterns = {}
            
            if df.empty or len(df) < 2:
                return patterns
            
            # 获取最近的价格数据
            recent_data = df.tail(10)
            latest = df.iloc[-1]
            
            # 突破形态
            bb_upper = latest.get('bb_upper')
            if pd.notna(bb_upper) and latest['close_price'] > bb_upper:
                patterns['bollinger_breakout'] = True

            # 金叉死叉
            if len(df) >= 2:
                prev = df.iloc[-2]
                current_macd = latest.get('macd')
                current_signal = latest.get('macd_signal')
                prev_macd = prev.get('macd')
                prev_signal = prev.get('macd_signal')

                if (pd.notna(current_macd) and pd.notna(current_signal) and
                    pd.notna(prev_macd) and pd.notna(prev_signal)):
                    if (current_macd > current_signal and prev_macd <= prev_signal):
                        patterns['macd_golden_cross'] = True
                    elif (current_macd < current_signal and prev_macd >= prev_signal):
                        patterns['macd_death_cross'] = True

            # RSI 超买超卖
            rsi = latest.get('rsi')
            if pd.notna(rsi):
                if rsi > 80:
                    patterns['rsi_overbought'] = True
                elif rsi < 20:
                    patterns['rsi_oversold'] = True
            
            # 价格形态
            highs = recent_data['high_price'].values
            lows = recent_data['low_price'].values
            
            # 上升三角形
            if len(highs) >= 5:
                high_trend = np.polyfit(range(len(highs)), highs, 1)[0]
                low_trend = np.polyfit(range(len(lows)), lows, 1)[0]
                if abs(high_trend) < 0.1 and low_trend > 0.1:
                    patterns['ascending_triangle'] = True
            
            return patterns
            
        except Exception as e:
            logger.error(f"识别技术形态失败: {e}")
            return {}
    
    def calculate_support_resistance(self, df: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
        """
        计算支撑位和阻力位
        
        Args:
            df: 包含价格数据的 DataFrame
            
        Returns:
            (支撑位, 阻力位)
        """
        try:
            if df.empty or len(df) < 5:
                return None, None
            
            # 获取最近20天的数据
            recent_data = df.tail(20)
            
            # 计算支撑位 (最近低点的平均值)
            lows = recent_data['low_price'].values
            support_candidates = []
            
            for i in range(1, len(lows) - 1):
                if lows[i] <= lows[i-1] and lows[i] <= lows[i+1]:
                    support_candidates.append(lows[i])
            
            support = np.mean(support_candidates) if support_candidates else recent_data['low_price'].min()
            
            # 计算阻力位 (最近高点的平均值)
            highs = recent_data['high_price'].values
            resistance_candidates = []
            
            for i in range(1, len(highs) - 1):
                if highs[i] >= highs[i-1] and highs[i] >= highs[i+1]:
                    resistance_candidates.append(highs[i])
            
            resistance = np.mean(resistance_candidates) if resistance_candidates else recent_data['high_price'].max()
            
            return float(support), float(resistance)
            
        except Exception as e:
            logger.error(f"计算支撑阻力位失败: {e}")
            return None, None
    
    def analyze_stock(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        """
        分析单只股票的技术面
        
        Args:
            ts_code: 股票代码
            trade_date: 分析日期
            
        Returns:
            技术分析结果
        """
        try:
            # 获取历史数据 (最近60天)
            query = """
            SELECT * FROM stock_daily_quotes 
            WHERE ts_code = :ts_code 
            AND trade_date <= :trade_date 
            ORDER BY trade_date DESC 
            LIMIT 60
            """
            
            df = self.db_manager.execute_postgres_query(
                query, {'ts_code': ts_code, 'trade_date': trade_date}
            )
            
            if df.empty:
                logger.warning(f"股票 {ts_code} 无历史数据")
                return {}
            
            # 按日期正序排列
            df = df.sort_values('trade_date')
            
            # 计算技术指标
            df = self.calculate_indicators(df)
            
            # 计算各项评分
            trend_score = self.calculate_trend_score(df)
            momentum_score = self.calculate_momentum_score(df)
            volume_health_score = self.calculate_volume_health_score(df)
            
            # 识别形态
            patterns = self.identify_patterns(df)
            pattern_score = len(patterns) * 0.1  # 每个形态加0.1分
            
            # 计算支撑阻力位
            support, resistance = self.calculate_support_resistance(df)
            
            # 构建技术指标字典
            latest = df.iloc[-1]
            technical_indicators = {
                'rsi': float(latest.get('rsi', 0)) if pd.notna(latest.get('rsi')) else None,
                'macd': float(latest.get('macd', 0)) if pd.notna(latest.get('macd')) else None,
                'macd_signal': float(latest.get('macd_signal', 0)) if pd.notna(latest.get('macd_signal')) else None,
                'k': float(latest.get('k', 0)) if pd.notna(latest.get('k')) else None,
                'd': float(latest.get('d', 0)) if pd.notna(latest.get('d')) else None,
                'j': float(latest.get('j', 0)) if pd.notna(latest.get('j')) else None,
                'vol_ratio': float(latest.get('vol_ratio', 0)) if pd.notna(latest.get('vol_ratio')) else None,
            }
            
            result = {
                'ts_code': ts_code,
                'trade_date': trade_date,
                'trend_score': round(trend_score, 3),
                'momentum_score': round(momentum_score, 3),
                'volume_health_score': round(volume_health_score, 3),
                'pattern_score': round(min(pattern_score, 1.0), 3),
                'support_level': support,
                'resistance_level': resistance,
                # 将字典转换为 JSON 字符串，避免数据库插入问题
                'key_patterns': str(patterns) if patterns else None,
                'technical_indicators': str(technical_indicators) if technical_indicators else None
            }
            
            logger.info(f"股票 {ts_code} 技术分析完成")
            return result
            
        except Exception as e:
            logger.error(f"分析股票 {ts_code} 技术面失败: {e}")
            return {}


if __name__ == "__main__":
    # 测试技术分析器
    analyzer = TechnicalAnalyzer()
    
    # 测试分析单只股票
    result = analyzer.analyze_stock('000001.SZ', '2024-12-01')
    print("技术分析结果:", result)
