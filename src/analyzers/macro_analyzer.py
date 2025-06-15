"""
宏观环境分析器 - 判断市场环境与行业轮动
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from loguru import logger

from config.settings import analysis_settings
from src.utils.database import get_db_manager


class MacroAnalyzer:
    """宏观环境分析器"""
    
    def __init__(self):
        self.db_manager = get_db_manager()
        
        # 主要指数代码
        self.major_indices = {
            '000001.SH': '上证指数',
            '399001.SZ': '深证成指',
            '399006.SZ': '创业板指',
            '000300.SH': '沪深300',
            '000905.SH': '中证500'
        }
        
        # 行业板块代码 (示例)
        self.sector_indices = {
            '银行': '399986.SZ',
            '房地产': '399393.SZ', 
            '医药生物': '399394.SZ',
            '电子': '399396.SZ',
            '计算机': '399397.SZ',
            '新能源': '399808.SZ'
        }
        
        logger.info("宏观环境分析器初始化完成")
    
    def get_market_indices_data(self, trade_date: str, days: int = 20) -> pd.DataFrame:
        """
        获取市场指数数据
        
        Args:
            trade_date: 交易日期
            days: 获取天数
            
        Returns:
            指数数据DataFrame
        """
        try:
            # 构建查询条件
            index_codes = list(self.major_indices.keys())
            placeholders = ','.join([f"'{code}'" for code in index_codes])
            
            query = f"""
            SELECT ts_code, trade_date, close_price, pct_chg, vol, amount
            FROM stock_daily_quotes 
            WHERE ts_code IN ({placeholders})
            AND trade_date <= :trade_date 
            ORDER BY trade_date DESC 
            LIMIT :limit
            """
            
            df = self.db_manager.execute_postgres_query(
                query, {'trade_date': trade_date, 'limit': len(index_codes) * days}
            )
            
            return df.sort_values(['ts_code', 'trade_date'])
            
        except Exception as e:
            logger.error(f"获取市场指数数据失败: {e}")
            return pd.DataFrame()
    
    def get_sector_performance(self, trade_date: str, days: int = 5) -> Dict[str, float]:
        """
        获取行业板块表现
        
        Args:
            trade_date: 交易日期
            days: 统计天数
            
        Returns:
            行业表现字典
        """
        try:
            sector_performance = {}
            
            # 模拟行业表现数据 (实际应从真实数据源获取)
            sectors = ['银行', '房地产', '医药生物', '电子', '计算机', '新能源', 
                      '军工', '消费', '有色金属', '化工', '机械设备', '汽车']
            
            for sector in sectors:
                # 生成模拟的行业涨跌幅
                performance = np.random.normal(0, 2)  # 均值0，标准差2的正态分布
                sector_performance[sector] = round(performance, 2)
            
            return sector_performance
            
        except Exception as e:
            logger.error(f"获取行业表现失败: {e}")
            return {}
    
    def calculate_market_regime(self, indices_data: pd.DataFrame) -> float:
        """
        计算市场状态评分
        
        Args:
            indices_data: 指数数据
            
        Returns:
            市场状态评分 (0-1，0为熊市，1为牛市)
        """
        try:
            if indices_data.empty:
                return 0.5
            
            score = 0.0
            
            # 主要指数表现 (40%)
            main_indices = ['000001.SH', '399001.SZ', '000300.SH']
            index_scores = []
            
            for index_code in main_indices:
                index_data = indices_data[indices_data['ts_code'] == index_code]
                if len(index_data) >= 10:
                    # 计算短期趋势 (5日)
                    recent_5 = index_data.tail(5)['pct_chg'].mean()
                    
                    # 计算中期趋势 (20日)
                    recent_20 = index_data.tail(20)['pct_chg'].mean()
                    
                    # 趋势评分
                    trend_score = 0.5
                    if recent_5 > 1 and recent_20 > 0:
                        trend_score = 0.8
                    elif recent_5 > 0 and recent_20 > 0:
                        trend_score = 0.7
                    elif recent_5 > 0:
                        trend_score = 0.6
                    elif recent_5 < -1 and recent_20 < 0:
                        trend_score = 0.2
                    elif recent_5 < 0 and recent_20 < 0:
                        trend_score = 0.3
                    elif recent_5 < 0:
                        trend_score = 0.4
                    
                    index_scores.append(trend_score)
            
            if index_scores:
                score += np.mean(index_scores) * 0.4
            
            # 市场波动率 (30%)
            all_changes = indices_data['pct_chg'].dropna()
            if len(all_changes) > 0:
                volatility = all_changes.std()
                # 低波动率通常表示稳定的上涨环境
                if volatility < 1.0:
                    score += 0.3
                elif volatility < 1.5:
                    score += 0.2
                elif volatility < 2.0:
                    score += 0.1
            
            # 成交量变化 (30%)
            volume_changes = []
            for index_code in main_indices:
                index_data = indices_data[indices_data['ts_code'] == index_code]
                if len(index_data) >= 10:
                    recent_vol = index_data.tail(5)['vol'].mean()
                    avg_vol = index_data.tail(20)['vol'].mean()
                    if avg_vol > 0:
                        vol_ratio = recent_vol / avg_vol
                        volume_changes.append(vol_ratio)
            
            if volume_changes:
                avg_vol_change = np.mean(volume_changes)
                if avg_vol_change > 1.2:  # 放量
                    score += 0.3
                elif avg_vol_change > 1.0:
                    score += 0.2
                elif avg_vol_change > 0.8:
                    score += 0.1
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算市场状态失败: {e}")
            return 0.5
    
    def calculate_sector_rotation(self, sector_performance: Dict[str, float]) -> float:
        """
        计算行业轮动评分
        
        Args:
            sector_performance: 行业表现数据
            
        Returns:
            行业轮动评分 (0-1)
        """
        try:
            if not sector_performance:
                return 0.5
            
            performances = list(sector_performance.values())
            
            # 计算行业表现分化程度
            performance_std = np.std(performances)
            performance_range = max(performances) - min(performances)
            
            # 分化程度评分 (50%)
            score = 0.0
            if performance_range > 5:  # 行业分化明显
                score += 0.5
            elif performance_range > 3:
                score += 0.3
            elif performance_range > 1:
                score += 0.1
            
            # 强势行业数量 (30%)
            strong_sectors = sum(1 for p in performances if p > 2)
            weak_sectors = sum(1 for p in performances if p < -2)
            
            if strong_sectors >= 3:
                score += 0.3
            elif strong_sectors >= 2:
                score += 0.2
            elif strong_sectors >= 1:
                score += 0.1
            
            # 轮动活跃度 (20%)
            # 这里简化处理，实际应该比较不同时期的行业排名变化
            if performance_std > 2:
                score += 0.2
            elif performance_std > 1:
                score += 0.1
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算行业轮动失败: {e}")
            return 0.5
    
    def calculate_risk_appetite(self, indices_data: pd.DataFrame, 
                              sector_performance: Dict[str, float]) -> float:
        """
        计算风险偏好评分
        
        Args:
            indices_data: 指数数据
            sector_performance: 行业表现
            
        Returns:
            风险偏好评分 (0-1)
        """
        try:
            score = 0.0
            
            # 成长股vs价值股表现 (40%)
            growth_sectors = ['计算机', '电子', '新能源', '医药生物']
            value_sectors = ['银行', '房地产', '有色金属', '化工']
            
            growth_performance = np.mean([
                sector_performance.get(sector, 0) for sector in growth_sectors
            ])
            value_performance = np.mean([
                sector_performance.get(sector, 0) for sector in value_sectors
            ])
            
            if growth_performance > value_performance + 1:
                score += 0.4  # 高风险偏好
            elif growth_performance > value_performance:
                score += 0.3
            elif abs(growth_performance - value_performance) < 0.5:
                score += 0.2  # 中性
            else:
                score += 0.1  # 低风险偏好
            
            # 创业板相对表现 (35%)
            if not indices_data.empty:
                cyb_data = indices_data[indices_data['ts_code'] == '399006.SZ']
                sh_data = indices_data[indices_data['ts_code'] == '000001.SH']
                
                if not cyb_data.empty and not sh_data.empty:
                    cyb_change = cyb_data.tail(5)['pct_chg'].mean()
                    sh_change = sh_data.tail(5)['pct_chg'].mean()
                    
                    relative_performance = cyb_change - sh_change
                    if relative_performance > 1:
                        score += 0.35
                    elif relative_performance > 0:
                        score += 0.25
                    elif relative_performance > -1:
                        score += 0.15
                    else:
                        score += 0.05
            
            # 小盘股活跃度 (25%)
            if not indices_data.empty:
                zz500_data = indices_data[indices_data['ts_code'] == '000905.SH']
                if not zz500_data.empty and len(zz500_data) >= 5:
                    recent_vol = zz500_data.tail(5)['vol'].mean()
                    avg_vol = zz500_data['vol'].mean()
                    
                    if avg_vol > 0:
                        vol_ratio = recent_vol / avg_vol
                        if vol_ratio > 1.5:
                            score += 0.25
                        elif vol_ratio > 1.2:
                            score += 0.2
                        elif vol_ratio > 1.0:
                            score += 0.15
                        else:
                            score += 0.1
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算风险偏好失败: {e}")
            return 0.5
    
    def calculate_liquidity_condition(self, indices_data: pd.DataFrame) -> float:
        """
        计算流动性状况评分
        
        Args:
            indices_data: 指数数据
            
        Returns:
            流动性状况评分 (0-1)
        """
        try:
            if indices_data.empty:
                return 0.5
            
            score = 0.0
            
            # 整体成交量水平 (60%)
            total_volumes = []
            for index_code in self.major_indices.keys():
                index_data = indices_data[indices_data['ts_code'] == index_code]
                if not index_data.empty:
                    recent_vol = index_data.tail(5)['vol'].mean()
                    total_volumes.append(recent_vol)
            
            if total_volumes:
                avg_volume = np.mean(total_volumes)
                # 这里需要与历史平均水平比较，简化处理
                if avg_volume > 1e9:  # 高成交量
                    score += 0.6
                elif avg_volume > 5e8:
                    score += 0.4
                elif avg_volume > 1e8:
                    score += 0.2
            
            # 成交金额变化 (40%)
            total_amounts = []
            for index_code in self.major_indices.keys():
                index_data = indices_data[indices_data['ts_code'] == index_code]
                if len(index_data) >= 10:
                    recent_amount = index_data.tail(5)['amount'].mean()
                    avg_amount = index_data.tail(20)['amount'].mean()
                    if avg_amount > 0:
                        amount_ratio = recent_amount / avg_amount
                        total_amounts.append(amount_ratio)
            
            if total_amounts:
                avg_amount_ratio = np.mean(total_amounts)
                if avg_amount_ratio > 1.3:
                    score += 0.4
                elif avg_amount_ratio > 1.1:
                    score += 0.3
                elif avg_amount_ratio > 0.9:
                    score += 0.2
                else:
                    score += 0.1
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算流动性状况失败: {e}")
            return 0.5
    
    def analyze_macro_environment(self, trade_date: str) -> Dict[str, Any]:
        """
        分析宏观环境
        
        Args:
            trade_date: 分析日期
            
        Returns:
            宏观环境分析结果
        """
        try:
            # 获取指数数据
            indices_data = self.get_market_indices_data(trade_date)
            
            # 获取行业表现
            sector_performance = self.get_sector_performance(trade_date)
            
            # 计算各项评分
            market_regime = self.calculate_market_regime(indices_data)
            sector_rotation = self.calculate_sector_rotation(sector_performance)
            risk_appetite = self.calculate_risk_appetite(indices_data, sector_performance)
            liquidity_condition = self.calculate_liquidity_condition(indices_data)
            
            # 构建宏观数据
            macro_data = {
                'major_indices_performance': {},
                'sector_performance': sector_performance,
                'market_summary': {
                    'regime': '牛市' if market_regime > 0.7 else '熊市' if market_regime < 0.3 else '震荡',
                    'risk_level': '高' if risk_appetite > 0.7 else '低' if risk_appetite < 0.3 else '中',
                    'liquidity': '充裕' if liquidity_condition > 0.7 else '紧张' if liquidity_condition < 0.3 else '适中'
                }
            }
            
            # 添加主要指数表现
            for index_code, index_name in self.major_indices.items():
                index_data = indices_data[indices_data['ts_code'] == index_code]
                if not index_data.empty:
                    latest = index_data.iloc[-1]
                    macro_data['major_indices_performance'][index_name] = {
                        'latest_change': float(latest['pct_chg']),
                        'latest_price': float(latest['close_price'])
                    }
            
            result = {
                'trade_date': trade_date,
                'market_regime': round(market_regime, 3),
                'sector_rotation': round(sector_rotation, 3),
                'risk_appetite': round(risk_appetite, 3),
                'liquidity_condition': round(liquidity_condition, 3),
                # 将字典转换为 JSON 字符串，避免数据库插入问题
                'macro_data': str(macro_data) if macro_data else None
            }
            
            logger.info(f"宏观环境分析完成: {trade_date}")
            return result
            
        except Exception as e:
            logger.error(f"宏观环境分析失败: {e}")
            return {}


if __name__ == "__main__":
    # 测试宏观环境分析器
    analyzer = MacroAnalyzer()
    
    # 测试分析宏观环境
    result = analyzer.analyze_macro_environment('2024-12-01')
    print("宏观环境分析结果:", result)
