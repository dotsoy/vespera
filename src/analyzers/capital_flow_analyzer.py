"""
资金流分析器 - 追踪"聪明钱"的动向
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from loguru import logger

from config.settings import analysis_settings
from src.utils.database import get_db_manager


class CapitalFlowAnalyzer:
    """资金流分析器"""
    
    def __init__(self):
        self.db_manager = get_db_manager()
        self.volume_ma_period = analysis_settings.volume_ma_period
        self.money_flow_threshold = analysis_settings.money_flow_threshold
        
        logger.info("资金流分析器初始化完成")
    
    def calculate_main_force_score(self, df: pd.DataFrame) -> float:
        """
        计算主力资金评分
        
        Args:
            df: 包含资金流数据的 DataFrame
            
        Returns:
            主力资金评分 (0-1)
        """
        try:
            if df.empty or len(df) < 5:
                return 0.0
            
            latest = df.iloc[-1]
            score = 0.0
            
            # 大单净流入评分 (40%)
            large_net_inflow = latest['buy_lg_amount'] - latest['sell_lg_amount']
            total_large_amount = latest['buy_lg_amount'] + latest['sell_lg_amount']
            
            if total_large_amount > 0:
                large_flow_ratio = large_net_inflow / total_large_amount
                if large_flow_ratio > 0.3:
                    score += 0.4
                elif large_flow_ratio > 0.1:
                    score += 0.2
                elif large_flow_ratio > 0:
                    score += 0.1
            
            # 超大单净流入评分 (35%)
            extra_large_net_inflow = latest['buy_elg_amount'] - latest['sell_elg_amount']
            total_extra_large_amount = latest['buy_elg_amount'] + latest['sell_elg_amount']
            
            if total_extra_large_amount > 0:
                extra_large_flow_ratio = extra_large_net_inflow / total_extra_large_amount
                if extra_large_flow_ratio > 0.5:
                    score += 0.35
                elif extra_large_flow_ratio > 0.2:
                    score += 0.2
                elif extra_large_flow_ratio > 0:
                    score += 0.1
            
            # 主力资金持续性评分 (25%)
            if len(df) >= 5:
                recent_5_days = df.tail(5)
                large_inflow_days = 0
                
                for _, row in recent_5_days.iterrows():
                    large_net = row['buy_lg_amount'] - row['sell_lg_amount']
                    extra_large_net = row['buy_elg_amount'] - row['sell_elg_amount']
                    
                    if large_net > 0 or extra_large_net > 0:
                        large_inflow_days += 1
                
                consistency_ratio = large_inflow_days / 5
                score += consistency_ratio * 0.25
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算主力资金评分失败: {e}")
            return 0.0
    
    def calculate_retail_sentiment_score(self, df: pd.DataFrame) -> float:
        """
        计算散户情绪评分
        
        Args:
            df: 包含资金流数据的 DataFrame
            
        Returns:
            散户情绪评分 (0-1)
        """
        try:
            if df.empty or len(df) < 3:
                return 0.0
            
            latest = df.iloc[-1]
            score = 0.0
            
            # 小单流向评分 (50%)
            small_net_inflow = latest['buy_sm_amount'] - latest['sell_sm_amount']
            total_small_amount = latest['buy_sm_amount'] + latest['sell_sm_amount']
            
            if total_small_amount > 0:
                small_flow_ratio = small_net_inflow / total_small_amount
                # 散户追涨杀跌，适度流入为好
                if -0.1 <= small_flow_ratio <= 0.2:
                    score += 0.5
                elif -0.2 <= small_flow_ratio <= 0.3:
                    score += 0.3
                elif small_flow_ratio > 0.5:  # 过度追涨
                    score += 0.1
            
            # 中单流向评分 (30%)
            medium_net_inflow = latest['buy_md_amount'] - latest['sell_md_amount']
            total_medium_amount = latest['buy_md_amount'] + latest['sell_md_amount']
            
            if total_medium_amount > 0:
                medium_flow_ratio = medium_net_inflow / total_medium_amount
                if 0 <= medium_flow_ratio <= 0.3:
                    score += 0.3
                elif medium_flow_ratio > 0.3:
                    score += 0.15
            
            # 散户行为稳定性评分 (20%)
            if len(df) >= 3:
                recent_3_days = df.tail(3)
                small_ratios = []
                
                for _, row in recent_3_days.iterrows():
                    small_net = row['buy_sm_amount'] - row['sell_sm_amount']
                    small_total = row['buy_sm_amount'] + row['sell_sm_amount']
                    if small_total > 0:
                        small_ratios.append(small_net / small_total)
                
                if small_ratios:
                    stability = 1 - np.std(small_ratios)  # 标准差越小越稳定
                    score += max(0, stability) * 0.2
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算散户情绪评分失败: {e}")
            return 0.0
    
    def calculate_institutional_activity(self, df: pd.DataFrame) -> float:
        """
        计算机构活跃度评分
        
        Args:
            df: 包含资金流数据的 DataFrame
            
        Returns:
            机构活跃度评分 (0-1)
        """
        try:
            if df.empty or len(df) < 10:
                return 0.0
            
            score = 0.0
            
            # 大单交易活跃度 (40%)
            recent_10_days = df.tail(10)
            avg_large_volume = recent_10_days['buy_lg_vol'].mean() + recent_10_days['sell_lg_vol'].mean()
            latest_large_volume = df.iloc[-1]['buy_lg_vol'] + df.iloc[-1]['sell_lg_vol']
            
            if avg_large_volume > 0:
                activity_ratio = latest_large_volume / avg_large_volume
                if activity_ratio > 1.5:
                    score += 0.4
                elif activity_ratio > 1.2:
                    score += 0.3
                elif activity_ratio > 1.0:
                    score += 0.2
            
            # 超大单交易活跃度 (35%)
            avg_extra_large_volume = recent_10_days['buy_elg_vol'].mean() + recent_10_days['sell_elg_vol'].mean()
            latest_extra_large_volume = df.iloc[-1]['buy_elg_vol'] + df.iloc[-1]['sell_elg_vol']
            
            if avg_extra_large_volume > 0:
                extra_activity_ratio = latest_extra_large_volume / avg_extra_large_volume
                if extra_activity_ratio > 2.0:
                    score += 0.35
                elif extra_activity_ratio > 1.5:
                    score += 0.25
                elif extra_activity_ratio > 1.0:
                    score += 0.15
            
            # 机构资金集中度 (25%)
            latest = df.iloc[-1]
            total_amount = (latest['buy_sm_amount'] + latest['sell_sm_amount'] + 
                          latest['buy_md_amount'] + latest['sell_md_amount'] +
                          latest['buy_lg_amount'] + latest['sell_lg_amount'] +
                          latest['buy_elg_amount'] + latest['sell_elg_amount'])
            
            if total_amount > 0:
                institutional_ratio = ((latest['buy_lg_amount'] + latest['sell_lg_amount'] +
                                      latest['buy_elg_amount'] + latest['sell_elg_amount']) / total_amount)
                score += institutional_ratio * 0.25
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算机构活跃度失败: {e}")
            return 0.0
    
    def calculate_flow_consistency(self, df: pd.DataFrame) -> float:
        """
        计算资金流一致性评分
        
        Args:
            df: 包含资金流数据的 DataFrame
            
        Returns:
            资金流一致性评分 (0-1)
        """
        try:
            if df.empty or len(df) < 5:
                return 0.0
            
            recent_5_days = df.tail(5)
            score = 0.0
            
            # 主力资金方向一致性 (60%)
            main_force_directions = []
            for _, row in recent_5_days.iterrows():
                large_net = row['buy_lg_amount'] - row['sell_lg_amount']
                extra_large_net = row['buy_elg_amount'] - row['sell_elg_amount']
                main_net = large_net + extra_large_net
                
                if main_net > 0:
                    main_force_directions.append(1)
                elif main_net < 0:
                    main_force_directions.append(-1)
                else:
                    main_force_directions.append(0)
            
            if main_force_directions:
                # 计算方向一致性
                positive_days = sum(1 for d in main_force_directions if d > 0)
                negative_days = sum(1 for d in main_force_directions if d < 0)
                
                consistency = max(positive_days, negative_days) / len(main_force_directions)
                score += consistency * 0.6
            
            # 净流入趋势一致性 (40%)
            net_flows = []
            for _, row in recent_5_days.iterrows():
                net_flow = row['net_mf_amount']
                net_flows.append(net_flow)
            
            if len(net_flows) >= 3:
                # 计算趋势一致性
                trend_changes = 0
                for i in range(1, len(net_flows)):
                    if (net_flows[i] > 0) != (net_flows[i-1] > 0):
                        trend_changes += 1
                
                trend_consistency = 1 - (trend_changes / (len(net_flows) - 1))
                score += trend_consistency * 0.4
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算资金流一致性失败: {e}")
            return 0.0
    
    def calculate_volume_price_correlation(self, money_flow_df: pd.DataFrame, 
                                         price_df: pd.DataFrame) -> float:
        """
        计算量价相关性评分
        
        Args:
            money_flow_df: 资金流数据
            price_df: 价格数据
            
        Returns:
            量价相关性评分 (0-1)
        """
        try:
            if money_flow_df.empty or price_df.empty:
                return 0.0
            
            # 合并数据
            merged_df = pd.merge(money_flow_df, price_df, on='trade_date', how='inner')
            
            if len(merged_df) < 5:
                return 0.0
            
            score = 0.0
            
            # 净流入与价格变化相关性 (70%)
            price_changes = merged_df['pct_chg'].values
            net_flows = merged_df['net_mf_amount'].values
            
            if len(price_changes) >= 5:
                correlation = np.corrcoef(price_changes, net_flows)[0, 1]
                if not np.isnan(correlation):
                    # 正相关性越强越好
                    if correlation > 0.5:
                        score += 0.7
                    elif correlation > 0.3:
                        score += 0.5
                    elif correlation > 0.1:
                        score += 0.3
                    elif correlation > 0:
                        score += 0.1
            
            # 主力资金与价格趋势一致性 (30%)
            recent_data = merged_df.tail(3)
            consistent_days = 0
            
            for _, row in recent_data.iterrows():
                price_up = row['pct_chg'] > 0
                main_inflow = (row['buy_lg_amount'] - row['sell_lg_amount'] + 
                             row['buy_elg_amount'] - row['sell_elg_amount']) > 0
                
                if price_up == main_inflow:
                    consistent_days += 1
            
            consistency_ratio = consistent_days / len(recent_data)
            score += consistency_ratio * 0.3
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算量价相关性失败: {e}")
            return 0.0
    
    def analyze_stock(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        """
        分析单只股票的资金流
        
        Args:
            ts_code: 股票代码
            trade_date: 分析日期
            
        Returns:
            资金流分析结果
        """
        try:
            # 获取资金流数据 (最近20天)
            money_flow_query = """
            SELECT * FROM money_flow_daily 
            WHERE ts_code = :ts_code 
            AND trade_date <= :trade_date 
            ORDER BY trade_date DESC 
            LIMIT 20
            """
            
            money_flow_df = self.db_manager.execute_postgres_query(
                money_flow_query, {'ts_code': ts_code, 'trade_date': trade_date}
            )
            
            if money_flow_df.empty:
                logger.warning(f"股票 {ts_code} 无资金流数据")
                return {}
            
            # 按日期正序排列
            money_flow_df = money_flow_df.sort_values('trade_date')
            
            # 获取价格数据用于量价分析
            price_query = """
            SELECT trade_date, pct_chg FROM stock_daily_quotes 
            WHERE ts_code = :ts_code 
            AND trade_date <= :trade_date 
            ORDER BY trade_date DESC 
            LIMIT 20
            """
            
            price_df = self.db_manager.execute_postgres_query(
                price_query, {'ts_code': ts_code, 'trade_date': trade_date}
            )
            
            # 计算各项评分
            main_force_score = self.calculate_main_force_score(money_flow_df)
            retail_sentiment_score = self.calculate_retail_sentiment_score(money_flow_df)
            institutional_activity = self.calculate_institutional_activity(money_flow_df)
            flow_consistency = self.calculate_flow_consistency(money_flow_df)
            volume_price_correlation = self.calculate_volume_price_correlation(
                money_flow_df, price_df
            )
            
            # 构建分析数据
            latest = money_flow_df.iloc[-1]
            flow_analysis = {
                'net_main_inflow': float(latest['buy_lg_amount'] + latest['buy_elg_amount'] - 
                                       latest['sell_lg_amount'] - latest['sell_elg_amount']),
                'net_retail_inflow': float(latest['buy_sm_amount'] + latest['buy_md_amount'] - 
                                         latest['sell_sm_amount'] - latest['sell_md_amount']),
                'total_turnover': float(latest['buy_sm_amount'] + latest['buy_md_amount'] + 
                                      latest['buy_lg_amount'] + latest['buy_elg_amount']),
                'main_force_ratio': float((latest['buy_lg_amount'] + latest['buy_elg_amount']) / 
                                        (latest['buy_sm_amount'] + latest['buy_md_amount'] + 
                                         latest['buy_lg_amount'] + latest['buy_elg_amount'])) if (latest['buy_sm_amount'] + latest['buy_md_amount'] + 
                                         latest['buy_lg_amount'] + latest['buy_elg_amount']) > 0 else 0.0
            }
            
            result = {
                'ts_code': ts_code,
                'trade_date': trade_date,
                'main_force_score': round(main_force_score, 3),
                'retail_sentiment_score': round(retail_sentiment_score, 3),
                'institutional_activity': round(institutional_activity, 3),
                'flow_consistency': round(flow_consistency, 3),
                'volume_price_correlation': round(volume_price_correlation, 3),
                # 将字典转换为 JSON 字符串，避免数据库插入问题
                'flow_analysis': str(flow_analysis) if flow_analysis else None
            }
            
            logger.info(f"股票 {ts_code} 资金流分析完成")
            return result
            
        except Exception as e:
            logger.error(f"分析股票 {ts_code} 资金流失败: {e}")
            return {}


if __name__ == "__main__":
    # 测试资金流分析器
    analyzer = CapitalFlowAnalyzer()
    
    # 测试分析单只股票
    result = analyzer.analyze_stock('000001.SZ', '2024-12-01')
    print("资金流分析结果:", result)
