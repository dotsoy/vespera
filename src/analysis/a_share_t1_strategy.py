"""
A股T+1交易策略分析模块
专门针对A股T+1交易制度设计的策略分析工具
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from loguru import logger


class AShareT1Strategy:
    """A股T+1交易策略分析器"""
    
    def __init__(self):
        self.name = "A股T+1策略分析器"
        
        # A股市场特征参数
        self.market_params = {
            'limit_up_pct': 10.0,      # 普通股票涨停幅度
            'limit_down_pct': -10.0,   # 普通股票跌停幅度
            'gem_limit_pct': 20.0,     # 创业板/科创板涨跌停幅度
            'st_limit_pct': 5.0,       # ST股票涨跌停幅度
            'trading_hours': {
                'morning': ('09:30', '11:30'),
                'afternoon': ('13:00', '15:00')
            },
            'settlement_days': 1       # T+1结算
        }
    
    def analyze_overnight_risk(self, data: pd.DataFrame) -> Dict:
        """分析隔夜风险"""
        logger.info("📊 分析A股隔夜风险")
        
        try:
            if data.empty or 'close' not in data.columns:
                return {}
            
            # 按股票分组分析
            results = {}
            
            for ts_code in data['ts_code'].unique():
                stock_data = data[data['ts_code'] == ts_code].sort_values('trade_date')
                
                if len(stock_data) < 2:
                    continue
                
                # 计算隔夜收益率（次日开盘价相对前日收盘价）
                stock_data = stock_data.copy()
                stock_data['next_open'] = stock_data['open'].shift(-1)
                stock_data['overnight_return'] = (
                    (stock_data['next_open'] - stock_data['close']) / stock_data['close'] * 100
                )
                
                overnight_returns = stock_data['overnight_return'].dropna()
                
                if len(overnight_returns) > 0:
                    results[ts_code] = {
                        'avg_overnight_return': overnight_returns.mean(),
                        'overnight_volatility': overnight_returns.std(),
                        'positive_overnight_ratio': (overnight_returns > 0).mean(),
                        'max_overnight_gain': overnight_returns.max(),
                        'max_overnight_loss': overnight_returns.min(),
                        'overnight_risk_score': abs(overnight_returns.mean()) + overnight_returns.std()
                    }
            
            # 整体市场隔夜风险
            all_overnight_returns = []
            for stock_results in results.values():
                if 'avg_overnight_return' in stock_results:
                    all_overnight_returns.append(stock_results['avg_overnight_return'])
            
            market_overnight_risk = {
                'market_avg_overnight_return': np.mean(all_overnight_returns) if all_overnight_returns else 0,
                'market_overnight_volatility': np.std(all_overnight_returns) if all_overnight_returns else 0,
                'high_risk_stocks': [
                    code for code, result in results.items() 
                    if result.get('overnight_risk_score', 0) > 2.0
                ]
            }
            
            logger.success(f"✅ 隔夜风险分析完成，覆盖 {len(results)} 只股票")
            
            return {
                'individual_stocks': results,
                'market_summary': market_overnight_risk
            }
            
        except Exception as e:
            logger.error(f"❌ 隔夜风险分析失败: {e}")
            return {}
    
    def identify_t1_trading_opportunities(self, data: pd.DataFrame) -> Dict:
        """识别T+1交易机会"""
        logger.info("🎯 识别T+1交易机会")
        
        try:
            opportunities = {}
            
            for ts_code in data['ts_code'].unique():
                stock_data = data[data['ts_code'] == ts_code].sort_values('trade_date')
                
                if len(stock_data) < 5:
                    continue
                
                stock_data = stock_data.copy()
                
                # 计算技术指标
                stock_data['ma5'] = stock_data['close'].rolling(5).mean()
                stock_data['ma10'] = stock_data['close'].rolling(10).mean()
                stock_data['rsi'] = self._calculate_rsi(stock_data['close'])
                stock_data['volume_ma5'] = stock_data['vol'].rolling(5).mean()
                
                # T+1交易机会识别
                opportunities[ts_code] = []
                
                for i in range(1, len(stock_data)):
                    current = stock_data.iloc[i]
                    prev = stock_data.iloc[i-1]
                    
                    # 机会1: 突破买入信号（适合T+1持有）
                    if (current['close'] > current['ma5'] and 
                        prev['close'] <= prev['ma5'] and
                        current['vol'] > current['volume_ma5'] * 1.2):
                        
                        opportunities[ts_code].append({
                            'date': current['trade_date'],
                            'type': '突破买入',
                            'signal_strength': 'medium',
                            'entry_price': current['close'],
                            'reason': 'MA5突破+放量',
                            't1_suitability': 'high'  # 适合T+1
                        })
                    
                    # 机会2: 超跌反弹（T+1快进快出）
                    if (current['pct_chg'] < -5 and 
                        current['rsi'] < 30 and
                        current['vol'] > current['volume_ma5'] * 1.5):
                        
                        opportunities[ts_code].append({
                            'date': current['trade_date'],
                            'type': '超跌反弹',
                            'signal_strength': 'high',
                            'entry_price': current['close'],
                            'reason': '超跌+RSI超卖+放量',
                            't1_suitability': 'medium'  # 需要快速反应
                        })
                    
                    # 机会3: 涨停板次日机会
                    if prev['pct_chg'] >= 9.8:  # 前日涨停
                        opportunities[ts_code].append({
                            'date': current['trade_date'],
                            'type': '涨停次日',
                            'signal_strength': 'low',
                            'entry_price': current['open'],
                            'reason': '涨停板次日观察',
                            't1_suitability': 'low'  # 风险较高
                        })
            
            # 统计机会分布
            total_opportunities = sum(len(opps) for opps in opportunities.values())
            opportunity_types = {}
            
            for stock_opps in opportunities.values():
                for opp in stock_opps:
                    opp_type = opp['type']
                    if opp_type not in opportunity_types:
                        opportunity_types[opp_type] = 0
                    opportunity_types[opp_type] += 1
            
            logger.success(f"✅ 识别到 {total_opportunities} 个T+1交易机会")
            
            return {
                'opportunities': opportunities,
                'summary': {
                    'total_opportunities': total_opportunities,
                    'opportunity_types': opportunity_types,
                    'stocks_with_opportunities': len([k for k, v in opportunities.items() if v])
                }
            }
            
        except Exception as e:
            logger.error(f"❌ T+1交易机会识别失败: {e}")
            return {}
    
    def calculate_t1_risk_metrics(self, data: pd.DataFrame) -> Dict:
        """计算T+1风险指标"""
        logger.info("⚠️ 计算T+1风险指标")
        
        try:
            risk_metrics = {}
            
            for ts_code in data['ts_code'].unique():
                stock_data = data[data['ts_code'] == ts_code].sort_values('trade_date')
                
                if len(stock_data) < 10:
                    continue
                
                # 计算各种风险指标
                returns = stock_data['pct_chg'].dropna()
                
                # 基础风险指标
                volatility = returns.std()
                max_drawdown = self._calculate_max_drawdown(stock_data['close'])
                var_95 = np.percentile(returns, 5)  # 95% VaR
                
                # T+1特有风险
                # 1. 隔夜跳空风险
                stock_data_copy = stock_data.copy()
                stock_data_copy['gap'] = (
                    (stock_data_copy['open'] - stock_data_copy['close'].shift(1)) / 
                    stock_data_copy['close'].shift(1) * 100
                )
                gap_risk = stock_data_copy['gap'].std()
                
                # 2. 流动性风险（基于成交量）
                volume_volatility = stock_data['vol'].std() / stock_data['vol'].mean()
                
                # 3. 涨跌停风险
                limit_up_days = (returns >= 9.8).sum()
                limit_down_days = (returns <= -9.8).sum()
                limit_risk = (limit_up_days + limit_down_days) / len(returns)
                
                # 综合T+1风险评分
                t1_risk_score = (
                    volatility * 0.3 +
                    abs(var_95) * 0.3 +
                    gap_risk * 0.2 +
                    volume_volatility * 0.1 +
                    limit_risk * 100 * 0.1
                )
                
                risk_metrics[ts_code] = {
                    'volatility': volatility,
                    'max_drawdown': max_drawdown,
                    'var_95': var_95,
                    'gap_risk': gap_risk,
                    'volume_volatility': volume_volatility,
                    'limit_risk': limit_risk,
                    't1_risk_score': t1_risk_score,
                    'risk_level': self._classify_risk_level(t1_risk_score)
                }
            
            # 市场整体风险
            all_scores = [metrics['t1_risk_score'] for metrics in risk_metrics.values()]
            market_risk = {
                'avg_market_risk': np.mean(all_scores) if all_scores else 0,
                'high_risk_stocks': [
                    code for code, metrics in risk_metrics.items()
                    if metrics['risk_level'] == 'high'
                ],
                'low_risk_stocks': [
                    code for code, metrics in risk_metrics.items()
                    if metrics['risk_level'] == 'low'
                ]
            }
            
            logger.success(f"✅ T+1风险指标计算完成，覆盖 {len(risk_metrics)} 只股票")
            
            return {
                'individual_stocks': risk_metrics,
                'market_summary': market_risk
            }
            
        except Exception as e:
            logger.error(f"❌ T+1风险指标计算失败: {e}")
            return {}
    
    def generate_t1_trading_plan(self, data: pd.DataFrame) -> Dict:
        """生成T+1交易计划"""
        logger.info("📋 生成T+1交易计划")
        
        try:
            # 获取分析结果
            overnight_risk = self.analyze_overnight_risk(data)
            opportunities = self.identify_t1_trading_opportunities(data)
            risk_metrics = self.calculate_t1_risk_metrics(data)
            
            # 生成交易计划
            trading_plan = {
                'plan_date': datetime.now().strftime('%Y-%m-%d'),
                'market_analysis': {
                    'overnight_risk_level': self._assess_market_risk_level(overnight_risk),
                    'opportunity_count': opportunities.get('summary', {}).get('total_opportunities', 0),
                    'high_risk_stocks': risk_metrics.get('market_summary', {}).get('high_risk_stocks', [])
                },
                'recommended_actions': [],
                'risk_warnings': [],
                'position_suggestions': {}
            }
            
            # 基于分析结果生成建议
            if opportunities.get('opportunities'):
                for ts_code, stock_opps in opportunities['opportunities'].items():
                    for opp in stock_opps:
                        if opp['t1_suitability'] == 'high':
                            trading_plan['recommended_actions'].append({
                                'stock': ts_code,
                                'action': 'buy',
                                'reason': opp['reason'],
                                'entry_price': opp['entry_price'],
                                'confidence': opp['signal_strength']
                            })
            
            # 风险警告
            if risk_metrics.get('market_summary', {}).get('high_risk_stocks'):
                trading_plan['risk_warnings'].append({
                    'type': 'high_risk_stocks',
                    'message': f"高风险股票: {len(risk_metrics['market_summary']['high_risk_stocks'])} 只",
                    'stocks': risk_metrics['market_summary']['high_risk_stocks'][:5]  # 只显示前5只
                })
            
            # T+1特殊建议
            trading_plan['t1_specific_advice'] = [
                "当日买入的股票次日才能卖出，请谨慎选择买入时机",
                "关注盘后消息，避免隔夜重大风险",
                "设置合理的止损位，利用涨跌停制度保护资金",
                "优先选择流动性好的股票，便于次日出货",
                "避免在周五买入短线股票，增加隔夜风险"
            ]
            
            logger.success("✅ T+1交易计划生成完成")
            
            return trading_plan
            
        except Exception as e:
            logger.error(f"❌ T+1交易计划生成失败: {e}")
            return {}
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """计算RSI指标"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _calculate_max_drawdown(self, prices: pd.Series) -> float:
        """计算最大回撤"""
        cumulative = (1 + prices.pct_change()).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        return drawdown.min()
    
    def _classify_risk_level(self, risk_score: float) -> str:
        """分类风险等级"""
        if risk_score < 3:
            return 'low'
        elif risk_score < 6:
            return 'medium'
        else:
            return 'high'
    
    def _assess_market_risk_level(self, overnight_risk: Dict) -> str:
        """评估市场风险等级"""
        if not overnight_risk or 'market_summary' not in overnight_risk:
            return 'unknown'
        
        market_vol = overnight_risk['market_summary'].get('market_overnight_volatility', 0)
        
        if market_vol < 1:
            return 'low'
        elif market_vol < 2:
            return 'medium'
        else:
            return 'high'


def analyze_a_share_data(data_file: str) -> Dict:
    """分析A股数据的便捷函数"""
    try:
        # 读取数据
        data = pd.read_csv(data_file)
        
        # 创建分析器
        analyzer = AShareT1Strategy()
        
        # 执行分析
        results = {
            'overnight_risk': analyzer.analyze_overnight_risk(data),
            'trading_opportunities': analyzer.identify_t1_trading_opportunities(data),
            'risk_metrics': analyzer.calculate_t1_risk_metrics(data),
            'trading_plan': analyzer.generate_t1_trading_plan(data)
        }
        
        return results
        
    except Exception as e:
        logger.error(f"❌ A股数据分析失败: {e}")
        return {}
