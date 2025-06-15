#!/usr/bin/env python3
"""
A股全市场高级技术分析
深度分析5000+股票的技术指标和市场结构
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.data.a_share_stock_universe import AShareStockUniverse
from src.analysis.full_market_analyzer import FullMarketAnalyzer

logger = get_logger("advanced_market_analysis")


class AdvancedMarketAnalyzer:
    """高级市场分析器"""
    
    def __init__(self):
        self.market_structure = {}
        self.sector_rotation = {}
        self.momentum_analysis = {}
        
    def analyze_market_structure(self, data: pd.DataFrame) -> dict:
        """分析市场结构"""
        logger.info("🏗️ 分析市场结构")
        
        try:
            latest_data = data.groupby('ts_code').last().reset_index()
            
            structure = {
                'breadth_indicators': {},
                'momentum_distribution': {},
                'volume_analysis': {},
                'volatility_analysis': {}
            }
            
            # 市场广度指标
            total_stocks = len(latest_data)
            up_stocks = (latest_data['pct_chg'] > 0).sum()
            down_stocks = (latest_data['pct_chg'] < 0).sum()
            
            structure['breadth_indicators'] = {
                'advance_decline_ratio': up_stocks / down_stocks if down_stocks > 0 else float('inf'),
                'advance_percentage': up_stocks / total_stocks * 100,
                'decline_percentage': down_stocks / total_stocks * 100,
                'new_highs': (latest_data['pct_chg'] > 5).sum(),
                'new_lows': (latest_data['pct_chg'] < -5).sum()
            }
            
            # 动量分布
            momentum_ranges = [
                ('强势', latest_data['pct_chg'] > 3),
                ('偏强', (latest_data['pct_chg'] > 1) & (latest_data['pct_chg'] <= 3)),
                ('震荡', (latest_data['pct_chg'] >= -1) & (latest_data['pct_chg'] <= 1)),
                ('偏弱', (latest_data['pct_chg'] >= -3) & (latest_data['pct_chg'] < -1)),
                ('弱势', latest_data['pct_chg'] < -3)
            ]
            
            for label, condition in momentum_ranges:
                count = condition.sum()
                structure['momentum_distribution'][label] = {
                    'count': count,
                    'percentage': count / total_stocks * 100
                }
            
            # 成交量分析
            avg_volume = latest_data['vol'].mean()
            high_volume_threshold = avg_volume * 2
            
            structure['volume_analysis'] = {
                'average_volume': avg_volume,
                'high_volume_stocks': (latest_data['vol'] > high_volume_threshold).sum(),
                'volume_concentration': latest_data['vol'].std() / avg_volume,
                'total_turnover': latest_data['amount'].sum() / 100000000  # 亿元
            }
            
            # 波动率分析
            structure['volatility_analysis'] = {
                'average_volatility': latest_data['pct_chg'].std(),
                'high_volatility_stocks': (abs(latest_data['pct_chg']) > 5).sum(),
                'volatility_skew': latest_data['pct_chg'].skew(),
                'volatility_kurtosis': latest_data['pct_chg'].kurtosis()
            }
            
            logger.success("✅ 市场结构分析完成")
            return structure
            
        except Exception as e:
            logger.error(f"❌ 市场结构分析失败: {e}")
            return {}
    
    def analyze_sector_rotation(self, data: pd.DataFrame, universe: AShareStockUniverse) -> dict:
        """分析板块轮动"""
        logger.info("🔄 分析板块轮动")
        
        try:
            # 合并股票信息
            stock_info = universe.stock_info[['ts_code', 'board', 'industry']]
            latest_data = data.groupby('ts_code').last().reset_index()
            sector_data = latest_data.merge(stock_info, on='ts_code', how='left')
            
            rotation_analysis = {
                'board_performance': {},
                'industry_performance': {},
                'rotation_signals': {},
                'capital_flow': {}
            }
            
            # 板块表现分析
            board_stats = sector_data.groupby('board').agg({
                'pct_chg': ['mean', 'median', 'std', 'count'],
                'vol': 'sum',
                'amount': 'sum',
                'turnover_rate': 'mean'
            }).round(2)
            
            board_stats.columns = ['avg_change', 'median_change', 'volatility', 'stock_count',
                                  'total_volume', 'total_amount', 'avg_turnover']
            
            # 计算板块强度评分
            for board in board_stats.index:
                stats = board_stats.loc[board]
                
                # 综合评分 = 涨跌幅权重50% + 成交活跃度权重30% + 参与度权重20%
                change_score = min(max(stats['avg_change'] / 5 * 50, -50), 50)  # 涨跌幅评分
                volume_score = min(stats['avg_turnover'] / 10 * 30, 30)  # 成交活跃度评分
                participation_score = min(stats['stock_count'] / 100 * 20, 20)  # 参与度评分
                
                total_score = change_score + volume_score + participation_score
                
                rotation_analysis['board_performance'][board] = {
                    'performance': stats.to_dict(),
                    'strength_score': round(total_score, 2),
                    'trend': '强势' if total_score > 60 else '偏强' if total_score > 30 else '震荡' if total_score > -30 else '偏弱' if total_score > -60 else '弱势'
                }
            
            # 行业表现分析（前20个行业）
            industry_stats = sector_data.groupby('industry').agg({
                'pct_chg': ['mean', 'count'],
                'amount': 'sum'
            }).round(2)
            
            industry_stats.columns = ['avg_change', 'stock_count', 'total_amount']
            industry_stats = industry_stats[industry_stats['stock_count'] >= 5]  # 至少5只股票
            industry_stats = industry_stats.sort_values('avg_change', ascending=False).head(20)
            
            rotation_analysis['industry_performance'] = industry_stats.to_dict('index')
            
            # 轮动信号识别
            board_changes = {board: data['avg_change'] for board, data in rotation_analysis['board_performance'].items()}
            sorted_boards = sorted(board_changes.items(), key=lambda x: x[1], reverse=True)
            
            rotation_analysis['rotation_signals'] = {
                'leading_sectors': [board for board, _ in sorted_boards[:2]],
                'lagging_sectors': [board for board, _ in sorted_boards[-2:]],
                'rotation_strength': abs(sorted_boards[0][1] - sorted_boards[-1][1])
            }
            
            # 资金流向分析
            total_amount = sector_data['amount'].sum()
            
            for board in sector_data['board'].unique():
                board_amount = sector_data[sector_data['board'] == board]['amount'].sum()
                rotation_analysis['capital_flow'][board] = {
                    'amount': board_amount / 100000000,  # 亿元
                    'percentage': board_amount / total_amount * 100
                }
            
            logger.success("✅ 板块轮动分析完成")
            return rotation_analysis
            
        except Exception as e:
            logger.error(f"❌ 板块轮动分析失败: {e}")
            return {}
    
    def analyze_momentum_patterns(self, data: pd.DataFrame) -> dict:
        """分析动量模式"""
        logger.info("📈 分析动量模式")
        
        try:
            momentum_patterns = {
                'trend_analysis': {},
                'momentum_stocks': {},
                'reversal_signals': {},
                'breakout_analysis': {}
            }
            
            # 趋势分析
            trend_stocks = {'上升趋势': 0, '下降趋势': 0, '横盘整理': 0}
            momentum_stocks_list = []
            reversal_candidates = []
            breakout_candidates = []
            
            for code in data['ts_code'].unique():
                stock_data = data[data['ts_code'] == code].sort_values('trade_date')
                
                if len(stock_data) < 10:
                    continue
                
                latest = stock_data.iloc[-1]
                
                # 趋势判断（基于MA5和MA20）
                if 'ma5' in latest and 'ma20' in latest:
                    if latest['ma5'] > latest['ma20'] and latest['close'] > latest['ma5']:
                        trend_stocks['上升趋势'] += 1
                        
                        # 动量股票识别
                        if latest['pct_chg'] > 2 and latest.get('vol_ratio', 1) > 1.5:
                            momentum_stocks_list.append({
                                'code': code,
                                'price': latest['close'],
                                'change': latest['pct_chg'],
                                'volume_ratio': latest.get('vol_ratio', 1),
                                'rsi': latest.get('rsi', 50)
                            })
                    
                    elif latest['ma5'] < latest['ma20'] and latest['close'] < latest['ma5']:
                        trend_stocks['下降趋势'] += 1
                        
                        # 反转信号识别
                        if latest['pct_chg'] > 1 and latest.get('rsi', 50) < 30:
                            reversal_candidates.append({
                                'code': code,
                                'price': latest['close'],
                                'change': latest['pct_chg'],
                                'rsi': latest.get('rsi', 50),
                                'reason': '超跌反弹'
                            })
                    
                    else:
                        trend_stocks['横盘整理'] += 1
                
                # 突破分析
                if len(stock_data) >= 20:
                    recent_high = stock_data['high'].tail(20).max()
                    recent_low = stock_data['low'].tail(20).min()
                    
                    if latest['close'] > recent_high * 0.98:  # 接近突破新高
                        breakout_candidates.append({
                            'code': code,
                            'price': latest['close'],
                            'resistance': recent_high,
                            'breakout_potential': (latest['close'] / recent_high - 1) * 100
                        })
            
            momentum_patterns['trend_analysis'] = trend_stocks
            momentum_patterns['momentum_stocks'] = sorted(momentum_stocks_list, 
                                                        key=lambda x: x['change'], reverse=True)[:20]
            momentum_patterns['reversal_signals'] = sorted(reversal_candidates, 
                                                         key=lambda x: x['change'], reverse=True)[:10]
            momentum_patterns['breakout_analysis'] = sorted(breakout_candidates, 
                                                          key=lambda x: x['breakout_potential'], reverse=True)[:10]
            
            logger.success("✅ 动量模式分析完成")
            return momentum_patterns
            
        except Exception as e:
            logger.error(f"❌ 动量模式分析失败: {e}")
            return {}
    
    def generate_trading_signals(self, data: pd.DataFrame) -> dict:
        """生成交易信号"""
        logger.info("📡 生成交易信号")
        
        try:
            signals = {
                'buy_signals': [],
                'sell_signals': [],
                'watch_list': [],
                'risk_alerts': []
            }
            
            latest_data = data.groupby('ts_code').last().reset_index()
            
            for _, stock in latest_data.iterrows():
                code = stock['ts_code']
                
                # 买入信号
                buy_conditions = [
                    stock['pct_chg'] > 2,  # 涨幅超过2%
                    stock.get('vol_ratio', 1) > 1.5,  # 成交量放大
                    stock.get('rsi', 50) < 70,  # RSI不超买
                    stock.get('macd', 0) > stock.get('macd_signal', 0)  # MACD金叉
                ]
                
                if sum(buy_conditions) >= 3:
                    signals['buy_signals'].append({
                        'code': code,
                        'price': stock['close'],
                        'change': stock['pct_chg'],
                        'signal_strength': sum(buy_conditions),
                        'reason': '技术突破+量价配合'
                    })
                
                # 卖出信号
                sell_conditions = [
                    stock['pct_chg'] < -3,  # 跌幅超过3%
                    stock.get('rsi', 50) > 80,  # RSI超买
                    stock.get('vol_ratio', 1) > 2,  # 成交量异常放大
                    stock.get('macd', 0) < stock.get('macd_signal', 0)  # MACD死叉
                ]
                
                if sum(sell_conditions) >= 3:
                    signals['sell_signals'].append({
                        'code': code,
                        'price': stock['close'],
                        'change': stock['pct_chg'],
                        'signal_strength': sum(sell_conditions),
                        'reason': '技术破位+量能异常'
                    })
                
                # 观察名单
                watch_conditions = [
                    0 < stock['pct_chg'] < 2,  # 小幅上涨
                    stock.get('vol_ratio', 1) > 1.2,  # 成交量温和放大
                    30 < stock.get('rsi', 50) < 60  # RSI在合理区间
                ]
                
                if sum(watch_conditions) >= 2:
                    signals['watch_list'].append({
                        'code': code,
                        'price': stock['close'],
                        'change': stock['pct_chg'],
                        'reason': '温和上涨+量能配合'
                    })
                
                # 风险警示
                risk_conditions = [
                    abs(stock['pct_chg']) > 8,  # 大幅波动
                    stock.get('vol_ratio', 1) > 5,  # 成交量暴增
                    stock.get('rsi', 50) > 85 or stock.get('rsi', 50) < 15  # RSI极值
                ]
                
                if any(risk_conditions):
                    signals['risk_alerts'].append({
                        'code': code,
                        'price': stock['close'],
                        'change': stock['pct_chg'],
                        'risk_type': '高波动风险' if abs(stock['pct_chg']) > 8 else '成交异常' if stock.get('vol_ratio', 1) > 5 else 'RSI极值'
                    })
            
            # 按信号强度排序
            signals['buy_signals'] = sorted(signals['buy_signals'], 
                                          key=lambda x: x['signal_strength'], reverse=True)[:20]
            signals['sell_signals'] = sorted(signals['sell_signals'], 
                                           key=lambda x: x['signal_strength'], reverse=True)[:20]
            signals['watch_list'] = signals['watch_list'][:30]
            signals['risk_alerts'] = signals['risk_alerts'][:20]
            
            logger.success("✅ 交易信号生成完成")
            return signals
            
        except Exception as e:
            logger.error(f"❌ 交易信号生成失败: {e}")
            return {}


def run_advanced_analysis():
    """运行高级分析"""
    logger.info("🚀 A股全市场高级技术分析")
    logger.info("=" * 80)
    
    try:
        # 加载之前的分析结果
        analysis_dir = project_root / 'analysis' / 'full_market'
        latest_dir = max(analysis_dir.glob('*'), key=os.path.getctime) if analysis_dir.exists() else None
        
        if not latest_dir or not (latest_dir / 'market_data.csv').exists():
            logger.error("❌ 未找到市场数据，请先运行 full_market_analysis.py")
            return False
        
        logger.info(f"📂 加载分析数据: {latest_dir}")
        
        # 加载数据
        market_data = pd.read_csv(latest_dir / 'market_data.csv')
        universe = AShareStockUniverse()
        universe.load_from_file(str(project_root / 'data' / 'universe' / 'a_share_universe.csv'))
        
        # 创建高级分析器
        analyzer = AdvancedMarketAnalyzer()
        
        # 1. 市场结构分析
        logger.info("\n🎯 步骤1: 市场结构分析")
        market_structure = analyzer.analyze_market_structure(market_data)
        
        # 2. 板块轮动分析
        logger.info("\n🎯 步骤2: 板块轮动分析")
        sector_rotation = analyzer.analyze_sector_rotation(market_data, universe)
        
        # 3. 动量模式分析
        logger.info("\n🎯 步骤3: 动量模式分析")
        momentum_patterns = analyzer.analyze_momentum_patterns(market_data)
        
        # 4. 交易信号生成
        logger.info("\n🎯 步骤4: 交易信号生成")
        trading_signals = analyzer.generate_trading_signals(market_data)
        
        # 5. 生成高级分析报告
        logger.info("\n🎯 步骤5: 生成高级分析报告")
        generate_advanced_report(market_structure, sector_rotation, momentum_patterns, trading_signals)
        
        # 6. 保存结果
        save_advanced_results(latest_dir, market_structure, sector_rotation, momentum_patterns, trading_signals)
        
        logger.success("🎉 A股全市场高级技术分析完成！")
        return True
        
    except Exception as e:
        logger.error(f"❌ 高级分析异常: {e}")
        return False


def generate_advanced_report(market_structure, sector_rotation, momentum_patterns, trading_signals):
    """生成高级分析报告"""
    print("\n" + "=" * 80)
    print("🔬 A股全市场高级技术分析报告")
    print("=" * 80)
    print(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 市场结构
    if market_structure:
        print("🏗️ 市场结构分析:")
        breadth = market_structure.get('breadth_indicators', {})
        print(f"  涨跌比: {breadth.get('advance_decline_ratio', 0):.2f}")
        print(f"  上涨占比: {breadth.get('advance_percentage', 0):.1f}%")
        print(f"  新高股票: {breadth.get('new_highs', 0)} 只")
        print(f"  新低股票: {breadth.get('new_lows', 0)} 只")
        
        momentum_dist = market_structure.get('momentum_distribution', {})
        print("  动量分布:")
        for level, data in momentum_dist.items():
            print(f"    {level}: {data.get('count', 0)} 只 ({data.get('percentage', 0):.1f}%)")
        print()
    
    # 板块轮动
    if sector_rotation:
        print("🔄 板块轮动分析:")
        board_perf = sector_rotation.get('board_performance', {})
        print("  板块强度排名:")
        sorted_boards = sorted(board_perf.items(), 
                             key=lambda x: x[1].get('strength_score', 0), reverse=True)
        for i, (board, data) in enumerate(sorted_boards, 1):
            score = data.get('strength_score', 0)
            trend = data.get('trend', '未知')
            print(f"    {i}. {board}: {score:.1f}分 ({trend})")
        
        rotation_signals = sector_rotation.get('rotation_signals', {})
        leading = rotation_signals.get('leading_sectors', [])
        lagging = rotation_signals.get('lagging_sectors', [])
        print(f"  领涨板块: {', '.join(leading)}")
        print(f"  落后板块: {', '.join(lagging)}")
        print()
    
    # 动量模式
    if momentum_patterns:
        print("📈 动量模式分析:")
        trend_analysis = momentum_patterns.get('trend_analysis', {})
        for trend, count in trend_analysis.items():
            print(f"  {trend}: {count} 只")
        
        momentum_stocks = momentum_patterns.get('momentum_stocks', [])
        if momentum_stocks:
            print("  强势股票 (前5只):")
            for i, stock in enumerate(momentum_stocks[:5], 1):
                print(f"    {i}. {stock['code']}: +{stock['change']:.2f}%, 量比{stock['volume_ratio']:.1f}")
        print()
    
    # 交易信号
    if trading_signals:
        print("📡 交易信号:")
        buy_signals = trading_signals.get('buy_signals', [])
        print(f"  买入信号: {len(buy_signals)} 只")
        if buy_signals:
            print("  重点关注 (前3只):")
            for i, signal in enumerate(buy_signals[:3], 1):
                print(f"    {i}. {signal['code']}: ¥{signal['price']:.2f}, +{signal['change']:.2f}%")
        
        sell_signals = trading_signals.get('sell_signals', [])
        print(f"  卖出信号: {len(sell_signals)} 只")
        
        watch_list = trading_signals.get('watch_list', [])
        print(f"  观察名单: {len(watch_list)} 只")
        
        risk_alerts = trading_signals.get('risk_alerts', [])
        print(f"  风险警示: {len(risk_alerts)} 只")
        print()
    
    print("💡 投资策略建议:")
    print("  • 关注领涨板块的强势股票")
    print("  • 重点关注技术突破+量价配合的个股")
    print("  • 注意风险警示股票，及时止损")
    print("  • T+1制度下，当日买入次日才能卖出")
    print("=" * 80)


def save_advanced_results(output_dir, market_structure, sector_rotation, momentum_patterns, trading_signals):
    """保存高级分析结果"""
    try:
        import json
        
        # 保存到JSON文件
        advanced_results = {
            'market_structure': market_structure,
            'sector_rotation': sector_rotation,
            'momentum_patterns': momentum_patterns,
            'trading_signals': trading_signals,
            'analysis_time': datetime.now().isoformat()
        }
        
        with open(output_dir / 'advanced_analysis.json', 'w', encoding='utf-8') as f:
            json.dump(advanced_results, f, ensure_ascii=False, indent=2, default=str)
        
        logger.success(f"✅ 高级分析结果已保存到: {output_dir / 'advanced_analysis.json'}")
        
    except Exception as e:
        logger.error(f"❌ 保存高级分析结果失败: {e}")


if __name__ == "__main__":
    success = run_advanced_analysis()
    sys.exit(0 if success else 1)
