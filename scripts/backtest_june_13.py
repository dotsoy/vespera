#!/usr/bin/env python3
"""
6月13日真实数据回测分析
"""
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.database import get_db_manager
from src.data_sources.tushare_client import TushareClient
from src.analyzers.technical_analyzer import TechnicalAnalyzer
from src.analyzers.capital_flow_analyzer import CapitalFlowAnalyzer
from src.fusion.signal_fusion_engine import SignalFusionEngine

logger = get_logger("backtest_june_13")


def get_real_data_for_date(target_date: str = "20240613") -> pd.DataFrame:
    """获取指定日期的真实股票数据"""
    try:
        logger.info(f"开始获取 {target_date} 的真实股票数据")
        
        # 初始化Tushare客户端
        tushare_client = TushareClient()
        
        # 获取主要股票列表 (选择一些代表性股票)
        representative_stocks = [
            '000001.SZ',  # 平安银行
            '000002.SZ',  # 万科A
            '000858.SZ',  # 五粮液
            '002415.SZ',  # 海康威视
            '300059.SZ',  # 东方财富
            '600000.SH',  # 浦发银行
            '600036.SH',  # 招商银行
            '600519.SH',  # 贵州茅台
            '600887.SH',  # 伊利股份
            '000858.SZ',  # 五粮液
        ]
        
        # 获取这些股票在目标日期前60天的数据
        all_data = []
        
        for ts_code in representative_stocks:
            try:
                logger.info(f"获取股票 {ts_code} 的历史数据")
                
                # 计算开始日期 (目标日期前60天)
                target_dt = datetime.strptime(target_date, "%Y%m%d")
                start_dt = target_dt - timedelta(days=80)  # 多取一些天数以确保有60个交易日
                start_date = start_dt.strftime("%Y%m%d")
                
                # 获取日线数据
                stock_data = tushare_client.get_daily_quotes(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=target_date
                )
                
                if not stock_data.empty:
                    # 只保留最近60个交易日
                    stock_data = stock_data.sort_values('trade_date').tail(60)
                    all_data.append(stock_data)
                    logger.info(f"成功获取股票 {ts_code} 的 {len(stock_data)} 条记录")
                else:
                    logger.warning(f"股票 {ts_code} 无数据")
                    
            except Exception as e:
                logger.error(f"获取股票 {ts_code} 数据失败: {e}")
                continue
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            logger.info(f"总共获取 {len(combined_data)} 条股票数据")
            return combined_data
        else:
            logger.error("未能获取任何股票数据")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"获取真实数据失败: {e}")
        return pd.DataFrame()


def analyze_stock_performance(df: pd.DataFrame, target_date: str = "2024-06-13") -> dict:
    """分析股票在目标日期的表现"""
    try:
        logger.info(f"开始分析 {target_date} 的股票表现")
        
        # 初始化分析器
        technical_analyzer = TechnicalAnalyzer()
        
        results = {}
        
        # 按股票分组分析
        for ts_code in df['ts_code'].unique():
            try:
                stock_data = df[df['ts_code'] == ts_code].copy()
                stock_data = stock_data.sort_values('trade_date')
                
                if len(stock_data) < 30:
                    logger.warning(f"股票 {ts_code} 数据不足，跳过分析")
                    continue
                
                # 获取目标日期的数据
                target_data = stock_data[stock_data['trade_date'] == target_date]
                if target_data.empty:
                    logger.warning(f"股票 {ts_code} 在 {target_date} 无数据")
                    continue
                
                # 计算技术指标
                stock_data_with_indicators = technical_analyzer.calculate_indicators(stock_data)
                
                # 获取目标日期的技术指标
                target_indicators = stock_data_with_indicators[
                    stock_data_with_indicators['trade_date'] == target_date
                ].iloc[0]
                
                # 计算各项评分
                trend_score = technical_analyzer.calculate_trend_score(stock_data_with_indicators)
                momentum_score = technical_analyzer.calculate_momentum_score(stock_data_with_indicators)
                volume_score = technical_analyzer.calculate_volume_health_score(stock_data_with_indicators)
                
                # 识别技术形态
                patterns = technical_analyzer.identify_patterns(stock_data_with_indicators)
                
                # 计算支撑阻力位
                support, resistance = technical_analyzer.calculate_support_resistance(stock_data_with_indicators)
                
                # 计算后续几天的实际表现 (如果有数据)
                future_performance = calculate_future_performance(stock_data, target_date)
                
                results[ts_code] = {
                    'target_date': target_date,
                    'close_price': float(target_indicators['close_price']),
                    'volume': int(target_indicators['vol']),
                    'pct_chg': float(target_indicators['pct_chg']),
                    
                    # 技术指标
                    'ma_5': float(target_indicators.get('ma_5', 0)),
                    'ma_20': float(target_indicators.get('ma_20', 0)),
                    'rsi': float(target_indicators.get('rsi', 50)),
                    'macd': float(target_indicators.get('macd', 0)),
                    'macd_signal': float(target_indicators.get('macd_signal', 0)),
                    
                    # 评分
                    'trend_score': trend_score,
                    'momentum_score': momentum_score,
                    'volume_score': volume_score,
                    
                    # 技术形态
                    'patterns': patterns,
                    'support': support,
                    'resistance': resistance,
                    
                    # 后续表现
                    'future_performance': future_performance
                }
                
                logger.info(f"完成股票 {ts_code} 的分析")
                
            except Exception as e:
                logger.error(f"分析股票 {ts_code} 失败: {e}")
                continue
        
        return results
        
    except Exception as e:
        logger.error(f"股票表现分析失败: {e}")
        return {}


def calculate_future_performance(stock_data: pd.DataFrame, target_date: str, days: int = 5) -> dict:
    """计算目标日期后的实际表现"""
    try:
        stock_data = stock_data.sort_values('trade_date')
        target_idx = stock_data[stock_data['trade_date'] == target_date].index
        
        if len(target_idx) == 0:
            return {}
        
        target_idx = target_idx[0]
        target_price = stock_data.loc[target_idx, 'close_price']
        
        performance = {}
        
        # 计算后续几天的表现
        for day in range(1, days + 1):
            future_idx = target_idx + day
            if future_idx < len(stock_data):
                future_price = stock_data.iloc[future_idx]['close_price']
                future_date = stock_data.iloc[future_idx]['trade_date']
                return_pct = (future_price - target_price) / target_price * 100
                
                performance[f'day_{day}'] = {
                    'date': future_date,
                    'price': float(future_price),
                    'return_pct': float(return_pct)
                }
        
        return performance
        
    except Exception as e:
        logger.error(f"计算未来表现失败: {e}")
        return {}


def generate_trading_signals(analysis_results: dict) -> pd.DataFrame:
    """基于分析结果生成交易信号"""
    try:
        signals = []
        
        for ts_code, result in analysis_results.items():
            # 综合评分
            technical_score = (result['trend_score'] + result['momentum_score'] + result['volume_score']) / 3
            
            # 生成信号
            if technical_score >= 0.7:
                signal_type = 'BUY'
                confidence = technical_score
            elif technical_score <= 0.3:
                signal_type = 'SELL'
                confidence = 1 - technical_score
            else:
                signal_type = 'HOLD'
                confidence = 0.5
            
            # 计算目标价和止损价
            current_price = result['close_price']
            support = result.get('support', current_price * 0.95)
            resistance = result.get('resistance', current_price * 1.05)
            
            if signal_type == 'BUY':
                entry_price = current_price
                stop_loss = max(support, current_price * 0.95)
                target_price = min(resistance, current_price * 1.1)
            elif signal_type == 'SELL':
                entry_price = current_price
                stop_loss = min(resistance, current_price * 1.05)
                target_price = max(support, current_price * 0.9)
            else:
                entry_price = current_price
                stop_loss = current_price * 0.95
                target_price = current_price * 1.05
            
            signals.append({
                'ts_code': ts_code,
                'trade_date': result['target_date'],
                'signal_type': signal_type,
                'confidence_score': confidence,
                'technical_score': technical_score,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'target_price': target_price,
                'current_price': current_price,
                'rsi': result['rsi'],
                'trend_score': result['trend_score'],
                'momentum_score': result['momentum_score'],
                'volume_score': result['volume_score'],
                'patterns': str(result['patterns']),
                'future_performance': result.get('future_performance', {})
            })
        
        return pd.DataFrame(signals)
        
    except Exception as e:
        logger.error(f"生成交易信号失败: {e}")
        return pd.DataFrame()


def evaluate_signal_performance(signals_df: pd.DataFrame) -> dict:
    """评估信号的实际表现"""
    try:
        logger.info("开始评估信号表现")
        
        evaluation = {
            'total_signals': len(signals_df),
            'buy_signals': len(signals_df[signals_df['signal_type'] == 'BUY']),
            'sell_signals': len(signals_df[signals_df['signal_type'] == 'SELL']),
            'hold_signals': len(signals_df[signals_df['signal_type'] == 'HOLD']),
            'signal_accuracy': {},
            'average_returns': {},
            'best_performers': [],
            'worst_performers': []
        }
        
        # 分析每个信号的表现
        for _, signal in signals_df.iterrows():
            future_perf = signal.get('future_performance', {})
            if future_perf:
                # 计算5日后的表现
                if 'day_5' in future_perf:
                    actual_return = future_perf['day_5']['return_pct']
                    signal_type = signal['signal_type']
                    
                    if signal_type not in evaluation['average_returns']:
                        evaluation['average_returns'][signal_type] = []
                    
                    evaluation['average_returns'][signal_type].append(actual_return)
                    
                    # 记录最佳和最差表现
                    performance_record = {
                        'ts_code': signal['ts_code'],
                        'signal_type': signal_type,
                        'confidence': signal['confidence_score'],
                        'actual_return': actual_return,
                        'predicted_direction': 1 if signal_type == 'BUY' else (-1 if signal_type == 'SELL' else 0)
                    }
                    
                    if actual_return > 5:  # 5%以上涨幅
                        evaluation['best_performers'].append(performance_record)
                    elif actual_return < -5:  # 5%以上跌幅
                        evaluation['worst_performers'].append(performance_record)
        
        # 计算平均收益
        for signal_type, returns in evaluation['average_returns'].items():
            if returns:
                evaluation['average_returns'][signal_type] = {
                    'mean': np.mean(returns),
                    'std': np.std(returns),
                    'count': len(returns),
                    'positive_rate': len([r for r in returns if r > 0]) / len(returns)
                }
        
        # 排序最佳和最差表现
        evaluation['best_performers'] = sorted(
            evaluation['best_performers'], 
            key=lambda x: x['actual_return'], 
            reverse=True
        )[:5]
        
        evaluation['worst_performers'] = sorted(
            evaluation['worst_performers'], 
            key=lambda x: x['actual_return']
        )[:5]
        
        return evaluation
        
    except Exception as e:
        logger.error(f"评估信号表现失败: {e}")
        return {}


def main():
    """主函数"""
    logger.info("🚀 开始6月13日真实数据回测分析")
    logger.info("=" * 60)
    
    try:
        # 1. 获取真实数据
        logger.info("📊 步骤1: 获取6月13日真实股票数据")
        real_data = get_real_data_for_date("20240613")
        
        if real_data.empty:
            logger.error("❌ 无法获取真实数据，退出分析")
            return False
        
        logger.info(f"✅ 成功获取 {len(real_data)} 条真实数据")
        
        # 2. 分析股票表现
        logger.info("🔍 步骤2: 分析股票技术面表现")
        analysis_results = analyze_stock_performance(real_data, "2024-06-13")
        
        if not analysis_results:
            logger.error("❌ 股票分析失败，退出")
            return False
        
        logger.info(f"✅ 成功分析 {len(analysis_results)} 只股票")
        
        # 3. 生成交易信号
        logger.info("📈 步骤3: 生成交易信号")
        signals_df = generate_trading_signals(analysis_results)
        
        if signals_df.empty:
            logger.error("❌ 交易信号生成失败")
            return False
        
        logger.info(f"✅ 成功生成 {len(signals_df)} 个交易信号")
        
        # 4. 评估信号表现
        logger.info("📊 步骤4: 评估信号实际表现")
        evaluation = evaluate_signal_performance(signals_df)
        
        # 5. 输出结果
        logger.info("\n" + "=" * 60)
        logger.info("📋 6月13日回测分析结果")
        logger.info("=" * 60)
        
        # 信号统计
        logger.info(f"📊 信号统计:")
        logger.info(f"  总信号数: {evaluation.get('total_signals', 0)}")
        logger.info(f"  买入信号: {evaluation.get('buy_signals', 0)}")
        logger.info(f"  卖出信号: {evaluation.get('sell_signals', 0)}")
        logger.info(f"  持有信号: {evaluation.get('hold_signals', 0)}")
        
        # 平均收益
        avg_returns = evaluation.get('average_returns', {})
        for signal_type, stats in avg_returns.items():
            if isinstance(stats, dict):
                logger.info(f"  {signal_type} 信号平均收益: {stats['mean']:.2f}% (胜率: {stats['positive_rate']:.1%})")
        
        # 最佳表现
        best_performers = evaluation.get('best_performers', [])
        if best_performers:
            logger.info(f"\n🏆 最佳表现 (前5名):")
            for i, perf in enumerate(best_performers, 1):
                logger.info(f"  {i}. {perf['ts_code']}: {perf['signal_type']} → {perf['actual_return']:.2f}%")
        
        # 最差表现
        worst_performers = evaluation.get('worst_performers', [])
        if worst_performers:
            logger.info(f"\n📉 最差表现 (前5名):")
            for i, perf in enumerate(worst_performers, 1):
                logger.info(f"  {i}. {perf['ts_code']}: {perf['signal_type']} → {perf['actual_return']:.2f}%")
        
        # 保存结果
        logger.info(f"\n💾 保存分析结果...")
        signals_df.to_csv('backtest_june_13_signals.csv', index=False)
        logger.info(f"✅ 交易信号已保存到 backtest_june_13_signals.csv")
        
        logger.success("🎉 6月13日回测分析完成！")
        return True
        
    except Exception as e:
        logger.error(f"❌ 回测分析失败: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
