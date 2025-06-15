#!/usr/bin/env python3
"""
使用真实股票数据进行分析
"""
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.analyzers.technical_analyzer import TechnicalAnalyzer
from src.fusion.signal_fusion_engine import SignalFusionEngine

logger = get_logger("analyze_real_data")


def load_real_data():
    """加载真实股票数据"""
    try:
        logger.info("📊 加载真实股票数据")
        
        data_dir = Path("data/stock_data")
        
        # 加载股票基础信息
        basic_file = data_dir / "stock_basic.csv"
        basic_data = pd.read_csv(basic_file)
        logger.info(f"✅ 加载了 {len(basic_data)} 只股票的基础信息")
        
        # 加载日线数据
        daily_file = data_dir / "stock_daily_quotes.csv"
        daily_data = pd.read_csv(daily_file)
        logger.info(f"✅ 加载了 {len(daily_data)} 条日线数据")
        
        # 数据预处理
        daily_data['trade_date'] = pd.to_datetime(daily_data['trade_date']).dt.strftime('%Y-%m-%d')
        
        # 重命名列以匹配系统预期
        if 'vol' not in daily_data.columns and 'volume' in daily_data.columns:
            daily_data['vol'] = daily_data['volume']
        
        # 确保必要的列存在
        required_columns = ['ts_code', 'trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'vol']
        missing_columns = [col for col in required_columns if col not in daily_data.columns]
        
        if missing_columns:
            logger.error(f"❌ 缺少必要的列: {missing_columns}")
            return None, None
        
        logger.info("✅ 数据加载和预处理完成")
        return daily_data, basic_data
        
    except Exception as e:
        logger.error(f"❌ 加载真实数据失败: {e}")
        return None, None


def analyze_stock_performance(daily_data: pd.DataFrame, basic_data: pd.DataFrame):
    """分析股票表现"""
    try:
        logger.info("🔍 开始分析股票表现")
        
        # 初始化技术分析器
        technical_analyzer = TechnicalAnalyzer()
        
        analysis_results = []
        
        # 按股票分组分析
        for ts_code in daily_data['ts_code'].unique():
            try:
                stock_data = daily_data[daily_data['ts_code'] == ts_code].copy()
                stock_data = stock_data.sort_values('trade_date')
                
                if len(stock_data) < 30:
                    logger.warning(f"股票 {ts_code} 数据不足，跳过分析")
                    continue
                
                # 获取股票基础信息
                stock_info = basic_data[basic_data['ts_code'] == ts_code].iloc[0]
                
                # 计算技术指标 (使用 Polars 加速)
                stock_data_with_indicators = technical_analyzer.calculate_indicators(stock_data)
                
                # 获取最新数据
                latest_data = stock_data_with_indicators.iloc[-1]
                
                # 计算各项评分
                trend_score = technical_analyzer.calculate_trend_score(stock_data_with_indicators)
                momentum_score = technical_analyzer.calculate_momentum_score(stock_data_with_indicators)
                volume_score = technical_analyzer.calculate_volume_health_score(stock_data_with_indicators)
                
                # 识别技术形态
                patterns = technical_analyzer.identify_patterns(stock_data_with_indicators)
                
                # 计算支撑阻力位
                support, resistance = technical_analyzer.calculate_support_resistance(stock_data_with_indicators)
                
                # 综合评分
                overall_score = (trend_score + momentum_score + volume_score) / 3
                
                # 生成投资建议
                if overall_score >= 0.7:
                    recommendation = "强烈买入"
                    confidence = overall_score
                elif overall_score >= 0.5:
                    recommendation = "买入"
                    confidence = overall_score
                elif overall_score >= 0.3:
                    recommendation = "持有"
                    confidence = 0.5
                else:
                    recommendation = "卖出"
                    confidence = 1 - overall_score
                
                analysis_results.append({
                    'ts_code': ts_code,
                    'name': stock_info['name'],
                    'industry': stock_info['industry'],
                    'latest_date': latest_data['trade_date'],
                    'close_price': latest_data['close_price'],
                    'pct_chg': latest_data.get('pct_chg', 0),
                    'volume': latest_data['vol'],
                    
                    # 技术指标
                    'ma_5': latest_data.get('ma_5', 0),
                    'ma_20': latest_data.get('ma_20', 0),
                    'rsi': latest_data.get('rsi', 50),
                    'macd': latest_data.get('macd', 0),
                    'macd_signal': latest_data.get('macd_signal', 0),
                    'bb_upper': latest_data.get('bb_upper', 0),
                    'bb_lower': latest_data.get('bb_lower', 0),
                    
                    # 评分
                    'trend_score': trend_score,
                    'momentum_score': momentum_score,
                    'volume_score': volume_score,
                    'overall_score': overall_score,
                    
                    # 投资建议
                    'recommendation': recommendation,
                    'confidence': confidence,
                    
                    # 技术形态
                    'patterns': patterns,
                    'support': support,
                    'resistance': resistance,
                })
                
                logger.info(f"✅ 完成 {ts_code} ({stock_info['name']}) 分析")
                
            except Exception as e:
                logger.error(f"❌ 分析股票 {ts_code} 失败: {e}")
                continue
        
        logger.success(f"🎉 股票分析完成，共分析 {len(analysis_results)} 只股票")
        return analysis_results
        
    except Exception as e:
        logger.error(f"❌ 股票表现分析失败: {e}")
        return []


def generate_analysis_report(analysis_results: list):
    """生成分析报告"""
    try:
        logger.info("📋 生成分析报告")
        
        if not analysis_results:
            logger.error("❌ 没有分析结果，无法生成报告")
            return
        
        # 转换为 DataFrame
        results_df = pd.DataFrame(analysis_results)
        
        # 按综合评分排序
        results_df = results_df.sort_values('overall_score', ascending=False)
        
        # 保存详细结果
        results_file = "real_stock_analysis_results.csv"
        results_df.to_csv(results_file, index=False, encoding='utf-8')
        logger.info(f"✅ 详细分析结果已保存到: {results_file}")
        
        # 生成报告
        logger.info("\n" + "=" * 80)
        logger.info("📊 真实股票数据分析报告")
        logger.info("=" * 80)
        
        # 总体统计
        total_stocks = len(results_df)
        avg_score = results_df['overall_score'].mean()
        
        logger.info(f"📈 总体统计:")
        logger.info(f"  分析股票数量: {total_stocks}")
        logger.info(f"  平均综合评分: {avg_score:.3f}")
        logger.info(f"  数据日期范围: {results_df['latest_date'].min()} - {results_df['latest_date'].max()}")
        
        # 投资建议分布
        recommendation_counts = results_df['recommendation'].value_counts()
        logger.info(f"\n💡 投资建议分布:")
        for rec, count in recommendation_counts.items():
            percentage = count / total_stocks * 100
            logger.info(f"  {rec}: {count} 只 ({percentage:.1f}%)")
        
        # 行业分析
        industry_stats = results_df.groupby('industry').agg({
            'overall_score': 'mean',
            'ts_code': 'count'
        }).round(3)
        industry_stats.columns = ['平均评分', '股票数量']
        industry_stats = industry_stats.sort_values('平均评分', ascending=False)
        
        logger.info(f"\n🏭 行业表现分析:")
        for industry, row in industry_stats.iterrows():
            logger.info(f"  {industry}: 平均评分 {row['平均评分']:.3f} ({row['股票数量']} 只股票)")
        
        # 最佳表现股票 (前5名)
        top_performers = results_df.head(5)
        logger.info(f"\n🏆 最佳表现股票 (前5名):")
        for i, (_, stock) in enumerate(top_performers.iterrows(), 1):
            logger.info(f"  {i}. {stock['name']} ({stock['ts_code']})")
            logger.info(f"     行业: {stock['industry']} | 评分: {stock['overall_score']:.3f} | 建议: {stock['recommendation']}")
            logger.info(f"     最新价格: ¥{stock['close_price']:.2f} | 涨跌幅: {stock['pct_chg']:.2f}%")
            logger.info(f"     RSI: {stock['rsi']:.1f} | 支撑位: ¥{stock['support']:.2f} | 阻力位: ¥{stock['resistance']:.2f}")
        
        # 最差表现股票 (后5名)
        worst_performers = results_df.tail(5)
        logger.info(f"\n📉 需要关注股票 (后5名):")
        for i, (_, stock) in enumerate(worst_performers.iterrows(), 1):
            logger.info(f"  {i}. {stock['name']} ({stock['ts_code']})")
            logger.info(f"     行业: {stock['industry']} | 评分: {stock['overall_score']:.3f} | 建议: {stock['recommendation']}")
            logger.info(f"     最新价格: ¥{stock['close_price']:.2f} | 涨跌幅: {stock['pct_chg']:.2f}%")
            logger.info(f"     RSI: {stock['rsi']:.1f} | 支撑位: ¥{stock['support']:.2f} | 阻力位: ¥{stock['resistance']:.2f}")
        
        # 技术指标统计
        logger.info(f"\n📊 技术指标统计:")
        logger.info(f"  平均RSI: {results_df['rsi'].mean():.1f}")
        logger.info(f"  RSI超买(>70): {len(results_df[results_df['rsi'] > 70])} 只")
        logger.info(f"  RSI超卖(<30): {len(results_df[results_df['rsi'] < 30])} 只")
        logger.info(f"  MACD金叉: {len(results_df[results_df['macd'] > results_df['macd_signal']])} 只")
        logger.info(f"  MACD死叉: {len(results_df[results_df['macd'] < results_df['macd_signal']])} 只")
        
        # 价格位置分析
        results_df['price_position'] = (results_df['close_price'] - results_df['bb_lower']) / (results_df['bb_upper'] - results_df['bb_lower'])
        logger.info(f"\n📍 价格位置分析 (布林带):")
        logger.info(f"  上轨附近(>0.8): {len(results_df[results_df['price_position'] > 0.8])} 只")
        logger.info(f"  中轨附近(0.4-0.6): {len(results_df[(results_df['price_position'] >= 0.4) & (results_df['price_position'] <= 0.6)])} 只")
        logger.info(f"  下轨附近(<0.2): {len(results_df[results_df['price_position'] < 0.2])} 只")
        
        logger.info("\n" + "=" * 80)
        logger.success("📋 分析报告生成完成！")
        
        return results_df
        
    except Exception as e:
        logger.error(f"❌ 生成分析报告失败: {e}")
        return None


def main():
    """主函数"""
    logger.info("🚀 启明星真实股票数据分析")
    logger.info("=" * 60)
    
    try:
        # 1. 加载真实数据
        logger.info("📋 步骤1: 加载真实股票数据")
        daily_data, basic_data = load_real_data()
        
        if daily_data is None or basic_data is None:
            logger.error("❌ 数据加载失败，退出分析")
            return False
        
        # 2. 分析股票表现
        logger.info("📋 步骤2: 分析股票表现")
        analysis_results = analyze_stock_performance(daily_data, basic_data)
        
        if not analysis_results:
            logger.error("❌ 股票分析失败，退出")
            return False
        
        # 3. 生成分析报告
        logger.info("📋 步骤3: 生成分析报告")
        results_df = generate_analysis_report(analysis_results)
        
        if results_df is None:
            logger.error("❌ 报告生成失败")
            return False
        
        logger.success("🎉 真实股票数据分析完成！")
        logger.info("=" * 60)
        logger.info("📁 输出文件:")
        logger.info("  real_stock_analysis_results.csv - 详细分析结果")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 分析过程失败: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
