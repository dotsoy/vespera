#!/usr/bin/env python3
"""
测试技术分析器性能提升
"""
import sys
import time
import numpy as np
import pandas as pd
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.analyzers.technical_analyzer import TechnicalAnalyzer

logger = get_logger("technical_analyzer_performance")


def generate_test_data(n_stocks: int = 100, n_days: int = 60) -> pd.DataFrame:
    """生成测试数据"""
    logger.info(f"生成测试数据: {n_stocks} 只股票 × {n_days} 天")
    
    np.random.seed(42)
    data = []
    
    for i in range(n_stocks):
        ts_code = f"{str(i+1).zfill(6)}.SZ"
        
        # 生成价格数据
        base_price = np.random.uniform(10, 100)
        price_changes = np.random.normal(0, 0.02, n_days)
        prices = [base_price]
        
        for change in price_changes[1:]:
            new_price = prices[-1] * (1 + change)
            prices.append(max(new_price, 1.0))
        
        for day in range(n_days):
            open_price = prices[day] * np.random.uniform(0.98, 1.02)
            high_price = max(open_price, prices[day]) * np.random.uniform(1.0, 1.05)
            low_price = min(open_price, prices[day]) * np.random.uniform(0.95, 1.0)
            close_price = prices[day]
            volume = np.random.randint(1000000, 100000000)
            
            data.append({
                'ts_code': ts_code,
                'trade_date': f"2024-{(day % 12) + 1:02d}-{(day % 28) + 1:02d}",
                'open_price': round(open_price, 2),
                'high_price': round(high_price, 2),
                'low_price': round(low_price, 2),
                'close_price': round(close_price, 2),
                'vol': volume,
                'pct_chg': round(price_changes[day] * 100, 2)
            })
    
    df = pd.DataFrame(data)
    logger.info(f"测试数据生成完成: {len(df)} 条记录")
    return df


def test_technical_analyzer_performance():
    """测试技术分析器性能"""
    logger.info("🚀 技术分析器性能测试")
    logger.info("=" * 60)
    
    # 创建技术分析器实例
    analyzer = TechnicalAnalyzer()
    
    # 测试不同规模的数据
    test_cases = [
        (50, 60, "小规模"),
        (100, 60, "中规模"),
        (200, 60, "大规模")
    ]
    
    results = {}
    
    for n_stocks, n_days, case_name in test_cases:
        logger.info(f"\n📊 {case_name}测试 ({n_stocks} 股票 × {n_days} 天)")
        logger.info("-" * 40)
        
        # 生成测试数据
        df = generate_test_data(n_stocks, n_days)
        
        # 测试技术指标计算性能
        logger.info("⚡ 测试技术指标计算...")
        start_time = time.time()
        
        # 按股票分组计算技术指标
        processed_stocks = 0
        for ts_code in df['ts_code'].unique():
            stock_data = df[df['ts_code'] == ts_code].sort_values('trade_date')
            try:
                # 使用技术分析器计算指标
                result_df = analyzer.calculate_indicators(stock_data)
                processed_stocks += 1
            except Exception as e:
                logger.warning(f"处理股票 {ts_code} 失败: {e}")
        
        calculation_time = time.time() - start_time
        
        # 测试完整分析流程性能 (模拟)
        logger.info("📈 测试完整分析流程...")
        start_time = time.time()
        
        analysis_results = []
        for ts_code in df['ts_code'].unique()[:10]:  # 只测试前10只股票
            stock_data = df[df['ts_code'] == ts_code].sort_values('trade_date')
            try:
                # 计算技术指标
                stock_data = analyzer.calculate_indicators(stock_data)
                
                # 计算各项评分
                trend_score = analyzer.calculate_trend_score(stock_data)
                momentum_score = analyzer.calculate_momentum_score(stock_data)
                volume_health_score = analyzer.calculate_volume_health_score(stock_data)
                
                # 识别形态
                patterns = analyzer.identify_patterns(stock_data)
                
                # 计算支撑阻力位
                support, resistance = analyzer.calculate_support_resistance(stock_data)
                
                analysis_results.append({
                    'ts_code': ts_code,
                    'trend_score': trend_score,
                    'momentum_score': momentum_score,
                    'volume_health_score': volume_health_score,
                    'patterns': patterns,
                    'support': support,
                    'resistance': resistance
                })
                
            except Exception as e:
                logger.warning(f"分析股票 {ts_code} 失败: {e}")
        
        analysis_time = time.time() - start_time
        
        # 保存结果
        results[case_name] = {
            'data_size': len(df),
            'stocks_count': n_stocks,
            'processed_stocks': processed_stocks,
            'calculation_time': calculation_time,
            'analysis_time': analysis_time,
            'analysis_results': len(analysis_results),
            'avg_time_per_stock': calculation_time / max(processed_stocks, 1),
            'throughput': processed_stocks / calculation_time if calculation_time > 0 else 0
        }
        
        # 输出结果
        logger.info(f"📈 {case_name}测试结果:")
        logger.info(f"  数据规模: {len(df):,} 条记录")
        logger.info(f"  股票数量: {n_stocks}")
        logger.info(f"  处理成功: {processed_stocks}/{n_stocks}")
        logger.info(f"  指标计算时间: {calculation_time:.2f}s")
        logger.info(f"  完整分析时间: {analysis_time:.2f}s")
        logger.info(f"  平均每股时间: {calculation_time / max(processed_stocks, 1):.3f}s")
        logger.info(f"  处理吞吐量: {processed_stocks / calculation_time:.1f} 股票/秒")
        
        # 显示分析结果样本
        if analysis_results:
            sample = analysis_results[0]
            logger.info(f"  样本分析结果:")
            logger.info(f"    趋势评分: {sample['trend_score']:.3f}")
            logger.info(f"    动量评分: {sample['momentum_score']:.3f}")
            logger.info(f"    量能评分: {sample['volume_health_score']:.3f}")
            logger.info(f"    识别形态: {len(sample['patterns'])} 个")
    
    # 输出总结
    logger.info("\n" + "=" * 60)
    logger.info("📋 技术分析器性能总结")
    logger.info("=" * 60)
    
    total_throughput = 0
    valid_tests = 0
    
    for case_name, result in results.items():
        logger.info(f"\n{case_name} ({result['stocks_count']} 股票):")
        logger.info(f"  处理成功率: {result['processed_stocks']}/{result['stocks_count']} ({result['processed_stocks']/result['stocks_count']*100:.1f}%)")
        logger.info(f"  平均每股时间: {result['avg_time_per_stock']:.3f}s")
        logger.info(f"  处理吞吐量: {result['throughput']:.1f} 股票/秒")
        
        if result['throughput'] > 0:
            total_throughput += result['throughput']
            valid_tests += 1
    
    if valid_tests > 0:
        avg_throughput = total_throughput / valid_tests
        logger.info(f"\n🎯 平均处理吞吐量: {avg_throughput:.1f} 股票/秒")
        
        # 估算实际生产环境性能
        daily_stocks = 5000  # 假设每日分析5000只股票
        estimated_time = daily_stocks / avg_throughput / 60  # 转换为分钟
        
        logger.info(f"📊 生产环境估算:")
        logger.info(f"  每日分析 {daily_stocks} 只股票")
        logger.info(f"  预计耗时: {estimated_time:.1f} 分钟")
        
        if estimated_time < 10:
            logger.success("🚀 性能优秀！可以在10分钟内完成全市场分析")
        elif estimated_time < 30:
            logger.success("✅ 性能良好！可以在30分钟内完成全市场分析")
        elif estimated_time < 60:
            logger.info("⚠️ 性能一般，需要1小时内完成全市场分析")
        else:
            logger.warning("❌ 性能需要优化，全市场分析耗时过长")
    
    return results


def main():
    """主函数"""
    logger.info("🚀 启明星技术分析器性能测试")
    logger.info("=" * 60)
    
    try:
        # 运行性能测试
        results = test_technical_analyzer_performance()
        
        if results:
            logger.success("🎉 技术分析器性能测试完成！")
            return True
        else:
            logger.error("❌ 技术分析器性能测试失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 测试过程中发生错误: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
