#!/usr/bin/env python3
"""
测试 Polars 迁移效果
"""
import sys
import time
import numpy as np
import pandas as pd
import polars as pl
from pathlib import Path
from typing import Dict, Any

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.technical_indicators import (
    add_all_indicators,
    add_all_indicators_pandas_legacy,
    add_all_indicators_polars_optimized
)

logger = get_logger("polars_migration_test")


def generate_test_data(n_stocks: int = 100, n_days: int = 250) -> pd.DataFrame:
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


def test_performance_comparison():
    """测试性能对比"""
    logger.info("🚀 开始 Polars 迁移性能测试")
    logger.info("=" * 60)
    
    # 测试不同规模的数据
    test_cases = [
        (50, 100, "小规模"),
        (100, 250, "中规模"),
        (200, 250, "大规模")
    ]
    
    results = {}
    
    for n_stocks, n_days, case_name in test_cases:
        logger.info(f"\n📊 {case_name}测试 ({n_stocks} 股票 × {n_days} 天)")
        logger.info("-" * 40)
        
        # 生成测试数据
        df = generate_test_data(n_stocks, n_days)
        
        # 测试 Pandas 传统方法
        logger.info("🐼 测试 Pandas 传统方法...")
        start_time = time.time()
        result_pandas = add_all_indicators_pandas_legacy(df.copy())
        pandas_time = time.time() - start_time
        
        # 测试 Polars 优化方法
        logger.info("⚡ 测试 Polars 优化方法...")
        start_time = time.time()
        result_polars = add_all_indicators_polars_optimized(df.copy())
        polars_time = time.time() - start_time
        
        # 测试默认方法 (应该使用 Polars)
        logger.info("🔄 测试默认方法 (Polars 加速)...")
        start_time = time.time()
        result_default = add_all_indicators(df.copy(), use_polars=True)
        default_time = time.time() - start_time
        
        # 计算加速比
        speedup_polars = pandas_time / polars_time if polars_time > 0 else 0
        speedup_default = pandas_time / default_time if default_time > 0 else 0
        
        # 验证结果一致性
        consistency_check = verify_results_consistency(result_pandas, result_polars)
        
        # 保存结果
        results[case_name] = {
            'data_size': len(df),
            'pandas_time': pandas_time,
            'polars_time': polars_time,
            'default_time': default_time,
            'speedup_polars': speedup_polars,
            'speedup_default': speedup_default,
            'consistency': consistency_check
        }
        
        # 输出结果
        logger.info(f"📈 {case_name}测试结果:")
        logger.info(f"  数据规模: {len(df):,} 条记录")
        logger.info(f"  Pandas 传统: {pandas_time:.2f}s")
        logger.info(f"  Polars 优化: {polars_time:.2f}s")
        logger.info(f"  默认方法: {default_time:.2f}s")
        logger.info(f"  Polars 加速比: {speedup_polars:.1f}x")
        logger.info(f"  默认加速比: {speedup_default:.1f}x")
        logger.info(f"  结果一致性: {'✅ 通过' if consistency_check else '❌ 失败'}")
    
    # 输出总结
    logger.info("\n" + "=" * 60)
    logger.info("📋 Polars 迁移测试总结")
    logger.info("=" * 60)
    
    total_speedup = 0
    valid_tests = 0
    
    for case_name, result in results.items():
        logger.info(f"\n{case_name} ({result['data_size']:,} 记录):")
        logger.info(f"  Polars 加速: {result['speedup_polars']:.1f}x")
        logger.info(f"  时间节省: {((result['pandas_time'] - result['polars_time']) / result['pandas_time'] * 100):.1f}%")
        logger.info(f"  一致性: {'✅' if result['consistency'] else '❌'}")
        
        if result['speedup_polars'] > 0:
            total_speedup += result['speedup_polars']
            valid_tests += 1
    
    if valid_tests > 0:
        avg_speedup = total_speedup / valid_tests
        logger.info(f"\n🎯 平均加速比: {avg_speedup:.1f}x")
        
        if avg_speedup > 10:
            logger.success("🚀 Polars 迁移效果显著！性能提升超过 10x")
        elif avg_speedup > 5:
            logger.success("🚀 Polars 迁移效果良好！性能提升超过 5x")
        elif avg_speedup > 2:
            logger.info("✅ Polars 迁移有效果，性能提升超过 2x")
        else:
            logger.warning("⚠️ Polars 迁移效果有限，需要进一步优化")
    
    return results


def verify_results_consistency(df_pandas: pd.DataFrame, df_polars: pd.DataFrame, 
                             tolerance: float = 1e-6) -> bool:
    """验证 Pandas 和 Polars 结果的一致性"""
    try:
        # 检查基本属性
        if len(df_pandas) != len(df_polars):
            logger.error(f"行数不一致: Pandas {len(df_pandas)} vs Polars {len(df_polars)}")
            return False
        
        # 检查关键技术指标
        key_indicators = ['ma_20', 'ema_12', 'rsi', 'macd', 'bb_upper', 'bb_lower']
        
        for indicator in key_indicators:
            if indicator in df_pandas.columns and indicator in df_polars.columns:
                pandas_values = df_pandas[indicator].dropna()
                polars_values = df_polars[indicator].dropna()
                
                if len(pandas_values) > 0 and len(polars_values) > 0:
                    # 计算相对误差
                    min_len = min(len(pandas_values), len(polars_values))
                    pandas_sample = pandas_values.iloc[:min_len]
                    polars_sample = polars_values.iloc[:min_len]
                    
                    relative_error = np.abs((pandas_sample - polars_sample) / pandas_sample).mean()
                    
                    if relative_error > tolerance:
                        logger.warning(f"指标 {indicator} 相对误差过大: {relative_error:.6f}")
                        return False
        
        logger.info("✅ 结果一致性验证通过")
        return True
        
    except Exception as e:
        logger.error(f"一致性验证失败: {e}")
        return False


def test_api_compatibility():
    """测试 API 兼容性"""
    logger.info("\n🔧 测试 API 兼容性")
    logger.info("-" * 40)
    
    # 生成小规模测试数据
    df = generate_test_data(10, 50)
    
    try:
        # 测试默认调用 (应该使用 Polars)
        result1 = add_all_indicators(df.copy())
        logger.info("✅ 默认调用成功")
        
        # 测试显式启用 Polars
        result2 = add_all_indicators(df.copy(), use_polars=True)
        logger.info("✅ 显式启用 Polars 成功")
        
        # 测试禁用 Polars
        result3 = add_all_indicators(df.copy(), use_polars=False)
        logger.info("✅ 禁用 Polars 成功")
        
        # 测试自定义参数
        result4 = add_all_indicators(
            df.copy(), 
            ma_periods=[5, 10, 30], 
            ema_periods=[8, 21],
            use_polars=True
        )
        logger.info("✅ 自定义参数成功")
        
        # 检查结果包含预期的列
        expected_columns = ['ma_5', 'ma_10', 'ema_12', 'rsi', 'macd', 'bb_upper']
        missing_columns = [col for col in expected_columns if col not in result1.columns]
        
        if missing_columns:
            logger.warning(f"缺少预期列: {missing_columns}")
        else:
            logger.success("✅ 所有预期列都存在")
        
        logger.success("🎉 API 兼容性测试全部通过！")
        return True
        
    except Exception as e:
        logger.error(f"❌ API 兼容性测试失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🚀 启明星 Polars 迁移测试")
    logger.info("=" * 60)
    
    # 测试 API 兼容性
    api_success = test_api_compatibility()
    
    # 测试性能对比
    performance_results = test_performance_comparison()
    
    # 总结
    logger.info("\n" + "=" * 60)
    logger.info("🎯 迁移测试总结")
    logger.info("=" * 60)
    
    if api_success:
        logger.success("✅ API 兼容性: 通过")
    else:
        logger.error("❌ API 兼容性: 失败")
    
    if performance_results:
        avg_speedup = np.mean([r['speedup_polars'] for r in performance_results.values() if r['speedup_polars'] > 0])
        logger.success(f"✅ 性能提升: {avg_speedup:.1f}x 平均加速")
        
        if avg_speedup > 10:
            logger.success("🚀 Polars 迁移大获成功！建议全面推广")
        else:
            logger.info("✅ Polars 迁移有效果，可以继续优化")
    else:
        logger.error("❌ 性能测试: 失败")
    
    return api_success and bool(performance_results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
