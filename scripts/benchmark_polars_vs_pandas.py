#!/usr/bin/env python3
"""
Polars vs Pandas 性能基准测试
"""
import sys
import time
import psutil
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Callable
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("benchmark")

# 尝试导入 Polars
try:
    import polars as pl
    POLARS_AVAILABLE = True
    logger.info("✅ Polars 可用")
except ImportError:
    POLARS_AVAILABLE = False
    logger.warning("⚠️ Polars 未安装，将跳过 Polars 测试")


def generate_test_data(n_stocks: int = 1000, n_days: int = 250) -> pd.DataFrame:
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
            prices.append(max(new_price, 1.0))  # 价格不能为负
        
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


def measure_performance(func: Callable, *args, **kwargs) -> Dict[str, Any]:
    """测量函数性能"""
    # 获取初始内存使用
    process = psutil.Process()
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    
    # 执行函数并测量时间
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    
    # 获取峰值内存使用
    peak_memory = process.memory_info().rss / 1024 / 1024  # MB
    memory_used = peak_memory - initial_memory
    
    return {
        'execution_time': end_time - start_time,
        'memory_used': max(0, memory_used),
        'result_size': len(result) if hasattr(result, '__len__') else 0
    }


def benchmark_technical_indicators_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Pandas 技术指标计算基准"""
    logger.info("🐼 Pandas 技术指标计算...")
    
    # 按股票分组计算技术指标
    def calculate_indicators_for_stock(group):
        group = group.sort_values('trade_date').copy()
        
        # 移动平均
        group['ma_5'] = group['close_price'].rolling(5, min_periods=1).mean()
        group['ma_20'] = group['close_price'].rolling(20, min_periods=1).mean()
        
        # 简单 RSI 计算
        delta = group['close_price'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14, min_periods=1).mean()
        rs = gain / loss.replace(0, np.inf)
        group['rsi'] = 100 - (100 / (1 + rs))
        
        # 成交量比率
        group['vol_ma'] = group['vol'].rolling(20, min_periods=1).mean()
        group['vol_ratio'] = group['vol'] / group['vol_ma']
        
        return group
    
    result = df.groupby('ts_code').apply(calculate_indicators_for_stock)
    return result.reset_index(drop=True)


def benchmark_technical_indicators_polars(df: pd.DataFrame) -> pd.DataFrame:
    """Polars 技术指标计算基准"""
    if not POLARS_AVAILABLE:
        return df
    
    logger.info("⚡ Polars 技术指标计算...")
    
    # 转换为 Polars DataFrame
    pl_df = pl.from_pandas(df)
    
    # 使用 Polars 高效计算
    result = pl_df.sort(['ts_code', 'trade_date']).with_columns([
        # 移动平均
        pl.col('close_price').rolling_mean(5).over('ts_code').alias('ma_5'),
        pl.col('close_price').rolling_mean(20).over('ts_code').alias('ma_20'),
        
        # 成交量比率
        pl.col('vol').rolling_mean(20).over('ts_code').alias('vol_ma'),
    ]).with_columns([
        (pl.col('vol') / pl.col('vol_ma')).alias('vol_ratio'),
        
        # 简化的 RSI 计算
        (pl.col('close_price').diff().over('ts_code')).alias('price_diff')
    ]).with_columns([
        pl.when(pl.col('price_diff') > 0)
        .then(pl.col('price_diff'))
        .otherwise(0)
        .rolling_mean(14).over('ts_code').alias('gain'),
        
        pl.when(pl.col('price_diff') < 0)
        .then(-pl.col('price_diff'))
        .otherwise(0)
        .rolling_mean(14).over('ts_code').alias('loss')
    ]).with_columns([
        (100 - (100 / (1 + pl.col('gain') / pl.col('loss')))).alias('rsi')
    ]).drop(['price_diff', 'gain', 'loss'])
    
    return result.to_pandas()


def benchmark_aggregation_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Pandas 聚合操作基准"""
    logger.info("🐼 Pandas 聚合操作...")
    
    # 复杂聚合操作
    result = df.groupby('ts_code').agg({
        'close_price': ['mean', 'std', 'min', 'max'],
        'vol': ['sum', 'mean'],
        'pct_chg': ['mean', 'std']
    }).reset_index()
    
    # 展平列名
    result.columns = ['_'.join(col).strip() if col[1] else col[0] for col in result.columns]
    
    return result


def benchmark_aggregation_polars(df: pd.DataFrame) -> pd.DataFrame:
    """Polars 聚合操作基准"""
    if not POLARS_AVAILABLE:
        return df
    
    logger.info("⚡ Polars 聚合操作...")
    
    pl_df = pl.from_pandas(df)
    
    result = pl_df.group_by('ts_code').agg([
        pl.col('close_price').mean().alias('close_price_mean'),
        pl.col('close_price').std().alias('close_price_std'),
        pl.col('close_price').min().alias('close_price_min'),
        pl.col('close_price').max().alias('close_price_max'),
        pl.col('vol').sum().alias('vol_sum'),
        pl.col('vol').mean().alias('vol_mean'),
        pl.col('pct_chg').mean().alias('pct_chg_mean'),
        pl.col('pct_chg').std().alias('pct_chg_std')
    ])
    
    return result.to_pandas()


def benchmark_filtering_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Pandas 过滤操作基准"""
    logger.info("🐼 Pandas 过滤操作...")
    
    # 复杂过滤条件
    result = df[
        (df['close_price'] > 20) & 
        (df['close_price'] < 80) &
        (df['vol'] > 5000000) &
        (df['pct_chg'].abs() < 5)
    ].copy()
    
    return result


def benchmark_filtering_polars(df: pd.DataFrame) -> pd.DataFrame:
    """Polars 过滤操作基准"""
    if not POLARS_AVAILABLE:
        return df
    
    logger.info("⚡ Polars 过滤操作...")
    
    pl_df = pl.from_pandas(df)
    
    result = pl_df.filter(
        (pl.col('close_price') > 20) & 
        (pl.col('close_price') < 80) &
        (pl.col('vol') > 5000000) &
        (pl.col('pct_chg').abs() < 5)
    )
    
    return result.to_pandas()


def run_benchmark_suite():
    """运行完整基准测试套件"""
    logger.info("=" * 60)
    logger.info("🚀 Polars vs Pandas 性能基准测试")
    logger.info("=" * 60)
    
    # 测试不同数据规模
    test_sizes = [
        (100, 30, "小规模"),    # 3K 记录
        (500, 100, "中规模"),   # 50K 记录
        (1000, 250, "大规模")   # 250K 记录
    ]
    
    results = {}
    
    for n_stocks, n_days, size_name in test_sizes:
        logger.info(f"\n📊 {size_name}测试 ({n_stocks} 股票 × {n_days} 天)")
        logger.info("-" * 40)
        
        # 生成测试数据
        df = generate_test_data(n_stocks, n_days)
        
        # 测试技术指标计算
        logger.info("🧮 技术指标计算测试...")
        pandas_tech = measure_performance(benchmark_technical_indicators_pandas, df)
        
        if POLARS_AVAILABLE:
            polars_tech = measure_performance(benchmark_technical_indicators_polars, df)
            tech_speedup = pandas_tech['execution_time'] / polars_tech['execution_time']
            tech_memory_ratio = polars_tech['memory_used'] / max(pandas_tech['memory_used'], 1)
        else:
            polars_tech = {'execution_time': 0, 'memory_used': 0}
            tech_speedup = 0
            tech_memory_ratio = 0
        
        # 测试聚合操作
        logger.info("📈 聚合操作测试...")
        pandas_agg = measure_performance(benchmark_aggregation_pandas, df)
        
        if POLARS_AVAILABLE:
            polars_agg = measure_performance(benchmark_aggregation_polars, df)
            agg_speedup = pandas_agg['execution_time'] / polars_agg['execution_time']
            agg_memory_ratio = polars_agg['memory_used'] / max(pandas_agg['memory_used'], 1)
        else:
            polars_agg = {'execution_time': 0, 'memory_used': 0}
            agg_speedup = 0
            agg_memory_ratio = 0
        
        # 测试过滤操作
        logger.info("🔍 过滤操作测试...")
        pandas_filter = measure_performance(benchmark_filtering_pandas, df)
        
        if POLARS_AVAILABLE:
            polars_filter = measure_performance(benchmark_filtering_polars, df)
            filter_speedup = pandas_filter['execution_time'] / polars_filter['execution_time']
            filter_memory_ratio = polars_filter['memory_used'] / max(pandas_filter['memory_used'], 1)
        else:
            polars_filter = {'execution_time': 0, 'memory_used': 0}
            filter_speedup = 0
            filter_memory_ratio = 0
        
        # 保存结果
        results[size_name] = {
            'data_size': len(df),
            'technical_indicators': {
                'pandas_time': pandas_tech['execution_time'],
                'polars_time': polars_tech['execution_time'],
                'speedup': tech_speedup,
                'memory_ratio': tech_memory_ratio
            },
            'aggregation': {
                'pandas_time': pandas_agg['execution_time'],
                'polars_time': polars_agg['execution_time'],
                'speedup': agg_speedup,
                'memory_ratio': agg_memory_ratio
            },
            'filtering': {
                'pandas_time': pandas_filter['execution_time'],
                'polars_time': polars_filter['execution_time'],
                'speedup': filter_speedup,
                'memory_ratio': filter_memory_ratio
            }
        }
        
        # 输出结果
        logger.info(f"📊 {size_name}测试结果:")
        logger.info(f"  数据规模: {len(df):,} 条记录")
        
        if POLARS_AVAILABLE:
            logger.info(f"  技术指标计算:")
            logger.info(f"    Pandas: {pandas_tech['execution_time']:.2f}s")
            logger.info(f"    Polars: {polars_tech['execution_time']:.2f}s")
            logger.info(f"    加速比: {tech_speedup:.1f}x")
            
            logger.info(f"  聚合操作:")
            logger.info(f"    Pandas: {pandas_agg['execution_time']:.2f}s")
            logger.info(f"    Polars: {polars_agg['execution_time']:.2f}s")
            logger.info(f"    加速比: {agg_speedup:.1f}x")
            
            logger.info(f"  过滤操作:")
            logger.info(f"    Pandas: {pandas_filter['execution_time']:.2f}s")
            logger.info(f"    Polars: {polars_filter['execution_time']:.2f}s")
            logger.info(f"    加速比: {filter_speedup:.1f}x")
        else:
            logger.warning("  Polars 未安装，跳过性能对比")
    
    # 输出总结
    logger.info("\n" + "=" * 60)
    logger.info("📋 基准测试总结")
    logger.info("=" * 60)
    
    if POLARS_AVAILABLE:
        for size_name, result in results.items():
            logger.info(f"\n{size_name} ({result['data_size']:,} 记录):")
            logger.info(f"  技术指标: {result['technical_indicators']['speedup']:.1f}x 加速")
            logger.info(f"  聚合操作: {result['aggregation']['speedup']:.1f}x 加速")
            logger.info(f"  过滤操作: {result['filtering']['speedup']:.1f}x 加速")
        
        # 计算平均加速比
        avg_tech_speedup = np.mean([r['technical_indicators']['speedup'] for r in results.values()])
        avg_agg_speedup = np.mean([r['aggregation']['speedup'] for r in results.values()])
        avg_filter_speedup = np.mean([r['filtering']['speedup'] for r in results.values()])
        
        logger.info(f"\n🎯 平均性能提升:")
        logger.info(f"  技术指标计算: {avg_tech_speedup:.1f}x")
        logger.info(f"  聚合操作: {avg_agg_speedup:.1f}x")
        logger.info(f"  过滤操作: {avg_filter_speedup:.1f}x")
        
        if avg_tech_speedup > 2.0:
            logger.success("🚀 Polars 在技术指标计算上有显著优势！")
        if avg_agg_speedup > 3.0:
            logger.success("🚀 Polars 在聚合操作上有显著优势！")
        
        logger.info(f"\n💡 推荐:")
        if avg_tech_speedup > 1.5 or avg_agg_speedup > 2.0:
            logger.success("✅ 强烈推荐迁移到 Polars！")
        else:
            logger.info("⚠️ 性能提升有限，需要权衡迁移成本")
    else:
        logger.warning("❌ 无法进行性能对比，请安装 Polars: pip install polars")
    
    return results


if __name__ == "__main__":
    run_benchmark_suite()
