"""
技术指标计算工具模块 - 基于 Polars + Tulipy
高性能版本，支持 Pandas 和 Polars 双接口
"""
import numpy as np
import pandas as pd
import polars as pl
import tulipy as ti
from typing import Tuple, Optional, Union
from loguru import logger


def safe_indicator_calculation(func, *args, **kwargs):
    """
    安全的指标计算包装器
    
    Args:
        func: Tulipy 指标函数
        *args: 位置参数
        **kwargs: 关键字参数
        
    Returns:
        计算结果或 None
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.warning(f"指标计算失败: {func.__name__}, 错误: {e}")
        return None


def calculate_sma(data: np.ndarray, period: int) -> np.ndarray:
    """
    计算简单移动平均线
    
    Args:
        data: 价格数据
        period: 周期
        
    Returns:
        SMA 数组，前面用 NaN 填充
    """
    if len(data) < period:
        return np.full(len(data), np.nan)
    
    sma_values = safe_indicator_calculation(ti.sma, data, period=period)
    if sma_values is None:
        return np.full(len(data), np.nan)
    
    # 填充前面的 NaN 值
    result = np.full(len(data), np.nan)
    result[period-1:] = sma_values
    return result


def calculate_ema(data: np.ndarray, period: int) -> np.ndarray:
    """
    计算指数移动平均线
    
    Args:
        data: 价格数据
        period: 周期
        
    Returns:
        EMA 数组，前面用 NaN 填充
    """
    if len(data) < period:
        return np.full(len(data), np.nan)
    
    ema_values = safe_indicator_calculation(ti.ema, data, period=period)
    if ema_values is None:
        return np.full(len(data), np.nan)
    
    # 填充前面的 NaN 值
    result = np.full(len(data), np.nan)
    if len(ema_values) == len(data):
        result = ema_values
    else:
        result[period-1:period-1+len(ema_values)] = ema_values
    return result


def calculate_rsi(data: np.ndarray, period: int = 14) -> np.ndarray:
    """
    计算 RSI 指标
    
    Args:
        data: 价格数据
        period: 周期，默认 14
        
    Returns:
        RSI 数组，前面用 NaN 填充
    """
    if len(data) < period + 1:
        return np.full(len(data), np.nan)
    
    rsi_values = safe_indicator_calculation(ti.rsi, data, period=period)
    if rsi_values is None:
        return np.full(len(data), np.nan)
    
    # 填充前面的 NaN 值
    result = np.full(len(data), np.nan)
    result[period:] = rsi_values
    return result


def calculate_macd(data: np.ndarray, fast_period: int = 12, 
                  slow_period: int = 26, signal_period: int = 9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    计算 MACD 指标
    
    Args:
        data: 价格数据
        fast_period: 快线周期
        slow_period: 慢线周期
        signal_period: 信号线周期
        
    Returns:
        (MACD, Signal, Histogram) 三个数组
    """
    data_len = len(data)
    
    if data_len < slow_period + signal_period:
        return (np.full(data_len, np.nan), 
                np.full(data_len, np.nan), 
                np.full(data_len, np.nan))
    
    macd_result = safe_indicator_calculation(
        ti.macd, data, 
        short_period=fast_period,
        long_period=slow_period, 
        signal_period=signal_period
    )
    
    if macd_result is None:
        return (np.full(data_len, np.nan), 
                np.full(data_len, np.nan), 
                np.full(data_len, np.nan))
    
    macd_values, signal_values, hist_values = macd_result
    
    # 填充前面的 NaN 值
    start_idx = slow_period + signal_period - 2
    
    macd_full = np.full(data_len, np.nan)
    signal_full = np.full(data_len, np.nan)
    hist_full = np.full(data_len, np.nan)
    
    macd_full[start_idx:] = macd_values
    signal_full[start_idx:] = signal_values
    hist_full[start_idx:] = hist_values
    
    return macd_full, signal_full, hist_full


def calculate_bbands(data: np.ndarray, period: int = 20, 
                    stddev: float = 2.0) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    计算布林带
    
    Args:
        data: 价格数据
        period: 周期
        stddev: 标准差倍数
        
    Returns:
        (下轨, 中轨, 上轨) 三个数组
    """
    data_len = len(data)
    
    if data_len < period:
        return (np.full(data_len, np.nan), 
                np.full(data_len, np.nan), 
                np.full(data_len, np.nan))
    
    bb_result = safe_indicator_calculation(ti.bbands, data, period=period, stddev=stddev)
    
    if bb_result is None:
        return (np.full(data_len, np.nan), 
                np.full(data_len, np.nan), 
                np.full(data_len, np.nan))
    
    bb_lower, bb_middle, bb_upper = bb_result
    
    # 填充前面的 NaN 值
    lower_full = np.full(data_len, np.nan)
    middle_full = np.full(data_len, np.nan)
    upper_full = np.full(data_len, np.nan)
    
    lower_full[period-1:] = bb_lower
    middle_full[period-1:] = bb_middle
    upper_full[period-1:] = bb_upper
    
    return lower_full, middle_full, upper_full


def calculate_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, 
                 period: int = 14) -> np.ndarray:
    """
    计算平均真实波幅 (ATR)
    
    Args:
        high: 最高价数组
        low: 最低价数组
        close: 收盘价数组
        period: 周期
        
    Returns:
        ATR 数组
    """
    data_len = len(close)
    
    if data_len < period:
        return np.full(data_len, np.nan)
    
    atr_values = safe_indicator_calculation(ti.atr, high, low, close, period=period)
    
    if atr_values is None:
        return np.full(data_len, np.nan)
    
    # 填充前面的 NaN 值
    result = np.full(data_len, np.nan)
    result[period-1:] = atr_values
    return result


def calculate_stoch(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                   k_period: int = 14, k_slowing_period: int = 3, 
                   d_period: int = 3) -> Tuple[np.ndarray, np.ndarray]:
    """
    计算随机指标 (KD)
    
    Args:
        high: 最高价数组
        low: 最低价数组
        close: 收盘价数组
        k_period: K 值周期
        k_slowing_period: K 值平滑周期
        d_period: D 值周期
        
    Returns:
        (K, D) 两个数组
    """
    data_len = len(close)
    
    if data_len < k_period + k_slowing_period + d_period:
        return np.full(data_len, np.nan), np.full(data_len, np.nan)
    
    stoch_result = safe_indicator_calculation(
        ti.stoch, high, low, close,
        k_period=k_period,
        k_slowing_period=k_slowing_period,
        d_period=d_period
    )
    
    if stoch_result is None:
        return np.full(data_len, np.nan), np.full(data_len, np.nan)
    
    k_values, d_values = stoch_result
    
    # 填充前面的 NaN 值
    k_start_idx = k_period + k_slowing_period - 2
    d_start_idx = k_period + k_slowing_period + d_period - 3
    
    k_full = np.full(data_len, np.nan)
    d_full = np.full(data_len, np.nan)
    
    k_full[k_start_idx:] = k_values
    d_full[d_start_idx:] = d_values
    
    return k_full, d_full


def calculate_willr(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                   period: int = 14) -> np.ndarray:
    """
    计算威廉指标 (Williams %R)
    
    Args:
        high: 最高价数组
        low: 最低价数组
        close: 收盘价数组
        period: 周期
        
    Returns:
        Williams %R 数组
    """
    data_len = len(close)
    
    if data_len < period:
        return np.full(data_len, np.nan)
    
    willr_values = safe_indicator_calculation(ti.willr, high, low, close, period=period)
    
    if willr_values is None:
        return np.full(data_len, np.nan)
    
    # 填充前面的 NaN 值
    result = np.full(data_len, np.nan)
    result[period-1:] = willr_values
    return result


def calculate_obv(close: np.ndarray, volume: np.ndarray) -> np.ndarray:
    """
    计算能量潮 (OBV)
    
    Args:
        close: 收盘价数组
        volume: 成交量数组
        
    Returns:
        OBV 数组
    """
    data_len = len(close)
    
    if data_len < 2:
        return np.full(data_len, np.nan)
    
    obv_values = safe_indicator_calculation(ti.obv, close, volume)
    
    if obv_values is None:
        return np.full(data_len, np.nan)
    
    # 填充前面的 NaN 值
    result = np.full(data_len, np.nan)
    result[1:] = obv_values
    return result


def add_all_indicators(df: Union[pd.DataFrame, pl.DataFrame],
                      ma_periods: list = [5, 10, 20, 60],
                      ema_periods: list = [12, 26],
                      use_polars: bool = True) -> Union[pd.DataFrame, pl.DataFrame]:
    """
    为 DataFrame 添加所有技术指标 (支持 Pandas 和 Polars)

    Args:
        df: 包含 OHLCV 数据的 DataFrame (Pandas 或 Polars)
        ma_periods: MA 周期列表
        ema_periods: EMA 周期列表
        use_polars: 是否使用 Polars 高性能计算

    Returns:
        添加了技术指标的 DataFrame
    """
    try:
        # 检查输入类型
        is_pandas_input = isinstance(df, pd.DataFrame)
        is_polars_input = isinstance(df, pl.DataFrame)

        if is_pandas_input and df.empty:
            logger.warning("数据为空，无法计算技术指标")
            return df
        elif is_polars_input and df.height == 0:
            logger.warning("数据为空，无法计算技术指标")
            return df

        # 如果启用 Polars 且输入是 Pandas，则使用高性能路径
        if use_polars and is_pandas_input:
            return add_all_indicators_polars_optimized(df, ma_periods, ema_periods)
        elif is_polars_input:
            return add_all_indicators_polars_native(df, ma_periods, ema_periods)
        else:
            return add_all_indicators_pandas_legacy(df, ma_periods, ema_periods)

    except Exception as e:
        logger.error(f"计算技术指标失败: {e}")
        return df


def add_all_indicators_polars_optimized(df: pd.DataFrame,
                                       ma_periods: list = [5, 10, 20, 60],
                                       ema_periods: list = [12, 26]) -> pd.DataFrame:
    """
    使用 Polars 高性能计算技术指标，返回 Pandas DataFrame (兼容性接口)

    Args:
        df: Pandas DataFrame
        ma_periods: MA 周期列表
        ema_periods: EMA 周期列表

    Returns:
        添加了技术指标的 Pandas DataFrame
    """
    try:
        if df.empty:
            logger.warning("数据为空，无法计算技术指标")
            return df

        if len(df) < 30:
            logger.warning(f"数据不足({len(df)}条)，将计算部分技术指标")

        # 转换为 Polars DataFrame
        pl_df = pl.from_pandas(df)

        # 使用 Polars 高性能计算
        result_pl = add_all_indicators_polars_native(pl_df, ma_periods, ema_periods)

        # 转换回 Pandas DataFrame
        result_pd = result_pl.to_pandas()

        logger.info(f"成功计算 {len(result_pd)} 条记录的技术指标 (Polars 加速)")
        return result_pd

    except Exception as e:
        logger.error(f"Polars 计算失败，回退到 Pandas: {e}")
        return add_all_indicators_pandas_legacy(df, ma_periods, ema_periods)


def add_all_indicators_polars_native(df: pl.DataFrame,
                                   ma_periods: list = [5, 10, 20, 60],
                                   ema_periods: list = [12, 26]) -> pl.DataFrame:
    """
    Polars 原生高性能技术指标计算 (简化版)

    Args:
        df: Polars DataFrame
        ma_periods: MA 周期列表
        ema_periods: EMA 周期列表

    Returns:
        添加了技术指标的 Polars DataFrame
    """
    try:
        if df.height == 0:
            logger.warning("数据为空，无法计算技术指标")
            return df

        if df.height < 30:
            logger.warning(f"数据不足({df.height}条)，将计算部分技术指标")

        # 确保数据按股票代码和日期排序
        df_sorted = df.sort(['ts_code', 'trade_date'])

        # 使用简化的 Polars 表达式，避免复杂的窗口函数
        result = df_sorted.with_columns([
            # 移动平均线
            pl.col('close_price').rolling_mean(5, min_periods=1).alias('ma_5'),
            pl.col('close_price').rolling_mean(10, min_periods=1).alias('ma_10'),
            pl.col('close_price').rolling_mean(20, min_periods=1).alias('ma_20'),
            pl.col('close_price').rolling_mean(60, min_periods=1).alias('ma_60'),

            # 指数移动平均线
            pl.col('close_price').ewm_mean(span=12).alias('ema_12'),
            pl.col('close_price').ewm_mean(span=26).alias('ema_26'),

            # 成交量移动平均
            pl.col('vol').rolling_mean(20, min_periods=1).alias('vol_ma'),

            # 布林带中轨
            pl.col('close_price').rolling_mean(20, min_periods=1).alias('bb_middle'),
            pl.col('close_price').rolling_std(20, min_periods=1).alias('bb_std'),

            # 价格变化
            pl.col('close_price').diff().alias('price_diff'),

            # ATR 组件
            (pl.col('high_price') - pl.col('low_price')).alias('hl'),
            (pl.col('high_price') - pl.col('close_price').shift(1)).abs().alias('hc'),
            (pl.col('low_price') - pl.col('close_price').shift(1)).abs().alias('lc'),
        ]).with_columns([
            # 成交量比率
            (pl.col('vol') / pl.col('vol_ma')).alias('vol_ratio'),

            # MACD
            (pl.col('ema_12') - pl.col('ema_26')).alias('macd'),

            # 布林带上下轨
            (pl.col('bb_middle') + 2 * pl.col('bb_std')).alias('bb_upper'),
            (pl.col('bb_middle') - 2 * pl.col('bb_std')).alias('bb_lower'),

            # RSI 组件
            pl.when(pl.col('price_diff') > 0).then(pl.col('price_diff')).otherwise(0).alias('gain'),
            pl.when(pl.col('price_diff') < 0).then(-pl.col('price_diff')).otherwise(0).alias('loss'),

            # ATR 真实波幅
            pl.max_horizontal(['hl', 'hc', 'lc']).alias('true_range'),
        ]).with_columns([
            # MACD 信号线
            pl.col('macd').ewm_mean(span=9).alias('macd_signal'),

            # RSI
            (pl.col('gain').rolling_mean(14, min_periods=1) /
             pl.col('loss').rolling_mean(14, min_periods=1)).alias('rs'),

            # ATR
            pl.col('true_range').rolling_mean(14, min_periods=1).alias('atr'),
        ]).with_columns([
            # MACD 柱状图
            (pl.col('macd') - pl.col('macd_signal')).alias('macd_hist'),

            # RSI 最终值
            (100 - (100 / (1 + pl.col('rs')))).alias('rsi'),

            # 威廉指标 (简化版)
            ((pl.col('close_price') - pl.col('low_price').rolling_min(14, min_periods=1)) /
             (pl.col('high_price').rolling_max(14, min_periods=1) -
              pl.col('low_price').rolling_min(14, min_periods=1)) * (-100) + 100).alias('williams_r'),

            # KDJ K 值 (简化版)
            ((pl.col('close_price') - pl.col('low_price').rolling_min(14, min_periods=1)) /
             (pl.col('high_price').rolling_max(14, min_periods=1) -
              pl.col('low_price').rolling_min(14, min_periods=1)) * 100).alias('k_raw'),
        ]).with_columns([
            # KDJ K 和 D 值
            pl.col('k_raw').ewm_mean(span=3).alias('k'),
        ]).with_columns([
            pl.col('k').ewm_mean(span=3).alias('d'),
        ]).with_columns([
            # J 值
            (3 * pl.col('k') - 2 * pl.col('d')).alias('j'),

            # OBV (简化版，使用简单的成交量指标)
            pl.col('vol').alias('obv'),  # 暂时使用成交量作为 OBV 的简化版
        ]).drop([
            # 清理中间计算列
            'price_diff', 'hl', 'hc', 'lc', 'bb_std', 'gain', 'loss',
            'true_range', 'rs', 'k_raw'
        ])

        logger.info(f"成功计算 {result.height} 条记录的技术指标 (Polars 原生)")
        return result

    except Exception as e:
        logger.error(f"Polars 原生计算失败: {e}")
        # 如果 Polars 计算失败，转换为 Pandas 并使用传统方法
        df_pandas = df.to_pandas()
        result_pandas = add_all_indicators_pandas_legacy(df_pandas, ma_periods, ema_periods)
        return pl.from_pandas(result_pandas)


def add_all_indicators_pandas_legacy(df: pd.DataFrame,
                                    ma_periods: list = [5, 10, 20, 60],
                                    ema_periods: list = [12, 26]) -> pd.DataFrame:
    """
    原始 Pandas 版本的技术指标计算 (向后兼容)

    Args:
        df: 包含 OHLCV 数据的 DataFrame
        ma_periods: MA 周期列表
        ema_periods: EMA 周期列表

    Returns:
        添加了技术指标的 DataFrame
    """
    try:
        if df.empty:
            logger.warning("数据为空，无法计算技术指标")
            return df

        if len(df) < 30:
            logger.warning(f"数据不足({len(df)}条)，将计算部分技术指标")
            # 对于数据不足的情况，仍然尝试计算，但会有很多 NaN 值
        
        # 确保数据按日期排序
        df = df.sort_values('trade_date').copy()
        
        # 使用 Pandas 内置方法计算技术指标 (简化版)

        # 移动平均线
        for period in ma_periods:
            df[f'ma_{period}'] = df['close_price'].rolling(period, min_periods=1).mean()

        # 指数移动平均线
        for period in ema_periods:
            df[f'ema_{period}'] = df['close_price'].ewm(span=period).mean()

        # 简化的 RSI 计算
        delta = df['close_price'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14, min_periods=1).mean()
        rs = gain / loss.replace(0, np.inf)
        df['rsi'] = 100 - (100 / (1 + rs))

        # MACD
        if 'ema_12' in df.columns and 'ema_26' in df.columns:
            df['macd'] = df['ema_12'] - df['ema_26']
            df['macd_signal'] = df['macd'].ewm(span=9).mean()
            df['macd_hist'] = df['macd'] - df['macd_signal']

        # 布林带
        df['bb_middle'] = df['close_price'].rolling(20, min_periods=1).mean()
        bb_std = df['close_price'].rolling(20, min_periods=1).std()
        df['bb_upper'] = df['bb_middle'] + 2 * bb_std
        df['bb_lower'] = df['bb_middle'] - 2 * bb_std

        # ATR (简化版)
        high_low = df['high_price'] - df['low_price']
        high_close = np.abs(df['high_price'] - df['close_price'].shift())
        low_close = np.abs(df['low_price'] - df['close_price'].shift())
        true_range = np.maximum(high_low, np.maximum(high_close, low_close))
        df['atr'] = pd.Series(true_range).rolling(14, min_periods=1).mean()

        # 成交量指标
        df['vol_ma'] = df['vol'].rolling(20, min_periods=1).mean()
        df['vol_ratio'] = df['vol'] / df['vol_ma']

        # OBV (简化版)
        obv = []
        obv_value = 0
        prev_close = None
        for _, row in df.iterrows():
            if prev_close is not None:
                if row['close_price'] > prev_close:
                    obv_value += row['vol']
                elif row['close_price'] < prev_close:
                    obv_value -= row['vol']
            obv.append(obv_value)
            prev_close = row['close_price']
        df['obv'] = obv

        # 威廉指标 (简化版)
        high_14 = df['high_price'].rolling(14, min_periods=1).max()
        low_14 = df['low_price'].rolling(14, min_periods=1).min()
        df['williams_r'] = -100 * (high_14 - df['close_price']) / (high_14 - low_14)

        # KDJ (简化版)
        k_values = 100 * (df['close_price'] - low_14) / (high_14 - low_14)
        df['k'] = k_values.ewm(span=3).mean()
        df['d'] = df['k'].ewm(span=3).mean()
        df['j'] = 3 * df['k'] - 2 * df['d']
        
        logger.info(f"成功计算 {len(df)} 条记录的技术指标")
        return df
        
    except Exception as e:
        logger.error(f"计算技术指标失败: {e}")
        return df


if __name__ == "__main__":
    # 测试技术指标计算
    import pandas as pd
    
    # 创建测试数据
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    
    # 生成模拟价格数据
    base_price = 100
    prices = [base_price]
    for i in range(99):
        change = np.random.normal(0, 0.02)  # 2% 标准差
        new_price = prices[-1] * (1 + change)
        prices.append(new_price)
    
    test_df = pd.DataFrame({
        'trade_date': dates,
        'open_price': prices,
        'high_price': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices],
        'low_price': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices],
        'close_price': prices,
        'vol': np.random.randint(1000000, 10000000, 100)
    })
    
    # 计算技术指标
    result_df = add_all_indicators(test_df)
    
    print("技术指标计算完成！")
    print(f"数据行数: {len(result_df)}")
    print(f"列数: {len(result_df.columns)}")
    print("\n最后5行数据:")
    print(result_df[['trade_date', 'close_price', 'ma_20', 'rsi', 'macd']].tail())
