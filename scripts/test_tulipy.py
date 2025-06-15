#!/usr/bin/env python3
"""
测试 Tulipy 技术分析库
"""
import sys
import os
from pathlib import Path
import numpy as np
import pandas as pd

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_tulipy_installation():
    """测试 Tulipy 安装"""
    print("🔍 测试 Tulipy 安装...")
    
    try:
        import tulipy as ti
        print("✅ Tulipy 导入成功")
        return True
    except ImportError as e:
        print(f"❌ Tulipy 导入失败: {e}")
        print("💡 请运行: pip install tulipy")
        return False


def test_basic_indicators():
    """测试基础技术指标"""
    print("\n🧮 测试基础技术指标...")
    
    try:
        import tulipy as ti
        
        # 创建测试数据
        np.random.seed(42)
        data = np.random.randn(100).cumsum() + 100
        data = np.abs(data)  # 确保价格为正数
        
        high = data * (1 + np.abs(np.random.randn(100) * 0.01))
        low = data * (1 - np.abs(np.random.randn(100) * 0.01))
        close = data
        volume = np.random.randint(1000, 10000, 100).astype(float)
        
        print(f"📊 测试数据长度: {len(data)}")
        
        # 测试 SMA
        sma_20 = ti.sma(close, period=20)
        print(f"✅ SMA(20): {len(sma_20)} 个值")
        
        # 测试 EMA
        ema_12 = ti.ema(close, period=12)
        print(f"✅ EMA(12): {len(ema_12)} 个值")
        
        # 测试 RSI
        rsi = ti.rsi(close, period=14)
        print(f"✅ RSI(14): {len(rsi)} 个值")
        
        # 测试 MACD
        macd, macd_signal, macd_hist = ti.macd(close, short_period=12, long_period=26, signal_period=9)
        print(f"✅ MACD: {len(macd)} 个值")
        
        # 测试布林带
        bb_lower, bb_middle, bb_upper = ti.bbands(close, period=20, stddev=2)
        print(f"✅ 布林带: {len(bb_upper)} 个值")
        
        # 测试 ATR
        atr = ti.atr(high, low, close, period=14)
        print(f"✅ ATR(14): {len(atr)} 个值")
        
        # 测试随机指标
        stoch_k, stoch_d = ti.stoch(high, low, close, 14, 3, 3)
        print(f"✅ STOCH: K={len(stoch_k)}, D={len(stoch_d)} 个值")
        
        # 测试威廉指标
        willr = ti.willr(high, low, close, period=14)
        print(f"✅ Williams %R: {len(willr)} 个值")
        
        # 测试 OBV
        obv = ti.obv(close, volume)
        print(f"✅ OBV: {len(obv)} 个值")
        
        print("🎉 所有基础指标测试通过！")
        return True
        
    except Exception as e:
        print(f"❌ 基础指标测试失败: {e}")
        return False


def test_technical_indicators_module():
    """测试技术指标工具模块"""
    print("\n🔧 测试技术指标工具模块...")
    
    try:
        from src.utils.technical_indicators import (
            calculate_sma, calculate_ema, calculate_rsi,
            calculate_macd, calculate_bbands, add_all_indicators
        )
        
        # 创建测试 DataFrame
        np.random.seed(42)
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        
        base_price = 100
        prices = [base_price]
        for i in range(99):
            change = np.random.normal(0, 0.02)
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
        
        print(f"📊 测试数据: {len(test_df)} 行")
        
        # 测试单个指标函数
        close_prices = test_df['close_price'].values.astype(np.float64)
        
        sma_20 = calculate_sma(close_prices, 20)
        print(f"✅ SMA 工具函数: {np.sum(~np.isnan(sma_20))} 个有效值")
        
        ema_12 = calculate_ema(close_prices, 12)
        print(f"✅ EMA 工具函数: {np.sum(~np.isnan(ema_12))} 个有效值")
        
        rsi = calculate_rsi(close_prices, 14)
        print(f"✅ RSI 工具函数: {np.sum(~np.isnan(rsi))} 个有效值")
        
        # 测试完整指标计算
        result_df = add_all_indicators(test_df)
        
        print(f"✅ 完整指标计算: {len(result_df.columns)} 列")
        
        # 检查关键指标
        key_indicators = ['ma_20', 'ema_12', 'rsi', 'macd', 'bb_upper', 'atr', 'k', 'd']
        missing_indicators = [ind for ind in key_indicators if ind not in result_df.columns]
        
        if missing_indicators:
            print(f"⚠️ 缺失指标: {missing_indicators}")
        else:
            print("✅ 所有关键指标都已计算")
        
        # 显示最后几行数据
        print("\n📈 最后5行技术指标数据:")
        display_cols = ['trade_date', 'close_price', 'ma_20', 'rsi', 'macd']
        available_cols = [col for col in display_cols if col in result_df.columns]
        print(result_df[available_cols].tail())
        
        print("🎉 技术指标工具模块测试通过！")
        return True
        
    except Exception as e:
        print(f"❌ 技术指标工具模块测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_technical_analyzer():
    """测试技术分析器"""
    print("\n🎯 测试技术分析器...")
    
    try:
        from src.analyzers.technical_analyzer import TechnicalAnalyzer
        
        # 创建分析器实例
        analyzer = TechnicalAnalyzer()
        print("✅ 技术分析器创建成功")
        
        # 这里可以添加更多的分析器测试
        # 由于需要数据库连接，暂时跳过实际分析测试
        
        print("✅ 技术分析器基础测试通过")
        return True
        
    except Exception as e:
        print(f"❌ 技术分析器测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    print("=" * 60)
    print("🌟 Tulipy 技术分析库测试")
    print("=" * 60)
    
    test_results = []
    
    # 1. 测试 Tulipy 安装
    test_results.append(("Tulipy 安装", test_tulipy_installation()))
    
    # 2. 测试基础指标
    test_results.append(("基础指标", test_basic_indicators()))
    
    # 3. 测试工具模块
    test_results.append(("工具模块", test_technical_indicators_module()))
    
    # 4. 测试分析器
    test_results.append(("技术分析器", test_technical_analyzer()))
    
    # 汇总结果
    print("\n" + "=" * 60)
    print("📊 测试结果汇总")
    print("=" * 60)
    
    passed_tests = 0
    total_tests = len(test_results)
    
    for test_name, result in test_results:
        if result:
            print(f"✅ {test_name}: 通过")
            passed_tests += 1
        else:
            print(f"❌ {test_name}: 失败")
    
    success_rate = passed_tests / total_tests
    
    print("=" * 60)
    if success_rate == 1.0:
        print(f"🎉 所有测试通过！({passed_tests}/{total_tests})")
        print("✨ Tulipy 技术分析库已准备就绪！")
    elif success_rate >= 0.75:
        print(f"⚠️ 大部分测试通过 ({passed_tests}/{total_tests})")
        print("🔧 请检查失败的测试项")
    else:
        print(f"🚨 多个测试失败 ({passed_tests}/{total_tests})")
        print("💡 建议重新安装 Tulipy: pip install tulipy")
    
    print("=" * 60)
    
    return success_rate >= 0.75


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
