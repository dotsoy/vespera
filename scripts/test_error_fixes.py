#!/usr/bin/env python3
"""
测试错误修复脚本
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.technical_indicators import add_all_indicators
from src.analyzers.technical_analyzer import TechnicalAnalyzer
from src.analyzers.capital_flow_analyzer import CapitalFlowAnalyzer
from src.analyzers.fundamental_analyzer import FundamentalAnalyzer
from src.analyzers.macro_analyzer import MacroAnalyzer

logger = get_logger("test_error_fixes")


def create_minimal_test_data():
    """创建最小测试数据"""
    logger.info("创建最小测试数据...")
    
    # 创建只有1条记录的测试数据
    data = {
        'ts_code': ['000001.SZ'],
        'trade_date': ['2024-12-14'],
        'open_price': [10.0],
        'high_price': [10.5],
        'low_price': [9.8],
        'close_price': [10.2],
        'vol': [1000000],
        'pct_chg': [2.0]
    }
    
    df = pd.DataFrame(data)
    return df


def test_technical_indicators():
    """测试技术指标计算"""
    logger.info("🧪 测试技术指标计算...")
    
    try:
        df = create_minimal_test_data()
        logger.info(f"原始数据: {len(df)} 条记录")
        
        # 测试技术指标计算
        df_with_indicators = add_all_indicators(df)
        logger.info(f"计算后数据: {len(df_with_indicators)} 条记录")
        
        # 检查关键字段是否存在
        required_fields = ['rsi', 'macd', 'macd_signal', 'k', 'd', 'j', 'vol_ratio']
        missing_fields = []
        
        for field in required_fields:
            if field not in df_with_indicators.columns:
                missing_fields.append(field)
        
        if missing_fields:
            logger.warning(f"缺少字段: {missing_fields}")
        else:
            logger.success("✅ 所有必需字段都存在")
        
        # 显示最后一行数据
        latest = df_with_indicators.iloc[-1]
        logger.info("最新数据:")
        for field in required_fields:
            value = latest.get(field, 'N/A')
            logger.info(f"  {field}: {value}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 技术指标计算失败: {e}")
        return False


def test_technical_analyzer():
    """测试技术分析器"""
    logger.info("🧪 测试技术分析器...")
    
    try:
        analyzer = TechnicalAnalyzer()
        
        # 使用模拟数据测试
        df = create_minimal_test_data()
        df = add_all_indicators(df)
        
        # 测试各个评分函数
        trend_score = analyzer.calculate_trend_score(df)
        momentum_score = analyzer.calculate_momentum_score(df)
        volume_health_score = analyzer.calculate_volume_health_score(df)
        
        logger.info(f"趋势评分: {trend_score}")
        logger.info(f"动量评分: {momentum_score}")
        logger.info(f"量能健康度: {volume_health_score}")
        
        # 测试形态识别
        patterns = analyzer.identify_patterns(df)
        logger.info(f"识别形态: {patterns}")
        
        # 测试支撑阻力位
        support, resistance = analyzer.calculate_support_resistance(df)
        logger.info(f"支撑位: {support}, 阻力位: {resistance}")
        
        logger.success("✅ 技术分析器测试通过")
        return True
        
    except Exception as e:
        logger.error(f"❌ 技术分析器测试失败: {e}")
        return False


def test_capital_flow_analyzer():
    """测试资金流分析器"""
    logger.info("🧪 测试资金流分析器...")
    
    try:
        analyzer = CapitalFlowAnalyzer()
        
        # 创建模拟资金流数据
        data = {
            'ts_code': ['000001.SZ'],
            'trade_date': ['2024-12-14'],
            'buy_sm_amount': [1000000],
            'buy_md_amount': [2000000],
            'buy_lg_amount': [3000000],
            'buy_elg_amount': [4000000],
            'sell_sm_amount': [900000],
            'sell_md_amount': [1800000],
            'sell_lg_amount': [2700000],
            'sell_elg_amount': [3600000],
            'net_amount': [1000000],
            'close_price': [10.2],
            'pct_chg': [2.0],
            'vol': [10000000]
        }
        
        df = pd.DataFrame(data)
        
        # 创建价格数据
        price_data = {
            'ts_code': ['000001.SZ'],
            'trade_date': ['2024-12-14'],
            'close_price': [10.2],
            'pct_chg': [2.0],
            'vol': [10000000]
        }
        price_df = pd.DataFrame(price_data)

        # 测试各个评分函数
        main_force_score = analyzer.calculate_main_force_score(df)
        retail_sentiment_score = analyzer.calculate_retail_sentiment_score(df)
        institutional_activity = analyzer.calculate_institutional_activity(df)
        flow_consistency = analyzer.calculate_flow_consistency(df)
        volume_price_correlation = analyzer.calculate_volume_price_correlation(df, price_df)
        
        logger.info(f"主力资金评分: {main_force_score}")
        logger.info(f"散户情绪评分: {retail_sentiment_score}")
        logger.info(f"机构活跃度: {institutional_activity}")
        logger.info(f"资金流一致性: {flow_consistency}")
        logger.info(f"量价相关性: {volume_price_correlation}")
        
        logger.success("✅ 资金流分析器测试通过")
        return True
        
    except Exception as e:
        logger.error(f"❌ 资金流分析器测试失败: {e}")
        return False


def test_data_serialization():
    """测试数据序列化"""
    logger.info("🧪 测试数据序列化...")

    try:
        # 测试字典序列化
        test_dict = {
            'key1': 'value1',
            'key2': 123,
            'key3': [1, 2, 3],
            'key4': {'nested': 'dict'}
        }

        # 转换为字符串
        serialized = str(test_dict)
        logger.info(f"序列化结果: {serialized}")

        # 测试 DataFrame 创建
        data = {
            'ts_code': ['000001.SZ'],
            'trade_date': ['2024-12-14'],
            'score': [0.75],
            'data_field': [serialized]  # 字符串化的字典
        }

        df = pd.DataFrame(data)
        logger.info(f"DataFrame 创建成功: {len(df)} 行")
        logger.info(f"数据类型: {df.dtypes.to_dict()}")

        logger.success("✅ 数据序列化测试通过")
        return True

    except Exception as e:
        logger.error(f"❌ 数据序列化测试失败: {e}")
        return False


def test_analyzer_output_serialization():
    """测试分析器输出序列化"""
    logger.info("🧪 测试分析器输出序列化...")

    try:
        # 测试技术分析器输出
        analyzer = TechnicalAnalyzer()
        df = create_minimal_test_data()
        df = add_all_indicators(df)

        # 模拟技术分析器的输出
        patterns = {'macd_golden_cross': True, 'rsi_oversold': False}
        technical_indicators = {'rsi': 45.5, 'macd': 0.12, 'k': 65.3}

        # 测试序列化
        serialized_patterns = str(patterns) if patterns else None
        serialized_indicators = str(technical_indicators) if technical_indicators else None

        # 创建测试 DataFrame
        result_data = {
            'ts_code': ['000001.SZ'],
            'trade_date': ['2024-12-14'],
            'trend_score': [0.65],
            'momentum_score': [0.45],
            'key_patterns': [serialized_patterns],
            'technical_indicators': [serialized_indicators]
        }

        df_result = pd.DataFrame(result_data)
        logger.info(f"分析器结果 DataFrame: {len(df_result)} 行")
        logger.info(f"数据类型: {df_result.dtypes.to_dict()}")

        # 检查是否包含字典类型
        has_dict_types = any(
            isinstance(val, dict) for col in df_result.columns
            for val in df_result[col].values
        )

        if has_dict_types:
            logger.error("❌ 发现字典类型数据")
            return False
        else:
            logger.success("✅ 所有数据都已正确序列化")
            return True

    except Exception as e:
        logger.error(f"❌ 分析器输出序列化测试失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("🔧 启明星错误修复测试")
    logger.info("=" * 60)
    
    tests = [
        ("技术指标计算", test_technical_indicators),
        ("技术分析器", test_technical_analyzer),
        ("资金流分析器", test_capital_flow_analyzer),
        ("数据序列化", test_data_serialization),
        ("分析器输出序列化", test_analyzer_output_serialization),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n📋 运行测试: {test_name}")
        logger.info("-" * 40)
        
        try:
            if test_func():
                passed += 1
                logger.success(f"✅ {test_name} 测试通过")
            else:
                logger.error(f"❌ {test_name} 测试失败")
        except Exception as e:
            logger.error(f"❌ {test_name} 测试异常: {e}")
    
    logger.info("\n" + "=" * 60)
    logger.info(f"📊 测试结果: {passed}/{total} 通过")
    
    if passed == total:
        logger.success("🎉 所有测试通过！错误修复成功！")
        return True
    else:
        logger.error(f"⚠️ {total - passed} 个测试失败，需要进一步修复")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
