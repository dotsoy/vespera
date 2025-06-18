#!/usr/bin/env python3
"""
测试数据兼容层功能
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime
from src.data_sources.data_compatibility_layer import DataCompatibilityLayer
from src.data_sources.enums import DataType

def test_data_transformation():
    """测试数据转换功能"""
    print("=== 测试数据转换功能 ===")
    
    # 创建测试数据（模拟 AllTick 数据源的数据格式）
    test_data = {
        'trade_date': ['2025-06-17', '2025-06-16', '2025-06-15'],
        'symbol': ['688775.SH', '688775.SH', '688775.SH'],
        'open': [10.5, 10.2, 10.0],
        'close': [10.8, 10.5, 10.2],
        'high': [11.0, 10.8, 10.5],
        'low': [10.3, 10.0, 9.8],
        'vol': [1000000, 950000, 900000],
        'amount': [10800000.0, 9975000.0, 9180000.0],
        'amplitude': [6.67, 7.84, 7.00],
        'pct_chg': [2.86, 5.00, 2.00],
        'change': [0.30, 0.50, 0.20],
        'turnover_rate': [2.5, 2.4, 2.3],
        'ts_code': ['688775.SH', '688775.SH', '688775.SH'],
        'pre_close': [10.5, 10.0, 9.8]
    }
    
    df = pd.DataFrame(test_data)
    print(f"原始数据形状: {df.shape}")
    print(f"原始数据列: {list(df.columns)}")
    print(f"原始数据前3行:\n{df.head(3)}")
    
    # 使用数据兼容层转换数据
    transformed_df = DataCompatibilityLayer.transform_data(df, DataType.STOCK_DAILY, 'alltick')
    
    print(f"\n转换后数据形状: {transformed_df.shape}")
    print(f"转换后数据列: {list(transformed_df.columns)}")
    print(f"转换后数据前3行:\n{transformed_df.head(3)}")
    
    # 验证数据
    is_valid = DataCompatibilityLayer.validate_data(transformed_df, DataType.STOCK_DAILY)
    print(f"\n数据验证结果: {'通过' if is_valid else '失败'}")
    
    return transformed_df

def test_table_schema():
    """测试表结构获取"""
    print("\n=== 测试表结构获取 ===")
    
    # 获取 daily_quotes 表结构
    schema = DataCompatibilityLayer.get_table_schema('daily_quotes')
    print(f"daily_quotes 表结构: {schema}")
    
    # 获取 stock_basic 表结构
    schema = DataCompatibilityLayer.get_table_schema('stock_basic')
    print(f"stock_basic 表结构: {schema}")

def test_field_mapping():
    """测试字段映射获取"""
    print("\n=== 测试字段映射获取 ===")
    
    # 获取 AllTick 数据源的字段映射
    mapping = DataCompatibilityLayer.get_field_mapping(DataType.STOCK_DAILY, 'alltick')
    print(f"AllTick STOCK_DAILY 字段映射: {mapping}")
    
    # 获取支持的数据源
    sources = DataCompatibilityLayer.get_supported_data_sources(DataType.STOCK_DAILY)
    print(f"支持的数据源: {sources}")

def test_table_name():
    """测试表名获取"""
    print("\n=== 测试表名获取 ===")
    
    table_name = DataCompatibilityLayer.get_table_name(DataType.STOCK_DAILY)
    print(f"STOCK_DAILY 对应的表名: {table_name}")
    
    table_name = DataCompatibilityLayer.get_table_name(DataType.STOCK_BASIC)
    print(f"STOCK_BASIC 对应的表名: {table_name}")

if __name__ == "__main__":
    print("开始测试数据兼容层...")
    
    try:
        # 测试数据转换
        transformed_df = test_data_transformation()
        
        # 测试表结构
        test_table_schema()
        
        # 测试字段映射
        test_field_mapping()
        
        # 测试表名获取
        test_table_name()
        
        print("\n✅ 所有测试通过！")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc() 