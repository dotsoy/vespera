#!/usr/bin/env python3
"""
测试AData数据源优先级和数据获取功能
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_sources.data_source_factory import get_data_service
from src.data_sources.base_data_source import DataRequest, DataType
from datetime import datetime, timedelta
from loguru import logger

def test_adata_priority():
    """测试AData数据源优先级"""
    logger.info("开始测试AData数据源优先级...")
    
    # 获取数据服务
    data_service = get_data_service()
    
    # 检查可用的数据源
    available_sources = data_service.get_available_sources()
    logger.info(f"可用的数据源: {list(available_sources.keys())}")
    
    # 测试股票代码
    test_symbols = ['301558', '000001', '600000']
    
    # 计算日期范围
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    for symbol in test_symbols:
        logger.info(f"\n=== 测试股票代码: {symbol} ===")
        
        # 创建数据请求
        request = DataRequest(
            data_type=DataType.STOCK_DAILY,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        # 获取数据
        df = data_service.get_data(request)
        
        if not df.empty:
            logger.success(f"成功获取 {symbol} 的数据，记录数: {len(df)}")
            logger.info(f"数据列: {list(df.columns)}")
            logger.info(f"前3行数据:\n{df.head(3)}")
        else:
            logger.error(f"无法获取 {symbol} 的数据")

if __name__ == "__main__":
    test_adata_priority() 