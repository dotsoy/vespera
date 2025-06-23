#!/usr/bin/env python3
"""
直接测试AData数据源
模拟实际使用场景，获取具体股票数据
"""
import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from src.data_sources.adata_data_source import AdataDataSource
from src.data_sources.base_data_source import DataRequest, DataType

def test_adata_direct():
    """直接测试AData数据源"""
    logger.info("🧪 直接测试AData数据源...")
    
    try:
        # 创建AData数据源实例
        adata_source = AdataDataSource()
        
        # 测试初始化
        if adata_source.initialize():
            logger.success("✅ AData数据源初始化成功")
        else:
            logger.error("❌ AData数据源初始化失败")
            return False
            
        # 测试获取股票基础信息
        logger.info("📊 测试获取股票基础信息...")
        request = DataRequest(data_type=DataType.STOCK_BASIC)
        response = adata_source.fetch_data(request)
        
        if response.success and not response.data.empty:
            logger.success(f"✅ 成功获取股票基础信息，共{len(response.data)}条记录")
            logger.info(f"   数据列: {list(response.data.columns)}")
            logger.info(f"   前5条记录:\n{response.data.head()}")
        else:
            logger.error("❌ 获取股票基础信息失败")
            return False
            
        # 测试获取具体股票的日线数据
        test_stocks = ['000001', '300783', '688775']  # 包含你提到的股票
        
        for stock_code in test_stocks:
            logger.info(f"📈 测试获取股票 {stock_code} 的日线数据...")
            request = DataRequest(
                data_type=DataType.STOCK_DAILY,
                symbol=stock_code,
                start_date='2024-01-01',
                end_date='2024-01-10'
            )
            response = adata_source.fetch_data(request)
            
            if response.success and not response.data.empty:
                logger.success(f"✅ 成功获取股票 {stock_code} 日线数据，共{len(response.data)}条记录")
                logger.info(f"   数据列: {list(response.data.columns)}")
                logger.info(f"   前3条记录:\n{response.data.head(3)}")
            else:
                logger.warning(f"⚠️ 获取股票 {stock_code} 日线数据失败或为空")
                logger.info(f"   错误信息: {response.error_message}")
                
        return True
        
    except Exception as e:
        logger.error(f"❌ AData直接测试失败: {e}")
        return False

def main():
    """主函数"""
    logger.info("🚀 开始直接测试AData数据源...")
    
    success = test_adata_direct()
    
    if success:
        logger.info("🎉 AData直接测试完成！")
        return 0
    else:
        logger.error("💥 AData直接测试失败！")
        return 1

if __name__ == "__main__":
    exit(main()) 