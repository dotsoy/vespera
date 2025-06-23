#!/usr/bin/env python3
"""
测试AData数据源
验证AData数据源的功能和可用性
"""
import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
import pandas as pd
import adata
from datetime import datetime, date

def test_adata_basic_functionality():
    """测试AData基本功能"""
    logger.info("🧪 测试AData基本功能...")
    
    try:
        # 测试获取股票列表
        logger.info("📊 测试获取股票列表...")
        stock_list = adata.stock.info.all_code()
        
        if stock_list is not None and not stock_list.empty:
            logger.success(f"✅ 成功获取股票列表，共{len(stock_list)}只股票")
            logger.info(f"   数据列: {list(stock_list.columns)}")
            logger.info(f"   前5只股票: {stock_list.head()}")
        else:
            logger.error("❌ 获取股票列表失败")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"❌ AData基本功能测试失败: {e}")
        return False

def test_adata_stock_data():
    """测试AData股票数据获取"""
    logger.info("🧪 测试AData股票数据获取...")
    
    try:
        # 测试获取平安银行的日线数据
        symbol = "000001"
        start_date = "20240101"
        end_date = "20240110"
        
        logger.info(f"📈 测试获取股票 {symbol} 的日线数据...")
        stock_data = adata.stock.market.get_market(
            stock_code=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        if stock_data is not None and not stock_data.empty:
            logger.success(f"✅ 成功获取股票数据，共{len(stock_data)}条记录")
            logger.info(f"   数据列: {list(stock_data.columns)}")
            logger.info(f"   数据示例:\n{stock_data.head()}")
        else:
            logger.warning("⚠️ 获取股票数据为空")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ AData股票数据测试失败: {e}")
        return False

def test_adata_index_data():
    """测试AData指数数据获取"""
    logger.info("🧪 测试AData指数数据获取...")
    
    try:
        # 测试获取上证指数数据
        symbol = "000001"  # 上证指数
        start_date = "20240101"
        end_date = "20240110"
        
        logger.info(f"📊 测试获取指数 {symbol} 的数据...")
        index_data = adata.stock.market.get_market_index(
            index_code=symbol,
            start_date=start_date
        )
        # 手动筛选end_date
        if index_data is not None and not index_data.empty and end_date:
            index_data = index_data[index_data['trade_date'] <= end_date]
        
        if index_data is not None and not index_data.empty:
            logger.success(f"✅ 成功获取指数数据，共{len(index_data)}条记录")
            logger.info(f"   数据列: {list(index_data.columns)}")
            logger.info(f"   数据示例:\n{index_data.head()}")
        else:
            logger.warning("⚠️ 获取指数数据为空")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ AData指数数据测试失败: {e}")
        return False

def test_adata_data_source_integration():
    """测试AData数据源集成"""
    logger.info("🧪 测试AData数据源集成...")
    
    try:
        from src.data_sources.adata_data_source import AdataDataSource
        from src.data_sources.base_data_source import DataRequest, DataType
        
        # 创建AData数据源实例
        adata_source = AdataDataSource()
        
        # 测试初始化
        if adata_source.initialize():
            logger.success("✅ AData数据源初始化成功")
        else:
            logger.error("❌ AData数据源初始化失败")
            return False
            
        # 测试获取股票基础信息
        request = DataRequest(data_type=DataType.STOCK_BASIC)
        response = adata_source.fetch_data(request)
        
        if response.success and not response.data.empty:
            logger.success(f"✅ 成功获取股票基础信息，共{len(response.data)}条记录")
            logger.info(f"   数据列: {list(response.data.columns)}")
        else:
            logger.error("❌ 获取股票基础信息失败")
            return False
            
        # 测试获取日线数据
        request = DataRequest(
            data_type=DataType.STOCK_DAILY,
            symbol='000001',
            start_date='2024-01-01',
            end_date='2024-01-10'
        )
        response = adata_source.fetch_data(request)
        
        if response.success and not response.data.empty:
            logger.success(f"✅ 成功获取日线数据，共{len(response.data)}条记录")
            logger.info(f"   数据列: {list(response.data.columns)}")
        else:
            logger.warning("⚠️ 获取日线数据失败或为空")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ AData数据源集成测试失败: {e}")
        return False

def test_adata_data_source_factory():
    """测试AData在数据源工厂中的集成"""
    logger.info("🧪 测试AData在数据源工厂中的集成...")
    
    try:
        from src.data_sources.data_source_factory import get_data_service
        from src.data_sources.base_data_source import DataRequest, DataType
        
        # 获取数据服务
        data_service = get_data_service()
        
        # 获取AData数据源
        adata_source = data_service.get_source('adata')
        if adata_source is None:
            logger.error("❌ AData数据源未找到")
            return False
            
        logger.success("✅ AData数据源获取成功")
        
        # 获取所有可用数据源
        available_sources = data_service.get_available_sources()
        logger.info(f"✅ 可用数据源: {list(available_sources.keys())}")
        
        # 测试通过工厂获取数据
        request = DataRequest(data_type=DataType.STOCK_BASIC)
        data = data_service.get_data(request)
        
        if not data.empty:
            logger.success(f"✅ 通过工厂成功获取数据，共{len(data)}条记录")
        else:
            logger.warning("⚠️ 通过工厂获取数据为空")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ AData数据源工厂测试失败: {e}")
        return False

def main():
    """主函数"""
    logger.info("🚀 开始测试AData数据源...")
    
    # 测试AData基本功能
    basic_success = test_adata_basic_functionality()
    
    # 测试AData股票数据
    stock_success = test_adata_stock_data()
    
    # 测试AData指数数据
    index_success = test_adata_index_data()
    
    # 测试AData数据源集成
    integration_success = test_adata_data_source_integration()
    
    # 测试AData数据源工厂
    factory_success = test_adata_data_source_factory()
    
    # 总结
    logger.info("\n📊 测试结果总结:")
    logger.info(f"   AData基本功能: {'✅ 通过' if basic_success else '❌ 失败'}")
    logger.info(f"   AData股票数据: {'✅ 通过' if stock_success else '❌ 失败'}")
    logger.info(f"   AData指数数据: {'✅ 通过' if index_success else '❌ 失败'}")
    logger.info(f"   AData数据源集成: {'✅ 通过' if integration_success else '❌ 失败'}")
    logger.info(f"   AData数据源工厂: {'✅ 通过' if factory_success else '❌ 失败'}")
    
    if basic_success and integration_success:
        logger.info("🎉 AData数据源测试完成！AData数据源工作正常。")
        return 0
    else:
        logger.error("💥 AData数据源测试失败！")
        return 1

if __name__ == "__main__":
    exit(main()) 