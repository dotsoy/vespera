#!/usr/bin/env python3
"""
测试新数据源配置
验证AkShare和Tushare数据源的功能
"""
import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from src.data_sources.data_source_factory import get_data_service
from src.data_sources.base_data_source import DataRequest, DataType

def test_akshare_data_source():
    """测试AkShare数据源"""
    logger.info("🧪 测试AkShare数据源...")
    
    try:
        # 获取数据服务
        data_service = get_data_service()
        
        # 获取AkShare数据源
        akshare_source = data_service.get_source('akshare')
        if akshare_source is None:
            logger.error("❌ AkShare数据源未找到")
            return False
            
        logger.info("✅ AkShare数据源获取成功")
        
        # 测试初始化
        if akshare_source.initialize():
            logger.info("✅ AkShare数据源初始化成功")
        else:
            logger.error("❌ AkShare数据源初始化失败")
            return False
            
        # 测试获取股票基础信息
        request = DataRequest(data_type=DataType.STOCK_BASIC)
        response = akshare_source.fetch_data(request)
        
        if response.success and not response.data.empty:
            logger.info(f"✅ 成功获取股票基础信息，共{len(response.data)}条记录")
            logger.info(f"   数据列: {list(response.data.columns)}")
        else:
            logger.error("❌ 获取股票基础信息失败")
            return False
            
        # 测试获取日线数据
        request = DataRequest(
            data_type=DataType.STOCK_DAILY,
            symbol='000001.SZ',
            start_date='2024-01-01',
            end_date='2024-01-10'
        )
        response = akshare_source.fetch_data(request)
        
        if response.success and not response.data.empty:
            logger.info(f"✅ 成功获取日线数据，共{len(response.data)}条记录")
            logger.info(f"   数据列: {list(response.data.columns)}")
        else:
            logger.warning("⚠️ 获取日线数据失败或为空")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ AkShare数据源测试失败: {e}")
        return False

def test_tushare_data_source():
    """测试Tushare数据源"""
    logger.info("🧪 测试Tushare数据源...")
    
    try:
        # 获取数据服务
        data_service = get_data_service()
        
        # 获取Tushare数据源
        tushare_source = data_service.get_source('tushare')
        if tushare_source is None:
            logger.info("ℹ️ Tushare数据源未配置（需要token）")
            return True  # 不是错误，只是未配置
            
        logger.info("✅ Tushare数据源获取成功")
        
        # 测试初始化
        if tushare_source.initialize():
            logger.info("✅ Tushare数据源初始化成功")
        else:
            logger.error("❌ Tushare数据源初始化失败")
            return False
            
        # 测试获取股票基础信息
        request = DataRequest(data_type=DataType.STOCK_BASIC)
        response = tushare_source.fetch_data(request)
        
        if response.success and not response.data.empty:
            logger.info(f"✅ 成功获取股票基础信息，共{len(response.data)}条记录")
        else:
            logger.error("❌ 获取股票基础信息失败")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Tushare数据源测试失败: {e}")
        return False

def test_data_source_factory():
    """测试数据源工厂"""
    logger.info("🧪 测试数据源工厂...")
    
    try:
        # 获取数据服务
        data_service = get_data_service()
        
        # 获取所有可用数据源
        available_sources = data_service.get_available_sources()
        logger.info(f"✅ 可用数据源: {list(available_sources.keys())}")
        
        # 测试数据获取
        request = DataRequest(data_type=DataType.STOCK_BASIC)
        data = data_service.get_data(request)
        
        if not data.empty:
            logger.info(f"✅ 通过工厂成功获取数据，共{len(data)}条记录")
        else:
            logger.warning("⚠️ 通过工厂获取数据为空")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据源工厂测试失败: {e}")
        return False

def main():
    """主函数"""
    logger.info("🚀 开始测试新数据源配置...")
    
    # 测试AkShare数据源
    akshare_success = test_akshare_data_source()
    
    # 测试Tushare数据源
    tushare_success = test_tushare_data_source()
    
    # 测试数据源工厂
    factory_success = test_data_source_factory()
    
    # 总结
    logger.info("\n📊 测试结果总结:")
    logger.info(f"   AkShare数据源: {'✅ 通过' if akshare_success else '❌ 失败'}")
    logger.info(f"   Tushare数据源: {'✅ 通过' if tushare_success else '❌ 失败'}")
    logger.info(f"   数据源工厂: {'✅ 通过' if factory_success else '❌ 失败'}")
    
    if akshare_success and factory_success:
        logger.info("🎉 新数据源配置测试完成！AkShare数据源工作正常。")
        return 0
    else:
        logger.error("💥 新数据源配置测试失败！")
        return 1

if __name__ == "__main__":
    exit(main()) 