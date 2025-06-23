#!/usr/bin/env python3
"""
测试备份管理器与数据拉取的集成功能
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.backup_manager import get_backup_manager
from src.data_sources.data_source_factory import get_data_service
from src.data_sources.base_data_source import DataRequest, DataType
from datetime import datetime, timedelta
from loguru import logger

def test_backup_integration():
    """测试备份集成功能"""
    logger.info("开始测试备份集成功能...")
    
    # 1. 测试备份管理器
    backup_manager = get_backup_manager()
    logger.info("备份管理器初始化成功")
    
    # 2. 测试数据拉取
    data_service = get_data_service()
    logger.info("数据服务初始化成功")
    
    # 3. 测试股票数据拉取
    test_symbols = ['000001', '600000']
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    for symbol in test_symbols:
        logger.info(f"\n=== 测试股票: {symbol} ===")
        
        # 拉取数据
        request = DataRequest(
            data_type=DataType.STOCK_DAILY,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        df = data_service.get_data(request)
        
        if not df.empty:
            logger.success(f"成功获取 {symbol} 数据，记录数: {len(df)}")
            
            # 触发备份
            logger.info(f"开始为 {symbol} 执行备份...")
            backup_success = backup_manager.backup_after_data_update("test")
            
            if backup_success:
                logger.success(f"✅ {symbol} 备份完成")
            else:
                logger.error(f"❌ {symbol} 备份失败")
        else:
            logger.error(f"无法获取 {symbol} 的数据")
    
    # 4. 测试备份状态查询
    logger.info("\n=== 查询备份状态 ===")
    backup_status = backup_manager.get_backup_status()
    
    if backup_status:
        logger.info(f"ClickHouse备份数量: {len(backup_status.get('clickhouse_backups', []))}")
        logger.info(f"PostgreSQL备份数量: {len(backup_status.get('postgres_backups', []))}")
        logger.info(f"总备份大小: {backup_status.get('total_size', 0) / (1024*1024):.2f} MB")
        
        if backup_status.get('newest_backup'):
            logger.info(f"最新备份时间: {backup_status['newest_backup']}")
    else:
        logger.warning("无法获取备份状态")

if __name__ == "__main__":
    test_backup_integration() 