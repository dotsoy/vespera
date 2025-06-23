#!/usr/bin/env python3
"""
立即清理过期备份，只保留最新的备份文件
"""
import sys
import os
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.backup_manager import get_backup_manager
from loguru import logger

def cleanup_old_backups():
    """清理过期备份"""
    logger.info("开始清理过期备份文件...")
    
    backup_manager = get_backup_manager()
    
    # 手动调用清理方法
    backup_manager._cleanup_old_backups()
    
    # 显示清理后的状态
    backup_status = backup_manager.get_backup_status()
    
    logger.info("清理完成！")
    logger.info(f"ClickHouse备份数量: {len(backup_status.get('clickhouse_backups', []))}")
    logger.info(f"PostgreSQL备份数量: {len(backup_status.get('postgres_backups', []))}")
    logger.info(f"总备份大小: {backup_status.get('total_size', 0) / (1024*1024):.2f} MB")
    
    if backup_status.get('newest_backup'):
        logger.info(f"最新备份时间: {backup_status['newest_backup']}")

if __name__ == "__main__":
    cleanup_old_backups() 