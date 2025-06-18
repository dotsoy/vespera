#!/usr/bin/env python3
"""
定时备份 ClickHouse 数据库
"""
import sys
from pathlib import Path
import schedule
import time
from loguru import logger

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from scripts.backup_clickhouse import backup_clickhouse

def job():
    """执行备份任务"""
    logger.info("开始执行定时备份...")
    if backup_clickhouse():
        logger.info("✅ 定时备份完成")
    else:
        logger.error("❌ 定时备份失败")

def main():
    """主函数"""
    # 设置每天凌晨 2 点执行备份
    schedule.every().day.at("02:00").do(job)
    
    logger.info("定时备份服务已启动")
    logger.info("将在每天凌晨 2:00 执行备份")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("定时备份服务已停止")
        sys.exit(0) 