#!/usr/bin/env python3
"""
启动数据更新调度器
"""
import sys
import signal
import time
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.services.data_scheduler import get_scheduler

logger = get_logger("start_data_scheduler")

def signal_handler(signum, frame):
    """信号处理器"""
    logger.info("接收到停止信号，正在关闭调度器...")
    scheduler = get_scheduler()
    scheduler.stop()
    sys.exit(0)

def main():
    """主函数"""
    logger.info("🚀 启动数据更新调度器")
    logger.info("=" * 50)
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 获取并启动调度器
        scheduler = get_scheduler()
        scheduler.start()
        
        logger.info("调度器已启动，按 Ctrl+C 停止")
        logger.info("每日9:00自动执行数据更新")
        
        # 显示最后更新信息
        last_update = scheduler.get_last_update_info()
        if last_update:
            logger.info(f"最后更新: {last_update['update_date']} {last_update['update_time']} - {last_update['status']}")
        else:
            logger.info("暂无更新记录")
        
        # 保持运行
        while True:
            time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("接收到中断信号，正在停止调度器...")
        scheduler = get_scheduler()
        scheduler.stop()
    except Exception as e:
        logger.error(f"调度器运行失败: {e}")
        return False
        
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
