#!/usr/bin/env python3
"""
手动触发数据更新（用于测试和紧急更新）
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.services.data_scheduler import get_scheduler

logger = get_logger("manual_data_update")

def main():
    """主函数"""
    logger.info("🚀 手动触发数据更新")
    logger.info("=" * 50)
    
    try:
        # 获取调度器实例
        scheduler = get_scheduler()
        
        # 初始化数据源
        if not scheduler.data_source.initialize():
            logger.error("❌ 数据源初始化失败")
            return False
            
        logger.info("✅ 数据源初始化成功")
        
        # 执行增量更新
        logger.info("开始执行增量数据更新...")
        success = scheduler._perform_incremental_update()
        
        if success:
            scheduler._record_update_completion()
            logger.info("✅ 手动数据更新完成")
            
            # 显示更新信息
            last_update = scheduler.get_last_update_info()
            if last_update:
                logger.info(f"更新时间: {last_update['update_time']}")
                logger.info(f"更新状态: {last_update['status']}")
                logger.info(f"更新信息: {last_update['message']}")
        else:
            logger.error("❌ 手动数据更新失败")
            
        return success
        
    except Exception as e:
        logger.error(f"❌ 手动更新执行失败: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
