#!/usr/bin/env python3
"""
创建数据更新日志表
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.database import get_db_manager

logger = get_logger("create_update_log_table")

def create_update_log_table():
    """创建数据更新日志表"""
    try:
        db_manager = get_db_manager()
        
        # 创建数据更新日志表
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS data_update_log (
            id SERIAL PRIMARY KEY,
            update_date DATE NOT NULL,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) NOT NULL CHECK (status IN ('started', 'completed', 'failed')),
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- 创建索引
        CREATE INDEX IF NOT EXISTS idx_data_update_log_date ON data_update_log(update_date);
        CREATE INDEX IF NOT EXISTS idx_data_update_log_status ON data_update_log(status);
        CREATE INDEX IF NOT EXISTS idx_data_update_log_time ON data_update_log(update_time);
        """
        
        db_manager.execute_postgres_command(create_table_sql)
        logger.info("✅ 数据更新日志表创建成功")
        
        # 检查表是否存在并添加缺失的列
        try:
            # 检查stock_basic表是否有updated_at列
            check_column_sql = """
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'stock_basic' AND column_name = 'updated_at'
            """
            result = db_manager.execute_postgres_query(check_column_sql)
            
            if result.empty:
                # 添加updated_at列到stock_basic表
                alter_sql = """
                ALTER TABLE stock_basic 
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """
                db_manager.execute_postgres_command(alter_sql)
                logger.info("✅ 为stock_basic表添加updated_at列")
                
        except Exception as e:
            logger.warning(f"检查/添加stock_basic表列失败: {e}")
            
        try:
            # 检查stock_daily_quotes表是否有updated_at列
            check_column_sql = """
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'stock_daily_quotes' AND column_name = 'updated_at'
            """
            result = db_manager.execute_postgres_query(check_column_sql)
            
            if result.empty:
                # 添加updated_at列到stock_daily_quotes表
                alter_sql = """
                ALTER TABLE stock_daily_quotes 
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """
                db_manager.execute_postgres_command(alter_sql)
                logger.info("✅ 为stock_daily_quotes表添加updated_at列")
                
        except Exception as e:
            logger.warning(f"检查/添加stock_daily_quotes表列失败: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 创建数据更新日志表失败: {e}")
        return False

def main():
    """主函数"""
    logger.info("🚀 创建数据更新日志表")
    logger.info("=" * 50)
    
    success = create_update_log_table()
    
    if success:
        logger.info("✅ 数据更新日志表创建完成")
    else:
        logger.error("❌ 数据更新日志表创建失败")
        
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
