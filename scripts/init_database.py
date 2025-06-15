#!/usr/bin/env python3
"""
数据库初始化脚本
"""
import sys
import os
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.database import get_db_manager
from src.utils.logger import get_logger
from config.settings import app_settings

logger = get_logger("init_database")


def test_database_connections():
    """测试数据库连接"""
    logger.info("开始测试数据库连接...")
    
    db_manager = get_db_manager()
    results = db_manager.test_connections()
    
    for db_name, status in results.items():
        if status:
            logger.success(f"{db_name} 连接成功 ✓")
        else:
            logger.error(f"{db_name} 连接失败 ✗")
    
    all_connected = all(results.values())
    
    if all_connected:
        logger.success("所有数据库连接测试通过！")
    else:
        logger.error("部分数据库连接失败，请检查配置")
        return False
    
    return True


def create_directories():
    """创建必要的目录"""
    logger.info("创建必要的目录...")
    
    directories = [
        app_settings.data_dir,
        app_settings.logs_dir,
        project_root / "data" / "raw",
        project_root / "data" / "processed",
        project_root / "data" / "cache",
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"目录已创建: {directory}")


def check_environment():
    """检查环境配置"""
    logger.info("检查环境配置...")
    
    from config.settings import data_settings, db_settings
    
    # 检查必要的环境变量
    required_vars = {
        'TUSHARE_TOKEN': data_settings.tushare_token,
        'POSTGRES_PASSWORD': db_settings.postgres_password,
        'CLICKHOUSE_PASSWORD': db_settings.clickhouse_password,
        'REDIS_PASSWORD': db_settings.redis_password
    }
    
    missing_vars = []
    for var_name, var_value in required_vars.items():
        if not var_value or var_value == "":
            missing_vars.append(var_name)
    
    if missing_vars:
        logger.warning(f"以下环境变量未设置: {', '.join(missing_vars)}")
        logger.info("请复制 .env.example 为 .env 并填入正确的值")
        return False
    
    logger.success("环境配置检查通过")
    return True


def initialize_database_schema():
    """初始化数据库表结构"""
    logger.info("初始化数据库表结构...")
    
    try:
        db_manager = get_db_manager()
        
        # 检查主要表是否存在 (四维分析)
        tables_to_check = [
            'stock_basic',
            'stock_daily_quotes',
            'money_flow_daily',
            'technical_daily_profiles',
            'capital_flow_profiles',
            'fundamental_profiles',
            'macro_profiles',
            'trading_signals'
        ]
        
        for table in tables_to_check:
            try:
                result = db_manager.execute_postgres_query(
                    f"SELECT COUNT(*) as count FROM {table} LIMIT 1"
                )
                logger.success(f"表 {table} 存在且可访问")
            except Exception as e:
                logger.error(f"表 {table} 不存在或无法访问: {e}")
                return False
        
        logger.success("数据库表结构检查通过")
        return True
        
    except Exception as e:
        logger.error(f"数据库表结构检查失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("=" * 50)
    logger.info("启明星项目数据库初始化开始")
    logger.info("=" * 50)
    
    # 1. 创建目录
    create_directories()
    
    # 2. 检查环境配置
    if not check_environment():
        logger.error("环境配置检查失败，初始化终止")
        sys.exit(1)
    
    # 3. 测试数据库连接
    if not test_database_connections():
        logger.error("数据库连接测试失败，初始化终止")
        sys.exit(1)
    
    # 4. 初始化数据库表结构
    if not initialize_database_schema():
        logger.error("数据库表结构初始化失败，初始化终止")
        sys.exit(1)
    
    logger.info("=" * 50)
    logger.success("启明星项目数据库初始化完成！")
    logger.info("=" * 50)
    
    logger.info("下一步操作建议:")
    logger.info("1. 启动 Docker 服务: docker-compose up -d")
    logger.info("2. 运行数据获取: python scripts/fetch_data.py")
    logger.info("3. 启动仪表盘: streamlit run dashboard/app.py")


if __name__ == "__main__":
    main()
