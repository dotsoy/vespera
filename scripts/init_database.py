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
        
        # 创建必要的表 (四维分析)
        tables_to_create = {
            'stock_basic': """
                CREATE TABLE IF NOT EXISTS stock_basic (
                    ts_code VARCHAR(20) PRIMARY KEY,
                    symbol VARCHAR(10),
                    name VARCHAR(50),
                    area VARCHAR(50),
                    industry VARCHAR(50),
                    market VARCHAR(50),
                    exchange VARCHAR(10),
                    list_status VARCHAR(10),
                    list_date DATE,
                    delist_date DATE,
                    is_hs VARCHAR(10),
                    board VARCHAR(50)
                )
            """,
            'stock_daily_quotes': """
                CREATE TABLE IF NOT EXISTS stock_daily_quotes (
                    id SERIAL PRIMARY KEY,
                    ts_code VARCHAR(20),
                    trade_date DATE,
                    open_price DECIMAL(10,2),
                    high_price DECIMAL(10,2),
                    low_price DECIMAL(10,2),
                    close_price DECIMAL(10,2),
                    pre_close DECIMAL(10,2),
                    pct_chg DECIMAL(10,2),
                    vol BIGINT,
                    amount DECIMAL(15,2),
                    turnover_rate DECIMAL(10,2),
                    turnover_rate_f DECIMAL(10,2),
                    volume_ratio DECIMAL(10,2),
                    pe DECIMAL(10,2),
                    pe_ttm DECIMAL(10,2),
                    pb DECIMAL(10,2),
                    ps DECIMAL(10,2),
                    ps_ttm DECIMAL(10,2),
                    dv_ratio DECIMAL(10,2),
                    dv_ttm DECIMAL(10,2),
                    total_share BIGINT,
                    float_share BIGINT,
                    free_share BIGINT,
                    total_mv DECIMAL(15,2),
                    circ_mv DECIMAL(15,2),
                    UNIQUE(ts_code, trade_date)
                )
            """,
            'money_flow_daily': """
                CREATE TABLE IF NOT EXISTS money_flow_daily (
                    id SERIAL PRIMARY KEY,
                    ts_code VARCHAR(20),
                    trade_date DATE,
                    buy_sm_vol BIGINT,
                    buy_sm_amount DECIMAL(15,2),
                    sell_sm_vol BIGINT,
                    sell_sm_amount DECIMAL(15,2),
                    buy_md_vol BIGINT,
                    buy_md_amount DECIMAL(15,2),
                    sell_md_vol BIGINT,
                    sell_md_amount DECIMAL(15,2),
                    buy_lg_vol BIGINT,
                    buy_lg_amount DECIMAL(15,2),
                    sell_lg_vol BIGINT,
                    sell_lg_amount DECIMAL(15,2),
                    buy_elg_vol BIGINT,
                    buy_elg_amount DECIMAL(15,2),
                    sell_elg_vol BIGINT,
                    sell_elg_amount DECIMAL(15,2),
                    net_mf_vol BIGINT,
                    net_mf_amount DECIMAL(15,2),
                    UNIQUE(ts_code, trade_date)
                )
            """,
            'technical_daily_profiles': """
                CREATE TABLE IF NOT EXISTS technical_daily_profiles (
                    id SERIAL PRIMARY KEY,
                    ts_code VARCHAR(20),
                    trade_date DATE,
                    ma_5 DECIMAL(10,2),
                    ma_10 DECIMAL(10,2),
                    ma_20 DECIMAL(10,2),
                    ma_60 DECIMAL(10,2),
                    ema_12 DECIMAL(10,2),
                    ema_26 DECIMAL(10,2),
                    rsi DECIMAL(10,2),
                    macd DECIMAL(10,2),
                    macd_signal DECIMAL(10,2),
                    macd_hist DECIMAL(10,2),
                    bb_upper DECIMAL(10,2),
                    bb_middle DECIMAL(10,2),
                    bb_lower DECIMAL(10,2),
                    UNIQUE(ts_code, trade_date)
                )
            """,
            'capital_flow_profiles': """
                CREATE TABLE IF NOT EXISTS capital_flow_profiles (
                    id SERIAL PRIMARY KEY,
                    ts_code VARCHAR(20),
                    trade_date DATE,
                    net_inflow_main DECIMAL(15,2),
                    net_inflow_large DECIMAL(15,2),
                    net_inflow_medium DECIMAL(15,2),
                    net_inflow_small DECIMAL(15,2),
                    volume_trend_score DECIMAL(5,2),
                    concentration_score DECIMAL(5,2),
                    capital_flow_score DECIMAL(5,2),
                    UNIQUE(ts_code, trade_date)
                )
            """,
            'fundamental_profiles': """
                CREATE TABLE IF NOT EXISTS fundamental_profiles (
                    id SERIAL PRIMARY KEY,
                    ts_code VARCHAR(20),
                    trade_date DATE,
                    pe_ratio DECIMAL(10,2),
                    pb_ratio DECIMAL(10,2),
                    ps_ratio DECIMAL(10,2),
                    peg_ratio DECIMAL(10,2),
                    roa DECIMAL(10,2),
                    roe DECIMAL(10,2),
                    profit_margin DECIMAL(10,2),
                    debt_ratio DECIMAL(10,2),
                    fundamental_score DECIMAL(5,2),
                    UNIQUE(ts_code, trade_date)
                )
            """,
            'macro_profiles': """
                CREATE TABLE IF NOT EXISTS macro_profiles (
                    id SERIAL PRIMARY KEY,
                    trade_date DATE,
                    cci_index DECIMAL(10,2),
                    pmi_index DECIMAL(10,2),
                    money_supply_m2 DECIMAL(15,2),
                    interest_rate DECIMAL(10,2),
                    market_sentiment_index DECIMAL(10,2),
                    macro_score DECIMAL(5,2),
                    UNIQUE(trade_date)
                )
            """,
            'trading_signals': """
                CREATE TABLE IF NOT EXISTS trading_signals (
                    id SERIAL PRIMARY KEY,
                    ts_code VARCHAR(20),
                    trade_date DATE,
                    signal_type VARCHAR(50),
                    signal_strength DECIMAL(5,2),
                    signal_confidence DECIMAL(5,2),
                    signal_description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ts_code, trade_date, signal_type)
                )
            """
        }
        
        for table, create_sql in tables_to_create.items():
            try:
                db_manager.execute_postgres_command(create_sql)
                logger.success(f"表 {table} 已创建")
            except Exception as e:
                logger.error(f"表 {table} 创建失败: {e}")
                return False
        
        logger.success("数据库表结构初始化成功")
        return True
        
    except Exception as e:
        logger.error(f"数据库表结构初始化失败: {e}")
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
