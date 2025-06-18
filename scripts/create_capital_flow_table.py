#!/usr/bin/env python3
"""
创建资金流向表
"""
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from config.settings import db_settings

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.utils.database import get_db_manager
from src.utils.logger import get_logger

logger = get_logger("create_capital_flow_table")


def create_capital_flow_table():
    # 创建数据库引擎
    engine = create_engine(db_settings.postgres_url)
        
    # 创建表的 SQL
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS capital_flow_daily (
            id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        stock_code VARCHAR(10) NOT NULL,
        stock_name VARCHAR(50) NOT NULL,
        main_net_inflow DECIMAL(20,2),
        retail_net_inflow DECIMAL(20,2),
        super_large_net_inflow DECIMAL(20,2),
        large_net_inflow DECIMAL(20,2),
        medium_net_inflow DECIMAL(20,2),
        small_net_inflow DECIMAL(20,2),
        total_amount DECIMAL(20,2),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(date, stock_code)
        );
        """
        
    try:
        # 执行创建表的 SQL
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print("capital_flow_daily 表创建成功！")
    except Exception as e:
        print(f"创建表时出错: {str(e)}")


def create_other_missing_tables():
    """创建其他可能缺失的表"""
    try:
        db_manager = get_db_manager()
        
        # 创建技术指标表
        technical_indicators_sql = """
        CREATE TABLE IF NOT EXISTS technical_indicators_daily (
            id SERIAL PRIMARY KEY,
            ts_code VARCHAR(20) NOT NULL,
            trade_date DATE NOT NULL,
            ma5 DECIMAL(10, 4),
            ma10 DECIMAL(10, 4),
            ma20 DECIMAL(10, 4),
            ma60 DECIMAL(10, 4),
            rsi DECIMAL(10, 4),
            macd DECIMAL(10, 4),
            macd_signal DECIMAL(10, 4),
            macd_hist DECIMAL(10, 4),
            bollinger_upper DECIMAL(10, 4),
            bollinger_middle DECIMAL(10, 4),
            bollinger_lower DECIMAL(10, 4),
            kdj_k DECIMAL(10, 4),
            kdj_d DECIMAL(10, 4),
            kdj_j DECIMAL(10, 4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, trade_date)
        );
        """
        
        db_manager.execute_postgres_command(technical_indicators_sql)
        logger.info("技术指标表创建成功")
        
        # 创建市场情绪表
        market_sentiment_sql = """
        CREATE TABLE IF NOT EXISTS market_sentiment_daily (
            id SERIAL PRIMARY KEY,
            ts_code VARCHAR(20) NOT NULL,
            trade_date DATE NOT NULL,
            sentiment_score DECIMAL(10, 4),
            news_sentiment DECIMAL(10, 4),
            social_sentiment DECIMAL(10, 4),
            analyst_rating DECIMAL(10, 4),
            institutional_activity DECIMAL(10, 4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, trade_date)
        );
        """
        
        db_manager.execute_postgres_command(market_sentiment_sql)
        logger.info("市场情绪表创建成功")
        
        # 创建财务指标表
        financial_indicators_sql = """
        CREATE TABLE IF NOT EXISTS financial_indicators_quarterly (
            id SERIAL PRIMARY KEY,
            ts_code VARCHAR(20) NOT NULL,
            report_date DATE NOT NULL,
            pe_ratio DECIMAL(10, 4),
            pb_ratio DECIMAL(10, 4),
            ps_ratio DECIMAL(10, 4),
            roe DECIMAL(10, 4),
            roa DECIMAL(10, 4),
            debt_ratio DECIMAL(10, 4),
            current_ratio DECIMAL(10, 4),
            quick_ratio DECIMAL(10, 4),
            gross_margin DECIMAL(10, 4),
            net_margin DECIMAL(10, 4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, report_date)
        );
        """
        
        db_manager.execute_postgres_command(financial_indicators_sql)
        logger.info("财务指标表创建成功")
        
        return True
        
    except Exception as e:
        logger.error(f"创建其他表失败: {e}")
        return False


if __name__ == "__main__":
    logger.info("开始创建资金流向相关表...")
    
    # 创建资金流向表
    create_capital_flow_table()
    
    # 创建其他表
    if create_other_missing_tables():
        logger.info("✅ 其他相关表创建成功")
    else:
        logger.error("❌ 其他相关表创建失败")
        sys.exit(1)
    
    logger.info("🎉 所有表创建完成！")
