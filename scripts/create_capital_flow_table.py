#!/usr/bin/env python3
"""
创建资金流向表
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.database import get_db_manager
from src.utils.logger import get_logger

logger = get_logger("create_capital_flow_table")


def create_capital_flow_table():
    """创建资金流向表"""
    try:
        db_manager = get_db_manager()
        
        # 创建资金流向表
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS capital_flow_daily (
            id SERIAL PRIMARY KEY,
            ts_code VARCHAR(20) NOT NULL,
            trade_date DATE NOT NULL,
            main_net_inflow DECIMAL(20, 2),
            main_net_inflow_rate DECIMAL(10, 4),
            super_large_net_inflow DECIMAL(20, 2),
            super_large_net_inflow_rate DECIMAL(10, 4),
            large_net_inflow DECIMAL(20, 2),
            large_net_inflow_rate DECIMAL(10, 4),
            medium_net_inflow DECIMAL(20, 2),
            medium_net_inflow_rate DECIMAL(10, 4),
            small_net_inflow DECIMAL(20, 2),
            small_net_inflow_rate DECIMAL(10, 4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, trade_date)
        );
        """
        
        db_manager.execute_postgres_command(create_table_sql)
        logger.info("资金流向表创建成功")
        
        # 创建索引
        index_sqls = [
            "CREATE INDEX IF NOT EXISTS idx_capital_flow_ts_code ON capital_flow_daily(ts_code);",
            "CREATE INDEX IF NOT EXISTS idx_capital_flow_trade_date ON capital_flow_daily(trade_date);",
            "CREATE INDEX IF NOT EXISTS idx_capital_flow_ts_code_date ON capital_flow_daily(ts_code, trade_date);"
        ]
        
        for sql in index_sqls:
            db_manager.execute_postgres_command(sql)
        
        logger.info("资金流向表索引创建成功")
        
        # 插入一些示例数据
        sample_data_sql = """
        INSERT INTO capital_flow_daily 
        (ts_code, trade_date, main_net_inflow, main_net_inflow_rate, 
         super_large_net_inflow, super_large_net_inflow_rate,
         large_net_inflow, large_net_inflow_rate,
         medium_net_inflow, medium_net_inflow_rate,
         small_net_inflow, small_net_inflow_rate)
        VALUES 
        ('000001.SZ', '2024-06-17', 1000000.00, 0.05, 500000.00, 0.025, 500000.00, 0.025, -300000.00, -0.015, -700000.00, -0.035),
        ('000002.SZ', '2024-06-17', -500000.00, -0.02, -200000.00, -0.008, -300000.00, -0.012, 100000.00, 0.004, 400000.00, 0.016),
        ('600000.SH', '2024-06-17', 2000000.00, 0.08, 1200000.00, 0.048, 800000.00, 0.032, -600000.00, -0.024, -1400000.00, -0.056)
        ON CONFLICT (ts_code, trade_date) DO NOTHING;
        """
        
        db_manager.execute_postgres_command(sample_data_sql)
        logger.info("示例数据插入成功")
        
        return True
        
    except Exception as e:
        logger.error(f"创建资金流向表失败: {e}")
        return False


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
    if create_capital_flow_table():
        logger.info("✅ 资金流向表创建成功")
    else:
        logger.error("❌ 资金流向表创建失败")
        sys.exit(1)
    
    # 创建其他表
    if create_other_missing_tables():
        logger.info("✅ 其他相关表创建成功")
    else:
        logger.error("❌ 其他相关表创建失败")
        sys.exit(1)
    
    logger.info("🎉 所有表创建完成！")
