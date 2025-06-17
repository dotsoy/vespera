#!/usr/bin/env python3
"""
填充示例数据以测试个股全息透视功能
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.database import get_db_manager
from src.utils.logger import get_logger

logger = get_logger("populate_sample_data")


def generate_technical_indicators_data():
    """生成技术指标示例数据"""
    try:
        db_manager = get_db_manager()
        
        # 获取现有股票代码
        stock_query = "SELECT ts_code FROM stock_basic LIMIT 10"
        stocks_df = db_manager.execute_postgres_query(stock_query)
        
        if stocks_df.empty:
            logger.warning("没有找到股票数据，使用默认股票代码")
            stock_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ']
        else:
            stock_codes = stocks_df['ts_code'].tolist()[:5]  # 取前5只股票
        
        # 生成最近30天的数据
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        data_records = []
        
        for stock_code in stock_codes:
            current_date = start_date
            while current_date <= end_date:
                # 跳过周末
                if current_date.weekday() < 5:
                    record = {
                        'ts_code': stock_code,
                        'trade_date': current_date.strftime('%Y-%m-%d'),
                        'ma5': round(np.random.uniform(10, 50), 4),
                        'ma10': round(np.random.uniform(10, 50), 4),
                        'ma20': round(np.random.uniform(10, 50), 4),
                        'ma60': round(np.random.uniform(10, 50), 4),
                        'rsi': round(np.random.uniform(20, 80), 4),
                        'macd': round(np.random.uniform(-2, 2), 4),
                        'macd_signal': round(np.random.uniform(-2, 2), 4),
                        'macd_hist': round(np.random.uniform(-1, 1), 4),
                        'bollinger_upper': round(np.random.uniform(50, 60), 4),
                        'bollinger_middle': round(np.random.uniform(40, 50), 4),
                        'bollinger_lower': round(np.random.uniform(30, 40), 4),
                        'kdj_k': round(np.random.uniform(20, 80), 4),
                        'kdj_d': round(np.random.uniform(20, 80), 4),
                        'kdj_j': round(np.random.uniform(0, 100), 4)
                    }
                    data_records.append(record)
                
                current_date += timedelta(days=1)
        
        # 批量插入数据
        for record in data_records:
            insert_sql = f"""
            INSERT INTO technical_indicators_daily
            (ts_code, trade_date, ma5, ma10, ma20, ma60, rsi, macd, macd_signal, macd_hist,
             bollinger_upper, bollinger_middle, bollinger_lower, kdj_k, kdj_d, kdj_j)
            VALUES ('{record['ts_code']}', '{record['trade_date']}', {record['ma5']}, {record['ma10']},
                    {record['ma20']}, {record['ma60']}, {record['rsi']}, {record['macd']},
                    {record['macd_signal']}, {record['macd_hist']}, {record['bollinger_upper']},
                    {record['bollinger_middle']}, {record['bollinger_lower']}, {record['kdj_k']},
                    {record['kdj_d']}, {record['kdj_j']})
            ON CONFLICT (ts_code, trade_date) DO UPDATE SET
            ma5 = EXCLUDED.ma5, ma10 = EXCLUDED.ma10, ma20 = EXCLUDED.ma20,
            rsi = EXCLUDED.rsi, macd = EXCLUDED.macd
            """
            db_manager.execute_postgres_command(insert_sql)
        
        logger.info(f"成功插入 {len(data_records)} 条技术指标数据")
        return True
        
    except Exception as e:
        logger.error(f"生成技术指标数据失败: {e}")
        return False


def generate_capital_flow_data():
    """生成资金流向示例数据"""
    try:
        db_manager = get_db_manager()
        
        # 获取现有股票代码
        stock_query = "SELECT ts_code FROM stock_basic LIMIT 10"
        stocks_df = db_manager.execute_postgres_query(stock_query)
        
        if stocks_df.empty:
            stock_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ']
        else:
            stock_codes = stocks_df['ts_code'].tolist()[:5]
        
        # 生成最近30天的数据
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        data_records = []
        
        for stock_code in stock_codes:
            current_date = start_date
            while current_date <= end_date:
                # 跳过周末
                if current_date.weekday() < 5:
                    main_inflow = np.random.uniform(-5000000, 5000000)
                    record = {
                        'ts_code': stock_code,
                        'trade_date': current_date.strftime('%Y-%m-%d'),
                        'main_net_inflow': round(main_inflow, 2),
                        'main_net_inflow_rate': round(main_inflow / 100000000, 4),
                        'super_large_net_inflow': round(np.random.uniform(-2000000, 2000000), 2),
                        'super_large_net_inflow_rate': round(np.random.uniform(-0.02, 0.02), 4),
                        'large_net_inflow': round(np.random.uniform(-3000000, 3000000), 2),
                        'large_net_inflow_rate': round(np.random.uniform(-0.03, 0.03), 4),
                        'medium_net_inflow': round(np.random.uniform(-1000000, 1000000), 2),
                        'medium_net_inflow_rate': round(np.random.uniform(-0.01, 0.01), 4),
                        'small_net_inflow': round(np.random.uniform(-500000, 500000), 2),
                        'small_net_inflow_rate': round(np.random.uniform(-0.005, 0.005), 4)
                    }
                    data_records.append(record)
                
                current_date += timedelta(days=1)
        
        # 批量插入数据
        for record in data_records:
            insert_sql = f"""
            INSERT INTO capital_flow_daily
            (ts_code, trade_date, main_net_inflow, main_net_inflow_rate,
             super_large_net_inflow, super_large_net_inflow_rate,
             large_net_inflow, large_net_inflow_rate,
             medium_net_inflow, medium_net_inflow_rate,
             small_net_inflow, small_net_inflow_rate)
            VALUES ('{record['ts_code']}', '{record['trade_date']}', {record['main_net_inflow']},
                    {record['main_net_inflow_rate']}, {record['super_large_net_inflow']},
                    {record['super_large_net_inflow_rate']}, {record['large_net_inflow']},
                    {record['large_net_inflow_rate']}, {record['medium_net_inflow']},
                    {record['medium_net_inflow_rate']}, {record['small_net_inflow']},
                    {record['small_net_inflow_rate']})
            ON CONFLICT (ts_code, trade_date) DO UPDATE SET
            main_net_inflow = EXCLUDED.main_net_inflow,
            main_net_inflow_rate = EXCLUDED.main_net_inflow_rate
            """
            db_manager.execute_postgres_command(insert_sql)
        
        logger.info(f"成功插入 {len(data_records)} 条资金流向数据")
        return True
        
    except Exception as e:
        logger.error(f"生成资金流向数据失败: {e}")
        return False


def generate_market_sentiment_data():
    """生成市场情绪示例数据"""
    try:
        db_manager = get_db_manager()
        
        # 获取现有股票代码
        stock_query = "SELECT ts_code FROM stock_basic LIMIT 10"
        stocks_df = db_manager.execute_postgres_query(stock_query)
        
        if stocks_df.empty:
            stock_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ']
        else:
            stock_codes = stocks_df['ts_code'].tolist()[:5]
        
        # 生成最近30天的数据
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        data_records = []
        
        for stock_code in stock_codes:
            current_date = start_date
            while current_date <= end_date:
                # 跳过周末
                if current_date.weekday() < 5:
                    record = {
                        'ts_code': stock_code,
                        'trade_date': current_date.strftime('%Y-%m-%d'),
                        'sentiment_score': round(np.random.uniform(0, 10), 4),
                        'news_sentiment': round(np.random.uniform(0, 10), 4),
                        'social_sentiment': round(np.random.uniform(0, 10), 4),
                        'analyst_rating': round(np.random.uniform(0, 10), 4),
                        'institutional_activity': round(np.random.uniform(0, 10), 4)
                    }
                    data_records.append(record)
                
                current_date += timedelta(days=1)
        
        # 批量插入数据
        for record in data_records:
            insert_sql = f"""
            INSERT INTO market_sentiment_daily
            (ts_code, trade_date, sentiment_score, news_sentiment, social_sentiment,
             analyst_rating, institutional_activity)
            VALUES ('{record['ts_code']}', '{record['trade_date']}', {record['sentiment_score']},
                    {record['news_sentiment']}, {record['social_sentiment']},
                    {record['analyst_rating']}, {record['institutional_activity']})
            ON CONFLICT (ts_code, trade_date) DO UPDATE SET
            sentiment_score = EXCLUDED.sentiment_score,
            news_sentiment = EXCLUDED.news_sentiment
            """
            db_manager.execute_postgres_command(insert_sql)
        
        logger.info(f"成功插入 {len(data_records)} 条市场情绪数据")
        return True
        
    except Exception as e:
        logger.error(f"生成市场情绪数据失败: {e}")
        return False


if __name__ == "__main__":
    logger.info("开始生成示例数据...")
    
    # 生成技术指标数据
    if generate_technical_indicators_data():
        logger.info("✅ 技术指标数据生成成功")
    else:
        logger.error("❌ 技术指标数据生成失败")
    
    # 生成资金流向数据
    if generate_capital_flow_data():
        logger.info("✅ 资金流向数据生成成功")
    else:
        logger.error("❌ 资金流向数据生成失败")
    
    # 生成市场情绪数据
    if generate_market_sentiment_data():
        logger.info("✅ 市场情绪数据生成成功")
    else:
        logger.error("❌ 市场情绪数据生成失败")
    
    logger.info("🎉 示例数据生成完成！")
