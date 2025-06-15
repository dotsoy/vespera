#!/usr/bin/env python3
"""
获取样本数据脚本 - 用于测试系统
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.database import get_db_manager
from src.utils.logger import get_logger

logger = get_logger("fetch_sample_data")


def generate_sample_stock_basic():
    """生成样本股票基础信息"""
    logger.info("生成样本股票基础信息...")
    
    # 样本股票数据
    sample_stocks = [
        ('000001.SZ', '000001', '平安银行', '深圳', '银行', '主板', '1991-04-03', 'S'),
        ('000002.SZ', '000002', '万科A', '深圳', '房地产开发', '主板', '1991-01-29', 'S'),
        ('000858.SZ', '000858', '五粮液', '四川', '白酒', '主板', '1998-04-27', 'S'),
        ('002415.SZ', '002415', '海康威视', '浙江', '安防设备', '中小板', '2010-05-20', 'S'),
        ('300059.SZ', '300059', '东方财富', '上海', '互联网金融', '创业板', '2010-03-19', 'S'),
        ('600036.SH', '600036', '招商银行', '广东', '银行', '主板', '2002-04-09', 'S'),
        ('600519.SH', '600519', '贵州茅台', '贵州', '白酒', '主板', '2001-08-27', 'S'),
        ('600887.SH', '600887', '伊利股份', '内蒙古', '乳品', '主板', '1996-03-12', 'S'),
        ('000858.SZ', '000858', '五粮液', '四川', '白酒', '主板', '1998-04-27', 'S'),
        ('002594.SZ', '002594', '比亚迪', '广东', '新能源汽车', '中小板', '2011-06-30', 'S'),
    ]
    
    df = pd.DataFrame(sample_stocks, columns=[
        'ts_code', 'symbol', 'name', 'area', 'industry', 'market', 'list_date', 'is_hs'
    ])
    
    df['list_date'] = pd.to_datetime(df['list_date'])
    
    return df


def generate_sample_daily_quotes(stock_codes, trade_date, days=30):
    """生成样本日线行情数据"""
    logger.info(f"生成样本日线行情数据，股票数: {len(stock_codes)}, 天数: {days}")
    
    all_data = []
    
    for ts_code in stock_codes:
        # 为每只股票生成历史数据
        base_price = random.uniform(10, 100)  # 基础价格
        
        for i in range(days):
            date = datetime.strptime(trade_date, '%Y-%m-%d') - timedelta(days=days-1-i)
            
            # 跳过周末
            if date.weekday() >= 5:
                continue
            
            # 生成价格数据 (简单随机游走)
            change_pct = random.normalvariate(0, 2)  # 平均0%，标准差2%的涨跌幅
            
            if i == 0:
                close_price = base_price
            else:
                close_price = prev_close * (1 + change_pct / 100)
            
            # 生成OHLC
            high_price = close_price * random.uniform(1.0, 1.05)
            low_price = close_price * random.uniform(0.95, 1.0)
            open_price = random.uniform(low_price, high_price)
            
            # 生成成交量
            vol = random.randint(1000000, 50000000)
            amount = vol * close_price * random.uniform(0.8, 1.2)
            
            all_data.append({
                'ts_code': ts_code,
                'trade_date': date.strftime('%Y-%m-%d'),
                'open_price': round(open_price, 2),
                'high_price': round(high_price, 2),
                'low_price': round(low_price, 2),
                'close_price': round(close_price, 2),
                'pre_close': round(prev_close if i > 0 else close_price, 2),
                'change_amount': round(close_price - (prev_close if i > 0 else close_price), 2),
                'pct_chg': round(change_pct, 2),
                'vol': vol,
                'amount': round(amount, 2)
            })
            
            prev_close = close_price
    
    return pd.DataFrame(all_data)


def generate_sample_money_flow(stock_codes, trade_date, days=20):
    """生成样本资金流数据"""
    logger.info(f"生成样本资金流数据，股票数: {len(stock_codes)}, 天数: {days}")
    
    all_data = []
    
    for ts_code in stock_codes:
        for i in range(days):
            date = datetime.strptime(trade_date, '%Y-%m-%d') - timedelta(days=days-1-i)
            
            # 跳过周末
            if date.weekday() >= 5:
                continue
            
            # 生成资金流数据
            total_amount = random.uniform(1e8, 1e10)  # 总成交金额
            
            # 按比例分配到不同资金类型
            sm_ratio = random.uniform(0.3, 0.5)  # 小单比例
            md_ratio = random.uniform(0.2, 0.3)  # 中单比例
            lg_ratio = random.uniform(0.15, 0.25)  # 大单比例
            elg_ratio = 1 - sm_ratio - md_ratio - lg_ratio  # 超大单比例
            
            # 计算各类资金金额
            sm_amount = total_amount * sm_ratio
            md_amount = total_amount * md_ratio
            lg_amount = total_amount * lg_ratio
            elg_amount = total_amount * elg_ratio
            
            # 生成买卖比例 (随机但有一定趋势)
            market_sentiment = random.uniform(-0.2, 0.2)  # 市场情绪
            
            all_data.append({
                'ts_code': ts_code,
                'trade_date': date.strftime('%Y-%m-%d'),
                'buy_sm_vol': int(random.uniform(0.4, 0.6) * sm_amount / 20),
                'buy_sm_amount': round(sm_amount * (0.5 + market_sentiment * 0.5), 2),
                'sell_sm_vol': int(random.uniform(0.4, 0.6) * sm_amount / 20),
                'sell_sm_amount': round(sm_amount * (0.5 - market_sentiment * 0.5), 2),
                'buy_md_vol': int(random.uniform(0.4, 0.6) * md_amount / 30),
                'buy_md_amount': round(md_amount * (0.5 + market_sentiment * 0.3), 2),
                'sell_md_vol': int(random.uniform(0.4, 0.6) * md_amount / 30),
                'sell_md_amount': round(md_amount * (0.5 - market_sentiment * 0.3), 2),
                'buy_lg_vol': int(random.uniform(0.4, 0.6) * lg_amount / 50),
                'buy_lg_amount': round(lg_amount * (0.5 + market_sentiment * 0.8), 2),
                'sell_lg_vol': int(random.uniform(0.4, 0.6) * lg_amount / 50),
                'sell_lg_amount': round(lg_amount * (0.5 - market_sentiment * 0.8), 2),
                'buy_elg_vol': int(random.uniform(0.4, 0.6) * elg_amount / 100),
                'buy_elg_amount': round(elg_amount * (0.5 + market_sentiment), 2),
                'sell_elg_vol': int(random.uniform(0.4, 0.6) * elg_amount / 100),
                'sell_elg_amount': round(elg_amount * (0.5 - market_sentiment), 2),
                'net_mf_vol': int(random.normalvariate(0, 1000000)),
                'net_mf_amount': round(total_amount * market_sentiment, 2)
            })
    
    return pd.DataFrame(all_data)


def main():
    """主函数"""
    logger.info("=" * 50)
    logger.info("开始生成样本数据")
    logger.info("=" * 50)
    
    try:
        db_manager = get_db_manager()
        
        # 测试数据库连接
        connection_status = db_manager.test_connections()
        if not connection_status.get('postgres', False):
            logger.error("PostgreSQL 连接失败，请检查数据库配置")
            return False
        
        trade_date = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 生成股票基础信息
        stock_basic_df = generate_sample_stock_basic()
        db_manager.insert_dataframe_to_postgres(
            stock_basic_df, 'stock_basic', if_exists='replace'
        )
        logger.success(f"股票基础信息已保存，共 {len(stock_basic_df)} 条记录")
        
        # 2. 生成日线行情数据
        stock_codes = stock_basic_df['ts_code'].tolist()
        quotes_df = generate_sample_daily_quotes(stock_codes, trade_date, days=60)
        db_manager.insert_dataframe_to_postgres(
            quotes_df, 'stock_daily_quotes', if_exists='replace'
        )
        logger.success(f"日线行情数据已保存，共 {len(quotes_df)} 条记录")
        
        # 3. 生成资金流数据
        money_flow_df = generate_sample_money_flow(stock_codes, trade_date, days=30)
        db_manager.insert_dataframe_to_postgres(
            money_flow_df, 'money_flow_daily', if_exists='replace'
        )
        logger.success(f"资金流数据已保存，共 {len(money_flow_df)} 条记录")
        
        logger.info("=" * 50)
        logger.success("样本数据生成完成！")
        logger.info("=" * 50)
        
        logger.info("下一步操作:")
        logger.info("1. 运行技术分析: python -c \"from src.analyzers.technical_analyzer import TechnicalAnalyzer; analyzer = TechnicalAnalyzer(); print('技术分析器已就绪')\"")
        logger.info("2. 启动仪表盘: streamlit run dashboard/app.py")
        logger.info("3. 查看 Airflow: http://localhost:8080")
        
        return True
        
    except Exception as e:
        logger.error(f"生成样本数据失败: {e}")
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
