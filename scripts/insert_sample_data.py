#!/usr/bin/env python3
"""
直接插入样本数据到数据库
"""
import sys
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.database import get_db_manager

logger = get_logger("insert_sample_data")


def insert_stock_basic():
    """插入股票基础信息"""
    logger.info("📋 插入股票基础信息...")
    
    try:
        db_manager = get_db_manager()
        
        # 插入股票基础信息
        insert_sql = """
        INSERT INTO stock_basic (ts_code, symbol, name, area, industry, market, list_date, is_hs) VALUES
        ('000001.SZ', '000001', '平安银行', '深圳', '银行', '主板', '2000-04-27', 'Y'),
        ('600000.SH', '600000', '浦发银行', '上海', '银行', '主板', '1999-11-10', 'Y'),
        ('600036.SH', '600036', '招商银行', '深圳', '银行', '主板', '2002-04-09', 'Y'),
        ('601166.SH', '601166', '兴业银行', '福州', '银行', '主板', '2007-02-05', 'Y'),
        ('601398.SH', '601398', '工商银行', '北京', '银行', '主板', '2006-10-27', 'Y'),
        ('000002.SZ', '000002', '万科A', '深圳', '房地产', '主板', '1991-01-29', 'Y'),
        ('000858.SZ', '000858', '五粮液', '宜宾', '白酒', '主板', '1998-04-27', 'Y'),
        ('300059.SZ', '300059', '东方财富', '上海', '金融服务', '创业板', '2010-03-19', 'Y'),
        ('300750.SZ', '300750', '宁德时代', '宁德', '新能源', '创业板', '2018-06-11', 'Y'),
        ('600519.SH', '600519', '贵州茅台', '贵阳', '白酒', '主板', '2001-08-27', 'Y'),
        ('002415.SZ', '002415', '海康威视', '杭州', '安防', '主板', '2010-05-28', 'Y'),
        ('600104.SH', '600104', '上汽集团', '上海', '汽车', '主板', '1997-11-25', 'Y'),
        ('000725.SZ', '000725', '京东方A', '北京', '显示器', '主板', '2001-01-12', 'Y'),
        ('002594.SZ', '002594', '比亚迪', '深圳', '新能源汽车', '主板', '2011-06-30', 'Y'),
        ('000661.SZ', '000661', '长春高新', '长春', '医药', '主板', '1996-12-18', 'Y'),
        ('300015.SZ', '300015', '爱尔眼科', '长沙', '医疗服务', '创业板', '2009-10-30', 'Y'),
        ('688981.SH', '688981', '中芯国际', '上海', '半导体', '科创板', '2020-07-16', 'Y'),
        ('688036.SH', '688036', '传音控股', '深圳', '消费电子', '科创板', '2019-09-30', 'Y'),
        ('000063.SZ', '000063', '中兴通讯', '深圳', '通信设备', '主板', '1997-11-18', 'Y'),
        ('600276.SH', '600276', '恒瑞医药', '连云港', '医药', '主板', '2000-10-12', 'Y'),
        ('002230.SZ', '002230', '科大讯飞', '合肥', '人工智能', '主板', '2008-05-12', 'Y'),
        ('600887.SH', '600887', '伊利股份', '呼和浩特', '乳制品', '主板', '1996-03-12', 'Y'),
        ('000568.SZ', '000568', '泸州老窖', '泸州', '白酒', '主板', '1994-05-09', 'Y'),
        ('600309.SH', '600309', '万华化学', '烟台', '化工', '主板', '2001-01-12', 'Y'),
        ('002142.SZ', '002142', '宁波银行', '宁波', '银行', '主板', '2007-07-19', 'Y')
        ON CONFLICT (ts_code) DO NOTHING;
        """
        
        db_manager.execute_postgres_query(insert_sql)
        logger.info("✅ 股票基础信息插入完成")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 插入股票基础信息失败: {e}")
        return False


def insert_daily_quotes():
    """插入6月13日行情数据"""
    logger.info("📈 插入6月13日行情数据...")
    
    try:
        db_manager = get_db_manager()
        
        # 插入行情数据
        insert_sql = """
        INSERT INTO stock_daily_quotes (ts_code, trade_date, open_price, high_price, low_price, close_price, pre_close, change_amount, pct_chg, vol, amount) VALUES
        ('000001.SZ', '2024-06-13', 14.50, 14.80, 14.30, 14.65, 14.45, 0.20, 1.38, 15000000, 220000000),
        ('600000.SH', '2024-06-13', 8.20, 8.35, 8.15, 8.28, 8.18, 0.10, 1.22, 25000000, 206000000),
        ('600036.SH', '2024-06-13', 35.50, 36.20, 35.30, 35.85, 35.40, 0.45, 1.27, 18000000, 645000000),
        ('601166.SH', '2024-06-13', 16.80, 17.10, 16.65, 16.95, 16.75, 0.20, 1.19, 12000000, 202000000),
        ('601398.SH', '2024-06-13', 5.85, 5.92, 5.82, 5.89, 5.84, 0.05, 0.86, 45000000, 264000000),
        ('000002.SZ', '2024-06-13', 9.50, 9.68, 9.42, 9.58, 9.48, 0.10, 1.05, 22000000, 210000000),
        ('000858.SZ', '2024-06-13', 128.50, 131.20, 127.80, 130.45, 128.90, 1.55, 1.20, 8000000, 1040000000),
        ('300059.SZ', '2024-06-13', 12.80, 13.15, 12.65, 12.95, 12.75, 0.20, 1.57, 35000000, 453000000),
        ('300750.SZ', '2024-06-13', 185.50, 189.20, 184.30, 187.65, 186.20, 1.45, 0.78, 12000000, 2250000000),
        ('600519.SH', '2024-06-13', 1750.00, 1768.50, 1742.00, 1755.80, 1748.20, 7.60, 0.43, 2500000, 4390000000),
        ('002415.SZ', '2024-06-13', 32.50, 33.20, 32.20, 32.85, 32.40, 0.45, 1.39, 15000000, 492000000),
        ('600104.SH', '2024-06-13', 16.20, 16.45, 16.05, 16.32, 16.15, 0.17, 1.05, 18000000, 293000000),
        ('000725.SZ', '2024-06-13', 3.85, 3.92, 3.82, 3.88, 3.84, 0.04, 1.04, 55000000, 213000000),
        ('002594.SZ', '2024-06-13', 245.50, 250.20, 243.80, 248.65, 246.30, 2.35, 0.95, 8000000, 1970000000),
        ('000661.SZ', '2024-06-13', 145.20, 148.50, 144.30, 147.85, 145.80, 2.05, 1.41, 3500000, 512000000),
        ('300015.SZ', '2024-06-13', 18.50, 18.85, 18.35, 18.72, 18.45, 0.27, 1.46, 12000000, 224000000),
        ('688981.SH', '2024-06-13', 45.20, 46.50, 44.80, 45.95, 45.10, 0.85, 1.88, 25000000, 1148000000),
        ('688036.SH', '2024-06-13', 85.50, 87.20, 84.80, 86.45, 85.20, 1.25, 1.47, 8000000, 690000000),
        ('000063.SZ', '2024-06-13', 28.50, 29.20, 28.20, 28.85, 28.40, 0.45, 1.58, 20000000, 576000000),
        ('600276.SH', '2024-06-13', 52.80, 54.20, 52.50, 53.65, 52.90, 0.75, 1.42, 8000000, 429000000),
        ('002230.SZ', '2024-06-13', 38.50, 39.50, 38.20, 39.15, 38.40, 0.75, 1.95, 15000000, 587000000),
        ('600887.SH', '2024-06-13', 28.20, 28.65, 28.05, 28.45, 28.15, 0.30, 1.07, 12000000, 341000000),
        ('000568.SZ', '2024-06-13', 95.50, 97.80, 95.20, 96.85, 95.80, 1.05, 1.10, 6000000, 580000000),
        ('600309.SH', '2024-06-13', 78.50, 80.20, 78.10, 79.45, 78.80, 0.65, 0.82, 10000000, 794000000),
        ('002142.SZ', '2024-06-13', 22.50, 22.85, 22.35, 22.68, 22.45, 0.23, 1.02, 8000000, 181000000)
        ON CONFLICT (ts_code, trade_date) DO NOTHING;
        """
        
        db_manager.execute_postgres_query(insert_sql)
        logger.info("✅ 6月13日行情数据插入完成")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 插入行情数据失败: {e}")
        return False


def verify_data():
    """验证数据"""
    logger.info("🔍 验证插入的数据...")
    
    try:
        db_manager = get_db_manager()
        
        # 检查股票基础信息
        stock_count = db_manager.execute_postgres_query("SELECT COUNT(*) as count FROM stock_basic")
        stock_count_val = stock_count['count'].iloc[0] if not stock_count.empty else 0
        
        # 检查行情数据
        quotes_count = db_manager.execute_postgres_query("SELECT COUNT(*) as count FROM stock_daily_quotes")
        quotes_count_val = quotes_count['count'].iloc[0] if not quotes_count.empty else 0
        
        # 检查最新数据日期
        latest_date = db_manager.execute_postgres_query("SELECT MAX(trade_date) as latest_date FROM stock_daily_quotes")
        latest_date_val = latest_date['latest_date'].iloc[0] if not latest_date.empty else None
        
        logger.info("📊 数据验证结果:")
        logger.info(f"  股票基础信息: {stock_count_val:,} 条")
        logger.info(f"  日线行情数据: {quotes_count_val:,} 条")
        logger.info(f"  最新数据日期: {latest_date_val}")
        
        # 显示样本数据
        if stock_count_val > 0:
            sample_stocks = db_manager.execute_postgres_query(
                "SELECT ts_code, name, industry, market FROM stock_basic LIMIT 5"
            )
            logger.info("📋 样本股票:")
            for _, row in sample_stocks.iterrows():
                logger.info(f"  {row['ts_code']} - {row['name']} - {row['industry']} - {row['market']}")
        
        if quotes_count_val > 0:
            sample_quotes = db_manager.execute_postgres_query(
                "SELECT ts_code, trade_date, close_price, pct_chg FROM stock_daily_quotes LIMIT 5"
            )
            logger.info("📈 样本行情:")
            for _, row in sample_quotes.iterrows():
                logger.info(f"  {row['ts_code']} - {row['trade_date']} - ¥{row['close_price']:.2f} - {row['pct_chg']:.2f}%")
        
        return stock_count_val > 0 and quotes_count_val > 0
        
    except Exception as e:
        logger.error(f"❌ 验证数据失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🚀 插入A股样本数据")
    logger.info("=" * 40)
    
    success_count = 0
    total_tasks = 3
    
    # 1. 插入股票基础信息
    logger.info("\n📋 任务 1/3: 插入股票基础信息")
    if insert_stock_basic():
        success_count += 1
        logger.info("✅ 任务 1 完成")
    else:
        logger.error("❌ 任务 1 失败")
    
    # 2. 插入行情数据
    logger.info("\n📈 任务 2/3: 插入6月13日行情数据")
    if insert_daily_quotes():
        success_count += 1
        logger.info("✅ 任务 2 完成")
    else:
        logger.error("❌ 任务 2 失败")
    
    # 3. 验证数据
    logger.info("\n🔍 任务 3/3: 验证数据")
    if verify_data():
        success_count += 1
        logger.info("✅ 任务 3 完成")
    else:
        logger.error("❌ 任务 3 失败")
    
    # 总结
    logger.info("\n" + "=" * 40)
    logger.info("📊 插入任务总结")
    logger.info("=" * 40)
    logger.info(f"完成任务: {success_count}/{total_tasks}")
    
    if success_count == total_tasks:
        logger.info("🎉 A股样本数据插入成功!")
        logger.info("\n🚀 下一步操作:")
        logger.info("1. 访问Dashboard: http://localhost:8505")
        logger.info("2. 进入'数据管理'页面查看数据")
        logger.info("3. 测试数据更新功能")
        logger.info("4. 在'策略分析'页面选择股票进行分析")
        return True
    else:
        logger.error(f"⚠️ {total_tasks - success_count} 个任务失败")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
