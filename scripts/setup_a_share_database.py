#!/usr/bin/env python3
"""
A股数据库初始化脚本
创建数据库表结构并导入基础数据
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.database import get_db_manager

logger = get_logger("setup_a_share_db")


def create_stock_basic_data():
    """创建股票基础信息数据"""
    logger.info("📋 创建A股基础信息数据...")
    
    # 创建代表性的A股股票列表
    stock_data = [
        # 银行股
        {'ts_code': '000001.SZ', 'symbol': '000001', 'name': '平安银行', 'area': '深圳', 'industry': '银行', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '600000.SH', 'symbol': '600000', 'name': '浦发银行', 'area': '上海', 'industry': '银行', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '600036.SH', 'symbol': '600036', 'name': '招商银行', 'area': '深圳', 'industry': '银行', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '601166.SH', 'symbol': '601166', 'name': '兴业银行', 'area': '福州', 'industry': '银行', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '601398.SH', 'symbol': '601398', 'name': '工商银行', 'area': '北京', 'industry': '银行', 'market': '主板', 'is_hs': 'Y'},
        
        # 科技股
        {'ts_code': '000002.SZ', 'symbol': '000002', 'name': '万科A', 'area': '深圳', 'industry': '房地产', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '000858.SZ', 'symbol': '000858', 'name': '五粮液', 'area': '宜宾', 'industry': '白酒', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '300059.SZ', 'symbol': '300059', 'name': '东方财富', 'area': '上海', 'industry': '金融服务', 'market': '创业板', 'is_hs': 'Y'},
        {'ts_code': '300750.SZ', 'symbol': '300750', 'name': '宁德时代', 'area': '宁德', 'industry': '新能源', 'market': '创业板', 'is_hs': 'Y'},
        
        # 消费股
        {'ts_code': '600519.SH', 'symbol': '600519', 'name': '贵州茅台', 'area': '贵阳', 'industry': '白酒', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '002415.SZ', 'symbol': '002415', 'name': '海康威视', 'area': '杭州', 'industry': '安防', 'market': '主板', 'is_hs': 'Y'},
        
        # 制造业
        {'ts_code': '600104.SH', 'symbol': '600104', 'name': '上汽集团', 'area': '上海', 'industry': '汽车', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '000725.SZ', 'symbol': '000725', 'name': '京东方A', 'area': '北京', 'industry': '显示器', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '002594.SZ', 'symbol': '002594', 'name': '比亚迪', 'area': '深圳', 'industry': '新能源汽车', 'market': '主板', 'is_hs': 'Y'},
        
        # 医药股
        {'ts_code': '000661.SZ', 'symbol': '000661', 'name': '长春高新', 'area': '长春', 'industry': '医药', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '300015.SZ', 'symbol': '300015', 'name': '爱尔眼科', 'area': '长沙', 'industry': '医疗服务', 'market': '创业板', 'is_hs': 'Y'},
        
        # 科创板
        {'ts_code': '688981.SH', 'symbol': '688981', 'name': '中芯国际', 'area': '上海', 'industry': '半导体', 'market': '科创板', 'is_hs': 'Y'},
        {'ts_code': '688036.SH', 'symbol': '688036', 'name': '传音控股', 'area': '深圳', 'industry': '消费电子', 'market': '科创板', 'is_hs': 'Y'},
        
        # 更多主流股票
        {'ts_code': '000063.SZ', 'symbol': '000063', 'name': '中兴通讯', 'area': '深圳', 'industry': '通信设备', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '600276.SH', 'symbol': '600276', 'name': '恒瑞医药', 'area': '连云港', 'industry': '医药', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '002230.SZ', 'symbol': '002230', 'name': '科大讯飞', 'area': '合肥', 'industry': '人工智能', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '600887.SH', 'symbol': '600887', 'name': '伊利股份', 'area': '呼和浩特', 'industry': '乳制品', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '000568.SZ', 'symbol': '000568', 'name': '泸州老窖', 'area': '泸州', 'industry': '白酒', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '600309.SH', 'symbol': '600309', 'name': '万华化学', 'area': '烟台', 'industry': '化工', 'market': '主板', 'is_hs': 'Y'},
        {'ts_code': '002142.SZ', 'symbol': '002142', 'name': '宁波银行', 'area': '宁波', 'industry': '银行', 'market': '主板', 'is_hs': 'Y'},
    ]
    
    # 添加上市日期
    for stock in stock_data:
        # 大部分股票设置为较早上市
        stock['list_date'] = datetime(2010, 1, 1) + timedelta(days=np.random.randint(0, 3650))
    
    df = pd.DataFrame(stock_data)
    logger.info(f"✅ 创建了 {len(df)} 只A股基础信息")
    
    return df


def create_daily_quotes_data(stock_list, target_date='2024-06-13'):
    """创建指定日期的日线行情数据"""
    logger.info(f"📈 创建 {target_date} 的日线行情数据...")
    
    quotes_data = []
    
    for _, stock in stock_list.iterrows():
        ts_code = stock['ts_code']
        name = stock['name']
        
        # 根据股票类型设置基础价格
        if '茅台' in name:
            base_price = 1800
        elif '银行' in stock['industry']:
            base_price = 15
        elif '科创板' in stock['market']:
            base_price = 80
        elif '创业板' in stock['market']:
            base_price = 60
        else:
            base_price = 50
        
        # 生成价格数据
        change_pct = np.random.normal(0, 0.03)  # 3%标准差
        change_pct = max(-0.10, min(0.10, change_pct))  # 限制在±10%
        
        pre_close = base_price
        open_price = pre_close * (1 + np.random.normal(0, 0.01))
        close_price = pre_close * (1 + change_pct)
        high_price = max(open_price, close_price) * (1 + abs(np.random.normal(0, 0.02)))
        low_price = min(open_price, close_price) * (1 - abs(np.random.normal(0, 0.02)))
        
        # 生成成交量
        base_volume = np.random.randint(1000000, 20000000)
        if abs(change_pct) > 0.05:
            volume = base_volume * np.random.uniform(1.5, 3.0)
        else:
            volume = base_volume * np.random.uniform(0.8, 1.2)
        
        amount = volume * (high_price + low_price) / 2
        
        quote = {
            'ts_code': ts_code,
            'trade_date': datetime.strptime(target_date, '%Y-%m-%d'),
            'open_price': round(open_price, 2),
            'high_price': round(high_price, 2),
            'low_price': round(low_price, 2),
            'close_price': round(close_price, 2),
            'pre_close': round(pre_close, 2),
            'change_amount': round(close_price - pre_close, 2),
            'pct_chg': round(change_pct * 100, 2),
            'vol': int(volume),
            'amount': round(amount, 2)
        }
        
        quotes_data.append(quote)
    
    df = pd.DataFrame(quotes_data)
    logger.info(f"✅ 创建了 {len(df)} 条 {target_date} 行情数据")
    
    return df


def setup_database():
    """设置数据库"""
    logger.info("🔧 设置A股数据库...")
    
    try:
        db_manager = get_db_manager()
        
        # 1. 创建股票基础信息
        stock_basic_df = create_stock_basic_data()
        
        # 2. 保存股票基础信息到数据库
        logger.info("💾 保存股票基础信息到数据库...")
        db_manager.insert_dataframe_to_postgres(
            stock_basic_df, 'stock_basic', if_exists='replace'
        )
        logger.info("✅ 股票基础信息已保存")
        
        # 3. 创建6月13日行情数据
        quotes_df = create_daily_quotes_data(stock_basic_df, '2024-06-13')
        
        # 4. 保存行情数据到数据库
        logger.info("💾 保存行情数据到数据库...")
        db_manager.insert_dataframe_to_postgres(
            quotes_df, 'stock_daily_quotes', if_exists='append'
        )
        logger.info("✅ 行情数据已保存")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 设置数据库失败: {e}")
        return False


def verify_setup():
    """验证设置结果"""
    logger.info("🔍 验证数据库设置...")
    
    try:
        db_manager = get_db_manager()
        
        # 检查股票基础信息
        stock_count_query = "SELECT COUNT(*) as count FROM stock_basic"
        stock_count_result = db_manager.execute_postgres_query(stock_count_query)
        stock_count = stock_count_result['count'].iloc[0] if not stock_count_result.empty else 0
        
        # 检查行情数据
        quotes_count_query = "SELECT COUNT(*) as count FROM stock_daily_quotes"
        quotes_count_result = db_manager.execute_postgres_query(quotes_count_query)
        quotes_count = quotes_count_result['count'].iloc[0] if not quotes_count_result.empty else 0
        
        # 检查最新数据日期
        latest_date_query = "SELECT MAX(trade_date) as latest_date FROM stock_daily_quotes"
        latest_date_result = db_manager.execute_postgres_query(latest_date_query)
        latest_date = latest_date_result['latest_date'].iloc[0] if not latest_date_result.empty else None
        
        logger.info("📊 数据库验证结果:")
        logger.info(f"  股票基础信息: {stock_count:,} 条")
        logger.info(f"  日线行情数据: {quotes_count:,} 条")
        logger.info(f"  最新数据日期: {latest_date}")
        
        # 显示样本数据
        if stock_count > 0:
            sample_query = "SELECT ts_code, name, industry, market FROM stock_basic LIMIT 5"
            sample_result = db_manager.execute_postgres_query(sample_query)
            
            logger.info("📋 样本股票:")
            for _, row in sample_result.iterrows():
                logger.info(f"  {row['ts_code']} - {row['name']} - {row['industry']} - {row['market']}")
        
        return stock_count > 0 and quotes_count > 0
        
    except Exception as e:
        logger.error(f"❌ 验证失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🚀 A股数据库初始化")
    logger.info("=" * 50)
    
    success_count = 0
    total_tasks = 2
    
    # 1. 设置数据库
    logger.info("\n📋 任务 1/2: 设置数据库")
    logger.info("-" * 30)
    if setup_database():
        success_count += 1
        logger.info("✅ 任务 1 完成")
    else:
        logger.error("❌ 任务 1 失败")
    
    # 2. 验证设置
    logger.info("\n🔍 任务 2/2: 验证设置")
    logger.info("-" * 30)
    if verify_setup():
        success_count += 1
        logger.info("✅ 任务 2 完成")
    else:
        logger.error("❌ 任务 2 失败")
    
    # 总结
    logger.info("\n" + "=" * 50)
    logger.info("📊 初始化总结")
    logger.info("=" * 50)
    logger.info(f"完成任务: {success_count}/{total_tasks}")
    
    if success_count == total_tasks:
        logger.info("🎉 A股数据库初始化成功!")
        logger.info("\n🚀 下一步操作:")
        logger.info("1. 访问Dashboard: http://localhost:8505")
        logger.info("2. 进入'数据管理'页面查看数据")
        logger.info("3. 测试数据更新功能")
        return True
    else:
        logger.error(f"⚠️ {total_tasks - success_count} 个任务失败")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
