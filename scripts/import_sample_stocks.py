#!/usr/bin/env python3
"""
导入样本股票数据 (使用公开数据源)
"""
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.database import get_db_manager
from src.data_sources.tushare_client import TushareClient

logger = get_logger("import_sample_stocks")


def clean_database():
    """清理数据库"""
    try:
        logger.info("🧹 清理数据库")

        db = get_db_manager()

        # 创建空表结构
        logger.info("重建股票日线数据表...")
        empty_daily = pd.DataFrame(columns=[
            'ts_code', 'trade_date', 'open_price', 'high_price',
            'low_price', 'close_price', 'vol', 'pct_chg'
        ])
        db.insert_dataframe_to_postgres(empty_daily, 'stock_daily_quotes', if_exists='replace', index=False)

        logger.info("重建股票基础信息表...")
        empty_basic = pd.DataFrame(columns=[
            'ts_code', 'symbol', 'name', 'area', 'industry',
            'market', 'list_date', 'list_status'
        ])
        db.insert_dataframe_to_postgres(empty_basic, 'stock_basic', if_exists='replace', index=False)

        logger.success("✅ 数据库清理完成")
        return True

    except Exception as e:
        logger.error(f"❌ 数据库清理失败: {e}")
        return False


def get_sample_stock_list():
    """获取样本股票列表"""
    try:
        logger.info("📋 创建样本股票列表")
        
        # 创建代表性股票列表 (各行业龙头)
        sample_stocks = [
            # 银行股
            {'ts_code': '000001.SZ', 'symbol': '000001', 'name': '平安银行', 'industry': '银行', 'market': '主板'},
            {'ts_code': '600000.SH', 'symbol': '600000', 'name': '浦发银行', 'industry': '银行', 'market': '主板'},
            {'ts_code': '600036.SH', 'symbol': '600036', 'name': '招商银行', 'industry': '银行', 'market': '主板'},
            {'ts_code': '601166.SH', 'symbol': '601166', 'name': '兴业银行', 'industry': '银行', 'market': '主板'},
            
            # 白酒股
            {'ts_code': '600519.SH', 'symbol': '600519', 'name': '贵州茅台', 'industry': '白酒', 'market': '主板'},
            {'ts_code': '000858.SZ', 'symbol': '000858', 'name': '五粮液', 'industry': '白酒', 'market': '主板'},
            {'ts_code': '000568.SZ', 'symbol': '000568', 'name': '泸州老窖', 'industry': '白酒', 'market': '主板'},
            
            # 科技股
            {'ts_code': '000002.SZ', 'symbol': '000002', 'name': '万科A', 'industry': '房地产', 'market': '主板'},
            {'ts_code': '002415.SZ', 'symbol': '002415', 'name': '海康威视', 'industry': '电子', 'market': '中小板'},
            {'ts_code': '300059.SZ', 'symbol': '300059', 'name': '东方财富', 'industry': '软件服务', 'market': '创业板'},
            {'ts_code': '300750.SZ', 'symbol': '300750', 'name': '宁德时代', 'industry': '电池', 'market': '创业板'},
            
            # 消费股
            {'ts_code': '600887.SH', 'symbol': '600887', 'name': '伊利股份', 'industry': '食品饮料', 'market': '主板'},
            {'ts_code': '000858.SZ', 'symbol': '000858', 'name': '五粮液', 'industry': '食品饮料', 'market': '主板'},
            
            # 医药股
            {'ts_code': '000001.SZ', 'symbol': '000001', 'name': '平安银行', 'industry': '银行', 'market': '主板'},
            
            # 新能源
            {'ts_code': '002594.SZ', 'symbol': '002594', 'name': '比亚迪', 'industry': '汽车制造', 'market': '中小板'},
            
            # 保险
            {'ts_code': '601318.SH', 'symbol': '601318', 'name': '中国平安', 'industry': '保险', 'market': '主板'},
            
            # 石油化工
            {'ts_code': '600028.SH', 'symbol': '600028', 'name': '中国石化', 'industry': '石油化工', 'market': '主板'},
            
            # 钢铁
            {'ts_code': '600019.SH', 'symbol': '600019', 'name': '宝钢股份', 'industry': '钢铁', 'market': '主板'},
            
            # 电力
            {'ts_code': '600900.SH', 'symbol': '600900', 'name': '长江电力', 'industry': '电力', 'market': '主板'},
            
            # 通信
            {'ts_code': '600050.SH', 'symbol': '600050', 'name': '中国联通', 'industry': '通信', 'market': '主板'},
        ]
        
        # 去重并创建DataFrame
        unique_stocks = {}
        for stock in sample_stocks:
            if stock['ts_code'] not in unique_stocks:
                unique_stocks[stock['ts_code']] = stock
        
        stock_df = pd.DataFrame(list(unique_stocks.values()))
        
        # 添加其他必要字段
        stock_df['area'] = '深圳'
        stock_df['list_date'] = '20100101'
        stock_df['list_status'] = 'L'
        
        logger.info(f"✅ 创建了 {len(stock_df)} 只样本股票")
        return stock_df
        
    except Exception as e:
        logger.error(f"❌ 创建样本股票列表失败: {e}")
        return pd.DataFrame()


def import_stock_basic_info(stock_df: pd.DataFrame):
    """导入股票基础信息"""
    try:
        logger.info("📊 导入股票基础信息")

        db = get_db_manager()

        # 导入数据
        db.insert_dataframe_to_postgres(stock_df, 'stock_basic', if_exists='append', index=False)

        logger.success(f"✅ 成功导入 {len(stock_df)} 只股票基础信息")
        return len(stock_df)

    except Exception as e:
        logger.error(f"❌ 导入股票基础信息失败: {e}")
        return 0


def import_stock_daily_data(stock_list: pd.DataFrame, days: int = 60):
    """导入股票日线数据"""
    try:
        logger.info(f"📈 导入股票日线数据 (最近{days}天)")
        
        tushare_client = TushareClient()
        db = get_db_manager()
        
        # 计算日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        
        logger.info(f"数据日期范围: {start_date} - {end_date}")
        
        success_count = 0
        error_count = 0
        total_records = 0
        
        for _, stock in stock_list.iterrows():
            ts_code = stock['ts_code']
            
            try:
                logger.info(f"获取股票 {ts_code} ({stock['name']}) 的数据")
                
                # 获取日线数据
                daily_data = tushare_client.get_daily_quotes(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if not daily_data.empty:
                    # 只保留最近的交易日
                    daily_data = daily_data.sort_values('trade_date').tail(days)
                    
                    # 插入数据库
                    db.insert_dataframe_to_postgres(
                        daily_data,
                        'stock_daily_quotes',
                        if_exists='append',
                        index=False
                    )
                    
                    success_count += 1
                    total_records += len(daily_data)
                    logger.info(f"✅ {ts_code} 导入 {len(daily_data)} 条记录")
                else:
                    logger.warning(f"⚠️ {ts_code} 无数据")
                    error_count += 1
                
                # 控制API调用频率
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"❌ 获取 {ts_code} 数据失败: {e}")
                error_count += 1
                continue
        
        logger.success(f"🎉 股票日线数据导入完成！")
        logger.info(f"📊 导入统计:")
        logger.info(f"  成功股票: {success_count}")
        logger.info(f"  失败股票: {error_count}")
        logger.info(f"  总记录数: {total_records}")
        logger.info(f"  成功率: {success_count/(success_count+error_count)*100:.1f}%")
        
        return success_count, total_records
        
    except Exception as e:
        logger.error(f"❌ 导入股票日线数据失败: {e}")
        return 0, 0


def verify_import_results():
    """验证导入结果"""
    try:
        logger.info("🔍 验证导入结果")
        
        db = get_db_manager()
        
        # 检查股票基础信息
        basic_query = """
        SELECT 
            COUNT(*) as total_stocks,
            COUNT(DISTINCT industry) as industries,
            COUNT(DISTINCT market) as markets
        FROM stock_basic
        """
        
        basic_stats = db.execute_postgres_query(basic_query)
        logger.info(f"股票基础信息: {basic_stats.iloc[0].to_dict()}")
        
        # 检查日线数据
        daily_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT ts_code) as stocks_with_data,
            MIN(trade_date) as earliest_date,
            MAX(trade_date) as latest_date
        FROM stock_daily_quotes
        """
        
        daily_stats = db.execute_postgres_query(daily_query)
        logger.info(f"日线数据统计: {daily_stats.iloc[0].to_dict()}")
        
        # 检查行业分布
        industry_query = """
        SELECT 
            sb.industry,
            COUNT(sb.ts_code) as total_stocks,
            COUNT(DISTINCT sdq.ts_code) as stocks_with_data
        FROM stock_basic sb
        LEFT JOIN stock_daily_quotes sdq ON sb.ts_code = sdq.ts_code
        GROUP BY sb.industry
        ORDER BY stocks_with_data DESC
        """
        
        industry_stats = db.execute_postgres_query(industry_query)
        logger.info("行业分布:")
        for _, row in industry_stats.iterrows():
            logger.info(f"  {row['industry']}: {row['stocks_with_data']}/{row['total_stocks']} 只股票有数据")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 验证导入结果失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🚀 开始导入样本股票数据")
    logger.info("=" * 60)
    
    try:
        # 步骤1: 清理数据库
        logger.info("📋 步骤1: 清理数据库")
        if not clean_database():
            logger.error("❌ 数据库清理失败，退出")
            return False
        
        # 步骤2: 创建样本股票列表
        logger.info("📋 步骤2: 创建样本股票列表")
        stock_list = get_sample_stock_list()
        
        if stock_list.empty:
            logger.error("❌ 无法创建股票列表，退出")
            return False
        
        # 步骤3: 导入股票基础信息
        logger.info("📋 步骤3: 导入股票基础信息")
        basic_count = import_stock_basic_info(stock_list)
        
        if basic_count == 0:
            logger.error("❌ 股票基础信息导入失败，退出")
            return False
        
        # 步骤4: 导入股票日线数据
        logger.info("📋 步骤4: 导入股票日线数据")
        success_count, total_records = import_stock_daily_data(stock_list, days=60)
        
        if success_count == 0:
            logger.error("❌ 股票日线数据导入失败，退出")
            return False
        
        # 步骤5: 验证导入结果
        logger.info("📋 步骤5: 验证导入结果")
        verify_import_results()
        
        logger.success("🎉 样本股票数据导入完成！")
        logger.info("=" * 60)
        logger.info("📊 导入总结:")
        logger.info(f"  股票基础信息: {basic_count} 只")
        logger.info(f"  日线数据股票: {success_count} 只")
        logger.info(f"  日线数据记录: {total_records} 条")
        logger.info(f"  平均每股记录: {total_records/max(success_count,1):.1f} 条")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据导入任务失败: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
