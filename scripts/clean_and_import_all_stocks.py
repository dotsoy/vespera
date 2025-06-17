#!/usr/bin/env python3
"""
清理数据库模拟数据并录入全A股真实数据
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
from src.data_sources.akshare_data_source import AkShareDataSource
from src.data_sources.base_data_source import DataRequest, DataType

logger = get_logger("clean_and_import")


def clean_existing_data():
    """清理数据库中的现有数据"""
    try:
        logger.info("🧹 开始清理数据库中的现有数据")

        db = get_db_manager()

        # 检查现有数据
        try:
            check_query = """
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT ts_code) as total_stocks,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date
            FROM stock_daily_quotes
            """

            existing_data = db.execute_postgres_query(check_query)
            logger.info(f"现有数据统计: {existing_data.iloc[0].to_dict()}")
        except Exception as e:
            logger.info(f"检查现有数据时出错: {e}")

        # 使用 pandas 的方式清理数据 (通过创建空表)
        logger.info("清理股票日线数据...")
        try:
            # 创建空的 DataFrame 并覆盖表
            empty_df = pd.DataFrame(columns=['ts_code', 'trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'vol', 'pct_chg'])
            empty_df.to_sql('stock_daily_quotes', db.postgres_engine, if_exists='replace', index=False)
            logger.info("✅ 股票日线数据表已清理")
        except Exception as e:
            logger.warning(f"清理日线数据时出错: {e}")

        # 清理股票基础信息
        logger.info("清理股票基础信息...")
        try:
            empty_basic = pd.DataFrame(columns=['ts_code', 'symbol', 'name', 'area', 'industry', 'market', 'list_date', 'list_status'])
            empty_basic.to_sql('stock_basic', db.postgres_engine, if_exists='replace', index=False)
            logger.info("✅ 股票基础信息表已清理")
        except Exception as e:
            logger.warning(f"清理基础信息时出错: {e}")

        logger.success("✅ 数据清理完成")
        return True

    except Exception as e:
        logger.error(f"❌ 数据清理失败: {e}")
        return False


def get_all_a_stocks():
    """获取全A股股票列表"""
    try:
        logger.info("📋 获取全A股股票列表")

        akshare_client = AkShareDataSource()

        # 初始化数据源
        if not akshare_client.initialize():
            logger.error("❌ AkShare数据源初始化失败")
            return pd.DataFrame()

        # 获取所有A股股票基础信息
        request = DataRequest(data_type=DataType.STOCK_BASIC)
        response = akshare_client.fetch_data(request)

        if not response.success or response.data.empty:
            logger.error("❌ 无法获取股票基础信息")
            return pd.DataFrame()

        stock_basic = response.data

        # 过滤A股 (排除退市、ST等)
        a_stocks = stock_basic[
            (stock_basic['market'].isin(['深交所主板', '创业板', '上交所主板', '科创板', '北交所']))
        ].copy()

        # 进一步过滤，排除退市股票
        a_stocks = a_stocks[
            (~a_stocks['name'].str.contains('ST', na=False)) &
            (~a_stocks['name'].str.contains('退', na=False)) &
            (a_stocks['list_status'] == 'L')  # 上市状态
        ].copy()

        logger.info(f"✅ 获取到 {len(a_stocks)} 只A股股票")
        return a_stocks

    except Exception as e:
        logger.error(f"❌ 获取股票列表失败: {e}")
        return pd.DataFrame()


def import_stock_basic_info(stock_basic_df: pd.DataFrame):
    """导入股票基础信息"""
    try:
        logger.info("📊 导入股票基础信息")
        
        db = get_db_manager()
        
        # 准备数据
        stock_data = stock_basic_df[[
            'ts_code', 'symbol', 'name', 'area', 'industry', 
            'market', 'list_date', 'list_status'
        ]].copy()
        
        # 批量插入
        success_count = 0
        batch_size = 100
        
        for i in range(0, len(stock_data), batch_size):
            batch = stock_data.iloc[i:i+batch_size]
            
            try:
                # 使用 pandas to_sql 方法批量插入
                batch.to_sql(
                    'stock_basic', 
                    db.postgres_engine, 
                    if_exists='append', 
                    index=False,
                    method='multi'
                )
                success_count += len(batch)
                logger.info(f"已导入 {success_count}/{len(stock_data)} 只股票基础信息")
                
            except Exception as e:
                logger.error(f"批量导入失败: {e}")
                continue
        
        logger.success(f"✅ 股票基础信息导入完成，成功导入 {success_count} 只股票")
        return success_count
        
    except Exception as e:
        logger.error(f"❌ 导入股票基础信息失败: {e}")
        return 0


def import_stock_daily_data(stock_list: pd.DataFrame, days: int = 60):
    """导入股票日线数据"""
    try:
        logger.info(f"📈 开始导入股票日线数据 (最近{days}天)")

        akshare_client = AkShareDataSource()
        db = get_db_manager()

        # 初始化数据源
        if not akshare_client.initialize():
            logger.error("❌ AkShare数据源初始化失败")
            return 0, 0

        # 计算日期范围
        end_date = datetime.now()
        start_date = datetime.now() - timedelta(days=days*2)  # 多取一些天数确保有足够交易日

        logger.info(f"数据日期范围: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")

        total_stocks = len(stock_list)
        success_count = 0
        error_count = 0
        total_records = 0

        # 分批处理股票
        batch_size = 20  # 每批处理20只股票，AkShare频率限制较严

        for batch_start in range(0, total_stocks, batch_size):
            batch_end = min(batch_start + batch_size, total_stocks)
            batch_stocks = stock_list.iloc[batch_start:batch_end]

            logger.info(f"处理第 {batch_start//batch_size + 1} 批股票 ({batch_start+1}-{batch_end}/{total_stocks})")

            batch_data = []

            for _, stock in batch_stocks.iterrows():
                ts_code = stock['ts_code']

                try:
                    # 获取股票日线数据
                    request = DataRequest(
                        data_type=DataType.DAILY_QUOTES,
                        symbol=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    response = akshare_client.fetch_data(request)

                    if response.success and not response.data.empty:
                        daily_data = response.data
                        # 只保留最近的交易日
                        daily_data = daily_data.sort_values('trade_date').tail(days)
                        batch_data.append(daily_data)
                        success_count += 1
                        total_records += len(daily_data)

                        if success_count % 10 == 0:
                            logger.info(f"已处理 {success_count} 只股票，累计 {total_records} 条记录")
                    else:
                        logger.warning(f"股票 {ts_code} 无数据")
                        error_count += 1

                    # 控制API调用频率，AkShare需要更长间隔
                    time.sleep(0.2)

                except Exception as e:
                    logger.error(f"获取股票 {ts_code} 数据失败: {e}")
                    error_count += 1
                    continue
            
            # 批量插入数据库
            if batch_data:
                try:
                    combined_batch = pd.concat(batch_data, ignore_index=True)
                    
                    # 插入数据库
                    combined_batch.to_sql(
                        'stock_daily_quotes',
                        db.postgres_engine,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    
                    logger.info(f"✅ 第 {batch_start//batch_size + 1} 批数据插入完成，{len(combined_batch)} 条记录")
                    
                except Exception as e:
                    logger.error(f"❌ 第 {batch_start//batch_size + 1} 批数据插入失败: {e}")
            
            # 批次间休息
            time.sleep(1)
        
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
            COUNT(DISTINCT market) as markets,
            COUNT(DISTINCT industry) as industries
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
            MAX(trade_date) as latest_date,
            AVG(vol) as avg_volume
        FROM stock_daily_quotes
        """
        
        daily_stats = db.execute_postgres_query(daily_query)
        logger.info(f"日线数据统计: {daily_stats.iloc[0].to_dict()}")
        
        # 检查数据完整性
        completeness_query = """
        SELECT 
            sb.market,
            COUNT(sb.ts_code) as total_stocks,
            COUNT(DISTINCT sdq.ts_code) as stocks_with_data,
            ROUND(COUNT(DISTINCT sdq.ts_code) * 100.0 / COUNT(sb.ts_code), 2) as data_completeness_pct
        FROM stock_basic sb
        LEFT JOIN stock_daily_quotes sdq ON sb.ts_code = sdq.ts_code
        GROUP BY sb.market
        ORDER BY data_completeness_pct DESC
        """
        
        completeness = db.execute_postgres_query(completeness_query)
        logger.info("各市场数据完整性:")
        for _, row in completeness.iterrows():
            logger.info(f"  {row['market']}: {row['stocks_with_data']}/{row['total_stocks']} ({row['data_completeness_pct']}%)")
        
        # 检查最新数据日期
        latest_query = """
        SELECT 
            trade_date,
            COUNT(*) as stock_count
        FROM stock_daily_quotes
        WHERE trade_date >= (SELECT MAX(trade_date) FROM stock_daily_quotes)
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT 5
        """
        
        latest_data = db.execute_postgres_query(latest_query)
        logger.info("最新交易日数据:")
        for _, row in latest_data.iterrows():
            logger.info(f"  {row['trade_date']}: {row['stock_count']} 只股票")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 验证导入结果失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🚀 开始清理数据库并导入全A股数据")
    logger.info("=" * 60)
    
    try:
        # 步骤1: 清理现有数据
        logger.info("📋 步骤1: 清理现有数据")
        if not clean_existing_data():
            logger.error("❌ 数据清理失败，退出")
            return False
        
        # 步骤2: 获取全A股股票列表
        logger.info("📋 步骤2: 获取全A股股票列表")
        stock_list = get_all_a_stocks()
        
        if stock_list.empty:
            logger.error("❌ 无法获取股票列表，退出")
            return False
        
        logger.info(f"✅ 获取到 {len(stock_list)} 只A股股票")
        
        # 步骤3: 导入股票基础信息
        logger.info("📋 步骤3: 导入股票基础信息")
        basic_count = import_stock_basic_info(stock_list)
        
        if basic_count == 0:
            logger.error("❌ 股票基础信息导入失败，退出")
            return False
        
        # 步骤4: 导入股票日线数据
        logger.info("📋 步骤4: 导入股票日线数据")
        
        # 先导入少量股票测试
        test_stocks = stock_list.head(100)  # 先导入100只股票测试
        logger.info(f"🧪 测试模式: 先导入 {len(test_stocks)} 只股票")
        
        success_count, total_records = import_stock_daily_data(test_stocks, days=60)
        
        if success_count == 0:
            logger.error("❌ 股票日线数据导入失败，退出")
            return False
        
        # 步骤5: 验证导入结果
        logger.info("📋 步骤5: 验证导入结果")
        if not verify_import_results():
            logger.warning("⚠️ 导入结果验证有问题")
        
        # 询问是否继续导入剩余股票
        remaining_stocks = len(stock_list) - len(test_stocks)
        if remaining_stocks > 0:
            logger.info(f"🤔 测试导入完成，还有 {remaining_stocks} 只股票待导入")
            logger.info("如需导入全部股票，请修改脚本中的 test_stocks = stock_list.head(100) 为 test_stocks = stock_list")
        
        logger.success("🎉 数据导入任务完成！")
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
