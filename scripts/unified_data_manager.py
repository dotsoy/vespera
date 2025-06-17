#!/usr/bin/env python3
"""
统一数据管理脚本 - 简化版本
整合了原有的多个数据导入和管理脚本功能
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import argparse

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("unified_data_manager")


def setup_database():
    """初始化数据库"""
    logger.info("🔧 初始化数据库...")
    
    try:
        from src.utils.database import get_db_manager
        db_manager = get_db_manager()
        
        # 测试连接
        connections = db_manager.test_connections()
        if not connections.get('postgres', False):
            logger.error("❌ PostgreSQL连接失败")
            return False
            
        logger.success("✅ 数据库连接正常")
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据库初始化失败: {e}")
        return False


def import_stock_basic_info():
    """导入股票基础信息"""
    logger.info("📊 导入股票基础信息...")
    
    try:
        from src.data_sources.akshare_data_source import AkShareDataSource
        from src.data_sources.base_data_source import DataRequest, DataType
        from src.utils.database import get_db_manager
        
        # 初始化数据源
        client = AkShareDataSource()
        if not client.initialize():
            logger.error("❌ AkShare初始化失败")
            return False
            
        # 获取股票基础信息
        request = DataRequest(data_type=DataType.STOCK_BASIC)
        response = client.fetch_data(request)
        
        if not response.success or response.data.empty:
            logger.error("❌ 获取股票基础信息失败")
            return False
            
        # 保存到数据库
        db_manager = get_db_manager()
        stock_basic_df = response.data
        
        insert_count = 0
        for _, row in stock_basic_df.iterrows():
            try:
                insert_sql = """
                INSERT INTO stock_basic (ts_code, symbol, name, area, industry, market, list_date, is_hs)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ts_code) DO UPDATE SET
                name = EXCLUDED.name,
                area = EXCLUDED.area,
                industry = EXCLUDED.industry,
                market = EXCLUDED.market,
                list_date = EXCLUDED.list_date,
                is_hs = EXCLUDED.is_hs
                """
                db_manager.execute_postgres_query(insert_sql, params=(
                    row['ts_code'], row['symbol'], row['name'], row['area'],
                    row['industry'], row['market'], row['list_date'], row['is_hs']
                ))
                insert_count += 1
            except Exception as e:
                logger.warning(f"⚠️ 插入股票 {row['ts_code']} 失败: {e}")
        
        logger.success(f"✅ 成功导入 {insert_count} 只股票基础信息")
        return True
        
    except Exception as e:
        logger.error(f"❌ 导入股票基础信息失败: {e}")
        return False


def import_sample_data(stock_count=10, days=30):
    """导入样本数据"""
    logger.info(f"📈 导入样本数据（{stock_count}只股票，{days}天数据）...")
    
    try:
        from src.data_sources.akshare_data_source import AkShareDataSource
        from src.data_sources.base_data_source import DataRequest, DataType
        from src.utils.database import get_db_manager
        import time
        
        # 初始化数据源
        client = AkShareDataSource()
        if not client.initialize():
            logger.error("❌ AkShare初始化失败")
            return False
            
        # 获取股票列表
        db_manager = get_db_manager()
        stock_query = f"SELECT ts_code, name FROM stock_basic WHERE is_hs = 'Y' LIMIT {stock_count}"
        stock_df = db_manager.execute_postgres_query(stock_query)
        
        if stock_df.empty:
            logger.error("❌ 数据库中无股票基础信息，请先运行 --import-basic")
            return False
        
        # 计算日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        total_records = 0
        success_count = 0
        
        for _, stock in stock_df.iterrows():
            ts_code = stock['ts_code']
            
            try:
                logger.info(f"获取 {ts_code} ({stock['name']}) 的数据...")
                
                # 获取日线数据
                request = DataRequest(
                    data_type=DataType.DAILY_QUOTES,
                    symbol=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                response = client.fetch_data(request)
                
                if response.success and not response.data.empty:
                    daily_data = response.data
                    
                    # 保存到数据库
                    insert_count = 0
                    for _, row in daily_data.iterrows():
                        try:
                            insert_sql = """
                            INSERT INTO stock_daily_quotes
                            (ts_code, trade_date, open_price, high_price, low_price, close_price,
                             pre_close, change_amount, pct_chg, vol, amount)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                            open_price = EXCLUDED.open_price,
                            high_price = EXCLUDED.high_price,
                            low_price = EXCLUDED.low_price,
                            close_price = EXCLUDED.close_price,
                            pre_close = EXCLUDED.pre_close,
                            change_amount = EXCLUDED.change_amount,
                            pct_chg = EXCLUDED.pct_chg,
                            vol = EXCLUDED.vol,
                            amount = EXCLUDED.amount
                            """
                            db_manager.execute_postgres_query(insert_sql, params=(
                                row['ts_code'], row['trade_date'], row['open_price'], row['high_price'],
                                row['low_price'], row['close_price'], row['pre_close'], row['change_amount'],
                                row['pct_chg'], row['vol'], row['amount']
                            ))
                            insert_count += 1
                        except Exception as insert_e:
                            logger.warning(f"⚠️ 插入 {ts_code} 数据失败: {insert_e}")
                    
                    total_records += insert_count
                    success_count += 1
                    logger.success(f"✅ {ts_code} 导入 {insert_count} 条记录")
                else:
                    logger.warning(f"⚠️ {ts_code} 无数据")
                
                # 控制API调用频率
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ {ts_code} 数据获取失败: {e}")
        
        logger.success(f"✅ 样本数据导入完成：{success_count}/{len(stock_df)} 只股票，共 {total_records} 条记录")
        return True
        
    except Exception as e:
        logger.error(f"❌ 导入样本数据失败: {e}")
        return False


def validate_data():
    """验证数据质量"""
    logger.info("🔍 验证数据质量...")
    
    try:
        from src.utils.database import get_db_manager
        
        db_manager = get_db_manager()
        
        # 检查股票基础信息
        basic_query = "SELECT COUNT(*) as count FROM stock_basic"
        basic_result = db_manager.execute_postgres_query(basic_query)
        basic_count = basic_result['count'].iloc[0] if not basic_result.empty else 0
        
        # 检查日线行情数据
        quotes_query = "SELECT COUNT(*) as count FROM stock_daily_quotes"
        quotes_result = db_manager.execute_postgres_query(quotes_query)
        quotes_count = quotes_result['count'].iloc[0] if not quotes_result.empty else 0
        
        # 检查最新数据日期
        latest_query = "SELECT MAX(trade_date) as latest_date FROM stock_daily_quotes"
        latest_result = db_manager.execute_postgres_query(latest_query)
        latest_date = latest_result['latest_date'].iloc[0] if not latest_result.empty else None
        
        logger.info(f"📊 数据统计:")
        logger.info(f"  股票基础信息: {basic_count:,} 条")
        logger.info(f"  日线行情数据: {quotes_count:,} 条")
        logger.info(f"  最新数据日期: {latest_date}")
        
        if basic_count > 0 and quotes_count > 0:
            logger.success("✅ 数据验证通过")
            return True
        else:
            logger.error("❌ 数据验证失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 数据验证失败: {e}")
        return False


def clear_data():
    """清理数据"""
    logger.warning("⚠️ 清理数据...")
    
    try:
        from src.utils.database import get_db_manager
        
        db_manager = get_db_manager()
        
        # 清理日线行情数据
        db_manager.execute_postgres_query("DELETE FROM stock_daily_quotes")
        logger.info("✅ 已清理日线行情数据")
        
        # 清理股票基础信息
        db_manager.execute_postgres_query("DELETE FROM stock_basic")
        logger.info("✅ 已清理股票基础信息")
        
        logger.success("✅ 数据清理完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据清理失败: {e}")
        return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="统一数据管理工具")
    parser.add_argument("--setup-db", action="store_true", help="初始化数据库")
    parser.add_argument("--import-basic", action="store_true", help="导入股票基础信息")
    parser.add_argument("--import-sample", action="store_true", help="导入样本数据")
    parser.add_argument("--stock-count", type=int, default=10, help="样本股票数量")
    parser.add_argument("--days", type=int, default=30, help="历史数据天数")
    parser.add_argument("--validate", action="store_true", help="验证数据质量")
    parser.add_argument("--clear", action="store_true", help="清理数据")
    parser.add_argument("--all", action="store_true", help="执行完整流程")
    
    args = parser.parse_args()
    
    logger.info("🚀 启明星统一数据管理工具")
    logger.info("=" * 50)
    
    success = True
    
    if args.all:
        # 执行完整流程
        logger.info("📋 执行完整数据管理流程...")
        success &= setup_database()
        success &= import_stock_basic_info()
        success &= import_sample_data(args.stock_count, args.days)
        success &= validate_data()
    else:
        # 执行指定操作
        if args.setup_db:
            success &= setup_database()
        if args.import_basic:
            success &= import_stock_basic_info()
        if args.import_sample:
            success &= import_sample_data(args.stock_count, args.days)
        if args.validate:
            success &= validate_data()
        if args.clear:
            success &= clear_data()
    
    if success:
        logger.success("🎉 数据管理操作完成！")
    else:
        logger.error("❌ 数据管理操作失败！")
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
