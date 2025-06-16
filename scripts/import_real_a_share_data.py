#!/usr/bin/env python3
"""
真实A股数据导入脚本
使用Tushare API导入真实的A股数据
1. 导入全市场股票基础信息
2. 导入6月13日全市场日线数据
"""
import sys
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.database import get_db_manager
from src.data_sources.tushare_client import TushareClient

logger = get_logger("real_a_share_import")


def import_stock_basic():
    """导入股票基础信息"""
    logger.info("🔄 开始导入A股基础信息...")
    
    try:
        # 初始化客户端
        client = TushareClient()
        db_manager = get_db_manager()
        
        # 获取股票基础信息
        logger.info("📡 从Tushare获取股票基础信息...")
        stock_basic_df = client.get_stock_basic()
        
        if stock_basic_df.empty:
            logger.error("❌ 未获取到股票基础信息")
            return False
        
        logger.info(f"✅ 获取到 {len(stock_basic_df)} 只股票基础信息")
        
        # 显示样本数据
        logger.info("📋 样本数据:")
        for _, row in stock_basic_df.head(5).iterrows():
            logger.info(f"  {row['ts_code']} - {row['name']} - {row['market']} - {row['industry']}")
        
        # 保存到数据库
        logger.info("💾 保存到数据库...")
        db_manager.insert_dataframe_to_postgres(
            stock_basic_df, 'stock_basic', if_exists='replace'
        )
        
        logger.info("✅ 股票基础信息导入完成!")
        return True
        
    except Exception as e:
        logger.error(f"❌ 导入股票基础信息失败: {e}")
        return False


def import_daily_quotes(trade_date='20240613'):
    """导入指定日期的日线行情数据"""
    logger.info(f"🔄 开始导入 {trade_date} 日线行情数据...")
    
    try:
        # 初始化客户端
        client = TushareClient()
        db_manager = get_db_manager()
        
        # 获取股票列表
        logger.info("📋 获取股票列表...")
        stock_query = """
        SELECT ts_code FROM stock_basic 
        WHERE market IN ('主板', '创业板', '科创板')
        AND is_hs = 'Y'
        ORDER BY ts_code
        """
        stock_df = db_manager.execute_postgres_query(stock_query)
        
        if stock_df.empty:
            logger.error("❌ 数据库中无股票基础信息，请先导入股票基础信息")
            return False
        
        stock_list = stock_df['ts_code'].tolist()
        logger.info(f"📊 准备导入 {len(stock_list)} 只股票的 {trade_date} 行情数据")
        
        # 批量获取行情数据
        logger.info("📡 从Tushare批量获取行情数据...")
        quotes_df = client.batch_get_daily_quotes(stock_list, trade_date)
        
        if quotes_df.empty:
            logger.warning(f"⚠️ {trade_date} 未获取到行情数据（可能是非交易日）")
            return False
        
        logger.info(f"✅ 获取到 {len(quotes_df)} 条行情记录")
        
        # 显示样本数据
        logger.info("📋 样本数据:")
        for _, row in quotes_df.head(5).iterrows():
            logger.info(f"  {row['ts_code']} - 开盘:{row['open_price']:.2f} 收盘:{row['close_price']:.2f} 涨跌幅:{row['pct_chg']:.2f}%")
        
        # 保存到数据库
        logger.info("💾 保存到数据库...")
        db_manager.insert_dataframe_to_postgres(
            quotes_df, 'stock_daily_quotes', if_exists='append'
        )
        
        logger.info(f"✅ {trade_date} 日线行情数据导入完成!")
        return True
        
    except Exception as e:
        logger.error(f"❌ 导入日线行情数据失败: {e}")
        return False


def verify_import():
    """验证导入结果"""
    logger.info("🔍 验证导入结果...")
    
    try:
        db_manager = get_db_manager()
        
        # 检查股票基础信息
        stock_count_query = "SELECT COUNT(*) as count FROM stock_basic"
        stock_count_result = db_manager.execute_postgres_query(stock_count_query)
        stock_count = stock_count_result['count'].iloc[0] if not stock_count_result.empty else 0
        
        # 检查日线行情数据
        quotes_count_query = "SELECT COUNT(*) as count FROM stock_daily_quotes"
        quotes_count_result = db_manager.execute_postgres_query(quotes_count_query)
        quotes_count = quotes_count_result['count'].iloc[0] if not quotes_count_result.empty else 0
        
        # 检查最新数据日期
        latest_date_query = "SELECT MAX(trade_date) as latest_date FROM stock_daily_quotes"
        latest_date_result = db_manager.execute_postgres_query(latest_date_query)
        latest_date = latest_date_result['latest_date'].iloc[0] if not latest_date_result.empty else None
        
        # 显示结果
        logger.info("📊 导入结果统计:")
        logger.info(f"  股票基础信息: {stock_count:,} 条")
        logger.info(f"  日线行情数据: {quotes_count:,} 条")
        logger.info(f"  最新数据日期: {latest_date}")
        
        # 显示市场分布
        if stock_count > 0:
            market_query = """
            SELECT market, COUNT(*) as count 
            FROM stock_basic 
            GROUP BY market 
            ORDER BY count DESC
            """
            market_result = db_manager.execute_postgres_query(market_query)
            
            logger.info("📈 市场分布:")
            for _, row in market_result.iterrows():
                logger.info(f"  {row['market']}: {row['count']:,} 只")
        
        # 显示行业分布（前10）
        if stock_count > 0:
            industry_query = """
            SELECT industry, COUNT(*) as count 
            FROM stock_basic 
            GROUP BY industry 
            ORDER BY count DESC 
            LIMIT 10
            """
            industry_result = db_manager.execute_postgres_query(industry_query)
            
            logger.info("🏭 主要行业分布:")
            for _, row in industry_result.iterrows():
                logger.info(f"  {row['industry']}: {row['count']:,} 只")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 验证导入结果失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🚀 开始真实A股数据导入")
    logger.info("=" * 60)
    
    success_count = 0
    total_tasks = 3
    
    # 1. 导入股票基础信息
    logger.info("\n📋 任务 1/3: 导入股票基础信息")
    logger.info("-" * 40)
    if import_stock_basic():
        success_count += 1
        logger.info("✅ 任务 1 完成")
    else:
        logger.error("❌ 任务 1 失败")
    
    # 2. 导入6月13日日线数据
    logger.info("\n📈 任务 2/3: 导入6月13日日线数据")
    logger.info("-" * 40)
    if import_daily_quotes('20240613'):
        success_count += 1
        logger.info("✅ 任务 2 完成")
    else:
        logger.error("❌ 任务 2 失败")
    
    # 3. 验证导入结果
    logger.info("\n🔍 任务 3/3: 验证导入结果")
    logger.info("-" * 40)
    if verify_import():
        success_count += 1
        logger.info("✅ 任务 3 完成")
    else:
        logger.error("❌ 任务 3 失败")
    
    # 总结
    logger.info("\n" + "=" * 60)
    logger.info("📊 导入任务总结")
    logger.info("=" * 60)
    logger.info(f"完成任务: {success_count}/{total_tasks}")
    
    if success_count == total_tasks:
        logger.info("🎉 所有任务完成！真实A股数据导入成功")
        logger.info("\n🚀 下一步操作:")
        logger.info("1. 访问Dashboard: http://localhost:8505")
        logger.info("2. 进入'数据管理'页面查看导入结果")
        logger.info("3. 在'策略分析'页面选择股票进行分析")
        return True
    else:
        logger.error(f"⚠️ {total_tasks - success_count} 个任务失败")
        logger.info("\n🔧 故障排除:")
        logger.info("1. 检查Tushare Token是否配置正确")
        logger.info("2. 检查数据库连接是否正常")
        logger.info("3. 检查网络连接是否稳定")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
