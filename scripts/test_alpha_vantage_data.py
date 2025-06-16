#!/usr/bin/env python3
"""
测试Alpha Vantage数据拉取功能
"""
import sys
from pathlib import Path
import os

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 加载.env文件
from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from src.utils.logger import get_logger
from src.data_sources.alpha_vantage_client import AlphaVantageClient

logger = get_logger("test_alpha_vantage_data")


def test_stock_basic_info():
    """测试股票基础信息获取"""
    logger.info("🔍 测试股票基础信息获取...")
    
    try:
        client = AlphaVantageClient()
        
        # 测试获取几只热门美股的基础信息
        test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
        
        all_basic_info = []
        
        for symbol in test_symbols:
            try:
                logger.info(f"📡 获取 {symbol} 基础信息...")
                basic_info = client.get_stock_basic(symbol)
                
                if not basic_info.empty:
                    logger.info(f"✅ {symbol}: {basic_info['name'].iloc[0]} - {basic_info['industry'].iloc[0]}")
                    all_basic_info.append(basic_info)
                else:
                    logger.warning(f"⚠️ {symbol}: 未获取到基础信息")
                
                # API频率限制
                import time
                time.sleep(12)  # Alpha Vantage免费版限制
                
            except Exception as e:
                logger.error(f"❌ {symbol}: 获取基础信息失败 - {e}")
        
        if all_basic_info:
            import pandas as pd
            combined_df = pd.concat(all_basic_info, ignore_index=True)
            logger.info(f"✅ 总计获取到 {len(combined_df)} 只股票基础信息")
            
            # 显示详细信息
            logger.info("📋 股票基础信息详情:")
            for _, row in combined_df.iterrows():
                logger.info(f"  {row['symbol']}: {row['name']} ({row['industry']}) - 市值: {row['market_cap']}")
            
            return combined_df
        else:
            logger.error("❌ 未获取到任何股票基础信息")
            return None
            
    except Exception as e:
        logger.error(f"❌ 测试股票基础信息失败: {e}")
        return None


def test_daily_quotes():
    """测试日线行情数据获取"""
    logger.info("🔍 测试日线行情数据获取...")
    
    try:
        client = AlphaVantageClient()
        
        # 测试获取AAPL的日线数据
        symbol = 'AAPL'
        logger.info(f"📡 获取 {symbol} 日线行情数据...")
        
        quotes_df = client.get_daily_quotes(symbol, outputsize='compact')
        
        if not quotes_df.empty:
            logger.info(f"✅ {symbol}: 获取到 {len(quotes_df)} 条日线数据")
            
            # 显示最新几天的数据
            logger.info("📋 最新行情数据:")
            latest_data = quotes_df.tail(5)
            for _, row in latest_data.iterrows():
                logger.info(f"  {row['trade_date']}: 开盘 ${row['open_price']:.2f}, 收盘 ${row['close_price']:.2f}, 涨跌幅 {row['pct_chg']:.2f}%")
            
            return quotes_df
        else:
            logger.error(f"❌ {symbol}: 未获取到日线数据")
            return None
            
    except Exception as e:
        logger.error(f"❌ 测试日线行情数据失败: {e}")
        return None


def test_batch_quotes():
    """测试批量获取行情数据"""
    logger.info("🔍 测试批量获取行情数据...")
    
    try:
        client = AlphaVantageClient()
        
        # 测试批量获取几只股票的行情数据
        symbols = ['AAPL', 'MSFT', 'GOOGL']  # 限制数量以避免API限制
        
        logger.info(f"📡 批量获取 {len(symbols)} 只股票的行情数据...")
        batch_quotes = client.batch_get_daily_quotes(symbols, outputsize='compact')
        
        if not batch_quotes.empty:
            logger.info(f"✅ 批量获取成功: 总计 {len(batch_quotes)} 条行情数据")
            
            # 按股票分组显示
            for symbol in symbols:
                symbol_data = batch_quotes[batch_quotes['ts_code'] == symbol]
                if not symbol_data.empty:
                    latest = symbol_data.iloc[-1]
                    logger.info(f"  {symbol}: {len(symbol_data)} 条数据, 最新价格 ${latest['close_price']:.2f}")
            
            return batch_quotes
        else:
            logger.error("❌ 批量获取失败，未获取到任何数据")
            return None
            
    except Exception as e:
        logger.error(f"❌ 测试批量获取失败: {e}")
        return None


def save_to_database(basic_info_df, quotes_df):
    """保存数据到数据库"""
    logger.info("💾 测试保存数据到数据库...")
    
    try:
        from src.utils.database import get_db_manager
        db_manager = get_db_manager()
        
        saved_count = 0
        
        # 保存股票基础信息
        if basic_info_df is not None and not basic_info_df.empty:
            try:
                # 使用SQL直接插入
                for _, row in basic_info_df.iterrows():
                    insert_sql = """
                    INSERT INTO stock_basic (ts_code, symbol, name, area, industry, market, list_date, is_hs) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ts_code) DO UPDATE SET
                    name = EXCLUDED.name,
                    area = EXCLUDED.area,
                    industry = EXCLUDED.industry,
                    market = EXCLUDED.market
                    """
                    db_manager.execute_postgres_query(insert_sql, params=(
                        row['ts_code'], row['symbol'], row['name'], row.get('country', 'US'),
                        row['industry'], 'NASDAQ', '2000-01-01', 'N'  # 美股标记
                    ))
                    saved_count += 1
                
                logger.info(f"✅ 保存 {saved_count} 条股票基础信息到数据库")
            except Exception as e:
                logger.error(f"❌ 保存股票基础信息失败: {e}")
        
        # 保存行情数据
        if quotes_df is not None and not quotes_df.empty:
            try:
                quotes_saved = 0
                for _, row in quotes_df.iterrows():
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
                    vol = EXCLUDED.vol
                    """
                    db_manager.execute_postgres_query(insert_sql, params=(
                        row['ts_code'], row['trade_date'], row['open_price'], row['high_price'],
                        row['low_price'], row['close_price'], row.get('pre_close', 0), 
                        row.get('change_amount', 0), row.get('pct_chg', 0), row['vol'], 0
                    ))
                    quotes_saved += 1
                
                logger.info(f"✅ 保存 {quotes_saved} 条行情数据到数据库")
            except Exception as e:
                logger.error(f"❌ 保存行情数据失败: {e}")
        
        return saved_count > 0
        
    except Exception as e:
        logger.error(f"❌ 数据库保存失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🚀 Alpha Vantage数据拉取测试")
    logger.info("=" * 50)
    
    # 检查API Key
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        logger.error("❌ ALPHA_VANTAGE_API_KEY环境变量未设置")
        return False
    
    logger.info(f"🔧 使用API Key: {api_key[:5]}...{api_key[-5:]}")
    
    results = {}
    
    # 1. 测试股票基础信息
    logger.info("\n📋 任务 1/4: 测试股票基础信息获取")
    logger.info("-" * 40)
    basic_info_df = test_stock_basic_info()
    results['basic_info'] = basic_info_df is not None
    
    # 2. 测试日线行情数据
    logger.info("\n📋 任务 2/4: 测试日线行情数据获取")
    logger.info("-" * 40)
    quotes_df = test_daily_quotes()
    results['daily_quotes'] = quotes_df is not None
    
    # 3. 测试批量获取
    logger.info("\n📋 任务 3/4: 测试批量获取行情数据")
    logger.info("-" * 40)
    batch_quotes_df = test_batch_quotes()
    results['batch_quotes'] = batch_quotes_df is not None
    
    # 4. 测试数据库保存
    logger.info("\n📋 任务 4/4: 测试数据库保存")
    logger.info("-" * 40)
    db_success = save_to_database(basic_info_df, quotes_df)
    results['database_save'] = db_success
    
    # 总结
    logger.info("\n" + "=" * 50)
    logger.info("📊 测试结果总结")
    logger.info("=" * 50)
    
    success_count = sum(results.values())
    total_tasks = len(results)
    
    for task, success in results.items():
        status = "✅ 成功" if success else "❌ 失败"
        logger.info(f"{task}: {status}")
    
    logger.info(f"\n总体结果: {success_count}/{total_tasks} 任务成功")
    
    if success_count >= 2:  # 至少基础信息和行情数据成功
        logger.info("🎉 Alpha Vantage数据拉取测试基本成功！")
        logger.info("\n🚀 下一步操作:")
        logger.info("1. 访问Dashboard: http://localhost:8507")
        logger.info("2. 进入'数据管理'页面")
        logger.info("3. 选择'Alpha Vantage'数据源")
        logger.info("4. 测试数据更新功能")
        return True
    else:
        logger.error("⚠️ Alpha Vantage数据拉取测试失败")
        logger.info("\n🔧 故障排除:")
        logger.info("1. 检查API Key是否有效")
        logger.info("2. 检查网络连接")
        logger.info("3. 注意API频率限制")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
