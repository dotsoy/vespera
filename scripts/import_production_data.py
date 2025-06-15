#!/usr/bin/env python3
"""
生产数据导入脚本
使用AllTick和Alpha Vantage导入真实的股票数据
排除ST股票和北交所股票
"""
import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.stock_filter import StockFilter
from src.data_sources.base_data_source import DataRequest, DataType
from src.data_sources.alltick_data_source import AllTickDataSource
from src.data_sources.alpha_vantage_data_source import AlphaVantageDataSource
from src.data_sources.data_source_manager import DataSourceManager

logger = get_logger("import_production_data")

# 生产环境token
ALLTICK_TOKEN = "5d77b3af30d6b74b6bad3340996cb399-c-app"
ALPHA_VANTAGE_API_KEY = "3SHZ17DOQBH5X6IX"


def setup_production_data_sources():
    """配置生产数据源"""
    logger.info("🔧 配置生产数据源")
    
    try:
        # 创建数据源管理器
        manager = DataSourceManager()
        
        # 配置AllTick数据源
        logger.info("配置AllTick数据源...")
        alltick_config = {
            'priority': 1,
            'rate_limit': 100,
            'timeout': 30,
            'description': 'AllTick生产环境'
        }
        
        alltick_source = AllTickDataSource(ALLTICK_TOKEN, alltick_config)
        if alltick_source.initialize():
            manager.register_data_source(alltick_source)
            logger.success("✅ AllTick数据源配置成功")
        else:
            logger.error("❌ AllTick数据源初始化失败")
            return None
        
        # 配置Alpha Vantage数据源
        logger.info("配置Alpha Vantage数据源...")
        alpha_config = {
            'priority': 2,
            'rate_limit': 5,
            'timeout': 30,
            'description': 'Alpha Vantage生产环境'
        }
        
        alpha_source = AlphaVantageDataSource(ALPHA_VANTAGE_API_KEY, alpha_config)
        if alpha_source.initialize():
            manager.register_data_source(alpha_source)
            logger.success("✅ Alpha Vantage数据源配置成功")
        else:
            logger.warning("⚠️ Alpha Vantage数据源初始化失败，将仅使用AllTick")
        
        return manager
        
    except Exception as e:
        logger.error(f"❌ 数据源配置失败: {e}")
        return None


def get_stock_list(data_manager):
    """获取股票列表，排除ST股票和北交所"""
    logger.info("📋 获取股票列表")
    
    try:
        # 使用AllTick获取股票基础信息
        request = DataRequest(
            data_type=DataType.STOCK_BASIC,
            extra_params={
                'market': 'cn',
                'type': 'stock'
            }
        )
        
        response = data_manager.get_data(request)
        
        if not response.success:
            logger.error(f"❌ 获取股票列表失败: {response.error_message}")
            return pd.DataFrame()
        
        stock_data = response.data
        logger.info(f"获取到 {len(stock_data)} 只股票")

        # 使用股票过滤器进行过滤
        stock_filter = StockFilter()

        # 确定列名
        code_column = 'ts_code' if 'ts_code' in stock_data.columns else 'symbol'
        name_column = 'name' if 'name' in stock_data.columns else 'name'

        # 应用过滤器
        stock_data = stock_filter.filter_stock_list(
            stock_data,
            code_column=code_column,
            name_column=name_column
        )
        
        logger.success(f"✅ 最终股票列表: {len(stock_data)} 只")
        return stock_data
        
    except Exception as e:
        logger.error(f"❌ 获取股票列表失败: {e}")
        return pd.DataFrame()


def import_stock_basic_info(data_manager, stock_list):
    """导入股票基础信息"""
    logger.info("📊 导入股票基础信息")
    
    try:
        if stock_list.empty:
            logger.error("❌ 股票列表为空")
            return False
        
        # 这里应该将股票基础信息保存到数据库
        # 示例代码（需要根据实际数据库连接调整）:
        """
        from src.utils.database import get_db_manager
        
        db_manager = get_db_manager()
        
        # 保存到PostgreSQL
        db_manager.insert_dataframe_to_postgres(
            stock_list, 
            'stock_basic', 
            if_exists='replace'
        )
        """
        
        # 临时实现：保存到本地文件
        output_dir = project_root / 'data' / 'production'
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / 'stock_basic.csv'
        stock_list.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.success(f"✅ 股票基础信息已保存到: {output_file}")
        logger.info(f"共保存 {len(stock_list)} 只股票的基础信息")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 导入股票基础信息失败: {e}")
        return False


def import_historical_data(data_manager, stock_list, days=30):
    """导入历史数据"""
    logger.info(f"📈 导入最近 {days} 天的历史数据")
    
    try:
        if stock_list.empty:
            logger.error("❌ 股票列表为空")
            return False
        
        # 计算日期范围
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        logger.info(f"数据日期范围: {start_date} 到 {end_date}")
        
        # 限制股票数量以避免API限制
        max_stocks = 50  # 限制处理的股票数量
        if len(stock_list) > max_stocks:
            logger.warning(f"⚠️ 股票数量过多，仅处理前 {max_stocks} 只股票")
            stock_list = stock_list.head(max_stocks)
        
        all_data = []
        success_count = 0
        total_count = len(stock_list)
        
        for idx, (_, stock) in enumerate(stock_list.iterrows(), 1):
            stock_code = stock.get('ts_code', stock.get('symbol', ''))
            stock_name = stock.get('name', '')
            
            logger.info(f"[{idx}/{total_count}] 获取 {stock_code} ({stock_name}) 的历史数据")
            
            try:
                request = DataRequest(
                    data_type=DataType.DAILY_QUOTES,
                    symbol=stock_code,
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d')
                )
                
                response = data_manager.get_data(request)
                
                if response.success and not response.data.empty:
                    all_data.append(response.data)
                    success_count += 1
                    logger.success(f"✅ 获取成功，{len(response.data)} 条记录")
                else:
                    logger.warning(f"⚠️ 获取失败: {response.error_message}")
                
                # 控制请求频率
                time.sleep(0.6)  # AllTick限制
                
            except Exception as e:
                logger.warning(f"⚠️ 获取 {stock_code} 数据时出错: {e}")
                continue
        
        # 合并所有数据
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            
            # 保存数据
            output_dir = project_root / 'data' / 'production'
            output_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = output_dir / 'daily_quotes.csv'
            combined_data.to_csv(output_file, index=False, encoding='utf-8')
            
            logger.success(f"✅ 历史数据已保存到: {output_file}")
            logger.info(f"成功获取 {success_count}/{total_count} 只股票的数据")
            logger.info(f"总记录数: {len(combined_data)}")
            
            # 这里应该保存到数据库
            # 示例代码:
            """
            from src.utils.database import get_db_manager
            
            db_manager = get_db_manager()
            
            # 保存到PostgreSQL
            db_manager.insert_dataframe_to_postgres(
                combined_data, 
                'daily_quotes', 
                if_exists='append'
            )
            
            # 保存到ClickHouse（用于分析）
            db_manager.insert_dataframe_to_clickhouse(
                combined_data,
                'stock_quotes_daily'
            )
            """
            
            return True
        else:
            logger.error("❌ 未获取到任何历史数据")
            return False
        
    except Exception as e:
        logger.error(f"❌ 导入历史数据失败: {e}")
        return False


def validate_imported_data():
    """验证导入的数据"""
    logger.info("🔍 验证导入的数据")
    
    try:
        data_dir = project_root / 'data' / 'production'
        
        # 检查股票基础信息
        basic_file = data_dir / 'stock_basic.csv'
        if basic_file.exists():
            basic_data = pd.read_csv(basic_file)
            logger.success(f"✅ 股票基础信息: {len(basic_data)} 条记录")
            
            # 显示数据样例
            if not basic_data.empty:
                logger.info("股票基础信息样例:")
                print(basic_data.head().to_string())
        else:
            logger.warning("⚠️ 未找到股票基础信息文件")
        
        # 检查历史数据
        quotes_file = data_dir / 'daily_quotes.csv'
        if quotes_file.exists():
            quotes_data = pd.read_csv(quotes_file)
            logger.success(f"✅ 历史行情数据: {len(quotes_data)} 条记录")
            
            # 数据统计
            if not quotes_data.empty:
                unique_stocks = quotes_data['ts_code'].nunique() if 'ts_code' in quotes_data.columns else 0
                date_range = None
                if 'trade_date' in quotes_data.columns:
                    date_range = f"{quotes_data['trade_date'].min()} 到 {quotes_data['trade_date'].max()}"
                
                logger.info(f"涵盖股票数: {unique_stocks}")
                if date_range:
                    logger.info(f"日期范围: {date_range}")
                
                # 显示数据样例
                logger.info("历史数据样例:")
                print(quotes_data.head().to_string())
        else:
            logger.warning("⚠️ 未找到历史行情数据文件")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据验证失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🚀 开始导入生产数据")
    logger.info("=" * 80)
    
    logger.info("配置信息:")
    logger.info(f"  AllTick Token: {ALLTICK_TOKEN[:20]}...")
    logger.info(f"  Alpha Vantage Key: {ALPHA_VANTAGE_API_KEY[:10]}...")
    logger.info("  过滤条件: 排除ST股票和北交所")
    
    # 执行导入步骤
    steps = [
        ("配置生产数据源", lambda: setup_production_data_sources()),
        ("获取股票列表", None),  # 需要数据管理器参数
        ("导入股票基础信息", None),  # 需要参数
        ("导入历史数据", None),  # 需要参数
        ("验证导入数据", validate_imported_data)
    ]
    
    data_manager = None
    stock_list = pd.DataFrame()
    
    try:
        # 1. 配置数据源
        logger.info("\n🎯 步骤1: 配置生产数据源")
        data_manager = setup_production_data_sources()
        if not data_manager:
            logger.error("❌ 数据源配置失败，无法继续")
            return False
        
        # 2. 获取股票列表
        logger.info("\n🎯 步骤2: 获取股票列表")
        stock_list = get_stock_list(data_manager)
        if stock_list.empty:
            logger.error("❌ 获取股票列表失败，无法继续")
            return False
        
        # 3. 导入股票基础信息
        logger.info("\n🎯 步骤3: 导入股票基础信息")
        if not import_stock_basic_info(data_manager, stock_list):
            logger.error("❌ 导入股票基础信息失败")
            return False
        
        # 4. 导入历史数据
        logger.info("\n🎯 步骤4: 导入历史数据")
        if not import_historical_data(data_manager, stock_list):
            logger.error("❌ 导入历史数据失败")
            return False
        
        # 5. 验证数据
        logger.info("\n🎯 步骤5: 验证导入数据")
        if not validate_imported_data():
            logger.warning("⚠️ 数据验证失败")
        
        logger.success("🎉 生产数据导入完成！")
        return True
        
    except Exception as e:
        logger.error(f"❌ 导入过程异常: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
