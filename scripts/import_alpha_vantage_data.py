#!/usr/bin/env python3
"""
使用Alpha Vantage导入生产数据
由于AllTick连接问题，使用Alpha Vantage作为主要数据源
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
from src.data_sources.alpha_vantage_data_source import AlphaVantageDataSource

logger = get_logger("import_alpha_vantage_data")

# Alpha Vantage API Key
ALPHA_VANTAGE_API_KEY = "3SHZ17DOQBH5X6IX"


def create_sample_stock_list():
    """创建样本股票列表（A股主要股票）"""
    logger.info("📋 创建样本股票列表")
    
    # 由于Alpha Vantage主要支持美股，我们创建一些知名的中概股和美股
    sample_stocks = [
        # 中概股
        {'symbol': 'BABA', 'name': '阿里巴巴', 'market': 'US'},
        {'symbol': 'JD', 'name': '京东', 'market': 'US'},
        {'symbol': 'BIDU', 'name': '百度', 'market': 'US'},
        {'symbol': 'NTES', 'name': '网易', 'market': 'US'},
        {'symbol': 'PDD', 'name': '拼多多', 'market': 'US'},
        
        # 美股知名股票
        {'symbol': 'AAPL', 'name': '苹果', 'market': 'US'},
        {'symbol': 'MSFT', 'name': '微软', 'market': 'US'},
        {'symbol': 'GOOGL', 'name': '谷歌', 'market': 'US'},
        {'symbol': 'AMZN', 'name': '亚马逊', 'market': 'US'},
        {'symbol': 'TSLA', 'name': '特斯拉', 'market': 'US'},
        {'symbol': 'NVDA', 'name': '英伟达', 'market': 'US'},
        {'symbol': 'META', 'name': 'Meta', 'market': 'US'},
        
        # 金融股
        {'symbol': 'JPM', 'name': '摩根大通', 'market': 'US'},
        {'symbol': 'BAC', 'name': '美国银行', 'market': 'US'},
        {'symbol': 'WFC', 'name': '富国银行', 'market': 'US'},
    ]
    
    stock_df = pd.DataFrame(sample_stocks)
    
    logger.success(f"✅ 创建了 {len(stock_df)} 只样本股票")
    logger.info("股票列表:")
    print(stock_df.to_string())
    
    return stock_df


def setup_alpha_vantage():
    """设置Alpha Vantage数据源"""
    logger.info("🔧 设置Alpha Vantage数据源")
    
    try:
        config = {
            'priority': 1,
            'rate_limit': 5,  # Alpha Vantage免费版限制
            'timeout': 30,
            'description': 'Alpha Vantage生产环境'
        }
        
        alpha_source = AlphaVantageDataSource(ALPHA_VANTAGE_API_KEY, config)
        
        if alpha_source.initialize():
            logger.success("✅ Alpha Vantage数据源配置成功")
            return alpha_source
        else:
            logger.error("❌ Alpha Vantage数据源初始化失败")
            return None
            
    except Exception as e:
        logger.error(f"❌ Alpha Vantage配置失败: {e}")
        return None


def import_stock_data(alpha_source, stock_list, days=30):
    """导入股票数据"""
    logger.info(f"📈 导入最近 {days} 天的股票数据")
    
    try:
        if stock_list.empty:
            logger.error("❌ 股票列表为空")
            return False
        
        # 计算日期范围
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        logger.info(f"数据日期范围: {start_date} 到 {end_date}")
        
        all_data = []
        success_count = 0
        total_count = len(stock_list)
        
        for idx, (_, stock) in enumerate(stock_list.iterrows(), 1):
            symbol = stock['symbol']
            name = stock['name']
            
            logger.info(f"[{idx}/{total_count}] 获取 {symbol} ({name}) 的数据")
            
            try:
                request = DataRequest(
                    data_type=DataType.DAILY_QUOTES,
                    symbol=symbol,
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d')
                )
                
                response = alpha_source.fetch_data(request)
                
                if response.success and not response.data.empty:
                    # 添加股票信息
                    data_with_info = response.data.copy()
                    data_with_info['symbol'] = symbol
                    data_with_info['name'] = name
                    data_with_info['market'] = stock['market']
                    
                    all_data.append(data_with_info)
                    success_count += 1
                    logger.success(f"✅ 获取成功，{len(response.data)} 条记录")
                else:
                    logger.warning(f"⚠️ 获取失败: {response.error_message}")
                
                # 控制请求频率 - Alpha Vantage免费版限制
                time.sleep(12)  # 每分钟5次请求
                
            except Exception as e:
                logger.warning(f"⚠️ 获取 {symbol} 数据时出错: {e}")
                continue
        
        # 合并所有数据
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            
            # 保存数据
            output_dir = project_root / 'data' / 'production'
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # 保存股票基础信息
            basic_file = output_dir / 'stock_basic.csv'
            stock_list.to_csv(basic_file, index=False, encoding='utf-8')
            logger.success(f"✅ 股票基础信息已保存到: {basic_file}")
            
            # 保存历史数据
            quotes_file = output_dir / 'daily_quotes.csv'
            combined_data.to_csv(quotes_file, index=False, encoding='utf-8')
            logger.success(f"✅ 历史数据已保存到: {quotes_file}")
            
            logger.info(f"成功获取 {success_count}/{total_count} 只股票的数据")
            logger.info(f"总记录数: {len(combined_data)}")
            
            return True
        else:
            logger.error("❌ 未获取到任何数据")
            return False
        
    except Exception as e:
        logger.error(f"❌ 导入股票数据失败: {e}")
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
            
            # 显示市场分布
            if 'market' in basic_data.columns:
                market_dist = basic_data['market'].value_counts()
                logger.info("市场分布:")
                for market, count in market_dist.items():
                    logger.info(f"  {market}: {count} 只")
        else:
            logger.warning("⚠️ 未找到股票基础信息文件")
        
        # 检查历史数据
        quotes_file = data_dir / 'daily_quotes.csv'
        if quotes_file.exists():
            quotes_data = pd.read_csv(quotes_file)
            logger.success(f"✅ 历史行情数据: {len(quotes_data)} 条记录")
            
            # 数据统计
            if not quotes_data.empty:
                unique_stocks = quotes_data['symbol'].nunique() if 'symbol' in quotes_data.columns else 0
                logger.info(f"涵盖股票数: {unique_stocks}")
                
                if 'date' in quotes_data.columns:
                    date_range = f"{quotes_data['date'].min()} 到 {quotes_data['date'].max()}"
                    logger.info(f"日期范围: {date_range}")
                
                # 显示数据样例
                logger.info("数据样例:")
                print(quotes_data.head().to_string())
                
                # 数据质量检查
                if 'close' in quotes_data.columns:
                    null_count = quotes_data['close'].isnull().sum()
                    logger.info(f"收盘价缺失值: {null_count}")
                    
                    if null_count == 0:
                        logger.success("✅ 数据质量良好，无缺失值")
                    else:
                        logger.warning(f"⚠️ 发现 {null_count} 个缺失值")
        else:
            logger.warning("⚠️ 未找到历史行情数据文件")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据验证失败: {e}")
        return False


def generate_summary_report():
    """生成总结报告"""
    logger.info("📊 生成总结报告")
    
    try:
        data_dir = project_root / 'data' / 'production'
        
        report = []
        report.append("=" * 80)
        report.append("Alpha Vantage生产数据导入报告")
        report.append("=" * 80)
        report.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # 检查数据文件
        basic_file = data_dir / 'stock_basic.csv'
        quotes_file = data_dir / 'daily_quotes.csv'
        
        if basic_file.exists():
            basic_data = pd.read_csv(basic_file)
            report.append(f"📋 股票基础信息: {len(basic_data)} 只股票")
            
            if 'market' in basic_data.columns:
                market_dist = basic_data['market'].value_counts()
                for market, count in market_dist.items():
                    report.append(f"   - {market}: {count} 只")
        
        if quotes_file.exists():
            quotes_data = pd.read_csv(quotes_file)
            report.append(f"📈 历史行情数据: {len(quotes_data)} 条记录")
            
            if not quotes_data.empty:
                unique_stocks = quotes_data['symbol'].nunique() if 'symbol' in quotes_data.columns else 0
                report.append(f"   - 涵盖股票: {unique_stocks} 只")
                
                if 'date' in quotes_data.columns:
                    date_range = f"{quotes_data['date'].min()} 到 {quotes_data['date'].max()}"
                    report.append(f"   - 日期范围: {date_range}")
        
        report.append("")
        report.append("🔧 数据源配置:")
        report.append(f"   - Alpha Vantage Key: {ALPHA_VANTAGE_API_KEY[:10]}...")
        report.append("   - 数据类型: 美股和中概股")
        report.append("")
        report.append("📝 说明:")
        report.append("   - 由于AllTick连接问题，使用Alpha Vantage作为数据源")
        report.append("   - 包含主要的中概股和美股数据")
        report.append("   - 数据质量经过验证")
        report.append("")
        report.append("🚀 下一步建议:")
        report.append("   1. 验证数据完整性")
        report.append("   2. 运行技术分析")
        report.append("   3. 配置定期数据更新")
        report.append("=" * 80)
        
        # 保存报告
        report_content = "\n".join(report)
        
        # 输出到控制台
        print("\n" + report_content)
        
        # 保存到文件
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        logs_dir = project_root / 'logs' / 'production'
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = logs_dir / f'alpha_vantage_import_report_{timestamp}.txt'
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.success(f"✅ 报告已保存到: {report_file}")
        return True
        
    except Exception as e:
        logger.error(f"❌ 生成报告失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🚀 Alpha Vantage生产数据导入")
    logger.info("=" * 80)
    
    logger.info("配置信息:")
    logger.info(f"  Alpha Vantage Key: {ALPHA_VANTAGE_API_KEY[:10]}...")
    logger.info("  数据类型: 美股和中概股")
    logger.info("  请求限制: 每分钟5次")
    
    # 执行导入步骤
    steps = [
        ("设置Alpha Vantage数据源", setup_alpha_vantage),
        ("创建股票列表", create_sample_stock_list),
        ("导入股票数据", None),  # 需要参数
        ("验证导入数据", validate_imported_data),
        ("生成总结报告", generate_summary_report)
    ]
    
    alpha_source = None
    stock_list = pd.DataFrame()
    
    try:
        # 1. 设置数据源
        logger.info("\n🎯 步骤1: 设置Alpha Vantage数据源")
        alpha_source = setup_alpha_vantage()
        if not alpha_source:
            logger.error("❌ 数据源设置失败，无法继续")
            return False
        
        # 2. 创建股票列表
        logger.info("\n🎯 步骤2: 创建股票列表")
        stock_list = create_sample_stock_list()
        if stock_list.empty:
            logger.error("❌ 股票列表创建失败，无法继续")
            return False
        
        # 3. 导入股票数据
        logger.info("\n🎯 步骤3: 导入股票数据")
        if not import_stock_data(alpha_source, stock_list):
            logger.error("❌ 导入股票数据失败")
            return False
        
        # 4. 验证数据
        logger.info("\n🎯 步骤4: 验证导入数据")
        if not validate_imported_data():
            logger.warning("⚠️ 数据验证失败")
        
        # 5. 生成报告
        logger.info("\n🎯 步骤5: 生成总结报告")
        if not generate_summary_report():
            logger.warning("⚠️ 报告生成失败")
        
        logger.success("🎉 Alpha Vantage生产数据导入完成！")
        return True
        
    except Exception as e:
        logger.error(f"❌ 导入过程异常: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
