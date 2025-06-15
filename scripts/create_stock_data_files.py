#!/usr/bin/env python3
"""
创建股票数据文件 (CSV格式)
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
from src.data_sources.tushare_client import TushareClient

logger = get_logger("create_stock_data")


def create_data_directory():
    """创建数据目录"""
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    stock_data_dir = data_dir / "stock_data"
    stock_data_dir.mkdir(exist_ok=True)
    
    return stock_data_dir


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
            
            # 医药
            {'ts_code': '000001.SZ', 'symbol': '000001', 'name': '平安银行', 'industry': '银行', 'market': '主板'},
            
            # 更多代表性股票
            {'ts_code': '600276.SH', 'symbol': '600276', 'name': '恒瑞医药', 'industry': '医药生物', 'market': '主板'},
            {'ts_code': '000002.SZ', 'symbol': '000002', 'name': '万科A', 'industry': '房地产', 'market': '主板'},
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


def download_stock_data(stock_list: pd.DataFrame, data_dir: Path, days: int = 120):
    """下载股票数据并保存为文件"""
    try:
        logger.info(f"📈 下载股票数据 (最近{days}天)")
        
        tushare_client = TushareClient()
        
        # 计算日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        
        logger.info(f"数据日期范围: {start_date} - {end_date}")
        
        all_daily_data = []
        success_count = 0
        error_count = 0
        
        for _, stock in stock_list.iterrows():
            ts_code = stock['ts_code']
            
            try:
                logger.info(f"下载股票 {ts_code} ({stock['name']}) 的数据")
                
                # 获取日线数据
                daily_data = tushare_client.get_daily_quotes(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if not daily_data.empty:
                    # 只保留最近的交易日
                    daily_data = daily_data.sort_values('trade_date').tail(days)
                    all_daily_data.append(daily_data)
                    success_count += 1
                    logger.info(f"✅ {ts_code} 下载 {len(daily_data)} 条记录")
                else:
                    logger.warning(f"⚠️ {ts_code} 无数据")
                    error_count += 1
                
                # 控制API调用频率
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"❌ 下载 {ts_code} 数据失败: {e}")
                error_count += 1
                continue
        
        # 合并所有数据
        if all_daily_data:
            combined_daily = pd.concat(all_daily_data, ignore_index=True)
            
            # 保存股票基础信息
            basic_file = data_dir / "stock_basic.csv"
            stock_list.to_csv(basic_file, index=False, encoding='utf-8')
            logger.info(f"✅ 股票基础信息已保存到: {basic_file}")
            
            # 保存日线数据
            daily_file = data_dir / "stock_daily_quotes.csv"
            combined_daily.to_csv(daily_file, index=False, encoding='utf-8')
            logger.info(f"✅ 股票日线数据已保存到: {daily_file}")
            
            # 保存数据统计
            stats = {
                'total_stocks': len(stock_list),
                'successful_downloads': success_count,
                'failed_downloads': error_count,
                'total_records': len(combined_daily),
                'date_range': f"{start_date} - {end_date}",
                'download_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            stats_df = pd.DataFrame([stats])
            stats_file = data_dir / "download_stats.csv"
            stats_df.to_csv(stats_file, index=False, encoding='utf-8')
            logger.info(f"✅ 下载统计已保存到: {stats_file}")
            
            logger.success(f"🎉 股票数据下载完成！")
            logger.info(f"📊 下载统计:")
            logger.info(f"  成功股票: {success_count}")
            logger.info(f"  失败股票: {error_count}")
            logger.info(f"  总记录数: {len(combined_daily)}")
            logger.info(f"  成功率: {success_count/(success_count+error_count)*100:.1f}%")
            
            return True
        else:
            logger.error("❌ 没有成功下载任何数据")
            return False
        
    except Exception as e:
        logger.error(f"❌ 下载股票数据失败: {e}")
        return False


def create_sample_analysis_data():
    """创建用于分析的样本数据"""
    try:
        logger.info("📊 创建分析样本数据")
        
        data_dir = create_data_directory()
        
        # 检查是否已有数据文件
        daily_file = data_dir / "stock_daily_quotes.csv"
        basic_file = data_dir / "stock_basic.csv"
        
        if daily_file.exists() and basic_file.exists():
            logger.info("发现现有数据文件，加载中...")
            
            daily_data = pd.read_csv(daily_file)
            basic_data = pd.read_csv(basic_file)
            
            logger.info(f"✅ 加载了 {len(basic_data)} 只股票的基础信息")
            logger.info(f"✅ 加载了 {len(daily_data)} 条日线数据")
            
            return daily_data, basic_data
        else:
            logger.info("未发现数据文件，开始下载...")
            
            # 获取股票列表
            stock_list = get_sample_stock_list()
            if stock_list.empty:
                logger.error("❌ 无法创建股票列表")
                return None, None
            
            # 下载数据
            if download_stock_data(stock_list, data_dir):
                # 重新加载数据
                daily_data = pd.read_csv(daily_file)
                basic_data = pd.read_csv(basic_file)
                
                return daily_data, basic_data
            else:
                logger.error("❌ 数据下载失败")
                return None, None
        
    except Exception as e:
        logger.error(f"❌ 创建分析样本数据失败: {e}")
        return None, None


def verify_data_quality(daily_data: pd.DataFrame, basic_data: pd.DataFrame):
    """验证数据质量"""
    try:
        logger.info("🔍 验证数据质量")
        
        # 基础统计
        logger.info(f"股票基础信息: {len(basic_data)} 只股票")
        logger.info(f"日线数据: {len(daily_data)} 条记录")
        
        # 检查数据完整性
        stocks_with_data = daily_data['ts_code'].nunique()
        logger.info(f"有数据的股票: {stocks_with_data} 只")
        
        # 检查日期范围
        if not daily_data.empty:
            min_date = daily_data['trade_date'].min()
            max_date = daily_data['trade_date'].max()
            logger.info(f"数据日期范围: {min_date} - {max_date}")
        
        # 检查行业分布
        if not basic_data.empty:
            industry_counts = basic_data['industry'].value_counts()
            logger.info("行业分布:")
            for industry, count in industry_counts.items():
                logger.info(f"  {industry}: {count} 只股票")
        
        # 检查数据质量
        missing_data = daily_data.isnull().sum()
        if missing_data.sum() > 0:
            logger.warning("发现缺失数据:")
            for col, count in missing_data.items():
                if count > 0:
                    logger.warning(f"  {col}: {count} 个缺失值")
        else:
            logger.info("✅ 数据完整，无缺失值")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据质量验证失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🚀 创建股票数据文件")
    logger.info("=" * 60)
    
    try:
        # 创建分析数据
        daily_data, basic_data = create_sample_analysis_data()
        
        if daily_data is None or basic_data is None:
            logger.error("❌ 数据创建失败")
            return False
        
        # 验证数据质量
        if not verify_data_quality(daily_data, basic_data):
            logger.warning("⚠️ 数据质量验证有问题")
        
        logger.success("🎉 股票数据文件创建完成！")
        logger.info("=" * 60)
        logger.info("📁 数据文件位置:")
        logger.info("  data/stock_data/stock_basic.csv - 股票基础信息")
        logger.info("  data/stock_data/stock_daily_quotes.csv - 股票日线数据")
        logger.info("  data/stock_data/download_stats.csv - 下载统计")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据文件创建失败: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
