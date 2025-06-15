"""
Tushare 数据获取客户端
"""
import pandas as pd
import tushare as ts
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import time
from loguru import logger

from config.settings import data_settings
from src.utils.database import get_db_manager


class TushareClient:
    """Tushare 数据获取客户端"""
    
    def __init__(self):
        self.token = data_settings.tushare_token
        if not self.token:
            raise ValueError("Tushare token 未配置，请在环境变量中设置 TUSHARE_TOKEN")
        
        # 初始化 Tushare
        ts.set_token(self.token)
        self.pro = ts.pro_api()
        self.db_manager = get_db_manager()
        
        logger.info("Tushare 客户端初始化完成")
    
    def get_stock_basic(self, exchange: str = '', list_status: str = 'L') -> pd.DataFrame:
        """
        获取股票基础信息
        
        Args:
            exchange: 交易所代码 SSE上交所 SZSE深交所
            list_status: 上市状态 L上市 D退市 P暂停上市
        """
        try:
            logger.info(f"获取股票基础信息: exchange={exchange}, list_status={list_status}")
            
            df = self.pro.stock_basic(
                exchange=exchange,
                list_status=list_status,
                fields='ts_code,symbol,name,area,industry,market,list_date,is_hs'
            )
            
            # 数据清洗
            df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d', errors='coerce')
            
            # 过滤条件
            if data_settings.exclude_st_stocks:
                df = df[~df['name'].str.contains('ST|退', na=False)]
            
            # 过滤新股
            if data_settings.exclude_new_stocks_days > 0:
                cutoff_date = datetime.now() - timedelta(days=data_settings.exclude_new_stocks_days)
                df = df[df['list_date'] <= cutoff_date]
            
            logger.info(f"获取到 {len(df)} 只股票的基础信息")
            return df
            
        except Exception as e:
            logger.error(f"获取股票基础信息失败: {e}")
            raise
    
    def get_daily_quotes(self, ts_code: str = '', trade_date: str = '', 
                        start_date: str = '', end_date: str = '') -> pd.DataFrame:
        """
        获取股票日线行情
        
        Args:
            ts_code: 股票代码
            trade_date: 交易日期 YYYYMMDD
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        try:
            logger.info(f"获取日线行情: ts_code={ts_code}, trade_date={trade_date}")
            
            df = self.pro.daily(
                ts_code=ts_code,
                trade_date=trade_date,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
            )
            
            if df.empty:
                logger.warning("未获取到日线行情数据")
                return df
            
            # 数据类型转换
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df = df.rename(columns={
                'open': 'open_price',
                'high': 'high_price', 
                'low': 'low_price',
                'close': 'close_price',
                'change': 'change_amount'
            })
            
            # 按日期排序
            df = df.sort_values(['ts_code', 'trade_date'])
            
            logger.info(f"获取到 {len(df)} 条日线行情数据")
            return df
            
        except Exception as e:
            logger.error(f"获取日线行情失败: {e}")
            raise
    
    def get_money_flow(self, ts_code: str = '', trade_date: str = '',
                      start_date: str = '', end_date: str = '') -> pd.DataFrame:
        """
        获取资金流向数据
        
        Args:
            ts_code: 股票代码
            trade_date: 交易日期 YYYYMMDD
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        try:
            logger.info(f"获取资金流向: ts_code={ts_code}, trade_date={trade_date}")
            
            df = self.pro.moneyflow(
                ts_code=ts_code,
                trade_date=trade_date,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                logger.warning("未获取到资金流向数据")
                return df
            
            # 数据类型转换
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            
            # 按日期排序
            df = df.sort_values(['ts_code', 'trade_date'])
            
            logger.info(f"获取到 {len(df)} 条资金流向数据")
            return df
            
        except Exception as e:
            logger.error(f"获取资金流向失败: {e}")
            raise
    
    def get_top10_holders(self, ts_code: str, period: str = '') -> pd.DataFrame:
        """
        获取前十大股东信息
        
        Args:
            ts_code: 股票代码
            period: 报告期 YYYYMMDD
        """
        try:
            logger.info(f"获取前十大股东: ts_code={ts_code}, period={period}")
            
            df = self.pro.top10_holders(ts_code=ts_code, period=period)
            
            if df.empty:
                logger.warning("未获取到前十大股东数据")
                return df
            
            logger.info(f"获取到 {len(df)} 条前十大股东数据")
            return df
            
        except Exception as e:
            logger.error(f"获取前十大股东失败: {e}")
            raise
    
    def get_news(self, src: str = '', start_date: str = '', end_date: str = '') -> pd.DataFrame:
        """
        获取新闻数据
        
        Args:
            src: 新闻来源
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        try:
            logger.info(f"获取新闻数据: src={src}, start_date={start_date}")
            
            df = self.pro.news(
                src=src,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                logger.warning("未获取到新闻数据")
                return df
            
            # 数据类型转换
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            logger.info(f"获取到 {len(df)} 条新闻数据")
            return df
            
        except Exception as e:
            logger.error(f"获取新闻数据失败: {e}")
            raise
    
    def batch_get_daily_quotes(self, stock_list: List[str], trade_date: str) -> pd.DataFrame:
        """
        批量获取多只股票的日线行情
        
        Args:
            stock_list: 股票代码列表
            trade_date: 交易日期 YYYYMMDD
        """
        all_data = []
        
        for i, ts_code in enumerate(stock_list):
            try:
                # API 限流控制
                if i > 0 and i % 200 == 0:
                    logger.info(f"已处理 {i} 只股票，暂停 1 分钟...")
                    time.sleep(60)
                elif i > 0 and i % 20 == 0:
                    time.sleep(1)
                
                df = self.get_daily_quotes(ts_code=ts_code, trade_date=trade_date)
                if not df.empty:
                    all_data.append(df)
                    
            except Exception as e:
                logger.error(f"获取股票 {ts_code} 行情失败: {e}")
                continue
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            logger.info(f"批量获取完成，共 {len(result)} 条记录")
            return result
        else:
            logger.warning("批量获取未得到任何数据")
            return pd.DataFrame()
    
    def save_stock_basic_to_db(self) -> None:
        """保存股票基础信息到数据库"""
        try:
            df = self.get_stock_basic()
            if not df.empty:
                self.db_manager.insert_dataframe_to_postgres(
                    df, 'stock_basic', if_exists='replace'
                )
                logger.info("股票基础信息已保存到数据库")
        except Exception as e:
            logger.error(f"保存股票基础信息失败: {e}")
            raise
    
    def save_daily_quotes_to_db(self, trade_date: str) -> None:
        """保存指定日期的日线行情到数据库"""
        try:
            # 获取所有股票列表
            stock_basic = self.db_manager.execute_postgres_query(
                "SELECT ts_code FROM stock_basic WHERE market IN ('主板', '创业板', '科创板')"
            )
            
            if stock_basic.empty:
                logger.warning("数据库中无股票基础信息，请先更新股票基础信息")
                return
            
            stock_list = stock_basic['ts_code'].tolist()
            
            # 批量获取行情数据
            df = self.batch_get_daily_quotes(stock_list, trade_date)
            
            if not df.empty:
                self.db_manager.insert_dataframe_to_postgres(
                    df, 'stock_daily_quotes', if_exists='append'
                )
                logger.info(f"日期 {trade_date} 的行情数据已保存到数据库")
        except Exception as e:
            logger.error(f"保存日线行情失败: {e}")
            raise


if __name__ == "__main__":
    # 测试 Tushare 客户端
    client = TushareClient()
    
    # 测试获取股票基础信息
    basic_df = client.get_stock_basic()
    print(f"获取到 {len(basic_df)} 只股票")
    
    # 测试获取日线行情
    if not basic_df.empty:
        sample_code = basic_df.iloc[0]['ts_code']
        quotes_df = client.get_daily_quotes(ts_code=sample_code, trade_date='20241201')
        print(f"获取到 {len(quotes_df)} 条行情数据")
