#!/usr/bin/env python3
"""
Alpha Vantage API 客户端
简化版本，用于测试和基础数据获取
"""
import os
import requests
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any
import time

from src.utils.logger import get_logger

logger = get_logger("alpha_vantage_client")


class AlphaVantageClient:
    """Alpha Vantage API 客户端"""
    
    def __init__(self, api_key: Optional[str] = None):
        """初始化Alpha Vantage客户端"""
        self.api_key = api_key or os.getenv('ALPHA_VANTAGE_API_KEY')
        if not self.api_key:
            raise ValueError("Alpha Vantage API key未配置，请设置ALPHA_VANTAGE_API_KEY环境变量")
        
        self.base_url = "https://www.alphavantage.co/query"
        self.session = requests.Session()
        
        logger.info("Alpha Vantage客户端初始化完成")
    
    def test_connection(self) -> bool:
        """测试连接"""
        try:
            # 测试获取AAPL股票数据
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': 'AAPL',
                'apikey': self.api_key,
                'outputsize': 'compact'
            }
            
            response = self.session.get(self.base_url, params=params, timeout=10)
            logger.info(f"Alpha Vantage连接测试响应: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                if 'Time Series (Daily)' in data:
                    logger.info("✅ Alpha Vantage连接测试成功")
                    return True
                elif 'Error Message' in data:
                    logger.error(f"❌ Alpha Vantage API错误: {data['Error Message']}")
                    return False
                elif 'Note' in data:
                    logger.warning("⚠️ Alpha Vantage API请求频率限制")
                    return True  # 虽然有限制，但连接是正常的
                else:
                    logger.error("❌ Alpha Vantage响应格式异常")
                    return False
            else:
                logger.error(f"❌ Alpha Vantage HTTP错误: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Alpha Vantage连接测试失败: {e}")
            return False
    
    def get_stock_basic(self, symbol: str) -> pd.DataFrame:
        """获取单只股票基础信息"""
        try:
            params = {
                'function': 'OVERVIEW',
                'symbol': symbol,
                'apikey': self.api_key
            }
            
            logger.info(f"获取{symbol}基础信息...")
            response = self.session.get(self.base_url, params=params, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"HTTP请求失败: {response.status_code}")
                return pd.DataFrame()
            
            data = response.json()
            
            if 'Error Message' in data:
                logger.error(f"API错误: {data['Error Message']}")
                return pd.DataFrame()
            
            if 'Note' in data:
                logger.warning("API请求频率限制")
                return pd.DataFrame()
            
            if not data or 'Symbol' not in data:
                logger.warning(f"未获取到{symbol}的基础信息")
                return pd.DataFrame()
            
            # 构建基础信息
            basic_info = {
                'ts_code': symbol,
                'symbol': data.get('Symbol', ''),
                'name': data.get('Name', ''),
                'industry': data.get('Industry', ''),
                'sector': data.get('Sector', ''),
                'market_cap': data.get('MarketCapitalization', ''),
                'country': data.get('Country', ''),
                'currency': data.get('Currency', ''),
                'exchange': data.get('Exchange', ''),
                'description': data.get('Description', '')
            }
            
            df = pd.DataFrame([basic_info])
            logger.info(f"✅ 获取到{symbol}基础信息")
            
            return df
            
        except Exception as e:
            logger.error(f"获取{symbol}基础信息失败: {e}")
            return pd.DataFrame()
    
    def get_daily_quotes(self, symbol: str, outputsize: str = 'compact') -> pd.DataFrame:
        """获取日线行情数据"""
        try:
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol,
                'apikey': self.api_key,
                'outputsize': outputsize  # compact 或 full
            }
            
            logger.info(f"获取{symbol}日线行情数据...")
            response = self.session.get(self.base_url, params=params, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"HTTP请求失败: {response.status_code}")
                return pd.DataFrame()
            
            data = response.json()
            
            if 'Error Message' in data:
                logger.error(f"API错误: {data['Error Message']}")
                return pd.DataFrame()
            
            if 'Note' in data:
                logger.warning("API请求频率限制")
                return pd.DataFrame()
            
            if 'Time Series (Daily)' not in data:
                logger.warning(f"未获取到{symbol}的行情数据")
                return pd.DataFrame()
            
            # 解析时间序列数据
            time_series = data['Time Series (Daily)']
            
            records = []
            for date_str, values in time_series.items():
                record = {
                    'ts_code': symbol,
                    'trade_date': date_str,
                    'open_price': float(values['1. open']),
                    'high_price': float(values['2. high']),
                    'low_price': float(values['3. low']),
                    'close_price': float(values['4. close']),
                    'vol': int(values['5. volume'])
                }
                records.append(record)
            
            df = pd.DataFrame(records)
            
            if not df.empty:
                # 确保日期格式和排序
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df = df.sort_values('trade_date')
                
                # 计算涨跌幅
                df['pct_chg'] = df['close_price'].pct_change() * 100
                df['change_amount'] = df['close_price'].diff()
                df['pre_close'] = df['close_price'].shift(1)
                
                # 格式化日期
                df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
            
            logger.info(f"✅ 获取到 {len(df)} 条{symbol}行情数据")
            return df
            
        except Exception as e:
            logger.error(f"获取{symbol}行情数据失败: {e}")
            return pd.DataFrame()
    
    def batch_get_daily_quotes(self, symbols: List[str], outputsize: str = 'compact') -> pd.DataFrame:
        """批量获取日线行情数据"""
        all_quotes = []
        
        for i, symbol in enumerate(symbols):
            try:
                quotes = self.get_daily_quotes(symbol, outputsize)
                if not quotes.empty:
                    all_quotes.append(quotes)
                
                # Alpha Vantage免费版有严格的频率限制，需要较长间隔
                if i < len(symbols) - 1:
                    logger.info(f"等待API频率限制... ({i+1}/{len(symbols)})")
                    time.sleep(12)  # 免费版每分钟5次请求，所以间隔12秒
                    
            except Exception as e:
                logger.error(f"获取{symbol}行情失败: {e}")
                continue
        
        if all_quotes:
            result = pd.concat(all_quotes, ignore_index=True)
            logger.info(f"✅ 批量获取完成，总计 {len(result)} 条行情数据")
            return result
        else:
            logger.warning("批量获取行情数据失败，未获取到任何数据")
            return pd.DataFrame()
    
    def get_us_stock_list(self) -> List[str]:
        """获取常见美股列表（Alpha Vantage没有直接的股票列表API）"""
        # 返回一些常见的美股代码用于测试
        common_us_stocks = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
            'META', 'NVDA', 'NFLX', 'BABA', 'JD',
            'PDD', 'NIO', 'XPEV', 'LI', 'BILI'
        ]
        
        logger.info(f"返回 {len(common_us_stocks)} 只常见美股代码")
        return common_us_stocks
    
    def close(self):
        """关闭连接"""
        if self.session:
            self.session.close()
        logger.info("Alpha Vantage客户端连接已关闭")
