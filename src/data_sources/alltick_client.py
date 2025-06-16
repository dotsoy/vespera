#!/usr/bin/env python3
"""
AllTick API 客户端
简化版本，用于测试和基础数据获取
"""
import os
import requests
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any
import time

from src.utils.logger import get_logger

logger = get_logger("alltick_client")


class AllTickClient:
    """AllTick API 客户端"""
    
    def __init__(self, token: Optional[str] = None):
        """初始化AllTick客户端"""
        self.token = token or os.getenv('ALLTICK_TOKEN')
        if not self.token:
            raise ValueError("AllTick token未配置，请设置ALLTICK_TOKEN环境变量")
        
        self.base_url = "https://quote.tradeswitcher.com/quote-stock-b-api"
        self.session = requests.Session()
        
        # 设置请求头
        self.session.headers.update({
            'token': self.token,
            'Content-Type': 'application/json',
            'User-Agent': 'QimingStar/1.0'
        })
        
        logger.info("AllTick客户端初始化完成")
    
    def test_connection(self) -> bool:
        """测试连接"""
        try:
            # 测试获取股票列表
            url = f"{self.base_url}/stock-list"
            params = {
                'market': 'cn',
                'token': self.token
            }
            
            response = self.session.get(url, params=params, timeout=10)
            logger.info(f"AllTick连接测试响应: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"AllTick响应数据: {data}")
                
                if data.get('code') == 0:
                    logger.info("✅ AllTick连接测试成功")
                    return True
                else:
                    logger.error(f"❌ AllTick API错误: {data.get('msg', 'Unknown error')}")
                    return False
            else:
                logger.error(f"❌ AllTick HTTP错误: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ AllTick连接测试失败: {e}")
            return False
    
    def get_stock_list(self, market: str = 'cn') -> pd.DataFrame:
        """获取股票列表"""
        try:
            url = f"{self.base_url}/stock-list"
            params = {
                'market': market,
                'token': self.token
            }
            
            logger.info(f"获取{market}市场股票列表...")
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"HTTP请求失败: {response.status_code}")
                return pd.DataFrame()
            
            data = response.json()
            logger.info(f"AllTick响应: {data}")
            
            if data.get('code') != 0:
                logger.error(f"API错误: {data.get('msg', 'Unknown error')}")
                return pd.DataFrame()
            
            stocks_data = data.get('data', [])
            if not stocks_data:
                logger.warning("未获取到股票数据")
                return pd.DataFrame()
            
            # 转换为DataFrame
            records = []
            for stock in stocks_data:
                record = {
                    'ts_code': stock.get('symbol', ''),
                    'symbol': stock.get('code', ''),
                    'name': stock.get('name', ''),
                    'market': stock.get('market', ''),
                    'exchange': stock.get('exchange', ''),
                    'type': stock.get('type', ''),
                    'status': stock.get('status', 'active')
                }
                records.append(record)
            
            df = pd.DataFrame(records)
            logger.info(f"✅ 获取到 {len(df)} 只股票信息")
            
            return df
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def get_daily_quotes(self, symbol: str, date: str = None) -> pd.DataFrame:
        """获取日线行情数据"""
        try:
            if not date:
                date = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/kline"
            params = {
                'symbol': symbol,
                'period': '1d',
                'start_time': date,
                'end_time': date,
                'token': self.token
            }
            
            logger.info(f"获取{symbol}在{date}的行情数据...")
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"HTTP请求失败: {response.status_code}")
                return pd.DataFrame()
            
            data = response.json()
            
            if data.get('code') != 0:
                logger.error(f"API错误: {data.get('msg', 'Unknown error')}")
                return pd.DataFrame()
            
            kline_data = data.get('data', [])
            if not kline_data:
                logger.warning(f"未获取到{symbol}的行情数据")
                return pd.DataFrame()
            
            # 转换为DataFrame
            records = []
            for kline in kline_data:
                record = {
                    'ts_code': symbol,
                    'trade_date': datetime.fromtimestamp(kline[0]).strftime('%Y-%m-%d'),
                    'open_price': float(kline[1]),
                    'high_price': float(kline[2]),
                    'low_price': float(kline[3]),
                    'close_price': float(kline[4]),
                    'vol': int(kline[5]),
                    'amount': float(kline[6]) if len(kline) > 6 else 0
                }
                records.append(record)
            
            df = pd.DataFrame(records)
            
            if not df.empty:
                # 计算涨跌幅
                df = df.sort_values('trade_date')
                df['pct_chg'] = df['close_price'].pct_change() * 100
                df['change_amount'] = df['close_price'].diff()
                df['pre_close'] = df['close_price'].shift(1)
            
            logger.info(f"✅ 获取到 {len(df)} 条{symbol}行情数据")
            return df
            
        except Exception as e:
            logger.error(f"获取{symbol}行情数据失败: {e}")
            return pd.DataFrame()
    
    def batch_get_daily_quotes(self, symbols: List[str], date: str = None) -> pd.DataFrame:
        """批量获取日线行情数据"""
        all_quotes = []
        
        for i, symbol in enumerate(symbols):
            try:
                quotes = self.get_daily_quotes(symbol, date)
                if not quotes.empty:
                    all_quotes.append(quotes)
                
                # 控制请求频率
                if i < len(symbols) - 1:
                    time.sleep(0.1)  # 100ms间隔
                    
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
    
    def close(self):
        """关闭连接"""
        if self.session:
            self.session.close()
        logger.info("AllTick客户端连接已关闭")
