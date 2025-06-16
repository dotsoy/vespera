"""
Alpha Vantage数据源实现
"""
import pandas as pd
import requests
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date
import time
import json
import os
from loguru import logger

from .base_data_source import (
    BaseDataSource, DataRequest, DataResponse, DataType, 
    DataSourceType, DataSourceStatus, DataSourceError,
    RateLimitError, AuthenticationError, NetworkError
)


class AlphaVantageDataSource(BaseDataSource):
    """Alpha Vantage数据源实现"""
    
    def __init__(self, api_key: Optional[str] = None, config: Optional[Dict[str, Any]] = None):
        default_config = {
            'rate_limit': 5,     # 免费版每分钟5次请求
            'priority': 3,       # 较低优先级
            'description': 'Alpha Vantage金融数据API',
            'cost_per_request': 0.0  # 免费版
        }
        if config:
            default_config.update(config)
            
        super().__init__("Alpha Vantage", DataSourceType.ALPHA_VANTAGE, default_config)
        # 从环境变量或参数获取API key
        self.api_key = api_key or os.getenv('ALPHA_VANTAGE_API_KEY')
        if not self.api_key:
            raise ValueError("Alpha Vantage API key未配置，请设置ALPHA_VANTAGE_API_KEY环境变量或传入api_key参数")
        self.base_url = "https://www.alphavantage.co/query"
        
    def initialize(self) -> bool:
        """初始化Alpha Vantage连接"""
        try:
            if not self.api_key:
                logger.error("Alpha Vantage API key 未配置")
                self.status = DataSourceStatus.ERROR
                return False
            
            # 测试API连接
            test_params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': 'AAPL',
                'apikey': self.api_key,
                'outputsize': 'compact'
            }
            
            response = requests.get(self.base_url, params=test_params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if 'Time Series (Daily)' in data:
                    self.status = DataSourceStatus.AVAILABLE
                    logger.info("Alpha Vantage数据源初始化成功")
                    return True
                elif 'Error Message' in data:
                    logger.error(f"Alpha Vantage API错误: {data['Error Message']}")
                    self.status = DataSourceStatus.ERROR
                    return False
                elif 'Note' in data:
                    logger.warning("Alpha Vantage API请求频率限制")
                    self.status = DataSourceStatus.LIMITED
                    return True
                else:
                    logger.error("Alpha Vantage API响应格式异常")
                    self.status = DataSourceStatus.ERROR
                    return False
            else:
                logger.error(f"Alpha Vantage API请求失败: {response.status_code}")
                self.status = DataSourceStatus.ERROR
                return False
                
        except Exception as e:
            self.status = DataSourceStatus.ERROR
            logger.error(f"Alpha Vantage初始化失败: {e}")
            return False
    
    def get_supported_data_types(self) -> List[DataType]:
        """获取支持的数据类型"""
        return [
            DataType.DAILY_QUOTES,
            DataType.MINUTE_QUOTES,
            DataType.STOCK_BASIC
        ]
    
    def check_availability(self) -> DataSourceStatus:
        """检查数据源可用性"""
        try:
            # 简单的可用性测试
            test_params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': 'AAPL',
                'apikey': self.api_key,
                'outputsize': 'compact'
            }
            
            response = requests.get(self.base_url, params=test_params, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if 'Time Series (Daily)' in data:
                    self.status = DataSourceStatus.AVAILABLE
                elif 'Note' in data:
                    self.status = DataSourceStatus.LIMITED
                else:
                    self.status = DataSourceStatus.ERROR
            else:
                self.status = DataSourceStatus.ERROR
                
        except Exception as e:
            logger.warning(f"Alpha Vantage可用性检查失败: {e}")
            self.status = DataSourceStatus.ERROR
        
        return self.status
    
    def fetch_data(self, request: DataRequest) -> DataResponse:
        """获取数据"""
        if not self.validate_request(request):
            return DataResponse(
                data=pd.DataFrame(),
                source=self.name,
                data_type=request.data_type,
                timestamp=datetime.now(),
                success=False,
                error_message="请求参数验证失败"
            )
        
        if not self._check_rate_limit():
            raise RateLimitError("请求频率超限", self.name)
        
        try:
            self._update_request_stats()
            
            if request.data_type == DataType.DAILY_QUOTES:
                data = self._fetch_daily_quotes(request)
            elif request.data_type == DataType.MINUTE_QUOTES:
                data = self._fetch_minute_quotes(request)
            elif request.data_type == DataType.STOCK_BASIC:
                data = self._fetch_stock_basic(request)
            else:
                raise DataSourceError(f"不支持的数据类型: {request.data_type}", self.name)
            
            return DataResponse(
                data=data,
                source=self.name,
                data_type=request.data_type,
                timestamp=datetime.now(),
                success=True,
                metadata={"records": len(data)}
            )
            
        except Exception as e:
            logger.error(f"Alpha Vantage获取数据失败: {e}")
            return DataResponse(
                data=pd.DataFrame(),
                source=self.name,
                data_type=request.data_type,
                timestamp=datetime.now(),
                success=False,
                error_message=str(e)
            )
    
    def _fetch_daily_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取日线行情数据"""
        symbol = request.symbol
        if not symbol:
            raise DataSourceError("股票代码不能为空", self.name)
        
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'apikey': self.api_key,
            'outputsize': 'full'  # 获取完整历史数据
        }
        
        response = requests.get(self.base_url, params=params, timeout=30)
        
        if response.status_code != 200:
            raise NetworkError(f"API请求失败: {response.status_code}", self.name)
        
        data = response.json()
        
        if 'Error Message' in data:
            raise DataSourceError(f"API错误: {data['Error Message']}", self.name)
        
        if 'Note' in data:
            raise RateLimitError("API请求频率限制", self.name)
        
        if 'Time Series (Daily)' not in data:
            raise DataSourceError("响应数据格式异常", self.name)
        
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
            
            # 格式化日期
            df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
            
            # 根据请求的日期范围过滤
            if request.start_date or request.end_date:
                df = self._filter_by_date_range(df, request.start_date, request.end_date)
        
        return df
    
    def _fetch_minute_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取分钟级行情数据"""
        symbol = request.symbol
        if not symbol:
            raise DataSourceError("股票代码不能为空", self.name)
        
        params = request.extra_params or {}
        interval = params.get('interval', '1min')  # 1min, 5min, 15min, 30min, 60min
        
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': symbol,
            'interval': interval,
            'apikey': self.api_key,
            'outputsize': 'full'
        }
        
        response = requests.get(self.base_url, params=params, timeout=30)
        
        if response.status_code != 200:
            raise NetworkError(f"API请求失败: {response.status_code}", self.name)
        
        data = response.json()
        
        if 'Error Message' in data:
            raise DataSourceError(f"API错误: {data['Error Message']}", self.name)
        
        if 'Note' in data:
            raise RateLimitError("API请求频率限制", self.name)
        
        time_series_key = f'Time Series ({interval})'
        if time_series_key not in data:
            raise DataSourceError("响应数据格式异常", self.name)
        
        # 解析时间序列数据
        time_series = data[time_series_key]
        
        records = []
        for datetime_str, values in time_series.items():
            record = {
                'ts_code': symbol,
                'datetime': datetime_str,
                'open_price': float(values['1. open']),
                'high_price': float(values['2. high']),
                'low_price': float(values['3. low']),
                'close_price': float(values['4. close']),
                'vol': int(values['5. volume'])
            }
            records.append(record)
        
        df = pd.DataFrame(records)
        
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.sort_values('datetime')
        
        return df
    
    def _fetch_stock_basic(self, request: DataRequest) -> pd.DataFrame:
        """获取股票基础信息"""
        symbol = request.symbol
        if not symbol:
            raise DataSourceError("股票代码不能为空", self.name)
        
        params = {
            'function': 'OVERVIEW',
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        response = requests.get(self.base_url, params=params, timeout=30)
        
        if response.status_code != 200:
            raise NetworkError(f"API请求失败: {response.status_code}", self.name)
        
        data = response.json()
        
        if 'Error Message' in data:
            raise DataSourceError(f"API错误: {data['Error Message']}", self.name)
        
        if 'Note' in data:
            raise RateLimitError("API请求频率限制", self.name)
        
        if not data or 'Symbol' not in data:
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
        
        return pd.DataFrame([basic_info])
    
    def _filter_by_date_range(self, df: pd.DataFrame, start_date: Optional[Union[str, date, datetime]], 
                             end_date: Optional[Union[str, date, datetime]]) -> pd.DataFrame:
        """根据日期范围过滤数据"""
        if df.empty:
            return df
        
        # 确保trade_date列是datetime类型
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        if start_date:
            start_dt = pd.to_datetime(start_date)
            df = df[df['trade_date'] >= start_dt]
        
        if end_date:
            end_dt = pd.to_datetime(end_date)
            df = df[df['trade_date'] <= end_dt]
        
        # 重新格式化日期
        df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
        
        return df
    
    def validate_request(self, request: DataRequest) -> bool:
        """验证请求参数"""
        # 调用父类验证
        if not super().validate_request(request):
            return False
        
        # Alpha Vantage特定验证
        if request.data_type in [DataType.DAILY_QUOTES, DataType.MINUTE_QUOTES, DataType.STOCK_BASIC]:
            if not request.symbol:
                logger.warning("Alpha Vantage需要提供股票代码")
                return False
        
        return True
    
    def close(self):
        """关闭连接"""
        # Alpha Vantage不需要特殊的关闭操作
        self.status = DataSourceStatus.UNAVAILABLE
        super().close()
