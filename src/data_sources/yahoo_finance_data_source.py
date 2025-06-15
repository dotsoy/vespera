"""
Yahoo Finance数据源实现
"""
import pandas as pd
import yfinance as yf
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date, timedelta
import time
from loguru import logger

from .base_data_source import (
    BaseDataSource, DataRequest, DataResponse, DataType, 
    DataSourceType, DataSourceStatus, DataSourceError,
    RateLimitError, NetworkError
)


class YahooFinanceDataSource(BaseDataSource):
    """Yahoo Finance数据源实现"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        default_config = {
            'rate_limit': 2000,  # 每分钟2000次请求
            'priority': 2,       # 中等优先级
            'description': 'Yahoo Finance免费金融数据接口',
            'cost_per_request': 0.0  # 免费
        }
        if config:
            default_config.update(config)
            
        super().__init__("Yahoo Finance", DataSourceType.YAHOO_FINANCE, default_config)
        
    def initialize(self) -> bool:
        """初始化Yahoo Finance连接"""
        try:
            # 测试连接 - 获取一个简单的股票数据
            test_ticker = yf.Ticker("AAPL")
            test_data = test_ticker.history(period="1d")
            
            if not test_data.empty:
                self.status = DataSourceStatus.AVAILABLE
                logger.info("Yahoo Finance数据源初始化成功")
                return True
            else:
                self.status = DataSourceStatus.ERROR
                logger.error("Yahoo Finance连接测试失败")
                return False
                
        except Exception as e:
            self.status = DataSourceStatus.ERROR
            logger.error(f"Yahoo Finance初始化失败: {e}")
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
            test_ticker = yf.Ticker("AAPL")
            test_data = test_ticker.history(period="1d")
            
            if not test_data.empty:
                self.status = DataSourceStatus.AVAILABLE
            else:
                self.status = DataSourceStatus.ERROR
                
        except Exception as e:
            logger.warning(f"Yahoo Finance可用性检查失败: {e}")
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
            logger.error(f"Yahoo Finance获取数据失败: {e}")
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
        symbol = self._convert_symbol(request.symbol)
        
        if not symbol:
            raise DataSourceError("股票代码不能为空", self.name)
        
        ticker = yf.Ticker(symbol)
        
        # 确定时间范围
        if request.start_date and request.end_date:
            start_date = self._parse_date(request.start_date)
            end_date = self._parse_date(request.end_date)
            data = ticker.history(start=start_date, end=end_date)
        else:
            # 默认获取最近1年的数据
            data = ticker.history(period="1y")
        
        if data.empty:
            return pd.DataFrame()
        
        # 重置索引，将日期作为列
        data = data.reset_index()
        
        # 标准化列名
        column_mapping = {
            'Date': 'trade_date',
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price',
            'Volume': 'vol'
        }
        
        data = data.rename(columns=column_mapping)
        
        # 添加股票代码
        data['ts_code'] = request.symbol
        
        # 计算涨跌幅
        if 'close_price' in data.columns:
            data['pct_chg'] = data['close_price'].pct_change() * 100
            data['change_amount'] = data['close_price'].diff()
        
        # 确保日期格式
        if 'trade_date' in data.columns:
            data['trade_date'] = pd.to_datetime(data['trade_date']).dt.strftime('%Y-%m-%d')
        
        # 选择需要的列
        required_columns = ['ts_code', 'trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'vol', 'pct_chg']
        available_columns = [col for col in required_columns if col in data.columns]
        data = data[available_columns]
        
        return data
    
    def _fetch_minute_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取分钟级行情数据"""
        symbol = self._convert_symbol(request.symbol)
        
        if not symbol:
            raise DataSourceError("股票代码不能为空", self.name)
        
        ticker = yf.Ticker(symbol)
        
        # Yahoo Finance分钟数据有限制，通常只能获取最近几天的数据
        params = request.extra_params or {}
        period = params.get('period', '5d')  # 默认5天
        interval = params.get('interval', '1m')  # 默认1分钟
        
        data = ticker.history(period=period, interval=interval)
        
        if data.empty:
            return pd.DataFrame()
        
        # 重置索引，将时间作为列
        data = data.reset_index()
        
        # 标准化列名
        column_mapping = {
            'Datetime': 'datetime',
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price',
            'Volume': 'vol'
        }
        
        data = data.rename(columns=column_mapping)
        
        # 添加股票代码
        data['ts_code'] = request.symbol
        
        return data
    
    def _fetch_stock_basic(self, request: DataRequest) -> pd.DataFrame:
        """获取股票基础信息"""
        symbol = self._convert_symbol(request.symbol)
        
        if not symbol:
            raise DataSourceError("股票代码不能为空", self.name)
        
        ticker = yf.Ticker(symbol)
        
        try:
            info = ticker.info
            
            if not info:
                return pd.DataFrame()
            
            # 构建基础信息DataFrame
            basic_info = {
                'ts_code': request.symbol,
                'symbol': symbol,
                'name': info.get('longName', ''),
                'industry': info.get('industry', ''),
                'sector': info.get('sector', ''),
                'market_cap': info.get('marketCap', 0),
                'country': info.get('country', ''),
                'currency': info.get('currency', ''),
                'exchange': info.get('exchange', ''),
                'website': info.get('website', ''),
                'description': info.get('longBusinessSummary', '')
            }
            
            return pd.DataFrame([basic_info])
            
        except Exception as e:
            logger.warning(f"获取股票 {symbol} 基础信息失败: {e}")
            return pd.DataFrame()
    
    def _convert_symbol(self, symbol: Optional[str]) -> str:
        """转换股票代码格式"""
        if not symbol:
            return ""
        
        # 如果是中国股票代码，需要转换格式
        if symbol.endswith('.SH'):
            # 上海交易所
            return symbol.replace('.SH', '.SS')
        elif symbol.endswith('.SZ'):
            # 深圳交易所
            return symbol.replace('.SZ', '.SZ')
        else:
            # 其他市场直接返回
            return symbol
    
    def _parse_date(self, date_input: Union[str, date, datetime]) -> datetime:
        """解析日期"""
        if isinstance(date_input, str):
            try:
                return pd.to_datetime(date_input)
            except:
                raise DataSourceError(f"无效的日期格式: {date_input}", self.name)
        elif isinstance(date_input, date):
            return datetime.combine(date_input, datetime.min.time())
        elif isinstance(date_input, datetime):
            return date_input
        else:
            raise DataSourceError(f"不支持的日期类型: {type(date_input)}", self.name)
    
    def validate_request(self, request: DataRequest) -> bool:
        """验证请求参数"""
        # 调用父类验证
        if not super().validate_request(request):
            return False
        
        # Yahoo Finance特定验证
        if request.data_type in [DataType.DAILY_QUOTES, DataType.MINUTE_QUOTES]:
            if not request.symbol:
                logger.warning("Yahoo Finance需要提供股票代码")
                return False
        
        return True
    
    def close(self):
        """关闭连接"""
        # Yahoo Finance不需要特殊的关闭操作
        self.status = DataSourceStatus.UNAVAILABLE
        super().close()
