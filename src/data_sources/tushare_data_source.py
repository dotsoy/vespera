"""
Tushare数据源实现
提供股票行情、基础信息等数据
"""
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from loguru import logger

from .base_data_source import (
    BaseDataSource, DataRequest, DataResponse, DataType,
    DataSourceType, DataSourceStatus, DataSourceInfo,
    DataSourceError, RateLimitError
)

try:
    import tushare as ts
    TUSHARE_AVAILABLE = True
except ImportError:
    TUSHARE_AVAILABLE = False
    logger.warning("Tushare未安装，相关功能将不可用")


class TushareDataSource(BaseDataSource):
    """Tushare数据源"""
    
    def __init__(self, token: str = None, **config):
        super().__init__(
            name="tushare",
            source_type=DataSourceType.API,
            config=config
        )
        self.token = token or config.get('token')
        self.pro = None
        self._last_request_time = 0
        self._request_interval = 0.2  # 200ms间隔，避免频率限制
        
    def initialize(self) -> bool:
        """初始化Tushare连接"""
        if not TUSHARE_AVAILABLE:
            logger.error("Tushare库未安装")
            return False
            
        if not self.token:
            logger.error("Tushare token未配置")
            return False
            
        try:
            ts.set_token(self.token)
            self.pro = ts.pro_api()
            
            # 测试连接
            test_df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')
            if test_df is not None and not test_df.empty:
                self.status = DataSourceStatus.AVAILABLE
                logger.info("Tushare数据源初始化成功")
                return True
            else:
                logger.error("Tushare连接测试失败")
                return False
                
        except Exception as e:
            logger.error(f"Tushare初始化失败: {e}")
            return False
    
    def check_availability(self) -> DataSourceStatus:
        """检查数据源可用性"""
        if not TUSHARE_AVAILABLE or not self.pro:
            return DataSourceStatus.UNAVAILABLE
            
        try:
            # 简单的可用性检查
            test_df = self.pro.trade_cal(exchange='SSE', start_date='20240101', end_date='20240102')
            if test_df is not None:
                return DataSourceStatus.AVAILABLE
            else:
                return DataSourceStatus.LIMITED
        except Exception as e:
            logger.warning(f"Tushare可用性检查失败: {e}")
            return DataSourceStatus.LIMITED
    
    def get_supported_data_types(self) -> List[DataType]:
        """获取支持的数据类型"""
        return [
            DataType.STOCK_BASIC,
            DataType.DAILY_QUOTES,
            DataType.INDEX_DATA,
            DataType.FINANCIAL_DATA
        ]
    
    def validate_request(self, request: DataRequest) -> bool:
        """验证请求参数"""
        if request.data_type not in self.get_supported_data_types():
            return False
            
        if request.data_type in [DataType.DAILY_QUOTES] and not request.symbol:
            return False
            
        return True
    
    def _rate_limit_check(self):
        """频率限制检查"""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self._request_interval:
            sleep_time = self._request_interval - time_since_last
            time.sleep(sleep_time)
        
        self._last_request_time = time.time()
    
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
        
        try:
            self._rate_limit_check()
            
            if request.data_type == DataType.STOCK_BASIC:
                data = self._fetch_stock_basic(request)
            elif request.data_type == DataType.DAILY_QUOTES:
                data = self._fetch_daily_quotes(request)
            elif request.data_type == DataType.INDEX_DATA:
                data = self._fetch_index_data(request)
            elif request.data_type == DataType.FINANCIAL_DATA:
                data = self._fetch_financial_data(request)
            else:
                raise DataSourceError(f"不支持的数据类型: {request.data_type}", self.name)
            
            return DataResponse(
                data=data,
                source=self.name,
                data_type=request.data_type,
                timestamp=datetime.now(),
                success=True
            )
            
        except Exception as e:
            logger.error(f"Tushare获取数据失败: {e}")
            return DataResponse(
                data=pd.DataFrame(),
                source=self.name,
                data_type=request.data_type,
                timestamp=datetime.now(),
                success=False,
                error_message=str(e)
            )
    
    def _fetch_stock_basic(self, request: DataRequest) -> pd.DataFrame:
        """获取股票基础信息"""
        try:
            df = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market,list_date'
            )
            return df
        except Exception as e:
            logger.error(f"获取股票基础信息失败: {e}")
            return pd.DataFrame()
    
    def _fetch_daily_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取日线行情数据"""
        try:
            symbol = request.symbol
            start_date = request.start_date.strftime('%Y%m%d') if request.start_date else None
            end_date = request.end_date.strftime('%Y%m%d') if request.end_date else None
            
            df = self.pro.daily(
                ts_code=symbol,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                return df
            
            # 标准化列名
            df = df.rename(columns={
                'trade_date': 'trade_date',
                'open': 'open_price',
                'close': 'close_price',
                'high': 'high_price',
                'low': 'low_price',
                'vol': 'vol',
                'amount': 'amount'
            })
            
            # 确保日期格式正确
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            
            return df
            
        except Exception as e:
            logger.error(f"获取日线行情失败: {e}")
            return pd.DataFrame()
    
    def _fetch_index_data(self, request: DataRequest) -> pd.DataFrame:
        """获取指数数据"""
        try:
            symbol = request.symbol or "000001.SH"  # 默认上证指数
            start_date = request.start_date.strftime('%Y%m%d') if request.start_date else None
            end_date = request.end_date.strftime('%Y%m%d') if request.end_date else None
            
            df = self.pro.index_daily(
                ts_code=symbol,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                return df
            
            # 标准化列名
            df = df.rename(columns={
                'trade_date': 'trade_date',
                'open': 'open_price',
                'close': 'close_price',
                'high': 'high_price',
                'low': 'low_price',
                'vol': 'vol',
                'amount': 'amount'
            })
            
            return df
            
        except Exception as e:
            logger.error(f"获取指数数据失败: {e}")
            return pd.DataFrame()
    
    def _fetch_financial_data(self, request: DataRequest) -> pd.DataFrame:
        """获取财务数据"""
        try:
            symbol = request.symbol
            if not symbol:
                raise DataSourceError("股票代码不能为空", self.name)
            
            # 获取基本财务数据
            df = self.pro.fina_indicator(
                ts_code=symbol,
                start_date='20200101',
                end_date='20241231'
            )
            
            return df
            
        except Exception as e:
            logger.error(f"获取财务数据失败: {e}")
            return pd.DataFrame()
    
    def close(self):
        """关闭数据源连接"""
        self.pro = None
        self.status = DataSourceStatus.CLOSED
        logger.info("Tushare数据源已关闭")
