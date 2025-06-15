"""
Tushare数据源实现
适配统一数据源接口
"""
import pandas as pd
import tushare as ts
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date
import time
from loguru import logger

from config.settings import data_settings
from .base_data_source import (
    BaseDataSource, DataRequest, DataResponse, DataType, 
    DataSourceType, DataSourceStatus, DataSourceError,
    RateLimitError, AuthenticationError, NetworkError
)


class TushareDataSource(BaseDataSource):
    """Tushare数据源实现"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        default_config = {
            'rate_limit': 200,  # 每分钟200次请求
            'priority': 1,      # 高优先级
            'description': 'Tushare专业金融数据接口'
        }
        if config:
            default_config.update(config)
            
        super().__init__("Tushare", DataSourceType.TUSHARE, default_config)
        self.token = None
        self.pro = None
        
    def initialize(self) -> bool:
        """初始化Tushare连接"""
        try:
            self.token = data_settings.tushare_token
            if not self.token:
                logger.error("Tushare token 未配置")
                self.status = DataSourceStatus.ERROR
                return False
            
            # 初始化 Tushare
            ts.set_token(self.token)
            self.pro = ts.pro_api()
            
            # 测试连接
            test_df = self.pro.stock_basic(list_status='L', fields='ts_code', limit=1)
            if test_df is not None and not test_df.empty:
                self.status = DataSourceStatus.AVAILABLE
                logger.info("Tushare数据源初始化成功")
                return True
            else:
                self.status = DataSourceStatus.ERROR
                logger.error("Tushare连接测试失败")
                return False
                
        except Exception as e:
            self.status = DataSourceStatus.ERROR
            logger.error(f"Tushare初始化失败: {e}")
            return False
    
    def get_supported_data_types(self) -> List[DataType]:
        """获取支持的数据类型"""
        return [
            DataType.STOCK_BASIC,
            DataType.DAILY_QUOTES,
            DataType.FINANCIAL_DATA,
            DataType.INDEX_DATA,
            DataType.NEWS_DATA
        ]
    
    def check_availability(self) -> DataSourceStatus:
        """检查数据源可用性"""
        if not self.pro:
            return DataSourceStatus.UNAVAILABLE
        
        try:
            # 简单的可用性测试
            test_df = self.pro.stock_basic(list_status='L', fields='ts_code', limit=1)
            if test_df is not None and not test_df.empty:
                self.status = DataSourceStatus.AVAILABLE
            else:
                self.status = DataSourceStatus.ERROR
        except Exception as e:
            logger.warning(f"Tushare可用性检查失败: {e}")
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
            
            if request.data_type == DataType.STOCK_BASIC:
                data = self._fetch_stock_basic(request)
            elif request.data_type == DataType.DAILY_QUOTES:
                data = self._fetch_daily_quotes(request)
            elif request.data_type == DataType.FINANCIAL_DATA:
                data = self._fetch_financial_data(request)
            elif request.data_type == DataType.INDEX_DATA:
                data = self._fetch_index_data(request)
            elif request.data_type == DataType.NEWS_DATA:
                data = self._fetch_news_data(request)
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
        params = request.extra_params or {}
        
        df = self.pro.stock_basic(
            exchange=params.get('exchange', ''),
            list_status=params.get('list_status', 'L'),
            fields=','.join(request.fields) if request.fields else 'ts_code,symbol,name,area,industry,market,list_date,is_hs'
        )
        
        if df.empty:
            return df
        
        # 数据清洗
        if 'list_date' in df.columns:
            df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d', errors='coerce')
        
        # 过滤ST股票
        if data_settings.exclude_st_stocks and 'name' in df.columns:
            df = df[~df['name'].str.contains('ST|退', na=False)]
        
        # 添加list_status列（如果不存在）
        if 'list_status' not in df.columns:
            df['list_status'] = 'L'
        
        return df
    
    def _fetch_daily_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取日线行情数据"""
        params = request.extra_params or {}
        
        # 处理日期格式
        start_date = self._format_date(request.start_date) if request.start_date else ''
        end_date = self._format_date(request.end_date) if request.end_date else ''
        trade_date = params.get('trade_date', '')
        
        # 处理股票代码
        ts_code = request.symbol or ''
        
        df = self.pro.daily(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=','.join(request.fields) if request.fields else 'ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
        )
        
        if df.empty:
            return df
        
        # 数据类型转换和列名标准化
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        
        # 重命名列以符合系统标准
        column_mapping = {
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price',
            'change': 'change_amount'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        # 按日期排序
        df = df.sort_values(['ts_code', 'trade_date'])
        
        return df
    
    def _fetch_financial_data(self, request: DataRequest) -> pd.DataFrame:
        """获取财务数据"""
        params = request.extra_params or {}
        ts_code = request.symbol or ''
        
        # 根据具体需求选择财务数据接口
        data_subtype = params.get('subtype', 'income')
        
        if data_subtype == 'income':
            df = self.pro.income(ts_code=ts_code, period=params.get('period', ''))
        elif data_subtype == 'balancesheet':
            df = self.pro.balancesheet(ts_code=ts_code, period=params.get('period', ''))
        elif data_subtype == 'cashflow':
            df = self.pro.cashflow(ts_code=ts_code, period=params.get('period', ''))
        else:
            raise DataSourceError(f"不支持的财务数据子类型: {data_subtype}", self.name)
        
        return df
    
    def _fetch_index_data(self, request: DataRequest) -> pd.DataFrame:
        """获取指数数据"""
        params = request.extra_params or {}
        
        start_date = self._format_date(request.start_date) if request.start_date else ''
        end_date = self._format_date(request.end_date) if request.end_date else ''
        
        df = self.pro.index_daily(
            ts_code=request.symbol or '',
            start_date=start_date,
            end_date=end_date,
            fields=','.join(request.fields) if request.fields else 'ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
        )
        
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        
        return df
    
    def _fetch_news_data(self, request: DataRequest) -> pd.DataFrame:
        """获取新闻数据"""
        params = request.extra_params or {}
        
        start_date = self._format_date(request.start_date) if request.start_date else ''
        end_date = self._format_date(request.end_date) if request.end_date else ''
        
        df = self.pro.news(
            src=params.get('src', ''),
            start_date=start_date,
            end_date=end_date
        )
        
        if not df.empty and 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
        
        return df
    
    def _format_date(self, date_input: Union[str, date, datetime]) -> str:
        """格式化日期为YYYYMMDD格式"""
        if isinstance(date_input, str):
            if len(date_input) == 8 and date_input.isdigit():
                return date_input
            else:
                # 尝试解析其他格式
                try:
                    dt = pd.to_datetime(date_input)
                    return dt.strftime('%Y%m%d')
                except:
                    return date_input
        elif isinstance(date_input, (date, datetime)):
            return date_input.strftime('%Y%m%d')
        else:
            return str(date_input)
    
    def close(self):
        """关闭连接"""
        self.pro = None
        self.status = DataSourceStatus.UNAVAILABLE
        super().close()
