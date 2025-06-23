"""
AData数据源实现
适配统一数据源接口，提供A股市场数据
"""
import pandas as pd
import adata
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date, timedelta
import time
from loguru import logger

from config.settings import data_settings
from .base_data_source import (
    BaseDataSource, DataRequest, DataResponse, DataType,
    DataSourceType, DataSourceStatus, DataSourceError,
    RateLimitError, AuthenticationError, NetworkError
)


class AdataDataSource(BaseDataSource):
    """AData数据源实现"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化AData数据源

        Args:
            config: 配置参数
        """
        super().__init__(
            name="adata",
            source_type=DataSourceType.CUSTOM,
            config=config or {}
        )

        # AData无需API key，免费使用
        self.timeout = self.config.get('timeout', 30)
        self.rate_limit = self.config.get('rate_limit', 100)  # 每分钟请求限制

        # 请求统计和频率控制
        self.request_count = 0
        self.last_request_time = None
        self.request_interval = 60 / self.rate_limit  # 请求间隔（秒）

        logger.info(f"AData数据源初始化完成，频率限制: {self.rate_limit}/分钟")

    def initialize(self) -> bool:
        """初始化数据源"""
        try:
            # 测试AData连接 - 获取股票列表
            test_df = adata.stock.info.all_code()
            if test_df is not None and not test_df.empty:
                self.status = DataSourceStatus.READY
                self._initialized = True  # 设置初始化标志
                logger.success("AData数据源初始化成功")
                return True
            else:
                self.status = DataSourceStatus.ERROR
                logger.error("AData数据源初始化失败：无法获取测试数据")
                return False

        except Exception as e:
            self.status = DataSourceStatus.ERROR
            logger.error(f"AData数据源初始化失败: {e}")
            return False

    def check_availability(self) -> DataSourceStatus:
        """检查数据源可用性"""
        try:
            # 简单测试请求
            test_df = adata.stock.info.all_code()
            if test_df is not None and not test_df.empty:
                self.status = DataSourceStatus.AVAILABLE
                logger.info("AData数据源可用性检查成功")
            else:
                self.status = DataSourceStatus.ERROR
                logger.warning("AData数据源可用性检查失败：返回空数据")
        except Exception as e:
            self.status = DataSourceStatus.ERROR
            logger.error(f"AData可用性检查失败: {e}")

        return self.status

    def get_supported_data_types(self) -> List[DataType]:
        """获取支持的数据类型"""
        return [
            DataType.STOCK_BASIC,
            DataType.STOCK_DAILY
        ]

    def validate_request(self, request: DataRequest) -> bool:
        """验证请求参数"""
        if request.data_type not in self.get_supported_data_types():
            return False

        # 股票代码验证
        if request.data_type in [DataType.STOCK_DAILY] and not request.symbol:
            return False

        return True

    def _check_rate_limit(self) -> bool:
        """检查频率限制"""
        current_time = time.time()

        if self.last_request_time:
            time_diff = current_time - self.last_request_time
            if time_diff < self.request_interval:
                time.sleep(self.request_interval - time_diff)

        return True

    def _update_request_stats(self):
        """更新请求统计"""
        self.request_count += 1
        self.last_request_time = time.time()

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
            self._check_rate_limit()
            self._update_request_stats()

            # 处理symbol，去除.SH/.SZ等后缀
            symbol = request.symbol
            if symbol and isinstance(symbol, str):
                original_symbol = symbol
                if symbol.endswith('.SH') or symbol.endswith('.SZ') or symbol.endswith('.BJ'):
                    symbol = symbol.split('.')[0]
                logger.error(f"AData fetch_data: 原始symbol={original_symbol}, 处理后symbol={symbol}")
                # 用处理后的symbol替换request.symbol
                request = request.copy(update={"symbol": symbol})

            if request.data_type == DataType.STOCK_BASIC:
                data = self._fetch_stock_basic(request)
            elif request.data_type == DataType.STOCK_DAILY:
                data = self._fetch_daily_quotes(request)
            elif request.data_type == DataType.INDEX_DATA:
                data = self._fetch_index_data(request)
            else:
                raise DataSourceError(f"AData暂不支持的数据类型: {request.data_type}", self.name)

            return DataResponse(
                data=data,
                source=self.name,
                data_type=request.data_type,
                timestamp=datetime.now(),
                success=True
            )

        except Exception as e:
            logger.error(f"AData获取数据失败: {e}")
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
            # 获取所有股票代码
            df = adata.stock.info.all_code()
            
            if df.empty:
                return df

            # 标准化列名
            df = self._standardize_stock_basic_data(df)
            
            return df

        except Exception as e:
            logger.error(f"获取股票基础信息失败: {e}")
            return pd.DataFrame()

    def _standardize_stock_basic_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化股票基础信息数据"""
        try:
            # 检查并重命名列名
            column_mapping = {
                'stock_code': 'ts_code',
                'short_name': 'name',
                'exchange': 'exchange',
                'list_date': 'list_date'
            }

            # 重命名存在的列
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})

            # 确保必需的列存在
            required_columns = ['ts_code', 'name']
            for col in required_columns:
                if col not in df.columns:
                    if col == 'ts_code' and 'stock_code' in df.columns:
                        df['ts_code'] = df['stock_code']
                    elif col == 'name' and 'short_name' in df.columns:
                        df['name'] = df['short_name']
                    else:
                        df[col] = ''

            return df

        except Exception as e:
            logger.error(f"标准化股票基础信息数据失败: {e}")
            return df

    def _fetch_daily_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取日线行情数据"""
        try:
            stock_code = request.symbol
            start_date = request.start_date
            end_date = request.end_date

            # 格式化日期
            if start_date:
                start_date = start_date.strftime('%Y%m%d') if isinstance(start_date, (date, datetime)) else str(start_date).replace('-', '')
            if end_date:
                end_date = end_date.strftime('%Y%m%d') if isinstance(end_date, (date, datetime)) else str(end_date).replace('-', '')

            # 获取日线数据 - 使用正确的AData API
            df = adata.stock.market.get_market(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            logger.info(f"AData原始日线数据返回: 列={list(df.columns)}\n前5行=\n{df.head()}")

            if df.empty:
                return df

            # 标准化数据
            df = self._standardize_daily_quotes_data(df, stock_code)

            return df

        except Exception as e:
            logger.error(f"获取日线行情数据失败: {e}")
            return pd.DataFrame()

    def _standardize_daily_quotes_data(self, df: pd.DataFrame, ts_code: str) -> pd.DataFrame:
        """标准化日线行情数据"""
        try:
            # 先强制重命名volume为vol
            if 'volume' in df.columns:
                df = df.rename(columns={'volume': 'vol'})
            # 其余字段重命名
            column_mapping = {
                'date': 'trade_date',
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price',
                'amount': 'amount',
                'change': 'pct_chg',
                'pct_chg': 'pct_chg'
            }
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            # 添加股票代码列
            df['ts_code'] = ts_code
            # 确保vol为整数类型
            if 'vol' in df.columns:
                df['vol'] = df['vol'].fillna(0).astype(int)
            # 确保日期格式正确
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df
        except Exception as e:
            logger.error(f"标准化日线行情数据失败: {e}")
            return df

    def _fetch_index_data(self, request: DataRequest) -> pd.DataFrame:
        """获取指数数据"""
        try:
            index_code = request.symbol or '000001'  # 默认上证指数
            start_date = request.start_date
            end_date = request.end_date

            # 格式化日期
            if start_date:
                start_date = start_date.strftime('%Y%m%d') if isinstance(start_date, (date, datetime)) else str(start_date).replace('-', '')
            # end_date 不传递给API

            # 获取指数数据 - 只传index_code和start_date
            df = adata.stock.market.get_market_index(
                index_code=index_code,
                start_date=start_date
            )
            logger.info(f"AData原始指数数据返回: 列={list(df.columns)}\n前5行=\n{df.head()}")

            if df.empty:
                return df

            # 如果有end_date，手动筛选
            if end_date:
                end_date_fmt = end_date.strftime('%Y%m%d') if isinstance(end_date, (date, datetime)) else str(end_date).replace('-', '')
                if 'trade_date' in df.columns:
                    df = df[df['trade_date'] <= end_date_fmt]

            # 标准化数据
            df = self._standardize_daily_quotes_data(df, index_code)

            return df

        except Exception as e:
            logger.error(f"获取指数数据失败: {e}")
            return pd.DataFrame()

    def close(self):
        """关闭数据源"""
        self.status = DataSourceStatus.CLOSED
        logger.info("AData数据源已关闭") 