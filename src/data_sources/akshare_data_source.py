"""
AkShare数据源实现
适配统一数据源接口，提供A股市场数据
"""
import pandas as pd
import akshare as ak
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


class AkShareDataSource(BaseDataSource):
    """AkShare数据源实现"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化AkShare数据源

        Args:
            config: 配置参数
        """
        super().__init__(
            name="akshare",
            source_type=DataSourceType.AKSHARE,
            config=config or {}
        )

        # AkShare无需API key，免费使用
        self.timeout = self.config.get('timeout', data_settings.akshare_timeout)
        self.rate_limit = self.config.get('rate_limit', 1000)  # 每分钟请求限制

        # 请求统计
        self.request_count = 0
        self.last_request_time = None
        self.request_interval = 60 / self.rate_limit  # 请求间隔（秒）

        logger.info(f"AkShare数据源初始化完成，频率限制: {self.rate_limit}/分钟")

    def initialize(self) -> bool:
        """初始化数据源"""
        try:
            # 测试AkShare连接
            test_df = ak.stock_info_a_code_name()
            if test_df is not None and not test_df.empty:
                self.status = DataSourceStatus.READY
                logger.success("AkShare数据源初始化成功")
                return True
            else:
                self.status = DataSourceStatus.ERROR
                logger.error("AkShare数据源初始化失败：无法获取测试数据")
                return False

        except Exception as e:
            self.status = DataSourceStatus.ERROR
            logger.error(f"AkShare数据源初始化失败: {e}")
            return False

    def check_availability(self) -> DataSourceStatus:
        """检查数据源可用性"""
        try:
            # 简单测试请求
            test_df = ak.stock_info_a_code_name()
            if test_df is not None and not test_df.empty:
                self.status = DataSourceStatus.READY
            else:
                self.status = DataSourceStatus.ERROR
        except Exception as e:
            self.status = DataSourceStatus.ERROR
            logger.error(f"AkShare可用性检查失败: {e}")

        return self.status

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

        # 股票代码验证
        if request.data_type in [DataType.DAILY_QUOTES] and not request.symbol:
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

        if not self._check_rate_limit():
            raise RateLimitError("请求频率超限", self.name)

        try:
            self._update_request_stats()

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
                success=True,
                metadata={"records": len(data)}
            )

        except Exception as e:
            logger.error(f"AkShare数据获取失败: {e}")
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
            # 获取A股股票基础信息
            df = ak.stock_info_a_code_name()

            if df.empty:
                return df

            # 标准化列名
            df = df.rename(columns={
                'code': 'ts_code',
                'name': 'name'
            })

            # 添加标准字段
            df['symbol'] = df['ts_code']
            df['market'] = df['ts_code'].apply(self._get_market_from_code)
            df['exchange'] = df['ts_code'].apply(self._get_exchange_from_code)
            df['list_status'] = 'L'  # 默认为上市状态

            # 过滤ST股票
            if data_settings.exclude_st_stocks:
                df = df[~df['name'].str.contains('ST|退', na=False)]

            return df

        except Exception as e:
            logger.error(f"获取股票基础信息失败: {e}")
            return pd.DataFrame()

    def _fetch_daily_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取日线行情数据"""
        try:
            symbol = request.symbol
            if not symbol:
                raise DataSourceError("股票代码不能为空", self.name)

            # 处理股票代码格式
            symbol = self._format_symbol(symbol)

            # 处理日期参数
            start_date = request.start_date.strftime('%Y%m%d') if request.start_date else None
            end_date = request.end_date.strftime('%Y%m%d') if request.end_date else None

            # 获取日线数据
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )

            if df.empty:
                return df

            # 标准化列名和数据格式
            df = df.rename(columns={
                '日期': 'trade_date',
                '开盘': 'open_price',
                '收盘': 'close_price',
                '最高': 'high_price',
                '最低': 'low_price',
                '成交量': 'vol',
                '成交额': 'amount',
                '涨跌幅': 'pct_chg',
                '涨跌额': 'change_amount',
                '换手率': 'turnover_rate',
                '股票代码': 'symbol',
                '振幅': 'amplitude'
            })

            # 删除中文列名（如果还有的话）
            chinese_columns = [col for col in df.columns if any('\u4e00' <= char <= '\u9fff' for char in col)]
            if chinese_columns:
                df = df.drop(columns=chinese_columns)

            # 添加股票代码
            df['ts_code'] = request.symbol

            # 确保日期格式正确
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')

            return df

        except Exception as e:
            logger.error(f"获取日线行情失败: {e}")
            return pd.DataFrame()

    def _fetch_index_data(self, request: DataRequest) -> pd.DataFrame:
        """获取指数数据"""
        try:
            symbol = request.symbol or "000001"  # 默认上证指数

            # 处理日期参数
            start_date = request.start_date.strftime('%Y%m%d') if request.start_date else None
            end_date = request.end_date.strftime('%Y%m%d') if request.end_date else None

            # 获取指数数据
            df = ak.stock_zh_index_daily(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )

            if df.empty:
                return df

            # 标准化列名
            df = df.rename(columns={
                'date': 'trade_date',
                'open': 'open_price',
                'close': 'close_price',
                'high': 'high_price',
                'low': 'low_price',
                'volume': 'vol',
                'amount': 'amount'
            })

            # 添加指数代码
            df['ts_code'] = symbol

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

            # 处理股票代码格式
            symbol = self._format_symbol(symbol)

            # 获取财务数据（这里可以根据需要选择不同的财务数据接口）
            df = ak.stock_financial_abstract(symbol=symbol)

            if df.empty:
                return df

            # 添加股票代码
            df['ts_code'] = request.symbol

            return df

        except Exception as e:
            logger.error(f"获取财务数据失败: {e}")
            return pd.DataFrame()

    def _format_symbol(self, symbol: str) -> str:
        """格式化股票代码"""
        # AkShare使用6位数字代码，去掉交易所后缀
        if '.' in symbol:
            return symbol.split('.')[0]
        return symbol

    def _get_market_from_code(self, code: str) -> str:
        """根据股票代码获取市场"""
        if code.startswith('00'):
            return '深交所主板'
        elif code.startswith('30'):
            return '创业板'
        elif code.startswith('60'):
            return '上交所主板'
        elif code.startswith('68'):
            return '科创板'
        elif code.startswith('8') or code.startswith('4'):
            return '北交所'
        else:
            return '其他'

    def _get_exchange_from_code(self, code: str) -> str:
        """根据股票代码获取交易所"""
        if code.startswith(('00', '30')):
            return 'SZSE'
        elif code.startswith(('60', '68')):
            return 'SSE'
        elif code.startswith(('8', '4')):
            return 'BSE'
        else:
            return 'OTHER'

    def close(self):
        """关闭数据源连接"""
        # AkShare无需显式关闭连接
        self.status = DataSourceStatus.CLOSED
        logger.info("AkShare数据源已关闭")