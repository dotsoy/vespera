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
        self.rate_limit = self.config.get('rate_limit', 200)  # 每分钟请求限制，降低以避免封禁

        # 请求统计和频率控制
        self.request_count = 0
        self.last_request_time = None
        self.request_interval = 60 / self.rate_limit  # 请求间隔（秒）

        # 多接口轮换机制
        self._interface_usage = {}  # 记录每个接口的使用次数
        self._interface_cooldown = {}  # 记录接口冷却时间
        self._interface_errors = {}  # 记录接口错误次数
        self._current_interface_index = 0  # 当前使用的接口索引

        # 接口轮换配置
        self.max_interface_requests = 50  # 单个接口最大连续请求次数
        self.interface_cooldown_time = 300  # 接口冷却时间（秒）
        self.max_interface_errors = 3  # 接口最大错误次数

        # 降低冷却阈值，确保请求不被频繁拒绝
        self.cool_down_threshold = 0.5  # 降低冷却阈值

        logger.info(f"AkShare数据源初始化完成，频率限制: {self.rate_limit}/分钟，支持多接口轮换")

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
                self.status = DataSourceStatus.AVAILABLE
                logger.info("AkShare数据源可用性检查成功")
            else:
                self.status = DataSourceStatus.ERROR
                logger.warning("AkShare数据源可用性检查失败：返回空数据")
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

    def _get_available_interfaces(self, data_type: DataType) -> List[str]:
        """获取指定数据类型的可用接口列表"""
        current_time = time.time()

        if data_type == DataType.DAILY_QUOTES:
            all_interfaces = [
                'stock_zh_a_hist',      # 主要接口
                'stock_zh_a_daily',     # 备用接口1
                'tool_trade_date_hist_sina'  # 备用接口2（如果可用）
            ]
        elif data_type == DataType.STOCK_BASIC:
            all_interfaces = [
                'stock_info_a_code_name',  # 主要接口
                'stock_zh_a_spot_em',      # 备用接口1
                'stock_zh_a_spot'          # 备用接口2
            ]
        elif data_type == DataType.INDEX_DATA:
            all_interfaces = [
                'stock_zh_index_daily',    # 主要接口
                'index_zh_a_hist',         # 备用接口1
                'stock_zh_index_daily_em'  # 备用接口2
            ]
        else:
            return ['default']

        # 过滤掉冷却中或错误过多的接口
        available_interfaces = []
        for interface in all_interfaces:
            # 检查冷却时间
            if interface in self._interface_cooldown:
                if current_time < self._interface_cooldown[interface]:
                    logger.debug(f"接口 {interface} 仍在冷却期")
                    continue
                else:
                    # 冷却期结束，清除记录
                    del self._interface_cooldown[interface]

            # 检查错误次数
            error_count = self._interface_errors.get(interface, 0)
            if error_count >= self.max_interface_errors:
                logger.debug(f"接口 {interface} 错误次数过多 ({error_count})")
                continue

            available_interfaces.append(interface)

        return available_interfaces or [all_interfaces[0]]  # 至少返回一个接口

    def _select_best_interface(self, data_type: DataType) -> str:
        """智能选择最佳接口"""
        available_interfaces = self._get_available_interfaces(data_type)

        if len(available_interfaces) == 1:
            return available_interfaces[0]

        # 根据使用次数选择最少使用的接口
        interface_scores = {}
        for interface in available_interfaces:
            usage_count = self._interface_usage.get(interface, 0)
            error_count = self._interface_errors.get(interface, 0)

            # 计算接口评分（使用次数越少，错误次数越少，评分越高）
            score = 100 - usage_count - (error_count * 10)
            interface_scores[interface] = score

        # 选择评分最高的接口
        best_interface = max(interface_scores.items(), key=lambda x: x[1])[0]
        logger.debug(f"选择接口: {best_interface}, 评分: {interface_scores}")

        return best_interface

    def _record_interface_usage(self, interface: str, success: bool, error_msg: str = None):
        """记录接口使用情况"""
        current_time = time.time()

        # 更新使用次数
        self._interface_usage[interface] = self._interface_usage.get(interface, 0) + 1

        if not success:
            # 记录错误
            self._interface_errors[interface] = self._interface_errors.get(interface, 0) + 1

            # 如果错误次数达到阈值或者是频率限制错误，设置冷却时间
            if (self._interface_errors[interface] >= self.max_interface_errors or
                (error_msg and any(keyword in error_msg.lower() for keyword in ['频率', 'rate', 'limit', '限制']))):

                cooldown_time = current_time + self.interface_cooldown_time
                self._interface_cooldown[interface] = cooldown_time
                logger.warning(f"接口 {interface} 进入冷却期，冷却至: {datetime.fromtimestamp(cooldown_time)}")
        else:
            # 成功时重置错误计数
            if interface in self._interface_errors:
                self._interface_errors[interface] = max(0, self._interface_errors[interface] - 1)

        # 如果单个接口使用次数过多，强制冷却
        if self._interface_usage[interface] >= self.max_interface_requests:
            cooldown_time = current_time + self.interface_cooldown_time
            self._interface_cooldown[interface] = cooldown_time
            self._interface_usage[interface] = 0  # 重置使用计数
            logger.info(f"接口 {interface} 使用次数达到上限，强制冷却")

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
        """获取股票基础信息（支持多接口轮换）"""
        # 选择最佳接口
        interface = self._select_best_interface(DataType.STOCK_BASIC)
        logger.debug(f"使用接口 {interface} 获取股票基础信息")

        try:
            df = self._fetch_stock_basic_with_interface(interface)

            if df.empty:
                logger.warning(f"接口 {interface} 返回空数据")
                self._record_interface_usage(interface, False, "返回空数据")

                # 尝试备用接口
                available_interfaces = self._get_available_interfaces(DataType.STOCK_BASIC)
                for backup_interface in available_interfaces:
                    if backup_interface != interface:
                        logger.info(f"尝试备用接口: {backup_interface}")
                        try:
                            df = self._fetch_stock_basic_with_interface(backup_interface)
                            if not df.empty:
                                self._record_interface_usage(backup_interface, True)
                                break
                        except Exception as e:
                            logger.warning(f"备用接口 {backup_interface} 也失败: {e}")
                            self._record_interface_usage(backup_interface, False, str(e))
                            continue
            else:
                self._record_interface_usage(interface, True)

            if df.empty:
                return df

            # 标准化数据格式
            df = self._standardize_stock_basic_data(df)
            return df

        except Exception as e:
            error_msg = str(e)
            logger.error(f"接口 {interface} 获取股票基础信息失败: {error_msg}")
            self._record_interface_usage(interface, False, error_msg)

            # 尝试备用接口
            available_interfaces = self._get_available_interfaces(DataType.STOCK_BASIC)
            for backup_interface in available_interfaces:
                if backup_interface != interface:
                    logger.info(f"尝试备用接口: {backup_interface}")
                    try:
                        df = self._fetch_stock_basic_with_interface(backup_interface)
                        if not df.empty:
                            df = self._standardize_stock_basic_data(df)
                            self._record_interface_usage(backup_interface, True)
                            return df
                    except Exception as backup_e:
                        logger.warning(f"备用接口 {backup_interface} 也失败: {backup_e}")
                        self._record_interface_usage(backup_interface, False, str(backup_e))
                        continue

            # 所有接口都失败
            raise DataSourceError(f"所有接口都无法获取数据: {error_msg}", self.name)

    def _fetch_stock_basic_with_interface(self, interface: str) -> pd.DataFrame:
        """使用指定接口获取股票基础信息"""
        if interface == 'stock_info_a_code_name':
            return ak.stock_info_a_code_name()
        elif interface == 'stock_zh_a_spot_em':
            # 备用接口1 - 东方财富实时数据
            try:
                df = ak.stock_zh_a_spot_em()
                # 只保留需要的列
                if not df.empty and '代码' in df.columns and '名称' in df.columns:
                    return df[['代码', '名称']].rename(columns={'代码': 'code', '名称': 'name'})
                return df
            except AttributeError:
                raise DataSourceError(f"接口 {interface} 不存在", self.name)
        elif interface == 'stock_zh_a_spot':
            # 备用接口2
            try:
                return ak.stock_zh_a_spot()
            except AttributeError:
                raise DataSourceError(f"接口 {interface} 不存在", self.name)
        else:
            # 默认使用主接口
            return ak.stock_info_a_code_name()

    def _standardize_stock_basic_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化股票基础信息数据格式"""
        if df.empty:
            return df

        # 标准化列名
        column_mapping = {
            'code': 'ts_code',
            'name': 'name',
            '代码': 'ts_code',
            '名称': 'name',
            'symbol': 'ts_code'
        }

        df = df.rename(columns=column_mapping)

        # 确保必要的列存在
        if 'ts_code' not in df.columns or 'name' not in df.columns:
            logger.error("股票基础信息缺少必要字段")
            return pd.DataFrame()

        # 添加标准字段
        df['symbol'] = df['ts_code']
        df['area'] = '未知'
        df['industry'] = '未知'
        df['market'] = df['ts_code'].apply(self._get_market_from_code)
        df['exchange'] = df['ts_code'].apply(self._get_exchange_from_code)
        df['list_status'] = 'L'
        df['is_hs'] = 'N'
        df['list_date'] = '20000101'

        # 过滤ST股票
        if data_settings.exclude_st_stocks:
            df = df[~df['name'].str.contains('ST|退', na=False)]

        return df

    def _fetch_daily_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取日线行情数据（支持多接口轮换）"""
        symbol = request.symbol
        if not symbol:
            raise DataSourceError("股票代码不能为空", self.name)

        # 处理股票代码格式
        symbol = self._format_symbol(symbol)

        # 处理日期参数
        start_date = request.start_date.strftime('%Y%m%d') if request.start_date else None
        end_date = request.end_date.strftime('%Y%m%d') if request.end_date else None

        # 选择最佳接口
        interface = self._select_best_interface(DataType.DAILY_QUOTES)
        logger.debug(f"使用接口 {interface} 获取 {symbol} 的日线数据")

        try:
            df = self._fetch_daily_quotes_with_interface(interface, symbol, start_date, end_date)

            if df.empty:
                logger.warning(f"接口 {interface} 返回空数据")
                self._record_interface_usage(interface, False, "返回空数据")

                # 尝试备用接口
                available_interfaces = self._get_available_interfaces(DataType.DAILY_QUOTES)
                for backup_interface in available_interfaces:
                    if backup_interface != interface:
                        logger.info(f"尝试备用接口: {backup_interface}")
                        try:
                            df = self._fetch_daily_quotes_with_interface(backup_interface, symbol, start_date, end_date)
                            if not df.empty:
                                self._record_interface_usage(backup_interface, True)
                                break
                        except Exception as e:
                            logger.warning(f"备用接口 {backup_interface} 也失败: {e}")
                            self._record_interface_usage(backup_interface, False, str(e))
                            continue
            else:
                self._record_interface_usage(interface, True)

            if df.empty:
                return df

            # 标准化数据格式
            df = self._standardize_daily_quotes_data(df, request.symbol)
            return df

        except Exception as e:
            error_msg = str(e)
            logger.error(f"接口 {interface} 获取日线行情失败: {error_msg}")
            self._record_interface_usage(interface, False, error_msg)

            # 尝试备用接口
            available_interfaces = self._get_available_interfaces(DataType.DAILY_QUOTES)
            for backup_interface in available_interfaces:
                if backup_interface != interface:
                    logger.info(f"尝试备用接口: {backup_interface}")
                    try:
                        df = self._fetch_daily_quotes_with_interface(backup_interface, symbol, start_date, end_date)
                        if not df.empty:
                            df = self._standardize_daily_quotes_data(df, request.symbol)
                            self._record_interface_usage(backup_interface, True)
                            return df
                    except Exception as backup_e:
                        logger.warning(f"备用接口 {backup_interface} 也失败: {backup_e}")
                        self._record_interface_usage(backup_interface, False, str(backup_e))
                        continue

            # 所有接口都失败
            raise DataSourceError(f"所有接口都无法获取数据: {error_msg}", self.name)

    def _fetch_daily_quotes_with_interface(self, interface: str, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用指定接口获取日线数据"""
        if interface == 'stock_zh_a_hist':
            return ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
        elif interface == 'stock_zh_a_daily':
            # 备用接口1 - 如果AkShare有这个接口
            try:
                return ak.stock_zh_a_daily(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date
                )
            except AttributeError:
                # 如果接口不存在，抛出异常
                raise DataSourceError(f"接口 {interface} 不存在", self.name)
        elif interface == 'tool_trade_date_hist_sina':
            # 备用接口2 - 使用新浪数据
            try:
                df = ak.tool_trade_date_hist_sina(symbol=symbol)
                if not df.empty:
                    logger.info("备用接口 tool_trade_date_hist_sina 获取日线行情成功")
                    return df
            except Exception as e:
                logger.warning(f"备用接口 tool_trade_date_hist_sina 也失败: {e}")
                raise DataSourceError(f"接口 {interface} 不存在", self.name)
        else:
            # 默认使用主接口
            return ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )

    def _standardize_daily_quotes_data(self, df: pd.DataFrame, ts_code: str) -> pd.DataFrame:
        """标准化日线数据格式"""
        if df.empty:
            return df

        # 统一字段名
        df = df.rename(columns={
            'date': 'trade_date',
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price',
            'volume': 'vol'
        })

        # 添加股票代码
        df['ts_code'] = ts_code

        # 确保日期格式正确
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')

        # 添加缺失的字段
        if 'pre_close' not in df.columns:
            df['pre_close'] = 0.0
        if 'change_amount' not in df.columns:
            df['change_amount'] = 0.0

        # 过滤无交易的日线数据（成交量为0或缺失的日期）
        df = df[df['vol'] > 0]

        return df

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

    def get_interface_status(self) -> Dict[str, Any]:
        """获取接口状态信息"""
        current_time = time.time()
        status = {
            'total_requests': self.request_count,
            'interfaces': {}
        }

        for data_type in [DataType.DAILY_QUOTES, DataType.STOCK_BASIC, DataType.INDEX_DATA]:
            interfaces = self._get_available_interfaces(data_type)
            for interface in interfaces:
                usage_count = self._interface_usage.get(interface, 0)
                error_count = self._interface_errors.get(interface, 0)

                cooldown_remaining = 0
                if interface in self._interface_cooldown:
                    cooldown_remaining = max(0, self._interface_cooldown[interface] - current_time)

                status['interfaces'][interface] = {
                    'usage_count': usage_count,
                    'error_count': error_count,
                    'cooldown_remaining': cooldown_remaining,
                    'is_available': cooldown_remaining == 0 and error_count < self.max_interface_errors
                }

        return status

    def reset_interface_stats(self):
        """重置接口统计信息"""
        self._interface_usage.clear()
        self._interface_cooldown.clear()
        self._interface_errors.clear()
        logger.info("接口统计信息已重置")

    def force_interface_cooldown(self, interface: str, duration: int = None):
        """强制设置接口冷却时间"""
        duration = duration or self.interface_cooldown_time
        cooldown_time = time.time() + duration
        self._interface_cooldown[interface] = cooldown_time
        logger.info(f"强制设置接口 {interface} 冷却 {duration} 秒")

    def close(self):
        """关闭数据源连接"""
        # AkShare无需显式关闭连接
        self.status = DataSourceStatus.CLOSED

        # 输出接口使用统计
        if self._interface_usage:
            logger.info("接口使用统计:")
            for interface, count in self._interface_usage.items():
                error_count = self._interface_errors.get(interface, 0)
                logger.info(f"  {interface}: 使用 {count} 次, 错误 {error_count} 次")

        logger.info("AkShare数据源已关闭")
