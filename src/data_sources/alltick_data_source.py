"""
AllTick数据源实现
https://apis.alltick.co/ - 专业金融数据API
"""
import pandas as pd
import requests
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date, timedelta
import time
import json
from loguru import logger

from .base_data_source import (
    BaseDataSource, DataRequest, DataResponse, DataType, 
    DataSourceType, DataSourceStatus, DataSourceError,
    RateLimitError, AuthenticationError, NetworkError
)


class AllTickDataSource(BaseDataSource):
    """AllTick数据源实现"""
    
    def __init__(self, token: str, config: Optional[Dict[str, Any]] = None):
        default_config = {
            'rate_limit': 100,   # 每分钟100次请求 (根据AllTick限制调整)
            'priority': 2,       # 中等优先级
            'description': 'AllTick专业金融数据API',
            'cost_per_request': 0.001,  # 根据实际费用调整
            'timeout': 30
        }
        if config:
            default_config.update(config)
            
        super().__init__("AllTick", DataSourceType.ALLTICK, default_config)
        self.token = token
        self.base_url = "https://apis.alltick.co"
        self.session = requests.Session()
        
        # 设置请求头
        self.session.headers.update({
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'User-Agent': 'QimingStar/1.0'
        })
        
        # 请求频率控制
        self._last_request_time = 0
        self._min_request_interval = 0.6  # 最小请求间隔(秒) - 每分钟100次
        
    def initialize(self) -> bool:
        """初始化AllTick连接"""
        try:
            if not self.token:
                logger.error("AllTick token 未配置")
                self.status = DataSourceStatus.ERROR
                return False
            
            # 测试API连接 - 获取用户信息
            test_url = f"{self.base_url}/user/info"
            
            response = self.session.get(test_url, timeout=self.config.get('timeout', 30))
            
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 0:  # AllTick成功响应码
                    self.status = DataSourceStatus.AVAILABLE
                    logger.info("AllTick数据源初始化成功")
                    logger.info(f"用户信息: {data.get('data', {})}")
                    return True
                else:
                    logger.error(f"AllTick API错误: {data.get('msg', 'Unknown error')}")
                    self.status = DataSourceStatus.ERROR
                    return False
            elif response.status_code == 401:
                logger.error("AllTick认证失败，请检查token")
                self.status = DataSourceStatus.ERROR
                return False
            else:
                logger.error(f"AllTick API请求失败: {response.status_code}")
                self.status = DataSourceStatus.ERROR
                return False
                
        except Exception as e:
            self.status = DataSourceStatus.ERROR
            logger.error(f"AllTick初始化失败: {e}")
            return False
    
    def get_supported_data_types(self) -> List[DataType]:
        """获取支持的数据类型"""
        return [
            DataType.STOCK_BASIC,
            DataType.DAILY_QUOTES,
            DataType.MINUTE_QUOTES,
            DataType.TICK_DATA,
            DataType.INDEX_DATA
        ]
    
    def check_availability(self) -> DataSourceStatus:
        """检查数据源可用性"""
        try:
            # 简单的可用性测试
            test_url = f"{self.base_url}/user/info"
            
            response = self.session.get(test_url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 0:
                    self.status = DataSourceStatus.AVAILABLE
                else:
                    self.status = DataSourceStatus.ERROR
            elif response.status_code == 429:
                self.status = DataSourceStatus.LIMITED
            else:
                self.status = DataSourceStatus.ERROR
                
        except Exception as e:
            logger.warning(f"AllTick可用性检查失败: {e}")
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
        
        # 控制请求频率
        self._wait_for_rate_limit()
        
        try:
            self._update_request_stats()
            
            if request.data_type == DataType.STOCK_BASIC:
                data = self._fetch_stock_basic(request)
            elif request.data_type == DataType.DAILY_QUOTES:
                data = self._fetch_daily_quotes(request)
            elif request.data_type == DataType.MINUTE_QUOTES:
                data = self._fetch_minute_quotes(request)
            elif request.data_type == DataType.TICK_DATA:
                data = self._fetch_tick_data(request)
            elif request.data_type == DataType.INDEX_DATA:
                data = self._fetch_index_data(request)
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
            logger.error(f"AllTick获取数据失败: {e}")
            return DataResponse(
                data=pd.DataFrame(),
                source=self.name,
                data_type=request.data_type,
                timestamp=datetime.now(),
                success=False,
                error_message=str(e)
            )
    
    def _wait_for_rate_limit(self):
        """等待满足频率限制"""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self._min_request_interval:
            sleep_time = self._min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self._last_request_time = time.time()
    
    def _fetch_stock_basic(self, request: DataRequest) -> pd.DataFrame:
        """获取股票基础信息"""
        url = f"{self.base_url}/stock/symbol"
        
        params = {
            'market': request.extra_params.get('market', 'cn'),  # cn, us, hk等
            'type': request.extra_params.get('type', 'stock')
        }
        
        response = self.session.get(url, params=params, timeout=self.config.get('timeout', 30))
        
        if response.status_code != 200:
            raise NetworkError(f"API请求失败: {response.status_code}", self.name)
        
        data = response.json()
        
        if data.get('code') != 0:
            raise DataSourceError(f"API错误: {data.get('msg', 'Unknown error')}", self.name)
        
        symbols_data = data.get('data', [])
        
        if not symbols_data:
            return pd.DataFrame()
        
        # 转换为DataFrame
        records = []
        for symbol_info in symbols_data:
            record = {
                'ts_code': symbol_info.get('symbol', ''),
                'symbol': symbol_info.get('code', ''),
                'name': symbol_info.get('name', ''),
                'market': symbol_info.get('market', ''),
                'exchange': symbol_info.get('exchange', ''),
                'currency': symbol_info.get('currency', ''),
                'type': symbol_info.get('type', ''),
                'status': symbol_info.get('status', '')
            }
            records.append(record)
        
        return pd.DataFrame(records)
    
    def _fetch_daily_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取日线行情数据"""
        symbol = request.symbol
        if not symbol:
            raise DataSourceError("股票代码不能为空", self.name)
        
        url = f"{self.base_url}/stock/kline"
        
        params = {
            'symbol': symbol,
            'period': '1d',  # 日线
            'adjust': request.extra_params.get('adjust', 'qfq'),  # 前复权
        }
        
        # 处理日期参数
        if request.start_date:
            params['start_time'] = self._format_timestamp(request.start_date)
        if request.end_date:
            params['end_time'] = self._format_timestamp(request.end_date)
        
        response = self.session.get(url, params=params, timeout=self.config.get('timeout', 30))
        
        if response.status_code != 200:
            raise NetworkError(f"API请求失败: {response.status_code}", self.name)
        
        data = response.json()
        
        if data.get('code') != 0:
            raise DataSourceError(f"API错误: {data.get('msg', 'Unknown error')}", self.name)
        
        kline_data = data.get('data', [])
        
        if not kline_data:
            return pd.DataFrame()
        
        # 转换为DataFrame
        records = []
        for kline in kline_data:
            record = {
                'ts_code': symbol,
                'trade_date': self._format_date_from_timestamp(kline[0]),
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
        
        return df
    
    def _fetch_minute_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取分钟级行情数据"""
        symbol = request.symbol
        if not symbol:
            raise DataSourceError("股票代码不能为空", self.name)
        
        url = f"{self.base_url}/stock/kline"
        
        params = request.extra_params or {}
        period = params.get('period', '1m')  # 1m, 5m, 15m, 30m, 60m
        
        params = {
            'symbol': symbol,
            'period': period,
            'adjust': params.get('adjust', 'qfq')
        }
        
        # 处理日期参数
        if request.start_date:
            params['start_time'] = self._format_timestamp(request.start_date)
        if request.end_date:
            params['end_time'] = self._format_timestamp(request.end_date)
        
        response = self.session.get(url, params=params, timeout=self.config.get('timeout', 30))
        
        if response.status_code != 200:
            raise NetworkError(f"API请求失败: {response.status_code}", self.name)
        
        data = response.json()
        
        if data.get('code') != 0:
            raise DataSourceError(f"API错误: {data.get('msg', 'Unknown error')}", self.name)
        
        kline_data = data.get('data', [])
        
        if not kline_data:
            return pd.DataFrame()
        
        # 转换为DataFrame
        records = []
        for kline in kline_data:
            record = {
                'ts_code': symbol,
                'datetime': self._format_datetime_from_timestamp(kline[0]),
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
            df = df.sort_values('datetime')
        
        return df
    
    def _fetch_tick_data(self, request: DataRequest) -> pd.DataFrame:
        """获取逐笔数据"""
        symbol = request.symbol
        if not symbol:
            raise DataSourceError("股票代码不能为空", self.name)
        
        url = f"{self.base_url}/stock/tick"
        
        params = {
            'symbol': symbol,
            'date': request.extra_params.get('date', datetime.now().strftime('%Y-%m-%d'))
        }
        
        response = self.session.get(url, params=params, timeout=self.config.get('timeout', 30))
        
        if response.status_code != 200:
            raise NetworkError(f"API请求失败: {response.status_code}", self.name)
        
        data = response.json()
        
        if data.get('code') != 0:
            raise DataSourceError(f"API错误: {data.get('msg', 'Unknown error')}", self.name)
        
        tick_data = data.get('data', [])
        
        if not tick_data:
            return pd.DataFrame()
        
        # 转换为DataFrame
        records = []
        for tick in tick_data:
            record = {
                'ts_code': symbol,
                'time': tick.get('time', ''),
                'price': float(tick.get('price', 0)),
                'volume': int(tick.get('volume', 0)),
                'direction': tick.get('direction', ''),  # B买入, S卖出, N中性
                'type': tick.get('type', '')
            }
            records.append(record)
        
        return pd.DataFrame(records)
    
    def _fetch_index_data(self, request: DataRequest) -> pd.DataFrame:
        """获取指数数据"""
        # 指数数据使用相同的K线接口
        return self._fetch_daily_quotes(request)
    
    def _format_timestamp(self, date_input: Union[str, date, datetime]) -> int:
        """格式化日期为时间戳"""
        if isinstance(date_input, str):
            try:
                dt = pd.to_datetime(date_input)
                return int(dt.timestamp())
            except:
                raise DataSourceError(f"无效的日期格式: {date_input}", self.name)
        elif isinstance(date_input, date):
            dt = datetime.combine(date_input, datetime.min.time())
            return int(dt.timestamp())
        elif isinstance(date_input, datetime):
            return int(date_input.timestamp())
        else:
            raise DataSourceError(f"不支持的日期类型: {type(date_input)}", self.name)
    
    def _format_date_from_timestamp(self, timestamp: int) -> str:
        """从时间戳格式化日期"""
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime('%Y-%m-%d')
    
    def _format_datetime_from_timestamp(self, timestamp: int) -> str:
        """从时间戳格式化日期时间"""
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
    def validate_request(self, request: DataRequest) -> bool:
        """验证请求参数"""
        # 调用父类验证
        if not super().validate_request(request):
            return False
        
        # AllTick特定验证
        if request.data_type in [DataType.DAILY_QUOTES, DataType.MINUTE_QUOTES, DataType.TICK_DATA]:
            if not request.symbol:
                logger.warning("AllTick需要提供股票代码")
                return False
        
        return True
    
    def close(self):
        """关闭连接"""
        if self.session:
            self.session.close()
        self.status = DataSourceStatus.UNAVAILABLE
        super().close()
