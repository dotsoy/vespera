"""
AllTick数据源模块
提供实时和历史行情数据
"""
import pandas as pd
import requests
from loguru import logger
from typing import Optional, Dict, Any
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import uuid
from datetime import datetime, timedelta
from clickhouse_driver import Client as ClickHouseClient

from .base_data_source import (
    BaseDataSource,
    DataType,
    DataSourceType,
    DataRequest,
    DataSourceError
)
from .data_compatibility_layer import DataCompatibilityLayer

class AllTickDataSource(BaseDataSource):
    """AllTick数据源类"""
    
    def __init__(self, api_token: str, clickhouse_config: Optional[Dict[str, Any]] = None):
        """初始化AllTick数据源
        
        Args:
            api_token: API令牌
            clickhouse_config: ClickHouse数据库配置
        """
        super().__init__(name="alltick", source_type=DataSourceType.CUSTOM)
        self.api_token = api_token
        self.base_url = 'https://quote.alltick.io/quote-stock-b-api'
        # 只保留A股股票相关数据类型
        self.supported_data_types = [
            DataType.STOCK_DAILY,
            DataType.STOCK_BASIC,
            DataType.STOCK_MINUTE,
            DataType.STOCK_TICK,
            DataType.STOCK_ADJ_FACTOR,
            DataType.STOCK_COMPANY,
            DataType.STOCK_MANAGER,
            DataType.STOCK_REWARD,
            DataType.STOCK_FINANCIAL,
            DataType.STOCK_INDUSTRY,
            DataType.STOCK_CONCEPT,
            DataType.STOCK_HOLDER,
            DataType.STOCK_SHARE,
            DataType.STOCK_MONEYFLOW,
            DataType.STOCK_MARGIN,
            DataType.STOCK_SHORT,
            DataType.STOCK_BLOCKTRADE,
            DataType.STOCK_REPO,
            DataType.STOCK_FUNDAMENTAL,
            DataType.STOCK_NEWS,
            DataType.STOCK_ANNOUNCEMENT,
            DataType.STOCK_REPORT,
            DataType.STOCK_FORECAST,
            DataType.STOCK_EXPECTATION,
            DataType.STOCK_ESTIMATE
        ]
        
        # 初始化ClickHouse客户端
        self.clickhouse_client = None
        if clickhouse_config:
            try:
                self.clickhouse_client = ClickHouseClient(**clickhouse_config)
                logger.info("ClickHouse客户端初始化成功")
            except Exception as e:
                logger.error(f"ClickHouse客户端初始化失败: {e}")
        
        # 配置请求会话
        self.session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            respect_retry_after_header=True
        )
        
        # 配置适配器
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,
            pool_maxsize=20,
            pool_block=False
        )
        self.session.mount("https://", adapter)
        
        # 配置请求头
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Content-Type": "application/json"
        })
        
        # 禁用SSL警告
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
    def _get_from_clickhouse(self, request: DataRequest) -> Optional[pd.DataFrame]:
        """从ClickHouse数据库获取数据
        
        Args:
            request: 数据请求对象
            
        Returns:
            Optional[pd.DataFrame]: 数据DataFrame，如果数据不存在则返回None
        """
        if not self.clickhouse_client:
            return None
        
        try:
            # 优先使用 request 里的 start_date 和 end_date
            if hasattr(request, 'start_date') and request.start_date:
                if isinstance(request.start_date, str):
                    start_date = datetime.strptime(request.start_date, '%Y-%m-%d').date()
                elif isinstance(request.start_date, datetime):
                    start_date = request.start_date.date()
                elif isinstance(request.start_date, (pd.Timestamp,)):
                    start_date = request.start_date.to_pydatetime().date()
                else:
                    start_date = request.start_date
            else:
                end_date = datetime.now()
                start_date = (end_date - timedelta(days=365)).date()
            if hasattr(request, 'end_date') and request.end_date:
                if isinstance(request.end_date, str):
                    end_date = datetime.strptime(request.end_date, '%Y-%m-%d').date()
                elif isinstance(request.end_date, datetime):
                    end_date = request.end_date.date()
                elif isinstance(request.end_date, (pd.Timestamp,)):
                    end_date = request.end_date.to_pydatetime().date()
                else:
                    end_date = request.end_date
            else:
                end_date = datetime.now().date()
            
            # 根据数据类型构建查询
            if request.data_type == DataType.STOCK_DAILY:
                query = """
                SELECT 
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    vol,
                    amount
                FROM daily_quotes
                WHERE ts_code = %(ts_code)s
                AND trade_date BETWEEN %(start_date)s AND %(end_date)s
                ORDER BY trade_date DESC
                """
            elif request.data_type == DataType.STOCK_BASIC:
                query = """
                SELECT 
                    ts_code,
                    name,
                    industry,
                    area,
                    market,
                    list_date
                FROM stock_basic
                WHERE ts_code = %(ts_code)s
                """
            else:
                return None
                
            # 执行查询
            params = {
                'ts_code': request.symbol,
                'start_date': start_date,
                'end_date': end_date
            }
            
            logger.info(f"从ClickHouse查询数据: {query}")
            logger.debug(f"查询参数: {params}")
            
            result = self.clickhouse_client.execute(query, params)
            
            if not result:
                logger.info("ClickHouse中未找到数据")
                return None
                
            # 转换为DataFrame
            df = pd.DataFrame(result, columns=[
                'trade_date', 'open_price', 'high_price', 'low_price',
                'close_price', 'vol', 'amount'
            ] if request.data_type == DataType.STOCK_DAILY else [
                'ts_code', 'name', 'industry', 'area', 'market', 'list_date'
            ])
            
            logger.info(f"从ClickHouse获取到 {len(df)} 条数据")
            return df
            
        except Exception as e:
            logger.error(f"从ClickHouse获取数据失败: {e}")
            return None
            
    def _save_to_clickhouse(self, df: pd.DataFrame, data_type: DataType) -> bool:
        """保存数据到 ClickHouse"""
        try:
            # 使用数据兼容层转换数据
            transformed_df = DataCompatibilityLayer.transform_data(df, data_type, 'alltick')
            
            # 验证转换后的数据
            if not DataCompatibilityLayer.validate_data(transformed_df, data_type):
                logger.error("数据验证失败")
                return False
            
            table = DataCompatibilityLayer.get_table_name(data_type)
            
            # 构建完整的 INSERT 语句
            columns = ', '.join(transformed_df.columns)
            placeholders = ', '.join(['%(' + col + ')s' for col in transformed_df.columns])
            insert_sql = f"""
            INSERT INTO {table} ({columns}) VALUES ({placeholders})
            """
            
            # 执行插入
            records = transformed_df.to_dict('records')
            self.clickhouse_client.execute(insert_sql, records)
            
            logger.info(f"✅ 成功保存 {len(transformed_df)} 条 {data_type.value} 数据到 ClickHouse")
            return True
                
        except Exception as e:
            logger.error(f"保存数据到 ClickHouse 失败: {e}")
            return False
            
    def _calculate_trading_days(self) -> int:
        """计算近一年的交易日数量
        
        Returns:
            int: 交易日数量
        """
        # 计算从一年前到现在的交易日数量
        # 假设一年大约有250个交易日
        return 250
        
    def test_api_connection(self) -> bool:
        """测试API连接
        
        Returns:
            bool: 是否连接成功
        """
        try:
            # 测试API连接
            query = {
                "data": {
                    "code": "000001.SH",
                    "kline_type": 8,
                    "kline_timestamp_end": 0,
                    "query_kline_num": 1,
                    "adjust_type": 0
                },
                "trace": str(uuid.uuid4())
            }
            
            logger.info(f"测试AllTick API连接...")
            logger.debug(f"测试请求参数: {json.dumps(query, ensure_ascii=False)}")
            
            response = self.session.get(
                f"{self.base_url}/kline",
                params={
                    "token": self.api_token,
                    "query": json.dumps(query)
                },
                timeout=60,
                verify=False
            )
            response.raise_for_status()
            
            logger.debug(f"测试响应状态码: {response.status_code}")
            logger.debug(f"测试响应内容: {response.text}")
            
            data = response.json()
            logger.info(f"API返回数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
            
            # 检查返回状态码
            if data.get("ret") != 200:
                logger.error(f"API返回错误: {data.get('msg')}")
                return False
                
            # 检查返回的数据结构
            if "data" not in data or "kline_list" not in data["data"]:
                logger.error("API返回数据缺少必要字段")
                return False
                
            # 打印数据结构
            logger.info("API返回数据结构:")
            logger.info(f"数据类型: {type(data['data']['kline_list'])}")
            logger.info(f"数据长度: {len(data['data']['kline_list'])}")
            if data['data']['kline_list']:
                logger.info(f"第一条数据: {json.dumps(data['data']['kline_list'][0], ensure_ascii=False, indent=2)}")
                
            return True
            
        except Exception as e:
            logger.error(f"测试API连接失败: {e}")
            return False
        
    def initialize(self) -> bool:
        """初始化数据源
        
        Returns:
            bool: 是否初始化成功
        """
        try:
            # 首先测试API连接
            if not self.test_api_connection():
                logger.error("AllTick API连接测试失败")
                self.status = False
                return False
                
            self.status = True
            logger.info("AllTick数据源初始化成功")
            return True
            
        except Exception as e:
            logger.error(f"AllTick数据源初始化失败: {e}")
            self.status = False
            return False
            
    def is_available(self) -> bool:
        """检查数据源是否可用
        
        Returns:
            bool: 是否可用
        """
        return self.status and self.check_availability()
            
    def check_availability(self) -> bool:
        """检查数据源是否可用
        
        Returns:
            bool: 是否可用
        """
        return self.status
        
    def get_supported_data_types(self) -> list:
        """获取支持的数据类型
        
        Returns:
            list: 支持的数据类型列表
        """
        return self.supported_data_types
        
    def fetch_data(self, request: DataRequest) -> Optional[pd.DataFrame]:
        """获取数据
        
        Args:
            request: 数据请求对象
            
        Returns:
            Optional[pd.DataFrame]: 数据DataFrame
        """
        try:
            # 检查数据类型是否支持
            if request.data_type not in self.supported_data_types:
                logger.warning(f"不支持的数据类型: {request.data_type}")
                return None
                
            # 尝试从ClickHouse获取数据
            df = self._get_from_clickhouse(request)
            if df is not None and not df.empty:
                return df
                
            # 如果ClickHouse中没有数据，从API获取
            logger.info("ClickHouse中未找到数据，从API获取")
            
            if request.data_type == DataType.STOCK_DAILY:
                df = self._fetch_daily_quotes(request)
            elif request.data_type == DataType.STOCK_BASIC:
                df = self._fetch_stock_basic(request)
            else:
                return None
                
            if df is not None and not df.empty:
                # 保存到ClickHouse
                self._save_to_clickhouse(df, request.data_type)
                return df
                
            return None
            
        except Exception as e:
            logger.error(f"获取数据失败: {e}")
            return None
            
    def _fetch_daily_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取日线行情数据
        
        Args:
            request: 数据请求对象
            
        Returns:
            pd.DataFrame: 日线行情数据
        """
        try:
            # 使用 AllTick 专有股票代码
            code = request.symbol
            if not code.endswith(('.SH', '.SZ')):
                code = f"{code}.SH" if code.startswith('6') else f"{code}.SZ"
                
            # 计算近一年的交易日数量
            trading_days = self._calculate_trading_days()
                
            query = {
                "data": {
                    "code": code,
                    "kline_type": 8,  # 日线
                    "kline_timestamp_end": 0,  # 从最新交易日往前查
                    "query_kline_num": trading_days,  # 获取近一年的数据
                    "adjust_type": 0  # 不复权
                },
                "trace": str(uuid.uuid4())
            }
            
            logger.debug(f"AllTick日线行情请求参数: {json.dumps(query, ensure_ascii=False)}")
            
            response = self.session.get(
                f"{self.base_url}/kline",
                params={
                    "token": self.api_token,
                    "query": json.dumps(query)
                },
                timeout=60,
                verify=False
            )
            response.raise_for_status()
            
            logger.debug(f"AllTick日线行情响应状态码: {response.status_code}")
            logger.debug(f"AllTick日线行情响应内容: {response.text}")
            
            data = response.json()
            logger.info(f"日线行情API返回数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
            
            # 检查返回状态码
            if data.get("ret") != 200:
                raise DataSourceError(f"API返回错误: {data.get('msg')}", "alltick")
                
            # 检查返回的数据结构
            if "data" not in data or "kline_list" not in data["data"]:
                raise DataSourceError("API返回数据缺少必要字段", "alltick")
                
            # 打印数据结构
            logger.info("日线行情数据结构:")
            logger.info(f"数据类型: {type(data['data']['kline_list'])}")
            logger.info(f"数据长度: {len(data['data']['kline_list'])}")
            if data['data']['kline_list']:
                logger.info(f"第一条数据: {json.dumps(data['data']['kline_list'][0], ensure_ascii=False, indent=2)}")
                
            # 将数据转换为DataFrame
            df = pd.DataFrame(data["data"]["kline_list"])
            standardized_df = self._standardize_daily_quotes_data(df, code)
            
            # 打印标准化后的数据
            logger.info("标准化后的日线行情数据:")
            logger.info(f"数据形状: {standardized_df.shape}")
            logger.info(f"列名: {standardized_df.columns.tolist()}")
            if not standardized_df.empty:
                logger.info(f"第一条数据:\n{standardized_df.iloc[0].to_dict()}")
                
            return standardized_df
            
        except Exception as e:
            logger.error(f"获取日线行情数据失败: {e}")
            raise DataSourceError(f"获取日线行情数据失败: {e}", "alltick")
            
    def _fetch_stock_basic(self, request: DataRequest) -> pd.DataFrame:
        """获取股票基本信息
        
        Args:
            request: 数据请求对象
            
        Returns:
            pd.DataFrame: 股票基本信息
        """
        try:
            # 使用 AllTick 专有股票代码
            code = request.symbol
            if not code.endswith(('.SH', '.SZ')):
                code = f"{code}.SH" if code.startswith('6') else f"{code}.SZ"
                
            query = {
                "data": {
                    "code": code
                },
                "trace": str(uuid.uuid4())
            }
            
            logger.debug(f"AllTick股票基本信息请求参数: {json.dumps(query, ensure_ascii=False)}")
            
            response = self.session.get(
                f"{self.base_url}/stock/basic",
                params={
                    "token": self.api_token,
                    "query": json.dumps(query)
                },
                timeout=60,
                verify=False
            )
            response.raise_for_status()
            
            logger.debug(f"AllTick股票基本信息响应状态码: {response.status_code}")
            logger.debug(f"AllTick股票基本信息响应内容: {response.text}")
            
            data = response.json()
            logger.info(f"股票基本信息API返回数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
            
            # 检查返回状态码
            if data.get("ret") != 200:
                raise DataSourceError(f"API返回错误: {data.get('msg')}", "alltick")
                
            # 检查返回的数据结构
            if "data" not in data:
                raise DataSourceError("API返回数据缺少必要字段", "alltick")
                
            # 打印数据结构
            logger.info("股票基本信息数据结构:")
            logger.info(f"数据类型: {type(data['data'])}")
            logger.info(f"数据内容: {json.dumps(data['data'], ensure_ascii=False, indent=2)}")
                
            # 将数据转换为DataFrame
            df = pd.DataFrame([data["data"]])
            standardized_df = self._standardize_stock_basic_data(df)
            
            # 打印标准化后的数据
            logger.info("标准化后的股票基本信息:")
            logger.info(f"数据形状: {standardized_df.shape}")
            logger.info(f"列名: {standardized_df.columns.tolist()}")
            if not standardized_df.empty:
                logger.info(f"数据内容:\n{standardized_df.iloc[0].to_dict()}")
                
            return standardized_df
            
        except Exception as e:
            logger.error(f"获取股票基本信息失败: {e}")
            raise DataSourceError(f"获取股票基本信息失败: {e}", "alltick")
            
    def _fetch_index_data(self, request: DataRequest) -> pd.DataFrame:
        """获取指数数据
        
        Args:
            request: 数据请求对象
            
        Returns:
            pd.DataFrame: 指数数据
        """
        try:
            # 使用 AllTick 专有股票代码
            code = request.symbol
            if not code.endswith(('.SH', '.SZ')):
                code = f"{code}.SH" if code.startswith('6') else f"{code}.SZ"
                
            # 计算近一年的交易日数量
            trading_days = self._calculate_trading_days()
                
            query = {
                "data": {
                    "code": code,
                    "kline_type": 8,  # 日线
                    "kline_timestamp_end": 0,  # 从最新交易日往前查
                    "query_kline_num": trading_days,  # 获取近一年的数据
                    "adjust_type": 0  # 不复权
                },
                "trace": str(uuid.uuid4())
            }
            
            logger.debug(f"AllTick指数数据请求参数: {json.dumps(query, ensure_ascii=False)}")
            
            response = self.session.get(
                f"{self.base_url}/kline",
                params={
                    "token": self.api_token,
                    "query": json.dumps(query)
                },
                timeout=60,
                verify=False
            )
            response.raise_for_status()
            
            logger.debug(f"AllTick指数数据响应状态码: {response.status_code}")
            logger.debug(f"AllTick指数数据响应内容: {response.text}")
            
            data = response.json()
            logger.info(f"指数数据API返回数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
            
            # 检查返回状态码
            if data.get("ret") != 200:
                raise DataSourceError(f"API返回错误: {data.get('msg')}", "alltick")
                
            # 检查返回的数据结构
            if "data" not in data or "kline_list" not in data["data"]:
                raise DataSourceError("API返回数据缺少必要字段", "alltick")
                
            # 打印数据结构
            logger.info("指数数据结构:")
            logger.info(f"数据类型: {type(data['data']['kline_list'])}")
            logger.info(f"数据长度: {len(data['data']['kline_list'])}")
            if data['data']['kline_list']:
                logger.info(f"第一条数据: {json.dumps(data['data']['kline_list'][0], ensure_ascii=False, indent=2)}")
                
            # 将数据转换为DataFrame
            df = pd.DataFrame(data["data"]["kline_list"])
            standardized_df = self._standardize_index_data(df)
            
            # 打印标准化后的数据
            logger.info("标准化后的指数数据:")
            logger.info(f"数据形状: {standardized_df.shape}")
            logger.info(f"列名: {standardized_df.columns.tolist()}")
            if not standardized_df.empty:
                logger.info(f"第一条数据:\n{standardized_df.iloc[0].to_dict()}")
                
            return standardized_df
            
        except Exception as e:
            logger.error(f"获取指数数据失败: {e}")
            raise DataSourceError(f"获取指数数据失败: {e}", "alltick")
            
    def _standardize_daily_quotes_data(self, df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
        """标准化日线行情数据
        Args:
            df: 原始数据DataFrame
            symbol: 股票代码（如API返回数据中无symbol字段时补充）
        Returns:
            pd.DataFrame: 标准化后的数据
        """
        if df.empty:
            return df
        # 如果没有 symbol 字段，补充
        if 'symbol' not in df.columns and symbol is not None:
            df['symbol'] = symbol
        # 打印原始数据的列名
        logger.info(f"原始日线行情数据列名: {df.columns.tolist()}")
        # 重命名列
        column_mapping = {
            "timestamp": "trade_date",
            "open_price": "open_price",
            "high_price": "high_price",
            "low_price": "low_price",
            "close_price": "close_price",
            "volume": "vol",
            "turnover": "amount"
        }
        df = df.rename(columns=column_mapping)
        # 转换时间戳为日期格式
        df["trade_date"] = pd.to_datetime(df["trade_date"].astype(int), unit='s').dt.strftime("%Y%m%d")
        # 转换数值类型
        numeric_columns = ["open_price", "high_price", "low_price", "close_price", "vol", "amount"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        # 添加必需的列
        df['ts_code'] = df['symbol']  # 使用股票代码作为ts_code
        df['pre_close'] = df['close_price'].shift(1)  # 计算前收盘价
        df['change_amount'] = df['close_price'] - df['pre_close']  # 计算涨跌额
        df['pct_chg'] = (df['change_amount'] / df['pre_close'] * 100).round(2)  # 计算涨跌幅
        df['amplitude'] = ((df['high_price'] - df['low_price']) / df['pre_close'] * 100).round(2)  # 计算振幅
        df['turnover_rate'] = 0.0  # 默认换手率为0
        # 确保所有必需的列都存在
        required_columns = [
            'ts_code', 'symbol', 'trade_date', 'open_price', 'high_price', 
            'low_price', 'close_price', 'pre_close', 'change_amount', 
            'pct_chg', 'vol', 'amount', 'amplitude', 'turnover_rate'
        ]
        for col in required_columns:
            if col not in df.columns:
                df[col] = 0.0
        # 日志输出所有必需字段
        logger.info(f"标准化后日线行情数据列名: {df.columns.tolist()}")
        # 按日期排序
        df = df.sort_values('trade_date')
        return df
        
    def _standardize_stock_basic_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化股票基本信息
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            pd.DataFrame: 标准化后的数据
        """
        if df.empty:
            return df
            
        # 打印原始数据的列名
        logger.info(f"原始股票基本信息列名: {df.columns.tolist()}")
            
        # 重命名列
        column_mapping = {
            "code": "ts_code",
            "name": "name",
            "industry": "industry",
            "area": "area",
            "market": "market",
            "list_date": "list_date"
        }
        df = df.rename(columns=column_mapping)
        
        # 添加symbol列
        df['symbol'] = df['ts_code']
        
        return df
        
    def _standardize_index_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化指数数据
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            pd.DataFrame: 标准化后的数据
        """
        if df.empty:
            return df
            
        # 打印原始数据的列名
        logger.info(f"原始指数数据列名: {df.columns.tolist()}")
            
        # 重命名列
        column_mapping = {
            "timestamp": "trade_date",
            "open_price": "open_price",
            "high_price": "high_price",
            "low_price": "low_price",
            "close_price": "close_price",
            "volume": "vol",
            "turnover": "amount"
        }
        df = df.rename(columns=column_mapping)
        
        # 转换时间戳为日期格式
        df["trade_date"] = pd.to_datetime(df["trade_date"].astype(int), unit='s').dt.strftime("%Y%m%d")
        
        # 转换数值类型
        numeric_columns = ["open_price", "high_price", "low_price", "close_price", "vol", "amount"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # 添加必需的列
        df['ts_code'] = df['symbol']  # 使用股票代码作为ts_code
        df['symbol'] = df['symbol']   # 保持symbol列
        df['pre_close'] = df['close_price'].shift(1)  # 计算前收盘价
        df['change_amount'] = df['close_price'] - df['pre_close']  # 计算涨跌额
        df['pct_chg'] = (df['change_amount'] / df['pre_close'] * 100).round(2)  # 计算涨跌幅
        df['amplitude'] = ((df['high_price'] - df['low_price']) / df['pre_close'] * 100).round(2)  # 计算振幅
        df['turnover_rate'] = 0.0  # 默认换手率为0
        
        # 确保所有必需的列都存在
        required_columns = [
            'ts_code', 'symbol', 'trade_date', 'open_price', 'high_price', 
            'low_price', 'close_price', 'pre_close', 'change_amount', 
            'pct_chg', 'vol', 'amount', 'amplitude', 'turnover_rate'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = 0.0
                
        # 按日期排序
        df = df.sort_values('trade_date')
        
        return df
        
    def close(self):
        """关闭数据源连接"""
        if hasattr(self, 'session'):
            self.session.close()
        if hasattr(self, 'clickhouse_client') and self.clickhouse_client:
            self.clickhouse_client.disconnect()
        self.status = False
        logger.info("AllTick数据源已关闭") 