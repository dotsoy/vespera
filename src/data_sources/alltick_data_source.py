"""
AllTick数据源模块
提供实时和历史行情数据
"""
import pandas as pd
import requests
from loguru import logger
from typing import Optional, Dict, Any
import urllib3
import ssl
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base_data_source import (
    BaseDataSource,
    DataType,
    DataSourceType,
    DataRequest,
    DataSourceError
)

class AllTickDataSource(BaseDataSource):
    """AllTick数据源类"""
    
    def __init__(self, api_token: str):
        """初始化AllTick数据源
        
        Args:
            api_token: API令牌
        """
        super().__init__(name="alltick", source_type=DataSourceType.ALLTICK)
        self.api_token = api_token
        self.base_url = 'https://api.alltick.com/v1'
        self.supported_data_types = [
            DataType.DAILY_QUOTES,
            DataType.STOCK_BASIC,
            DataType.INDEX_DATA
        ]
        
        # 配置请求会话
        self.session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=5,  # 增加重试次数
            backoff_factor=1,  # 增加退避因子
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            respect_retry_after_header=True
        )
        
        # 配置适配器
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,  # 增加连接数
            pool_maxsize=20,  # 增加池大小
            pool_block=False  # 不阻塞连接池
        )
        self.session.mount("https://", adapter)
        
        # 配置会话
        self.session.verify = False
        self.session.trust_env = False
        
        # 配置代理（如果需要）
        # self.session.proxies = {
        #     'http': 'http://127.0.0.1:7890',
        #     'https': 'http://127.0.0.1:7890'
        # }
        
        # 配置请求头
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_token}",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        })
        
        # 禁用SSL警告
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
    def initialize(self) -> bool:
        """初始化数据源
        
        Returns:
            bool: 是否初始化成功
        """
        try:
            # 测试API连接
            response = self.session.get(
                f"{self.base_url}/test",
                timeout=60,  # 增加超时时间
                allow_redirects=True
            )
            response.raise_for_status()
            self.status = True
            logger.info("AllTick数据源初始化成功")
            return True
        except Exception as e:
            logger.error(f"AllTick数据源初始化失败: {e}")
            self.status = False
            return False
            
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
        if not self.status:
            raise DataSourceError("数据源未初始化", "alltick")
            
        if request.data_type not in self.supported_data_types:
            raise DataSourceError(f"不支持的数据类型: {request.data_type}", "alltick")
            
        try:
            if request.data_type == DataType.DAILY_QUOTES:
                return self._fetch_daily_quotes(request)
            elif request.data_type == DataType.STOCK_BASIC:
                return self._fetch_stock_basic(request)
            elif request.data_type == DataType.INDEX_DATA:
                return self._fetch_index_data(request)
            else:
                raise DataSourceError(f"不支持的数据类型: {request.data_type}", "alltick")
        except Exception as e:
            logger.error(f"获取数据失败: {e}")
            raise DataSourceError(f"获取数据失败: {e}", "alltick")
            
    def _fetch_daily_quotes(self, request: DataRequest) -> pd.DataFrame:
        """获取日线行情数据
        
        Args:
            request: 数据请求对象
            
        Returns:
            pd.DataFrame: 日线行情数据
        """
        try:
            response = self.session.get(
                f"{self.base_url}/daily",
                params={
                    "symbol": request.symbol,
                    "start_date": request.start_date.strftime("%Y%m%d"),
                    "end_date": request.end_date.strftime("%Y%m%d")
                },
                timeout=60,
                allow_redirects=True
            )
            response.raise_for_status()
            data = response.json()
            
            if not data.get("success"):
                raise DataSourceError(f"API返回错误: {data.get('message')}", "alltick")
                
            df = pd.DataFrame(data["data"])
            return self._standardize_daily_quotes_data(df)
            
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
            response = self.session.get(
                f"{self.base_url}/stock/basic",
                params={"symbol": request.symbol},
                timeout=60,
                allow_redirects=True
            )
            response.raise_for_status()
            data = response.json()
            
            if not data.get("success"):
                raise DataSourceError(f"API返回错误: {data.get('message')}", "alltick")
                
            df = pd.DataFrame([data["data"]])
            return self._standardize_stock_basic_data(df)
            
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
            response = self.session.get(
                f"{self.base_url}/index",
                params={
                    "symbol": request.symbol,
                    "start_date": request.start_date.strftime("%Y%m%d"),
                    "end_date": request.end_date.strftime("%Y%m%d")
                },
                timeout=60,
                allow_redirects=True
            )
            response.raise_for_status()
            data = response.json()
            
            if not data.get("success"):
                raise DataSourceError(f"API返回错误: {data.get('message')}", "alltick")
                
            df = pd.DataFrame(data["data"])
            return self._standardize_index_data(df)
            
        except Exception as e:
            logger.error(f"获取指数数据失败: {e}")
            raise DataSourceError(f"获取指数数据失败: {e}", "alltick")
            
    def _standardize_daily_quotes_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化日线行情数据
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            pd.DataFrame: 标准化后的数据
        """
        if df.empty:
            return df
            
        # 重命名列
        column_mapping = {
            "date": "trade_date",
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "close": "close_price",
            "volume": "vol",
            "amount": "amount"
        }
        df = df.rename(columns=column_mapping)
        
        # 确保日期格式正确
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d")
        
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
            
        # 重命名列
        column_mapping = {
            "date": "trade_date",
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "close": "close_price",
            "volume": "vol",
            "amount": "amount"
        }
        df = df.rename(columns=column_mapping)
        
        # 确保日期格式正确
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d")
        
        return df
        
    def close(self):
        """关闭数据源连接"""
        if hasattr(self, 'session'):
            self.session.close()
        self.status = False
        logger.info("AllTick数据源已关闭") 