"""
数据源抽象基类模块

该模块定义了数据源的通用接口和基础类，用于实现统一的数据访问层。
包含数据源类型、数据类型、请求/响应模型等核心组件的定义。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from enum import Enum
from typing import (
    Any, Dict, List, Optional, TypeVar, Union, 
    Generic, Type, ClassVar, Set, Tuple, cast, Callable
)
import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field, validator
from typing_extensions import Literal, Self
import threading

# 类型变量，用于泛型编程
T = TypeVar('T')

# 类型别名
TimestampType = Union[str, datetime, date, int, float]
SymbolType = Union[str, Tuple[str, str]]  # (symbol, exchange) 或 symbol
DataFrameOrDict = Union[pd.DataFrame, Dict[str, Any]]


class DataSourceType(str, Enum):
    """
    Enumerates all supported data source types in the system.
    
    This enum provides a type-safe way to reference different data source providers.
    Each enum value represents a specific data source and has a string representation
    for serialization and configuration purposes.
    
    The data sources are organized by provider and include both free and paid options.
    Each source has different coverage, update frequency, and data quality characteristics.
    
    Attributes:
        AKSHARE: AkShare data source (free, open-source, comprehensive Chinese market data)
        TUSHARE: Tushare data source (free and paid tiers, Chinese market data)
        YAHOO: Yahoo Finance data source (free, global market data)
        BAOSTOCK: BaoStock data source (free, Chinese market data)
        JQDATA: JoinQuant data source (paid, Chinese market data)
        RQDATA: RiceQuant data source (paid, Chinese market data)
        TUSHAREPRO: Tushare Pro data source (paid, professional Chinese market data)
        ALIYUN: Aliyun Finance data source (paid, Chinese market data)
        TENCENT: Tencent Finance data source (free, Chinese market data)
        SINA: Sina Finance data source (free, Chinese market data)
        EASTMONEY: East Money data source (free, Chinese market data)
        CUSTOM: Custom data source for user-defined data
    
    Example:
        >>> DataSourceType.AKSHARE
        <DataSourceType.AKSHARE: 'akshare'>
        >>> DataSourceType.from_string('tushare')
        <DataSourceType.TUSHARE: 'tushare'>
    """
    AKSHARE = "akshare"  # AkShare数据源 - 开源金融数据接口库
    TUSHARE = "tushare"  # Tushare数据源 - 金融大数据社区
    YAHOO = "yahoo"  # Yahoo Finance数据源 - 雅虎财经
    BAOSTOCK = "baostock"  # 宝硕数据 - 免费开源证券数据平台
    JQDATA = "jqdata"  # 聚宽数据 - 量化交易数据服务
    RQDATA = "rqdata"  # 米筐数据 - 量化金融数据服务
    TUSHAREPRO = "tusharepro"  # Tushare Pro数据源 - Tushare专业版
    ALIYUN = "aliyun"  # 阿里云金融数据 - 阿里云金融数据服务
    TENCENT = "tencent"  # 腾讯财经数据 - 腾讯财经数据接口
    SINA = "sina"  # 新浪财经数据 - 新浪财经数据接口
    EASTMONEY = "eastmoney"  # 东方财富数据 - 东方财富网数据接口
    CUSTOM = "custom"  # 自定义数据源 - 用户自定义数据源
    
    # Group data sources by their general characteristics
    _FREE_SOURCES = {AKSHARE, TUSHARE, YAHOO, BAOSTOCK, TENCENT, SINA, EASTMONEY}
    _PAID_SOURCES = {JQDATA, RQDATA, TUSHAREPRO, ALIYUN}
    _CHINA_MARKET_SOURCES = {
        AKSHARE, TUSHARE, BAOSTOCK, JQDATA, RQDATA, 
        TUSHAREPRO, ALIYUN, TENCENT, SINA, EASTMONEY
    }
    _GLOBAL_MARKET_SOURCES = {YAHOO}
    
    @classmethod
    def is_free_source(cls, source_type: Union['DataSourceType', str]) -> bool:
        """Check if the given data source is free to use.
        
        Args:
            source_type: The data source type to check
            
        Returns:
            bool: True if the data source is free, False otherwise
            
        Example:
            >>> DataSourceType.is_free_source(DataSourceType.AKSHARE)
            True
            >>> DataSourceType.is_free_source("jqdata")
            False
        """
        if isinstance(source_type, DataSourceType):
            return source_type in cls._FREE_SOURCES
        return source_type in {t.value for t in cls._FREE_SOURCES}
    
    @classmethod
    def is_paid_source(cls, source_type: Union['DataSourceType', str]) -> bool:
        """Check if the given data source requires payment.
        
        Args:
            source_type: The data source type to check
            
        Returns:
            bool: True if the data source requires payment, False otherwise
        """
        if isinstance(source_type, DataSourceType):
            return source_type in cls._PAID_SOURCES
        return source_type in {t.value for t in cls._PAID_SOURCES}
    
    @classmethod
    def is_china_market_source(cls, source_type: Union['DataSourceType', str]) -> bool:
        """Check if the data source provides China market data.
        
        Args:
            source_type: The data source type to check
            
        Returns:
            bool: True if the data source provides China market data
        """
        if isinstance(source_type, DataSourceType):
            return source_type in cls._CHINA_MARKET_SOURCES
        return source_type in {t.value for t in cls._CHINA_MARKET_SOURCES}
    
    @classmethod
    def is_global_market_source(cls, source_type: Union['DataSourceType', str]) -> bool:
        """Check if the data source provides global market data.
        
        Args:
            source_type: The data source type to check
            
        Returns:
            bool: True if the data source provides global market data
        """
        if isinstance(source_type, DataSourceType):
            return source_type in cls._GLOBAL_MARKET_SOURCES
        return source_type in {t.value for t in cls._GLOBAL_MARKET_SOURCES}
    
    @classmethod
    def from_string(cls, value: str) -> 'DataSourceType':
        """Convert a string to a DataSourceType enum member.
        
        This is case-insensitive and handles common variations in source names.
        
        Args:
            value: The string value to convert
            
        Returns:
            DataSourceType: The corresponding DataSourceType enum member
            
        Raises:
            ValueError: If the string does not match any DataSourceType
            
        Example:
            >>> DataSourceType.from_string("AKShare")
            <DataSourceType.AKSHARE: 'akshare'>
        """
        normalized_value = value.lower().strip()
        
        # Handle common variations and aliases
        aliases = {
            'akshare': cls.AKSHARE,
            'tushare': cls.TUSHARE,
            'tusharepro': cls.TUSHAREPRO,
            'tushare_pro': cls.TUSHAREPRO,
            'tushare-pro': cls.TUSHAREPRO,
            'pro': cls.TUSHAREPRO,
            'baostock': cls.BAOSTOCK,
            'bao_stock': cls.BAOSTOCK,
            'jqdata': cls.JQDATA,
            'jq': cls.JQDATA,
            'joinquant': cls.JQDATA,
            'join_quant': cls.JQDATA,
            'rqdata': cls.RQDATA,
            'rq': cls.RQDATA,
            'ricequant': cls.RQDATA,
            'rice_quant': cls.RQDATA,
            'aliyun': cls.ALIYUN,
            'alibaba': cls.ALIYUN,
            'ali_cloud': cls.ALIYUN,
            'tencent': cls.TENCENT,
            'qq': cls.TENCENT,
            'sina': cls.SINA,
            'eastmoney': cls.EASTMONEY,
            'east_money': cls.EASTMONEY,
            'eastmoney.com': cls.EASTMONEY,
            'custom': cls.CUSTOM,
            'user': cls.CUSTOM,
            'local': cls.CUSTOM
        }
        
        # First try direct match
        try:
            return cls(normalized_value)
        except ValueError:
            # Then try aliases
            if normalized_value in aliases:
                return aliases[normalized_value]
            
            # If still not found, try case-insensitive match
            for member in cls:
                if member.value.lower() == normalized_value:
                    return member
            
            # If we get here, no match was found
            valid_values = [f"'{t.value}'" for t in cls]
            raise ValueError(
                f"Invalid data source type: '{value}'. "
                f"Valid values are: {', '.join(valid_values)}"
            )
    
    def get_display_name(self) -> str:
        """Get a human-readable display name for the data source.
        
        Returns:
            str: A formatted display name for the data source
            
        Example:
            >>> DataSourceType.AKSHARE.get_display_name()
            'AKShare'
        """
        name_map = {
            self.AKSHARE: "AKShare",
            self.TUSHARE: "Tushare",
            self.TUSHAREPRO: "Tushare Pro",
            self.BAOSTOCK: "BaoStock",
            self.JQDATA: "JoinQuant",
            self.RQDATA: "RiceQuant",
            self.ALIYUN: "Aliyun Finance",
            self.TENCENT: "Tencent Finance",
            self.SINA: "Sina Finance",
            self.EASTMONEY: "East Money",
            self.YAHOO: "Yahoo Finance",
            self.CUSTOM: "Custom Source"
        }
        return name_map.get(self, self.value.capitalize())
    
    def get_description(self) -> str:
        """Get a brief description of the data source.
        
        Returns:
            str: A description of the data source
        """
        descriptions = {
            self.AKSHARE: "Open-source financial data interface library with comprehensive China market coverage",
            self.TUSHARE: "Financial big data community with free and paid tiers for China market data",
            self.TUSHAREPRO: "Professional version of Tushare with enhanced data quality and coverage",
            self.BAOSTOCK: "Free open-source securities data platform focused on China market",
            self.JQDATA: "Quantitative trading data service with high-quality China market data",
            self.RQDATA: "Quantitative financial data service with institutional-grade datasets",
            self.ALIYUN: "Alibaba Cloud's financial data service with comprehensive China market coverage",
            self.TENCENT: "Free financial data interface from Tencent Finance",
            self.SINA: "Free financial data interface from Sina Finance",
            self.EASTMONEY: "Free financial data interface from East Money (东方财富网)",
            self.YAHOO: "Free global market data from Yahoo Finance",
            self.CUSTOM: "User-defined custom data source for integrating proprietary data"
        }
        return descriptions.get(self, "No description available")
    
    def __str__(self) -> str:
        """Return the string representation of the data source type.
        
        Returns:
            str: The string value of the enum member
        """
        return self.value


class DataType(str, Enum):
    """
    Enumerates all supported data types in the data source system.
    
    This enum provides a type-safe way to reference different types of financial
    market data that can be requested from data sources. Each enum value represents
    a specific data category and has a string representation for serialization.
    
    The enum is organized into logical groups with clear prefixes:
    - stock_*: Stock-specific data (prices, fundamentals, etc.)
    - index_*: Index-related data
    - tick_*: High-frequency tick data
    - *_daily, *_minute: Timeframe-specific data
    
    Attributes:
        STOCK_DAILY: Daily OHLCV data for stocks
        STOCK_MINUTE: Minute-level OHLCV data for stocks
        STOCK_TICK: Tick-by-tick trade data for stocks
        STOCK_ADJ_FACTOR: Adjustment factors for stock prices
        INDEX_DAILY: Daily index values
        INDEX_COMPONENT: Constituents of an index
        STOCK_BASIC: Basic stock information (symbol, name, etc.)
        STOCK_COMPANY: Company information
        STOCK_MANAGER: Company management information
        STOCK_REWARD: Stock dividends and distributions
        STOCK_FINANCIAL: Financial statements and metrics
        STOCK_INDUSTRY: Industry classification
        STOCK_CONCEPT: Concept/thematic classification
        STOCK_HOLDER: Shareholder information
        STOCK_SHARE: Share capital structure
        STOCK_MONEYFLOW: Money flow data
        STOCK_MARGIN: Margin trading data
        STOCK_SHORT: Short selling data
        STOCK_BLOCKTRADE: Block trade data
        STOCK_REPO: Stock repurchase data
        STOCK_FUNDAMENTAL: Fundamental analysis data
        STOCK_NEWS: News articles
        STOCK_ANNOUNCEMENT: Corporate announcements
        STOCK_REPORT: Research reports
        STOCK_FORECAST: Earnings forecasts
        STOCK_EXPECTATION: Market expectations
        STOCK_ESTIMATE: Analyst estimates
        STOCK_INDEX: Stock index data
        STOCK_INDEX_WEIGHT: Index constituent weights
        STOCK_INDEX_DETAIL: Detailed index information
        STOCK_INDEX_COMPARE: Index comparison data
        STOCK_INDEX_CONS: Index constituents
        STOCK_INDEX_DAILY: Daily index data
        STOCK_INDEX_WEEKLY: Weekly index data
        STOCK_INDEX_MONTHLY: Monthly index data
        STOCK_INDEX_MIN: Minute-level index data
        STOCK_INDEX_TICK: Tick-by-tick index data
        STOCK_INDEX_TICK_BRIEF: Brief tick data
        STOCK_INDEX_TICK_DEAL: Index trade data
        STOCK_INDEX_TICK_ORDERS: Index order data
        STOCK_INDEX_TICK_QUEUE: Order book data
        STOCK_INDEX_TICK_DEAL_QUEUE: Trade queue data
        STOCK_INDEX_TICK_ORDERS_QUEUE: Order queue data
        STOCK_INDEX_TICK_QUEUE_EXTRA: Extended order book data
        STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA: Extended trade queue data
        STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA: Extended order queue data
        STOCK_INDEX_TICK_QUEUE_EXTRA2: Secondary extended order book data
        STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA2: Secondary extended trade queue data
        STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA2: Secondary extended order queue data
        STOCK_INDEX_TICK_QUEUE_EXTRA3: Tertiary extended order book data
        STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA3: Tertiary extended trade queue data
        STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA3: Tertiary extended order queue data
        STOCK_INDEX_TICK_QUEUE_EXTRA4: Quaternary extended order book data
        STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA4: Quaternary extended trade queue data
        STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA4: Quaternary extended order queue data
    """
    # Stock Data Types
    STOCK_DAILY = "stock_daily"  # 股票日线数据
    STOCK_MINUTE = "stock_minute"  # 股票分钟数据
    STOCK_TICK = "stock_tick"  # 股票tick数据
    STOCK_ADJ_FACTOR = "stock_adj_factor"  # 复权因子
    
    # Index Data Types
    INDEX_DAILY = "index_daily"  # 指数日线数据
    INDEX_COMPONENT = "index_component"  # 指数成分股
    
    # Stock Information
    STOCK_BASIC = "stock_basic"  # 股票基础信息
    STOCK_COMPANY = "stock_company"  # 上市公司信息
    STOCK_MANAGER = "stock_manager"  # 上市公司管理层
    STOCK_REWARD = "stock_reward"  # 分红送股
    STOCK_FINANCIAL = "stock_financial"  # 财务指标
    STOCK_INDUSTRY = "stock_industry"  # 行业分类
    STOCK_CONCEPT = "stock_concept"  # 概念分类
    STOCK_HOLDER = "stock_holder"  # 股东信息
    STOCK_SHARE = "stock_share"  # 股本结构
    STOCK_MONEYFLOW = "stock_moneyflow"  # 资金流向
    STOCK_MARGIN = "stock_margin"  # 融资融券
    STOCK_SHORT = "stock_short"  # 卖空数据
    STOCK_BLOCKTRADE = "stock_blocktrade"  # 大宗交易
    STOCK_REPO = "stock_repo"  # 股票回购
    STOCK_FUNDAMENTAL = "stock_fundamental"  # 基本面数据
    
    # Market Data
    STOCK_NEWS = "stock_news"  # 新闻数据
    STOCK_ANNOUNCEMENT = "stock_announcement"  # 公告数据
    STOCK_REPORT = "stock_report"  # 研究报告
    STOCK_FORECAST = "stock_forecast"  # 业绩预告
    STOCK_EXPECTATION = "stock_expectation"  # 业绩预期
    STOCK_ESTIMATE = "stock_estimate"  # 盈利预测
    
    # Index Data
    STOCK_INDEX = "stock_index"  # 股票指数
    STOCK_INDEX_WEIGHT = "stock_index_weight"  # 指数权重
    STOCK_INDEX_DETAIL = "stock_index_detail"  # 指数详情
    STOCK_INDEX_COMPARE = "stock_index_compare"  # 指数对比
    STOCK_INDEX_CONS = "stock_index_cons"  # 指数成分股
    STOCK_INDEX_DAILY = "stock_index_daily"  # 指数日线
    STOCK_INDEX_WEEKLY = "stock_index_weekly"  # 指数周线
    STOCK_INDEX_MONTHLY = "stock_index_monthly"  # 指数月线
    STOCK_INDEX_MIN = "stock_index_min"  # 指数分钟
    
    # Tick Data
    STOCK_INDEX_TICK = "stock_index_tick"  # 指数tick
    STOCK_INDEX_TICK_BRIEF = "stock_index_tick_brief"  # 指数tick简略
    STOCK_INDEX_TICK_DEAL = "stock_index_tick_deal"  # 指数tick成交
    STOCK_INDEX_TICK_ORDERS = "stock_index_tick_orders"  # 指数tick委托
    STOCK_INDEX_TICK_QUEUE = "stock_index_tick_queue"  # 指数tick队列
    STOCK_INDEX_TICK_DEAL_QUEUE = "stock_index_tick_deal_queue"  # 指数tick成交队列
    STOCK_INDEX_TICK_ORDERS_QUEUE = "stock_index_tick_orders_queue"  # 指数tick委托队列
    
    # Extended Tick Data (various levels)
    STOCK_INDEX_TICK_QUEUE_EXTRA = "stock_index_tick_queue_extra"
    STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA = "stock_index_tick_deal_queue_extra"
    STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA = "stock_index_tick_orders_queue_extra"
    
    STOCK_INDEX_TICK_QUEUE_EXTRA2 = "stock_index_tick_queue_extra2"
    STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA2 = "stock_index_tick_deal_queue_extra2"
    STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA2 = "stock_index_tick_orders_queue_extra2"
    
    STOCK_INDEX_TICK_QUEUE_EXTRA3 = "stock_index_tick_queue_extra3"
    STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA3 = "stock_index_tick_deal_queue_extra3"
    STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA3 = "stock_index_tick_orders_queue_extra3"
    
    STOCK_INDEX_TICK_QUEUE_EXTRA4 = "stock_index_tick_queue_extra4"
    STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA4 = "stock_index_tick_deal_queue_extra4"
    STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA4 = "stock_index_tick_orders_queue_extra4"
    
    # Groupings of related data types for easier validation
    _STOCK_DATA_TYPES = {
        STOCK_DAILY, STOCK_MINUTE, STOCK_TICK, STOCK_ADJ_FACTOR,
        STOCK_BASIC, STOCK_COMPANY, STOCK_MANAGER, STOCK_REWARD,
        STOCK_FINANCIAL, STOCK_INDUSTRY, STOCK_CONCEPT, STOCK_HOLDER,
        STOCK_SHARE, STOCK_MONEYFLOW, STOCK_MARGIN, STOCK_SHORT,
        STOCK_BLOCKTRADE, STOCK_REPO, STOCK_FUNDAMENTAL, STOCK_NEWS,
        STOCK_ANNOUNCEMENT, STOCK_REPORT, STOCK_FORECAST, STOCK_EXPECTATION,
        STOCK_ESTIMATE
    }
    
    _INDEX_DATA_TYPES = {
        INDEX_DAILY, INDEX_COMPONENT, STOCK_INDEX, STOCK_INDEX_WEIGHT,
        STOCK_INDEX_DETAIL, STOCK_INDEX_COMPARE, STOCK_INDEX_CONS,
        STOCK_INDEX_DAILY, STOCK_INDEX_WEEKLY, STOCK_INDEX_MONTHLY,
        STOCK_INDEX_MIN, STOCK_INDEX_TICK, STOCK_INDEX_TICK_BRIEF,
        STOCK_INDEX_TICK_DEAL, STOCK_INDEX_TICK_ORDERS, STOCK_INDEX_TICK_QUEUE,
        STOCK_INDEX_TICK_DEAL_QUEUE, STOCK_INDEX_TICK_ORDERS_QUEUE,
        STOCK_INDEX_TICK_QUEUE_EXTRA, STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA,
        STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA, STOCK_INDEX_TICK_QUEUE_EXTRA2,
        STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA2, STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA2,
        STOCK_INDEX_TICK_QUEUE_EXTRA3, STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA3,
        STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA3, STOCK_INDEX_TICK_QUEUE_EXTRA4,
        STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA4, STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA4
    }
    
    _TICK_DATA_TYPES = {
        STOCK_TICK, STOCK_INDEX_TICK, STOCK_INDEX_TICK_BRIEF,
        STOCK_INDEX_TICK_DEAL, STOCK_INDEX_TICK_ORDERS, STOCK_INDEX_TICK_QUEUE,
        STOCK_INDEX_TICK_DEAL_QUEUE, STOCK_INDEX_TICK_ORDERS_QUEUE,
        STOCK_INDEX_TICK_QUEUE_EXTRA, STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA,
        STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA, STOCK_INDEX_TICK_QUEUE_EXTRA2,
        STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA2, STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA2,
        STOCK_INDEX_TICK_QUEUE_EXTRA3, STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA3,
        STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA3, STOCK_INDEX_TICK_QUEUE_EXTRA4,
        STOCK_INDEX_TICK_DEAL_QUEUE_EXTRA4, STOCK_INDEX_TICK_ORDERS_QUEUE_EXTRA4
    }
    
    @classmethod
    def is_stock_data_type(cls, data_type: Union['DataType', str]) -> bool:
        """Check if the given data type is related to stock data.
        
        Args:
            data_type: The data type to check, either as a DataType enum or string
            
        Returns:
            bool: True if the data type is stock-related, False otherwise
            
        Example:
            >>> DataType.is_stock_data_type(DataType.STOCK_DAILY)
            True
            >>> DataType.is_stock_data_type("stock_daily")
            True
            >>> DataType.is_stock_data_type(DataType.INDEX_DAILY)
            False
        """
        if isinstance(data_type, DataType):
            return data_type in cls._STOCK_DATA_TYPES
        return data_type in {dt.value for dt in cls._STOCK_DATA_TYPES}
    
    @classmethod
    def is_index_data_type(cls, data_type: Union['DataType', str]) -> bool:
        """Check if the given data type is related to index data.
        
        Args:
            data_type: The data type to check, either as a DataType enum or string
            
        Returns:
            bool: True if the data type is index-related, False otherwise
        """
        if isinstance(data_type, DataType):
            return data_type in cls._INDEX_DATA_TYPES
        return data_type in {dt.value for dt in cls._INDEX_DATA_TYPES}
    
    @classmethod
    def is_tick_data_type(cls, data_type: Union['DataType', str]) -> bool:
        """Check if the given data type is tick data.
        
        Args:
            data_type: The data type to check, either as a DataType enum or string
            
        Returns:
            bool: True if the data type is tick data, False otherwise
        """
        if isinstance(data_type, DataType):
            return data_type in cls._TICK_DATA_TYPES
        return data_type in {dt.value for dt in cls._TICK_DATA_TYPES}
    
    @classmethod
    def get_data_type_category(cls, data_type: Union['DataType', str]) -> str:
        """Get the category of the given data type.
        
        Categories include: 'stock', 'index', 'tick', or 'other'.
        
        Args:
            data_type: The data type to check
            
        Returns:
            str: The category of the data type
        """
        if cls.is_stock_data_type(data_type):
            return 'stock'
        elif cls.is_index_data_type(data_type):
            return 'index'
        elif cls.is_tick_data_type(data_type):
            return 'tick'
        return 'other'
    
    @classmethod
    def get_all_data_types(cls) -> List['DataType']:
        """Get a list of all available data types.
        
        Returns:
            List[DataType]: A list of all DataType enum members
        """
        return list(cls)
    
    @classmethod
    def from_string(cls, value: str) -> 'DataType':
        """Convert a string to a DataType enum member.
        
        Args:
            value: The string value to convert
            
        Returns:
            DataType: The corresponding DataType enum member
            
        Raises:
            ValueError: If the string does not match any DataType
        """
        try:
            return cls(value)
        except ValueError as e:
            valid_values = [dt.value for dt in cls]
            raise ValueError(
                f"Invalid data type: {value}. "
                f"Valid values are: {', '.join(valid_values)}"
            ) from e
    CUSTOM_DATA = "custom_data"


class DataRequest(BaseModel):
    """
    表示对数据源的金融市场数据请求。
    
    此类定义了数据请求的结构和验证规则。它支持单个或多个标的符号的请求、
    日期范围选择和字段筛选。
    
    该类提供了类型安全、输入验证和便捷的方法来处理金融数据请求。
    设计上足够灵活以支持各种数据源，同时保持一致的接口。
    
    属性:
        data_type: 请求的数据类型（例如：stock_daily, stock_minute）
        symbol: 单个标的符号（与symbols互斥）
        symbols: 标的符号列表（与symbol互斥）
        start_date: 数据范围的开始日期（包含）
        end_date: 数据范围的结束日期（包含）
        fields: 响应中包含的特定字段
        extra_params: 数据源特定的额外参数
        
    示例:
        >>> # 请求单只股票的日线数据
        >>> request = DataRequest(
        ...     data_type=DataType.STOCK_DAILY,
        ...     symbol="600519.SH",
        ...     start_date="2023-01-01",
        ...     end_date="2023-12-31",
        ...     fields=["open", "high", "low", "close", "volume"]
        ... )
        >>>
        >>> # 请求多只股票的数据
        >>> request = DataRequest(
        ...     data_type=DataType.STOCK_DAILY,
        ...     symbols=["600519.SH", "000858.SZ", "000333.SZ"],
        ...     start_date=date(2023, 1, 1),
        ...     end_date=date(2023, 12, 31)
        ... )
    """
    data_type: DataType = Field(
        ...,
        description=(
            "请求的数据类型。"
            "这决定了响应数据的结构和内容。"
        ),
        example=DataType.STOCK_DAILY
    )
    
    symbol: Optional[str] = Field(
        None,
        description=(
            "请求数据的单个金融工具代码。"
            "与'symbols'互斥。例如：'600519.SH' 或 'AAPL.US'"
        ),
        min_length=1,
        max_length=50,
        pattern=r'^[a-zA-Z0-9\.\-:]+$',  # 基本的代码格式验证
        example="600519.SH"
    )
    
    symbols: Optional[List[str]] = Field(
        None,
        description=(
            "请求数据的金融工具代码列表。"
            "与'symbol'互斥。"
            "用于批量请求以减少API调用次数。"
        ),
        min_items=1,
        max_items=1000,  # 防止过大的请求
        example=["600519.SH", "000858.SZ"]
    )
    
    start_date: Optional[Union[str, date, datetime]] = Field(
        None,
        description=(
            "数据请求的日期范围开始（包含）。"
            "可以是YYYY-MM-DD格式的日期字符串或date/datetime对象。"
            "如果未提供，数据源可能会应用默认值或返回错误。"
        ),
        example="2023-01-01"
    )
    
    end_date: Optional[Union[str, date, datetime]] = Field(
        None,
        description=(
            "数据请求的日期范围结束（包含）。"
            "可以是YYYY-MM-DD格式的日期字符串或date/datetime对象。"
            "如果未提供，默认为当前日期或数据源确定的其他值。"
        ),
        example="2023-12-31"
    )
    
    fields: Optional[List[str]] = Field(
        None,
        description=(
            "响应中包含的字段名列表。"
            "如果为None或空列表，则返回所有可用字段。"
            "这允许客户端通过仅请求它们需要的字段来减少网络流量。"
        ),
        min_items=1,
        example=["open", "high", "low", "close", "volume"]
    )
    
    extra_params: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "特定于数据源的额外参数。"
            "这允许在不修改基础请求结构的情况下使用数据源特定的功能。"
            "常见参数包括：复权类型、频率覆盖或数据质量过滤器。"
        ),
        example={"adjust": "hfq", "freq": "daily"}
    )
    
    # Pydantic configuration
    class Config:
        # Custom JSON encoders for date/datetime objects
        json_encoders = {
            date: lambda d: d.isoformat(),
            datetime: lambda d: d.isoformat(),
            DataType: lambda dt: dt.value  # Use the string value for DataType
        }
        
        # Prevent extra fields from being allowed
        extra = "forbid"
        
        # Enable validation on assignment
        validate_assignment = True
        
        # Enable arbitrary types for custom types like DataType
        arbitrary_types_allowed = True
        
        @classmethod
        def schema_extra(cls, schema: Dict[str, Any], model: Type['DataRequest']) -> None:
            """Add OpenAPI schema examples for better API documentation."""
            schema["examples"] = {
                "single_stock_daily": {
                    "summary": "Daily data for a single stock",
                    "value": {
                        "data_type": "stock_daily",
                        "symbol": "600519.SH",
                        "start_date": "2023-01-01",
                        "end_date": "2023-12-31",
                        "fields": ["open", "high", "low", "close", "volume", "amount"],
                        "extra_params": {"adjust": "hfq"}
                    }
                },
                "multiple_stocks": {
                    "summary": "Daily data for multiple stocks",
                    "value": {
                        "data_type": "stock_daily",
                        "symbols": ["600519.SH", "000858.SZ", "000333.SZ"],
                        "start_date": "2023-01-01",
                        "end_date": "2023-12-31"
                    }
                },
                "index_data": {
                    "summary": "Index component data",
                    "value": {
                        "data_type": "index_component",
                        "symbol": "000300.SH",
                        "fields": ["symbol", "name", "weight"]
                    }
                }
            }
    
    # 验证器
    @validator('symbol', 'symbols')
    def check_symbol_or_symbols(cls, v, values, **kwargs):
        """
        确保只提供了symbol或symbols中的一个，不能同时提供。
        
        Args:
            v: 当前字段的值
            values: 已经验证过的字段值
            **kwargs: 其他参数
            
        Raises:
            ValueError: 如果同时指定了'symbol'和'symbols'
            
        Returns:
            验证后的字段值
        """
        if 'symbol' in values and 'symbols' in values:
            if values['symbol'] is not None and values['symbols'] is not None:
                raise ValueError("不能同时指定'symbol'和'symbols'")
        return v
    
    @validator('start_date', 'end_date', pre=True)
    def parse_date(cls, v):
        """
        将字符串日期解析为日期对象。
        
        Args:
            v: 要解析的日期值，可以是字符串、date或datetime对象
            
        Returns:
            date或datetime对象
            
        Raises:
            ValueError: 如果日期格式无效
        """
        if v is None:
            return v
            
        if isinstance(v, (date, datetime)):
            return v
            
        if isinstance(v, str):
            try:
                # 首先尝试ISO格式 (YYYY-MM-DD)
                return datetime.strptime(v, "%Y-%m-%d").date()
            except ValueError:
                try:
                    # 尝试datetime格式
                    return datetime.fromisoformat(v)
                except (ValueError, TypeError):
                    pass
        
        raise ValueError(f"无效的日期格式: {v}。应为YYYY-MM-DD或ISO格式")
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        """
        验证结束日期不早于开始日期。
        
        Args:
            v: 结束日期值
            values: 已验证的字段值
            
        Returns:
            验证后的结束日期
            
        Raises:
            ValueError: 如果结束日期早于开始日期
        """
        if 'start_date' in values and values['start_date'] is not None and v is not None:
            start = values['start_date']
            end = v
            
            # 如果需要，转换为日期对象进行比较
            if isinstance(start, datetime):
                start = start.date()
            if isinstance(end, datetime):
                end = end.date()
                
            if end < start:
                raise ValueError("结束日期不能早于开始日期")
        return v
    
    @validator('fields', each_item=True)
    def validate_field_names(cls, v):
        """
        验证字段名，防止注入或无效字符。
        
        Args:
            v: 字段名
            
        Returns:
            验证后的字段名
            
        Raises:
            ValueError: 如果字段名包含无效字符
        """
        if not v.isidentifier() or not v.replace('_', '').isalnum():
            raise ValueError(f"无效的字段名: {v}")
        return v
    
    # 辅助方法
    def get_symbols(self) -> List[str]:
        """
        从请求中获取标的代码列表。
        
        返回:
            List[str]: 标的代码列表，即使请求中只包含单个代码
            
        示例:
            >>> request = DataRequest(symbol="600519.SH", data_type=DataType.STOCK_DAILY)
            >>> request.get_symbols()
            ['600519.SH']
            >>> request = DataRequest(symbols=["600519.SH", "000858.SZ"], data_type=DataType.STOCK_DAILY)
            >>> request.get_symbols()
            ['600519.SH', '000858.SZ']
        """
        if self.symbols is not None:
            return self.symbols
        elif self.symbol is not None:
            return [self.symbol]
        return []
    
    def with_symbols(self, symbols: Union[str, List[str]]) -> 'DataRequest':
        """
        创建一个带有指定标的代码的新请求。
        
        参数:
            symbols: 单个代码或代码列表
            
        返回:
            DataRequest: 更新了标的代码的新DataRequest实例
            
        示例:
            >>> request = DataRequest(data_type=DataType.STOCK_DAILY)
            >>> new_request = request.with_symbols("600519.SH")
            >>> new_request.symbol
            '600519.SH'
        """
        if isinstance(symbols, str):
            return self.copy(update={"symbol": symbols, "symbols": None})
        return self.copy(update={"symbol": None, "symbols": symbols})
    
    def with_date_range(self, start_date: Optional[Union[str, date, datetime]] = None, 
                       end_date: Optional[Union[str, date, datetime]] = None) -> 'DataRequest':
        """
        创建一个带有指定日期范围的新请求。
        
        参数:
            start_date: 数据范围的开始日期
            end_date: 数据范围的结束日期
            
        返回:
            DataRequest: 更新了日期范围的新DataRequest实例
        """
        return self.copy(update={"start_date": start_date, "end_date": end_date})
    
    def with_fields(self, fields: Optional[List[str]]) -> 'DataRequest':
        """
        创建一个带有指定字段的新请求。
        
        参数:
            fields: 响应中要包含的字段名列表
            
        返回:
            DataRequest: 更新了字段列表的新DataRequest实例
        """
        return self.copy(update={"fields": fields})
    
    def with_extra_params(self, **params) -> 'DataRequest':
        """
        创建一个带有额外参数的新请求。
        
        参数:
            **params: 额外的参数字典
            
        返回:
            DataRequest: 更新了额外参数的新DataRequest实例
            
        示例:
            >>> request = DataRequest(data_type=DataType.STOCK_DAILY, symbol="600519.SH")
            >>> new_request = request.with_extra_params(adjust="hfq", freq="daily")
            >>> new_request.extra_params
            {'adjust': 'hfq', 'freq': 'daily'}
        """
        extra = self.extra_params.copy()
        extra.update(params)
        return self.copy(update={"extra_params": extra})
    
    def to_dict(self, exclude_none: bool = True, **kwargs) -> Dict[str, Any]:
        """
        将请求转换为字典。
        
        参数:
            exclude_none: 是否从结果中排除None值
            **kwargs: 传递给dict()的额外参数
            
        返回:
            Dict[str, Any]: 请求的字典表示
        """
        if exclude_none:
            return {
                k: v for k, v in self.dict(**kwargs).items() 
                if v is not None and v != ([] if k == 'fields' else {})
            }
        return self.dict(**kwargs)
    
    def __str__(self) -> str:
        """
        返回请求的字符串表示。
        
        返回:
            str: 请求的字符串表示
        """
        symbols = self.symbol if self.symbol is not None else self.symbols
        date_range = ""
        if self.start_date or self.end_date:
            start = self.start_date.strftime("%Y-%m-%d") if self.start_date else ""
            end = self.end_date.strftime("%Y-%m-%d") if self.end_date else ""
            date_range = f" 从 {start} 到 {end}"
            
        fields = f"，包含字段 {self.fields}" if self.fields else ""
        return f"<DataRequest: {self.data_type} for {symbols}{date_range}{fields}>"


class DataResponse(BaseModel):
    """
    Represents a standardized response from a data source.
    
    This class provides a consistent interface for returning data from any data source,
    including success/failure status, error handling, and metadata. It supports both
    successful responses with data and error responses with appropriate error messages.
    
    The response can contain data in multiple formats (DataFrame, dict, etc.) and includes
    metadata about the request and response process.
    
    Attributes:
        data: The response data, typically as a pandas DataFrame or dictionary
        source: Name of the data source that generated this response
        data_type: Type of data contained in the response
        timestamp: When the response was generated (UTC)
        success: Whether the request was successful
        error_message: Error message if the request failed
        metadata: Additional metadata about the response
        
    Example:
        >>> # Successful response with DataFrame data
        >>> df = pd.DataFrame({"symbol": ["AAPL"], "price": [150.0]})
        >>> response = DataResponse(
        ...     data=df,
        ...     source="YahooFinance",
        ...     data_type=DataType.STOCK_DAILY,
        ...     success=True
        ... )
        >>> 
        >>> # Error response
        >>> error_response = DataResponse.from_error(
        ...     error="Invalid symbol",
        ...     source="YahooFinance",
        ...     data_type=DataType.STOCK_DAILY
        ... )
    """
    data: Union[pd.DataFrame, Dict[str, Any]] = Field(
        ...,
        description=(
            "The response data. Can be a pandas DataFrame for tabular data "
            "or a dictionary for structured data. For error responses, this "
            "is typically an empty dictionary."
        ),
        example={"symbol": ["AAPL"], "price": [150.0]}
    )
    
    source: str = Field(
        ...,
        description=(
            "Name of the data source that generated this response. "
            "This should match the name used when initializing the data source."
        ),
        example="YahooFinance",
        min_length=1,
        max_length=100
    )
    
    data_type: DataType = Field(
        ...,
        description=(
            "Type of data contained in the response. "
            "This should match the DataType from the original request."
        ),
        example=DataType.STOCK_DAILY
    )
    
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description=(
            "Timestamp when the response was generated, in UTC. "
            "Defaults to the current UTC time if not specified."
        ),
        example="2023-01-01T12:00:00Z"
    )
    
    success: bool = Field(
        ...,
        description=(
            "Whether the request was successful. "
            "If False, the error_message field should contain details "
            "about what went wrong."
        ),
        example=True
    )
    
    error_message: Optional[str] = Field(
        None,
        description=(
            "Error message describing what went wrong if the request failed. "
            "This should be None for successful responses."
        ),
        example="Invalid symbol: AAPLXX"
    )
    
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "数据源的附加元数据。\n"
            "可以包含以下信息：\n"
            "- 提供方信息(provider): 数据提供方名称\n"
            "- 文档链接(api_docs): API文档URL\n"
            "- 认证要求(requires_auth): 是否需要认证\n"
            "- 试用信息(trial_available): 是否提供试用\n"
            "- 其他提供方特定的配置项"
        ),
        example={
            "provider": "Yahoo Finance",
            "api_docs": "https://finance.yahoo.com",
            "requires_auth": False,
            "trial_available": True,
            "supported_markets": ["US", "HK", "SH", "SZ"],
            "data_delay": "15分钟"
        }
    )
    
    # Pydantic configuration
    class Config:
        # Allow arbitrary types to support pandas DataFrame
        arbitrary_types_allowed = True
        
        # Custom JSON encoders for special types
        json_encoders = {
            pd.DataFrame: lambda df: df.to_dict(orient="records"),
            datetime: lambda v: v.isoformat(),
            DataType: lambda dt: dt.value  # Use string value for DataType
        }
        
        # Enable validation on assignment
        validate_assignment = True
        
        @classmethod
        def schema_extra(cls, schema: Dict[str, Any], model: Type['DataResponse']) -> None:
            """Add OpenAPI schema examples for better API documentation."""
            schema["examples"] = {
                "successful_response": {
                    "summary": "Successful response with stock data",
                    "value": {
                        "data": [
                            {"date": "2023-01-01", "symbol": "AAPL", "close": 150.0},
                            {"date": "2023-01-02", "symbol": "AAPL", "close": 152.0}
                        ],
                        "source": "YahooFinance",
                        "data_type": "stock_daily",
                        "timestamp": "2023-01-02T16:30:00Z",
                        "success": True,
                        "metadata": {
                            "request_id": "req_123456",
                            "items_returned": 2
                        }
                    }
                },
                "error_response": {
                    "summary": "Error response for invalid request",
                    "value": {
                        "data": {},
                        "source": "YahooFinance",
                        "data_type": "stock_daily",
                        "timestamp": "2023-01-02T16:31:00Z",
                        "success": False,
                        "error_message": "Invalid symbol: AAPLXX",
                        "metadata": {
                            "error_type": "InvalidSymbolError",
                            "request_id": "req_789012",
                            "suggestions": ["AAPL", "AAPL.MX"]
                        }
                    }
                }
            }
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert the response data to a pandas DataFrame.
        
        Returns:
            pd.DataFrame: The response data as a DataFrame. If the data is already
                a DataFrame, it is returned as-is. If the data is a dictionary,
                it is converted to a DataFrame. Returns an empty DataFrame if
                the data is None or empty.
                
        Example:
            >>> response = DataResponse(
            ...     data={"symbol": ["AAPL"], "price": [150.0]},
            ...     source="Test",
            ...     data_type=DataType.STOCK_DAILY,
            ...     success=True
            ... )
            >>> df = response.to_dataframe()
            >>> df
              symbol  price
            0   AAPL  150.0
        """
        if isinstance(self.data, pd.DataFrame):
            return self.data
        return pd.DataFrame(self.data) if self.data else pd.DataFrame()
    
    def to_dict(self, exclude_none: bool = True, **kwargs) -> Dict[str, Any]:
        """
        Convert the response to a dictionary.
        
        Args:
            exclude_none: Whether to exclude None values from the result
            **kwargs: Additional arguments to pass to dict()
            
        Returns:
            Dict[str, Any]: Dictionary representation of the response
            
        Example:
            >>> response = DataResponse(
            ...     data={"symbol": "AAPL", "price": 150.0},
            ...     source="Test",
            ...     data_type=DataType.STOCK_DAILY,
            ...     success=True
            ... )
            >>> response.to_dict()
            {
                'data': {'symbol': 'AAPL', 'price': 150.0},
                'source': 'Test',
                'data_type': 'stock_daily',
                'timestamp': '2023-01-01T12:00:00',
                'success': True,
                'metadata': {}
            }
        """
        if exclude_none:
            return {
                k: v for k, v in self.dict(**kwargs).items() 
                if v is not None and v != ([] if k == 'fields' else {})
            }
        return self.dict(**kwargs)
    
    @classmethod
    def from_error(
        cls,
        error: Union[str, Exception],
        source: str,
        data_type: Union[DataType, str],
        **kwargs
    ) -> 'DataResponse':
        """
        Create a DataResponse instance from an error.
        
        This is a convenience method for creating error responses. The resulting
        response will have success=False and the error details will be populated.
        
        Args:
            error: The error that occurred. Can be an Exception instance or a string.
            source: Name of the data source that generated the error.
            data_type: Type of data that was being requested.
            **kwargs: Additional metadata to include in the response.
            
        Returns:
            DataResponse: A new DataResponse instance representing the error.
            
        Example:
            >>> # From a string error
            >>> response = DataResponse.from_error(
            ...     error="Invalid symbol: AAPLXX",
            ...     source="YahooFinance",
            ...     data_type=DataType.STOCK_DAILY,
            ...     request_id="req_123456"
            ... )
            >>> 
            >>> # From an exception
            >>> try:
            ...     # Some operation that might fail
            ...     raise ValueError("Invalid symbol")
            ... except Exception as e:
            ...     response = DataResponse.from_error(e, "YahooFinance", DataType.STOCK_DAILY)
        """
        if isinstance(data_type, str):
            try:
                data_type = DataType.from_string(data_type)
            except ValueError:
                # If we can't parse the data type, use a default
                data_type = DataType.CUSTOM_DATA
        
        error_type = "Error"
        error_message = str(error)
        
        if hasattr(error, '__class__'):
            error_type = error.__class__.__name__
            
            # For HTTP errors, extract more details
            if hasattr(error, 'response'):
                try:
                    error_response = getattr(error, 'response', {})
                    error_message = getattr(error_response, 'text', str(error))
                    if not error_message:
                        error_message = f"HTTP {getattr(error_response, 'status_code', '')}: {str(error)}"
                except:
                    pass
        
        # Prepare metadata with error details
        metadata = {
            "error_type": error_type,
            **kwargs
        }
        
        # Add traceback for debugging if available
        if hasattr(error, '__traceback__'):
            import traceback
            metadata["traceback"] = ''.join(
                traceback.format_exception(
                    type(error), error, error.__traceback__
                )
            )
        
        return cls(
            data={},
            source=source,
            data_type=data_type,
            success=False,
            error_message=error_message,
            metadata=metadata
        )
    
    def raise_for_status(self) -> None:
        """
        Raise an exception if the response indicates an error.
        
        Raises:
            DataSourceError: If the response indicates a failure
            
        Example:
            >>> response = DataResponse.from_error("Invalid symbol", "Test", "stock_daily")
            >>> response.raise_for_status()
            Traceback (most recent call last):
                ...
            DataSourceError: Request failed: Invalid symbol
        """
        if not self.success:
            error_msg = self.error_message or "Unknown error"
            error_type = self.metadata.get('error_type', 'DataSourceError')
            
            # Map to appropriate exception type if known
            if error_type == 'RateLimitError':
                raise RateLimitError(
                    source=self.source,
                    retry_after=self.metadata.get('retry_after'),
                    rate_limit=self.metadata.get('rate_limit'),
                    details={"message": error_msg, **self.metadata}
                )
            elif error_type == 'AuthenticationError':
                raise AuthenticationError(
                    source=self.source,
                    details={"message": error_msg, **self.metadata}
                )
            elif error_type == 'NetworkError':
                raise NetworkError(
                    source=self.source,
                    details={"message": error_msg, **self.metadata}
                )
            elif error_type == 'DataNotAvailableError':
                raise DataNotAvailableError(
                    source=self.source,
                    data_type=self.data_type,
                    details={"message": error_msg, **self.metadata}
                )
            else:
                raise DataSourceError(
                    source=self.source,
                    details={"message": error_msg, **self.metadata}
                )
    
    def __str__(self) -> str:
        """Return a string representation of the response."""
        status = "SUCCESS" if self.success else f"ERROR: {self.error_message}"
        data_type = self.data_type.value if hasattr(self.data_type, 'value') else str(self.data_type)
        
        data_preview = ""
        if self.data is not None:
            if isinstance(self.data, pd.DataFrame):
                data_preview = f"{len(self.data)} rows x {len(self.data.columns)} columns"
            elif isinstance(self.data, dict):
                data_preview = f"dict with {len(self.data)} items"
            elif isinstance(self.data, (list, tuple)):
                data_preview = f"{len(self.data)} items"
            else:
                data_preview = str(type(self.data).__name__)
        
        return (
            f"<DataResponse: {status} | Source: {self.source} | "
            f"Type: {data_type} | Data: {data_preview}>"
        )


class DataSourceStatus(str, Enum):
    """
    Represents the operational status of a data source.
    
    This enum defines the possible states a data source can be in, which helps
    in monitoring and managing the data source's availability and health. The status
    can be used to determine if a data source is ready to accept requests, is currently
    limited, or is experiencing issues.
    
    The enum values are lowercase strings to ensure consistency with REST API conventions
    and to make them easily serializable to JSON.
    
    Attributes:
        READY: The data source is initialized and ready to accept requests.
        AVAILABLE: The data source is currently available and operating normally.
        UNAVAILABLE: The data source is temporarily unavailable (may be down for maintenance).
        LIMITED: The data source is available but with limited functionality
                (e.g., rate limited, degraded performance).
        ERROR: The data source is in an error state and cannot process requests.
        CLOSED: The data source has been explicitly closed and cannot be used.
    
    Example:
        >>> status = DataSourceStatus.AVAILABLE
        >>> status.is_available()
        True
        >>> status.value
        'available'
        >>> DataSourceStatus.from_string('limited')
        <DataSourceStatus.LIMITED: 'limited'>
    """
    READY = "ready"
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    LIMITED = "limited"
    ERROR = "error"
    CLOSED = "closed"
    
    def is_available(self) -> bool:
        """
        Check if the status indicates the data source is available for use.
        
        Returns:
            bool: True if the status is either READY or AVAILABLE, False otherwise.
            
        Example:
            >>> DataSourceStatus.READY.is_available()
            True
            >>> DataSourceStatus.AVAILABLE.is_available()
            True
            >>> DataSourceStatus.UNAVAILABLE.is_available()
            False
        """
        return self in {DataSourceStatus.READY, DataSourceStatus.AVAILABLE}
    
    def is_error(self) -> bool:
        """
        Check if the status indicates an error condition.
        
        Returns:
            bool: True if the status is ERROR, False otherwise.
            
        Example:
            >>> DataSourceStatus.ERROR.is_error()
            True
            >>> DataSourceStatus.AVAILABLE.is_error()
            False
        """
        return self == DataSourceStatus.ERROR
    
    def is_limited(self) -> bool:
        """
        Check if the status indicates limited functionality.
        
        Returns:
            bool: True if the status is LIMITED, False otherwise.
            
        Example:
            >>> DataSourceStatus.LIMITED.is_limited()
            True
            >>> DataSourceStatus.AVAILABLE.is_limited()
            False
        """
        return self == DataSourceStatus.LIMITED
    
    def is_unavailable(self) -> bool:
        """
        Check if the status indicates the data source is unavailable.
        
        Returns:
            bool: True if the status is UNAVAILABLE or CLOSED, False otherwise.
            
        Example:
            >>> DataSourceStatus.UNAVAILABLE.is_unavailable()
            True
            >>> DataSourceStatus.CLOSED.is_unavailable()
            True
            >>> DataSourceStatus.AVAILABLE.is_unavailable()
            False
        """
        return self in {DataSourceStatus.UNAVAILABLE, DataSourceStatus.CLOSED}
    
    def can_accept_requests(self) -> bool:
        """
        Check if the data source can accept new requests.
        
        Returns:
            bool: True if the status is READY, AVAILABLE, or LIMITED, False otherwise.
            
        Example:
            >>> DataSourceStatus.AVAILABLE.can_accept_requests()
            True
            >>> DataSourceStatus.LIMITED.can_accept_requests()
            True
            >>> DataSourceStatus.ERROR.can_accept_requests()
            False
        """
        return self in {
            DataSourceStatus.READY,
            DataSourceStatus.AVAILABLE,
            DataSourceStatus.LIMITED
        }
    
    @classmethod
    def from_string(cls, value: str) -> 'DataSourceStatus':
        """
        Convert a string to a DataSourceStatus enum value.
        
        Args:
            value: The string value to convert (case-insensitive).
            
        Returns:
            DataSourceStatus: The corresponding enum value.
            
        Raises:
            ValueError: If the string does not match any enum value.
            
        Example:
            >>> DataSourceStatus.from_string('available')
            <DataSourceStatus.AVAILABLE: 'available'>
            >>> DataSourceStatus.from_string('ERROR')
            <DataSourceStatus.ERROR: 'error'>
            >>> DataSourceStatus.from_string('invalid')
            Traceback (most recent call last):
                ...
            ValueError: 'invalid' is not a valid DataSourceStatus
        """
        try:
            return cls(value.lower())
        except ValueError:
            valid_values = [e.value for e in cls]
            raise ValueError(
                f"'{value}' is not a valid DataSourceStatus. "
                f"Valid values are: {', '.join(valid_values)}"
            ) from None
    
    def __str__(self) -> str:
        """
        Return the string representation of the status.
        
        Returns:
            str: The string value of the enum.
            
        Example:
            >>> str(DataSourceStatus.AVAILABLE)
            'available'
        """
        return self.value
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the status to a dictionary representation.
        
        Returns:
            Dict[str, Any]: A dictionary with the status value and display name.
            
        Example:
            >>> DataSourceStatus.AVAILABLE.to_dict()
            {'value': 'available', 'display_name': 'Available'}
        """
        return {
            'value': self.value,
            'display_name': self.value.capitalize()
        }
    
    @classmethod
    def get_all_statuses(cls) -> List[Dict[str, Any]]:
        """
        Get all possible statuses with their metadata.
        
        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing status information.
            
        Example:
            >>> DataSourceStatus.get_all_statuses()
            [
                {'value': 'ready', 'display_name': 'Ready'},
                {'value': 'available', 'display_name': 'Available'},
                ...
            ]
        """
        return [status.to_dict() for status in cls]


class DataSourceInfo(BaseModel):
    """
    数据源元数据与能力描述类
    
    此类提供数据源的全面信息，包括其能力、限制和当前状态。
    用于查询和验证数据源支持的操作，是数据源发现和选择的关键组件。
    
    主要功能：
    - 描述数据源的基本信息和能力
    - 记录数据源的状态和限制
    - 支持数据源发现和选择
    - 提供数据源使用统计和成本信息
    
    Attributes:
        name: Unique identifier for the data source (e.g., "yahoo_finance", "binance")
        type: Categorization of the data source (e.g., MARKET, FUNDAMENTAL, ALTERNATIVE)
        status: Current operational status of the data source
        supported_data_types: List of data types this source can provide
        rate_limit: Maximum requests per minute (None means no explicit limit)
        cost_per_request: Cost in USD per API call (if applicable)
        priority: Priority level (1-10) for source selection (lower = higher priority)
        description: Human-readable description of the data source
        version: Version string following semantic versioning
        last_updated: Timestamp of the last update to this data source's information
        
    Example:
        >>> info = DataSourceInfo(
        ...     name="yahoo_finance",
        ...     type=DataSourceType.MARKET,
        ...     status=DataSourceStatus.ACTIVE,
        ...     supported_data_types=[
        ...         DataType.STOCK_DAILY,
        ...         DataType.STOCK_INTRADAY
        ...     ],
        ...     rate_limit=2000,  # 2000 requests per minute
        ...     cost_per_request=0.0,  # Free tier
        ...     priority=1,  # High priority
        ...     description="Yahoo Finance market data provider",
        ...     version="1.2.0"
        ... )
    """
    name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        pattern=r'^[a-z][a-z0-9_]*(?:-[a-z0-9]+)*$',
        description=(
            "数据源的唯一标识符。\n"
            "命名规范：\n"
            "- 使用小写字母\n"
            "- 单词间用下划线分隔\n"
            "- 必须以字母开头\n"
            "- 只能包含字母、数字和下划线\n"
            "- 长度限制为1-100个字符"
        ),
        example="yahoo_finance"
    )
    
    type: DataSourceType = Field(
        ...,
        description=(
            "数据源的分类。\n"
            "决定此数据源提供的数据的通用类别，"
            "如市场数据、基本面数据、另类数据等。"
        ),
        example=DataSourceType.AKSHARE
    )
    
    status: DataSourceStatus = Field(
        default=DataSourceStatus.UNAVAILABLE,
        description=(
            "数据源的当前运行状态。\n"
            "控制此数据源当前是否可用。\n"
            "系统会根据此状态决定是否向此数据源发送请求。"
        )
    )
    
    supported_data_types: List[DataType] = Field(
        default_factory=list,
        description=(
            "此数据源支持的数据类型列表。\n"
            "用于确定数据源是否能够满足特定的数据请求。\n"
            "如果列表为空，表示此数据源当前不支持任何数据类型。"
        ),
        example=[DataType.STOCK_DAILY, DataType.STOCK_MINUTE]
    )
    
    rate_limit: Optional[int] = Field(
        None,
        ge=0,
        description=(
            "每分钟允许的最大请求数。\n"
            "设置为None表示无限制。\n"
            "用于速率限制和请求调度，防止超过API限制。"
        ),
        example=2000
    )
    
    cost_per_request: Optional[float] = Field(
        None,
        ge=0.0,
        description=(
            "每次API请求的成本（美元）。\n"
            "用于成本跟踪和优化。\n"
            "对于免费层级或无限制计划，请设置为0。\n"
            "如果为None，表示成本未知或可变。"
        ),
        example=0.0
    )
    
    priority: int = Field(
        default=5,
        ge=1,
        le=10,
        description=(
            "数据源的优先级（1-10）。\n"
            "数值越小，优先级越高。\n"
            "当多个数据源都能满足请求时，\n"
            "系统会优先使用优先级最高的数据源。\n"
            "默认值为5，建议关键数据源使用1-3，\n"
            "备选数据源使用6-10。"
        ),
        example=1
    )
    
    description: Optional[str] = Field(
        None,
        max_length=1000,
        description=(
            "数据源的人类可读描述。\n"
            "应包含以下信息：\n"
            "- 数据覆盖范围（如市场、资产类别、地区）\n"
            "- 数据更新频率（实时、分钟级、日级等）\n"
            "- 任何特殊考虑或限制（如历史数据范围、请求限制等）\n"
            "- 认证要求和访问权限"
        ),
        example=(
            "提供全球股票、ETF、共同基金等金融工具的实时和历史市场数据。\n"
            "免费层级提供有限的数据访问频率。专业版提供更高的请求限制和更全面的数据集。\n"
            "历史数据可追溯至1970年。支持日线、周线、月线和分钟级K线数据。"
        )
    )
    
    version: str = Field(
        "1.0.0",
        pattern=r'^\d+\.\d+\.\d+(?:-[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*)?(?:\+[a-zA-Z0-9-]+)?$',
        description=(
            "遵循语义化版本规范(SemVer)的版本字符串。\n"
            "格式: 主版本号.次版本号.修订号[-预发布版本][+构建元数据]\n\n"
            "版本号递增规则：\n"
            "- 主版本号(MAJOR): 做了不兼容的API修改\n"
            "- 次版本号(MINOR): 向下兼容的功能性新增\n"
            "- 修订号(PATCH): 向下兼容的问题修正"
        ),
        example="1.2.0"
    )
    
    last_updated: datetime = Field(
        default_factory=datetime.utcnow,
        description=(
            "此数据源信息最后更新的时间戳。\n"
            "用于确定元数据是否为最新。\n"
            "系统会自动维护此字段，通常不需要手动设置。"
        )
    )
    
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "数据源的附加元数据。\n"
            "可以包含以下信息：\n"
            "- 提供方信息(provider): 数据提供方名称\n"
            "- 文档链接(api_docs): API文档URL\n"
            "- 认证要求(requires_auth): 是否需要认证\n"
            "- 试用信息(trial_available): 是否提供试用\n"
            "- 其他提供方特定的配置项"
        ),
        example={
            "provider": "Yahoo Finance",
            "api_docs": "https://finance.yahoo.com",
            "requires_auth": False,
            "trial_available": True,
            "supported_markets": ["US", "HK", "SH", "SZ"],
            "data_delay": "15分钟"
        }
    )
    
    @validator('supported_data_types', each_item=True)
    def validate_data_types(cls, v: DataType) -> DataType:
        """
        验证supported_data_types中的每个元素是否为有效的DataType枚举值。
        
        Args:
            v (DataType): 需要验证的数据类型
            
        Returns:
            DataType: 验证通过的数据类型
            
        Raises:
            ValueError: 如果值不是有效的DataType枚举实例
            
        Example:
            >>> DataSourceInfo.validate_data_types(DataType.STOCK_DAILY)
            <DataType.STOCK_DAILY: 'stock_daily'>
            
            >>> DataSourceInfo.validate_data_types("invalid_type")
            ValueError: Invalid data type: invalid_type. Must be an instance of DataType.
        """
        if not isinstance(v, DataType):
            raise ValueError(f"无效的数据类型: {v}. 必须是DataType枚举的实例。")
        return v
    
    def is_data_type_supported(self, data_type: Union[DataType, str]) -> bool:
        """
        检查此数据源是否支持指定的数据类型。
        
        Args:
            data_type (Union[DataType, str]): 要检查的数据类型，可以是DataType枚举或字符串
            
        Returns:
            bool: 如果支持该数据类型则返回True，否则返回False
            
        Example:
            >>> info = DataSourceInfo(
            ...     name="test",
            ...     type=DataSourceType.MARKET,
            ...     supported_data_types=[DataType.STOCK_DAILY]
            ... )
            >>> info.is_data_type_supported("stock_daily")  # 支持字符串形式
            True
            >>> info.is_data_type_supported(DataType.STOCK_DAILY)  # 支持枚举形式
            True
            >>> info.is_data_type_supported("stock_intraday")  # 不支持的
            False
            
        Note:
            - 如果传入的是字符串，会先尝试转换为DataType枚举
            - 如果字符串无法转换为有效的DataType，直接返回False
        """
        if isinstance(data_type, str):
            try:
                data_type = DataType.from_string(data_type)
            except ValueError:
                return False
        return data_type in self.supported_data_types
    
    def get_status_display(self) -> str:
        """
        获取状态的人类可读表示形式。
        
        Returns:
            str: 大写的状态字符串（如"ACTIVE"、"UNAVAILABLE"）
            
        Example:
            >>> info = DataSourceInfo(name="test", type=DataSourceType.MARKET)
            >>> info.get_status_display()
            'UNAVAILABLE'
            
            >>> info.status = DataSourceStatus.ACTIVE
            >>> info.get_status_display()
            'ACTIVE'
            
        Note:
            返回的状态字符串始终为大写，适合在日志或UI中显示。
        """
        return self.status.value.upper()
    
    def get_rate_limit_info(self) -> Dict[str, Any]:
        """
        获取速率限制信息的结构化格式。
        
        Returns:
            Dict[str, Any]: 包含速率限制详情的字典，包含以下键：
                - limit (int|None): 每分钟最大请求数，如果无限制则为None
                - unlimited (bool): 布尔值，表示是否无速率限制
                
        Example:
            >>> info = DataSourceInfo(name="test", type=DataSourceType.MARKET, rate_limit=1000)
            >>> info.get_rate_limit_info()
            {'limit': 1000, 'unlimited': False}
            
            >>> info.rate_limit = None
            >>> info.get_rate_limit_info()
            {'limit': None, 'unlimited': True}
            
        Note:
            当rate_limit为None时，表示没有明确的速率限制。
            这通常意味着数据源没有强制限制，或者限制未知。
        """
        return {
            'limit': self.rate_limit,
            'unlimited': self.rate_limit is None
        }
    
    def to_dict(self, include_metadata: bool = False) -> Dict[str, Any]:
        """
        将数据源信息转换为字典格式。
        
        Args:
            include_metadata (bool, optional): 是否包含元数据（如last_updated）。
                默认为False，不包括元数据。
            
        Returns:
            Dict[str, Any]: 包含数据源信息的字典
            
        Example:
            >>> info = DataSourceInfo(
            ...     name="test",
            ...     type=DataSourceType.MARKET,
            ...     supported_data_types=[DataType.STOCK_DAILY]
            ... )
            >>> info.to_dict()
            {
                'name': 'test',
                'type': 'market',
                'status': 'unavailable',
                'supported_data_types': ['stock_daily'],
                'rate_limit': None,
                'cost_per_request': None,
                'priority': 5,
                'description': None,
                'version': '1.0.0',
                'last_updated': '2023-01-01T00:00:00Z',
                'metadata': {...}
            }
            
        Note:
            - 枚举值会被自动转换为对应的字符串值
            - 日期时间对象会被转换为ISO格式的字符串
            - 默认包含metadata字段，但可以通过参数控制是否包含
        """
        # 使用Pydantic的dict方法获取基本字典表示
        data = self.dict(exclude={'metadata'} if not include_metadata else None)
        
        # 转换枚举值为对应的值
        data['type'] = self.type.value
        data['status'] = self.status.value
        data['supported_data_types'] = [dt.value for dt in self.supported_data_types]
        
        # 将datetime对象转换为ISO格式字符串
        if 'last_updated' in data and data['last_updated'] is not None:
            if isinstance(data['last_updated'], datetime):
                data['last_updated'] = data['last_updated'].isoformat()
        
        return data
    
    class Config:
        # Enable validation on assignment
        validate_assignment = True
        
        # Allow arbitrary types for custom validation
        arbitrary_types_allowed = True
        
        # Custom JSON encoders for special types
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            DataType: lambda dt: dt.value,
            DataSourceType: lambda dst: dst.value,
            DataSourceStatus: lambda dss: dss.value
        }
        
        # Schema examples for OpenAPI documentation
        schema_extra = {
            "example": {
                "name": "yahoo_finance",
                "type": "market",
                "status": "active",
                "version": "1.2.0",
                "supported_data_types": ["stock_daily", "stock_intraday"],
                "rate_limit": 2000,
                "cost_per_request": 0.0,
                "priority": 1,
                "description": "提供免费股票市场数据服务。支持全球主要交易所的实时和延迟数据。",
                "last_updated": "2023-01-01T12:00:00Z",
                "metadata": {
                    "provider": "Yahoo Finance",
                    "api_docs": "https://finance.yahoo.com",
                    "supported_markets": ["US", "HK", "SH", "SZ"],
                    "data_delay": "15分钟",
                    "requires_auth": False,
                    "trial_available": True
                }
            }
        }


class BaseDataSource(ABC):
    """
    数据源抽象基类
    
    所有数据源实现的基类，定义了统一的数据访问接口。
    子类需要实现抽象方法来提供具体的数据获取逻辑。
    
    Attributes:
        name (str): 数据源名称，用于标识不同的数据源实例
        source_type (DataSourceType): 数据源类型，如市场数据、基本面数据等
        config (Dict[str, Any]): 配置字典，包含数据源的各种配置项
        status (DataSourceStatus): 数据源当前状态，如就绪、可用、不可用等
        _last_request_time (Optional[datetime]): 最后一次请求的时间戳
        _request_count (int): 请求计数器，用于频率限制
        _lock (threading.RLock): 可重入锁，用于保证线程安全
        _initialized (bool): 标记数据源是否已初始化
        _closed (bool): 标记数据源是否已关闭
    """
    
    def __init__(self, name: str, source_type: DataSourceType, config: Optional[Dict[str, Any]] = None):
        """
        初始化数据源
        
        Args:
            name (str): 数据源名称，建议使用小写字母和下划线组合，如'yahoo_finance'
            source_type (DataSourceType): 数据源类型，必须是DataSourceType枚举值
            config (Optional[Dict[str, Any]], optional): 配置字典，支持以下键：
                - rate_limit (int, optional): 每分钟最大请求数，None表示无限制
                - cost_per_request (float, optional): 每次请求的成本(如API调用费用)
                - priority (int, optional): 优先级，1-10，值越小优先级越高
                - description (str, optional): 数据源描述信息
                - timeout (int, optional): 请求超时时间(秒)
                - retry (int, optional): 失败重试次数
                - retry_delay (int, optional): 重试间隔时间(秒)
                - api_key (str, optional): API密钥(如有需要)
                - base_url (str, optional): API基础URL
                
        Example:
            >>> config = {
            ...     'rate_limit': 100,  # 每分钟100次请求
            ...     'timeout': 30,      # 30秒超时
            ...     'retry': 3,         # 失败重试3次
            ...     'retry_delay': 1     # 每次重试间隔1秒
            ... }
            >>> source = YourDataSource('my_source', DataSourceType.MARKET, config)
        """
        self.name = name
        self.source_type = source_type
        self.config = config or {}
        self.status: DataSourceStatus = DataSourceStatus.UNAVAILABLE
        self._last_request_time: Optional[datetime] = None
        self._request_count: int = 0
        self._lock = threading.RLock()  # 可重入锁，用于线程安全
        self._initialized: bool = False  # 标记是否已初始化
        self._closed: bool = False  # 标记是否已关闭
    
    def __enter__(self) -> 'BaseDataSource':
        """
        上下文管理器入口，支持with语句
        
        Returns:
            BaseDataSource: 返回自身实例，支持链式调用
            
        Example:
            >>> with MyDataSource('source1', DataSourceType.MARKET) as source:
            ...     response = source.fetch_data(request)
        """
        self.initialize()
        return self
    
    def __exit__(self, exc_type: Optional[Type[BaseException]], 
                exc_val: Optional[BaseException], 
                exc_tb: Optional[TracebackType]) -> None:
        """
        上下文管理器退出时自动关闭连接
        
        Args:
            exc_type: 异常类型，如果没有异常则为None
            exc_val: 异常实例，如果没有异常则为None
            exc_tb: 异常回溯信息，如果没有异常则为None
            
        Note:
            无论是否发生异常，都会确保资源被正确释放
        """
        self.close()
    
    @abstractmethod
    def initialize(self) -> bool:
        """
        初始化数据源连接
        
        执行必要的初始化操作，如建立连接、验证凭据、加载配置等。
        此方法会在数据源首次使用前自动调用，也可以手动调用以重新初始化。
        
        Returns:
            bool: 如果初始化成功返回True，否则返回False
            
        Raises:
            DataSourceError: 初始化过程中发生错误时抛出
            AuthenticationError: 认证失败时抛出
            
        Example:
            >>> source = MyDataSource('source1', DataSourceType.MARKET)
            >>> if not source.initialize():
            ...     print("初始化失败")
        """
        pass
    
    @abstractmethod
    def get_supported_data_types(self) -> List[DataType]:
        """
        获取此数据源支持的数据类型
        
        返回此数据源能够提供的数据类型列表。
        系统会根据此方法返回的列表来验证数据请求是否可以被处理。
        
        Returns:
            List[DataType]: 支持的数据类型枚举值列表
            
        Example:
            >>> source = MyDataSource('source1', DataSourceType.MARKET)
            >>> supported = source.get_supported_data_types()
            >>> print(f"支持的数据类型: {supported}")
            [<DataType.STOCK_DAILY: 'stock_daily'>, ...]
            
        Note:
            子类必须重写此方法以返回实际支持的数据类型
        """
        pass
    
    @abstractmethod
    def check_availability(self) -> DataSourceStatus:
        """
        检查数据源的当前可用性状态
        
        执行必要的检查以确定数据源是否可用，包括但不限于：
        - 网络连接状态
        - 认证凭据有效性
        - API配额/限制
        - 服务端状态
        
        Returns:
            DataSourceStatus: 数据源的当前状态
            
        Example:
            >>> status = source.check_availability()
            >>> if status == DataSourceStatus.AVAILABLE:
            ...     print("数据源可用")
            ... else:
            ...     print(f"数据源状态: {status}")
        """
        pass
    
    @abstractmethod
    def fetch_data(self, request: DataRequest) -> DataResponse:
        """
        从数据源获取数据
        
        这是数据源的核心方法，负责处理数据请求并返回结果。
        实现时应该处理参数验证、请求构造、错误处理和结果格式化。
        
        Args:
            request (DataRequest): 包含数据请求参数的对象
            
        Returns:
            DataResponse: 包含请求结果或错误信息的响应对象
            
        Raises:
            DataSourceError: 数据获取过程中发生一般性错误时抛出
            RateLimitError: 请求频率超过限制时抛出
            DataNotAvailableError: 请求的数据不存在或不可用时抛出
            AuthenticationError: 认证失败或凭据无效时抛出
            NetworkError: 网络通信错误时抛出
            
        Example:
            >>> request = DataRequest(
            ...     data_type=DataType.STOCK_DAILY,
            ...     symbol='AAPL',
            ...     start_date='2023-01-01',
            ...     end_date='2023-01-31'
            ... )
            >>> response = source.fetch_data(request)
            >>> if response.success:
            ...     df = response.to_dataframe()
            ...     print(df.head())
        """
        pass
    
    def get_info(self) -> DataSourceInfo:
        """
        获取数据源的完整信息
        
        返回一个包含数据源所有相关信息的对象，包括名称、类型、状态、
        支持的数据类型、速率限制、成本等元数据。
        
        Returns:
            DataSourceInfo: 包含数据源详细信息的对象，包含以下字段：
                - name (str): 数据源名称
                - type (DataSourceType): 数据源类型
                - status (DataSourceStatus): 当前状态
                - supported_data_types (List[DataType]): 支持的数据类型列表
                - rate_limit (Optional[int]): 速率限制(次/分钟)
                - cost_per_request (Optional[float]): 每次请求成本
                - priority (int): 优先级(1-10)
                - description (Optional[str]): 描述信息
                - version (str): 版本号
                - last_updated (datetime): 最后更新时间
                
        Note:
            默认实现从实例属性和配置中获取信息。
            子类可以重写此方法以提供更详细或动态的信息。
            
        Example:
            >>> info = source.get_info()
            >>> print(f"数据源: {info.name}")
            >>> print(f"状态: {info.status}")
            >>> print(f"支持的数据类型: {info.supported_data_types}")
        """
        return DataSourceInfo(
            name=self.name,
            type=self.source_type,
            status=self.status,
            supported_data_types=self.get_supported_data_types(),
            rate_limit=self.config.get('rate_limit'),
            cost_per_request=self.config.get('cost_per_request'),
            priority=self.config.get('priority', 1),
            description=self.config.get('description'),
            version=self.config.get('version', '1.0.0')
        )
    
    def validate_request(self, request: DataRequest) -> Tuple[bool, Optional[str]]:
        """
        验证数据请求参数的有效性
        
        执行一系列验证检查以确保请求参数有效且可以被处理。
        包括检查数据源状态、支持的数据类型、必要参数是否存在以及参数格式等。
        
        Args:
            request (DataRequest): 要验证的数据请求对象
            
        Returns:
            Tuple[bool, Optional[str]]: 
                - 第一个元素表示验证是否通过(True/False)
                - 第二个元素是错误信息，验证通过时为None
                
        Note:
            子类可以重写此方法以添加特定于数据源的自定义验证逻辑。
            在重写时，建议先调用父类方法进行基本验证。
            
        Example:
            >>> request = DataRequest(data_type=DataType.STOCK_DAILY, symbol='AAPL')
            >>> is_valid, error = source.validate_request(request)
            >>> if not is_valid:
            ...     print(f"请求无效: {error}")
        """
        # 检查数据源是否已初始化
        if not self._initialized and not self._closed:
            return False, "数据源未初始化，请先调用initialize()方法"
            
        # 检查数据源是否已关闭
        if self._closed:
            return False, "数据源已关闭，无法处理请求"
        
        # 检查数据类型是否支持
        supported_types = self.get_supported_data_types()
        if request.data_type not in supported_types:
            msg = f"数据源{self.name}不支持数据类型: {request.data_type}，支持的类型: {supported_types}"
            logger.warning(msg)
            return False, msg
        
        # 检查必要参数
        if request.data_type in [DataType.DAILY_QUOTES, DataType.MINUTE_QUOTES, DataType.TICK_DATA]:
            if not request.symbol and not request.symbols:
                msg = "市场数据请求必须提供symbol或symbols参数"
                logger.warning(msg)
                return False, msg
        
        # 检查日期范围
        if request.start_date and request.end_date:
            try:
                start = pd.to_datetime(request.start_date)
                end = pd.to_datetime(request.end_date)
                if start > end:
                    msg = f"开始日期({start})不能晚于结束日期({end})"
                    logger.warning(msg)
                    return False, msg
            except (ValueError, TypeError) as e:
                msg = f"日期格式无效: {str(e)}"
                logger.warning(msg)
                return False, msg
        
        return True, None
    
    def _check_rate_limit(self) -> Tuple[bool, Optional[float]]:
        """
        检查当前请求是否超过频率限制
        
        根据配置的rate_limit参数检查是否允许发起新请求。
        如果请求被限流，返回需要等待的秒数。
        
        Returns:
            Tuple[bool, Optional[float]]: 
                - 第一个元素表示是否允许请求(True表示允许)
                - 第二个元素表示需要等待的秒数(如果被限流)，否则为0.0
                
        Note:
            此方法内部使用线程锁保证线程安全。
            如果rate_limit未设置或小于等于0，表示没有限制。
            
        Example:
            >>> allowed, wait_time = source._check_rate_limit()
            >>> if not allowed:
            ...     print(f"需要等待 {wait_time:.1f} 秒后再试")
            ...     time.sleep(wait_time)
        """
        rate_limit = self.config.get('rate_limit')
        if not rate_limit or rate_limit <= 0:
            return True, 0.0  # 没有速率限制
        
        now = datetime.utcnow()
        with self._lock:  # 加锁保证线程安全
            if self._last_request_time:
                time_diff = (now - self._last_request_time).total_seconds()
                
                # 如果距离上次请求超过1分钟，重置计数器
                if time_diff >= 60:
                    self._request_count = 0
                # 检查是否超过频率限制
                elif self._request_count >= rate_limit:
                    wait_time = 60 - time_diff
                    logger.warning(
                        f"数据源 {self.name} 请求频率超过限制. "
                        f"请等待 {wait_time:.2f} 秒..."
                    )
                    return False, wait_time
            
            # 更新请求统计
            self._request_count += 1
            self._last_request_time = now
            
            return True, 0.0
    
    def _update_request_stats(self) -> None:
        """
        更新请求统计信息
        
        在每次成功发起请求后调用此方法，更新最后请求时间和请求计数器。
        此方法使用线程锁保证线程安全。
        
        Note:
            这是一个内部方法，通常不需要在子类中直接调用。
            它会在fetch_data方法中自动调用。
            
        Example:
            >>> # 在子类中通常不需要直接调用此方法
            >>> # 它会在BaseDataSource.fetch_data中自动调用
            >>> response = self._make_http_request(url)
            >>> self._update_request_stats()  # 更新统计信息
            >>> return response
        """
        with self._lock:  # 加锁保证线程安全
            self._last_request_time = datetime.utcnow()
            self._request_count += 1
    
    def close(self) -> None:
        """
        关闭数据源连接并释放资源
        
        执行必要的清理操作，如关闭网络连接、释放文件句柄等。
        此方法可以被多次调用，但只有第一次调用会执行实际关闭操作。
        
        Note:
            子类应该重写此方法以执行特定的清理操作，
            但需要确保调用父类的close()方法。
            
        Example:
            >>> class MyDataSource(BaseDataSource):
            ...     def close(self):
            ...         # 执行特定于子类的清理操作
            ...         self._close_connection()
            ...         # 调用父类的close方法
            ...         super().close()
            ...     
            ...     def _close_connection(self):
            ...         # 关闭网络连接等资源
            ...         pass
        """
        if not self._closed:
            logger.info(f"正在关闭数据源: {self.name}")
            try:
                # 更新状态
                self.status = DataSourceStatus.CLOSED
                self._closed = True
                logger.debug(f"数据源 {self.name} 已成功关闭")
            except Exception as e:
                logger.error(f"关闭数据源 {self.name} 时出错: {str(e)}", exc_info=True)
                raise
    
    def is_available(self) -> bool:
        """
        检查数据源是否可用
        
        判断数据源是否已初始化且未关闭，这是执行数据请求的前提条件。
        
        Returns:
            bool: 
                - True: 数据源已初始化且未关闭，可以接受请求
                - False: 数据源未初始化或已关闭，不能处理请求
                
        Example:
            >>> if source.is_available():
            ...     response = source.fetch_data(request)
            ... else:
            ...     print("数据源不可用")
        """
        return self._initialized and not self._closed
    
    def get_status(self) -> DataSourceStatus:
        """
        获取数据源的当前状态
        
        返回数据源的当前状态枚举值，如AVAILABLE、UNAVAILABLE等。
        这是对status属性的一个封装，提供更好的方法调用接口。
        
        Returns:
            DataSourceStatus: 数据源的当前状态枚举值
            
        Example:
            >>> status = source.get_status()
            >>> if status == DataSourceStatus.AVAILABLE:
            ...     print("数据源可用")
            >>> # 或者使用is_available()方法检查
            >>> if source.is_available():
            ...     print("数据源可用")
        """
        return self.status
    
    def __str__(self) -> str:
        """
        返回数据源的字符串表示
        
        提供数据源的基本信息，包括类名、名称、类型和状态。
        适合用于日志记录和用户显示。
        
        Returns:
            str: 格式为 "ClassName(name='name', type=TYPE, status=STATUS)" 的字符串
            
        Example:
            >>> print(str(source))
            MyDataSource(name='yahoo', type=DataSourceType.MARKET, status=DataSourceStatus.AVAILABLE)
        """
        return f"{self.__class__.__name__}(name='{self.name}', type={self.source_type}, status={self.status})"
    
    def __repr__(self) -> str:
        """
        返回数据源的官方字符串表示
        
        提供数据源的完整内部状态信息，主要用于调试目的。
        与__str__不同，这里会显示更多内部状态。
        
        Returns:
            str: 包含数据源所有重要属性的字符串表示
            
        Note:
            这个方法的输出可能包含敏感信息，不建议在生产日志中使用。
            主要用于调试和开发阶段。
            
        Example:
            >>> repr(source)
            "MyDataSource(name='yahoo', source_type=DataSourceType.MARKET, status=DataSourceStatus.AVAILABLE, \
            initialized=True, closed=False)"
        """
        return (
            f"{self.__class__.__name__}("
            f"name='{self.name}', "
            f"source_type={self.source_type}, "
            f"status={self.status}, "
            f"initialized={self._initialized}, "
            f"closed={self._closed}"
            f")"
        )


class DataSourceError(Exception):
    """
    数据源基础异常类
    
    所有数据源相关异常的基类。
    
    Attributes:
        message: 错误信息
        source: 数据源名称
        error_code: 可选的错误代码
        details: 包含额外错误详情的字典
    """
    
    def __init__(
        self, 
        message: str, 
        source: str, 
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        初始化异常
        
        Args:
            message: 错误信息
            source: 数据源名称
            error_code: 可选的错误代码
            details: 包含额外错误详情的字典
        """
        self.message = message
        self.source = source
        self.error_code = error_code
        self.details = details or {}
        super().__init__(f"[{source}] {message}")
    
    def __str__(self) -> str:
        """返回异常的字符串表示"""
        parts = [f"[{self.source}] {self.message}"]
        if self.error_code:
            parts.append(f"(code: {self.error_code})")
        if self.details:
            parts.append(f"\nDetails: {self.details}")
        return " ".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        将异常转换为字典
        
        Returns:
            Dict[str, Any]: 包含异常信息的字典
        """
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "source": self.source,
            "error_code": self.error_code,
            "details": self.details
        }


class RateLimitError(DataSourceError):
    """
    请求频率限制异常
    
    当数据源的请求频率超过限制时抛出。
    
    Attributes:
        retry_after: 建议重试等待时间(秒)
        rate_limit: 允许的请求频率(次/分钟)
    """
    
    def __init__(
        self, 
        source: str, 
        retry_after: Optional[float] = None,
        rate_limit: Optional[int] = None,
        **kwargs
    ) -> None:
        """
        初始化异常
        
        Args:
            source: 数据源名称
            retry_after: 建议重试等待时间(秒)
            rate_limit: 允许的请求频率(次/分钟)
            **kwargs: 其他参数，将传递给父类
        """
        message = "Rate limit exceeded"
        if rate_limit:
            message += f" (limit: {rate_limit} requests/minute)"
        if retry_after:
            message += f", retry after {retry_after:.1f} seconds"
            
        details = {"retry_after": retry_after, "rate_limit": rate_limit}
        details.update(kwargs.get("details", {}))
        
        super().__init__(
            message=message,
            source=source,
            error_code="RATE_LIMIT_EXCEEDED",
            details=details
        )
        
        self.retry_after = retry_after
        self.rate_limit = rate_limit


class DataNotAvailableError(DataSourceError):
    """
    数据不可用异常
    
    当请求的数据不存在或不可用时抛出。
    
    Attributes:
        data_type: 请求的数据类型
        identifier: 数据标识符(如股票代码)
        date_range: 请求的日期范围
    """
    
    def __init__(
        self, 
        source: str, 
        data_type: Optional[Union[str, DataType]] = None,
        identifier: Optional[str] = None,
        date_range: Optional[Tuple[Optional[Union[str, date, datetime]], 
                                 Optional[Union[str, date, datetime]]]] = None,
        **kwargs
    ) -> None:
        """
        初始化异常
        
        Args:
            source: 数据源名称
            data_type: 请求的数据类型
            identifier: 数据标识符(如股票代码)
            date_range: 请求的日期范围(start_date, end_date)
            **kwargs: 其他参数，将传递给父类
        """
        message_parts = ["Requested data is not available"]
        details = {}
        
        if data_type:
            data_type_str = data_type.value if isinstance(data_type, DataType) else data_type
            message_parts.append(f"for data type: {data_type_str}")
            details["data_type"] = data_type_str
            
        if identifier:
            message_parts.append(f"(identifier: {identifier})")
            details["identifier"] = identifier
            
        if date_range and any(date_range):
            start, end = date_range
            date_range_str = f"{start} to {end}" if start and end else str(start or end)
            message_parts.append(f"in range: {date_range_str}")
            details["date_range"] = {"start": start, "end": end}
        
        details.update(kwargs.get("details", {}))
        
        super().__init__(
            message=" ".join(message_parts).strip(),
            source=source,
            error_code="DATA_NOT_AVAILABLE",
            details=details
        )
        
        self.data_type = data_type
        self.identifier = identifier
        self.date_range = date_range


class AuthenticationError(DataSourceError):
    """
    认证失败异常
    
    当数据源认证失败时抛出。
    
    Attributes:
        auth_method: 使用的认证方法
        reason: 认证失败的具体原因
    """
    
    def __init__(
        self, 
        source: str, 
        reason: Optional[str] = None,
        auth_method: Optional[str] = None,
        **kwargs
    ) -> None:
        """
        初始化异常
        
        Args:
            source: 数据源名称
            reason: 认证失败的具体原因
            auth_method: 使用的认证方法
            **kwargs: 其他参数，将传递给父类
        """
        message = "Authentication failed"
        if auth_method:
            message += f" (method: {auth_method})"
        if reason:
            message += f": {reason}"
            
        details = {
            "auth_method": auth_method,
            "reason": reason
        }
        details.update(kwargs.get("details", {}))
        
        super().__init__(
            message=message,
            source=source,
            error_code="AUTHENTICATION_FAILED",
            details=details
        )
        
        self.auth_method = auth_method
        self.reason = reason


class NetworkError(DataSourceError):
    """
    网络连接异常
    
    当与数据源的网络通信失败时抛出。
    
    Attributes:
        url: 请求的URL(如果适用)
        status_code: HTTP状态码(如果适用)
    """
    
    def __init__(
        self, 
        source: str, 
        message: Optional[str] = None,
        url: Optional[str] = None,
        status_code: Optional[int] = None,
        **kwargs
    ) -> None:
        """
        初始化异常
        
        Args:
            source: 数据源名称
            message: 错误信息
            url: 请求的URL
            status_code: HTTP状态码
            **kwargs: 其他参数，将传递给父类
        """
        if not message:
            message = "Network error occurred"
            if status_code:
                message += f" (status code: {status_code})"
            if url:
                message += f" while accessing {url}"
                
        details = {
            "url": url,
            "status_code": status_code
        }
        details.update(kwargs.get("details", {}))
        
        super().__init__(
            message=message,
            source=source,
            error_code="NETWORK_ERROR",
            details=details
        )
        
        self.url = url
        self.status_code = status_code
        
    @classmethod
    def from_exception(
        cls, 
        source: str, 
        exc: Exception, 
        url: Optional[str] = None,
        **kwargs
    ) -> 'NetworkError':
        """
        从其他异常创建NetworkError
        
        Args:
            source: 数据源名称
            exc: 原始异常
            url: 请求的URL
            **kwargs: 其他参数，将传递给构造函数
            
        Returns:
            NetworkError: 新创建的异常实例
        """
        status_code = getattr(exc, 'status_code', None) or getattr(exc, 'code', None)
        
        return cls(
            source=source,
            message=str(exc),
            url=url,
            status_code=status_code,
            details={
                "exception_type": exc.__class__.__name__,
                "exception_args": getattr(exc, 'args', []),
                **kwargs.pop('details', {})
            },
            **kwargs
        )
