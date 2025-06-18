"""
数据源枚举模块

该模块包含数据源系统中使用的各种枚举类型，包括数据源类型、数据类型和状态等。
"""
from enum import Enum
from typing import Set, Union, List, Optional, Dict, Any, Type, TypeVar, Tuple, cast, Callable, ClassVar

class DataSourceStatus(str, Enum):
    """
    数据源状态枚举
    
    这个枚举用于跟踪数据源的当前状态，对于监控和错误处理非常有用。
    
    属性:
        UNINITIALIZED: 未初始化
        INITIALIZING: 正在初始化
        READY: 已准备好
        BUSY: 正在处理请求
        ERROR: 发生错误
        OFFLINE: 离线
        MAINTENANCE: 维护中
        DEPRECATED: 已废弃
    """
    UNINITIALIZED = "uninitialized"  # 未初始化
    INITIALIZING = "initializing"    # 正在初始化
    READY = "ready"                  # 已准备好
    BUSY = "busy"                    # 正在处理请求
    ERROR = "error"                  # 发生错误
    OFFLINE = "offline"              # 离线
    MAINTENANCE = "maintenance"      # 维护中
    DEPRECATED = "deprecated"        # 已废弃
    
    @classmethod
    def is_operational(cls, status: Union['DataSourceStatus', str]) -> bool:
        """检查数据源是否处于可操作状态"""
        if isinstance(status, str):
            status = DataSourceStatus(status)
        return status in {cls.READY, cls.BUSY}
        
    @classmethod
    def is_error(cls, status: Union['DataSourceStatus', str]) -> bool:
        """检查数据源是否处于错误状态"""
        if isinstance(status, str):
            status = DataSourceStatus(status)
        return status == cls.ERROR
        
    @classmethod
    def is_available(cls, status: Union['DataSourceStatus', str]) -> bool:
        """检查数据源是否可用"""
        if isinstance(status, str):
            status = DataSourceStatus(status)
        return status in {cls.READY, cls.BUSY, cls.INITIALIZING}
        
    @classmethod
    def is_deprecated(cls, status: Union['DataSourceStatus', str]) -> bool:
        """检查数据源是否已废弃"""
        if isinstance(status, str):
            status = DataSourceStatus(status)
        return status == cls.DEPRECATED
        
    @classmethod
    def from_string(cls, value: str) -> 'DataSourceStatus':
        """从字符串创建DataSourceStatus枚举值"""
        try:
            return cls(value.lower())
        except ValueError as e:
            raise ValueError(f"无效的数据源状态: {value}") from e


class DataSourceType(str, Enum):
    """
    数据源类型枚举
    
    这个枚举定义了系统中支持的所有数据源类型。
    每个枚举值代表一个特定的数据源提供者，并有一个字符串表示用于序列化。
    
    属性:
        AKSHARE: AkShare数据源 (免费, 开源, 全面的中国市场数据)
        TUSHARE: Tushare数据源 (免费和付费版本, 中国市场数据)
        YAHOO: Yahoo Finance数据源 (免费, 全球市场数据)
        BAOSTOCK: BaoStock数据源 (免费, 中国A股数据)
        JQDATA: JoinQuant数据源 (付费, 中国市场数据)
        RQDATA: RiceQuant数据源 (付费, 中国市场数据)
        TUSHAREPRO: Tushare Pro数据源 (付费, 专业版)
        CUSTOM: 自定义数据源
    """
    AKSHARE = "akshare"  # AkShare数据源 - 开源金融数据接口库
    TUSHARE = "tushare"  # Tushare数据源 - 金融大数据社区
    YAHOO = "yahoo"  # Yahoo Finance数据源 - 雅虎财经
    BAOSTOCK = "baostock"  # 宝硕数据 - 免费开源证券数据平台
    JQDATA = "jqdata"  # 聚宽数据 - 量化交易数据服务
    RQDATA = "rqdata"  # 米筐数据 - 量化金融数据服务
    TUSHAREPRO = "tusharepro"  # Tushare Pro数据源 - Tushare专业版
    CUSTOM = "custom"  # 自定义数据源 - 用户自定义数据源
    
    # 按特性分组的数据源
    _FREE_SOURCES = {AKSHARE, TUSHARE, YAHOO, BAOSTOCK}
    _PAID_SOURCES = {JQDATA, RQDATA, TUSHAREPRO}
    _CHINA_MARKET_SOURCES = {AKSHARE, TUSHARE, BAOSTOCK, JQDATA, RQDATA, TUSHAREPRO}
    _GLOBAL_MARKET_SOURCES = {YAHOO}
    
    @classmethod
    def is_free_source(cls, source_type: Union['DataSourceType', str]) -> bool:
        """检查数据源是否免费"""
        if isinstance(source_type, str):
            source_type = DataSourceType(source_type)
        return source_type in cls._FREE_SOURCES
    
    @classmethod
    def is_paid_source(cls, source_type: Union['DataSourceType', str]) -> bool:
        """检查数据源是否需要付费"""
        if isinstance(source_type, str):
            source_type = DataSourceType(source_type)
        return source_type in cls._PAID_SOURCES
    
    @classmethod
    def is_china_market_source(cls, source_type: Union['DataSourceType', str]) -> bool:
        """检查数据源是否提供中国市场数据"""
        if isinstance(source_type, str):
            source_type = DataSourceType(source_type)
        return source_type in cls._CHINA_MARKET_SOURCES
    
    @classmethod
    def is_global_market_source(cls, source_type: Union['DataSourceType', str]) -> bool:
        """检查数据源是否提供全球市场数据"""
        if isinstance(source_type, str):
            source_type = DataSourceType(source_type)
        return source_type in cls._GLOBAL_MARKET_SOURCES
    
    @classmethod
    def from_string(cls, value: str) -> 'DataSourceType':
        """从字符串创建DataSourceType枚举值"""
        try:
            return cls(value.lower())
        except ValueError as e:
            raise ValueError(f"无效的数据源类型: {value}") from e


class DataType(str, Enum):
    """
    数据类型枚举
    
    这个枚举定义了数据源系统中支持的所有数据类型。
    每个枚举值代表一个特定的数据类别，并有一个字符串表示用于序列化。
    
    枚举按逻辑分组，使用清晰的前缀：
    - stock_*: 股票相关数据 (价格、基本面等)
    - index_*: 指数相关数据
    - tick_*: 高频tick数据
    - *_daily, *_minute: 特定时间框架的数据
    """
    # 股票数据类型
    STOCK_DAILY = "stock_daily"  # 股票日线数据
    STOCK_MINUTE = "stock_minute"  # 股票分钟线数据
    STOCK_TICK = "stock_tick"  # 股票tick数据
    STOCK_ADJ_FACTOR = "stock_adj_factor"  # 复权因子
    STOCK_BASIC = "stock_basic"  # 股票基本信息
    STOCK_COMPANY = "stock_company"  # 公司信息
    STOCK_MANAGER = "stock_manager"  # 管理层信息
    STOCK_REWARD = "stock_reward"  # 分红送股
    STOCK_FINANCIAL = "stock_financial"  # 财务数据
    STOCK_INDUSTRY = "stock_industry"  # 行业分类
    STOCK_CONCEPT = "stock_concept"  # 概念板块
    STOCK_HOLDER = "stock_holder"  # 股东信息
    STOCK_SHARE = "stock_share"  # 股本结构
    STOCK_MONEYFLOW = "stock_moneyflow"  # 资金流向
    STOCK_MARGIN = "stock_margin"  # 融资融券
    STOCK_SHORT = "stock_short"  # 融券做空
    STOCK_BLOCKTRADE = "stock_blocktrade"  # 大宗交易
    STOCK_REPO = "stock_repo"  # 股票回购
    STOCK_FUNDAMENTAL = "stock_fundamental"  # 基本面数据
    STOCK_NEWS = "stock_news"  # 新闻数据
    STOCK_ANNOUNCEMENT = "stock_announcement"  # 公司公告
    STOCK_REPORT = "stock_report"  # 研究报告
    STOCK_FORECAST = "stock_forecast"  # 业绩预告
    STOCK_EXPECTATION = "stock_expectation"  # 市场预期
    STOCK_ESTIMATE = "stock_estimate"  # 分析师预测
    
    # 指数数据类型
    INDEX_DAILY = "index_daily"  # 指数日线数据
    INDEX_COMPONENT = "index_component"  # 指数成分股
    STOCK_INDEX = "stock_index"  # 股票指数
    STOCK_INDEX_WEIGHT = "stock_index_weight"  # 指数权重
    STOCK_INDEX_DETAIL = "stock_index_detail"  # 指数详情
    STOCK_INDEX_COMPARE = "stock_index_compare"  # 指数对比
    STOCK_INDEX_CONS = "stock_index_cons"  # 指数成分股
    STOCK_INDEX_DAILY = "stock_index_daily"  # 指数日线
    STOCK_INDEX_WEEKLY = "stock_index_weekly"  # 指数周线
    STOCK_INDEX_MONTHLY = "stock_index_monthly"  # 指数月线
    STOCK_INDEX_MIN = "stock_index_min"  # 指数分钟线
    STOCK_INDEX_TICK = "stock_index_tick"  # 指数tick
    
    # 自定义数据类型
    CUSTOM_DATA = "custom_data"  # 自定义数据
    
    # 按类别分组的数据类型
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
        STOCK_INDEX_DAILY, STOCK_INDEX_WEEKLY, STOCK_INDEX_MONTHLY, STOCK_INDEX_MIN,
        STOCK_INDEX_TICK
    }
    
    _TICK_DATA_TYPES = {
        STOCK_TICK, STOCK_INDEX_TICK
    }
    
    @classmethod
    def is_stock_data_type(cls, data_type: Union['DataType', str]) -> bool:
        """检查数据类型是否与股票相关"""
        if isinstance(data_type, str):
            data_type = DataType(data_type)
        return data_type in cls._STOCK_DATA_TYPES
    
    @classmethod
    def is_index_data_type(cls, data_type: Union['DataType', str]) -> bool:
        """检查数据类型是否与指数相关"""
        if isinstance(data_type, str):
            data_type = DataType(data_type)
        return data_type in cls._INDEX_DATA_TYPES
    
    @classmethod
    def is_tick_data_type(cls, data_type: Union['DataType', str]) -> bool:
        """检查数据类型是否为tick数据"""
        if isinstance(data_type, str):
            data_type = DataType(data_type)
        return data_type in cls._TICK_DATA_TYPES
    
    @classmethod
    def get_data_type_category(cls, data_type: Union['DataType', str]) -> str:
        """
        获取数据类型的类别
        
        返回: 'stock'(股票), 'index'(指数), 'tick'(tick数据) 或 'other'(其他)
        """
        if isinstance(data_type, str):
            data_type = DataType(data_type)
            
        if data_type in cls._STOCK_DATA_TYPES:
            return 'stock'
        elif data_type in cls._INDEX_DATA_TYPES:
            return 'index'
        elif data_type in cls._TICK_DATA_TYPES:
            return 'tick'
        else:
            return 'other'
    
    @classmethod
    def get_all_data_types(cls) -> List['DataType']:
        """获取所有可用的数据类型"""
        return list(DataType)
    
    @classmethod
    def from_string(cls, value: str) -> 'DataType':
        """从字符串创建DataType枚举值"""
        try:
            return cls(value.lower())
        except ValueError as e:
            raise ValueError(f"无效的数据类型: {value}") from e
