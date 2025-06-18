"""
数据兼容层模块
用于隔离数据源与表结构，统一处理字段映射和转换
"""
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional, List
from loguru import logger
from .enums import DataType


class DataCompatibilityLayer:
    """数据兼容层，处理数据源与表结构之间的转换"""
    
    # 定义表结构字段映射
    TABLE_SCHEMAS = {
        'daily_quotes': {
            'trade_date': 'Date',
            'symbol': 'String', 
            'open_price': 'Float64',
            'close_price': 'Float64',
            'high_price': 'Float64',
            'low_price': 'Float64',
            'vol': 'Int64',
            'amount': 'Float64',
            'amplitude': 'Float64',
            'pct_chg': 'Float64',
            'change_amount': 'Float64',
            'turnover_rate': 'Float64',
            'ts_code': 'String',
            'pre_close': 'Float64'
        },
        'stock_basic': {
            'ts_code': 'String',
            'name': 'String',
            'industry': 'String',
            'area': 'String',
            'market': 'String',
            'list_date': 'String'
        }
    }
    
    # 定义数据源字段映射
    FIELD_MAPPINGS = {
        DataType.STOCK_DAILY: {
            # AllTick 数据源字段映射
            'alltick': {
                'trade_date': 'trade_date',
                'symbol': 'symbol',
                'open_price': 'open',
                'close_price': 'close',
                'high_price': 'high',
                'low_price': 'low',
                'vol': 'vol',
                'amount': 'amount',
                'amplitude': 'amplitude',
                'pct_chg': 'pct_chg',
                'change_amount': 'change',
                'turnover_rate': 'turnover_rate',
                'ts_code': 'ts_code',
                'pre_close': 'pre_close'
            },
            # Tushare 数据源字段映射
            'tushare': {
                'trade_date': 'trade_date',
                'symbol': 'ts_code',
                'open_price': 'open',
                'close_price': 'close',
                'high_price': 'high',
                'low_price': 'low',
                'vol': 'vol',
                'amount': 'amount',
                'amplitude': 'amplitude',
                'pct_chg': 'pct_chg',
                'change_amount': 'change',
                'turnover_rate': 'turnover_rate',
                'ts_code': 'ts_code',
                'pre_close': 'pre_close'
            },
            # AkShare 数据源字段映射
            'akshare': {
                'trade_date': 'date',
                'symbol': 'symbol',
                'open_price': 'open',
                'close_price': 'close',
                'high_price': 'high',
                'low_price': 'low',
                'vol': 'volume',
                'amount': 'amount',
                'amplitude': 'amplitude',
                'pct_chg': 'pct_chg',
                'change_amount': 'change',
                'turnover_rate': 'turnover_rate',
                'ts_code': 'ts_code',
                'pre_close': 'pre_close'
            }
        },
        DataType.STOCK_BASIC: {
            'alltick': {
                'ts_code': 'ts_code',
                'name': 'name',
                'industry': 'industry',
                'area': 'area',
                'market': 'market',
                'list_date': 'list_date'
            },
            'tushare': {
                'ts_code': 'ts_code',
                'name': 'name',
                'industry': 'industry',
                'area': 'area',
                'market': 'market',
                'list_date': 'list_date'
            },
            'akshare': {
                'ts_code': 'ts_code',
                'name': 'name',
                'industry': 'industry',
                'area': 'area',
                'market': 'market',
                'list_date': 'list_date'
            }
        }
    }
    
    @classmethod
    def get_table_schema(cls, table_name: str) -> Dict[str, str]:
        """获取表结构定义"""
        return cls.TABLE_SCHEMAS.get(table_name, {})
    
    @classmethod
    def get_field_mapping(cls, data_type: DataType, data_source: str) -> Dict[str, str]:
        """获取字段映射"""
        return cls.FIELD_MAPPINGS.get(data_type, {}).get(data_source, {})
    
    @classmethod
    def transform_data(cls, df: pd.DataFrame, data_type: DataType, data_source: str) -> pd.DataFrame:
        """转换数据格式，统一字段名和数据类型"""
        try:
            # 获取字段映射
            field_mapping = cls.get_field_mapping(data_type, data_source)
            if not field_mapping:
                logger.warning(f"未找到数据源 {data_source} 的字段映射")
                return df
            
            # 创建新的DataFrame，只包含目标字段
            transformed_data = {}
            table_schema = cls.get_table_schema(cls.get_table_name(data_type))
            
            for target_field, source_field in field_mapping.items():
                if source_field in df.columns:
                    # 根据目标字段类型进行数据转换
                    value = df[source_field]
                    target_type = table_schema.get(target_field, 'String')
                    
                    if target_type == 'Date':
                        # 转换日期格式
                        if pd.api.types.is_datetime64_any_dtype(value):
                            transformed_data[target_field] = value.dt.date
                        else:
                            transformed_data[target_field] = pd.to_datetime(value).dt.date
                    elif target_type == 'Int64':
                        # 转换整数类型
                        transformed_data[target_field] = pd.to_numeric(value, errors='coerce').fillna(0).astype('int64')
                    elif target_type == 'Float64':
                        # 转换浮点数类型
                        transformed_data[target_field] = pd.to_numeric(value, errors='coerce').fillna(0.0)
                    else:
                        # 字符串类型
                        transformed_data[target_field] = value.astype(str)
                else:
                    logger.warning(f"源字段 {source_field} 不存在于数据中")
                    # 设置默认值
                    target_type = table_schema.get(target_field, 'String')
                    if target_type == 'Date':
                        transformed_data[target_field] = pd.Series([datetime.now().date()] * len(df))
                    elif target_type == 'Int64':
                        transformed_data[target_field] = pd.Series([0] * len(df))
                    elif target_type == 'Float64':
                        transformed_data[target_field] = pd.Series([0.0] * len(df))
                    else:
                        transformed_data[target_field] = pd.Series([''] * len(df))
            
            # 创建转换后的DataFrame
            result_df = pd.DataFrame(transformed_data)
            
            # 确保字段顺序与表结构一致
            ordered_columns = list(table_schema.keys())
            result_df = result_df.reindex(columns=ordered_columns)
            
            logger.info(f"✅ 数据转换完成: {len(df)} 条记录，字段数: {len(result_df.columns)}")
            return result_df
            
        except Exception as e:
            logger.error(f"数据转换失败: {e}")
            return df
    
    @classmethod
    def get_table_name(cls, data_type: DataType) -> str:
        """根据数据类型获取表名"""
        if data_type == DataType.STOCK_DAILY:
            return 'daily_quotes'
        elif data_type == DataType.STOCK_BASIC:
            return 'stock_basic'
        else:
            raise ValueError(f"不支持的数据类型: {data_type}")
    
    @classmethod
    def validate_data(cls, df: pd.DataFrame, data_type: DataType) -> bool:
        """验证数据是否符合表结构要求"""
        try:
            table_name = cls.get_table_name(data_type)
            table_schema = cls.get_table_schema(table_name)
            
            # 检查必需字段
            required_fields = set(table_schema.keys())
            actual_fields = set(df.columns)
            
            missing_fields = required_fields - actual_fields
            if missing_fields:
                logger.error(f"缺少必需字段: {missing_fields}")
                return False
            
            # 检查数据类型
            for field, expected_type in table_schema.items():
                if field in df.columns:
                    if expected_type == 'Date':
                        if not pd.api.types.is_datetime64_any_dtype(df[field]):
                            logger.warning(f"字段 {field} 不是日期类型")
                    elif expected_type == 'Int64':
                        if not pd.api.types.is_integer_dtype(df[field]):
                            logger.warning(f"字段 {field} 不是整数类型")
                    elif expected_type == 'Float64':
                        if not pd.api.types.is_float_dtype(df[field]):
                            logger.warning(f"字段 {field} 不是浮点数类型")
            
            logger.info(f"✅ 数据验证通过: {len(df)} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"数据验证失败: {e}")
            return False
    
    @classmethod
    def add_data_source_field_mapping(cls, data_type: DataType, data_source: str, mapping: Dict[str, str]):
        """添加新的数据源字段映射"""
        if data_type not in cls.FIELD_MAPPINGS:
            cls.FIELD_MAPPINGS[data_type] = {}
        cls.FIELD_MAPPINGS[data_type][data_source] = mapping
        logger.info(f"✅ 添加数据源 {data_source} 的字段映射")
    
    @classmethod
    def get_supported_data_sources(cls, data_type: DataType) -> List[str]:
        """获取支持的数据源列表"""
        return list(cls.FIELD_MAPPINGS.get(data_type, {}).keys()) 