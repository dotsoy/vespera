"""
数据请求模型模块

该模块定义了数据请求相关的数据模型，用于表示对数据源的查询请求。
"""
from __future__ import annotations

from datetime import datetime, date, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union, Literal, TypeVar, Generic, Type, cast

from pydantic import BaseModel, Field, validator, root_validator

from ..enums import DataType, DataSourceType

# 类型别名
TimestampType = Union[str, datetime, date, int, float]
SymbolType = Union[str, Tuple[str, str]]  # (symbol, exchange) 或 symbol

class DataRequest(BaseModel):
    """
    数据请求类
    
    表示对数据源的数据查询请求，包含查询所需的所有参数。
    
    属性:
        data_type: 请求的数据类型
        symbols: 股票/指数代码列表
        start_time: 开始时间
        end_time: 结束时间
        source_type: 数据源类型
        params: 其他查询参数
        request_id: 请求ID
        priority: 请求优先级 (1-10, 1为最高)
        timeout: 超时时间(秒)
        cache: 是否使用缓存
        fields: 请求的字段列表
        limit: 返回结果数量限制
    """
    data_type: DataType = Field(..., description="请求的数据类型")
    symbols: List[SymbolType] = Field(default_factory=list, description="股票/指数代码列表")
    start_time: Optional[TimestampType] = Field(None, description="开始时间")
    end_time: Optional[TimestampType] = Field(None, description="结束时间")
    source_type: Optional[Union[DataSourceType, str]] = Field(None, description="数据源类型")
    params: Dict[str, Any] = Field(default_factory=dict, description="其他查询参数")
    request_id: Optional[str] = Field(None, description="请求ID")
    priority: int = Field(5, ge=1, le=10, description="请求优先级 (1-10, 1为最高)")
    timeout: float = Field(30.0, gt=0, description="超时时间(秒)")
    cache: bool = Field(True, description="是否使用缓存")
    fields: Optional[List[str]] = Field(None, description="请求的字段列表")
    limit: Optional[int] = Field(None, ge=1, description="返回结果数量限制")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
        }
    
    @validator('symbols', pre=True, each_item=True)
    def validate_symbols(cls, v):
        """验证股票/指数代码"""
        if isinstance(v, (list, tuple)):
            if len(v) != 2 or not all(isinstance(x, str) for x in v):
                raise ValueError("Symbol tuple must be (symbol: str, exchange: str)")
            return tuple(v)
        return str(v)
    
    @validator('start_time', 'end_time', pre=True)
    def validate_timestamp(cls, v):
        """验证时间戳"""
        if v is None:
            return None
            
        if isinstance(v, (int, float)):
            if v > 1e12:  # 毫秒时间戳
                v = v / 1000.0
            return datetime.fromtimestamp(v)
            
        if isinstance(v, str):
            try:
                # 尝试解析ISO格式时间字符串
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                raise ValueError(f"Invalid datetime string: {v}")
                
        if isinstance(v, (datetime, date)):
            return v
            
        raise ValueError(f"Unsupported timestamp type: {type(v)}")
    
    @root_validator(skip_on_failure=True)
    def validate_time_range(cls, values):
        """验证时间范围"""
        start_time = values.get('start_time')
        end_time = values.get('end_time')
        
        if start_time and end_time and start_time > end_time:
            raise ValueError("start_time must be before end_time")
            
        return values
    
    @property
    def symbol(self) -> Optional[SymbolType]:
        """获取单个symbol（如果有且仅有一个）"""
        if len(self.symbols) == 1:
            return self.symbols[0]
        return None
    
    @property
    def time_range(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        """获取时间范围"""
        return (self.start_time, self.end_time)
    
    def with_symbols(self, symbols: List[SymbolType]) -> 'DataRequest':
        """返回一个带有新symbols的请求副本"""
        return self.copy(update={'symbols': symbols})
    
    def with_time_range(self, start_time: Optional[TimestampType] = None, 
                       end_time: Optional[TimestampType] = None) -> 'DataRequest':
        """返回一个带有新时间范围的请求副本"""
        updates = {}
        if start_time is not None:
            updates['start_time'] = start_time
        if end_time is not None:
            updates['end_time'] = end_time
        return self.copy(update=updates)
    
    def with_params(self, **params) -> 'DataRequest':
        """返回一个带有额外参数的请求副本"""
        new_params = self.params.copy()
        new_params.update(params)
        return self.copy(update={'params': new_params})
    
    def to_dict(self) -> Dict[str, Any]:
        """将请求转换为字典"""
        return self.dict(exclude_unset=True)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataRequest':
        """从字典创建请求"""
        return cls(**data)
