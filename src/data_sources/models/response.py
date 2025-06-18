"""
数据响应模型模块

该模块定义了数据响应相关的数据模型，用于表示数据源的查询结果。
"""
from __future__ import annotations

from datetime import datetime, date
from enum import Enum
from typing import Any, Dict, List, Optional, Type, TypeVar, Union, Generic, Tuple, cast

import pandas as pd
from pydantic import BaseModel, Field, validator, root_validator

from ..enums import DataType, DataSourceType

# 类型别名
DataFrameOrDict = Union[pd.DataFrame, Dict[str, Any]]
TimestampType = Union[str, datetime, date, int, float]
SymbolType = Union[str, Tuple[str, str]]

class DataResponse(BaseModel):
    """
    数据响应类
    
    表示数据源的查询结果，包含数据、元数据和状态信息。
    
    属性:
        data: 响应数据 (DataFrame 或字典)
        request: 原始请求
        success: 请求是否成功
        message: 状态消息
        error: 错误信息
        data_type: 数据类型
        metadata: 元数据
        timestamp: 响应时间戳
    """
    data: Optional[DataFrameOrDict] = Field(None, description="响应数据 (DataFrame 或字典)")
    request: Optional[Dict[str, Any]] = Field(None, description="原始请求")
    success: bool = Field(True, description="请求是否成功")
    message: str = Field("", description="状态消息")
    error: Optional[str] = Field(None, description="错误信息")
    data_type: Optional[Union[DataType, str]] = Field(None, description="数据类型")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="元数据")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="响应时间戳")
    
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            pd.DataFrame: lambda v: v.to_dict(orient='records'),
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
        }
    
    @validator('data_type', pre=True)
    def validate_data_type(cls, v):
        """验证数据类型"""
        if v is None or isinstance(v, DataType):
            return v
        return DataType(v)
    
    @classmethod
    def success(
        cls,
        data: Optional[DataFrameOrDict] = None,
        data_type: Optional[Union[DataType, str]] = None,
        request: Optional[Dict[str, Any]] = None,
        message: str = "",
        **metadata
    ) -> 'DataResponse':
        """
        创建成功响应
        
        Args:
            data: 响应数据
            data_type: 数据类型
            request: 原始请求
            message: 成功消息
            **metadata: 额外元数据
            
        Returns:
            DataResponse: 成功响应对象
        """
        return cls(
            data=data,
            data_type=data_type,
            request=request,
            success=True,
            message=message or "Request completed successfully",
            metadata=metadata
        )
    
    @classmethod
    def error(
        cls,
        error: Union[str, Exception],
        data_type: Optional[Union[DataType, str]] = None,
        request: Optional[Dict[str, Any]] = None,
        **metadata
    ) -> 'DataResponse':
        """
        创建错误响应
        
        Args:
            error: 错误信息或异常对象
            data_type: 数据类型
            request: 原始请求
            **metadata: 额外元数据
            
        Returns:
            DataResponse: 错误响应对象
        """
        error_msg = str(error) if not isinstance(error, str) else error
        return cls(
            data=None,
            data_type=data_type,
            request=request,
            success=False,
            message=error_msg,
            error=error_msg,
            metadata=metadata
        )
    
    @property
    def is_empty(self) -> bool:
        """检查响应是否为空"""
        if self.data is None:
            return True
        if isinstance(self.data, dict):
            return len(self.data) == 0
        if isinstance(self.data, pd.DataFrame):
            return self.data.empty
        return False
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        将响应数据转换为DataFrame
        
        Returns:
            pd.DataFrame: 转换后的DataFrame
            
        Raises:
            ValueError: 如果数据无法转换为DataFrame
        """
        if self.data is None:
            return pd.DataFrame()
            
        if isinstance(self.data, pd.DataFrame):
            return self.data.copy()
            
        if isinstance(self.data, dict):
            if not self.data:
                return pd.DataFrame()
            # 尝试将字典转换为DataFrame
            try:
                return pd.DataFrame(self.data)
            except Exception as e:
                raise ValueError(f"无法将字典转换为DataFrame: {e}") from e
                
        raise ValueError(f"不支持的数据类型: {type(self.data)}")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        将响应转换为字典
        
        Returns:
            Dict[str, Any]: 包含响应数据的字典
        """
        result = self.dict(exclude={'data'})
        
        # 处理DataFrame数据
        if isinstance(self.data, pd.DataFrame):
            result['data'] = self.data.to_dict(orient='records')
        else:
            result['data'] = self.data
            
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataResponse':
        """
        从字典创建响应
        
        Args:
            data: 包含响应数据的字典
            
        Returns:
            DataResponse: 响应对象
        """
        # 处理DataFrame数据
        if 'data' in data and isinstance(data['data'], list):
            data = data.copy()
            data['data'] = pd.DataFrame(data['data'])
            
        return cls(**data)
