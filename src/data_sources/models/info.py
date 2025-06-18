"""
数据源信息模型模块

该模块定义了数据源信息相关的数据模型，用于描述和配置数据源的元数据。
"""
from __future__ import annotations

from datetime import datetime, date
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Type, TypeVar, ClassVar

from pydantic import BaseModel, Field, validator, root_validator, HttpUrl

from ..enums import DataSourceType, DataSourceStatus, DataType

# 类型别名
TimestampType = Union[str, datetime, date, int, float]

class DataSourceInfo(BaseModel):
    """
    数据源信息类
    
    描述数据源的元数据和配置信息。
    
    属性:
        name: 数据源名称（唯一标识）
        display_name: 显示名称
        description: 数据源描述
        source_type: 数据源类型
        status: 数据源状态
        version: 数据源版本
        last_updated: 最后更新时间
        supported_data_types: 支持的数据类型
        config_schema: 配置模式
        required_config: 必需配置项
        optional_config: 可选配置项
        rate_limit: 请求速率限制 (requests/second)
        is_premium: 是否付费数据源
        tags: 标签
        metadata: 额外元数据
    """
    name: str = Field(..., min_length=1, max_length=100, description="数据源名称（唯一标识）")
    display_name: str = Field(..., min_length=1, max_length=200, description="显示名称")
    description: str = Field("", description="数据源描述")
    source_type: Union[DataSourceType, str] = Field(..., description="数据源类型")
    status: Union[DataSourceStatus, str] = Field(
        DataSourceStatus.UNINITIALIZED, 
        description="数据源状态"
    )
    version: str = Field("1.0.0", description="数据源版本")
    last_updated: Optional[datetime] = Field(None, description="最后更新时间")
    supported_data_types: List[Union[DataType, str]] = Field(
        default_factory=list, 
        description="支持的数据类型"
    )
    config_schema: Dict[str, Any] = Field(
        default_factory=dict, 
        description="配置模式 (JSON Schema)"
    )
    required_config: List[str] = Field(
        default_factory=list, 
        description="必需配置项"
    )
    optional_config: List[str] = Field(
        default_factory=list, 
        description="可选配置项"
    )
    rate_limit: float = Field(
        1.0, 
        ge=0.1, 
        le=1000.0, 
        description="请求速率限制 (requests/second)"
    )
    is_premium: bool = Field(False, description="是否付费数据源")
    tags: List[str] = Field(
        default_factory=list, 
        description="标签",
        example=["stock", "china", "free"]
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict, 
        description="额外元数据"
    )
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
        }
    
    @validator('source_type', pre=True)
    def validate_source_type(cls, v):
        """验证数据源类型"""
        if isinstance(v, DataSourceType):
            return v
        try:
            return DataSourceType(v)
        except ValueError:
            return v  # 允许自定义类型
    
    @validator('status', pre=True)
    def validate_status(cls, v):
        """验证数据源状态"""
        if isinstance(v, DataSourceStatus):
            return v
        try:
            return DataSourceStatus(v)
        except ValueError:
            return v  # 允许自定义状态
    
    @validator('supported_data_types', pre=True, each_item=True)
    def validate_data_types(cls, v):
        """验证支持的数据类型"""
        if isinstance(v, DataType):
            return v
        try:
            return DataType(v)
        except ValueError:
            return v  # 允许自定义类型
    
    @root_validator(skip_on_failure=True)
    def set_defaults(cls, values):
        """设置默认值"""
        # 如果未设置last_updated，则设置为当前时间
        if 'last_updated' not in values or values['last_updated'] is None:
            values['last_updated'] = datetime.utcnow()
            
        # 确保required_config和optional_config是唯一的
        if 'required_config' in values and 'optional_config' in values:
            # 从optional_config中移除required_config中已存在的项
            values['optional_config'] = [
                item for item in values['optional_config'] 
                if item not in values['required_config']
            ]
            
        return values
    
    @property
    def is_available(self) -> bool:
        """检查数据源是否可用"""
        return DataSourceStatus.is_available(self.status)
    
    @property
    def is_operational(self) -> bool:
        """检查数据源是否可操作"""
        return DataSourceStatus.is_operational(self.status)
    
    def supports_data_type(self, data_type: Union[DataType, str]) -> bool:
        """
        检查数据源是否支持指定的数据类型
        
        Args:
            data_type: 要检查的数据类型
            
        Returns:
            bool: 如果支持则返回True
        """
        if not self.supported_data_types:
            return True  # 如果没有指定支持的数据类型，则假设支持所有类型
            
        if isinstance(data_type, str):
            try:
                data_type = DataType(data_type)
            except ValueError:
                return data_type in [str(t) for t in self.supported_data_types]
                
        return data_type in self.supported_data_types
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """
        验证配置是否有效
        
        Args:
            config: 要验证的配置字典
            
        Returns:
            bool: 如果配置有效则返回True
            
        Raises:
            ValueError: 如果配置无效
        """
        # 检查必需配置项
        missing = [field for field in self.required_config if field not in config]
        if missing:
            raise ValueError(f"缺少必需的配置项: {', '.join(missing)}")
            
        # 检查未知配置项
        known_fields = set(self.required_config + self.optional_config)
        unknown = [k for k in config.keys() if k not in known_fields]
        if unknown and self.config_schema:
            # 如果有配置模式，则允许额外的配置项
            pass
        elif unknown:
            raise ValueError(f"未知的配置项: {', '.join(unknown)}")
            
        # TODO: 如果提供了config_schema，可以使用它进行更详细的验证
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """
        将数据源信息转换为字典
        
        Returns:
            Dict[str, Any]: 包含数据源信息的字典
        """
        return self.dict(exclude_unset=True)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataSourceInfo':
        """
        从字典创建数据源信息
        
        Args:
            data: 包含数据源信息的字典
            
        Returns:
            DataSourceInfo: 数据源信息对象
        """
        return cls(**data)
    
    def update(self, **kwargs) -> 'DataSourceInfo':
        """
        更新数据源信息
        
        Args:
            **kwargs: 要更新的字段
            
        Returns:
            DataSourceInfo: 更新后的数据源信息对象
        """
        return self.copy(update=kwargs)
