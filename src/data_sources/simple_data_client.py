"""
简化的数据客户端
提供简单直接的数据获取接口，只支持AkShare
"""
from typing import Optional, List, Union
from datetime import datetime, date
import pandas as pd
from loguru import logger

from .akshare_data_source import AkShareDataSource
from .base_data_source import DataRequest, DataResponse, DataType


class SimpleDataClient:
    """简化的数据客户端"""
    
    def __init__(self):
        self.akshare_client = None
        self._initialize_akshare()
    
    def _initialize_akshare(self):
        """初始化AkShare客户端"""
        try:
            self.akshare_client = AkShareDataSource()
            if self.akshare_client.initialize():
                logger.info("✅ AkShare客户端初始化成功")
            else:
                logger.error("❌ AkShare客户端初始化失败")
                self.akshare_client = None
        except Exception as e:
            logger.error(f"❌ AkShare客户端初始化异常: {e}")
            self.akshare_client = None
    
    def is_available(self) -> bool:
        """检查数据源是否可用"""
        return self.akshare_client is not None
    
    def get_stock_basic(self) -> Optional[pd.DataFrame]:
        """获取股票基础信息"""
        if not self.is_available():
            logger.error("❌ AkShare客户端不可用")
            return None
        
        try:
            request = DataRequest(data_type=DataType.STOCK_BASIC)
            response = self.akshare_client.fetch_data(request)
            
            if response.success and not response.data.empty:
                logger.info(f"✅ 获取到 {len(response.data)} 只股票基础信息")
                return response.data
            else:
                logger.error(f"❌ 获取股票基础信息失败: {response.error_message}")
                return None
                
        except Exception as e:
            logger.error(f"❌ 获取股票基础信息异常: {e}")
            return None
    
    def get_daily_quotes(self, symbol: str, 
                        start_date: Optional[Union[str, date, datetime]] = None,
                        end_date: Optional[Union[str, date, datetime]] = None) -> Optional[pd.DataFrame]:
        """获取日线行情数据"""
        if not self.is_available():
            logger.error("❌ AkShare客户端不可用")
            return None
        
        try:
            request = DataRequest(
                data_type=DataType.DAILY_QUOTES,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )
            response = self.akshare_client.fetch_data(request)
            
            if response.success and not response.data.empty:
                logger.info(f"✅ 获取到 {symbol} 的 {len(response.data)} 条日线数据")
                return response.data
            else:
                logger.error(f"❌ 获取 {symbol} 日线数据失败: {response.error_message}")
                return None
                
        except Exception as e:
            logger.error(f"❌ 获取 {symbol} 日线数据异常: {e}")
            return None
    
    def get_batch_quotes(self, symbols: List[str],
                        start_date: Optional[Union[str, date, datetime]] = None,
                        end_date: Optional[Union[str, date, datetime]] = None) -> pd.DataFrame:
        """批量获取日线行情数据"""
        all_data = []
        
        for symbol in symbols:
            data = self.get_daily_quotes(symbol, start_date, end_date)
            if data is not None and not data.empty:
                all_data.append(data)
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            logger.info(f"✅ 批量获取完成，共 {len(result)} 条记录")
            return result
        else:
            logger.warning("⚠️ 批量获取无数据")
            return pd.DataFrame()
    
    def get_index_data(self, index_code: str = "000001",
                      start_date: Optional[Union[str, date, datetime]] = None,
                      end_date: Optional[Union[str, date, datetime]] = None) -> Optional[pd.DataFrame]:
        """获取指数数据"""
        if not self.is_available():
            logger.error("❌ AkShare客户端不可用")
            return None
        
        try:
            request = DataRequest(
                data_type=DataType.INDEX_DATA,
                symbol=index_code,
                start_date=start_date,
                end_date=end_date
            )
            response = self.akshare_client.fetch_data(request)
            
            if response.success and not response.data.empty:
                logger.info(f"✅ 获取到指数 {index_code} 的 {len(response.data)} 条数据")
                return response.data
            else:
                logger.error(f"❌ 获取指数 {index_code} 数据失败: {response.error_message}")
                return None
                
        except Exception as e:
            logger.error(f"❌ 获取指数 {index_code} 数据异常: {e}")
            return None
    
    def test_connection(self) -> bool:
        """测试连接"""
        if not self.is_available():
            return False
        
        try:
            # 尝试获取少量股票基础信息来测试连接
            basic_data = self.get_stock_basic()
            return basic_data is not None and not basic_data.empty
        except Exception as e:
            logger.error(f"❌ 连接测试失败: {e}")
            return False


# 全局客户端实例
_global_client: Optional[SimpleDataClient] = None


def get_simple_client() -> SimpleDataClient:
    """获取全局简化客户端实例"""
    global _global_client
    
    if _global_client is None:
        _global_client = SimpleDataClient()
    
    return _global_client


def test_simple_client():
    """测试简化客户端"""
    logger.info("🔍 测试简化数据客户端...")
    
    client = get_simple_client()
    
    # 测试连接
    if client.test_connection():
        logger.success("✅ 简化客户端连接测试成功")
        
        # 测试获取股票基础信息
        basic_data = client.get_stock_basic()
        if basic_data is not None:
            logger.success(f"✅ 获取股票基础信息成功: {len(basic_data)} 只股票")
        
        # 测试获取日线数据
        quotes_data = client.get_daily_quotes("000001.SZ")
        if quotes_data is not None:
            logger.success(f"✅ 获取日线数据成功: {len(quotes_data)} 条记录")
        
        return True
    else:
        logger.error("❌ 简化客户端连接测试失败")
        return False


if __name__ == "__main__":
    test_simple_client()
