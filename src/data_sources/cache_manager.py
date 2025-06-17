"""
数据缓存管理器
提供多级缓存和智能缓存策略
"""
import pandas as pd
import pickle
import hashlib
import json
from typing import Dict, Any, Optional, Union
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
from loguru import logger

from .base_data_source import DataResponse, DataRequest, DataType


class CacheLevel(str):
    """缓存级别"""
    MEMORY = "memory"      # 内存缓存
    DISK = "disk"          # 磁盘缓存
    DATABASE = "database"  # 数据库缓存


class CacheStrategy(str):
    """缓存策略"""
    LRU = "lru"           # 最近最少使用
    TTL = "ttl"           # 生存时间
    SMART = "smart"       # 智能缓存


class CacheManager:
    """缓存管理器"""
    
    def __init__(self, cache_dir: str = "cache", max_memory_size: int = 100):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        # 内存缓存
        self.memory_cache: Dict[str, Dict[str, Any]] = {}
        self.max_memory_size = max_memory_size
        self.access_times: Dict[str, datetime] = {}
        
        # 磁盘缓存目录
        self.disk_cache_dir = self.cache_dir / "disk"
        self.disk_cache_dir.mkdir(exist_ok=True)
        
        # 数据库缓存
        self.db_path = self.cache_dir / "cache.db"
        self._init_database()
        
        # 缓存配置
        self.cache_config = {
            DataType.STOCK_BASIC: {
                'ttl': timedelta(days=1),      # 股票基础信息缓存1天
                'level': CacheLevel.DISK,
                'strategy': CacheStrategy.TTL
            },
            DataType.DAILY_QUOTES: {
                'ttl': timedelta(hours=1),     # 日线数据缓存1小时
                'level': CacheLevel.MEMORY,
                'strategy': CacheStrategy.SMART
            },
            DataType.MINUTE_QUOTES: {
                'ttl': timedelta(minutes=5),   # 分钟数据缓存5分钟
                'level': CacheLevel.MEMORY,
                'strategy': CacheStrategy.TTL
            },
            DataType.FINANCIAL_DATA: {
                'ttl': timedelta(days=7),      # 财务数据缓存7天
                'level': CacheLevel.DATABASE,
                'strategy': CacheStrategy.TTL
            }
        }
    
    def _init_database(self):
        """初始化数据库缓存"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS cache_data (
                        cache_key TEXT PRIMARY KEY,
                        data_type TEXT,
                        data BLOB,
                        metadata TEXT,
                        created_at TIMESTAMP,
                        expires_at TIMESTAMP,
                        access_count INTEGER DEFAULT 0,
                        last_access TIMESTAMP
                    )
                ''')
                conn.commit()
        except Exception as e:
            logger.error(f"初始化数据库缓存失败: {e}")
    
    def get(self, request: DataRequest) -> Optional[DataResponse]:
        """获取缓存数据"""
        cache_key = self._generate_cache_key(request)
        
        # 获取缓存配置
        config = self.cache_config.get(request.data_type, {
            'ttl': timedelta(minutes=30),
            'level': CacheLevel.MEMORY,
            'strategy': CacheStrategy.TTL
        })
        
        # 根据缓存级别获取数据
        if config['level'] == CacheLevel.MEMORY:
            return self._get_from_memory(cache_key, config)
        elif config['level'] == CacheLevel.DISK:
            return self._get_from_disk(cache_key, config)
        elif config['level'] == CacheLevel.DATABASE:
            return self._get_from_database(cache_key, config)
        
        return None
    
    def put(self, request: DataRequest, response: pd.DataFrame) -> None:
        """存储数据到缓存"""
        if not (response is not None and not response.empty):
            return
        
        cache_key = self._generate_cache_key(request)
        
        # 获取缓存配置
        config = self.cache_config.get(request.data_type, {
            'ttl': timedelta(minutes=30),
            'level': CacheLevel.MEMORY,
            'strategy': CacheStrategy.TTL
        })
        
        # 根据缓存级别存储数据
        if config['level'] == CacheLevel.MEMORY:
            self._put_to_memory(cache_key, response, config)
        elif config['level'] == CacheLevel.DISK:
            self._put_to_disk(cache_key, response, config)
        elif config['level'] == CacheLevel.DATABASE:
            self._put_to_database(cache_key, response, config)
    
    def _get_from_memory(self, cache_key: str, config: Dict[str, Any]) -> Optional[DataResponse]:
        """从内存缓存获取数据"""
        if cache_key not in self.memory_cache:
            return None
        
        cache_item = self.memory_cache[cache_key]
        
        # 检查是否过期
        if datetime.now() > cache_item['expires_at']:
            del self.memory_cache[cache_key]
            if cache_key in self.access_times:
                del self.access_times[cache_key]
            return None
        
        # 更新访问时间
        self.access_times[cache_key] = datetime.now()
        
        logger.debug(f"从内存缓存获取数据: {cache_key}")
        return cache_item['response']
    
    def _put_to_memory(self, cache_key: str, response: pd.DataFrame, config: Dict[str, Any]):
        """存储数据到内存缓存"""
        # 检查内存缓存大小
        if len(self.memory_cache) >= self.max_memory_size:
            self._evict_memory_cache()
        
        expires_at = datetime.now() + config['ttl']
        
        self.memory_cache[cache_key] = {
            'response': response,
            'created_at': datetime.now(),
            'expires_at': expires_at
        }
        
        self.access_times[cache_key] = datetime.now()
        logger.debug(f"数据已存储到内存缓存: {cache_key}")
    
    def _get_from_disk(self, cache_key: str, config: Dict[str, Any]) -> Optional[DataResponse]:
        """从磁盘缓存获取数据"""
        cache_file = self.disk_cache_dir / f"{cache_key}.pkl"
        
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'rb') as f:
                cache_data = pickle.load(f)
            
            # 检查是否过期
            if datetime.now() > cache_data['expires_at']:
                cache_file.unlink()
                return None
            
            logger.debug(f"从磁盘缓存获取数据: {cache_key}")
            return cache_data['response']
            
        except Exception as e:
            logger.error(f"从磁盘缓存读取数据失败: {e}")
            return None
    
    def _put_to_disk(self, cache_key: str, response: pd.DataFrame, config: Dict[str, Any]):
        """存储数据到磁盘缓存"""
        cache_file = self.disk_cache_dir / f"{cache_key}.pkl"
        expires_at = datetime.now() + config['ttl']
        
        cache_data = {
            'response': response,
            'created_at': datetime.now(),
            'expires_at': expires_at
        }
        
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
            
            logger.debug(f"数据已存储到磁盘缓存: {cache_key}")
            
        except Exception as e:
            logger.error(f"存储数据到磁盘缓存失败: {e}")
    
    def _get_from_database(self, cache_key: str, config: Dict[str, Any]) -> Optional[DataResponse]:
        """从数据库缓存获取数据"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'SELECT data, metadata, expires_at FROM cache_data WHERE cache_key = ?',
                    (cache_key,)
                )
                row = cursor.fetchone()
                
                if not row:
                    return None
                
                data_blob, metadata_json, expires_at_str = row
                expires_at = datetime.fromisoformat(expires_at_str)
                
                # 检查是否过期
                if datetime.now() > expires_at:
                    conn.execute('DELETE FROM cache_data WHERE cache_key = ?', (cache_key,))
                    conn.commit()
                    return None
                
                # 更新访问统计
                conn.execute(
                    'UPDATE cache_data SET access_count = access_count + 1, last_access = ? WHERE cache_key = ?',
                    (datetime.now().isoformat(), cache_key)
                )
                conn.commit()
                
                # 反序列化数据
                response = pickle.loads(data_blob)
                
                logger.debug(f"从数据库缓存获取数据: {cache_key}")
                return response
                
        except Exception as e:
            logger.error(f"从数据库缓存读取数据失败: {e}")
            return None
    
    def _put_to_database(self, cache_key: str, response: pd.DataFrame, config: Dict[str, Any]):
        """存储数据到数据库缓存"""
        expires_at = datetime.now() + config['ttl']
        
        try:
            # 序列化数据
            data_blob = pickle.dumps(response)
            metadata = {
                'data_type': response.data_type.value,
                'source': response.source,
                'records': len(response)
            }
            metadata_json = json.dumps(metadata)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO cache_data 
                    (cache_key, data_type, data, metadata, created_at, expires_at, access_count, last_access)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                ''', (
                    cache_key,
                    response.data_type.value,
                    data_blob,
                    metadata_json,
                    datetime.now().isoformat(),
                    expires_at.isoformat(),
                    datetime.now().isoformat()
                ))
                conn.commit()
            
            logger.debug(f"数据已存储到数据库缓存: {cache_key}")
            
        except Exception as e:
            logger.error(f"存储数据到数据库缓存失败: {e}")
    
    def _evict_memory_cache(self):
        """清理内存缓存（LRU策略）"""
        if not self.access_times:
            return
        
        # 找到最久未访问的缓存项
        oldest_key = min(self.access_times.keys(), key=lambda k: self.access_times[k])
        
        if oldest_key in self.memory_cache:
            del self.memory_cache[oldest_key]
        if oldest_key in self.access_times:
            del self.access_times[oldest_key]
        
        logger.debug(f"清理内存缓存项: {oldest_key}")
    
    def _generate_cache_key(self, request: DataRequest) -> str:
        """生成缓存键"""
        key_data = {
            'data_type': request.data_type.value,
            'symbol': request.symbol,
            'symbols': sorted(request.symbols) if request.symbols else None,
            'start_date': str(request.start_date) if request.start_date else None,
            'end_date': str(request.end_date) if request.end_date else None,
            'fields': sorted(request.fields) if request.fields else None,
            'extra_params': sorted((request.extra_params or {}).items())
        }
        
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def clear_cache(self, data_type: Optional[DataType] = None):
        """清空缓存"""
        if data_type:
            # 清空特定数据类型的缓存
            keys_to_remove = []
            for key, item in self.memory_cache.items():
                if item['response'].data_type == data_type:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                if key in self.memory_cache:
                    del self.memory_cache[key]
                if key in self.access_times:
                    del self.access_times[key]
            
            # 清空数据库缓存
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute('DELETE FROM cache_data WHERE data_type = ?', (data_type.value,))
                    conn.commit()
            except Exception as e:
                logger.error(f"清空数据库缓存失败: {e}")
        else:
            # 清空所有缓存
            self.memory_cache.clear()
            self.access_times.clear()
            
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute('DELETE FROM cache_data')
                    conn.commit()
            except Exception as e:
                logger.error(f"清空数据库缓存失败: {e}")
        
        logger.info(f"缓存已清空: {data_type.value if data_type else '全部'}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        stats = {
            'memory_cache_size': len(self.memory_cache),
            'disk_cache_files': len(list(self.disk_cache_dir.glob('*.pkl'))),
            'database_cache_records': 0
        }
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('SELECT COUNT(*) FROM cache_data')
                stats['database_cache_records'] = cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"获取数据库缓存统计失败: {e}")
        
        return stats
