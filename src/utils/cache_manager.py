"""
多层缓存管理器
实现内存缓存、磁盘缓存和分布式缓存的统一管理
"""
import asyncio
import hashlib
import json
import pickle
import time
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import threading
from pathlib import Path
import pandas as pd

try:
    from src.utils.logger import get_logger
except ImportError:
    import logging
    def get_logger(name):
        return logging.getLogger(name)

logger = get_logger("cache_manager")


class CacheLevel(Enum):
    """缓存级别"""
    L1_MEMORY = "L1_MEMORY"      # 内存缓存
    L2_DISK = "L2_DISK"          # 磁盘缓存
    L3_REDIS = "L3_REDIS"        # Redis缓存


class CacheStrategy(Enum):
    """缓存策略"""
    LRU = "LRU"                  # 最近最少使用
    LFU = "LFU"                  # 最少使用频率
    FIFO = "FIFO"                # 先进先出
    TTL = "TTL"                  # 基于时间过期


@dataclass
class CacheEntry:
    """缓存条目"""
    key: str
    value: Any
    created_at: datetime
    last_accessed: datetime
    access_count: int = 0
    ttl_seconds: Optional[int] = None
    size_bytes: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_expired(self) -> bool:
        """检查是否过期"""
        if self.ttl_seconds is None:
            return False
        return (datetime.now() - self.created_at).total_seconds() > self.ttl_seconds
    
    def touch(self):
        """更新访问时间和次数"""
        self.last_accessed = datetime.now()
        self.access_count += 1
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "key": self.key,
            "created_at": self.created_at.isoformat(),
            "last_accessed": self.last_accessed.isoformat(),
            "access_count": self.access_count,
            "ttl_seconds": self.ttl_seconds,
            "size_bytes": self.size_bytes,
            "metadata": self.metadata
        }


class MemoryCache:
    """内存缓存实现"""
    
    def __init__(self, max_size: int = 1000, max_memory_mb: int = 100, strategy: CacheStrategy = CacheStrategy.LRU):
        self.max_size = max_size
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.strategy = strategy
        self.cache: Dict[str, CacheEntry] = {}
        self.current_memory_usage = 0
        self.lock = threading.RLock()
        
        # 统计信息
        self.hits = 0
        self.misses = 0
        self.evictions = 0
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        with self.lock:
            if key not in self.cache:
                self.misses += 1
                return None
            
            entry = self.cache[key]
            
            # 检查是否过期
            if entry.is_expired():
                self._remove_entry(key)
                self.misses += 1
                return None
            
            # 更新访问信息
            entry.touch()
            self.hits += 1
            
            logger.debug(f"内存缓存命中: {key}")
            return entry.value
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None, metadata: Dict[str, Any] = None) -> bool:
        """设置缓存值"""
        with self.lock:
            # 计算值的大小
            try:
                if isinstance(value, pd.DataFrame):
                    size_bytes = value.memory_usage(deep=True).sum()
                else:
                    size_bytes = len(pickle.dumps(value))
            except Exception:
                size_bytes = 1024  # 默认大小
            
            # 检查是否超过单个条目大小限制
            if size_bytes > self.max_memory_bytes * 0.5:
                logger.warning(f"缓存条目过大，拒绝缓存: {key} ({size_bytes} bytes)")
                return False
            
            # 如果键已存在，先移除旧条目
            if key in self.cache:
                self._remove_entry(key)
            
            # 确保有足够空间
            while (len(self.cache) >= self.max_size or 
                   self.current_memory_usage + size_bytes > self.max_memory_bytes):
                if not self._evict_one():
                    logger.warning("无法腾出缓存空间")
                    return False
            
            # 创建新条目
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.now(),
                last_accessed=datetime.now(),
                ttl_seconds=ttl_seconds,
                size_bytes=size_bytes,
                metadata=metadata or {}
            )
            
            self.cache[key] = entry
            self.current_memory_usage += size_bytes
            
            logger.debug(f"内存缓存设置: {key} ({size_bytes} bytes)")
            return True
    
    def delete(self, key: str) -> bool:
        """删除缓存条目"""
        with self.lock:
            if key in self.cache:
                self._remove_entry(key)
                return True
            return False
    
    def clear(self):
        """清空缓存"""
        with self.lock:
            self.cache.clear()
            self.current_memory_usage = 0
            logger.info("内存缓存已清空")
    
    def _remove_entry(self, key: str):
        """移除缓存条目"""
        if key in self.cache:
            entry = self.cache.pop(key)
            self.current_memory_usage -= entry.size_bytes
    
    def _evict_one(self) -> bool:
        """根据策略驱逐一个条目"""
        if not self.cache:
            return False
        
        if self.strategy == CacheStrategy.LRU:
            # 最近最少使用
            victim_key = min(self.cache.keys(), 
                           key=lambda k: self.cache[k].last_accessed)
        elif self.strategy == CacheStrategy.LFU:
            # 最少使用频率
            victim_key = min(self.cache.keys(), 
                           key=lambda k: self.cache[k].access_count)
        elif self.strategy == CacheStrategy.FIFO:
            # 先进先出
            victim_key = min(self.cache.keys(), 
                           key=lambda k: self.cache[k].created_at)
        else:
            # 默认LRU
            victim_key = min(self.cache.keys(), 
                           key=lambda k: self.cache[k].last_accessed)
        
        self._remove_entry(victim_key)
        self.evictions += 1
        logger.debug(f"驱逐缓存条目: {victim_key}")
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = self.hits / total_requests if total_requests > 0 else 0
            
            return {
                "cache_level": "L1_MEMORY",
                "strategy": self.strategy.value,
                "size": len(self.cache),
                "max_size": self.max_size,
                "memory_usage_bytes": self.current_memory_usage,
                "memory_usage_mb": round(self.current_memory_usage / 1024 / 1024, 2),
                "max_memory_mb": self.max_memory_bytes / 1024 / 1024,
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
                "hit_rate": hit_rate
            }


class DiskCache:
    """磁盘缓存实现"""
    
    def __init__(self, cache_dir: str = "cache", max_size_mb: int = 1000):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.index_file = self.cache_dir / "index.json"
        self.lock = threading.RLock()
        
        # 加载索引
        self.index = self._load_index()
        
        # 统计信息
        self.hits = 0
        self.misses = 0
    
    def _load_index(self) -> Dict[str, Dict[str, Any]]:
        """加载缓存索引"""
        if self.index_file.exists():
            try:
                with open(self.index_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载磁盘缓存索引失败: {e}")
        return {}
    
    def _save_index(self):
        """保存缓存索引"""
        try:
            with open(self.index_file, 'w', encoding='utf-8') as f:
                json.dump(self.index, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存磁盘缓存索引失败: {e}")
    
    def _get_cache_file_path(self, key: str) -> Path:
        """获取缓存文件路径"""
        # 使用MD5哈希避免文件名问题
        hash_key = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{hash_key}.cache"
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        with self.lock:
            if key not in self.index:
                self.misses += 1
                return None
            
            entry_info = self.index[key]
            
            # 检查是否过期
            if entry_info.get("ttl_seconds"):
                created_at = datetime.fromisoformat(entry_info["created_at"])
                if (datetime.now() - created_at).total_seconds() > entry_info["ttl_seconds"]:
                    self.delete(key)
                    self.misses += 1
                    return None
            
            # 读取缓存文件
            cache_file = self._get_cache_file_path(key)
            if not cache_file.exists():
                # 索引和文件不一致，清理索引
                del self.index[key]
                self._save_index()
                self.misses += 1
                return None
            
            try:
                with open(cache_file, 'rb') as f:
                    value = pickle.load(f)
                
                # 更新访问时间
                entry_info["last_accessed"] = datetime.now().isoformat()
                entry_info["access_count"] = entry_info.get("access_count", 0) + 1
                self._save_index()
                
                self.hits += 1
                logger.debug(f"磁盘缓存命中: {key}")
                return value
                
            except Exception as e:
                logger.error(f"读取磁盘缓存失败: {key} - {e}")
                self.delete(key)
                self.misses += 1
                return None
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None, metadata: Dict[str, Any] = None) -> bool:
        """设置缓存值"""
        with self.lock:
            try:
                # 序列化值
                cache_file = self._get_cache_file_path(key)
                with open(cache_file, 'wb') as f:
                    pickle.dump(value, f)
                
                # 获取文件大小
                file_size = cache_file.stat().st_size
                
                # 检查总大小限制
                current_size = sum(info.get("size_bytes", 0) for info in self.index.values())
                if current_size + file_size > self.max_size_bytes:
                    # 需要清理空间
                    self._cleanup_space(file_size)
                
                # 更新索引
                self.index[key] = {
                    "created_at": datetime.now().isoformat(),
                    "last_accessed": datetime.now().isoformat(),
                    "access_count": 0,
                    "ttl_seconds": ttl_seconds,
                    "size_bytes": file_size,
                    "metadata": metadata or {}
                }
                
                self._save_index()
                logger.debug(f"磁盘缓存设置: {key} ({file_size} bytes)")
                return True
                
            except Exception as e:
                logger.error(f"设置磁盘缓存失败: {key} - {e}")
                return False
    
    def delete(self, key: str) -> bool:
        """删除缓存条目"""
        with self.lock:
            if key not in self.index:
                return False
            
            # 删除文件
            cache_file = self._get_cache_file_path(key)
            if cache_file.exists():
                try:
                    cache_file.unlink()
                except Exception as e:
                    logger.error(f"删除缓存文件失败: {cache_file} - {e}")
            
            # 删除索引条目
            del self.index[key]
            self._save_index()
            return True
    
    def clear(self):
        """清空缓存"""
        with self.lock:
            # 删除所有缓存文件
            for cache_file in self.cache_dir.glob("*.cache"):
                try:
                    cache_file.unlink()
                except Exception as e:
                    logger.error(f"删除缓存文件失败: {cache_file} - {e}")
            
            # 清空索引
            self.index.clear()
            self._save_index()
            logger.info("磁盘缓存已清空")
    
    def _cleanup_space(self, needed_bytes: int):
        """清理磁盘空间"""
        # 按最后访问时间排序，删除最旧的条目
        sorted_keys = sorted(
            self.index.keys(),
            key=lambda k: self.index[k].get("last_accessed", "1970-01-01T00:00:00")
        )
        
        freed_bytes = 0
        for key in sorted_keys:
            if freed_bytes >= needed_bytes:
                break
            
            freed_bytes += self.index[key].get("size_bytes", 0)
            self.delete(key)
            logger.debug(f"清理磁盘缓存: {key}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = self.hits / total_requests if total_requests > 0 else 0
            
            total_size = sum(info.get("size_bytes", 0) for info in self.index.values())
            
            return {
                "cache_level": "L2_DISK",
                "size": len(self.index),
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / 1024 / 1024, 2),
                "max_size_mb": self.max_size_bytes / 1024 / 1024,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": hit_rate,
                "cache_dir": str(self.cache_dir)
            }


class RedisCache:
    """Redis缓存实现（可选）"""
    
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, password: str = None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.redis_client = None
        self.available = False
        
        # 统计信息
        self.hits = 0
        self.misses = 0
        
        self._connect()
    
    def _connect(self):
        """连接Redis"""
        try:
            import redis
            self.redis_client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=False
            )
            # 测试连接
            self.redis_client.ping()
            self.available = True
            logger.info(f"Redis缓存连接成功: {self.host}:{self.port}")
        except ImportError:
            logger.warning("Redis模块未安装，跳过Redis缓存")
        except Exception as e:
            logger.warning(f"Redis连接失败: {e}")
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        if not self.available:
            return None
        
        try:
            data = self.redis_client.get(key)
            if data is None:
                self.misses += 1
                return None
            
            value = pickle.loads(data)
            self.hits += 1
            logger.debug(f"Redis缓存命中: {key}")
            return value
            
        except Exception as e:
            logger.error(f"Redis获取失败: {key} - {e}")
            self.misses += 1
            return None
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None, metadata: Dict[str, Any] = None) -> bool:
        """设置缓存值"""
        if not self.available:
            return False
        
        try:
            data = pickle.dumps(value)
            if ttl_seconds:
                self.redis_client.setex(key, ttl_seconds, data)
            else:
                self.redis_client.set(key, data)
            
            logger.debug(f"Redis缓存设置: {key}")
            return True
            
        except Exception as e:
            logger.error(f"Redis设置失败: {key} - {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """删除缓存条目"""
        if not self.available:
            return False
        
        try:
            result = self.redis_client.delete(key)
            return result > 0
        except Exception as e:
            logger.error(f"Redis删除失败: {key} - {e}")
            return False
    
    def clear(self):
        """清空缓存"""
        if not self.available:
            return
        
        try:
            self.redis_client.flushdb()
            logger.info("Redis缓存已清空")
        except Exception as e:
            logger.error(f"Redis清空失败: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total_requests = self.hits + self.misses
        hit_rate = self.hits / total_requests if total_requests > 0 else 0
        
        stats = {
            "cache_level": "L3_REDIS",
            "available": self.available,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate
        }
        
        if self.available:
            try:
                info = self.redis_client.info()
                stats.update({
                    "used_memory": info.get("used_memory", 0),
                    "used_memory_human": info.get("used_memory_human", "0B"),
                    "connected_clients": info.get("connected_clients", 0),
                    "total_commands_processed": info.get("total_commands_processed", 0)
                })
            except Exception as e:
                logger.error(f"获取Redis统计信息失败: {e}")
        
        return stats


class MultiLevelCacheManager:
    """多层缓存管理器"""
    
    def __init__(self, 
                 enable_memory: bool = True,
                 enable_disk: bool = True,
                 enable_redis: bool = False,
                 memory_config: Dict[str, Any] = None,
                 disk_config: Dict[str, Any] = None,
                 redis_config: Dict[str, Any] = None):
        
        self.caches = {}
        
        # 初始化内存缓存
        if enable_memory:
            memory_config = memory_config or {}
            self.caches[CacheLevel.L1_MEMORY] = MemoryCache(**memory_config)
        
        # 初始化磁盘缓存
        if enable_disk:
            disk_config = disk_config or {}
            self.caches[CacheLevel.L2_DISK] = DiskCache(**disk_config)
        
        # 初始化Redis缓存
        if enable_redis:
            redis_config = redis_config or {}
            redis_cache = RedisCache(**redis_config)
            if redis_cache.available:
                self.caches[CacheLevel.L3_REDIS] = redis_cache
        
        logger.info(f"多层缓存管理器初始化完成，启用层级: {list(self.caches.keys())}")
    
    def get(self, key: str) -> Optional[Any]:
        """从缓存获取值（按层级顺序）"""
        # 按优先级顺序查找
        for level in [CacheLevel.L1_MEMORY, CacheLevel.L2_DISK, CacheLevel.L3_REDIS]:
            if level in self.caches:
                value = self.caches[level].get(key)
                if value is not None:
                    # 将值提升到更高层级的缓存
                    self._promote_to_higher_levels(key, value, level)
                    return value
        
        return None
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None, 
            levels: List[CacheLevel] = None, metadata: Dict[str, Any] = None) -> bool:
        """设置缓存值"""
        if levels is None:
            levels = list(self.caches.keys())
        
        success = False
        for level in levels:
            if level in self.caches:
                if self.caches[level].set(key, value, ttl_seconds, metadata):
                    success = True
        
        return success
    
    def delete(self, key: str, levels: List[CacheLevel] = None) -> bool:
        """删除缓存条目"""
        if levels is None:
            levels = list(self.caches.keys())
        
        success = False
        for level in levels:
            if level in self.caches:
                if self.caches[level].delete(key):
                    success = True
        
        return success
    
    def clear(self, levels: List[CacheLevel] = None):
        """清空缓存"""
        if levels is None:
            levels = list(self.caches.keys())
        
        for level in levels:
            if level in self.caches:
                self.caches[level].clear()
    
    def _promote_to_higher_levels(self, key: str, value: Any, current_level: CacheLevel):
        """将值提升到更高层级的缓存"""
        higher_levels = []
        
        if current_level == CacheLevel.L3_REDIS:
            higher_levels = [CacheLevel.L1_MEMORY, CacheLevel.L2_DISK]
        elif current_level == CacheLevel.L2_DISK:
            higher_levels = [CacheLevel.L1_MEMORY]
        
        for level in higher_levels:
            if level in self.caches:
                self.caches[level].set(key, value)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取所有缓存层的统计信息"""
        stats = {
            "timestamp": datetime.now().isoformat(),
            "enabled_levels": list(self.caches.keys()),
            "cache_stats": {}
        }
        
        total_hits = 0
        total_misses = 0
        
        for level, cache in self.caches.items():
            cache_stats = cache.get_stats()
            stats["cache_stats"][level.value] = cache_stats
            
            total_hits += cache_stats.get("hits", 0)
            total_misses += cache_stats.get("misses", 0)
        
        total_requests = total_hits + total_misses
        overall_hit_rate = total_hits / total_requests if total_requests > 0 else 0
        
        stats["overall"] = {
            "total_hits": total_hits,
            "total_misses": total_misses,
            "overall_hit_rate": overall_hit_rate
        }
        
        return stats
    
    def cache_decorator(self, ttl_seconds: Optional[int] = None, 
                       key_func: Optional[Callable] = None,
                       levels: List[CacheLevel] = None):
        """缓存装饰器"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                # 生成缓存键
                if key_func:
                    cache_key = key_func(*args, **kwargs)
                else:
                    cache_key = f"{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
                
                # 尝试从缓存获取
                cached_value = self.get(cache_key)
                if cached_value is not None:
                    return cached_value
                
                # 执行函数
                result = func(*args, **kwargs)
                
                # 缓存结果
                self.set(cache_key, result, ttl_seconds, levels)
                
                return result
            
            async def async_wrapper(*args, **kwargs):
                # 生成缓存键
                if key_func:
                    cache_key = key_func(*args, **kwargs)
                else:
                    cache_key = f"{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
                
                # 尝试从缓存获取
                cached_value = self.get(cache_key)
                if cached_value is not None:
                    return cached_value
                
                # 执行异步函数
                result = await func(*args, **kwargs)
                
                # 缓存结果
                self.set(cache_key, result, ttl_seconds, levels)
                
                return result
            
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                return wrapper
        
        return decorator


# 全局缓存管理器实例
global_cache_manager = MultiLevelCacheManager()


def cached(ttl_seconds: Optional[int] = None, 
          key_func: Optional[Callable] = None,
          levels: List[CacheLevel] = None):
    """缓存装饰器的便捷函数"""
    return global_cache_manager.cache_decorator(ttl_seconds, key_func, levels)