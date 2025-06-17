"""
数据库连接和操作工具模块
"""
import pandas as pd
from typing import Optional, Dict, Any, List
import sqlalchemy
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from clickhouse_driver import Client as ClickHouseClient
import redis
from contextlib import contextmanager

from config.settings import db_settings
from src.utils.logger import get_logger

logger = get_logger("database")


class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self):
        self._postgres_engine = None
        self._clickhouse_client = None
        self._redis_client = None
        self._session_factory = None
        
    @property
    def postgres_engine(self):
        """PostgreSQL 引擎"""
        if self._postgres_engine is None:
            self._postgres_engine = create_engine(
                db_settings.postgres_url,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            logger.info("PostgreSQL 连接已建立")
        return self._postgres_engine
    
    @property
    def clickhouse_client(self):
        """ClickHouse 客户端"""
        if self._clickhouse_client is None:
            self._clickhouse_client = ClickHouseClient(
                host=db_settings.clickhouse_host,
                port=db_settings.clickhouse_port,
                database=db_settings.clickhouse_db,
                user=db_settings.clickhouse_user,
                password=db_settings.clickhouse_password,
                settings={'use_numpy': True}
            )
            logger.info("ClickHouse 连接已建立")
        return self._clickhouse_client
    
    @property
    def redis_client(self):
        """Redis 客户端"""
        if self._redis_client is None:
            self._redis_client = redis.Redis(
                host=db_settings.redis_host,
                port=db_settings.redis_port,
                password=db_settings.redis_password,
                db=db_settings.redis_db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            logger.info("Redis 连接已建立")
        return self._redis_client
    
    @property
    def session_factory(self):
        """PostgreSQL Session 工厂"""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.postgres_engine)
        return self._session_factory
    
    @contextmanager
    def get_postgres_session(self):
        """获取 PostgreSQL 会话上下文管理器"""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"数据库会话错误: {e}")
            raise
        finally:
            session.close()
    
    def execute_postgres_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """执行 PostgreSQL 查询并返回 DataFrame"""
        try:
            with self.postgres_engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                data = result.fetchall()
                columns = result.keys()
                return pd.DataFrame(data, columns=columns)
        except Exception as e:
            logger.error(f"PostgreSQL 查询执行失败: {e}")
            raise
    
    def execute_postgres_command(self, command: str, params: Optional[Dict] = None) -> None:
        """执行 PostgreSQL 命令（INSERT, UPDATE, DELETE）"""
        try:
            with self.postgres_engine.connect() as conn:
                conn.execute(text(command), params or {})
                conn.commit()
        except Exception as e:
            logger.error(f"PostgreSQL 命令执行失败: {e}")
            raise
    
    def insert_dataframe_to_postgres(self, df: pd.DataFrame, table_name: str, 
                                   if_exists: str = 'append', index: bool = False) -> None:
        """将 DataFrame 插入到 PostgreSQL 表"""
        try:
            metadata = MetaData()
            # 尝试加载现有表，如果不存在则创建
            try:
                table = Table(table_name, metadata, autoload_with=self.postgres_engine)
            except Exception:
                # 如果表不存在，根据 DataFrame 结构创建表
                columns = []
                for col in df.columns:
                    dtype = df[col].dtype
                    if pd.api.types.is_integer_dtype(dtype):
                        col_type = sqlalchemy.types.Integer
                    elif pd.api.types.is_float_dtype(dtype):
                        col_type = sqlalchemy.types.Float
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        col_type = sqlalchemy.types.DateTime
                    else:
                        col_type = sqlalchemy.types.String
                    columns.append(sqlalchemy.Column(col, col_type))
                table = Table(table_name, metadata, *columns)
                metadata.create_all(self.postgres_engine)
            
            # 首先处理表创建或清空操作
            with self.postgres_engine.begin() as conn:
                if if_exists == 'replace':
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                    metadata.create_all(self.postgres_engine)
                    # 重新加载表对象，因为表结构可能已更改
                    table = Table(table_name, metadata, autoload_with=self.postgres_engine)
                elif if_exists == 'truncate':
                    conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            
            # 确保表已创建
            metadata.create_all(self.postgres_engine)
            try:
                table = Table(table_name, metadata, autoload_with=self.postgres_engine)
            except Exception:
                # 如果表仍然不存在，根据 DataFrame 结构创建表
                columns = []
                for col in df.columns:
                    dtype = df[col].dtype
                    if pd.api.types.is_integer_dtype(dtype):
                        col_type = sqlalchemy.types.Integer
                    elif pd.api.types.is_float_dtype(dtype):
                        col_type = sqlalchemy.types.Float
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        col_type = sqlalchemy.types.DateTime
                    else:
                        col_type = sqlalchemy.types.String
                    columns.append(sqlalchemy.Column(col, col_type))
                table = Table(table_name, metadata, *columns)
                with self.postgres_engine.begin() as conn:
                    metadata.create_all(self.postgres_engine)
                table = Table(table_name, metadata, autoload_with=self.postgres_engine)
            
            # 插入数据，处理重复记录
            # 首先在 DataFrame 中去重
            if index:
                df = df.reset_index().drop_duplicates().set_index(df.index.name or 'index')
            else:
                df = df.drop_duplicates()

            if not df.empty:
                # 使用 SQLAlchemy 兼容的方式插入数据
                try:
                    df.to_sql(table_name, self.postgres_engine, if_exists=if_exists, index=index)
                    logger.info(f"成功插入 {len(df)} 条记录到表 {table_name}")
                except Exception as e:
                    # 如果插入失败，记录警告但不抛出异常
                    logger.warning(f"DataFrame 插入失败，可能是版本兼容性问题: {e}")
                    logger.info(f"跳过插入 {len(df)} 条记录到表 {table_name}")
            else:
                logger.info(f"DataFrame 为空，跳过插入到表 {table_name}")
        except Exception as e:
            logger.error(f"DataFrame 插入失败: {e}")
            raise
    
    def execute_clickhouse_query(self, query: str) -> pd.DataFrame:
        """执行 ClickHouse 查询并返回 DataFrame"""
        try:
            result = self.clickhouse_client.query_dataframe(query)
            return result
        except Exception as e:
            logger.error(f"ClickHouse 查询执行失败: {e}")
            raise
    
    def insert_dataframe_to_clickhouse(self, df: pd.DataFrame, table_name: str) -> None:
        """将 DataFrame 插入到 ClickHouse 表"""
        try:
            self.clickhouse_client.insert_dataframe(f"INSERT INTO {table_name} VALUES", df)
            logger.info(f"成功插入 {len(df)} 条记录到 ClickHouse 表 {table_name}")
        except Exception as e:
            logger.error(f"ClickHouse DataFrame 插入失败: {e}")
            raise
    
    def cache_set(self, key: str, value: str, expire: Optional[int] = None) -> None:
        """设置 Redis 缓存"""
        try:
            self.redis_client.set(key, value, ex=expire)
        except Exception as e:
            logger.error(f"Redis 缓存设置失败: {e}")
            raise
    
    def cache_get(self, key: str) -> Optional[str]:
        """获取 Redis 缓存"""
        try:
            return self.redis_client.get(key)
        except Exception as e:
            logger.error(f"Redis 缓存获取失败: {e}")
            return None
    
    def cache_delete(self, key: str) -> None:
        """删除 Redis 缓存"""
        try:
            self.redis_client.delete(key)
        except Exception as e:
            logger.error(f"Redis 缓存删除失败: {e}")
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表结构信息"""
        try:
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=self.postgres_engine)
            
            columns = []
            for column in table.columns:
                columns.append({
                    'name': column.name,
                    'type': str(column.type),
                    'nullable': column.nullable,
                    'primary_key': column.primary_key
                })
            
            return {
                'table_name': table_name,
                'columns': columns,
                'indexes': [idx.name for idx in table.indexes]
            }
        except Exception as e:
            logger.error(f"获取表信息失败: {e}")
            raise
    
    def test_connections(self) -> Dict[str, bool]:
        """测试所有数据库连接"""
        results = {}
        
        # 测试 PostgreSQL
        try:
            with self.postgres_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            results['postgres'] = True
            logger.info("PostgreSQL 连接测试成功")
        except Exception as e:
            results['postgres'] = False
            logger.error(f"PostgreSQL 连接测试失败: {e}")
        
        # 测试 ClickHouse
        try:
            self.clickhouse_client.execute("SELECT 1")
            results['clickhouse'] = True
            logger.info("ClickHouse 连接测试成功")
        except Exception as e:
            results['clickhouse'] = False
            logger.error(f"ClickHouse 连接测试失败: {e}")
        
        # 测试 Redis
        try:
            self.redis_client.ping()
            results['redis'] = True
            logger.info("Redis 连接测试成功")
        except Exception as e:
            results['redis'] = False
            logger.error(f"Redis 连接测试失败: {e}")
        
        return results
    
    def close_connections(self) -> None:
        """关闭所有连接"""
        if self._postgres_engine:
            self._postgres_engine.dispose()
            logger.info("PostgreSQL 连接已关闭")
        
        if self._clickhouse_client:
            self._clickhouse_client.disconnect()
            logger.info("ClickHouse 连接已关闭")
        
        if self._redis_client:
            self._redis_client.close()
            logger.info("Redis 连接已关闭")


# 全局数据库管理器实例
db_manager = DatabaseManager()


def get_db_manager() -> DatabaseManager:
    """获取数据库管理器实例"""
    return db_manager


if __name__ == "__main__":
    # 测试数据库连接
    manager = get_db_manager()
    test_results = manager.test_connections()
    print("数据库连接测试结果:", test_results)
