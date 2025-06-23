#!/usr/bin/env python3
"""
数据库备份管理器
支持在数据拉取后立即进行全量备份，包含自动清理和错误处理
"""
import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import tarfile
import shutil
import threading
from typing import Optional, List
from loguru import logger

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config.settings import db_settings

class BackupManager:
    """数据库备份管理器"""
    
    def __init__(self):
        self.backup_dir = project_root / "backups"
        self.clickhouse_backup_dir = self.backup_dir / "clickhouse"
        self.postgres_backup_dir = self.backup_dir / "postgres"
        self.backup_retention_days = 0  # 0表示只保留最新备份
        
    def backup_after_data_update(self, backup_type: str = "full") -> bool:
        """
        数据更新后的备份操作
        
        Args:
            backup_type: 备份类型 ("full" 或 "incremental")
            
        Returns:
            bool: 备份是否成功
        """
        try:
            logger.info(f"开始执行 {backup_type} 备份...")
            
            # 创建备份目录
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            self.clickhouse_backup_dir.mkdir(parents=True, exist_ok=True)
            self.postgres_backup_dir.mkdir(parents=True, exist_ok=True)
            
            # 并行执行备份
            backup_tasks = []
            
            # ClickHouse备份
            clickhouse_thread = threading.Thread(
                target=self._backup_clickhouse,
                args=(backup_type,)
            )
            clickhouse_thread.start()
            backup_tasks.append(("clickhouse", clickhouse_thread))
            
            # PostgreSQL备份
            postgres_thread = threading.Thread(
                target=self._backup_postgres,
                args=(backup_type,)
            )
            postgres_thread.start()
            backup_tasks.append(("postgres", postgres_thread))
            
            # 等待所有备份完成
            success_count = 0
            for db_name, thread in backup_tasks:
                thread.join()
                if thread.is_alive():
                    logger.error(f"{db_name} 备份超时")
                else:
                    success_count += 1
            
            # 清理旧备份
            self._cleanup_old_backups()
            
            if success_count == len(backup_tasks):
                logger.success(f"✅ {backup_type} 备份完成")
                return True
            else:
                logger.error(f"❌ 部分备份失败 ({success_count}/{len(backup_tasks)})")
                return False
                
        except Exception as e:
            logger.error(f"备份操作失败: {e}")
            return False
    
    def _backup_clickhouse(self, backup_type: str):
        """备份ClickHouse数据库"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_tar = self.clickhouse_backup_dir / f"clickhouse_{backup_type}_{timestamp}.tar.gz"
            
            cmd = [
                "docker", "exec", "vespera-clickhouse-1",
                "clickhouse-client",
                f"--host={db_settings.clickhouse_host}",
                f"--port={db_settings.clickhouse_port}",
                f"--user={db_settings.clickhouse_user}",
                f"--password={db_settings.clickhouse_password}",
                f"--database={db_settings.clickhouse_db}",
                "--query"
            ]
            
            # 获取所有表名
            tables_cmd = cmd + ["SHOW TABLES"]
            tables = subprocess.check_output(tables_cmd).decode().strip().split("\n")
            
            backup_files = []
            for table in tables:
                if not table:
                    continue
                    
                # 导出建表语句
                table_sql = self.clickhouse_backup_dir / f"{table}_{timestamp}.sql"
                table_cmd = cmd + [f"SHOW CREATE TABLE {table}"]
                create_sql = subprocess.check_output(table_cmd).decode()
                create_sql = create_sql.replace('\\n', '\n')
                with open(table_sql, "w") as f:
                    f.write(create_sql)
                backup_files.append(table_sql)
                
                # 导出数据
                table_tsv = self.clickhouse_backup_dir / f"{table}_{timestamp}.tsv"
                data_cmd = cmd + [f"SELECT * FROM {table} FORMAT TabSeparated"]
                with open(table_tsv, "w") as f:
                    subprocess.run(data_cmd, stdout=f, check=True)
                backup_files.append(table_tsv)
                
                logger.info(f"ClickHouse表 {table} 备份完成")
            
            # 压缩备份文件
            with tarfile.open(backup_tar, "w:gz") as tar:
                for file in backup_files:
                    tar.add(file, arcname=file.name)
                    file.unlink()  # 删除原始文件
                    
            logger.info(f"ClickHouse备份完成: {backup_tar}")
            
        except Exception as e:
            logger.error(f"ClickHouse备份失败: {e}")
    
    def _backup_postgres(self, backup_type: str):
        """备份PostgreSQL数据库"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = self.postgres_backup_dir / f"postgres_{backup_type}_{timestamp}.sql"
            
            # 使用pg_dump进行备份
            cmd = [
                "docker", "exec", "qiming_postgres",
                "pg_dump",
                f"--host={db_settings.postgres_host}",
                f"--port={db_settings.postgres_port}",
                f"--username={db_settings.postgres_user}",
                f"--dbname={db_settings.postgres_db}",
                "--verbose",
                "--clean",
                "--no-owner",
                "--no-privileges"
            ]
            
            # 设置环境变量
            env = os.environ.copy()
            env["PGPASSWORD"] = db_settings.postgres_password
            
            with open(backup_file, "w") as f:
                subprocess.run(cmd, stdout=f, env=env, check=True)
                
            logger.info(f"PostgreSQL备份完成: {backup_file}")
            
        except Exception as e:
            logger.error(f"PostgreSQL备份失败: {e}")
    
    def _cleanup_old_backups(self):
        """清理旧备份文件，只保留最新的备份"""
        try:
            # 清理ClickHouse备份，只保留最新的
            clickhouse_backups = list(self.clickhouse_backup_dir.glob("*.tar.gz"))
            if len(clickhouse_backups) > 1:
                # 按修改时间排序，保留最新的
                clickhouse_backups.sort(key=lambda x: x.stat().st_mtime)
                # 删除除最新外的所有备份
                for backup_file in clickhouse_backups[:-1]:
                    backup_file.unlink()
                    logger.info(f"删除旧ClickHouse备份: {backup_file.name}")
            
            # 清理PostgreSQL备份，只保留最新的
            postgres_backups = list(self.postgres_backup_dir.glob("*.sql"))
            if len(postgres_backups) > 1:
                # 按修改时间排序，保留最新的
                postgres_backups.sort(key=lambda x: x.stat().st_mtime)
                # 删除除最新外的所有备份
                for backup_file in postgres_backups[:-1]:
                    backup_file.unlink()
                    logger.info(f"删除旧PostgreSQL备份: {backup_file.name}")
                    
        except Exception as e:
            logger.error(f"清理旧备份失败: {e}")
    
    def get_backup_status(self) -> dict:
        """获取备份状态信息"""
        try:
            status = {
                "clickhouse_backups": [],
                "postgres_backups": [],
                "total_size": 0,
                "oldest_backup": None,
                "newest_backup": None
            }
            
            # 统计ClickHouse备份
            for backup_file in self.clickhouse_backup_dir.glob("*.tar.gz"):
                file_stat = backup_file.stat()
                status["clickhouse_backups"].append({
                    "name": backup_file.name,
                    "size": file_stat.st_size,
                    "modified": datetime.fromtimestamp(file_stat.st_mtime)
                })
                status["total_size"] += file_stat.st_size
            
            # 统计PostgreSQL备份
            for backup_file in self.postgres_backup_dir.glob("*.sql"):
                file_stat = backup_file.stat()
                status["postgres_backups"].append({
                    "name": backup_file.name,
                    "size": file_stat.st_size,
                    "modified": datetime.fromtimestamp(file_stat.st_mtime)
                })
                status["total_size"] += file_stat.st_size
            
            # 计算最早和最晚备份时间
            all_backups = status["clickhouse_backups"] + status["postgres_backups"]
            if all_backups:
                status["oldest_backup"] = min(backup["modified"] for backup in all_backups)
                status["newest_backup"] = max(backup["modified"] for backup in all_backups)
            
            return status
            
        except Exception as e:
            logger.error(f"获取备份状态失败: {e}")
            return {}

# 全局备份管理器实例
_backup_manager = None

def get_backup_manager() -> BackupManager:
    """获取备份管理器实例"""
    global _backup_manager
    if _backup_manager is None:
        _backup_manager = BackupManager()
    return _backup_manager 