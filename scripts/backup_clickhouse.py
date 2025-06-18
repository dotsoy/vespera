#!/usr/bin/env python3
"""
ClickHouse 数据库备份脚本（兼容恢复）
"""
import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path
import tarfile
import shutil
from loguru import logger

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.settings import db_settings

def backup_clickhouse(tables: list = None) -> bool:
    """
    备份 ClickHouse 数据库（每表建表语句和数据分开导出）
    """
    try:
        backup_dir = project_root / "backups" / "clickhouse"
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_tar = backup_dir / f"clickhouse_backup_{timestamp}.tar.gz"

        cmd = [
            "docker", "exec", "qiming_clickhouse",
            "clickhouse-client",
            f"--host={db_settings.clickhouse_host}",
            f"--port={db_settings.clickhouse_port}",
            f"--user={db_settings.clickhouse_user}",
            f"--password={db_settings.clickhouse_password}",
            f"--database={db_settings.clickhouse_db}",
            "--query"
        ]

        # 获取所有表名
        if not tables:
            tables_cmd = cmd + ["SHOW TABLES"]
            tables = subprocess.check_output(tables_cmd).decode().strip().split("\n")

        backup_files = []
        for table in tables:
            if not table:
                continue
            # 1. 导出建表语句（确保为原始多行文本）
            table_sql = backup_dir / f"{table}_{timestamp}.sql"
            table_cmd = cmd + [f"SHOW CREATE TABLE {table}"]
            create_sql = subprocess.check_output(table_cmd).decode()
            # 替换所有 \n 为真实换行
            create_sql = create_sql.replace('\\n', '\n')
            with open(table_sql, "w") as f:
                f.write(create_sql)
            backup_files.append(table_sql)
            # 2. 导出数据（TabSeparated 格式）
            table_tsv = backup_dir / f"{table}_{timestamp}.tsv"
            data_cmd = cmd + [f"SELECT * FROM {table} FORMAT TabSeparated"]
            with open(table_tsv, "w") as f:
                subprocess.run(data_cmd, stdout=f, check=True)
            backup_files.append(table_tsv)
            logger.info(f"表 {table} 备份完成")

        # 压缩所有备份文件
        with tarfile.open(backup_tar, "w:gz") as tar:
            for file in backup_files:
                tar.add(file, arcname=file.name)
                file.unlink()  # 删除原始文件
        logger.info(f"备份完成: {backup_tar}")
        return True
    except Exception as e:
        logger.error(f"备份 ClickHouse 数据库失败: {str(e)}")
        return False

def cleanup_old_backups(backup_dir: Path, days: int = 30):
    """
    清理旧的备份文件
    
    Args:
        backup_dir: 备份目录
        days: 保留天数
    """
    try:
        current_time = datetime.now()
        for backup_file in backup_dir.glob("*.gz"):
            file_time = datetime.fromtimestamp(backup_file.stat().st_mtime)
            if (current_time - file_time).days > days:
                backup_file.unlink()
                logger.info(f"删除旧备份文件: {backup_file}")
    except Exception as e:
        logger.error(f"清理旧备份文件失败: {e}")

if __name__ == "__main__":
    logger.info("开始备份 ClickHouse 数据库...")
    if backup_clickhouse():
        logger.info("✅ 备份成功")
    else:
        logger.error("❌ 备份失败")
        sys.exit(1) 