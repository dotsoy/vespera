#!/usr/bin/env python3
"""
自动恢复最新的PostgreSQL和ClickHouse备份
"""
import os
import sys
from pathlib import Path
import subprocess
from datetime import datetime
from loguru import logger

project_root = Path(__file__).parent.parent
backup_dir = project_root / "backups"
postgres_dir = backup_dir / "postgres"
clickhouse_dir = backup_dir / "clickhouse"

# 容器名
POSTGRES_CONTAINER = "qiming_postgres"
CLICKHOUSE_CONTAINER = "vespera-clickhouse-1"

# 数据库参数（可根据config.settings自动读取）
POSTGRES_USER = os.environ.get("POSTGRES_USER", "qiming_user")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "qiming_star")


def find_latest_file(directory: Path, suffix: str) -> Path:
    files = list(directory.glob(f"*{suffix}"))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def restore_postgres():
    latest_sql = find_latest_file(postgres_dir, ".sql")
    if not latest_sql:
        logger.warning("未找到PostgreSQL备份文件，跳过恢复")
        return
    logger.info(f"正在恢复PostgreSQL备份: {latest_sql.name}")
    cmd = [
        "docker", "exec", "-i", POSTGRES_CONTAINER,
        "psql", "-U", POSTGRES_USER, POSTGRES_DB
    ]
    with open(latest_sql, "rb") as f:
        proc = subprocess.run(cmd, stdin=f)
        if proc.returncode == 0:
            logger.success("PostgreSQL恢复完成")
        else:
            logger.error("PostgreSQL恢复失败")


def restore_clickhouse():
    latest_tar = find_latest_file(clickhouse_dir, ".tar.gz")
    if not latest_tar:
        logger.warning("未找到ClickHouse备份文件，跳过恢复")
        return
    logger.info(f"正在恢复ClickHouse备份: {latest_tar.name}")
    # 解压到临时目录
    tmp_dir = clickhouse_dir / "_tmp_restore"
    if tmp_dir.exists():
        for f in tmp_dir.iterdir():
            f.unlink()
        tmp_dir.rmdir()
    tmp_dir.mkdir(parents=True, exist_ok=True)
    import tarfile
    with tarfile.open(latest_tar, "r:gz") as tar:
        tar.extractall(tmp_dir)
    # 先恢复建表，再恢复数据
    for sql_file in sorted(tmp_dir.glob("*.sql")):
        logger.info(f"执行建表: {sql_file.name}")
        cmd = [
            "docker", "exec", "-i", CLICKHOUSE_CONTAINER,
            "clickhouse-client"
        ]
        with open(sql_file, "rb") as f:
            subprocess.run(cmd, stdin=f)
    for tsv_file in sorted(tmp_dir.glob("*.tsv")):
        table = tsv_file.name.split("_")[0]
        logger.info(f"导入数据: {tsv_file.name} -> {table}")
        cmd = [
            "docker", "exec", "-i", CLICKHOUSE_CONTAINER,
            "clickhouse-client", "--query", f"INSERT INTO {table} FORMAT TabSeparated"
        ]
        with open(tsv_file, "rb") as f:
            subprocess.run(cmd, stdin=f)
    logger.success("ClickHouse恢复完成")
    # 清理临时目录
    for f in tmp_dir.iterdir():
        f.unlink()
    tmp_dir.rmdir()


def main():
    logger.info("开始自动恢复数据库...")
    restore_postgres()
    restore_clickhouse()
    logger.info("数据库恢复流程结束")

if __name__ == "__main__":
    main() 