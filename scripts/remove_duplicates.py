#!/usr/bin/env python3
"""
ClickHouse daily_quotes 全表去重脚本：
1. 导出去重后数据
2. 清空表
3. 导入去重后数据
"""
import subprocess
import os
from pathlib import Path

CLICKHOUSE_CONTAINER = "vespera-clickhouse-1"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = "9000"
CLICKHOUSE_USER = "qiming_user"
CLICKHOUSE_PASSWORD = "qiming_pass_2024"
CLICKHOUSE_DB = "qiming_timeseries"
TABLE = "daily_quotes"
DEDUP_FILE = "dedup_daily_quotes.tsv"

# 1. 导出去重后数据（以所有字段分组，保留唯一行）
print("[1] 正在导出去重后数据...")
export_cmd = [
    "docker", "exec", "-i", CLICKHOUSE_CONTAINER,
    "clickhouse-client",
    f"--host={CLICKHOUSE_HOST}",
    f"--port={CLICKHOUSE_PORT}",
    f"--user={CLICKHOUSE_USER}",
    f"--password={CLICKHOUSE_PASSWORD}",
    f"--database={CLICKHOUSE_DB}",
    "--query",
    f"SELECT * FROM {TABLE} GROUP BY * FORMAT TabSeparated"
]
with open(DEDUP_FILE, "w") as f:
    subprocess.run(export_cmd, stdout=f, check=True)
print(f"[1] 导出完成: {DEDUP_FILE}")

# 2. 清空表
print("[2] 正在清空表...")
truncate_cmd = [
    "docker", "exec", "-i", CLICKHOUSE_CONTAINER,
    "clickhouse-client",
    f"--host={CLICKHOUSE_HOST}",
    f"--port={CLICKHOUSE_PORT}",
    f"--user={CLICKHOUSE_USER}",
    f"--password={CLICKHOUSE_PASSWORD}",
    f"--database={CLICKHOUSE_DB}",
    "--query",
    f"TRUNCATE TABLE {TABLE}"
]
subprocess.run(truncate_cmd, check=True)
print("[2] 表已清空")

# 3. 导入去重后数据
print("[3] 正在导入去重后数据...")
import_cmd = [
    "docker", "exec", "-i", CLICKHOUSE_CONTAINER,
    "clickhouse-client",
    f"--host={CLICKHOUSE_HOST}",
    f"--port={CLICKHOUSE_PORT}",
    f"--user={CLICKHOUSE_USER}",
    f"--password={CLICKHOUSE_PASSWORD}",
    f"--database={CLICKHOUSE_DB}",
    "--query",
    f"INSERT INTO {TABLE} FORMAT TabSeparated"
]
with open(DEDUP_FILE, "r") as f:
    subprocess.run(import_cmd, stdin=f, check=True)
print("[3] 导入完成！全表去重已完成。") 