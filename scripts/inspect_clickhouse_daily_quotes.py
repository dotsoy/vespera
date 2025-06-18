#!/usr/bin/env python3
"""
统计 ClickHouse daily_quotes 表中每个 ts_code 的最小日期、最大日期和记录数
"""
import subprocess
from tabulate import tabulate

CLICKHOUSE_CONTAINER = "vespera-clickhouse-1"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = "9000"
CLICKHOUSE_USER = "qiming_user"
CLICKHOUSE_PASSWORD = "qiming_pass_2024"
CLICKHOUSE_DB = "qiming_timeseries"

query = (
    "SELECT ts_code, min(trade_date) as min_date, max(trade_date) as max_date, count(*) as cnt "
    "FROM daily_quotes GROUP BY ts_code ORDER BY ts_code"
)

cmd = [
    "docker", "exec", "-i", CLICKHOUSE_CONTAINER,
    "clickhouse-client",
    f"--host={CLICKHOUSE_HOST}",
    f"--port={CLICKHOUSE_PORT}",
    f"--user={CLICKHOUSE_USER}",
    f"--password={CLICKHOUSE_PASSWORD}",
    f"--database={CLICKHOUSE_DB}",
    "--query", query
]

result = subprocess.check_output(cmd).decode("utf-8")
rows = [line.split('\t') for line in result.strip().split('\n') if line.strip()]
print(tabulate(rows, headers=["ts_code", "min_date", "max_date", "count"], tablefmt="grid")) 