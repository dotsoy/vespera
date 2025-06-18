#!/usr/bin/env python3
"""
将 capital_flow_daily 表的 stock_code 字段迁移为 ts_code，保留原有数据。
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from sqlalchemy import create_engine, text
from config.settings import db_settings

engine = create_engine(db_settings.postgres_url)

migration_sql = [
    # 1. 添加新字段
    "ALTER TABLE capital_flow_daily ADD COLUMN IF NOT EXISTS ts_code VARCHAR(10);",
    # 2. 用原有 stock_code 数据填充 ts_code
    "UPDATE capital_flow_daily SET ts_code = stock_code WHERE ts_code IS NULL;",
    # 3. 删除原有唯一索引（如果存在）
    # 先查索引名
    "DROP INDEX IF EXISTS capital_flow_daily_date_stock_code_key;",
    # 4. 删除 stock_code 字段
    "ALTER TABLE capital_flow_daily DROP COLUMN IF EXISTS stock_code;",
    # 5. 创建新的唯一索引
    "CREATE UNIQUE INDEX IF NOT EXISTS capital_flow_daily_date_ts_code_key ON capital_flow_daily(date, ts_code);"
]

if __name__ == "__main__":
    with engine.connect() as conn:
        for sql in migration_sql:
            try:
                print(f"执行: {sql}")
                conn.execute(text(sql))
            except Exception as e:
                print(f"执行失败: {e}")
        conn.commit()
    print("字段迁移完成！") 