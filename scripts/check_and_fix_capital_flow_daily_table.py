#!/usr/bin/env python3
"""
自动检测并修复 capital_flow_daily 表结构，确保 ts_code 字段存在且唯一索引为 (date, ts_code)
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from sqlalchemy import create_engine, text
from config.settings import db_settings

engine = create_engine(db_settings.postgres_url)

check_column_sql = """
SELECT column_name FROM information_schema.columns WHERE table_name='capital_flow_daily' AND column_name='ts_code';
"""
add_column_sql = "ALTER TABLE capital_flow_daily ADD COLUMN ts_code VARCHAR(10);"

check_date_column_sql = """
SELECT column_name FROM information_schema.columns WHERE table_name='capital_flow_daily' AND column_name IN ('date', 'trade_date');
"""

check_index_sql = """
SELECT indexname FROM pg_indexes WHERE tablename='capital_flow_daily' AND indexdef LIKE '%(date, ts_code)%';
"""

def main():
    with engine.connect() as conn:
        # 检查 ts_code 字段
        result = conn.execute(text(check_column_sql)).fetchone()
        if not result:
            print("[修复] ts_code 字段不存在，正在添加...")
            conn.execute(text(add_column_sql))
            print("[完成] ts_code 字段已添加。请手动填充数据！")
        else:
            print("[OK] ts_code 字段已存在。")
        
        # 检查日期字段
        date_col = conn.execute(text(check_date_column_sql)).fetchone()
        if not date_col:
            print("[错误] 未找到日期字段（date 或 trade_date），请检查表结构！")
            return
        date_column = date_col[0]
        print(f"[OK] 日期字段为: {date_column}")
        
        # 检查唯一索引
        idx = conn.execute(text(check_index_sql)).fetchone()
        if not idx:
            print(f"[修复] 唯一索引 ({date_column}, ts_code) 不存在，正在创建...")
            create_index_sql = f"CREATE UNIQUE INDEX IF NOT EXISTS capital_flow_daily_{date_column}_ts_code_key ON capital_flow_daily({date_column}, ts_code);"
            conn.execute(text(create_index_sql))
            print("[完成] 唯一索引已创建。")
        else:
            print("[OK] 唯一索引已存在。")
        conn.commit()
    print("表结构检测与修复完成！")

if __name__ == "__main__":
    main()

# 检查其他问题
def check_other_issues():
    with engine.connect() as conn:
        # 检查表是否存在
        table_exists = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='capital_flow_daily');")).fetchone()[0]
        if not table_exists:
            print("[错误] 表 capital_flow_daily 不存在！")
            return
        
        # 检查字段类型
        column_types = conn.execute(text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='capital_flow_daily';")).fetchall()
        print("[检查] 字段类型:")
        for col in column_types:
            print(f"  {col[0]}: {col[1]}")
        
        # 检查数据完整性
        data_count = conn.execute(text("SELECT COUNT(*) FROM capital_flow_daily;")).fetchone()[0]
        print(f"[检查] 数据条数: {data_count}")
        
        # 检查 ts_code 是否为空
        null_ts_code = conn.execute(text("SELECT COUNT(*) FROM capital_flow_daily WHERE ts_code IS NULL;")).fetchone()[0]
        if null_ts_code > 0:
            print(f"[警告] ts_code 字段有 {null_ts_code} 条记录为空！")
        
        # 检查日期字段是否为空
        date_column = conn.execute(text(check_date_column_sql)).fetchone()[0]
        null_date = conn.execute(text(f"SELECT COUNT(*) FROM capital_flow_daily WHERE {date_column} IS NULL;")).fetchone()[0]
        if null_date > 0:
            print(f"[警告] {date_column} 字段有 {null_date} 条记录为空！")
        
        # 检查重复记录
        duplicate_records = conn.execute(text(f"SELECT {date_column}, ts_code, COUNT(*) FROM capital_flow_daily GROUP BY {date_column}, ts_code HAVING COUNT(*) > 1;")).fetchall()
        if duplicate_records:
            print("[警告] 存在重复记录:")
            for record in duplicate_records:
                print(f"  {record[0]}, {record[1]}: {record[2]} 条")
        
        print("其他问题检查完成！")

if __name__ == "__main__":
    main()
    check_other_issues()

# 检查 total_amount 字段
def check_total_amount():
    with engine.connect() as conn:
        # 检查 total_amount 字段是否存在
        total_amount_exists = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name='capital_flow_daily' AND column_name='total_amount');")).fetchone()[0]
        if not total_amount_exists:
            print("[错误] total_amount 字段不存在！")
            return
        
        # 检查 total_amount 字段类型
        total_amount_type = conn.execute(text("SELECT data_type FROM information_schema.columns WHERE table_name='capital_flow_daily' AND column_name='total_amount';")).fetchone()[0]
        print(f"[检查] total_amount 字段类型: {total_amount_type}")
        
        # 检查 total_amount 是否为空
        null_total_amount = conn.execute(text("SELECT COUNT(*) FROM capital_flow_daily WHERE total_amount IS NULL;")).fetchone()[0]
        if null_total_amount > 0:
            print(f"[警告] total_amount 字段有 {null_total_amount} 条记录为空！")
        
        print("total_amount 字段检查完成！")

if __name__ == "__main__":
    main()
    check_other_issues()
    check_total_amount()

# 修复 total_amount 字段
def fix_total_amount():
    with engine.connect() as conn:
        # 检查 total_amount 字段是否存在
        total_amount_exists = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name='capital_flow_daily' AND column_name='total_amount');")).fetchone()[0]
        if not total_amount_exists:
            print("[修复] total_amount 字段不存在，正在添加...")
            conn.execute(text("ALTER TABLE capital_flow_daily ADD COLUMN total_amount DECIMAL(20, 2);"))
            print("[完成] total_amount 字段已添加。请手动填充数据！")
        else:
            print("[OK] total_amount 字段已存在。")
        conn.commit()
    print("total_amount 字段修复完成！")

if __name__ == "__main__":
    main()
    check_other_issues()
    check_total_amount()
    fix_total_amount() 