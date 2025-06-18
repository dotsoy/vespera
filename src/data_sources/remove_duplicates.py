from clickhouse_driver import Client as ClickHouseClient

# 连接到 ClickHouse
client = ClickHouseClient(host='localhost', port=9000, user='qiming_user', password='qiming_pass_2024', database='qiming_timeseries')

# 1. 找出所有重复的ts_code和trade_date组合
query_duplicates = '''
SELECT ts_code, trade_date
FROM daily_quotes
GROUP BY ts_code, trade_date
HAVING COUNT(*) > 1
'''
duplicates = client.execute(query_duplicates)

# 2. 对每个重复组合，保留最小的id（或最早插入的一条），删除其它
# 假设没有自增id，则只能用tuple()的row_number()，ClickHouse 22.8+支持
for ts_code, trade_date in duplicates:
    # 查询所有重复行的rowid
    rows = client.execute(f'''
        SELECT * FROM daily_quotes
        WHERE ts_code = %(ts_code)s AND trade_date = %(trade_date)s
        ORDER BY trade_date
    ''', {'ts_code': ts_code, 'trade_date': trade_date})
    # 保留第一条，删除其它
    if len(rows) > 1:
        # 生成所有字段名
        columns = [desc[0] for desc in client.execute('DESCRIBE TABLE daily_quotes')]
        # 生成唯一条件
        keep = rows[0]
        for row in rows[1:]:
            # 构造where条件
            where = ' AND '.join([f"{col} = %({col})s" for col in columns])
            params = dict(zip(columns, row))
            client.execute(f"ALTER TABLE daily_quotes DELETE WHERE {where}", params)
        print(f"已去重: {ts_code} {trade_date}")

print("去重完成！") 