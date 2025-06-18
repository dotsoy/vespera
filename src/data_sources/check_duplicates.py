from clickhouse_driver import Client as ClickHouseClient
import pandas as pd

# 连接到 ClickHouse
client = ClickHouseClient(host='localhost', port=9000, user='qiming_user', password='qiming_pass_2024', database='qiming_timeseries')

# 查询重复数据
query = """
SELECT ts_code, trade_date, COUNT(*) as count
FROM daily_quotes
GROUP BY ts_code, trade_date
HAVING COUNT(*) > 1
"""

result = client.execute(query)

# 将结果转换为 DataFrame
df = pd.DataFrame(result, columns=['ts_code', 'trade_date', 'count'])

# 打印重复数据
print(df) 