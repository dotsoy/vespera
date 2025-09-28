"""
测试数据库连接和基本功能
"""
import sys
import os
import time
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, '/workspaces/vespera')

def test_postgresql_connection():
    """测试PostgreSQL连接"""
    print("="*60)
    print("测试PostgreSQL连接")
    print("="*60)
    
    try:
        import psycopg2
        from sqlalchemy import create_engine, text
        
        # 从环境变量读取配置
        from dotenv import load_dotenv
        load_dotenv()
        
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'qiming_star')
        user = os.getenv('POSTGRES_USER', 'qiming_user')
        password = os.getenv('POSTGRES_PASSWORD', 'qiming_pass_2024')
        
        # 构建连接字符串
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        print(f"连接到: {host}:{port}/{database}")
        
        # 等待数据库启动
        max_retries = 30
        for i in range(max_retries):
            try:
                engine = create_engine(connection_string)
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT version()"))
                    version = result.fetchone()[0]
                    print(f"✅ PostgreSQL连接成功")
                    print(f"   版本: {version}")
                    
                    # 测试基本操作
                    conn.execute(text("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name VARCHAR(50))"))
                    conn.execute(text("INSERT INTO test_table (name) VALUES ('test') ON CONFLICT DO NOTHING"))
                    result = conn.execute(text("SELECT COUNT(*) FROM test_table"))
                    count = result.fetchone()[0]
                    print(f"   测试表记录数: {count}")
                    
                    conn.commit()
                    return True
                    
            except Exception as e:
                if i < max_retries - 1:
                    print(f"   等待PostgreSQL启动... ({i+1}/{max_retries})")
                    time.sleep(2)
                else:
                    print(f"❌ PostgreSQL连接失败: {e}")
                    return False
        
    except Exception as e:
        print(f"❌ PostgreSQL测试失败: {e}")
        return False

def test_redis_connection():
    """测试Redis连接"""
    print("\n" + "="*60)
    print("测试Redis连接")
    print("="*60)
    
    try:
        import redis
        
        # 从环境变量读取配置
        host = os.getenv('REDIS_HOST', 'localhost')
        port = int(os.getenv('REDIS_PORT', '6379'))
        password = os.getenv('REDIS_PASSWORD', 'qiming_redis_2024')
        
        print(f"连接到: {host}:{port}")
        
        # 等待Redis启动
        max_retries = 15
        for i in range(max_retries):
            try:
                r = redis.Redis(host=host, port=port, password=password, decode_responses=True)
                
                # 测试连接
                r.ping()
                print("✅ Redis连接成功")
                
                # 测试基本操作
                r.set('test_key', 'test_value')
                value = r.get('test_key')
                print(f"   测试读写: {value}")
                
                # 获取Redis信息
                info = r.info()
                print(f"   Redis版本: {info.get('redis_version', 'unknown')}")
                print(f"   内存使用: {info.get('used_memory_human', 'unknown')}")
                
                return True
                
            except Exception as e:
                if i < max_retries - 1:
                    print(f"   等待Redis启动... ({i+1}/{max_retries})")
                    time.sleep(2)
                else:
                    print(f"❌ Redis连接失败: {e}")
                    return False
        
    except Exception as e:
        print(f"❌ Redis测试失败: {e}")
        return False

def test_clickhouse_connection():
    """测试ClickHouse连接"""
    print("\n" + "="*60)
    print("测试ClickHouse连接")
    print("="*60)
    
    try:
        import requests
        
        # 从环境变量读取配置
        host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        port = os.getenv('CLICKHOUSE_PORT', '8123')
        
        print(f"连接到: {host}:{port}")
        
        # 等待ClickHouse启动
        max_retries = 30
        for i in range(max_retries):
            try:
                # 测试HTTP接口
                response = requests.get(f"http://{host}:{port}/ping", timeout=5)
                
                if response.status_code == 200:
                    print("✅ ClickHouse连接成功")
                    
                    # 测试查询
                    query_response = requests.post(
                        f"http://{host}:{port}/",
                        data="SELECT version()",
                        timeout=5
                    )
                    
                    if query_response.status_code == 200:
                        version = query_response.text.strip()
                        print(f"   版本: {version}")
                    
                    return True
                else:
                    raise Exception(f"HTTP状态码: {response.status_code}")
                    
            except Exception as e:
                if i < max_retries - 1:
                    print(f"   等待ClickHouse启动... ({i+1}/{max_retries})")
                    time.sleep(3)
                else:
                    print(f"❌ ClickHouse连接失败: {e}")
                    return False
        
    except Exception as e:
        print(f"❌ ClickHouse测试失败: {e}")
        return False

def test_database_optimization():
    """测试数据库优化功能"""
    print("\n" + "="*60)
    print("测试数据库优化功能")
    print("="*60)
    
    try:
        # 检查优化脚本是否存在
        optimizer_script = "/workspaces/vespera/scripts/optimize_database_indexes.py"
        sql_script = "/workspaces/vespera/sql/create_optimized_indexes.sql"
        
        if os.path.exists(optimizer_script):
            print("✅ 数据库优化脚本存在")
        else:
            print("❌ 数据库优化脚本不存在")
            return False
        
        if os.path.exists(sql_script):
            print("✅ SQL优化脚本存在")
            
            # 读取SQL脚本内容
            with open(sql_script, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # 统计索引数量
            index_count = sql_content.count('CREATE INDEX')
            print(f"   包含 {index_count} 个索引定义")
            
        else:
            print("❌ SQL优化脚本不存在")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ 数据库优化测试失败: {e}")
        return False

def create_database_test_script():
    """创建数据库测试脚本"""
    print("\n" + "="*60)
    print("创建数据库测试脚本")
    print("="*60)
    
    test_script = '''
import asyncio
import sys
import os

sys.path.insert(0, '/workspaces/vespera')

async def test_database_operations():
    """测试数据库操作"""
    from sqlalchemy import create_engine, text
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # PostgreSQL连接
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'qiming_star')
    user = os.getenv('POSTGRES_USER', 'qiming_user')
    password = os.getenv('POSTGRES_PASSWORD', 'qiming_pass_2024')
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    try:
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # 创建示例表
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sample_stocks (
                    id SERIAL PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL,
                    name VARCHAR(100),
                    industry VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # 插入示例数据
            conn.execute(text("""
                INSERT INTO sample_stocks (ts_code, name, industry) 
                VALUES 
                    ('000001.SZ', '平安银行', '银行'),
                    ('000002.SZ', '万科A', '房地产'),
                    ('600000.SH', '浦发银行', '银行')
                ON CONFLICT DO NOTHING
            """))
            
            # 查询数据
            result = conn.execute(text("SELECT * FROM sample_stocks LIMIT 5"))
            rows = result.fetchall()
            
            print("示例股票数据:")
            for row in rows:
                print(f"  {row.ts_code}: {row.name} ({row.industry})")
            
            conn.commit()
            print("✅ 数据库操作测试成功")
            
    except Exception as e:
        print(f"❌ 数据库操作测试失败: {e}")

if __name__ == "__main__":
    asyncio.run(test_database_operations())
'''
    
    script_path = "/workspaces/vespera/test_database_operations.py"
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(test_script)
    
    print(f"✅ 数据库测试脚本已创建: {script_path}")
    return script_path

def main():
    """主测试函数"""
    print("🗄️ Vespera 数据库连接测试")
    print(f"测试时间: {datetime.now()}")
    
    # 等待Docker服务启动
    print("\n⏳ 等待Docker服务启动...")
    time.sleep(10)
    
    test_results = {}
    
    # 执行数据库连接测试
    test_results["postgresql"] = test_postgresql_connection()
    test_results["redis"] = test_redis_connection()
    test_results["clickhouse"] = test_clickhouse_connection()
    test_results["optimization"] = test_database_optimization()
    
    # 创建测试脚本
    script_path = create_database_test_script()
    
    # 统计结果
    total_tests = len(test_results)
    passed_tests = sum(1 for result in test_results.values() if result)
    success_rate = passed_tests / total_tests
    
    print("\n" + "="*60)
    print("数据库测试结果摘要")
    print("="*60)
    print(f"总测试数: {total_tests}")
    print(f"通过测试: {passed_tests}")
    print(f"成功率: {success_rate:.1%}")
    
    if success_rate >= 0.75:
        print("🎉 数据库连接正常!")
        print("\n📋 数据库使用指南:")
        print("1. 运行数据库操作测试:")
        print(f"   python {script_path}")
        print("\n2. 执行数据库优化:")
        print("   python scripts/optimize_database_indexes.py")
        print("\n3. 查看数据库状态:")
        print("   docker-compose ps")
        print("   docker-compose logs postgres")
    else:
        print("⚠️ 部分数据库连接存在问题")
        print("建议检查Docker服务状态和配置")
    
    return test_results

if __name__ == "__main__":
    main()