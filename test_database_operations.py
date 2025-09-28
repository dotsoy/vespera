
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
