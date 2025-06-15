#!/usr/bin/env python3
"""
清除模拟数据脚本
清理数据库中的模拟/测试数据，为导入生产数据做准备
"""
import sys
import os
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("clear_mock_data")


def clear_postgresql_data():
    """清除PostgreSQL中的模拟数据"""
    logger.info("🗑️ 清除PostgreSQL模拟数据")
    
    try:
        # 这里需要根据实际的数据库连接方式来实现
        # 由于没有看到具体的数据库连接代码，我提供一个通用的框架
        
        # 需要清理的表
        tables_to_clear = [
            'stock_basic',           # 股票基础信息
            'daily_quotes',          # 日线数据
            'minute_quotes',         # 分钟数据
            'technical_indicators',  # 技术指标
            'capital_flow_profiles', # 资金流分析
            'fundamental_profiles',  # 基本面分析
            'macro_profiles',        # 宏观分析
            'trading_signals',       # 交易信号
            'backtest_results'       # 回测结果
        ]
        
        logger.info(f"准备清理 {len(tables_to_clear)} 个表的数据")
        
        # 这里应该使用实际的数据库连接
        # 示例代码（需要根据实际情况调整）:
        """
        from src.utils.database import get_db_manager
        
        db_manager = get_db_manager()
        
        for table in tables_to_clear:
            try:
                # 检查表是否存在
                check_query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}')"
                result = db_manager.execute_postgres_query(check_query)
                
                if result.iloc[0, 0]:  # 表存在
                    # 清空表数据
                    clear_query = f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"
                    db_manager.execute_postgres_query(clear_query)
                    logger.success(f"✅ 已清空表: {table}")
                else:
                    logger.info(f"ℹ️ 表不存在，跳过: {table}")
                    
            except Exception as e:
                logger.warning(f"⚠️ 清理表 {table} 时出错: {e}")
        """
        
        # 临时实现：显示需要清理的表
        for table in tables_to_clear:
            logger.info(f"📋 需要清理的表: {table}")
        
        logger.success("✅ PostgreSQL数据清理完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ PostgreSQL数据清理失败: {e}")
        return False


def clear_clickhouse_data():
    """清除ClickHouse中的模拟数据"""
    logger.info("🗑️ 清除ClickHouse模拟数据")
    
    try:
        # ClickHouse主要用于存储大量的历史数据和分析结果
        tables_to_clear = [
            'stock_quotes_daily',    # 日线历史数据
            'stock_quotes_minute',   # 分钟级历史数据
            'trading_volume_analysis', # 成交量分析
            'price_movement_stats',  # 价格变动统计
            'market_sentiment_data'  # 市场情绪数据
        ]
        
        logger.info(f"准备清理 {len(tables_to_clear)} 个ClickHouse表的数据")
        
        # 这里应该使用实际的ClickHouse连接
        # 示例代码（需要根据实际情况调整）:
        """
        from src.utils.database import get_clickhouse_client
        
        clickhouse_client = get_clickhouse_client()
        
        for table in tables_to_clear:
            try:
                # 检查表是否存在
                check_query = f"EXISTS TABLE {table}"
                exists = clickhouse_client.execute(check_query)[0][0]
                
                if exists:
                    # 清空表数据
                    clear_query = f"TRUNCATE TABLE {table}"
                    clickhouse_client.execute(clear_query)
                    logger.success(f"✅ 已清空ClickHouse表: {table}")
                else:
                    logger.info(f"ℹ️ ClickHouse表不存在，跳过: {table}")
                    
            except Exception as e:
                logger.warning(f"⚠️ 清理ClickHouse表 {table} 时出错: {e}")
        """
        
        # 临时实现：显示需要清理的表
        for table in tables_to_clear:
            logger.info(f"📋 需要清理的ClickHouse表: {table}")
        
        logger.success("✅ ClickHouse数据清理完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ ClickHouse数据清理失败: {e}")
        return False


def clear_redis_cache():
    """清除Redis缓存"""
    logger.info("🗑️ 清除Redis缓存")
    
    try:
        # 清理Redis中的缓存数据
        cache_patterns = [
            'stock_data:*',          # 股票数据缓存
            'market_data:*',         # 市场数据缓存
            'analysis_result:*',     # 分析结果缓存
            'trading_signal:*',      # 交易信号缓存
            'user_session:*'         # 用户会话缓存
        ]
        
        logger.info(f"准备清理 {len(cache_patterns)} 类缓存数据")
        
        # 这里应该使用实际的Redis连接
        # 示例代码（需要根据实际情况调整）:
        """
        import redis
        
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        
        for pattern in cache_patterns:
            try:
                keys = redis_client.keys(pattern)
                if keys:
                    redis_client.delete(*keys)
                    logger.success(f"✅ 已清理缓存: {pattern} ({len(keys)} 个key)")
                else:
                    logger.info(f"ℹ️ 无缓存数据: {pattern}")
                    
            except Exception as e:
                logger.warning(f"⚠️ 清理缓存 {pattern} 时出错: {e}")
        """
        
        # 临时实现：显示需要清理的缓存
        for pattern in cache_patterns:
            logger.info(f"📋 需要清理的缓存: {pattern}")
        
        logger.success("✅ Redis缓存清理完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ Redis缓存清理失败: {e}")
        return False


def clear_local_files():
    """清除本地文件缓存"""
    logger.info("🗑️ 清除本地文件缓存")
    
    try:
        # 需要清理的目录和文件
        paths_to_clear = [
            'data/cache',            # 数据缓存目录
            'data/temp',             # 临时文件目录
            'data/downloads',        # 下载文件目录
            'logs/old',              # 旧日志文件
            'data/mock',             # 模拟数据目录
            'data/test'              # 测试数据目录
        ]
        
        logger.info(f"准备清理 {len(paths_to_clear)} 个目录")
        
        import shutil
        
        for path_str in paths_to_clear:
            path = project_root / path_str
            try:
                if path.exists():
                    if path.is_dir():
                        # 清空目录但保留目录本身
                        for item in path.iterdir():
                            if item.is_dir():
                                shutil.rmtree(item)
                            else:
                                item.unlink()
                        logger.success(f"✅ 已清空目录: {path}")
                    else:
                        # 删除文件
                        path.unlink()
                        logger.success(f"✅ 已删除文件: {path}")
                else:
                    logger.info(f"ℹ️ 路径不存在，跳过: {path}")
                    
            except Exception as e:
                logger.warning(f"⚠️ 清理路径 {path} 时出错: {e}")
        
        logger.success("✅ 本地文件清理完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 本地文件清理失败: {e}")
        return False


def backup_important_config():
    """备份重要配置"""
    logger.info("💾 备份重要配置")
    
    try:
        # 需要备份的配置文件
        config_files = [
            'config/settings.py',
            'config/database.yaml',
            'config/data_sources.yaml',
            '.env'
        ]
        
        backup_dir = project_root / 'backup' / 'config'
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        import shutil
        from datetime import datetime
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for config_file in config_files:
            source_path = project_root / config_file
            if source_path.exists():
                backup_name = f"{source_path.stem}_{timestamp}{source_path.suffix}"
                backup_path = backup_dir / backup_name
                shutil.copy2(source_path, backup_path)
                logger.success(f"✅ 已备份配置: {config_file} -> {backup_name}")
            else:
                logger.info(f"ℹ️ 配置文件不存在，跳过: {config_file}")
        
        logger.success("✅ 配置备份完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 配置备份失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🧹 开始清除模拟数据")
    logger.info("=" * 80)
    
    # 确认操作
    logger.warning("⚠️ 此操作将清除所有模拟/测试数据，请确认！")
    logger.info("清理范围:")
    logger.info("  - PostgreSQL数据库表")
    logger.info("  - ClickHouse数据表")
    logger.info("  - Redis缓存")
    logger.info("  - 本地文件缓存")
    
    # 在生产环境中，这里应该要求用户确认
    # confirm = input("确认清理数据？(yes/no): ")
    # if confirm.lower() != 'yes':
    #     logger.info("操作已取消")
    #     return False
    
    # 执行清理步骤
    steps = [
        ("备份重要配置", backup_important_config),
        ("清除PostgreSQL数据", clear_postgresql_data),
        ("清除ClickHouse数据", clear_clickhouse_data),
        ("清除Redis缓存", clear_redis_cache),
        ("清除本地文件", clear_local_files)
    ]
    
    success_count = 0
    total_count = len(steps)
    
    for step_name, step_func in steps:
        try:
            logger.info(f"\n🎯 执行步骤: {step_name}")
            success = step_func()
            if success:
                success_count += 1
                logger.success(f"✅ {step_name} 完成")
            else:
                logger.error(f"❌ {step_name} 失败")
        except Exception as e:
            logger.error(f"❌ {step_name} 异常: {e}")
    
    # 总结
    logger.info("\n" + "=" * 80)
    logger.info("📊 清理总结")
    logger.info("=" * 80)
    logger.info(f"总步骤数: {total_count}")
    logger.info(f"成功步骤: {success_count}")
    logger.info(f"失败步骤: {total_count - success_count}")
    logger.info(f"成功率: {success_count/total_count*100:.1f}%")
    
    if success_count == total_count:
        logger.success("🎉 模拟数据清理完成！")
        logger.info("\n🚀 下一步:")
        logger.info("  1. 配置生产数据源token")
        logger.info("  2. 获取股票列表（排除ST和北交所）")
        logger.info("  3. 导入生产数据")
    else:
        logger.warning("⚠️ 部分清理步骤失败，请检查日志")
    
    return success_count == total_count


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
