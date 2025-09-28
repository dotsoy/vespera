"""
数据库索引优化脚本
分析查询模式并创建优化索引
"""
import asyncio
import time
from typing import List, Dict, Any, Tuple
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.utils.logger import get_logger
from src.utils.exceptions import DatabaseException, VesperaException

logger = get_logger("database_optimizer")


class DatabaseIndexOptimizer:
    """数据库索引优化器"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)
        
        # 常见查询模式和对应的索引建议
        self.index_recommendations = {
            "daily_quotes": [
                {
                    "name": "idx_daily_quotes_ts_code_date",
                    "columns": ["ts_code", "trade_date"],
                    "type": "btree",
                    "description": "股票代码和交易日期复合索引，用于时间序列查询"
                },
                {
                    "name": "idx_daily_quotes_date",
                    "columns": ["trade_date"],
                    "type": "btree",
                    "description": "交易日期索引，用于按日期范围查询"
                },
                {
                    "name": "idx_daily_quotes_volume",
                    "columns": ["volume"],
                    "type": "btree",
                    "description": "成交量索引，用于成交量筛选"
                },
                {
                    "name": "idx_daily_quotes_close_price",
                    "columns": ["close"],
                    "type": "btree",
                    "description": "收盘价索引，用于价格筛选"
                }
            ],
            "capital_flow_daily": [
                {
                    "name": "idx_capital_flow_ts_code_date",
                    "columns": ["ts_code", "trade_date"],
                    "type": "btree",
                    "description": "资金流向股票代码和日期复合索引"
                },
                {
                    "name": "idx_capital_flow_net_amount",
                    "columns": ["net_amount"],
                    "type": "btree",
                    "description": "净流入金额索引，用于资金流向分析"
                },
                {
                    "name": "idx_capital_flow_main_net_inflow",
                    "columns": ["main_net_inflow"],
                    "type": "btree",
                    "description": "主力净流入索引"
                }
            ],
            "stock_basic": [
                {
                    "name": "idx_stock_basic_ts_code",
                    "columns": ["ts_code"],
                    "type": "btree",
                    "description": "股票代码唯一索引"
                },
                {
                    "name": "idx_stock_basic_industry",
                    "columns": ["industry"],
                    "type": "btree",
                    "description": "行业索引，用于行业分析"
                },
                {
                    "name": "idx_stock_basic_market",
                    "columns": ["market"],
                    "type": "btree",
                    "description": "市场索引（主板、创业板等）"
                },
                {
                    "name": "idx_stock_basic_list_status",
                    "columns": ["list_status"],
                    "type": "btree",
                    "description": "上市状态索引"
                }
            ],
            "technical_indicators": [
                {
                    "name": "idx_technical_ts_code_date",
                    "columns": ["ts_code", "trade_date"],
                    "type": "btree",
                    "description": "技术指标股票代码和日期复合索引"
                },
                {
                    "name": "idx_technical_rsi",
                    "columns": ["rsi"],
                    "type": "btree",
                    "description": "RSI指标索引"
                },
                {
                    "name": "idx_technical_macd",
                    "columns": ["macd"],
                    "type": "btree",
                    "description": "MACD指标索引"
                }
            ],
            "strategy_signals": [
                {
                    "name": "idx_signals_ts_code_date",
                    "columns": ["ts_code", "signal_date"],
                    "type": "btree",
                    "description": "策略信号股票代码和日期复合索引"
                },
                {
                    "name": "idx_signals_score",
                    "columns": ["conviction_score"],
                    "type": "btree",
                    "description": "信号确定性得分索引"
                },
                {
                    "name": "idx_signals_class",
                    "columns": ["signal_class"],
                    "type": "btree",
                    "description": "信号等级索引（S级、A级）"
                }
            ]
        }
    
    async def analyze_database_performance(self) -> Dict[str, Any]:
        """分析数据库性能"""
        logger.info("开始分析数据库性能...")
        
        analysis_results = {
            "timestamp": datetime.now().isoformat(),
            "table_stats": {},
            "index_usage": {},
            "slow_queries": [],
            "recommendations": []
        }
        
        try:
            with self.engine.connect() as conn:
                # 分析表统计信息
                analysis_results["table_stats"] = await self._analyze_table_stats(conn)
                
                # 分析索引使用情况
                analysis_results["index_usage"] = await self._analyze_index_usage(conn)
                
                # 查找慢查询
                analysis_results["slow_queries"] = await self._find_slow_queries(conn)
                
                # 生成优化建议
                analysis_results["recommendations"] = await self._generate_recommendations(conn)
                
        except Exception as e:
            logger.error(f"数据库性能分析失败: {e}")
            raise DatabaseException(f"数据库性能分析失败: {e}")
        
        logger.info("数据库性能分析完成")
        return analysis_results
    
    async def _analyze_table_stats(self, conn) -> Dict[str, Any]:
        """分析表统计信息"""
        query = """
        SELECT 
            schemaname,
            tablename,
            attname as column_name,
            n_distinct,
            correlation,
            most_common_vals,
            most_common_freqs
        FROM pg_stats 
        WHERE schemaname = 'public'
        ORDER BY tablename, attname;
        """
        
        result = conn.execute(text(query))
        stats = {}
        
        for row in result:
            table_name = row.tablename
            if table_name not in stats:
                stats[table_name] = {
                    "columns": {},
                    "total_columns": 0
                }
            
            stats[table_name]["columns"][row.column_name] = {
                "n_distinct": row.n_distinct,
                "correlation": row.correlation,
                "most_common_vals": row.most_common_vals,
                "most_common_freqs": row.most_common_freqs
            }
            stats[table_name]["total_columns"] += 1
        
        return stats
    
    async def _analyze_index_usage(self, conn) -> Dict[str, Any]:
        """分析索引使用情况"""
        query = """
        SELECT 
            schemaname,
            tablename,
            indexname,
            idx_tup_read,
            idx_tup_fetch,
            idx_scan
        FROM pg_stat_user_indexes
        ORDER BY idx_scan DESC;
        """
        
        result = conn.execute(text(query))
        index_usage = {}
        
        for row in result:
            table_name = row.tablename
            if table_name not in index_usage:
                index_usage[table_name] = []
            
            index_usage[table_name].append({
                "index_name": row.indexname,
                "tuples_read": row.idx_tup_read,
                "tuples_fetched": row.idx_tup_fetch,
                "scans": row.idx_scan
            })
        
        return index_usage
    
    async def _find_slow_queries(self, conn) -> List[Dict[str, Any]]:
        """查找慢查询"""
        # 注意：需要启用 pg_stat_statements 扩展
        query = """
        SELECT 
            query,
            calls,
            total_time,
            mean_time,
            rows
        FROM pg_stat_statements
        WHERE mean_time > 100  -- 平均执行时间超过100ms
        ORDER BY mean_time DESC
        LIMIT 10;
        """
        
        try:
            result = conn.execute(text(query))
            slow_queries = []
            
            for row in result:
                slow_queries.append({
                    "query": row.query[:200] + "..." if len(row.query) > 200 else row.query,
                    "calls": row.calls,
                    "total_time": row.total_time,
                    "mean_time": row.mean_time,
                    "rows": row.rows
                })
            
            return slow_queries
            
        except Exception as e:
            logger.warning(f"无法获取慢查询信息（可能未启用pg_stat_statements）: {e}")
            return []
    
    async def _generate_recommendations(self, conn) -> List[Dict[str, Any]]:
        """生成优化建议"""
        recommendations = []
        
        # 检查表是否存在
        existing_tables = await self._get_existing_tables(conn)
        
        for table_name, indexes in self.index_recommendations.items():
            if table_name not in existing_tables:
                continue
            
            # 检查现有索引
            existing_indexes = await self._get_existing_indexes(conn, table_name)
            
            for index_config in indexes:
                index_name = index_config["name"]
                
                if index_name not in existing_indexes:
                    recommendations.append({
                        "type": "create_index",
                        "table": table_name,
                        "index_name": index_name,
                        "columns": index_config["columns"],
                        "index_type": index_config["type"],
                        "description": index_config["description"],
                        "priority": "high" if "ts_code" in index_config["columns"] else "medium"
                    })
        
        # 检查未使用的索引
        for table_name in existing_tables:
            unused_indexes = await self._find_unused_indexes(conn, table_name)
            for index_name in unused_indexes:
                recommendations.append({
                    "type": "drop_index",
                    "table": table_name,
                    "index_name": index_name,
                    "description": f"索引 {index_name} 未被使用，建议删除",
                    "priority": "low"
                })
        
        return recommendations
    
    async def _get_existing_tables(self, conn) -> List[str]:
        """获取现有表列表"""
        query = """
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public';
        """
        
        result = conn.execute(text(query))
        return [row.tablename for row in result]
    
    async def _get_existing_indexes(self, conn, table_name: str) -> List[str]:
        """获取表的现有索引"""
        query = """
        SELECT indexname 
        FROM pg_indexes 
        WHERE schemaname = 'public' AND tablename = :table_name;
        """
        
        result = conn.execute(text(query), {"table_name": table_name})
        return [row.indexname for row in result]
    
    async def _find_unused_indexes(self, conn, table_name: str) -> List[str]:
        """查找未使用的索引"""
        query = """
        SELECT indexname
        FROM pg_stat_user_indexes
        WHERE schemaname = 'public' 
        AND tablename = :table_name
        AND idx_scan = 0;
        """
        
        result = conn.execute(text(query), {"table_name": table_name})
        return [row.indexname for row in result]
    
    async def create_optimized_indexes(self, recommendations: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """创建优化索引"""
        if recommendations is None:
            analysis = await self.analyze_database_performance()
            recommendations = analysis["recommendations"]
        
        results = {
            "created_indexes": [],
            "dropped_indexes": [],
            "errors": []
        }
        
        with self.engine.connect() as conn:
            for rec in recommendations:
                try:
                    if rec["type"] == "create_index":
                        await self._create_index(conn, rec)
                        results["created_indexes"].append(rec["index_name"])
                        logger.info(f"创建索引: {rec['index_name']}")
                        
                    elif rec["type"] == "drop_index":
                        await self._drop_index(conn, rec)
                        results["dropped_indexes"].append(rec["index_name"])
                        logger.info(f"删除索引: {rec['index_name']}")
                        
                except Exception as e:
                    error_msg = f"处理索引 {rec.get('index_name', 'unknown')} 失败: {e}"
                    results["errors"].append(error_msg)
                    logger.error(error_msg)
        
        return results
    
    async def _create_index(self, conn, rec: Dict[str, Any]):
        """创建索引"""
        columns_str = ", ".join(rec["columns"])
        index_type = rec.get("index_type", "btree")
        
        # 使用 CONCURRENTLY 避免锁表
        sql = f"""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS {rec["index_name"]} 
        ON {rec["table"]} USING {index_type} ({columns_str});
        """
        
        conn.execute(text(sql))
        conn.commit()
    
    async def _drop_index(self, conn, rec: Dict[str, Any]):
        """删除索引"""
        sql = f"DROP INDEX CONCURRENTLY IF EXISTS {rec['index_name']};"
        conn.execute(text(sql))
        conn.commit()
    
    async def analyze_query_performance(self, query: str) -> Dict[str, Any]:
        """分析特定查询的性能"""
        with self.engine.connect() as conn:
            # 获取查询执行计划
            explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
            
            try:
                result = conn.execute(text(explain_query))
                plan = result.fetchone()[0]
                
                return {
                    "query": query,
                    "execution_plan": plan,
                    "analysis_time": datetime.now().isoformat()
                }
                
            except Exception as e:
                logger.error(f"查询性能分析失败: {e}")
                raise DatabaseException(f"查询性能分析失败: {e}")
    
    async def benchmark_queries(self) -> Dict[str, Any]:
        """基准测试常见查询"""
        benchmark_queries = {
            "stock_daily_data": """
                SELECT * FROM daily_quotes 
                WHERE ts_code = '000001.SZ' 
                AND trade_date >= '2024-01-01' 
                AND trade_date <= '2024-12-31'
                ORDER BY trade_date;
            """,
            "capital_flow_analysis": """
                SELECT ts_code, SUM(net_amount) as total_net_inflow
                FROM capital_flow_daily 
                WHERE trade_date >= '2024-01-01'
                GROUP BY ts_code
                HAVING SUM(net_amount) > 1000000
                ORDER BY total_net_inflow DESC
                LIMIT 100;
            """,
            "technical_indicator_scan": """
                SELECT d.ts_code, d.close, t.rsi, t.macd
                FROM daily_quotes d
                JOIN technical_indicators t ON d.ts_code = t.ts_code AND d.trade_date = t.trade_date
                WHERE d.trade_date = (SELECT MAX(trade_date) FROM daily_quotes)
                AND t.rsi < 30
                ORDER BY t.rsi;
            """,
            "strategy_signal_lookup": """
                SELECT s.*, d.close as current_price
                FROM strategy_signals s
                JOIN daily_quotes d ON s.ts_code = d.ts_code
                WHERE s.signal_date >= CURRENT_DATE - INTERVAL '7 days'
                AND s.conviction_score >= 80
                AND d.trade_date = (SELECT MAX(trade_date) FROM daily_quotes WHERE ts_code = s.ts_code)
                ORDER BY s.conviction_score DESC;
            """
        }
        
        results = {}
        
        for query_name, query in benchmark_queries.items():
            logger.info(f"基准测试查询: {query_name}")
            
            try:
                start_time = time.time()
                
                with self.engine.connect() as conn:
                    result = conn.execute(text(query))
                    rows = result.fetchall()
                
                execution_time = time.time() - start_time
                
                results[query_name] = {
                    "execution_time": execution_time,
                    "rows_returned": len(rows),
                    "status": "success"
                }
                
                logger.info(f"查询 {query_name} 完成: {execution_time:.3f}s, {len(rows)} 行")
                
            except Exception as e:
                results[query_name] = {
                    "execution_time": None,
                    "rows_returned": 0,
                    "status": "error",
                    "error": str(e)
                }
                logger.error(f"查询 {query_name} 失败: {e}")
        
        return results
    
    async def generate_optimization_report(self) -> Dict[str, Any]:
        """生成完整的优化报告"""
        logger.info("生成数据库优化报告...")
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "database_analysis": {},
            "benchmark_results": {},
            "optimization_recommendations": [],
            "summary": {}
        }
        
        try:
            # 数据库分析
            report["database_analysis"] = await self.analyze_database_performance()
            
            # 基准测试
            report["benchmark_results"] = await self.benchmark_queries()
            
            # 优化建议
            report["optimization_recommendations"] = report["database_analysis"]["recommendations"]
            
            # 生成摘要
            report["summary"] = self._generate_summary(report)
            
        except Exception as e:
            logger.error(f"生成优化报告失败: {e}")
            raise VesperaException(f"生成优化报告失败: {e}")
        
        logger.info("数据库优化报告生成完成")
        return report
    
    def _generate_summary(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """生成报告摘要"""
        recommendations = report["optimization_recommendations"]
        benchmark_results = report["benchmark_results"]
        
        # 统计建议
        create_index_count = len([r for r in recommendations if r["type"] == "create_index"])
        drop_index_count = len([r for r in recommendations if r["type"] == "drop_index"])
        
        # 统计基准测试结果
        successful_queries = len([r for r in benchmark_results.values() if r["status"] == "success"])
        failed_queries = len([r for r in benchmark_results.values() if r["status"] == "error"])
        
        avg_execution_time = 0
        if successful_queries > 0:
            total_time = sum(r["execution_time"] for r in benchmark_results.values() 
                           if r["status"] == "success")
            avg_execution_time = total_time / successful_queries
        
        return {
            "total_recommendations": len(recommendations),
            "create_index_recommendations": create_index_count,
            "drop_index_recommendations": drop_index_count,
            "benchmark_queries_total": len(benchmark_results),
            "benchmark_queries_successful": successful_queries,
            "benchmark_queries_failed": failed_queries,
            "average_query_execution_time": avg_execution_time,
            "optimization_priority": "high" if create_index_count > 5 else "medium"
        }


async def main():
    """主函数"""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # 构建连接字符串
    connection_string = (
        f"postgresql://{os.getenv('POSTGRES_USER', 'qiming_user')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'qiming_pass_2024')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'qiming_star')}"
    )
    
    optimizer = DatabaseIndexOptimizer(connection_string)
    
    try:
        # 生成优化报告
        report = await optimizer.generate_optimization_report()
        
        # 保存报告
        import json
        with open("data/database_optimization_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print("数据库优化报告已生成: data/database_optimization_report.json")
        
        # 显示摘要
        summary = report["summary"]
        print(f"\n优化摘要:")
        print(f"- 总建议数: {summary['total_recommendations']}")
        print(f"- 建议创建索引: {summary['create_index_recommendations']}")
        print(f"- 建议删除索引: {summary['drop_index_recommendations']}")
        print(f"- 基准测试成功: {summary['benchmark_queries_successful']}/{summary['benchmark_queries_total']}")
        print(f"- 平均查询时间: {summary['average_query_execution_time']:.3f}s")
        
        # 询问是否执行优化
        if summary['create_index_recommendations'] > 0:
            response = input("\n是否执行索引优化? (y/N): ")
            if response.lower() == 'y':
                results = await optimizer.create_optimized_indexes()
                print(f"\n优化结果:")
                print(f"- 创建索引: {len(results['created_indexes'])}")
                print(f"- 删除索引: {len(results['dropped_indexes'])}")
                print(f"- 错误数: {len(results['errors'])}")
                
                if results['errors']:
                    print("错误详情:")
                    for error in results['errors']:
                        print(f"  - {error}")
        
    except Exception as e:
        logger.error(f"数据库优化失败: {e}")
        print(f"错误: {e}")


if __name__ == "__main__":
    asyncio.run(main())