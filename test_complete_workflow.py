#!/usr/bin/env python3
"""
Vespera 完整工作流程测试
验证从数据获取到信号生成的端到端流程
"""

import sys
import os
import asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

async def test_data_acquisition():
    """测试数据获取流程"""
    print("📊 测试数据获取流程")
    print("=" * 60)
    
    results = {}
    
    # 测试数据源管理器
    try:
        from src.data_sources.enhanced_data_source_manager import EnhancedDataSourceManager
        
        # 初始化数据源管理器
        data_manager = EnhancedDataSourceManager()
        print("✅ 数据源管理器初始化成功")
        
        # 测试获取股票列表
        try:
            stock_list = await data_manager.get_stock_list()
            if stock_list and len(stock_list) > 0:
                results['stock_list'] = True
                print(f"✅ 获取股票列表成功: {len(stock_list)} 只股票")
            else:
                results['stock_list'] = False
                print("⚠️ 股票列表为空")
        except Exception as e:
            results['stock_list'] = False
            print(f"❌ 获取股票列表失败: {e}")
        
        # 测试获取历史数据
        try:
            # 使用测试股票代码
            test_symbol = "000001.SZ"  # 平安银行
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            historical_data = await data_manager.get_historical_data(
                symbol=test_symbol,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            
            if historical_data is not None and len(historical_data) > 0:
                results['historical_data'] = True
                print(f"✅ 获取历史数据成功: {test_symbol}, {len(historical_data)} 条记录")
            else:
                results['historical_data'] = False
                print(f"⚠️ 历史数据为空: {test_symbol}")
        except Exception as e:
            results['historical_data'] = False
            print(f"❌ 获取历史数据失败: {e}")
        
    except Exception as e:
        results['data_manager'] = False
        print(f"❌ 数据源管理器初始化失败: {e}")
    
    return results

async def test_data_processing():
    """测试数据处理流程"""
    print("\n🔄 测试数据处理流程")
    print("=" * 60)
    
    results = {}
    
    # 测试异步处理器
    try:
        from src.utils.async_processor import AsyncProcessor
        
        processor = AsyncProcessor(max_workers=4)
        print("✅ 异步处理器初始化成功")
        
        # 测试批量处理
        async def sample_task(data):
            await asyncio.sleep(0.1)  # 模拟处理时间
            return data * 2
        
        test_data = list(range(10))
        processed_results = await processor.process_batch(test_data, sample_task)
        
        if len(processed_results) == len(test_data):
            results['async_processing'] = True
            print(f"✅ 异步批量处理成功: {len(processed_results)} 个任务")
        else:
            results['async_processing'] = False
            print("❌ 异步批量处理结果不匹配")
        
    except Exception as e:
        results['async_processing'] = False
        print(f"❌ 异步处理器测试失败: {e}")
    
    # 测试数据质量监控
    try:
        from src.monitoring.data_quality_monitor import DataQualityMonitor
        
        monitor = DataQualityMonitor()
        print("✅ 数据质量监控器初始化成功")
        
        # 创建测试数据
        test_df = pd.DataFrame({
            'symbol': ['000001.SZ'] * 100,
            'date': pd.date_range('2024-01-01', periods=100),
            'open': [10.0 + i * 0.1 for i in range(100)],
            'high': [10.5 + i * 0.1 for i in range(100)],
            'low': [9.5 + i * 0.1 for i in range(100)],
            'close': [10.2 + i * 0.1 for i in range(100)],
            'volume': [1000000 + i * 10000 for i in range(100)]
        })
        
        # 执行数据质量检查
        quality_report = monitor.check_data_quality(test_df)
        
        if quality_report and 'overall_score' in quality_report:
            results['data_quality'] = True
            print(f"✅ 数据质量检查成功: 评分 {quality_report['overall_score']:.2f}")
        else:
            results['data_quality'] = False
            print("❌ 数据质量检查失败")
        
    except Exception as e:
        results['data_quality'] = False
        print(f"❌ 数据质量监控测试失败: {e}")
    
    return results

def test_caching_system():
    """测试缓存系统"""
    print("\n💾 测试缓存系统")
    print("=" * 60)
    
    results = {}
    
    try:
        from src.utils.cache_manager import global_cache_manager
        
        # 测试缓存写入和读取
        test_key = "test_workflow_cache"
        test_data = {"timestamp": datetime.now().isoformat(), "data": [1, 2, 3, 4, 5]}
        
        # 写入缓存
        global_cache_manager.set(test_key, test_data, ttl=300)
        print("✅ 缓存写入成功")
        
        # 读取缓存
        cached_data = global_cache_manager.get(test_key)
        if cached_data == test_data:
            results['cache_operations'] = True
            print("✅ 缓存读取成功")
        else:
            results['cache_operations'] = False
            print("❌ 缓存数据不匹配")
        
        # 获取缓存统计
        cache_stats = global_cache_manager.get_cache_stats()
        if cache_stats:
            results['cache_stats'] = True
            print(f"✅ 缓存统计获取成功: {cache_stats}")
        else:
            results['cache_stats'] = False
            print("❌ 缓存统计获取失败")
        
    except Exception as e:
        results['cache_system'] = False
        print(f"❌ 缓存系统测试失败: {e}")
    
    return results

async def test_strategy_execution():
    """测试策略执行流程"""
    print("\n🎯 测试策略执行流程")
    print("=" * 60)
    
    results = {}
    
    # 测试技术指标计算
    try:
        from src.strategies.technical_indicators import TechnicalIndicators
        
        # 创建测试数据
        test_data = pd.DataFrame({
            'close': [10.0 + i * 0.1 for i in range(50)],
            'high': [10.5 + i * 0.1 for i in range(50)],
            'low': [9.5 + i * 0.1 for i in range(50)],
            'volume': [1000000 + i * 10000 for i in range(50)]
        })
        
        indicators = TechnicalIndicators()
        
        # 计算移动平均线
        ma_result = indicators.calculate_ma(test_data['close'], period=20)
        if ma_result is not None and len(ma_result) > 0:
            results['technical_indicators'] = True
            print("✅ 技术指标计算成功")
        else:
            results['technical_indicators'] = False
            print("❌ 技术指标计算失败")
        
    except Exception as e:
        results['technical_indicators'] = False
        print(f"❌ 技术指标测试失败: {e}")
    
    # 测试资金流向分析
    try:
        from src.strategies.capital_flow_analyzer import CapitalFlowAnalyzer
        
        analyzer = CapitalFlowAnalyzer()
        
        # 创建测试资金流数据
        flow_data = pd.DataFrame({
            'symbol': ['000001.SZ'] * 20,
            'date': pd.date_range('2024-01-01', periods=20),
            'net_inflow': [1000000 + i * 100000 for i in range(20)],
            'main_inflow': [500000 + i * 50000 for i in range(20)],
            'retail_inflow': [500000 + i * 50000 for i in range(20)]
        })
        
        # 分析资金流向
        flow_analysis = analyzer.analyze_capital_flow(flow_data)
        if flow_analysis and 'trend' in flow_analysis:
            results['capital_flow'] = True
            print(f"✅ 资金流向分析成功: 趋势 {flow_analysis['trend']}")
        else:
            results['capital_flow'] = False
            print("❌ 资金流向分析失败")
        
    except Exception as e:
        results['capital_flow'] = False
        print(f"❌ 资金流向分析测试失败: {e}")
    
    return results

def test_database_integration():
    """测试数据库集成"""
    print("\n🗄️ 测试数据库集成")
    print("=" * 60)
    
    results = {}
    
    try:
        from src.database.connection_manager import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # 测试PostgreSQL连接
        try:
            with db_manager.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT version();")
                version = cursor.fetchone()
                if version:
                    results['postgres'] = True
                    print("✅ PostgreSQL连接成功")
                else:
                    results['postgres'] = False
                    print("❌ PostgreSQL查询失败")
        except Exception as e:
            results['postgres'] = False
            print(f"❌ PostgreSQL连接失败: {e}")
        
        # 测试Redis连接
        try:
            redis_client = db_manager.get_redis_connection()
            redis_client.ping()
            results['redis'] = True
            print("✅ Redis连接成功")
        except Exception as e:
            results['redis'] = False
            print(f"❌ Redis连接失败: {e}")
        
        # 测试ClickHouse连接
        try:
            clickhouse_client = db_manager.get_clickhouse_connection()
            result = clickhouse_client.execute("SELECT version()")
            if result:
                results['clickhouse'] = True
                print("✅ ClickHouse连接成功")
            else:
                results['clickhouse'] = False
                print("❌ ClickHouse查询失败")
        except Exception as e:
            results['clickhouse'] = False
            print(f"❌ ClickHouse连接失败: {e}")
        
    except Exception as e:
        results['database_manager'] = False
        print(f"❌ 数据库管理器初始化失败: {e}")
    
    return results

async def test_signal_generation():
    """测试信号生成"""
    print("\n📈 测试信号生成")
    print("=" * 60)
    
    results = {}
    
    try:
        from src.strategies.signal_generator import SignalGenerator
        
        signal_gen = SignalGenerator()
        
        # 创建测试市场数据
        market_data = {
            'symbol': '000001.SZ',
            'price': 12.50,
            'volume': 1500000,
            'technical_score': 0.75,
            'capital_score': 0.80,
            'relative_strength': 0.65,
            'catalyst_score': 0.70
        }
        
        # 生成交易信号
        signal = signal_gen.generate_signal(market_data)
        
        if signal and 'action' in signal and 'confidence' in signal:
            results['signal_generation'] = True
            print(f"✅ 信号生成成功: {signal['action']} (置信度: {signal['confidence']:.2f})")
        else:
            results['signal_generation'] = False
            print("❌ 信号生成失败")
        
    except Exception as e:
        results['signal_generation'] = False
        print(f"❌ 信号生成测试失败: {e}")
    
    return results

async def main():
    """主测试函数"""
    print("🔄 Vespera 完整工作流程测试")
    print(f"测试时间: {datetime.now()}")
    print("=" * 80)
    
    # 执行所有测试
    start_time = time.time()
    
    data_acquisition_results = await test_data_acquisition()
    data_processing_results = await test_data_processing()
    caching_results = test_caching_system()
    strategy_results = await test_strategy_execution()
    database_results = test_database_integration()
    signal_results = await test_signal_generation()
    
    end_time = time.time()
    
    # 汇总所有结果
    all_results = {}
    all_results.update(data_acquisition_results)
    all_results.update(data_processing_results)
    all_results.update(caching_results)
    all_results.update(strategy_results)
    all_results.update(database_results)
    all_results.update(signal_results)
    
    # 计算总体成功率
    total_tests = len(all_results)
    passed_tests = sum(all_results.values())
    success_rate = passed_tests / total_tests * 100 if total_tests > 0 else 0
    
    print("\n📊 完整工作流程测试结果汇总")
    print("=" * 80)
    print(f"总测试数: {total_tests}")
    print(f"通过测试: {passed_tests}")
    print(f"成功率: {success_rate:.1f}%")
    print(f"测试耗时: {end_time - start_time:.2f} 秒")
    
    # 详细结果分析
    print("\n📋 详细测试结果:")
    for test_name, result in all_results.items():
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name}: {status}")
    
    # 给出总体评估
    if success_rate >= 90:
        print("\n🎉 工作流程测试优秀! 项目可以完美运行!")
    elif success_rate >= 80:
        print("\n✅ 工作流程测试良好! 项目基本可以正常运行!")
    elif success_rate >= 70:
        print("\n⚠️ 工作流程测试一般，存在一些问题需要修复")
    else:
        print("\n❌ 工作流程测试较差，需要重点修复问题")
    
    # 生成运行建议
    print("\n💡 运行建议:")
    if all_results.get('postgres', False) and all_results.get('redis', False):
        print("1. 数据库连接正常，可以启动完整系统")
    else:
        print("1. 请先确保数据库服务正常运行: docker-compose up -d")
    
    if all_results.get('cache_operations', False):
        print("2. 缓存系统正常，性能优化已启用")
    else:
        print("2. 缓存系统异常，可能影响性能")
    
    if all_results.get('signal_generation', False):
        print("3. 信号生成功能正常，可以进行量化交易")
    else:
        print("3. 信号生成功能异常，需要检查策略模块")
    
    print("\n🚀 启动命令:")
    print("   python -m src.main  # 启动主程序")
    print("   streamlit run dashboard/app.py  # 启动Dashboard")

if __name__ == "__main__":
    asyncio.run(main())