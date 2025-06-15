#!/usr/bin/env python3
"""
完整系统测试脚本
"""
import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.database import get_db_manager
from src.utils.logger import get_logger
from src.analyzers.technical_analyzer import TechnicalAnalyzer
from src.analyzers.capital_flow_analyzer import CapitalFlowAnalyzer
from src.analyzers.fundamental_analyzer import FundamentalAnalyzer
from src.analyzers.macro_analyzer import MacroAnalyzer
from src.fusion.signal_fusion_engine import SignalFusionEngine

logger = get_logger("test_full_system")


def test_database_connections():
    """测试数据库连接"""
    logger.info("🔗 测试数据库连接...")
    
    try:
        db_manager = get_db_manager()
        results = db_manager.test_connections()
        
        all_connected = True
        for db_name, status in results.items():
            if status:
                logger.success(f"✅ {db_name} 连接成功")
            else:
                logger.error(f"❌ {db_name} 连接失败")
                all_connected = False
        
        return all_connected
        
    except Exception as e:
        logger.error(f"数据库连接测试失败: {e}")
        return False


def test_data_availability():
    """测试数据可用性"""
    logger.info("📊 测试数据可用性...")
    
    try:
        db_manager = get_db_manager()
        
        # 检查股票基础信息
        stock_count = db_manager.execute_postgres_query("SELECT COUNT(*) as count FROM stock_basic")
        stock_num = stock_count.iloc[0]['count'] if not stock_count.empty else 0
        logger.info(f"股票基础信息: {stock_num} 条")
        
        # 检查日线行情
        quotes_count = db_manager.execute_postgres_query("SELECT COUNT(*) as count FROM stock_daily_quotes")
        quotes_num = quotes_count.iloc[0]['count'] if not quotes_count.empty else 0
        logger.info(f"日线行情数据: {quotes_num} 条")
        
        # 检查资金流数据
        money_flow_count = db_manager.execute_postgres_query("SELECT COUNT(*) as count FROM money_flow_daily")
        money_flow_num = money_flow_count.iloc[0]['count'] if not money_flow_count.empty else 0
        logger.info(f"资金流数据: {money_flow_num} 条")
        
        if stock_num > 0 and quotes_num > 0 and money_flow_num > 0:
            logger.success("✅ 基础数据完整")
            return True
        else:
            logger.warning("⚠️ 基础数据不完整，建议运行 fetch_sample_data.py")
            return False
            
    except Exception as e:
        logger.error(f"数据可用性测试失败: {e}")
        return False


def test_analyzers():
    """测试各分析器"""
    logger.info("🔍 测试分析器...")
    
    try:
        db_manager = get_db_manager()
        
        # 获取一只测试股票
        test_stock = db_manager.execute_postgres_query(
            "SELECT ts_code FROM stock_basic LIMIT 1"
        )
        
        if test_stock.empty:
            logger.error("没有可用的测试股票")
            return False
        
        ts_code = test_stock.iloc[0]['ts_code']
        trade_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"使用测试股票: {ts_code}")
        
        # 测试技术分析器
        logger.info("测试技术分析器...")
        tech_analyzer = TechnicalAnalyzer()
        tech_result = tech_analyzer.analyze_stock(ts_code, trade_date)
        if tech_result:
            logger.success("✅ 技术分析器正常")
        else:
            logger.warning("⚠️ 技术分析器返回空结果")
        
        # 测试资金流分析器
        logger.info("测试资金流分析器...")
        capital_analyzer = CapitalFlowAnalyzer()
        capital_result = capital_analyzer.analyze_stock(ts_code, trade_date)
        if capital_result:
            logger.success("✅ 资金流分析器正常")
        else:
            logger.warning("⚠️ 资金流分析器返回空结果")
        
        # 测试基本面分析器
        logger.info("测试基本面分析器...")
        fundamental_analyzer = FundamentalAnalyzer()
        fundamental_result = fundamental_analyzer.analyze_stock(ts_code, trade_date)
        if fundamental_result:
            logger.success("✅ 基本面分析器正常")
        else:
            logger.warning("⚠️ 基本面分析器返回空结果")
        

        
        # 测试宏观分析器
        logger.info("测试宏观分析器...")
        macro_analyzer = MacroAnalyzer()
        macro_result = macro_analyzer.analyze_macro_environment(trade_date)
        if macro_result:
            logger.success("✅ 宏观分析器正常")
        else:
            logger.warning("⚠️ 宏观分析器返回空结果")
        
        return True
        
    except Exception as e:
        logger.error(f"分析器测试失败: {e}")
        return False


def test_signal_fusion():
    """测试信号融合引擎"""
    logger.info("🎯 测试信号融合引擎...")
    
    try:
        db_manager = get_db_manager()
        
        # 获取测试股票
        test_stocks = db_manager.execute_postgres_query(
            "SELECT ts_code FROM stock_basic LIMIT 3"
        )
        
        if test_stocks.empty:
            logger.error("没有可用的测试股票")
            return False
        
        trade_date = datetime.now().strftime('%Y-%m-%d')
        fusion_engine = SignalFusionEngine()
        
        signals_generated = 0
        
        for _, row in test_stocks.iterrows():
            ts_code = row['ts_code']
            logger.info(f"测试股票 {ts_code} 的信号生成...")
            
            signal = fusion_engine.analyze_and_generate_signal(ts_code, trade_date)
            
            if signal:
                logger.success(f"✅ 为股票 {ts_code} 生成信号，置信度: {signal['confidence_score']:.3f}")
                signals_generated += 1
            else:
                logger.info(f"ℹ️ 股票 {ts_code} 未通过融合逻辑筛选")
        
        logger.info(f"共生成 {signals_generated} 个信号")
        
        if signals_generated > 0:
            logger.success("✅ 信号融合引擎正常工作")
        else:
            logger.warning("⚠️ 信号融合引擎未生成任何信号（可能是正常的）")
        
        return True
        
    except Exception as e:
        logger.error(f"信号融合测试失败: {e}")
        return False


def test_data_pipeline():
    """测试完整数据处理流程"""
    logger.info("🔄 测试完整数据处理流程...")
    
    try:
        db_manager = get_db_manager()
        trade_date = datetime.now().strftime('%Y-%m-%d')
        
        # 获取股票列表
        stocks = db_manager.execute_postgres_query("SELECT ts_code FROM stock_basic LIMIT 5")
        
        if stocks.empty:
            logger.error("没有可用的股票数据")
            return False
        
        # 初始化分析器
        tech_analyzer = TechnicalAnalyzer()
        capital_analyzer = CapitalFlowAnalyzer()
        
        tech_results = []
        capital_results = []
        
        # 批量分析
        for _, row in stocks.iterrows():
            ts_code = row['ts_code']
            
            # 技术分析
            tech_result = tech_analyzer.analyze_stock(ts_code, trade_date)
            if tech_result:
                tech_results.append(tech_result)
            
            # 资金流分析
            capital_result = capital_analyzer.analyze_stock(ts_code, trade_date)
            if capital_result:
                capital_results.append(capital_result)
        
        # 保存分析结果
        if tech_results:
            tech_df = pd.DataFrame(tech_results)
            db_manager.insert_dataframe_to_postgres(
                tech_df, 'technical_daily_profiles', if_exists='append'
            )
            logger.success(f"✅ 技术分析结果已保存: {len(tech_results)} 条")
        
        if capital_results:
            capital_df = pd.DataFrame(capital_results)
            db_manager.insert_dataframe_to_postgres(
                capital_df, 'capital_flow_profiles', if_exists='append'
            )
            logger.success(f"✅ 资金流分析结果已保存: {len(capital_results)} 条")
        
        return True
        
    except Exception as e:
        logger.error(f"数据处理流程测试失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("🌟 启明星系统完整测试开始")
    logger.info("=" * 60)
    
    test_results = []
    
    # 1. 测试数据库连接
    test_results.append(("数据库连接", test_database_connections()))
    
    # 2. 测试数据可用性
    test_results.append(("数据可用性", test_data_availability()))
    
    # 3. 测试分析器
    test_results.append(("分析器功能", test_analyzers()))
    
    # 4. 测试信号融合
    test_results.append(("信号融合", test_signal_fusion()))
    
    # 5. 测试数据处理流程
    test_results.append(("数据处理流程", test_data_pipeline()))
    
    # 汇总测试结果
    logger.info("=" * 60)
    logger.info("📊 测试结果汇总")
    logger.info("=" * 60)
    
    passed_tests = 0
    total_tests = len(test_results)
    
    for test_name, result in test_results:
        if result:
            logger.success(f"✅ {test_name}: 通过")
            passed_tests += 1
        else:
            logger.error(f"❌ {test_name}: 失败")
    
    success_rate = passed_tests / total_tests
    
    logger.info("=" * 60)
    if success_rate >= 0.8:
        logger.success(f"🎉 系统测试完成！通过率: {success_rate:.1%} ({passed_tests}/{total_tests})")
        logger.success("系统运行正常，可以开始使用！")
    elif success_rate >= 0.6:
        logger.warning(f"⚠️ 系统测试完成！通过率: {success_rate:.1%} ({passed_tests}/{total_tests})")
        logger.warning("系统基本可用，但建议检查失败的测试项")
    else:
        logger.error(f"🚨 系统测试完成！通过率: {success_rate:.1%} ({passed_tests}/{total_tests})")
        logger.error("系统存在严重问题，请检查配置和依赖")
    
    logger.info("=" * 60)
    
    return success_rate >= 0.6


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
