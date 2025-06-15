#!/usr/bin/env python3
"""
测试A股T+1策略分析
验证T+1交易策略分析模块的功能
"""
import sys
import os
from pathlib import Path
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.analysis.a_share_t1_strategy import AShareT1Strategy, analyze_a_share_data

logger = get_logger("test_a_share_t1_strategy")


def test_load_a_share_data():
    """测试加载A股数据"""
    logger.info("📊 测试加载A股数据")
    
    try:
        # 查找A股数据文件
        data_file = project_root / 'data' / 'production' / 'a_share' / 'a_share_daily_quotes.csv'
        
        if not data_file.exists():
            logger.error(f"❌ A股数据文件不存在: {data_file}")
            return None
        
        # 加载数据
        data = pd.read_csv(data_file)
        
        logger.success(f"✅ 成功加载A股数据: {len(data)} 条记录")
        logger.info(f"数据列: {list(data.columns)}")
        logger.info(f"股票数量: {data['ts_code'].nunique()}")
        logger.info(f"日期范围: {data['trade_date'].min()} 到 {data['trade_date'].max()}")
        
        # 显示数据样例
        logger.info("数据样例:")
        print(data.head().to_string())
        
        return data
        
    except Exception as e:
        logger.error(f"❌ 加载A股数据失败: {e}")
        return None


def test_overnight_risk_analysis(data):
    """测试隔夜风险分析"""
    logger.info("\n🌙 测试隔夜风险分析")
    
    try:
        analyzer = AShareT1Strategy()
        
        # 执行隔夜风险分析
        overnight_risk = analyzer.analyze_overnight_risk(data)
        
        if overnight_risk:
            logger.success("✅ 隔夜风险分析完成")
            
            # 显示市场总体风险
            market_summary = overnight_risk.get('market_summary', {})
            logger.info(f"市场平均隔夜收益: {market_summary.get('market_avg_overnight_return', 0):.2f}%")
            logger.info(f"市场隔夜波动率: {market_summary.get('market_overnight_volatility', 0):.2f}%")
            
            high_risk_stocks = market_summary.get('high_risk_stocks', [])
            if high_risk_stocks:
                logger.warning(f"高风险股票: {high_risk_stocks[:3]}")
            
            # 显示个股风险样例
            individual_stocks = overnight_risk.get('individual_stocks', {})
            if individual_stocks:
                sample_stock = list(individual_stocks.keys())[0]
                sample_risk = individual_stocks[sample_stock]
                logger.info(f"样例股票 {sample_stock} 隔夜风险:")
                logger.info(f"  平均隔夜收益: {sample_risk.get('avg_overnight_return', 0):.2f}%")
                logger.info(f"  隔夜波动率: {sample_risk.get('overnight_volatility', 0):.2f}%")
                logger.info(f"  风险评分: {sample_risk.get('overnight_risk_score', 0):.2f}")
            
            return True
        else:
            logger.error("❌ 隔夜风险分析失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 隔夜风险分析异常: {e}")
        return False


def test_trading_opportunities(data):
    """测试交易机会识别"""
    logger.info("\n🎯 测试交易机会识别")
    
    try:
        analyzer = AShareT1Strategy()
        
        # 执行交易机会识别
        opportunities = analyzer.identify_t1_trading_opportunities(data)
        
        if opportunities:
            logger.success("✅ 交易机会识别完成")
            
            # 显示总体统计
            summary = opportunities.get('summary', {})
            logger.info(f"总交易机会: {summary.get('total_opportunities', 0)} 个")
            logger.info(f"有机会的股票: {summary.get('stocks_with_opportunities', 0)} 只")
            
            # 显示机会类型分布
            opportunity_types = summary.get('opportunity_types', {})
            if opportunity_types:
                logger.info("机会类型分布:")
                for opp_type, count in opportunity_types.items():
                    logger.info(f"  {opp_type}: {count} 个")
            
            # 显示具体机会样例
            stock_opportunities = opportunities.get('opportunities', {})
            if stock_opportunities:
                for stock_code, stock_opps in list(stock_opportunities.items())[:2]:  # 只显示前2只股票
                    if stock_opps:
                        logger.info(f"股票 {stock_code} 的交易机会:")
                        for opp in stock_opps[:2]:  # 只显示前2个机会
                            logger.info(f"  {opp['date']}: {opp['type']} - {opp['reason']}")
                            logger.info(f"    信号强度: {opp['signal_strength']}, T+1适用性: {opp['t1_suitability']}")
            
            return True
        else:
            logger.error("❌ 交易机会识别失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 交易机会识别异常: {e}")
        return False


def test_risk_metrics(data):
    """测试风险指标计算"""
    logger.info("\n⚠️ 测试风险指标计算")
    
    try:
        analyzer = AShareT1Strategy()
        
        # 执行风险指标计算
        risk_metrics = analyzer.calculate_t1_risk_metrics(data)
        
        if risk_metrics:
            logger.success("✅ 风险指标计算完成")
            
            # 显示市场风险总结
            market_summary = risk_metrics.get('market_summary', {})
            logger.info(f"市场平均风险评分: {market_summary.get('avg_market_risk', 0):.2f}")
            
            high_risk_stocks = market_summary.get('high_risk_stocks', [])
            low_risk_stocks = market_summary.get('low_risk_stocks', [])
            
            logger.info(f"高风险股票: {len(high_risk_stocks)} 只")
            if high_risk_stocks:
                logger.warning(f"  示例: {high_risk_stocks[:3]}")
            
            logger.info(f"低风险股票: {len(low_risk_stocks)} 只")
            if low_risk_stocks:
                logger.info(f"  示例: {low_risk_stocks[:3]}")
            
            # 显示个股风险样例
            individual_stocks = risk_metrics.get('individual_stocks', {})
            if individual_stocks:
                sample_stock = list(individual_stocks.keys())[0]
                sample_metrics = individual_stocks[sample_stock]
                logger.info(f"样例股票 {sample_stock} 风险指标:")
                logger.info(f"  波动率: {sample_metrics.get('volatility', 0):.2f}%")
                logger.info(f"  最大回撤: {sample_metrics.get('max_drawdown', 0):.2f}%")
                logger.info(f"  95% VaR: {sample_metrics.get('var_95', 0):.2f}%")
                logger.info(f"  跳空风险: {sample_metrics.get('gap_risk', 0):.2f}%")
                logger.info(f"  T+1风险评分: {sample_metrics.get('t1_risk_score', 0):.2f}")
                logger.info(f"  风险等级: {sample_metrics.get('risk_level', 'unknown')}")
            
            return True
        else:
            logger.error("❌ 风险指标计算失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 风险指标计算异常: {e}")
        return False


def test_trading_plan_generation(data):
    """测试交易计划生成"""
    logger.info("\n📋 测试交易计划生成")
    
    try:
        analyzer = AShareT1Strategy()
        
        # 生成交易计划
        trading_plan = analyzer.generate_t1_trading_plan(data)
        
        if trading_plan:
            logger.success("✅ 交易计划生成完成")
            
            # 显示计划概要
            logger.info(f"计划日期: {trading_plan.get('plan_date', 'N/A')}")
            
            market_analysis = trading_plan.get('market_analysis', {})
            logger.info(f"市场分析:")
            logger.info(f"  隔夜风险等级: {market_analysis.get('overnight_risk_level', 'unknown')}")
            logger.info(f"  交易机会数量: {market_analysis.get('opportunity_count', 0)}")
            logger.info(f"  高风险股票数: {len(market_analysis.get('high_risk_stocks', []))}")
            
            # 显示推荐操作
            recommended_actions = trading_plan.get('recommended_actions', [])
            if recommended_actions:
                logger.info(f"推荐操作 ({len(recommended_actions)} 个):")
                for action in recommended_actions[:3]:  # 只显示前3个
                    logger.info(f"  {action['stock']}: {action['action']} @ ¥{action['entry_price']:.2f}")
                    logger.info(f"    理由: {action['reason']}")
                    logger.info(f"    信心度: {action['confidence']}")
            
            # 显示风险警告
            risk_warnings = trading_plan.get('risk_warnings', [])
            if risk_warnings:
                logger.warning("风险警告:")
                for warning in risk_warnings:
                    logger.warning(f"  {warning['type']}: {warning['message']}")
            
            # 显示T+1特殊建议
            t1_advice = trading_plan.get('t1_specific_advice', [])
            if t1_advice:
                logger.info("T+1交易建议:")
                for advice in t1_advice[:3]:  # 只显示前3条
                    logger.info(f"  • {advice}")
            
            return True
        else:
            logger.error("❌ 交易计划生成失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 交易计划生成异常: {e}")
        return False


def test_complete_analysis(data):
    """测试完整分析流程"""
    logger.info("\n🔄 测试完整分析流程")
    
    try:
        data_file = project_root / 'data' / 'production' / 'a_share' / 'a_share_daily_quotes.csv'
        
        # 使用便捷函数进行完整分析
        results = analyze_a_share_data(str(data_file))
        
        if results:
            logger.success("✅ 完整分析流程测试成功")
            
            # 显示各模块结果
            modules = ['overnight_risk', 'trading_opportunities', 'risk_metrics', 'trading_plan']
            for module in modules:
                if module in results and results[module]:
                    logger.info(f"✅ {module} 模块正常")
                else:
                    logger.warning(f"⚠️ {module} 模块异常")
            
            return True
        else:
            logger.error("❌ 完整分析流程失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 完整分析流程异常: {e}")
        return False


def main():
    """主函数"""
    logger.info("🧪 A股T+1策略分析测试")
    logger.info("=" * 80)
    
    # 1. 加载数据
    logger.info("🎯 步骤1: 加载A股数据")
    data = test_load_a_share_data()
    if data is None:
        logger.error("❌ 数据加载失败，无法继续测试")
        return False
    
    # 执行各项测试
    tests = [
        ("隔夜风险分析", lambda: test_overnight_risk_analysis(data)),
        ("交易机会识别", lambda: test_trading_opportunities(data)),
        ("风险指标计算", lambda: test_risk_metrics(data)),
        ("交易计划生成", lambda: test_trading_plan_generation(data)),
        ("完整分析流程", lambda: test_complete_analysis(data))
    ]
    
    success_count = 0
    total_count = len(tests)
    
    for test_name, test_func in tests:
        try:
            logger.info(f"\n🎯 测试: {test_name}")
            success = test_func()
            if success:
                success_count += 1
                logger.success(f"✅ {test_name} 测试通过")
            else:
                logger.error(f"❌ {test_name} 测试失败")
        except Exception as e:
            logger.error(f"❌ {test_name} 测试异常: {e}")
    
    # 总结
    logger.info("\n" + "=" * 80)
    logger.info("📊 测试总结")
    logger.info("=" * 80)
    logger.info(f"总测试数: {total_count}")
    logger.info(f"通过测试: {success_count}")
    logger.info(f"失败测试: {total_count - success_count}")
    logger.info(f"通过率: {success_count/total_count*100:.1f}%")
    
    if success_count == total_count:
        logger.success("🎉 所有T+1策略分析测试都通过！")
        logger.info("\n💡 A股T+1策略分析系统功能:")
        logger.info("  ✅ 隔夜风险评估")
        logger.info("  ✅ T+1交易机会识别")
        logger.info("  ✅ 风险指标计算")
        logger.info("  ✅ 交易计划生成")
        logger.info("  ✅ 完整分析流程")
        
        logger.info("\n🚀 系统已准备好进行A股T+1交易分析！")
    else:
        logger.warning("⚠️ 部分测试失败，请检查系统配置")
    
    return success_count == total_count


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
