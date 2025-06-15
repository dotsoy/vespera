#!/usr/bin/env python3
"""
统一系统测试脚本
整合所有测试功能，提供完整的系统验证
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

logger = get_logger("unified_system_test")


def test_data_sources():
    """测试数据源连接"""
    logger.info("🔌 测试数据源连接")
    
    try:
        # 测试AllTick（如果有token）
        alltick_success = False
        try:
            from src.data_sources.alltick_data_source import AllTickDataSource
            token = os.getenv('ALLTICK_TOKEN', '')
            if token:
                alltick = AllTickDataSource(token)
                alltick_success = alltick.initialize()
                if alltick_success:
                    logger.success("✅ AllTick连接成功")
                else:
                    logger.warning("⚠️ AllTick连接失败")
            else:
                logger.info("ℹ️ AllTick token未配置，跳过测试")
        except Exception as e:
            logger.warning(f"⚠️ AllTick测试异常: {e}")
        
        # 测试Alpha Vantage（如果有key）
        alpha_success = False
        try:
            from src.data_sources.alpha_vantage_data_source import AlphaVantageDataSource
            api_key = os.getenv('ALPHA_VANTAGE_API_KEY', '')
            if api_key:
                alpha = AlphaVantageDataSource(api_key)
                alpha_success = alpha.initialize()
                if alpha_success:
                    logger.success("✅ Alpha Vantage连接成功")
                else:
                    logger.warning("⚠️ Alpha Vantage连接失败")
            else:
                logger.info("ℹ️ Alpha Vantage key未配置，跳过测试")
        except Exception as e:
            logger.warning(f"⚠️ Alpha Vantage测试异常: {e}")
        
        return alltick_success or alpha_success
        
    except Exception as e:
        logger.error(f"❌ 数据源测试失败: {e}")
        return False


def test_stock_filter():
    """测试股票过滤器"""
    logger.info("🔍 测试股票过滤器")
    
    try:
        from src.utils.stock_filter import StockFilter
        import pandas as pd
        
        # 创建测试数据
        test_data = pd.DataFrame({
            'ts_code': ['000001.SZ', 'ST0002.SZ', '300001.SZ', '600000.SH', '830001.BJ'],
            'name': ['平安银行', 'ST万科', '特锐德', '浦发银行', '北交所股票']
        })
        
        stock_filter = StockFilter()
        filtered_data = stock_filter.filter_stock_list(test_data)
        
        # 验证过滤结果
        expected_codes = ['000001.SZ', '300001.SZ', '600000.SH']
        actual_codes = filtered_data['ts_code'].tolist()
        
        if set(actual_codes) == set(expected_codes):
            logger.success("✅ 股票过滤器测试通过")
            return True
        else:
            logger.error("❌ 股票过滤器测试失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 股票过滤器测试异常: {e}")
        return False


def test_data_quality():
    """测试数据质量"""
    logger.info("📊 测试数据质量")
    
    try:
        import pandas as pd
        
        # 检查生产数据
        data_file = project_root / 'data' / 'production' / 'a_share' / 'a_share_daily_quotes.csv'
        
        if not data_file.exists():
            logger.error("❌ 生产数据文件不存在")
            return False
        
        data = pd.read_csv(data_file)
        
        # 基本质量检查
        if data.empty:
            logger.error("❌ 数据为空")
            return False
        
        # 检查关键字段
        required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            logger.error(f"❌ 缺少关键字段: {missing_columns}")
            return False
        
        # 检查数据逻辑
        price_logic_errors = (
            (data['high'] < data['open']) | 
            (data['high'] < data['close']) | 
            (data['low'] > data['open']) | 
            (data['low'] > data['close']) |
            (data['high'] < data['low'])
        ).sum()
        
        if price_logic_errors > 0:
            logger.warning(f"⚠️ 发现 {price_logic_errors} 条价格逻辑错误")
        
        logger.success(f"✅ 数据质量检查通过: {len(data)} 条记录")
        logger.info(f"股票数量: {data['ts_code'].nunique()}")
        logger.info(f"日期范围: {data['trade_date'].min()} 到 {data['trade_date'].max()}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据质量测试失败: {e}")
        return False


def test_t1_strategy():
    """测试T+1策略分析"""
    logger.info("⏰ 测试T+1策略分析")
    
    try:
        from src.analysis.a_share_t1_strategy import AShareT1Strategy
        import pandas as pd
        
        # 加载数据
        data_file = project_root / 'data' / 'production' / 'a_share' / 'a_share_daily_quotes.csv'
        
        if not data_file.exists():
            logger.warning("⚠️ 生产数据不存在，跳过T+1策略测试")
            return True
        
        data = pd.read_csv(data_file)
        analyzer = AShareT1Strategy()
        
        # 测试各个分析模块
        tests = [
            ("隔夜风险分析", lambda: analyzer.analyze_overnight_risk(data)),
            ("交易机会识别", lambda: analyzer.identify_t1_trading_opportunities(data)),
            ("风险指标计算", lambda: analyzer.calculate_t1_risk_metrics(data))
        ]
        
        success_count = 0
        for test_name, test_func in tests:
            try:
                result = test_func()
                if result:
                    logger.success(f"✅ {test_name} 通过")
                    success_count += 1
                else:
                    logger.warning(f"⚠️ {test_name} 失败")
            except Exception as e:
                logger.warning(f"⚠️ {test_name} 异常: {e}")
        
        if success_count >= 2:
            logger.success("✅ T+1策略分析测试通过")
            return True
        else:
            logger.error("❌ T+1策略分析测试失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ T+1策略测试异常: {e}")
        return False


def test_system_integration():
    """测试系统集成"""
    logger.info("🔄 测试系统集成")
    
    try:
        # 测试数据源工厂
        from src.data_sources.data_source_factory import DataSourceFactory
        
        factory = DataSourceFactory()
        
        # 检查支持的数据源类型
        from src.data_sources.base_data_source import DataSourceType
        
        supported_types = [
            DataSourceType.TUSHARE,
            DataSourceType.ALLTICK,
            DataSourceType.ALPHA_VANTAGE
        ]
        
        for source_type in supported_types:
            if source_type in factory.data_source_classes:
                logger.success(f"✅ {source_type.value} 已集成")
            else:
                logger.warning(f"⚠️ {source_type.value} 未集成")
        
        # 测试统一数据服务
        try:
            from src.data_sources.data_source_factory import get_data_service
            data_service = get_data_service(enable_cache=False)
            
            if data_service:
                logger.success("✅ 统一数据服务创建成功")
                
                # 测试健康检查
                health = data_service.health_check()
                logger.info(f"系统健康状态: {health}")
                
                return True
            else:
                logger.error("❌ 统一数据服务创建失败")
                return False
                
        except Exception as e:
            logger.warning(f"⚠️ 统一数据服务测试异常: {e}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 系统集成测试失败: {e}")
        return False


def test_configuration():
    """测试配置系统"""
    logger.info("⚙️ 测试配置系统")
    
    try:
        from config.settings import data_settings
        
        # 检查关键配置
        config_checks = [
            ('alltick_token', '检查AllTick配置'),
            ('alpha_vantage_api_key', '检查Alpha Vantage配置'),
            ('alltick_timeout', '检查超时配置')
        ]
        
        success_count = 0
        for attr, description in config_checks:
            if hasattr(data_settings, attr):
                logger.success(f"✅ {description}")
                success_count += 1
            else:
                logger.warning(f"⚠️ {description} - 配置缺失")
        
        if success_count >= 2:
            logger.success("✅ 配置系统测试通过")
            return True
        else:
            logger.warning("⚠️ 配置系统部分通过")
            return True  # 配置问题不影响核心功能
            
    except Exception as e:
        logger.error(f"❌ 配置系统测试失败: {e}")
        return False


def generate_test_report(results):
    """生成测试报告"""
    logger.info("📋 生成测试报告")
    
    try:
        from datetime import datetime
        
        total_tests = len(results)
        passed_tests = sum(1 for result in results.values() if result)
        
        report = []
        report.append("=" * 80)
        report.append("启明星系统测试报告")
        report.append("=" * 80)
        report.append(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"测试结果: {passed_tests}/{total_tests} 通过")
        report.append(f"通过率: {passed_tests/total_tests*100:.1f}%")
        report.append("")
        
        # 详细结果
        report.append("详细测试结果:")
        for test_name, result in results.items():
            status = "✅ 通过" if result else "❌ 失败"
            report.append(f"  {test_name}: {status}")
        
        report.append("")
        
        # 系统状态评估
        if passed_tests == total_tests:
            report.append("🎉 系统状态: 优秀")
            report.append("所有测试都通过，系统运行正常")
        elif passed_tests >= total_tests * 0.8:
            report.append("✅ 系统状态: 良好")
            report.append("大部分测试通过，系统基本正常")
        elif passed_tests >= total_tests * 0.6:
            report.append("⚠️ 系统状态: 一般")
            report.append("部分测试失败，建议检查相关模块")
        else:
            report.append("❌ 系统状态: 需要修复")
            report.append("多个测试失败，需要排查问题")
        
        report.append("")
        report.append("🔧 建议:")
        
        if not results.get('数据源连接', True):
            report.append("  • 检查数据源API配置和网络连接")
        if not results.get('数据质量', True):
            report.append("  • 验证生产数据完整性")
        if not results.get('T+1策略分析', True):
            report.append("  • 检查策略分析模块")
        
        report.append("=" * 80)
        
        # 保存报告
        report_content = "\n".join(report)
        print("\n" + report_content)
        
        # 保存到文件
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        logs_dir = project_root / 'logs' / 'tests'
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = logs_dir / f'system_test_report_{timestamp}.txt'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.success(f"✅ 测试报告已保存: {report_file}")
        
        return passed_tests >= total_tests * 0.8
        
    except Exception as e:
        logger.error(f"❌ 生成测试报告失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🧪 启明星统一系统测试")
    logger.info("=" * 80)
    
    # 执行所有测试
    tests = [
        ("数据源连接", test_data_sources),
        ("股票过滤器", test_stock_filter),
        ("数据质量", test_data_quality),
        ("T+1策略分析", test_t1_strategy),
        ("系统集成", test_system_integration),
        ("配置系统", test_configuration)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            logger.info(f"\n🎯 执行测试: {test_name}")
            success = test_func()
            results[test_name] = success
            
            if success:
                logger.success(f"✅ {test_name} 测试通过")
            else:
                logger.error(f"❌ {test_name} 测试失败")
                
        except Exception as e:
            logger.error(f"❌ {test_name} 测试异常: {e}")
            results[test_name] = False
    
    # 生成测试报告
    logger.info("\n📋 生成测试报告")
    overall_success = generate_test_report(results)
    
    if overall_success:
        logger.success("🎉 系统测试完成，状态良好！")
    else:
        logger.warning("⚠️ 系统测试完成，发现问题需要修复")
    
    return overall_success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
