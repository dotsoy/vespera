#!/usr/bin/env python3
"""
生产数据管理器
统一管理模拟数据清理和生产数据导入
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

logger = get_logger("production_data_manager")


def set_production_environment():
    """设置生产环境变量"""
    logger.info("🔧 设置生产环境变量")

    try:
        # 设置生产环境的token
        os.environ['ALLTICK_TOKEN'] = "5d77b3af30d6b74b6bad3340996cb399-c-app"
        os.environ['ALPHA_VANTAGE_API_KEY'] = "3SHZ17DOQBH5X6IX"

        # 设置其他环境变量
        os.environ['ENVIRONMENT'] = "production"
        os.environ['DATA_SOURCE_PRIORITY'] = "alltick,alpha_vantage"

        logger.success("✅ 生产环境变量设置完成")

        # 显示配置信息
        logger.info("当前配置:")
        logger.info(f"  AllTick Token: {os.environ['ALLTICK_TOKEN'][:20]}...")
        logger.info(f"  Alpha Vantage Key: {os.environ['ALPHA_VANTAGE_API_KEY'][:10]}...")
        logger.info(f"  环境: {os.environ['ENVIRONMENT']}")

        return True

    except Exception as e:
        logger.error(f"❌ 设置环境变量失败: {e}")
        return False


def clear_mock_data():
    """清除模拟数据"""
    logger.info("🗑️ 清除模拟数据")
    
    try:
        # 运行清理脚本
        import subprocess
        
        clear_script = project_root / 'scripts' / 'clear_mock_data.py'
        
        result = subprocess.run(
            [sys.executable, str(clear_script)],
            cwd=project_root,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.success("✅ 模拟数据清理完成")
            return True
        else:
            logger.error(f"❌ 模拟数据清理失败: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 清理模拟数据时出错: {e}")
        return False


def import_production_data():
    """导入生产数据"""
    logger.info("📥 导入生产数据")
    
    try:
        # 运行导入脚本
        import subprocess
        
        import_script = project_root / 'scripts' / 'import_production_data.py'
        
        result = subprocess.run(
            [sys.executable, str(import_script)],
            cwd=project_root,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.success("✅ 生产数据导入完成")
            return True
        else:
            logger.error(f"❌ 生产数据导入失败: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 导入生产数据时出错: {e}")
        return False


def test_data_sources():
    """测试数据源连接"""
    logger.info("🧪 测试数据源连接")
    
    try:
        from src.data_sources.alltick_data_source import AllTickDataSource
        from src.data_sources.alpha_vantage_data_source import AlphaVantageDataSource
        
        # 测试AllTick
        logger.info("测试AllTick连接...")
        alltick = AllTickDataSource(os.environ['ALLTICK_TOKEN'])
        if alltick.initialize():
            logger.success("✅ AllTick连接成功")
            alltick_status = True
        else:
            logger.error("❌ AllTick连接失败")
            alltick_status = False
        
        # 测试Alpha Vantage
        logger.info("测试Alpha Vantage连接...")
        alpha = AlphaVantageDataSource(os.environ['ALPHA_VANTAGE_API_KEY'])
        if alpha.initialize():
            logger.success("✅ Alpha Vantage连接成功")
            alpha_status = True
        else:
            logger.error("❌ Alpha Vantage连接失败")
            alpha_status = False
        
        return alltick_status and alpha_status
        
    except Exception as e:
        logger.error(f"❌ 测试数据源时出错: {e}")
        return False


def create_data_directories():
    """创建数据目录"""
    logger.info("📁 创建数据目录")
    
    try:
        directories = [
            'data/production',
            'data/backup',
            'data/cache',
            'logs/production',
            'backup/config'
        ]
        
        for dir_path in directories:
            full_path = project_root / dir_path
            full_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"✅ 创建目录: {dir_path}")
        
        logger.success("✅ 数据目录创建完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 创建数据目录失败: {e}")
        return False


def generate_summary_report():
    """生成总结报告"""
    logger.info("📊 生成总结报告")
    
    try:
        data_dir = project_root / 'data' / 'production'
        
        report = []
        report.append("=" * 80)
        report.append("生产数据导入总结报告")
        report.append("=" * 80)
        report.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # 检查数据文件
        basic_file = data_dir / 'stock_basic.csv'
        quotes_file = data_dir / 'daily_quotes.csv'
        
        if basic_file.exists():
            import pandas as pd
            basic_data = pd.read_csv(basic_file)
            report.append(f"📋 股票基础信息: {len(basic_data)} 只股票")
            
            if 'ts_code' in basic_data.columns:
                sz_count = basic_data['ts_code'].str.endswith('.SZ').sum()
                sh_count = basic_data['ts_code'].str.endswith('.SH').sum()
                report.append(f"   - 深交所: {sz_count} 只")
                report.append(f"   - 上交所: {sh_count} 只")
        else:
            report.append("❌ 股票基础信息文件不存在")
        
        if quotes_file.exists():
            import pandas as pd
            quotes_data = pd.read_csv(quotes_file)
            report.append(f"📈 历史行情数据: {len(quotes_data)} 条记录")
            
            if not quotes_data.empty:
                unique_stocks = quotes_data['ts_code'].nunique() if 'ts_code' in quotes_data.columns else 0
                report.append(f"   - 涵盖股票: {unique_stocks} 只")
                
                if 'trade_date' in quotes_data.columns:
                    date_range = f"{quotes_data['trade_date'].min()} 到 {quotes_data['trade_date'].max()}"
                    report.append(f"   - 日期范围: {date_range}")
        else:
            report.append("❌ 历史行情数据文件不存在")
        
        report.append("")
        report.append("🔧 数据源配置:")
        report.append(f"   - AllTick Token: {os.environ.get('ALLTICK_TOKEN', 'N/A')[:20]}...")
        report.append(f"   - Alpha Vantage Key: {os.environ.get('ALPHA_VANTAGE_API_KEY', 'N/A')[:10]}...")
        report.append("")
        report.append("📝 过滤条件:")
        report.append("   - 排除ST股票")
        report.append("   - 排除北交所股票")
        report.append("   - 仅包含沪深A股")
        report.append("")
        report.append("🚀 下一步建议:")
        report.append("   1. 验证数据质量")
        report.append("   2. 运行技术分析")
        report.append("   3. 启动定期数据更新")
        report.append("=" * 80)
        
        # 保存报告
        report_content = "\n".join(report)
        
        # 输出到控制台
        print("\n" + report_content)
        
        # 保存到文件
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = project_root / 'logs' / 'production' / f'import_report_{timestamp}.txt'
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.success(f"✅ 报告已保存到: {report_file}")
        return True
        
    except Exception as e:
        logger.error(f"❌ 生成报告失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🌟 启明星生产数据管理器")
    logger.info("=" * 80)
    
    logger.info("操作流程:")
    logger.info("  1. 设置生产环境")
    logger.info("  2. 创建数据目录")
    logger.info("  3. 测试数据源连接")
    logger.info("  4. 清除模拟数据")
    logger.info("  5. 导入生产数据")
    logger.info("  6. 生成总结报告")
    
    # 确认操作
    logger.warning("⚠️ 此操作将清除所有现有数据并导入生产数据")
    
    # 在生产环境中应该要求确认
    # confirm = input("确认执行？(yes/no): ")
    # if confirm.lower() != 'yes':
    #     logger.info("操作已取消")
    #     return False
    
    steps = [
        ("设置生产环境", set_production_environment),
        ("创建数据目录", create_data_directories),
        ("测试数据源连接", test_data_sources),
        ("清除模拟数据", clear_mock_data),
        ("导入生产数据", import_production_data),
        ("生成总结报告", generate_summary_report)
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
                # 某些步骤失败可以继续
                if step_name in ["测试数据源连接"]:
                    logger.warning("⚠️ 继续执行后续步骤")
                else:
                    logger.error("❌ 关键步骤失败，停止执行")
                    break
        except Exception as e:
            logger.error(f"❌ {step_name} 异常: {e}")
            break
    
    # 总结
    logger.info("\n" + "=" * 80)
    logger.info("📊 执行总结")
    logger.info("=" * 80)
    logger.info(f"总步骤数: {total_count}")
    logger.info(f"成功步骤: {success_count}")
    logger.info(f"失败步骤: {total_count - success_count}")
    logger.info(f"成功率: {success_count/total_count*100:.1f}%")
    
    if success_count >= total_count - 1:  # 允许一个步骤失败
        logger.success("🎉 生产数据管理完成！")
        logger.info("\n🚀 系统已切换到生产环境")
        logger.info("数据源: AllTick + Alpha Vantage")
        logger.info("股票范围: 沪深A股（排除ST和北交所）")
    else:
        logger.warning("⚠️ 部分步骤失败，请检查日志")
    
    return success_count >= total_count - 1


if __name__ == "__main__":
    from datetime import datetime
    success = main()
    sys.exit(0 if success else 1)
