#!/usr/bin/env python3
"""
Marimo研究室功能演示
展示完整的研究室功能和使用流程
"""
import sys
import time
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.launch_marimo import MarimoLauncher
from src.utils.logger import get_logger

logger = get_logger("marimo_lab_demo")


def demo_marimo_lab():
    """演示Marimo研究室功能"""
    logger.info("🎬 Marimo研究室功能演示")
    logger.info("=" * 80)
    
    try:
        # 创建启动器
        launcher = MarimoLauncher()
        
        # 1. 检查系统状态
        logger.info("🔍 步骤1: 检查系统状态")
        
        marimo_installed = launcher.check_marimo_installed()
        if marimo_installed:
            logger.success("✅ Marimo已安装")
        else:
            logger.error("❌ Marimo未安装，请运行: pip install marimo")
            return False
        
        # 2. 展示可用笔记本
        logger.info("\n📚 步骤2: 展示可用笔记本")
        notebooks = launcher.get_available_notebooks()
        
        if notebooks:
            logger.info(f"发现 {len(notebooks)} 个研究笔记本:")
            for i, nb in enumerate(notebooks, 1):
                logger.info(f"  {i}. {nb['name']}")
                logger.info(f"     修改时间: {nb['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"     文件大小: {nb['size']} 字节")
        else:
            logger.warning("未发现笔记本，将创建示例笔记本")
            launcher.create_notebooks_directory()
            notebooks = launcher.get_available_notebooks()
        
        # 3. 演示笔记本启动
        if notebooks:
            logger.info("\n🚀 步骤3: 演示笔记本启动")
            demo_notebook = notebooks[0]['name']
            
            logger.info(f"启动演示笔记本: {demo_notebook}")
            result = launcher.launch_notebook(demo_notebook)
            
            if result['success']:
                logger.success(f"✅ 启动成功!")
                logger.info(f"📍 访问地址: {result['url']}")
                logger.info(f"🔌 运行端口: {result['port']}")
                
                # 4. 展示运行状态
                logger.info("\n📊 步骤4: 展示运行状态")
                time.sleep(2)  # 等待启动完成
                
                running_notebooks = launcher.list_running_notebooks()
                if running_notebooks:
                    logger.info("当前运行中的笔记本:")
                    for nb in running_notebooks:
                        logger.info(f"  • {nb['notebook']}")
                        logger.info(f"    端口: {nb['port']}")
                        logger.info(f"    地址: {nb['url']}")
                        logger.info(f"    启动时间: {nb['start_time'].strftime('%H:%M:%S')}")
                
                # 5. 演示停止功能
                logger.info("\n⏹️ 步骤5: 演示停止功能")
                logger.info("等待5秒后停止笔记本...")
                time.sleep(5)
                
                stop_result = launcher.stop_notebook(demo_notebook)
                if stop_result['success']:
                    logger.success(f"✅ {stop_result['message']}")
                else:
                    logger.error(f"❌ {stop_result['message']}")
                
            else:
                logger.error(f"❌ 启动失败: {result['message']}")
                return False
        
        # 6. 展示管理功能
        logger.info("\n🔧 步骤6: 展示管理功能")
        
        # 端口管理
        try:
            available_port = launcher.find_available_port()
            logger.info(f"下一个可用端口: {available_port}")
        except RuntimeError as e:
            logger.warning(f"端口查找: {e}")
        
        # 目录信息
        logger.info(f"笔记本目录: {launcher.notebooks_dir}")
        logger.info(f"端口范围: {launcher.base_port}-{launcher.max_port}")
        
        # 7. 使用建议
        logger.info("\n💡 步骤7: 使用建议")
        logger.info("Marimo研究室使用建议:")
        logger.info("  1. 启动Streamlit仪表盘: make dashboard")
        logger.info("  2. 在左侧边栏找到'🔬 Marimo 研究室'")
        logger.info("  3. 点击笔记本旁的🚀按钮启动")
        logger.info("  4. 点击🌐按钮在新标签页打开")
        logger.info("  5. 使用⏹️按钮停止不需要的笔记本")
        
        logger.success("🎉 Marimo研究室功能演示完成!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 演示过程异常: {e}")
        return False


def show_integration_demo():
    """展示集成效果"""
    logger.info("\n🔗 Streamlit集成演示")
    logger.info("=" * 50)
    
    logger.info("Marimo研究室已完美集成到Streamlit仪表盘中:")
    logger.info("")
    logger.info("📱 侧边栏功能:")
    logger.info("  • 📖 可用笔记本列表")
    logger.info("  • 🚀 一键启动按钮")
    logger.info("  • 🏃 运行状态显示")
    logger.info("  • 🌐 快速访问链接")
    logger.info("  • ⏹️ 停止控制按钮")
    logger.info("  • ➕ 快速创建功能")
    logger.info("")
    logger.info("🖥️ 管理面板功能:")
    logger.info("  • 📚 笔记本文件管理")
    logger.info("  • 🏃 运行状态监控")
    logger.info("  • ⚙️ 系统设置配置")
    logger.info("  • 📊 性能监控")
    logger.info("")
    logger.info("🎯 核心优势:")
    logger.info("  • 🔄 生产环境与研究环境无缝切换")
    logger.info("  • 📊 交互式数据科学分析")
    logger.info("  • 🚀 一键启动，即开即用")
    logger.info("  • 🔧 完善的管理和监控功能")


def show_notebook_templates():
    """展示笔记本模板"""
    logger.info("\n📚 笔记本模板展示")
    logger.info("=" * 50)
    
    templates = {
        "策略回测模板": {
            "文件": "strategy_backtest_template.py",
            "用途": "测试和验证交易策略",
            "特点": ["交互式参数调整", "实时策略信号", "完整回测分析", "风险收益评估"]
        },
        "个股深度分析": {
            "文件": "specific_stock_deep_dive.py", 
            "用途": "单只股票的全面分析",
            "特点": ["技术分析图表", "基本面评估", "综合投资建议", "多维度评分"]
        },
        "市场情绪分析": {
            "文件": "market_sentiment_analysis.py",
            "用途": "市场整体情绪分析",
            "特点": ["恐慌贪婪指数", "市场广度分析", "资金流向追踪", "情绪交易信号"]
        }
    }
    
    for name, info in templates.items():
        logger.info(f"📖 {name}")
        logger.info(f"   文件: {info['文件']}")
        logger.info(f"   用途: {info['用途']}")
        logger.info(f"   特点: {', '.join(info['特点'])}")
        logger.info("")


def main():
    """主函数"""
    logger.info("🎬 启明星 Marimo研究室 - 完整功能演示")
    logger.info("=" * 80)
    
    try:
        # 1. 功能演示
        demo_success = demo_marimo_lab()
        
        # 2. 集成展示
        show_integration_demo()
        
        # 3. 模板展示
        show_notebook_templates()
        
        # 4. 总结
        logger.info("\n🎊 演示总结")
        logger.info("=" * 50)
        
        if demo_success:
            logger.success("✅ 所有功能演示成功!")
            
            logger.info("\n🚀 立即体验:")
            logger.info("1. 运行: make dashboard")
            logger.info("2. 访问: http://localhost:8501")
            logger.info("3. 在左侧边栏找到'🔬 Marimo 研究室'")
            logger.info("4. 点击🚀按钮启动您的第一个研究笔记本!")
            
            logger.info("\n📖 详细文档:")
            logger.info("• 使用指南: docs/MARIMO_RESEARCH_LAB_GUIDE.md")
            logger.info("• 技术文档: scripts/launch_marimo.py")
            logger.info("• 组件代码: dashboard/components/marimo_lab.py")
            
            logger.info("\n🎯 核心价值:")
            logger.info("• 🔬 专业的交互式数据科学环境")
            logger.info("• 🔄 生产与研究环境无缝集成")
            logger.info("• 📊 丰富的量化分析模板")
            logger.info("• 🚀 一键启动，即开即用")
            
        else:
            logger.warning("⚠️ 部分功能演示失败")
            logger.info("请检查Marimo安装状态和系统配置")
        
        logger.info("\n🌟 Marimo研究室 - 让量化研究更高效、更直观、更有趣!")
        
        return demo_success
        
    except Exception as e:
        logger.error(f"❌ 演示异常: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
