#!/usr/bin/env python3
"""
测试Marimo研究室功能
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.launch_marimo import MarimoLauncher
from src.utils.logger import get_logger

logger = get_logger("test_marimo_lab")


def test_marimo_launcher():
    """测试Marimo启动器"""
    logger.info("🧪 测试Marimo启动器")
    
    try:
        launcher = MarimoLauncher()
        
        # 1. 测试Marimo安装检查
        logger.info("1. 检查Marimo安装状态")
        marimo_installed = launcher.check_marimo_installed()
        logger.info(f"Marimo安装状态: {'✅ 已安装' if marimo_installed else '❌ 未安装'}")
        
        if not marimo_installed:
            logger.warning("Marimo未安装，请运行: pip install marimo")
            return False
        
        # 2. 测试端口查找
        logger.info("2. 测试端口查找")
        try:
            port = launcher.find_available_port()
            logger.success(f"找到可用端口: {port}")
        except RuntimeError as e:
            logger.error(f"端口查找失败: {e}")
            return False
        
        # 3. 测试目录创建
        logger.info("3. 测试目录创建")
        launcher.create_notebooks_directory()
        if launcher.notebooks_dir.exists():
            logger.success(f"notebooks目录已创建: {launcher.notebooks_dir}")
        else:
            logger.error("notebooks目录创建失败")
            return False
        
        # 4. 测试笔记本列表获取
        logger.info("4. 测试笔记本列表获取")
        notebooks = launcher.get_available_notebooks()
        logger.info(f"发现 {len(notebooks)} 个笔记本:")
        for nb in notebooks:
            logger.info(f"  • {nb['name']}")
        
        # 5. 测试笔记本启动（如果有笔记本的话）
        if notebooks:
            logger.info("5. 测试笔记本启动")
            test_notebook = notebooks[0]['name']
            logger.info(f"尝试启动: {test_notebook}")
            
            result = launcher.launch_notebook(test_notebook)
            
            if result['success']:
                logger.success(f"✅ 启动成功: {result['url']}")
                
                # 等待一下然后停止
                import time
                time.sleep(2)
                
                logger.info("6. 测试笔记本停止")
                stop_result = launcher.stop_notebook(test_notebook)
                
                if stop_result['success']:
                    logger.success("✅ 停止成功")
                else:
                    logger.error(f"❌ 停止失败: {stop_result['message']}")
            else:
                logger.error(f"❌ 启动失败: {result['message']}")
        
        logger.success("🎉 Marimo启动器测试完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试异常: {e}")
        return False


def test_marimo_component():
    """测试Marimo组件"""
    logger.info("🧪 测试Marimo组件")
    
    try:
        from dashboard.components.marimo_lab import MarimoLabComponent
        
        # 创建组件实例
        component = MarimoLabComponent()
        logger.success("✅ Marimo组件创建成功")
        
        # 测试启动器
        launcher = component.launcher
        
        # 测试获取笔记本列表
        notebooks = launcher.get_available_notebooks()
        logger.info(f"组件获取到 {len(notebooks)} 个笔记本")
        
        # 测试运行状态
        running = launcher.list_running_notebooks()
        logger.info(f"当前运行中的笔记本: {len(running)} 个")
        
        logger.success("🎉 Marimo组件测试完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 组件测试失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🚀 Marimo研究室功能测试")
    logger.info("=" * 60)
    
    success_count = 0
    total_tests = 2
    
    # 测试启动器
    if test_marimo_launcher():
        success_count += 1
    
    # 测试组件
    if test_marimo_component():
        success_count += 1
    
    # 总结
    logger.info("\n" + "=" * 60)
    logger.info("📊 测试结果总结")
    logger.info("=" * 60)
    logger.info(f"总测试数: {total_tests}")
    logger.info(f"成功测试: {success_count}")
    logger.info(f"成功率: {success_count/total_tests*100:.1f}%")
    
    if success_count == total_tests:
        logger.success("🎉 所有测试通过！Marimo研究室功能正常")
        
        logger.info("\n💡 使用说明:")
        logger.info("1. 运行 streamlit run dashboard/app.py 启动仪表盘")
        logger.info("2. 在左侧边栏找到 '🔬 Marimo 研究室'")
        logger.info("3. 点击笔记本名称旁的 🚀 按钮启动")
        logger.info("4. 在新标签页中访问 Marimo 笔记本")
        logger.info("5. 使用 ⏹️ 按钮停止运行中的笔记本")
        
        return True
    else:
        logger.warning("⚠️ 部分测试失败，请检查相关配置")
        
        if success_count == 0:
            logger.info("\n🔧 故障排除:")
            logger.info("1. 确保已安装 Marimo: pip install marimo")
            logger.info("2. 检查端口 8081-8090 是否可用")
            logger.info("3. 确保有足够的文件系统权限")
        
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
