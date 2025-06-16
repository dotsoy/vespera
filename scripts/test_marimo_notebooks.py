#!/usr/bin/env python3
"""
测试所有Marimo笔记本的格式和启动能力
"""
import sys
import time
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.launch_marimo import MarimoLauncher
from src.utils.logger import get_logger

logger = get_logger("marimo_notebook_test")


def test_notebook_format(notebook_path):
    """测试笔记本格式是否正确"""
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 检查必要的Marimo格式
        required_elements = [
            'import marimo',
            '__generated_with',
            'app = marimo.App',
            '@app.cell',
            'if __name__ == "__main__":',
            'app.run()'
        ]
        
        missing_elements = []
        for element in required_elements:
            if element not in content:
                missing_elements.append(element)
        
        if missing_elements:
            return False, f"缺少必要元素: {', '.join(missing_elements)}"
        
        return True, "格式正确"
        
    except Exception as e:
        return False, f"读取文件失败: {e}"


def test_notebook_launch(launcher, notebook_name):
    """测试笔记本启动"""
    try:
        # 启动笔记本
        result = launcher.launch_notebook(notebook_name)
        
        if not result['success']:
            return False, result['message']
        
        # 等待启动完成
        time.sleep(3)
        
        # 检查是否在运行
        running_notebooks = launcher.list_running_notebooks()
        is_running = any(nb['notebook'] == notebook_name for nb in running_notebooks)
        
        if not is_running:
            return False, "启动后未找到运行中的进程"
        
        # 停止笔记本
        stop_result = launcher.stop_notebook(notebook_name)
        if not stop_result['success']:
            logger.warning(f"停止笔记本失败: {stop_result['message']}")
        
        return True, f"启动成功，端口: {result['port']}"
        
    except Exception as e:
        return False, f"启动测试异常: {e}"


def main():
    """主测试函数"""
    logger.info("🧪 开始测试Marimo笔记本")
    logger.info("=" * 60)
    
    try:
        # 创建启动器
        launcher = MarimoLauncher()
        
        # 检查Marimo安装
        if not launcher.check_marimo_installed():
            logger.error("❌ Marimo未安装，请运行: pip install marimo")
            return False
        
        logger.success("✅ Marimo已安装")
        
        # 获取所有笔记本
        notebooks = launcher.get_available_notebooks()
        
        if not notebooks:
            logger.error("❌ 未找到任何笔记本")
            return False
        
        logger.info(f"📚 发现 {len(notebooks)} 个笔记本")
        
        # 测试结果统计
        total_tests = 0
        passed_tests = 0
        failed_tests = []
        
        # 测试每个笔记本
        for notebook in notebooks:
            notebook_name = notebook['name']
            notebook_path = launcher.notebooks_dir / notebook_name
            
            logger.info(f"\n🔍 测试笔记本: {notebook_name}")
            logger.info("-" * 40)
            
            # 1. 格式测试
            total_tests += 1
            format_ok, format_msg = test_notebook_format(notebook_path)
            
            if format_ok:
                logger.success(f"✅ 格式测试: {format_msg}")
                passed_tests += 1
            else:
                logger.error(f"❌ 格式测试: {format_msg}")
                failed_tests.append(f"{notebook_name} - 格式错误: {format_msg}")
                continue  # 格式错误就不测试启动了
            
            # 2. 启动测试
            total_tests += 1
            launch_ok, launch_msg = test_notebook_launch(launcher, notebook_name)
            
            if launch_ok:
                logger.success(f"✅ 启动测试: {launch_msg}")
                passed_tests += 1
            else:
                logger.error(f"❌ 启动测试: {launch_msg}")
                failed_tests.append(f"{notebook_name} - 启动失败: {launch_msg}")
        
        # 测试结果总结
        logger.info("\n" + "=" * 60)
        logger.info("🎯 测试结果总结")
        logger.info("=" * 60)
        
        logger.info(f"📊 总测试数: {total_tests}")
        logger.info(f"✅ 通过测试: {passed_tests}")
        logger.info(f"❌ 失败测试: {total_tests - passed_tests}")
        logger.info(f"📈 通过率: {passed_tests / total_tests * 100:.1f}%")
        
        if failed_tests:
            logger.info("\n❌ 失败详情:")
            for failure in failed_tests:
                logger.error(f"  • {failure}")
        
        # 最终结果
        success_rate = passed_tests / total_tests
        
        if success_rate == 1.0:
            logger.success("\n🎉 所有测试通过！Marimo笔记本系统运行正常")
            
            logger.info("\n🚀 现在可以启动仪表盘体验完整功能:")
            logger.info("1. 运行: make dashboard")
            logger.info("2. 访问: http://localhost:8501")
            logger.info("3. 在左侧边栏找到'🔬 Marimo 研究室'")
            logger.info("4. 点击🚀按钮启动笔记本")
            
            return True
            
        elif success_rate >= 0.8:
            logger.warning("\n⚠️ 大部分测试通过，但存在一些问题")
            return False
            
        else:
            logger.error("\n💥 测试失败过多，请检查系统配置")
            return False
        
    except Exception as e:
        logger.error(f"❌ 测试过程异常: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
