#!/usr/bin/env python3
"""
启动新版本Dashboard的脚本
"""
import sys
import subprocess
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("dashboard_launcher")


def main():
    """启动Dashboard v2.0"""
    logger.info("🚀 启动量化投资分析平台 v2.0")
    
    try:
        # Dashboard应用路径
        app_path = project_root / "dashboard" / "app.py"
        
        if not app_path.exists():
            logger.error(f"Dashboard应用文件不存在: {app_path}")
            return False
        
        # 启动Streamlit应用
        cmd = [
            sys.executable, "-m", "streamlit", "run",
            str(app_path),
            "--server.port", "8502",
            "--server.address", "0.0.0.0",
            "--server.headless", "true",
            "--browser.gatherUsageStats", "false"
        ]

        logger.info("正在启动Streamlit服务器...")
        logger.info(f"访问地址: http://localhost:8502")
        logger.info("按 Ctrl+C 停止服务")
        
        # 运行命令
        subprocess.run(cmd, cwd=project_root)
        
    except KeyboardInterrupt:
        logger.info("用户中断，正在关闭服务...")
        return True
    except Exception as e:
        logger.error(f"启动Dashboard失败: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
