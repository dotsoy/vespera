#!/usr/bin/env python3
"""
启明星项目快速启动脚本
"""
import sys
import os
import subprocess
import time
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("quick_start")


def check_docker():
    """检查 Docker 是否安装并运行"""
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            logger.success("Docker 已安装")
            return True
        else:
            logger.error("Docker 未安装或无法访问")
            return False
    except FileNotFoundError:
        logger.error("Docker 未安装")
        return False


def check_docker_compose():
    """检查 Docker Compose 是否安装"""
    try:
        result = subprocess.run(['docker-compose', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            logger.success("Docker Compose 已安装")
            return True
        else:
            # 尝试新版本的命令
            result = subprocess.run(['docker', 'compose', 'version'], capture_output=True, text=True)
            if result.returncode == 0:
                logger.success("Docker Compose (新版本) 已安装")
                return True
            else:
                logger.error("Docker Compose 未安装")
                return False
    except FileNotFoundError:
        logger.error("Docker Compose 未安装")
        return False


def create_env_file():
    """创建 .env 文件"""
    env_file = project_root / ".env"
    env_example = project_root / ".env.example"
    
    if env_file.exists():
        logger.info(".env 文件已存在")
        return True
    
    if env_example.exists():
        try:
            # 复制示例文件
            with open(env_example, 'r', encoding='utf-8') as f:
                content = f.read()
            
            with open(env_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.success(".env 文件已创建")
            logger.warning("请编辑 .env 文件，填入正确的配置值（特别是 TUSHARE_TOKEN）")
            return True
        except Exception as e:
            logger.error(f"创建 .env 文件失败: {e}")
            return False
    else:
        logger.error(".env.example 文件不存在")
        return False


def start_docker_services():
    """启动 Docker 服务"""
    logger.info("启动 Docker 服务...")
    
    try:
        # 切换到项目根目录
        os.chdir(project_root)
        
        # 启动服务
        result = subprocess.run(
            ['docker-compose', 'up', '-d'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.success("Docker 服务启动成功")
            return True
        else:
            logger.error(f"Docker 服务启动失败: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"启动 Docker 服务时出错: {e}")
        return False


def wait_for_services():
    """等待服务启动完成"""
    logger.info("等待服务启动完成...")
    
    max_wait = 60  # 最大等待60秒
    wait_time = 0
    
    while wait_time < max_wait:
        try:
            from src.utils.database import get_db_manager
            db_manager = get_db_manager()
            
            # 测试数据库连接
            results = db_manager.test_connections()
            
            if results.get('postgres', False):
                logger.success("服务启动完成")
                return True
            else:
                logger.info(f"等待服务启动... ({wait_time}s)")
                time.sleep(5)
                wait_time += 5
                
        except Exception:
            logger.info(f"等待服务启动... ({wait_time}s)")
            time.sleep(5)
            wait_time += 5
    
    logger.warning("服务启动超时，但可能仍在启动中")
    return False


def install_dependencies():
    """安装 Python 依赖"""
    logger.info("安装 Python 依赖...")

    try:
        result = subprocess.run(
            [sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'],
            cwd=project_root,
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            logger.success("Python 依赖安装成功")

            # 测试 Tulipy 安装
            logger.info("测试 Tulipy 技术分析库...")
            try:
                import tulipy as ti
                logger.success("✅ Tulipy 安装成功")
            except ImportError:
                logger.warning("⚠️ Tulipy 导入失败，尝试重新安装...")
                subprocess.run([sys.executable, '-m', 'pip', 'install', 'tulipy'],
                             cwd=project_root)

            return True
        else:
            logger.error(f"Python 依赖安装失败: {result.stderr}")
            return False

    except Exception as e:
        logger.error(f"安装依赖时出错: {e}")
        return False


def initialize_database():
    """初始化数据库"""
    logger.info("初始化数据库...")
    
    try:
        result = subprocess.run(
            [sys.executable, 'scripts/init_database.py'],
            cwd=project_root,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.success("数据库初始化成功")
            return True
        else:
            logger.error(f"数据库初始化失败: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"初始化数据库时出错: {e}")
        return False


def start_dashboard():
    """启动仪表盘"""
    logger.info("启动 Streamlit 仪表盘...")
    
    try:
        # 在后台启动 Streamlit
        subprocess.Popen(
            [sys.executable, '-m', 'streamlit', 'run', 'dashboard/app.py'],
            cwd=project_root
        )
        
        logger.success("Streamlit 仪表盘已启动")
        logger.info("请在浏览器中访问: http://localhost:8501")
        return True
        
    except Exception as e:
        logger.error(f"启动仪表盘时出错: {e}")
        return False


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("🌟 启明星股票分析系统 - 快速启动")
    logger.info("=" * 60)
    
    # 1. 检查 Docker
    if not check_docker():
        logger.error("请先安装 Docker")
        return False
    
    if not check_docker_compose():
        logger.error("请先安装 Docker Compose")
        return False
    
    # 2. 创建环境文件
    if not create_env_file():
        logger.error("环境文件创建失败")
        return False
    
    # 3. 安装依赖
    if not install_dependencies():
        logger.error("依赖安装失败")
        return False
    
    # 4. 启动 Docker 服务
    if not start_docker_services():
        logger.error("Docker 服务启动失败")
        return False
    
    # 5. 等待服务启动
    wait_for_services()
    
    # 6. 初始化数据库
    if not initialize_database():
        logger.warning("数据库初始化失败，但系统可能仍可运行")
    
    # 7. 启动仪表盘
    start_dashboard()
    
    logger.info("=" * 60)
    logger.success("🎉 启明星系统启动完成！")
    logger.info("=" * 60)
    
    logger.info("📊 访问地址:")
    logger.info("  - Streamlit 仪表盘: http://localhost:8501")
    logger.info("  - Airflow 管理界面: http://localhost:8080")
    logger.info("  - PostgreSQL: localhost:5432")
    logger.info("  - ClickHouse: localhost:8123")
    logger.info("  - Redis: localhost:6379")
    
    logger.info("\n🔧 下一步操作:")
    logger.info("  1. 编辑 .env 文件，填入 Tushare Token")
    logger.info("  2. 运行 Tulipy 测试: make test-tulipy")
    logger.info("  3. 生成样本数据: make sample-data")
    logger.info("  4. 在 Airflow 中启用 daily_analysis DAG")
    logger.info("  5. 运行完整系统测试: make test-system")

    return True


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
