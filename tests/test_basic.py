"""
基础功能测试
"""
import pytest
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_imports():
    """测试基础模块导入"""
    try:
        from config.settings import app_settings, db_settings
        from src.utils.logger import get_logger
        assert True
    except ImportError as e:
        pytest.fail(f"模块导入失败: {e}")


def test_config_loading():
    """测试配置加载"""
    from config.settings import app_settings, db_settings
    
    assert app_settings.app_name == "启明星股票分析系统"
    assert app_settings.version == "1.0.0"
    assert db_settings.postgres_port == 5432


def test_logger():
    """测试日志系统"""
    from src.utils.logger import get_logger
    
    logger = get_logger("test")
    logger.info("测试日志信息")
    assert True


def test_database_manager_init():
    """测试数据库管理器初始化"""
    try:
        from src.utils.database import DatabaseManager
        manager = DatabaseManager()
        assert manager is not None
    except Exception as e:
        pytest.fail(f"数据库管理器初始化失败: {e}")


if __name__ == "__main__":
    pytest.main([__file__])
