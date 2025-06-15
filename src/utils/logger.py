"""
日志配置模块
"""
import sys
from pathlib import Path
from loguru import logger
from config.settings import app_settings


def setup_logger():
    """配置日志系统"""
    
    # 移除默认的日志处理器
    logger.remove()
    
    # 确保日志目录存在
    app_settings.logs_dir.mkdir(exist_ok=True)
    
    # 控制台日志格式
    console_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )
    
    # 文件日志格式
    file_format = (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
        "{level: <8} | "
        "{name}:{function}:{line} | "
        "{message}"
    )
    
    # 添加控制台处理器
    logger.add(
        sys.stdout,
        format=console_format,
        level=app_settings.log_level,
        colorize=True,
        backtrace=True,
        diagnose=True
    )
    
    # 添加文件处理器 - 所有日志
    logger.add(
        app_settings.logs_dir / "qiming_star.log",
        format=file_format,
        level="DEBUG",
        rotation=app_settings.log_rotation,
        retention=app_settings.log_retention,
        compression="zip",
        backtrace=True,
        diagnose=True
    )
    
    # 添加错误日志文件处理器
    logger.add(
        app_settings.logs_dir / "errors.log",
        format=file_format,
        level="ERROR",
        rotation=app_settings.log_rotation,
        retention=app_settings.log_retention,
        compression="zip",
        backtrace=True,
        diagnose=True
    )
    
    # 添加数据处理日志文件处理器
    logger.add(
        app_settings.logs_dir / "data_processing.log",
        format=file_format,
        level="INFO",
        rotation=app_settings.log_rotation,
        retention=app_settings.log_retention,
        compression="zip",
        filter=lambda record: "data" in record["name"].lower() or "analyzer" in record["name"].lower()
    )
    
    # 添加交易信号日志文件处理器
    logger.add(
        app_settings.logs_dir / "trading_signals.log",
        format=file_format,
        level="INFO",
        rotation=app_settings.log_rotation,
        retention=app_settings.log_retention,
        compression="zip",
        filter=lambda record: "signal" in record["name"].lower() or "fusion" in record["name"].lower()
    )
    
    logger.info(f"日志系统已初始化，日志级别: {app_settings.log_level}")
    logger.info(f"日志目录: {app_settings.logs_dir}")


def get_logger(name: str):
    """获取指定名称的日志器"""
    return logger.bind(name=name)


# 初始化日志系统
setup_logger()


if __name__ == "__main__":
    # 测试日志系统
    test_logger = get_logger("test")
    test_logger.debug("这是一条调试信息")
    test_logger.info("这是一条信息")
    test_logger.warning("这是一条警告")
    test_logger.error("这是一条错误信息")
    test_logger.success("这是一条成功信息")
