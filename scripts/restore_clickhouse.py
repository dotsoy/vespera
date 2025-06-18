#!/usr/bin/env python3
"""
ClickHouse 数据库恢复脚本（兼容新版备份）
"""
import os
import sys
import subprocess
from pathlib import Path
import tarfile
import shutil
from loguru import logger

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.settings import db_settings

def restore_clickhouse(backup_file: str = None) -> bool:
    """
    从备份文件恢复 ClickHouse 数据库（先建表再导入数据）
    """
    try:
        backup_dir = project_root / "backups" / "clickhouse"
        if not backup_file:
            backup_files = list(backup_dir.glob("clickhouse_backup_*.tar.gz"))
            if not backup_files:
                raise FileNotFoundError("未找到备份文件")
            backup_file = str(max(backup_files, key=lambda x: x.stat().st_mtime))
        extract_dir = backup_dir / "temp"
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir(exist_ok=True)
        with tarfile.open(backup_file, "r:gz") as tar:
            tar.extractall(path=extract_dir)
        # 1. 先执行所有 .sql 建表语句（用 shell 保证无转义）
        for sql_file in sorted(extract_dir.glob("*.sql")):
            shell_cmd = f"cat '{sql_file}' | docker exec -i vespera-clickhouse-1 clickhouse-client --host={db_settings.clickhouse_host} --port={db_settings.clickhouse_port} --user={db_settings.clickhouse_user} --password={db_settings.clickhouse_password} --database={db_settings.clickhouse_db}"
            ret = os.system(shell_cmd)
            if ret != 0:
                # 检查是否为 TABLE_ALREADY_EXISTS 错误
                with open(sql_file, 'r') as f:
                    sql_content = f.read()
                if 'CREATE TABLE' in sql_content:
                    logger.warning(f"表已存在，跳过: {sql_file.name}")
                    continue
                logger.error(f"建表失败: {sql_file.name}")
                raise Exception(f"建表失败: {sql_file.name}")
            logger.info(f"建表完成: {sql_file.name}")
        # 2. 再导入所有 .tsv 数据文件
        for tsv_file in sorted(extract_dir.glob("*.tsv")):
            # 正确提取表名（支持下划线表名）
            table_name = '_'.join(tsv_file.name.split('_')[:-2])
            # 清空表
            subprocess.run([
                "docker", "exec", "-i", "vespera-clickhouse-1",
                "clickhouse-client",
                f"--host={db_settings.clickhouse_host}",
                f"--port={db_settings.clickhouse_port}",
                f"--user={db_settings.clickhouse_user}",
                f"--password={db_settings.clickhouse_password}",
                f"--database={db_settings.clickhouse_db}",
                "--query", f"TRUNCATE TABLE {table_name}"
            ], check=True)
            logger.info(f"表已清空: {table_name}")
            # 转换日期格式
            temp_file = tsv_file.with_suffix('.temp.tsv')
            with open(tsv_file, 'r') as f_in, open(temp_file, 'w') as f_out:
                for line in f_in:
                    # 将 YYYY-MM-DD 格式转换为 YYYYMMDD
                    parts = line.split('\t')
                    if len(parts) > 0:
                        date_str = parts[0]
                        if '-' in date_str:
                            date_parts = date_str.split('-')
                            parts[0] = f"{date_parts[0]}{date_parts[1]}{date_parts[2]}"
                    f_out.write('\t'.join(parts))
            # 导入转换后的数据
            with open(temp_file, "r") as f:
                subprocess.run([
                    "docker", "exec", "-i", "vespera-clickhouse-1",
                    "clickhouse-client",
                    f"--host={db_settings.clickhouse_host}",
                    f"--port={db_settings.clickhouse_port}",
                    f"--user={db_settings.clickhouse_user}",
                    f"--password={db_settings.clickhouse_password}",
                    f"--database={db_settings.clickhouse_db}",
                    "--query", f"INSERT INTO {table_name} FORMAT TabSeparated"
                ], stdin=f, check=True)
            # 清理临时文件
            temp_file.unlink()
            logger.info(f"数据导入完成: {tsv_file.name}")
        shutil.rmtree(extract_dir)
        logger.info(f"数据库恢复完成: {backup_file}")
        return True
    except Exception as e:
        logger.error(f"恢复 ClickHouse 数据库失败: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("开始恢复 ClickHouse 数据库...")
    if restore_clickhouse():
        logger.info("✅ 恢复成功")
    else:
        logger.error("❌ 恢复失败") 