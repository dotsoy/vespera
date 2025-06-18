#!/bin/bash

# 等待 ClickHouse 服务启动
until clickhouse-client --host localhost --port 9000 --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --query "SELECT 1" > /dev/null 2>&1; do
    echo "等待 ClickHouse 服务启动..."
    sleep 1
done

# 检查是否存在备份文件
if [ -d "/backups" ] && [ "$(ls -A /backups)" ]; then
    echo "发现备份文件，开始恢复..."
    
    # 获取最新的备份文件
    latest_backup=$(ls -t /backups/*.gz | head -n1)
    
    if [ -n "$latest_backup" ]; then
        echo "使用备份文件: $latest_backup"
        
        # 解压备份文件
        gunzip -c "$latest_backup" > /tmp/restore.sql
        
        # 恢复数据库
        clickhouse-client --host localhost --port 9000 --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD < /tmp/restore.sql
        
        # 清理临时文件
        rm /tmp/restore.sql
        
        echo "数据库恢复完成"
    else
        echo "没有找到有效的备份文件"
    fi
else
    echo "没有找到备份文件，跳过恢复步骤"
fi 