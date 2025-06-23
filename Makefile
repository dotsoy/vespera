# 启明星项目 Makefile (四维分析系统)

.PHONY: help install start stop restart logs clean test dashboard sample-data test-system venv

# 变量定义
PYTHON = python3
PIP = pip3
DOCKER = docker
DOCKER_COMPOSE = docker-compose

# 默认目标
.DEFAULT_GOAL := help

# 帮助信息
help:
	@echo "启明星股票分析系统 (四维分析) - 可用命令:"
	@echo ""
	@echo "🚀 快速开始:"
	@echo "  quick-start - 快速启动整个系统"
	@echo "  sample-data - 生成样本数据"
	@echo "  test-system - 完整系统测试"
	@echo ""
	@echo "🔧 服务管理:"
	@echo "  install     - 安装依赖"
	@echo "  start       - 启动所有服务"
	@echo "  stop        - 停止所有服务"
	@echo "  restart     - 重启所有服务"
	@echo "  logs        - 查看服务日志"
	@echo "  status      - 查看服务状态"
	@echo ""
	@echo "📊 应用访问:"
	@echo "  dashboard   - 启动仪表盘"
	@echo ""
	@echo "🧪 测试与维护:"
	@echo "  test        - 运行基础测试"
	@echo "  test-tulipy - 测试 Tulipy 技术分析库"
	@echo "  clean       - 清理临时文件"
	@echo "  backup      - 备份数据库"
	@echo "  cleanup-backups - 清理过期备份，只保留最新"
	@echo "  init-db     - 初始化数据库"

# 安装依赖
install:
	@echo "安装 Python 依赖..."
	$(PIP) install -r requirements.txt
	@echo "依赖安装完成"

# 启动服务
start:
	@echo "启动 Docker 服务..."
	$(DOCKER_COMPOSE) up -d
	@echo "服务启动完成"

# 停止服务
stop:
	@echo "停止 Docker 服务..."
	$(DOCKER_COMPOSE) down
	@echo "服务已停止"

# 重启服务
restart: stop start

# 查看日志
logs:
	$(DOCKER_COMPOSE) logs -f

# 启动仪表盘
dashboard:
	@echo "🚀 启动 Streamlit 仪表盘..."
	@echo "📍 访问地址: http://localhost:8501"
	@echo "📊 Marimo研究室已集成到侧边栏"
	@echo "🔄 正在恢复数据库..."
	@$(PYTHON) scripts/restore_all.py
	@if [ -d ".venv" ]; then \
		source .venv/bin/activate && $(PYTHON) scripts/run_dashboard_v2.py; \
	else \
		echo "❌ 错误: 未找到虚拟环境 (.venv)"; \
		echo "请先运行: $(PYTHON) -m venv .venv && source .venv/bin/activate && make install"; \
		exit 1; \
	fi

# 注意: Airflow 已移除
# airflow 命令已移除，原因：性能问题，待日后重新考虑

# 运行测试
test:
	@echo "运行测试..."
	$(PYTHON) -m pytest tests/ -v

# 清理临时文件
clean:
	@echo "清理临时文件..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type f -name "*.log" -delete
	@echo "清理完成"

# 快速启动
quick-start:
	@echo "快速启动启明星系统..."
	$(PYTHON) scripts/quick_start.py

# 生成样本数据
sample-data:
	@echo "生成样本数据..."
	$(PYTHON) scripts/fetch_sample_data.py

# 完整系统测试
test-system:
	@echo "运行完整系统测试..."
	$(PYTHON) scripts/test_full_system.py

# 测试 Tulipy
test-tulipy:
	@echo "测试 Tulipy 技术分析库..."
	$(PYTHON) scripts/test_tulipy.py

# 初始化数据库
init-db:
	@echo "初始化数据库..."
	$(PYTHON) scripts/init_database.py

# 运行分析任务
run-analysis:
	@echo "运行五维分析任务..."
	$(PYTHON) -c "from src.analyzers.technical_analyzer import TechnicalAnalyzer; from datetime import datetime; analyzer = TechnicalAnalyzer(); print('技术分析完成')"

# 生成交易信号
generate-signals:
	@echo "生成交易信号..."
	$(PYTHON) -c "from src.fusion.signal_fusion_engine import SignalFusionEngine; from datetime import datetime; engine = SignalFusionEngine(); print('信号生成完成')"

# 构建 Docker 镜像
build:
	@echo "构建 Docker 镜像..."
	$(DOCKER_COMPOSE) build

# 查看服务状态
status:
	@echo "服务状态:"
	$(DOCKER_COMPOSE) ps

# 进入 PostgreSQL
psql:
	$(DOCKER_COMPOSE) exec postgres psql -U qiming_user -d qiming_star

# 进入 Redis
redis-cli:
	$(DOCKER_COMPOSE) exec redis redis-cli -a qiming_redis_2024

# 备份数据库
backup:
	@echo "备份数据库..."
	mkdir -p data/backups
	$(DOCKER_COMPOSE) exec postgres pg_dump -U qiming_user qiming_star > data/backups/backup_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "备份完成"

# 清理过期备份
cleanup-backups:
	@echo "清理过期备份，只保留最新备份..."
	$(PYTHON) scripts/cleanup_old_backups.py
	@echo "备份清理完成"

# 恢复数据库
restore:
	@echo "请指定备份文件: make restore-file BACKUP_FILE=data/backups/backup_xxx.sql"

restore-file:
	@echo "恢复数据库..."
	$(DOCKER_COMPOSE) exec -T postgres psql -U qiming_user qiming_star < $(BACKUP_FILE)
	@echo "恢复完成"

# 创建虚拟环境
venv:
	@if [ ! -d ".venv" ]; then \
		$(PYTHON) -m venv .venv; \
		echo "✅ 虚拟环境 .venv 已创建"; \
	else \
		echo "✅ 虚拟环境 .venv 已存在"; \
	fi
	@echo "要激活虚拟环境，请运行："
	@echo "source .venv/bin/activate"
