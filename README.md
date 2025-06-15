# 启明星 (Qiming Star) - 专业级股票分析工作站

## 项目概述

启明星是一个专业级的股票分析工作站，旨在通过多维度数据分析和智能信号融合，帮助投资者做出更理性的交易决策。

## 核心特性

### 🔍 四维分析框架
- **技术形态分析**: 股价的"势"与"能"
- **资金流分析**: "聪明钱"的动向追踪
- **基本面分析**: 短期催化剂识别
- **宏观环境分析**: 行业周期与市场环境判断

### 🎯 信号融合引擎
采用"资金为王，技术触发"的核心策略：
1. 资金面过滤：筛选全市场资金面最强的股票
2. 技术面触发：寻找明确的技术性买点
3. 信号生成：只有同时满足条件才生成交易信号

### 📊 专业级仪表盘
- 实时股票筛选结果
- 交互式K线图表
- 多维度评分雷达图
- 智能交易建议

## 技术架构

- **数据库**: PostgreSQL + ClickHouse + Redis
- **工作流调度**: 暂时移除 (性能考虑，待日后重新设计)
- **核心语言**: Python 3.9+
- **技术分析**: Tulipy (替代 TA-Lib，安装更简单)
- **前端框架**: Streamlit + Plotly
- **研究工具**: Marimo
- **部署方案**: Docker + Docker Compose

## 快速开始

### 环境要求
- Python 3.9+
- Docker & Docker Compose
- PostgreSQL 14+
- 8GB+ RAM

### 🚀 一键启动
```bash
# 克隆项目
git clone <repository-url>
cd cloudmere-agument

# 快速启动
make quick-start
```

### 🔧 手动安装步骤

1. **环境配置**
```bash
cp .env.example .env
# 编辑 .env 文件，填入 Tushare Token
```

2. **安装依赖**
```bash
make install
```

3. **测试技术分析库**
```bash
make test-tulipy
```

4. **启动服务**
```bash
make start
```

5. **初始化数据库**
```bash
make init-db
```

6. **⚠️ 数据安全警告**
```bash
# 系统已包含真实A股生产数据，请勿运行以下命令：
# make sample-data  # 会覆盖真实数据！
#
# 安全的命令：
make dashboard      # 启动仪表盘（安全）
```

7. **启动仪表盘**
```bash
make dashboard
```

8. **运行完整测试**
```bash
make test-system
```

## 项目结构

```
cloudmere-agument/
├── config/                 # 配置文件
├── data/                   # 数据存储
├── src/                    # 核心源码
│   ├── analyzers/          # 四维分析模块
│   ├── data_sources/       # 数据获取模块
│   ├── fusion/             # 信号融合引擎
│   └── utils/              # 工具函数
├── dashboard/              # Streamlit仪表盘
├── notebooks/              # Marimo研究笔记本
├── scripts/                # 脚本工具
├── tests/                  # 测试代码
├── docker/                 # Docker配置
└── docs/                   # 文档
```

## 🎯 开发进度

- [x] 项目架构设计
- [x] 数据层实现 (PostgreSQL + ClickHouse + Redis)
- [x] 四维分析引擎
  - [x] 技术分析模块 (基于 Tulipy)
  - [x] 资金流分析模块
  - [x] 基本面分析模块
  - [x] 宏观环境分析模块
- [x] 信号融合系统 ("资金为王，技术触发")
- [x] 前端仪表盘 (Streamlit + Plotly)
- [x] 工作流自动化 (Apache Airflow)
- [x] 容器化部署 (Docker + Docker Compose)
- [x] 测试与文档

## 🔧 可用命令

```bash
make help          # 查看所有可用命令
make quick-start    # 快速启动整个系统
make test-tulipy    # 测试 Tulipy 技术分析库
# make sample-data  # ⚠️ 已禁用：会覆盖真实A股数据！
make test-system    # 运行完整系统测试
make dashboard      # 启动仪表盘

## ⚠️ 数据安全重要提醒

**系统已包含真实A股生产数据，请勿运行以下脚本：**

### 🚫 危险脚本（会覆盖真实数据）
- `scripts/import_a_share_data.py` - 会生成模拟数据覆盖真实数据
- `scripts/clear_mock_data.py` - 会清除所有数据
- `scripts/production_data_manager.py` - 会重置生产环境
- `make sample-data` - 会覆盖真实数据

### ✅ 安全脚本（只读分析）
- `scripts/test_a_share_t1_strategy.py` - T+1策略分析测试
- `scripts/validate_a_share_data_quality.py` - 数据质量验证
- `scripts/stock_selection_analysis.py` - 选股分析（即将添加）
```

## 贡献指南

本项目专注于个人投资决策辅助，欢迎提出改进建议和bug报告。

## 许可证

MIT License

## 免责声明

本系统仅供学习和研究使用，不构成投资建议。投资有风险，决策需谨慎。
