# 🚀 启明星量化投资分析平台 v2.0

## 📋 项目概述

启明星是一个基于"资金为王，技术触发"理念的专业级量化投资分析平台，通过四维分析框架和智能信号融合引擎，为A股投资者提供科学的投资决策支持。

## ✨ 核心特性

### 🔍 四维分析框架
- **技术分析**: 威科夫量价分析 + 技术指标 (35%权重)
- **资金分析**: 主力资金流向追踪 (45%权重)
- **相对强度**: 个股vs大盘/行业强度对比 (15%权重)
- **催化剂分析**: 事件驱动机会识别 (5%权重)

### 🎯 启明星策略引擎
采用"资金为王，技术触发"的核心策略：
1. **资金面主导**: 45%权重，80分门槛，确保主力资金介入
2. **技术面确认**: 35%权重，75分门槛，精确进场时机
3. **信号分级**: S级(≥90分) 和 A级(≥75分) 信号
4. **T+1适配**: 专为A股T+1交易规则优化

### 🚀 Dashboard v2.0 全新设计
- **🖥️ 系统状态**: 实时监控系统资源、数据库连接、策略状态
- **📊 数据管理**: 股票选择、数据更新、导出功能
- **📡 数据源管理**: AllTick/Alpha Vantage数据源管理和监控
- **🎯 策略分析**: 启明星策略分析和信号生成
- **📈 回测可视化**: 买卖点标记、权益曲线、交易分析

## 🏗️ 技术架构

### 核心技术栈
- **数据存储**: PostgreSQL + ClickHouse + Redis
- **核心语言**: Python 3.9+
- **技术分析**: Tulipy (替代 TA-Lib，更易安装)
- **前端框架**: Streamlit + Plotly (Dashboard v2.0)
- **研究工具**: Marimo 交互式笔记本
- **数据源**: AllTick + Alpha Vantage + 模拟数据
- **部署方案**: Docker + Docker Compose

### 架构特点
- **模块化设计**: 组件化架构，易于维护和扩展
- **智能备选**: 数据库不可用时自动切换模拟数据
- **实时监控**: 系统状态、数据源、策略运行监控
- **多数据源**: 支持多个数据源的统一管理
- **T+1优化**: 专为A股交易规则设计

## 🚀 快速开始

### 环境要求
- Python 3.9+
- Docker & Docker Compose
- PostgreSQL 14+
- 8GB+ RAM

### ⚡ 一键启动 Dashboard v2.0
```bash
# 克隆项目
git clone <repository-url>
cd cloudmere-agument

# 安装依赖
make install

# 启动 Dashboard v2.0
make dashboard

# 访问地址
http://localhost:8501
```

### 🔧 完整安装步骤

1. **环境配置**
```bash
cp .env.example .env
# 编辑 .env 文件，配置数据源API Token
```

2. **安装依赖**
```bash
make install
```

3. **测试启明星策略**
```bash
python scripts/test_qiming_star_strategy.py
```

4. **启动 Dashboard v2.0**
```bash
make dashboard
# 或者使用新的启动脚本
python scripts/run_dashboard_v2.py
```

5. **启动 Marimo 研究室**
```bash
python scripts/launch_marimo.py launch qiming_star_strategy_analysis.py
```

6. **⚠️ 数据安全提醒**
```bash
# 系统支持真实数据和模拟数据
# Dashboard 会自动检测数据库状态并智能切换
# 安全使用，无需担心数据覆盖问题
```

## 📁 项目结构

```
cloudmere-agument/
├── config/                 # 配置文件
├── data/                   # 数据存储
├── src/                    # 核心源码
│   ├── strategies/         # 策略模块
│   │   └── qiming_star/    # 启明星策略
│   ├── analyzers/          # 四维分析模块
│   ├── data_sources/       # 数据获取模块
│   ├── fusion/             # 信号融合引擎
│   └── utils/              # 工具函数
├── dashboard/              # Dashboard v2.0
│   ├── app.py              # 主应用入口
│   └── components/         # 功能组件
│       ├── system_status.py        # 系统状态监控
│       ├── data_management.py      # 数据管理
│       ├── data_source_manager.py  # 数据源管理
│       ├── strategy_analysis.py    # 策略分析
│       └── backtest_visualization.py # 回测可视化
├── notebooks/              # Marimo研究笔记本
│   └── qiming_star_strategy_analysis.py # 启明星策略分析
├── scripts/                # 脚本工具
├── tests/                  # 测试代码
├── docker/                 # Docker配置
└── docs/                   # 文档
```

## 🎯 开发进度

### ✅ 已完成功能
- [x] **启明星策略系统** - 完整的四维分析框架
  - [x] 技术分析模块 (威科夫理论 + Tulipy)
  - [x] 资金分析模块 (主力资金追踪)
  - [x] 相对强度模块 (个股vs大盘/行业)
  - [x] 催化剂分析模块 (事件驱动)
  - [x] 信号融合引擎 ("资金为王，技术触发")
  - [x] 策略回测系统 (多策略对比)
- [x] **Dashboard v2.0** - 全新设计的分析平台
  - [x] 系统状态监控 (资源、数据库、策略)
  - [x] 数据管理 (股票选择、数据更新、导出)
  - [x] 数据源管理 (AllTick/Alpha Vantage监控)
  - [x] 策略分析 (信号生成、分析报告)
  - [x] 回测可视化 (买卖点、权益曲线)
- [x] **Marimo研究室** - 交互式策略分析
- [x] **完整测试** - 自动化测试覆盖
- [x] **智能备选** - 数据库不可用时的优雅降级

### 🔄 进行中
- [ ] 数据库连接优化
- [ ] 真实数据源集成 (AllTick, Alpha Vantage)
- [ ] 更多技术指标集成

## 🔧 可用命令

```bash
# 核心命令
make help                    # 查看所有可用命令
make install                 # 安装依赖
make dashboard               # 启动 Dashboard v2.0

# 策略测试
python scripts/test_qiming_star_strategy.py  # 测试启明星策略

# Marimo 研究室
python scripts/launch_marimo.py launch qiming_star_strategy_analysis.py

# 技术测试
make test-tulipy            # 测试 Tulipy 技术分析库
make test-system            # 运行完整系统测试

## 📊 Dashboard v2.0 功能导览

### 🖥️ 系统状态
- **系统资源监控**: CPU、内存、磁盘使用率
- **数据库状态**: PostgreSQL、ClickHouse、Redis连接
- **策略状态**: 可用策略列表和运行状态
- **系统日志**: 实时日志查看和筛选

### � 数据管理
- **数据概览**: 最新数据日期、股票数量、覆盖率
- **股票选择**: 多维筛选、智能搜索、批量选择
- **数据更新**: 支持增量、全量、指定日期更新
- **数据导出**: CSV、JSON格式导出

### 📡 数据源管理
- **数据源监控**: AllTick、Alpha Vantage状态监控
- **拉取管理**: 可视化数据拉取进度和状态
- **配置管理**: API Token配置和连接测试
- **历史统计**: 拉取历史和成功率统计

### 🎯 策略分析
- **启明星策略**: 四维权重配置和参数调优
- **信号生成**: S级/A级信号分级和详细分析
- **分析报告**: 确定性评分、风险收益比分析
- **交易建议**: 入场价、止损、目标价计算

### 📈 回测可视化
- **K线图**: 专业K线图 + 买卖点标记
- **权益曲线**: 策略表现和最大回撤分析
- **交易分析**: 盈亏分布、退出原因统计
- **月度表现**: 月度收益率和统计指标

## 🚀 快速体验

```bash
# 1. 启动 Dashboard v2.0
make dashboard

# 2. 访问系统
http://localhost:8501

# 3. 功能导航
🖥️ 系统状态 → 查看系统运行状态
📊 数据管理 → 选择股票进行分析
📡 数据源管理 → 配置和监控数据源
🎯 策略分析 → 执行启明星策略分析
📈 回测可视化 → 查看买卖点和交易分析

# 4. Marimo 研究室
python scripts/launch_marimo.py launch qiming_star_strategy_analysis.py
```

## 🤝 贡献指南

本项目专注于A股量化投资分析，欢迎提出改进建议和bug报告。

## 📄 许可证

MIT License

## ⚠️ 免责声明

本系统仅供学习和研究使用，不构成投资建议。投资有风险，决策需谨慎。

---

**🌟 启明星量化投资分析平台 - 让数据驱动投资决策！**
