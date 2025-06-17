# 数据源迁移说明

## 概述

本次更新将数据源从 Tushare 和 AllTick 迁移到 AkShare，以提供更稳定、免费的A股数据服务。

## 变更内容

### 移除的数据源
- **Tushare**: 专业金融数据接口（需要付费API Token）
- **AllTick**: 专业金融数据API（需要付费API Token）

### 新增的数据源
- **AkShare**: 免费的A股数据源，无需API Token

## 主要优势

### AkShare 优势
1. **免费使用**: 无需注册或购买API Token
2. **数据丰富**: 支持股票基础信息、日线行情、指数数据、财务数据
3. **更新及时**: 数据来源可靠，更新频率高
4. **易于使用**: 接口简单，无需复杂配置

### 对比表

| 特性 | Tushare | AllTick | AkShare |
|------|---------|---------|---------|
| 费用 | 付费 | 付费 | 免费 |
| API Token | 需要 | 需要 | 不需要 |
| 数据覆盖 | 全面 | 全面 | A股为主 |
| 使用门槛 | 高 | 高 | 低 |

## 技术变更

### 文件变更
- 移除: `src/data_sources/tushare_data_source.py`
- 移除: `src/data_sources/tushare_client.py`
- 移除: `src/data_sources/alltick_data_source.py`
- 移除: `src/data_sources/alltick_client.py`
- 新增: `src/data_sources/akshare_data_source.py`

### 配置变更
```python
# 旧配置 (已移除)
tushare_token: str = ""
alltick_token: str = ""

# 新配置
akshare_enabled: bool = True
akshare_timeout: int = 30
```

### 依赖变更
```txt
# 移除
tushare

# 新增
akshare
```

## 使用方法

### 基本使用
```python
from src.data_sources.akshare_data_source import AkShareDataSource
from src.data_sources.base_data_source import DataRequest, DataType

# 创建数据源
client = AkShareDataSource()

# 初始化
if client.initialize():
    # 获取股票基础信息
    request = DataRequest(data_type=DataType.STOCK_BASIC)
    response = client.fetch_data(request)
    
    if response.success:
        stock_data = response.data
        print(f"获取到 {len(stock_data)} 只股票信息")
```

### 获取日线行情
```python
from datetime import datetime, timedelta

# 获取单只股票日线数据
request = DataRequest(
    data_type=DataType.DAILY_QUOTES,
    symbol="000001",  # 平安银行
    start_date=datetime.now() - timedelta(days=30),
    end_date=datetime.now()
)

response = client.fetch_data(request)
if response.success:
    quotes_data = response.data
    print(f"获取到 {len(quotes_data)} 条行情数据")
```

## 数据字段映射

### 股票基础信息
| AkShare字段 | 标准字段 | 说明 |
|-------------|----------|------|
| code | ts_code | 股票代码 |
| name | name | 股票名称 |
| - | market | 市场分类 |
| - | exchange | 交易所 |

### 日线行情数据
| AkShare字段 | 标准字段 | 说明 |
|-------------|----------|------|
| 日期 | trade_date | 交易日期 |
| 开盘 | open_price | 开盘价 |
| 收盘 | close_price | 收盘价 |
| 最高 | high_price | 最高价 |
| 最低 | low_price | 最低价 |
| 成交量 | vol | 成交量 |
| 成交额 | amount | 成交额 |

## 测试验证

### 运行测试脚本
```bash
# 测试数据源连接
python scripts/test_data_sources.py

# 测试数据导入
python scripts/import_real_a_share_data.py

# 测试数据库插入
python scripts/test_database_insert.py
```

### 启动Dashboard
```bash
# 启动Web界面
python -m streamlit run dashboard/app.py --server.port=8506
```

访问 http://localhost:8506 查看数据源状态和管理界面。

## 注意事项

1. **频率限制**: AkShare有请求频率限制，建议控制在1000次/分钟以内
2. **数据范围**: 主要支持A股数据，国际市场数据有限
3. **网络依赖**: 需要稳定的网络连接访问数据源
4. **数据质量**: 免费数据源，数据质量可能不如付费服务

## 故障排除

### 常见问题
1. **网络连接失败**: 检查网络连接和防火墙设置
2. **数据获取为空**: 可能是非交易日或股票代码错误
3. **频率限制**: 降低请求频率，增加请求间隔

### 日志查看
```bash
# 查看日志文件
tail -f logs/vespera.log
```

## 迁移完成确认

- [x] 移除Tushare数据源
- [x] 移除AllTick数据源  
- [x] 实现AkShare数据源
- [x] 更新配置文件
- [x] 更新Dashboard界面
- [x] 更新相关脚本
- [x] 执行回归测试
- [x] 更新文档

## 联系支持

如有问题，请查看项目文档或提交Issue。
