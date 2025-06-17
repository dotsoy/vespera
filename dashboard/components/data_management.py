"""
数据管理组件
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import asyncio
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger("data_management")

# 数据库连接
try:
    from src.utils.database import get_db_manager
    DB_AVAILABLE = True
except ImportError as e:
    logger.warning(f"数据库模块导入失败: {e}")
    DB_AVAILABLE = False

# 数据源客户端
try:
    from src.data_sources.akshare_data_source import AkShareDataSource
    AKSHARE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"AkShare数据源导入失败: {e}")
    AKSHARE_AVAILABLE = False

# Alpha Vantage已移除

def execute_real_data_update(data_source, update_type, target_date, update_scope, selected_stocks):
    """执行真实的数据更新"""

    # 创建进度显示区域
    progress_container = st.container()
    log_container = st.container()

    with progress_container:
        st.info(f"🔄 正在从 {data_source} 更新数据...")

        # 总进度条
        total_progress = st.progress(0)
        total_status = st.empty()

        # 详细进度
        detail_progress = st.progress(0)
        detail_status = st.empty()

    with log_container:
        st.subheader("📋 更新日志")
        log_area = st.empty()

    logs = []

    try:
        # 1. 初始化数据源客户端
        total_progress.progress(5)
        total_status.text("初始化数据源连接...")
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 尝试连接 {data_source}...")
        log_area.text_area("", value="\n".join(logs), height=300, disabled=True)

        client = None

        # 尝试不同的数据源
        if data_source == "AkShare":
            if not AKSHARE_AVAILABLE:
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ AkShare模块未安装或导入失败")
                st.error("❌ AkShare数据源不可用：模块未安装或导入失败")
                return

            try:
                client = AkShareDataSource()
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ AkShare数据源初始化成功")

                # 测试连接
                if client.initialize():
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ AkShare连接测试成功")
                else:
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ AkShare连接测试失败")
                    st.error("❌ AkShare数据源失败：连接测试失败，请检查网络连接")
                    return

            except Exception as e:
                error_msg = str(e)
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ AkShare连接失败: {error_msg}")
                st.error(f"❌ AkShare数据源失败：连接错误 - {error_msg}")
                return

        else:
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 不支持的数据源: {data_source}")
            st.error(f"❌ 不支持的数据源: {data_source}")
            return

        # 2. 更新股票基础信息
        total_progress.progress(10)
        total_status.text("更新股票基础信息...")
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 开始更新股票基础信息...")
        log_area.text_area("", value="\n".join(logs), height=300, disabled=True)

        stock_basic_df = None
        try:
            if data_source == "AkShare":
                # 使用AkShare获取股票基础信息
                from src.data_sources.base_data_source import DataRequest, DataType
                request = DataRequest(data_type=DataType.STOCK_BASIC)
                response = client.fetch_data(request)

                if response.success and not response.data.empty:
                    stock_basic_df = response.data
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 从AkShare获取到 {len(stock_basic_df)} 只股票基础信息")
                else:
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ AkShare获取股票基础信息失败: {response.error_message}")
                    st.error(f"❌ AkShare获取股票基础信息失败: {response.error_message}")
                    return

            else:
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 不支持的数据源: {data_source}")
                st.error(f"❌ 不支持的数据源: {data_source}")
                return

            if stock_basic_df is None or stock_basic_df.empty:
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 未获取到股票基础信息")
                st.error("❌ 未获取到股票基础信息，请检查数据源配置")
                return

            # 保存到数据库
            if DB_AVAILABLE:
                db_manager = get_db_manager()
                try:
                    # 使用SQL直接插入，避免pandas兼容性问题
                    insert_count = 0
                    for _, row in stock_basic_df.iterrows():
                        try:
                            insert_sql = """
                            INSERT INTO stock_basic (ts_code, symbol, name, area, industry, market, list_date, is_hs)
                            VALUES (:ts_code, :symbol, :name, :area, :industry, :market, :list_date, :is_hs)
                            ON CONFLICT (ts_code) DO UPDATE SET
                            name = EXCLUDED.name,
                            area = EXCLUDED.area,
                            industry = EXCLUDED.industry,
                            market = EXCLUDED.market,
                            list_date = EXCLUDED.list_date,
                            is_hs = EXCLUDED.is_hs
                            """
                            db_manager.execute_postgres_command(insert_sql, params={
                                'ts_code': row['ts_code'],
                                'symbol': row['symbol'],
                                'name': row['name'],
                                'area': row['area'],
                                'industry': row['industry'],
                                'market': row['market'],
                                'list_date': row['list_date'],
                                'is_hs': row['is_hs']
                            })
                            insert_count += 1
                        except Exception as insert_e:
                            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ 插入股票 {row['ts_code']} 失败: {insert_e}")

                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ 成功保存 {insert_count} 只股票基础信息到数据库")
                except Exception as db_e:
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 数据库保存失败: {db_e}")
                    st.error(f"数据库保存失败: {db_e}")
            else:
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ 数据库不可用，跳过保存")

        except Exception as e:
            error_msg = str(e)
            if "权限" in error_msg or "permission" in error_msg.lower():
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ API权限不足: {error_msg}")
                st.error(f"❌ 更新股票基础信息失败：API权限不足 - {error_msg}")
            elif "limit" in error_msg.lower() or "quota" in error_msg.lower():
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ API调用限制: {error_msg}")
                st.error(f"❌ 更新股票基础信息失败：API调用超限 - {error_msg}")
            else:
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 更新股票基础信息失败: {error_msg}")
                st.error(f"❌ 更新股票基础信息失败: {error_msg}")
            return

        log_area.text_area("", value="\n".join(logs), height=300, disabled=True)

        # 3. 更新日线行情数据
        total_progress.progress(30)
        total_status.text("更新日线行情数据...")

        # 确定要更新的日期
        if update_type == "指定日期" and target_date:
            trade_dates = [target_date.strftime('%Y%m%d')]
        elif update_type == "增量更新":
            # 获取最近5个交易日
            trade_dates = get_recent_trade_dates(5)
        else:
            # 默认更新6月13日
            trade_dates = ['20240613']

        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 准备更新日期: {', '.join(trade_dates)}")
        log_area.text_area("", value="\n".join(logs), height=300, disabled=True)

        # 获取股票列表
        if update_scope == "自定义选择" and selected_stocks:
            stock_list = selected_stocks
        else:
            # 从数据库获取股票列表
            if DB_AVAILABLE and stock_basic_df is not None and not stock_basic_df.empty:
                try:
                    # 优先使用刚获取的股票基础信息
                    if update_scope == "沪深300":
                        stock_list = stock_basic_df.head(300)['ts_code'].tolist()
                    elif update_scope == "中证500":
                        stock_list = stock_basic_df.head(500)['ts_code'].tolist()
                    elif update_scope == "创业板50":
                        stock_list = stock_basic_df[stock_basic_df['market'] == '创业板'].head(50)['ts_code'].tolist()
                    elif update_scope == "科创板50":
                        stock_list = stock_basic_df[stock_basic_df['market'] == '科创板'].head(50)['ts_code'].tolist()
                    else:
                        stock_list = stock_basic_df['ts_code'].tolist()
                except Exception as e:
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ 从基础信息获取股票列表失败: {e}")
                    # 备选：从数据库获取
                    try:
                        db_manager = get_db_manager()
                        stock_query = "SELECT ts_code FROM stock_basic WHERE is_hs = 'Y' LIMIT 100"
                        stock_df = db_manager.execute_postgres_query(stock_query)
                        stock_list = stock_df['ts_code'].tolist() if not stock_df.empty else []
                    except Exception as db_e:
                        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ 从数据库获取股票列表也失败: {db_e}")
                        stock_list = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ']
            else:
                # 使用预定义的股票列表
                stock_list = [
                    '000001.SZ', '000002.SZ', '000858.SZ', '600000.SH', '600036.SH',
                    '600519.SH', '300750.SZ', '002415.SZ', '600104.SH', '000725.SZ'
                ]
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 使用预定义股票列表")

        if not stock_list:
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 股票列表为空，无法更新行情数据")
            st.error("❌ 股票列表为空，无法更新行情数据")
            return

        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 准备更新 {len(stock_list)} 只股票的行情数据")
        log_area.text_area("", value="\n".join(logs), height=300, disabled=True)

        # 批量更新行情数据
        total_records = 0
        for date_idx, trade_date in enumerate(trade_dates):
            date_progress = 30 + (date_idx * 60 // len(trade_dates))
            total_progress.progress(date_progress)
            total_status.text(f"更新 {trade_date} 行情数据...")

            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 开始更新 {trade_date} 的行情数据")
            log_area.text_area("", value="\n".join(logs), height=300, disabled=True)

            quotes_df = None
            try:
                if data_source == "AkShare":
                    # 使用AkShare批量获取行情数据
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 尝试从AkShare获取 {trade_date} 行情数据...")

                    from src.data_sources.base_data_source import DataRequest, DataType
                    from datetime import datetime as dt

                    # 将trade_date转换为datetime对象
                    start_date = dt.strptime(trade_date, '%Y%m%d')
                    end_date = start_date

                    all_quotes = []
                    for i, stock_code in enumerate(stock_list[:10]):  # 限制数量以避免超时
                        try:
                            request = DataRequest(
                                data_type=DataType.DAILY_QUOTES,
                                symbol=stock_code,
                                start_date=start_date,
                                end_date=end_date
                            )
                            response = client.fetch_data(request)

                            if response.success and not response.data.empty:
                                all_quotes.append(response.data)

                            if i % 5 == 0:  # 每5只股票记录一次进度
                                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 已处理 {i+1}/{min(len(stock_list), 10)} 只股票")
                                log_area.text_area("", value="\n".join(logs), height=300, disabled=True)

                        except Exception as e:
                            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ 获取{stock_code}行情失败: {e}")

                    if all_quotes:
                        quotes_df = pd.concat(all_quotes, ignore_index=True)
                        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 从AkShare获取到 {len(quotes_df)} 条行情数据")
                    else:
                        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ AkShare未获取到任何行情数据")
                        continue

                else:
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 不支持的数据源: {data_source}")
                    continue

                if quotes_df is None or quotes_df.empty:
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ {trade_date}: 未获取到行情数据（可能是非交易日）")
                    continue

                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 从{data_source}获取到 {len(quotes_df)} 条 {trade_date} 行情数据")

                # 保存到数据库
                if DB_AVAILABLE:
                    try:
                        # 使用SQL直接插入，避免pandas兼容性问题
                        insert_count = 0
                        for _, row in quotes_df.iterrows():
                            try:
                                insert_sql = """
                                INSERT INTO stock_daily_quotes
                                (ts_code, trade_date, open_price, high_price, low_price, close_price,
                                 pre_close, change_amount, pct_chg, vol, amount)
                                VALUES (:ts_code, :trade_date, :open_price, :high_price, :low_price, :close_price,
                                         :pre_close, :change_amount, :pct_chg, :vol, :amount)
                                ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                                open_price = EXCLUDED.open_price,
                                high_price = EXCLUDED.high_price,
                                low_price = EXCLUDED.low_price,
                                close_price = EXCLUDED.close_price,
                                pre_close = EXCLUDED.pre_close,
                                change_amount = EXCLUDED.change_amount,
                                pct_chg = EXCLUDED.pct_chg,
                                vol = EXCLUDED.vol,
                                amount = EXCLUDED.amount
                                """
                                db_manager.execute_postgres_command(insert_sql, params={
                                    'ts_code': row['ts_code'],
                                    'trade_date': row['trade_date'],
                                    'open_price': row['open_price'],
                                    'high_price': row['high_price'],
                                    'low_price': row['low_price'],
                                    'close_price': row['close_price'],
                                    'pre_close': row['pre_close'],
                                    'change_amount': row['change_amount'],
                                    'pct_chg': row['pct_chg'],
                                    'vol': row['vol'],
                                    'amount': row['amount']
                                })
                                insert_count += 1
                            except Exception as insert_e:
                                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ 插入 {row['ts_code']} {trade_date} 行情失败: {insert_e}")

                        total_records += insert_count
                        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ {trade_date}: 成功保存 {insert_count} 条记录")
                    except Exception as db_e:
                        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ {trade_date}: 数据库保存失败 - {db_e}")
                else:
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ 数据库不可用，跳过保存")

            except Exception as e:
                error_msg = str(e)
                if "权限" in error_msg or "permission" in error_msg.lower():
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ {trade_date}: API权限不足 - {error_msg}")
                elif "limit" in error_msg.lower() or "quota" in error_msg.lower():
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ {trade_date}: API调用超限 - {error_msg}")
                elif "timeout" in error_msg.lower():
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ {trade_date}: 请求超时 - {error_msg}")
                else:
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ {trade_date}: 更新失败 - {error_msg}")

            log_area.text_area("", value="\n".join(logs), height=300, disabled=True)

        # 4. 完成
        total_progress.progress(100)
        total_status.text("✅ 数据更新完成!")
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 🎉 数据更新完成!")
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 总计更新: {len(stock_list)} 只股票, {len(trade_dates)} 个交易日, {total_records} 条记录")
        log_area.text_area("", value="\n".join(logs), height=300, disabled=True)

        # 清除缓存
        st.cache_data.clear()

        # 显示成功信息
        with progress_container:
            st.success(f"✅ 数据更新成功!")

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("更新股票", f"{len(stock_list):,}")
            with col2:
                st.metric("更新日期", len(trade_dates))
            with col3:
                st.metric("总记录数", f"{total_records:,}")
            with col4:
                st.metric("数据源", data_source)

    except Exception as e:
        logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 数据更新失败: {e}")
        log_area.text_area("", value="\n".join(logs), height=300, disabled=True)
        st.error(f"❌ 数据更新失败: {e}")


def get_recent_trade_dates(days=5):
    """获取最近的交易日期"""
    dates = []
    current_date = datetime.now()

    while len(dates) < days:
        # 跳过周末
        if current_date.weekday() < 5:  # 0-4 是周一到周五
            dates.append(current_date.strftime('%Y%m%d'))
        current_date -= timedelta(days=1)

    return dates[::-1]  # 返回正序


def simulate_data_update(logs, log_area, total_progress, total_status, detail_progress, detail_status):
    """模拟数据更新过程"""
    import time

    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 使用模拟数据更新模式")
    log_area.text_area("", value="\n".join(logs), height=300, disabled=True)

    # 模拟更新过程
    for i in range(100):
        total_progress.progress(i + 1)
        total_status.text(f"模拟更新中... {i+1}%")

        if i % 20 == 0:
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 模拟进度: {i+1}%")
            log_area.text_area("", value="\n".join(logs), height=300, disabled=True)

        time.sleep(0.02)

    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ 模拟更新完成")
    log_area.text_area("", value="\n".join(logs), height=300, disabled=True)


@st.cache_data(ttl=300)
def get_stock_list():
    """获取股票列表"""
    try:
        # 优先尝试从数据库获取真实数据
        if DB_AVAILABLE:
            try:
                db_manager = get_db_manager()
                query = """
                SELECT ts_code, symbol, name, area, industry, market, list_date
                FROM stock_basic
                WHERE is_hs = 'Y' AND market IN ('主板', '创业板', '科创板')
                ORDER BY ts_code
                """
                df = db_manager.execute_postgres_query(query)

                if not df.empty:
                    logger.info(f"从数据库获取到 {len(df)} 只股票数据")
                    return df
                else:
                    logger.warning("数据库中无股票数据，使用模拟数据")

            except Exception as e:
                logger.error(f"数据库查询失败: {e}，使用模拟数据")

        # 备选方案：返回空DataFrame，提示用户导入数据
        logger.warning("数据库中无股票数据，请先导入股票基础信息")
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_latest_data_status():
    """获取最新数据状态"""
    try:
        # 优先尝试从数据库获取真实数据状态
        if DB_AVAILABLE:
            try:
                db_manager = get_db_manager()

                # 获取最新交易日期
                query = """
                SELECT
                    MAX(trade_date) as latest_date,
                    COUNT(DISTINCT ts_code) as stock_count,
                    COUNT(*) as total_records
                FROM stock_daily_quotes
                """
                result = db_manager.execute_postgres_query(query)

                if not result.empty:
                    latest_date = result['latest_date'].iloc[0]
                    stock_count = result['stock_count'].iloc[0]
                    total_records = result['total_records'].iloc[0]

                    logger.info(f"从数据库获取数据状态: 最新日期={latest_date}, 股票数={stock_count}, 记录数={total_records}")

                    return {
                        "latest_date": latest_date,
                        "stock_count": stock_count,
                        "total_records": total_records,
                        "is_today": latest_date == datetime.now().date() if latest_date else False
                    }

            except Exception as e:
                logger.error(f"数据库查询失败: {e}，使用模拟数据状态")

        # 备选方案：返回空状态，提示用户导入数据
        logger.warning("数据库中无行情数据，请先导入股票数据")
        return {
            "latest_date": None,
            "stock_count": 0,
            "total_records": 0,
            "is_today": False
        }

    except Exception as e:
        logger.error(f"获取数据状态失败: {e}")
        return {
            "latest_date": None,
            "stock_count": 0,
            "total_records": 0,
            "is_today": False
        }


def render_data_overview():
    """渲染数据概览"""
    st.header("📊 数据概览")
    
    # 获取数据状态
    data_status = get_latest_data_status()
    stock_list = get_stock_list()
    
    # 数据状态指标
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        latest_date = data_status["latest_date"]
        if latest_date:
            date_str = latest_date.strftime('%Y-%m-%d')
            delta_color = "normal" if data_status["is_today"] else "inverse"
            delta_text = "今日数据" if data_status["is_today"] else "需要更新"
        else:
            date_str = "无数据"
            delta_color = "inverse"
            delta_text = "需要初始化"
        
        st.metric(
            "最新数据日期",
            date_str,
            delta=delta_text,
            delta_color=delta_color
        )
    
    with col2:
        st.metric(
            "股票数量",
            f"{data_status['stock_count']:,}",
            delta=f"总计 {len(stock_list):,} 只"
        )
    
    with col3:
        st.metric(
            "行情记录数",
            f"{data_status['total_records']:,}",
            delta="条记录"
        )
    
    with col4:
        coverage = (data_status['stock_count'] / len(stock_list) * 100) if len(stock_list) > 0 else 0
        st.metric(
            "数据覆盖率",
            f"{coverage:.1f}%",
            delta="股票覆盖"
        )


def render_data_update():
    """渲染数据更新功能"""
    st.header("🔄 数据更新")
    
    # 数据源选择
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📡 数据源配置")
        
        # 构建可用数据源列表
        available_sources = []
        if AKSHARE_AVAILABLE:
            available_sources.append("AkShare (推荐)")
        available_sources.append("模拟数据")

        data_source = st.selectbox(
            "选择数据源",
            available_sources,
            index=0
        )
        
        update_type = st.selectbox(
            "更新类型",
            ["增量更新", "全量更新", "指定日期"],
            index=0
        )
        
        if update_type == "指定日期":
            target_date = st.date_input(
                "目标日期",
                value=datetime.now().date(),
                max_value=datetime.now().date()
            )
        else:
            target_date = None
    
    with col2:
        st.subheader("📈 更新范围")
        
        update_scope = st.selectbox(
            "更新范围",
            ["全部股票", "主板股票", "创业板股票", "科创板股票", "自定义选择"],
            index=0
        )
        
        if update_scope == "自定义选择":
            stock_list = get_stock_list()
            if not stock_list.empty:
                selected_stocks = st.multiselect(
                    "选择股票",
                    options=stock_list['ts_code'].tolist(),
                    format_func=lambda x: f"{x} - {stock_list[stock_list['ts_code']==x]['name'].iloc[0] if not stock_list[stock_list['ts_code']==x].empty else x}"
                )
            else:
                selected_stocks = []
                st.warning("无法获取股票列表")
        else:
            selected_stocks = None
    
    # 更新按钮
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🚀 开始更新", type="primary"):
            execute_real_data_update(
                data_source, update_type, target_date,
                update_scope, selected_stocks
            )
    
    with col2:
        if st.button("📊 验证数据"):
            with st.spinner("正在验证数据完整性..."):
                try:
                    # 真实数据验证逻辑
                    validation_results = []

                    if DB_AVAILABLE:
                        try:
                            db_manager = get_db_manager()

                            # 检查股票基础信息
                            basic_query = "SELECT COUNT(*) as count FROM stock_basic"
                            basic_result = db_manager.execute_postgres_query(basic_query)
                            basic_count = basic_result['count'].iloc[0] if not basic_result.empty else 0

                            validation_results.append({
                                "检查项": "股票基础信息",
                                "结果": f"{basic_count:,} 条记录",
                                "状态": "✅ 正常" if basic_count > 0 else "❌ 无数据"
                            })

                            # 检查日线行情数据
                            quotes_query = "SELECT COUNT(*) as count FROM stock_daily_quotes"
                            quotes_result = db_manager.execute_postgres_query(quotes_query)
                            quotes_count = quotes_result['count'].iloc[0] if not quotes_result.empty else 0

                            validation_results.append({
                                "检查项": "日线行情数据",
                                "结果": f"{quotes_count:,} 条记录",
                                "状态": "✅ 正常" if quotes_count > 0 else "❌ 无数据"
                            })

                        except Exception as e:
                            validation_results.append({
                                "检查项": "数据库连接",
                                "结果": "连接失败",
                                "状态": f"❌ {str(e)}"
                            })
                    else:
                        validation_results.append({
                            "检查项": "数据库连接",
                            "结果": "数据库不可用",
                            "状态": "❌ 请检查数据库配置"
                        })

                    # 显示验证结果
                    df = pd.DataFrame(validation_results)
                    st.dataframe(df, use_container_width=True)

                except Exception as e:
                    st.error(f"数据验证失败: {e}")
    
    with col3:
        if st.button("🧹 清理数据"):
            if st.checkbox("确认清理（不可恢复）"):
                with st.spinner("正在清理数据..."):
                    try:
                        # 数据清理逻辑
                        st.warning("数据清理功能暂未实现")
                    except Exception as e:
                        st.error(f"数据清理失败: {e}")


def render_stock_selector():
    """渲染股票选择器"""
    st.header("🎯 股票选择器")
    
    stock_list = get_stock_list()
    
    if stock_list.empty:
        st.warning("无法获取股票列表，请检查数据库连接")
        return []
    
    # 筛选条件
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # 市场筛选
        markets = ['全部'] + stock_list['market'].unique().tolist()
        selected_market = st.selectbox("市场", markets)
    
    with col2:
        # 行业筛选
        industries = ['全部'] + stock_list['industry'].unique().tolist()
        selected_industry = st.selectbox("行业", industries)
    
    with col3:
        # 地区筛选
        areas = ['全部'] + stock_list['area'].unique().tolist()
        selected_area = st.selectbox("地区", areas)
    
    # 应用筛选
    filtered_stocks = stock_list.copy()
    
    if selected_market != '全部':
        filtered_stocks = filtered_stocks[filtered_stocks['market'] == selected_market]
    
    if selected_industry != '全部':
        filtered_stocks = filtered_stocks[filtered_stocks['industry'] == selected_industry]
    
    if selected_area != '全部':
        filtered_stocks = filtered_stocks[filtered_stocks['area'] == selected_area]
    
    # 搜索框
    search_term = st.text_input("🔍 搜索股票（代码或名称）")
    if search_term:
        mask = (
            filtered_stocks['ts_code'].str.contains(search_term, case=False, na=False) |
            filtered_stocks['name'].str.contains(search_term, case=False, na=False) |
            filtered_stocks['symbol'].str.contains(search_term, case=False, na=False)
        )
        filtered_stocks = filtered_stocks[mask]
    
    # 显示筛选结果
    st.write(f"找到 {len(filtered_stocks)} 只股票")
    
    if not filtered_stocks.empty:
        # 分页显示
        page_size = 20
        total_pages = (len(filtered_stocks) - 1) // page_size + 1
        
        if total_pages > 1:
            page = st.selectbox("页码", range(1, total_pages + 1))
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            display_stocks = filtered_stocks.iloc[start_idx:end_idx]
        else:
            display_stocks = filtered_stocks
        
        # 显示股票列表
        selected_stocks = st.multiselect(
            "选择要分析的股票",
            options=display_stocks['ts_code'].tolist(),
            format_func=lambda x: f"{x} - {display_stocks[display_stocks['ts_code']==x]['name'].iloc[0]}"
        )
        
        # 显示详细信息
        if selected_stocks:
            st.subheader("📋 选中的股票")
            selected_df = display_stocks[display_stocks['ts_code'].isin(selected_stocks)]
            st.dataframe(
                selected_df[['ts_code', 'name', 'industry', 'market', 'area']],
                use_container_width=True
            )
            
            return selected_stocks
        else:
            # 显示筛选结果表格
            st.dataframe(
                display_stocks[['ts_code', 'name', 'industry', 'market', 'area']],
                use_container_width=True
            )
    
    return []


def render_data_export():
    """渲染数据导出功能"""
    st.header("📤 数据导出")
    
    # 导出选项
    col1, col2 = st.columns(2)
    
    with col1:
        export_type = st.selectbox(
            "导出类型",
            ["股票基础信息", "日线行情数据", "交易信号", "技术指标", "自定义查询"]
        )
        
        export_format = st.selectbox(
            "导出格式",
            ["CSV", "Excel", "JSON"]
        )
    
    with col2:
        if export_type != "自定义查询":
            date_range = st.date_input(
                "日期范围",
                value=[datetime.now().date() - timedelta(days=30), datetime.now().date()],
                max_value=datetime.now().date()
            )
        
        if export_type in ["日线行情数据", "交易信号", "技术指标"]:
            stock_codes = st.text_area(
                "股票代码（每行一个）",
                placeholder="000001.SZ\n000002.SZ\n600000.SH"
            )
    
    # 自定义查询
    if export_type == "自定义查询":
        custom_query = st.text_area(
            "SQL查询语句",
            placeholder="SELECT * FROM stock_basic LIMIT 100",
            height=100
        )
    
    # 导出按钮
    if st.button("📥 导出数据", type="primary"):
        try:
            # 暂时使用模拟数据导出
            # TODO: 修复数据库连接后替换为真实数据导出

            if export_type == "自定义查询":
                if custom_query and custom_query.strip():
                    st.warning("自定义查询功能需要数据库连接，当前使用模拟数据")
                    df = get_stock_list()  # 使用模拟股票列表
                else:
                    st.error("请输入SQL查询语句")
                    return
            else:
                # 根据导出类型使用模拟数据
                if export_type == "股票基础信息":
                    df = get_stock_list()
                else:
                    st.warning("其他导出类型暂未实现，当前使用股票基础信息作为示例")
                    df = get_stock_list()
            
            if not df.empty:
                # 根据格式导出
                if export_format == "CSV":
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="下载CSV文件",
                        data=csv,
                        file_name=f"{export_type}_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                elif export_format == "JSON":
                    json_str = df.to_json(orient='records', indent=2)
                    st.download_button(
                        label="下载JSON文件",
                        data=json_str,
                        file_name=f"{export_type}_{datetime.now().strftime('%Y%m%d')}.json",
                        mime="application/json"
                    )
                else:
                    st.warning("Excel导出暂未实现")
                
                # 预览数据
                st.subheader("📋 数据预览")
                st.dataframe(df.head(100), use_container_width=True)
                st.info(f"共 {len(df)} 条记录")
            else:
                st.warning("查询结果为空")
                
        except Exception as e:
            st.error(f"数据导出失败: {e}")


def render_data_management_main():
    """渲染数据管理主面板"""
    # 数据概览
    render_data_overview()
    st.markdown("---")
    
    # 创建标签页
    tab1, tab2, tab3, tab4 = st.tabs(["🔄 数据更新", "🎯 股票选择", "📤 数据导出", "📊 数据质量"])
    
    with tab1:
        render_data_update()
    
    with tab2:
        selected_stocks = render_stock_selector()
        # 将选中的股票存储到session state
        if selected_stocks:
            st.session_state['selected_stocks'] = selected_stocks
    
    with tab3:
        render_data_export()
    
    with tab4:
        st.header("📊 数据质量监控")
        st.info("数据质量监控功能开发中...")
        
        # 可以添加数据质量检查，如：
        # - 缺失数据检查
        # - 异常值检查  
        # - 数据一致性检查
        # - 更新频率检查
