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

# 数据库和数据源客户端（暂时注释掉，因为模块不存在或有依赖问题）
# from src.utils.database import get_db_manager
# from src.data_sources.alltick_client import AllTickClient
# from src.data_sources.alpha_vantage_client import AlphaVantageClient

logger = get_logger("data_management")


@st.cache_data(ttl=300)
def get_stock_list():
    """获取股票列表"""
    try:
        # 暂时使用模拟数据，因为数据库连接有问题
        # TODO: 修复数据库连接后替换为真实数据

        # 生成模拟股票列表
        stock_codes = [
            "000001.SZ", "000002.SZ", "000858.SZ", "000876.SZ", "002415.SZ",
            "600000.SH", "600036.SH", "600519.SH", "600887.SH", "601318.SH",
            "688001.SH", "688036.SH", "688111.SH", "688188.SH", "688599.SH"
        ]

        stock_names = [
            "平安银行", "万科A", "五粮液", "新希望", "欧菲光",
            "浦发银行", "招商银行", "贵州茅台", "伊利股份", "中国平安",
            "华兴源创", "传音控股", "金山办公", "柏楚电子", "天合光能"
        ]

        industries = [
            "银行", "房地产", "食品饮料", "农林牧渔", "电子",
            "银行", "银行", "食品饮料", "食品饮料", "非银金融",
            "电子", "电子", "计算机", "机械设备", "电力设备"
        ]

        markets = [
            "主板", "主板", "主板", "主板", "创业板",
            "主板", "主板", "主板", "主板", "主板",
            "科创板", "科创板", "科创板", "科创板", "科创板"
        ]

        areas = [
            "广东", "广东", "四川", "四川", "广东",
            "上海", "广东", "贵州", "内蒙古", "广东",
            "江苏", "广东", "北京", "上海", "江苏"
        ]

        df = pd.DataFrame({
            'ts_code': stock_codes,
            'symbol': [code.split('.')[0] for code in stock_codes],
            'name': stock_names,
            'area': areas,
            'industry': industries,
            'market': markets,
            'list_date': ['2020-01-01'] * len(stock_codes)
        })

        return df

    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_latest_data_status():
    """获取最新数据状态"""
    try:
        # 暂时使用模拟数据状态
        # TODO: 修复数据库连接后替换为真实数据

        # 模拟数据状态
        latest_date = datetime.now().date() - timedelta(days=1)  # 昨天的数据
        stock_count = 15  # 模拟15只股票
        total_records = 1500  # 模拟1500条记录

        return {
            "latest_date": latest_date,
            "stock_count": stock_count,
            "total_records": total_records,
            "is_today": False  # 不是今天的数据
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
        
        data_source = st.selectbox(
            "选择数据源",
            ["AllTick", "Alpha Vantage", "模拟数据"],
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
            with st.spinner("正在更新数据..."):
                try:
                    # 这里添加实际的数据更新逻辑
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # 模拟更新过程
                    for i in range(100):
                        progress_bar.progress(i + 1)
                        status_text.text(f"正在更新... {i+1}%")
                        # time.sleep(0.01)  # 实际使用时移除
                    
                    st.success("数据更新完成！")
                    st.cache_data.clear()  # 清除缓存
                    
                except Exception as e:
                    st.error(f"数据更新失败: {e}")
    
    with col2:
        if st.button("📊 验证数据"):
            with st.spinner("正在验证数据完整性..."):
                try:
                    # 模拟数据验证逻辑
                    # TODO: 修复数据库连接后替换为真实验证

                    validation_results = []

                    # 模拟检查基础数据
                    stock_count = 15  # 模拟股票数量
                    validation_results.append({
                        "检查项": "股票基础信息",
                        "结果": f"{stock_count:,} 条记录",
                        "状态": "⚠️ 模拟数据"
                    })

                    # 模拟检查行情数据
                    quote_count = 1500  # 模拟行情数据
                    validation_results.append({
                        "检查项": "日线行情数据",
                        "结果": f"{quote_count:,} 条记录",
                        "状态": "⚠️ 模拟数据"
                    })

                    # 显示验证结果
                    df = pd.DataFrame(validation_results)
                    st.dataframe(df, use_container_width=True)
                    st.warning("当前显示的是模拟数据验证结果，请修复数据库连接后查看真实数据")

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
