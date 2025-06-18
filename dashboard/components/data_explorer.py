"""
数据探索器 - 使用Perspective多维度展示数据库数据
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.database import get_db_manager

logger = get_logger("data_explorer")

# 检查数据库可用性
try:
    from src.utils.database import get_db_manager
    DB_AVAILABLE = True
except ImportError as e:
    logger.warning(f"数据库模块导入失败: {e}")
    DB_AVAILABLE = False

# Perspective CDN 配置
PERSPECTIVE_CDN = {
    'viewer': "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer/dist/cdn/perspective-viewer.js",
    'datagrid': "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-datagrid/dist/cdn/perspective-viewer-datagrid.js",
    'd3fc': "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-d3fc/dist/cdn/perspective-viewer-d3fc.js",
    'core': "https://cdn.jsdelivr.net/npm/@finos/perspective/dist/cdn/perspective.js"
}

def get_available_tables():
    """获取可用的数据库表"""
    if not DB_AVAILABLE:
        return []
    
    try:
        db_manager = get_db_manager()
        query = """
        SELECT table_name, 
               (SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = t.table_name AND table_schema = 'public') as column_count
        FROM information_schema.tables t
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        result = db_manager.execute_postgres_query(query)
        
        if not result.empty:
            tables = []
            for _, row in result.iterrows():
                tables.append({
                    'name': row['table_name'],
                    'columns': row['column_count']
                })
            return tables
        return []
        
    except Exception as e:
        logger.error(f"获取数据库表失败: {e}")
        return []


def get_table_info(table_name: str):
    """获取表的详细信息"""
    if not DB_AVAILABLE:
        return None
    
    try:
        db_manager = get_db_manager()
        
        # 获取表结构
        structure_query = f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND table_schema = 'public'
        ORDER BY ordinal_position
        """
        structure = db_manager.execute_postgres_query(structure_query)
        
        # 获取表记录数
        count_query = f"SELECT COUNT(*) as total_rows FROM {table_name}"
        count_result = db_manager.execute_postgres_query(count_query)
        total_rows = count_result['total_rows'].iloc[0] if not count_result.empty else 0
        
        # 表用途和字段定义的静态映射（可扩展为从数据库或配置文件加载）
        table_metadata = {
            'stock_data': {
                'purpose': '存储股票历史数据，用于分析和回测',
                'fields': {
                    'date': '交易日期',
                    'open': '开盘价',
                    'high': '最高价',
                    'low': '最低价',
                    'close': '收盘价',
                    'volume': '成交量',
                    'amount': '成交额'
                }
            },
            'index_data': {
                'purpose': '存储指数历史数据，用于市场趋势分析',
                'fields': {
                    'date': '交易日期',
                    'open': '开盘点位',
                    'high': '最高点位',
                    'low': '最低点位',
                    'close': '收盘点位',
                    'volume': '成交量',
                    'amount': '成交额'
                }
            }
            # 可添加更多表的元数据
        }
        
        metadata = table_metadata.get(table_name, {
            'purpose': '暂无该表的用途描述',
            'fields': {row['column_name']: '暂无描述' for _, row in structure.iterrows()}
        })
        
        return {
            'structure': structure,
            'total_rows': total_rows,
            'purpose': metadata['purpose'],
            'field_definitions': metadata['fields']
        }
        
    except Exception as e:
        logger.error(f"获取表信息失败: {e}")
        return None


def load_table_data(table_name: str, limit: int = 1000):
    """加载表数据"""
    if not DB_AVAILABLE:
        return pd.DataFrame()
    
    try:
        db_manager = get_db_manager()
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return db_manager.execute_postgres_query(query)
        
    except Exception as e:
        logger.error(f"加载表数据失败: {e}")
        return pd.DataFrame()


def render_perspective_table(data: pd.DataFrame, table_name: str):
    """使用Perspective CDN渲染数据表"""
    if data.empty:
        st.warning("数据为空")
        return

    try:
        # 注入 Perspective CDN 脚本
        st.markdown("""
        <script type="module">
            import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer/dist/cdn/perspective-viewer.js";
            import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-datagrid/dist/cdn/perspective-viewer-datagrid.js";
            import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-d3fc/dist/cdn/perspective-viewer-d3fc.js";
            import perspective from "https://cdn.jsdelivr.net/npm/@finos/perspective/dist/cdn/perspective.js";
        </script>
        """, unsafe_allow_html=True)

        # 配置选项
        st.subheader("📊 多维度数据分析")

        # 将数据转换为 JSON 格式
        json_data = data.to_json(orient='records')
        
        # 创建 Perspective 容器
        st.markdown(f"""
        <perspective-viewer style="height: 600px;">
            <script>
                const viewer = document.querySelector('perspective-viewer');
                const data = {json_data};
                viewer.load(data);
            </script>
        </perspective-viewer>
        """, unsafe_allow_html=True)

        # 显示数据统计
        st.subheader("📈 数据统计")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("总行数", f"{len(data):,}")
        with col2:
            st.metric("总列数", len(data.columns))
        with col3:
            numeric_cols = data.select_dtypes(include=['number']).columns
            st.metric("数值列", len(numeric_cols))
        with col4:
            categorical_cols = data.select_dtypes(include=['object', 'category']).columns
            st.metric("分类列", len(categorical_cols))

    except Exception as e:
        logger.error(f"渲染数据表失败: {e}")
        st.error(f"渲染数据表失败: {str(e)}")
        st.dataframe(data, use_container_width=True)


def render_data_explorer_main():
    """渲染数据探索器主界面"""
    st.header("🔍 数据探索器")
    st.markdown("使用 Perspective 进行多维度数据分析和可视化")
    
    if not DB_AVAILABLE:
        st.error("❌ 数据库连接不可用，请检查配置")
        return
    
    # 获取可用表
    tables = get_available_tables()
    
    if not tables:
        st.warning("⚠️ 数据库中没有找到表")
        return
    
    # 表选择
    st.subheader("📋 选择数据表")
    
    table_options = [f"{t['name']} ({t['columns']} 列)" for t in tables]
    selected_index = st.selectbox(
        "选择要分析的表",
        range(len(table_options)),
        format_func=lambda x: table_options[x]
    )
    
    selected_table = tables[selected_index]['name']
    
    # 显示表信息
    table_info = get_table_info(selected_table)
    
    if table_info:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader(f"📊 表: {selected_table}")
            st.metric("总记录数", f"{table_info['total_rows']:,}")
            st.markdown(f"**用途**: {table_info['purpose']}")
            
        with col2:
            st.subheader("🏗️ 表结构")
            if not table_info['structure'].empty:
                st.dataframe(
                    table_info['structure'][['column_name', 'data_type', 'is_nullable']],
                    use_container_width=True
                )
        
        # 显示字段定义
        st.subheader("📖 字段定义")
        field_definitions = []
        for col in table_info['structure']['column_name']:
            definition = table_info['field_definitions'].get(col, '暂无描述')
            field_definitions.append({
                '字段名': col,
                '定义': definition
            })
        st.dataframe(pd.DataFrame(field_definitions), use_container_width=True)
        
        # 数据加载选项
        st.subheader("⚙️ 数据加载选项")
        
        col1, col2 = st.columns(2)
        with col1:
            limit = st.number_input(
                "加载记录数限制",
                min_value=100,
                max_value=10000,
                value=1000,
                step=100
            )
        
        with col2:
            if st.button("🔄 加载数据", type="primary"):
                with st.spinner(f"正在加载 {selected_table} 表数据..."):
                    data = load_table_data(selected_table, limit)
                    
                    if not data.empty:
                        st.success(f"✅ 成功加载 {len(data)} 条记录")
                        
                        # 使用Perspective渲染数据
                        render_perspective_table(data, selected_table)
                        
                    else:
                        st.warning("⚠️ 表中没有数据或加载失败")
    else:
        st.error(f"❌ 无法获取表 {selected_table} 的信息")
