"""
Dashboard性能优化器
提供缓存、异步加载、数据预处理等性能优化功能
"""
import streamlit as st
import asyncio
import time
import threading
from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime, timedelta
from functools import wraps, lru_cache
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor
import hashlib
import pickle
import json

try:
    from src.utils.logger import get_logger
    from src.utils.cache_manager import global_cache_manager, CacheLevel
    from src.utils.async_processor import global_processor
except ImportError:
    import logging
    def get_logger(name):
        return logging.getLogger(name)
    
    # 创建模拟对象
    class MockCacheManager:
        def get(self, key): return None
        def set(self, key, value, ttl=None): pass
        def cache_decorator(self, ttl=None, key_func=None, levels=None):
            def decorator(func):
                return func
            return decorator
    
    global_cache_manager = MockCacheManager()
    global_processor = None

logger = get_logger("dashboard_performance")


class DashboardCache:
    """Dashboard专用缓存"""
    
    def __init__(self):
        self.session_cache = {}
        self.page_cache = {}
        self.component_cache = {}
    
    def get_session_cache(self, key: str) -> Any:
        """获取会话缓存"""
        session_id = self._get_session_id()
        return self.session_cache.get(f"{session_id}_{key}")
    
    def set_session_cache(self, key: str, value: Any, ttl_minutes: int = 30):
        """设置会话缓存"""
        session_id = self._get_session_id()
        cache_key = f"{session_id}_{key}"
        self.session_cache[cache_key] = {
            "value": value,
            "expires_at": datetime.now() + timedelta(minutes=ttl_minutes)
        }
    
    def get_page_cache(self, page_name: str, key: str) -> Any:
        """获取页面缓存"""
        cache_key = f"{page_name}_{key}"
        cached_item = self.page_cache.get(cache_key)
        
        if cached_item and cached_item["expires_at"] > datetime.now():
            return cached_item["value"]
        
        return None
    
    def set_page_cache(self, page_name: str, key: str, value: Any, ttl_minutes: int = 10):
        """设置页面缓存"""
        cache_key = f"{page_name}_{key}"
        self.page_cache[cache_key] = {
            "value": value,
            "expires_at": datetime.now() + timedelta(minutes=ttl_minutes)
        }
    
    def _get_session_id(self) -> str:
        """获取会话ID"""
        if hasattr(st, 'session_state') and hasattr(st.session_state, 'session_id'):
            return st.session_state.session_id
        else:
            # 生成临时会话ID
            if 'temp_session_id' not in st.session_state:
                st.session_state.temp_session_id = hashlib.md5(
                    str(time.time()).encode()
                ).hexdigest()[:8]
            return st.session_state.temp_session_id
    
    def clear_expired(self):
        """清理过期缓存"""
        now = datetime.now()
        
        # 清理会话缓存
        expired_keys = [
            key for key, item in self.session_cache.items()
            if item["expires_at"] <= now
        ]
        for key in expired_keys:
            del self.session_cache[key]
        
        # 清理页面缓存
        expired_keys = [
            key for key, item in self.page_cache.items()
            if item["expires_at"] <= now
        ]
        for key in expired_keys:
            del self.page_cache[key]


# 全局Dashboard缓存
dashboard_cache = DashboardCache()


def st_cache_data(ttl: int = 3600, show_spinner: bool = True, key: str = None):
    """Streamlit数据缓存装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            cache_key = key or f"{func.__name__}_{hash(str(args) + str(sorted(kwargs.items())))}"
            
            # 尝试从缓存获取
            cached_result = global_cache_manager.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # 显示加载指示器
            if show_spinner:
                with st.spinner(f"加载 {func.__name__}..."):
                    result = func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # 缓存结果
            global_cache_manager.set(cache_key, result, ttl)
            
            return result
        
        return wrapper
    return decorator


def st_cache_resource(ttl: int = 3600, show_spinner: bool = True):
    """Streamlit资源缓存装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = f"resource_{func.__name__}_{hash(str(args) + str(sorted(kwargs.items())))}"
            
            # 尝试从缓存获取
            cached_result = global_cache_manager.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            if show_spinner:
                with st.spinner(f"初始化 {func.__name__}..."):
                    result = func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # 缓存资源
            global_cache_manager.set(cache_key, result, ttl)
            
            return result
        
        return wrapper
    return decorator


class AsyncDataLoader:
    """异步数据加载器"""
    
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.loading_states = {}
    
    def load_async(self, 
                   loader_func: Callable,
                   placeholder_key: str,
                   *args,
                   show_progress: bool = True,
                   **kwargs) -> Any:
        """异步加载数据"""
        
        # 检查是否已在加载
        if placeholder_key in self.loading_states:
            if show_progress:
                st.info("数据加载中...")
            return None
        
        # 检查缓存
        cached_data = dashboard_cache.get_page_cache("async_data", placeholder_key)
        if cached_data is not None:
            return cached_data
        
        # 标记为加载中
        self.loading_states[placeholder_key] = True
        
        try:
            if show_progress:
                progress_bar = st.progress(0)
                status_text = st.empty()
                status_text.text("开始加载数据...")
            
            # 在线程池中执行加载
            future = self.executor.submit(loader_func, *args, **kwargs)
            
            # 模拟进度更新
            if show_progress:
                for i in range(100):
                    if future.done():
                        break
                    progress_bar.progress(i + 1)
                    time.sleep(0.01)
                
                status_text.text("数据加载完成!")
                progress_bar.progress(100)
            
            # 获取结果
            result = future.result(timeout=30)
            
            # 缓存结果
            dashboard_cache.set_page_cache("async_data", placeholder_key, result, ttl_minutes=5)
            
            if show_progress:
                progress_bar.empty()
                status_text.empty()
            
            return result
            
        except Exception as e:
            logger.error(f"异步加载失败: {e}")
            if show_progress:
                st.error(f"数据加载失败: {e}")
            return None
        finally:
            # 清除加载状态
            if placeholder_key in self.loading_states:
                del self.loading_states[placeholder_key]


# 全局异步加载器
async_loader = AsyncDataLoader()


class ChartOptimizer:
    """图表优化器"""
    
    @staticmethod
    def optimize_dataframe_for_chart(df: pd.DataFrame, max_points: int = 1000) -> pd.DataFrame:
        """优化DataFrame用于图表显示"""
        if len(df) <= max_points:
            return df
        
        # 数据采样
        step = len(df) // max_points
        return df.iloc[::step].copy()
    
    @staticmethod
    def create_optimized_line_chart(df: pd.DataFrame, 
                                  x_col: str, 
                                  y_col: str,
                                  title: str = "",
                                  max_points: int = 1000) -> go.Figure:
        """创建优化的线图"""
        # 优化数据
        optimized_df = ChartOptimizer.optimize_dataframe_for_chart(df, max_points)
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=optimized_df[x_col],
            y=optimized_df[y_col],
            mode='lines',
            name=y_col,
            line=dict(width=2)
        ))
        
        fig.update_layout(
            title=title,
            xaxis_title=x_col,
            yaxis_title=y_col,
            template="plotly_white",
            height=400,
            margin=dict(l=50, r=50, t=50, b=50)
        )
        
        return fig
    
    @staticmethod
    def create_optimized_candlestick(df: pd.DataFrame,
                                   date_col: str = 'trade_date',
                                   open_col: str = 'open',
                                   high_col: str = 'high', 
                                   low_col: str = 'low',
                                   close_col: str = 'close',
                                   volume_col: str = 'volume',
                                   max_points: int = 500) -> go.Figure:
        """创建优化的K线图"""
        # 优化数据
        optimized_df = ChartOptimizer.optimize_dataframe_for_chart(df, max_points)
        
        # 创建子图
        from plotly.subplots import make_subplots
        
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            subplot_titles=('价格', '成交量'),
            row_width=[0.7, 0.3]
        )
        
        # K线图
        fig.add_trace(
            go.Candlestick(
                x=optimized_df[date_col],
                open=optimized_df[open_col],
                high=optimized_df[high_col],
                low=optimized_df[low_col],
                close=optimized_df[close_col],
                name="K线"
            ),
            row=1, col=1
        )
        
        # 成交量
        if volume_col in optimized_df.columns:
            fig.add_trace(
                go.Bar(
                    x=optimized_df[date_col],
                    y=optimized_df[volume_col],
                    name="成交量",
                    marker_color='rgba(158,202,225,0.8)'
                ),
                row=2, col=1
            )
        
        fig.update_layout(
            template="plotly_white",
            height=600,
            xaxis_rangeslider_visible=False,
            margin=dict(l=50, r=50, t=50, b=50)
        )
        
        return fig


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self):
        self.metrics = {}
        self.start_times = {}
    
    def start_timer(self, operation: str):
        """开始计时"""
        self.start_times[operation] = time.time()
    
    def end_timer(self, operation: str):
        """结束计时"""
        if operation in self.start_times:
            duration = time.time() - self.start_times[operation]
            
            if operation not in self.metrics:
                self.metrics[operation] = []
            
            self.metrics[operation].append(duration)
            del self.start_times[operation]
            
            return duration
        return None
    
    def get_average_time(self, operation: str) -> float:
        """获取平均时间"""
        if operation in self.metrics and self.metrics[operation]:
            return sum(self.metrics[operation]) / len(self.metrics[operation])
        return 0.0
    
    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        report = {}
        
        for operation, times in self.metrics.items():
            if times:
                report[operation] = {
                    "count": len(times),
                    "average_time": sum(times) / len(times),
                    "min_time": min(times),
                    "max_time": max(times),
                    "total_time": sum(times)
                }
        
        return report


# 全局性能监控器
performance_monitor = PerformanceMonitor()


def performance_timer(operation_name: str):
    """性能计时装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            performance_monitor.start_timer(operation_name)
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = performance_monitor.end_timer(operation_name)
                if duration and duration > 1.0:  # 超过1秒的操作记录警告
                    logger.warning(f"操作 {operation_name} 耗时 {duration:.2f}s")
        
        return wrapper
    return decorator


class DataPreprocessor:
    """数据预处理器"""
    
    @staticmethod
    def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
        """优化DataFrame内存使用"""
        optimized_df = df.copy()
        
        for col in optimized_df.columns:
            col_type = optimized_df[col].dtype
            
            if col_type == 'object':
                # 尝试转换为category
                if optimized_df[col].nunique() / len(optimized_df) < 0.5:
                    optimized_df[col] = optimized_df[col].astype('category')
            
            elif col_type == 'int64':
                # 优化整数类型
                col_min = optimized_df[col].min()
                col_max = optimized_df[col].max()
                
                if col_min >= 0:
                    if col_max < 255:
                        optimized_df[col] = optimized_df[col].astype('uint8')
                    elif col_max < 65535:
                        optimized_df[col] = optimized_df[col].astype('uint16')
                    elif col_max < 4294967295:
                        optimized_df[col] = optimized_df[col].astype('uint32')
                else:
                    if col_min > -128 and col_max < 127:
                        optimized_df[col] = optimized_df[col].astype('int8')
                    elif col_min > -32768 and col_max < 32767:
                        optimized_df[col] = optimized_df[col].astype('int16')
                    elif col_min > -2147483648 and col_max < 2147483647:
                        optimized_df[col] = optimized_df[col].astype('int32')
            
            elif col_type == 'float64':
                # 优化浮点类型
                optimized_df[col] = pd.to_numeric(optimized_df[col], downcast='float')
        
        return optimized_df
    
    @staticmethod
    def prepare_chart_data(df: pd.DataFrame, 
                          date_col: str = None,
                          numeric_cols: List[str] = None) -> pd.DataFrame:
        """准备图表数据"""
        chart_df = df.copy()
        
        # 处理日期列
        if date_col and date_col in chart_df.columns:
            chart_df[date_col] = pd.to_datetime(chart_df[date_col])
            chart_df = chart_df.sort_values(date_col)
        
        # 处理数值列
        if numeric_cols:
            for col in numeric_cols:
                if col in chart_df.columns:
                    chart_df[col] = pd.to_numeric(chart_df[col], errors='coerce')
        
        # 移除空行
        chart_df = chart_df.dropna()
        
        return chart_df


class ComponentOptimizer:
    """组件优化器"""
    
    @staticmethod
    def create_optimized_selectbox(label: str,
                                 options: List[Any],
                                 key: str = None,
                                 max_options: int = 1000) -> Any:
        """创建优化的选择框"""
        if len(options) > max_options:
            # 对于大量选项，使用搜索功能
            search_term = st.text_input(f"搜索 {label}", key=f"{key}_search" if key else None)
            
            if search_term:
                filtered_options = [
                    opt for opt in options 
                    if search_term.lower() in str(opt).lower()
                ][:max_options]
            else:
                filtered_options = options[:max_options]
                st.info(f"显示前 {max_options} 个选项，使用搜索功能查找更多")
            
            return st.selectbox(label, filtered_options, key=key)
        else:
            return st.selectbox(label, options, key=key)
    
    @staticmethod
    def create_optimized_dataframe(df: pd.DataFrame,
                                 max_rows: int = 1000,
                                 key: str = None) -> None:
        """创建优化的数据表格"""
        if len(df) > max_rows:
            st.info(f"数据表包含 {len(df)} 行，显示前 {max_rows} 行")
            
            # 添加分页功能
            page_size = max_rows
            total_pages = (len(df) - 1) // page_size + 1
            
            if total_pages > 1:
                page = st.number_input(
                    "页码", 
                    min_value=1, 
                    max_value=total_pages, 
                    value=1,
                    key=f"{key}_page" if key else None
                )
                
                start_idx = (page - 1) * page_size
                end_idx = min(start_idx + page_size, len(df))
                display_df = df.iloc[start_idx:end_idx]
                
                st.info(f"第 {page}/{total_pages} 页，显示第 {start_idx+1}-{end_idx} 行")
            else:
                display_df = df.head(max_rows)
        else:
            display_df = df
        
        st.dataframe(display_df, key=key)


def optimize_dashboard_performance():
    """优化Dashboard性能的主函数"""
    
    # 设置页面配置
    if 'performance_optimized' not in st.session_state:
        st.set_page_config(
            page_title="Vespera Dashboard",
            page_icon="📈",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # 添加自定义CSS
        st.markdown("""
        <style>
        .main > div {
            padding-top: 2rem;
        }
        .stMetric {
            background-color: #f0f2f6;
            border: 1px solid #e0e0e0;
            padding: 1rem;
            border-radius: 0.5rem;
        }
        .stPlotlyChart {
            border: 1px solid #e0e0e0;
            border-radius: 0.5rem;
        }
        </style>
        """, unsafe_allow_html=True)
        
        st.session_state.performance_optimized = True
    
    # 清理过期缓存
    dashboard_cache.clear_expired()
    
    # 显示性能指标（仅在调试模式下）
    if st.sidebar.checkbox("显示性能指标", value=False):
        with st.sidebar.expander("性能监控"):
            report = performance_monitor.get_performance_report()
            if report:
                for operation, metrics in report.items():
                    st.metric(
                        f"{operation} (平均)",
                        f"{metrics['average_time']:.3f}s",
                        f"执行 {metrics['count']} 次"
                    )
            else:
                st.info("暂无性能数据")


# 便捷装饰器
def dashboard_page(title: str, cache_ttl: int = 300):
    """Dashboard页面装饰器"""
    def decorator(func):
        @wraps(func)
        @performance_timer(f"page_{func.__name__}")
        def wrapper(*args, **kwargs):
            # 设置页面标题
            st.title(title)
            
            # 优化性能
            optimize_dashboard_performance()
            
            # 执行页面函数
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def cached_component(ttl_minutes: int = 10, key_prefix: str = ""):
    """缓存组件装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            cache_key = f"{key_prefix}{func.__name__}_{hash(str(args) + str(sorted(kwargs.items())))}"
            
            # 尝试从页面缓存获取
            cached_result = dashboard_cache.get_page_cache("components", cache_key)
            if cached_result is not None:
                return cached_result
            
            # 执行函数
            result = func(*args, **kwargs)
            
            # 缓存结果
            dashboard_cache.set_page_cache("components", cache_key, result, ttl_minutes)
            
            return result
        
        return wrapper
    return decorator