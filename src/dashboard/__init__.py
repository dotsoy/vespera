"""
Dashboard模块
提供高性能的Streamlit Dashboard组件和优化工具
"""

from .performance_optimizer import (
    DashboardCache,
    AsyncDataLoader,
    ChartOptimizer,
    PerformanceMonitor,
    DataPreprocessor,
    ComponentOptimizer,
    dashboard_cache,
    async_loader,
    performance_monitor,
    st_cache_data,
    st_cache_resource,
    performance_timer,
    dashboard_page,
    cached_component,
    optimize_dashboard_performance
)

__all__ = [
    "DashboardCache",
    "AsyncDataLoader", 
    "ChartOptimizer",
    "PerformanceMonitor",
    "DataPreprocessor",
    "ComponentOptimizer",
    "dashboard_cache",
    "async_loader",
    "performance_monitor",
    "st_cache_data",
    "st_cache_resource", 
    "performance_timer",
    "dashboard_page",
    "cached_component",
    "optimize_dashboard_performance"
]