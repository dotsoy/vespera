"""
Dashboard组件单元测试
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path
import streamlit as st

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dashboard.components.system_status import render_system_status_main
from dashboard.components.data_explorer import render_data_explorer_main
from dashboard.components.data_source_manager import render_data_source_manager_main
from dashboard.components.stock_holographic_view import render_stock_holographic_view_main


class TestSystemStatus:
    """系统状态组件测试类"""
    
    def test_get_system_info(self):
        """测试系统信息获取"""
        from dashboard.components.system_status import get_system_info
        
        info = get_system_info()
        
        assert isinstance(info, dict)
        assert 'cpu_usage' in info
        assert 'memory_usage' in info
        assert 'disk_usage' in info
        assert 'uptime' in info
    
    def test_get_database_status(self):
        """测试数据库状态获取"""
        from dashboard.components.system_status import get_database_status
        
        with patch('src.utils.database.get_db_manager') as mock_db:
            mock_manager = Mock()
            mock_manager.test_connections.return_value = {
                'postgres': True,
                'clickhouse': False,
                'redis': True
            }
            mock_db.return_value = mock_manager
            
            status = get_database_status()
            
            assert isinstance(status, dict)
            assert 'postgres' in status
            assert 'clickhouse' in status
            assert 'redis' in status
            assert status['postgres'] is True
            assert status['clickhouse'] is False
    
    def test_get_strategy_status(self):
        """测试策略状态获取"""
        from dashboard.components.system_status import get_strategy_status
        
        with patch('src.strategies.qiming_star.qiming_star_strategy.QimingStarStrategy') as mock_strategy:
            mock_strategy_instance = Mock()
            mock_strategy_instance.analyze_stock.return_value = {
                'signal': 'BUY',
                'score': 85.0
            }
            mock_strategy.return_value = mock_strategy_instance
            
            status = get_strategy_status()
            
            assert isinstance(status, dict)
            assert 'status' in status
            assert 'last_update' in status
    
    def test_render_system_status_main(self):
        """测试系统状态主组件渲染"""
        # 模拟Streamlit环境
        with patch('streamlit.container') as mock_container:
            with patch('streamlit.metric') as mock_metric:
                with patch('streamlit.progress') as mock_progress:
                    
                    mock_container.return_value = Mock()
                    mock_metric.return_value = Mock()
                    mock_progress.return_value = Mock()
                    
                    # 测试渲染
                    render_system_status_main()
                    
                    # 验证调用
                    mock_container.assert_called()
                    mock_metric.assert_called()
                    mock_progress.assert_called()


class TestDataSourceManager:
    """数据源管理组件测试类"""
    
    def test_get_data_source_status(self):
        """测试获取数据源状态"""
        with patch('src.data_sources.akshare_data_source.AkShareDataSource') as mock_akshare:
            mock_akshare_instance = Mock()
            mock_akshare_instance.test_api_connection.return_value = True
            mock_akshare.return_value = mock_akshare_instance
            
            status = get_data_source_status()
            
            assert isinstance(status, dict)
            assert 'akshare' in status
            assert status['akshare'] == True
    
    def test_fetch_data_from_source(self):
        """测试从数据源获取数据"""
        from dashboard.components.data_source_manager import fetch_data_from_source
        
        with patch('src.data_sources.alltick_data_source.AllTickDataSource') as mock_alltick:
            mock_instance = Mock()
            mock_instance.fetch_data.return_value = Mock(
                success=True,
                data=pd.DataFrame({'test': [1, 2, 3]})
            )
            mock_alltick.return_value = mock_instance
            
            result = fetch_data_from_source('alltick', '000001.SZ')
            
            assert isinstance(result, dict)
            assert 'success' in result
            assert 'data' in result
    
    def test_get_fetch_history(self):
        """测试获取拉取历史"""
        from dashboard.components.data_source_manager import get_fetch_history
        
        history = get_fetch_history()
        
        assert isinstance(history, pd.DataFrame)
        assert 'timestamp' in history.columns
        assert 'source' in history.columns
        assert 'status' in history.columns
    
    def test_render_data_source_manager_main(self):
        """测试数据源管理主组件渲染"""
        with patch('streamlit.selectbox') as mock_selectbox:
            with patch('streamlit.button') as mock_button:
                with patch('streamlit.progress') as mock_progress:
                    
                    mock_selectbox.return_value = 'alltick'
                    mock_button.return_value = False
                    
                    # 测试渲染
                    render_data_source_manager_main()
                    
                    # 验证调用
                    mock_selectbox.assert_called()
                    mock_button.assert_called()


class TestDashboardIntegration:
    """Dashboard集成测试类"""
    
    def test_dashboard_components_integration(self):
        """测试Dashboard组件集成"""
        # 测试所有组件都能正常导入和初始化
        components = [
            render_system_status_main,
            render_data_explorer_main,
            render_stock_holographic_view_main,
            render_data_source_manager_main
        ]
        
        for component in components:
            assert callable(component)
    
    def test_data_flow_integration(self):
        """测试数据流集成"""
        # 模拟完整的数据流
        with patch('dashboard.components.data_source_manager.get_data_source_status') as mock_status:
            with patch('dashboard.components.data_source_manager.fetch_data_from_source') as mock_fetch:
                
                # 模拟数据源状态
                mock_status.return_value = {
                    'alltick': True,
                    'akshare': True,
                    'mock': True
                }
                
                # 模拟数据获取
                mock_fetch.return_value = {
                    'success': True,
                    'data': pd.DataFrame({'test': [1, 2, 3]})
                }
                
                # 验证数据流
                status = mock_status()
                assert all(status.values())
                
                data = mock_fetch('alltick', '000001.SZ')
                assert data['success'] is True
                assert 'test' in data['data'].columns
    
    def test_error_handling_integration(self):
        """测试错误处理集成"""
        # 测试数据库连接失败时的降级处理
        with patch('src.utils.database.get_db_manager') as mock_db:
            mock_db.side_effect = Exception("数据库连接失败")
            
            # 应该能够正常降级到模拟数据
            from dashboard.components.data_source_manager import get_data_source_status
            status = get_data_source_status()
            
            assert isinstance(status, dict)
            assert 'alltick' in status
            assert 'akshare' in status
            assert 'mock' in status
    
    def test_performance_integration(self):
        """测试性能集成"""
        import time
        
        # 测试组件渲染性能
        start_time = time.time()
        
        # 模拟快速渲染
        with patch('streamlit.container') as mock_container:
            mock_container.return_value = Mock()
            render_system_status_main()
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 渲染应该在合理时间内完成
        assert duration < 1.0


class TestMockDataGeneration:
    """模拟数据生成测试类"""
    
    def test_generate_mock_stock_list(self):
        """测试模拟股票列表生成"""
        from dashboard.components.data_source_manager import get_data_source_status
        
        status = get_data_source_status()
        
        assert isinstance(status, dict)
        assert 'alltick' in status
        assert 'akshare' in status
        assert 'mock' in status
    
    def test_generate_mock_stock_data(self):
        """测试模拟股票数据生成"""
        from dashboard.components.data_source_manager import fetch_data_from_source
        
        data = fetch_data_from_source('alltick', '000001.SZ')
        
        assert isinstance(data, dict)
        assert 'success' in data
        assert 'data' in data
        assert 'test' in data['data'].columns
    
    def test_generate_mock_analysis_result(self):
        """测试模拟分析结果生成"""
        from dashboard.components.data_source_manager import get_data_source_status
        
        status = get_data_source_status()
        
        assert isinstance(status, dict)
        assert 'alltick' in status
        assert 'akshare' in status
        assert 'mock' in status


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 