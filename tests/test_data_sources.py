"""
数据源测试模块
测试各种数据源的功能和错误处理
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch
from datetime import datetime, date

from src.data_sources.base_data_source import DataRequest, DataType, DataSourceError
from src.data_sources.akshare_data_source import AkShareDataSource
from src.data_sources.tushare_data_source import TushareDataSource
from src.data_sources.data_source_factory import DataSourceFactory
from src.data_sources.data_source_manager import DataSourceManager
from src.data_sources.models.request import DataRequest as ModelDataRequest
from src.data_sources.models.response import DataResponse as ModelDataResponse


class TestAkShareDataSource:
    """AkShare数据源测试类"""
    
    @pytest.fixture
    def akshare_client(self):
        """创建AkShare数据源实例"""
        return AkShareDataSource()
    
    def test_initialization(self, akshare_client):
        """测试初始化"""
        assert akshare_client is not None
        assert hasattr(akshare_client, 'initialize')
        assert hasattr(akshare_client, 'fetch_data')
    
    def test_initialize_success(self, akshare_client):
        """测试成功初始化"""
        with patch('akshare.stock_info_a_code_name') as mock_stock_info:
            mock_stock_info.return_value = pd.DataFrame({
                'code': ['000001', '000002'],
                'name': ['平安银行', '万科A']
            })
            
            result = akshare_client.initialize()
            
            assert result is True
    
    def test_initialize_failure(self, akshare_client):
        """测试初始化失败"""
        with patch('akshare.stock_info_a_code_name') as mock_stock_info:
            mock_stock_info.side_effect = Exception("网络连接失败")
            
            result = akshare_client.initialize()
            
            assert result is False
    
    def test_fetch_stock_basic_data(self, akshare_client):
        """测试获取股票基础信息"""
        with patch('akshare.stock_info_a_code_name') as mock_stock_info:
            mock_stock_info.return_value = pd.DataFrame({
                'code': ['000001', '000002'],
                'name': ['平安银行', '万科A']
            })
            
            request = DataRequest(data_type=DataType.STOCK_BASIC)
            response = akshare_client.fetch_data(request)
            
            assert response.success is True
            assert not response.data.empty
            assert len(response.data) == 2
    
    def test_fetch_daily_quotes(self, akshare_client):
        """测试获取日线数据"""
        with patch('akshare.stock_zh_a_hist') as mock_hist:
            mock_hist.return_value = pd.DataFrame({
                '日期': ['2024-01-01', '2024-01-02'],
                '开盘': [15.0, 16.0],
                '最高': [16.0, 17.0],
                '最低': [14.0, 15.0],
                '收盘': [15.5, 16.5],
                '成交量': [1000000, 1200000],
                '成交额': [15000000, 19200000],
            })
            
            request = DataRequest(
                data_type=DataType.STOCK_DAILY,
                symbol='000001.SZ',
                start_date='2024-01-01',
                end_date='2024-01-02'
            )
            response = akshare_client.fetch_data(request)
            
            assert response.success is True
            assert not response.data.empty
            assert len(response.data) == 2
    
    def test_fetch_data_with_invalid_request(self, akshare_client):
        """测试无效请求的数据获取"""
        request = DataRequest(data_type=None)
        response = akshare_client.fetch_data(request)
        
        assert response.success is False
        assert "不支持的数据类型" in response.error_message
    
    def test_fetch_data_with_network_error(self, akshare_client):
        """测试网络错误时的数据获取"""
        with patch('akshare.stock_info_a_code_name') as mock_stock_info:
            mock_stock_info.side_effect = Exception("网络连接失败")
            
            request = DataRequest(data_type=DataType.STOCK_BASIC)
            response = akshare_client.fetch_data(request)
            
            assert response.success is False
            assert "网络连接失败" in response.error_message


class TestTushareDataSource:
    """Tushare数据源测试类"""
    
    @pytest.fixture
    def tushare_client(self):
        """创建Tushare数据源实例"""
        return TushareDataSource()
    
    def test_initialization(self, tushare_client):
        """测试初始化"""
        assert tushare_client is not None
        assert hasattr(tushare_client, 'initialize')
        assert hasattr(tushare_client, 'fetch_data')
    
    def test_initialize_success(self, tushare_client):
        """测试成功初始化"""
        with patch('tushare.pro.client') as mock_tushare:
            mock_tushare.return_value = Mock()
            
            result = tushare_client.initialize()
            
            assert result is True
    
    def test_initialize_failure(self, tushare_client):
        """测试初始化失败"""
        with patch('tushare.pro.client') as mock_tushare:
            mock_tushare.side_effect = Exception("网络连接失败")
            
            result = tushare_client.initialize()
            
            assert result is False
    
    def test_fetch_stock_basic_data(self, tushare_client):
        """测试获取股票基础信息"""
        with patch('tushare.pro.client') as mock_tushare:
            mock_tushare.return_value = Mock()
            
            request = DataRequest(data_type=DataType.STOCK_BASIC)
            response = tushare_client.fetch_data(request)
            
            assert response.success is True
            assert not response.data.empty
            assert len(response.data) == 2
    
    def test_fetch_daily_quotes(self, tushare_client):
        """测试获取日线数据"""
        with patch('tushare.pro.client') as mock_tushare:
            mock_tushare.return_value = Mock()
            
            request = DataRequest(
                data_type=DataType.STOCK_DAILY,
                symbol='000001.SZ',
                start_date='2024-01-01',
                end_date='2024-01-02'
            )
            response = tushare_client.fetch_data(request)
            
            assert response.success is True
            assert not response.data.empty
            assert len(response.data) == 2
    
    def test_fetch_data_with_invalid_request(self, tushare_client):
        """测试无效请求的数据获取"""
        request = DataRequest(data_type=None)
        response = tushare_client.fetch_data(request)
        
        assert response.success is False
        assert "不支持的数据类型" in response.error_message
    
    def test_fetch_data_with_network_error(self, tushare_client):
        """测试网络错误时的数据获取"""
        with patch('tushare.pro.client') as mock_tushare:
            mock_tushare.side_effect = Exception("网络连接失败")
            
            request = DataRequest(data_type=DataType.STOCK_BASIC)
            response = tushare_client.fetch_data(request)
            
            assert response.success is False
            assert "网络连接失败" in response.error_message


class TestDataSourceFactory:
    """数据源工厂测试类"""
    
    def test_create_akshare_source(self):
        """测试创建AkShare数据源"""
        source = DataSourceFactory.create_data_source('akshare')
        
        assert source is not None
        assert isinstance(source, AkShareDataSource)
    
    def test_create_tushare_source(self):
        """测试创建Tushare数据源"""
        source = DataSourceFactory.create_data_source('tushare')
        
        assert source is not None
        assert isinstance(source, TushareDataSource)
    
    def test_create_invalid_source(self):
        """测试创建无效数据源"""
        with pytest.raises(ValueError):
            DataSourceFactory.create_data_source('invalid_source')
    
    def test_get_available_sources(self):
        """测试获取可用数据源列表"""
        sources = DataSourceFactory.get_available_sources()
        
        assert 'akshare' in sources
        assert 'tushare' in sources


class TestDataSourceManager:
    """数据源管理器测试类"""
    
    @pytest.fixture
    def manager(self):
        """创建数据源管理器实例"""
        return DataSourceManager()
    
    def test_initialization(self, manager):
        """测试初始化"""
        assert manager is not None
        assert hasattr(manager, 'data_sources')
        assert hasattr(manager, 'add_data_source')
        assert hasattr(manager, 'get_data_source')
    
    def test_add_data_source(self, manager):
        """测试添加数据源"""
        akshare_source = AkShareDataSource()
        manager.add_data_source('akshare', akshare_source)
        
        assert 'akshare' in manager.data_sources
        assert manager.data_sources['akshare'] == akshare_source
    
    def test_get_data_source(self, manager):
        """测试获取数据源"""
        akshare_source = AkShareDataSource()
        manager.add_data_source('akshare', akshare_source)
        
        retrieved_source = manager.get_data_source('akshare')
        
        assert retrieved_source == akshare_source
    
    def test_get_nonexistent_data_source(self, manager):
        """测试获取不存在的数据源"""
        source = manager.get_data_source('nonexistent')
        
        assert source is None
    
    def test_remove_data_source(self, manager):
        """测试移除数据源"""
        akshare_source = AkShareDataSource()
        manager.add_data_source('akshare', akshare_source)
        
        manager.remove_data_source('akshare')
        
        assert 'akshare' not in manager.data_sources
    
    def test_get_all_sources(self, manager):
        """测试获取所有数据源"""
        akshare_source = AkShareDataSource()
        tushare_source = TushareDataSource()
        
        manager.add_data_source('akshare', akshare_source)
        manager.add_data_source('tushare', tushare_source)
        
        all_sources = manager.get_all_sources()
        
        assert len(all_sources) == 2
        assert 'akshare' in all_sources
        assert 'tushare' in all_sources


class TestDataRequest:
    """数据请求测试类"""
    
    def test_create_valid_request(self):
        """测试创建有效请求"""
        request = DataRequest(
            data_type=DataType.STOCK_DAILY,
            symbol='000001.SZ',
            start_date='2024-01-01',
            end_date='2024-01-02'
        )
        
        assert request.data_type == DataType.STOCK_DAILY
        assert request.symbol == '000001.SZ'
        assert request.start_date == '2024-01-01'
        assert request.end_date == '2024-01-02'
    
    def test_create_request_with_defaults(self):
        """测试创建带默认值的请求"""
        request = DataRequest(data_type=DataType.STOCK_BASIC)
        
        assert request.data_type == DataType.STOCK_BASIC
        assert request.symbol is None
        assert request.start_date is None
        assert request.end_date is None
    
    def test_request_validation(self):
        """测试请求验证"""
        # 测试无效的数据类型
        with pytest.raises(ValueError):
            DataRequest(data_type=None)
        
        # 测试无效的日期格式
        with pytest.raises(ValueError):
            DataRequest(
                data_type=DataType.STOCK_DAILY,
                start_date='invalid_date'
            )


class TestDataResponse:
    """数据响应测试类"""
    
    def test_create_success_response(self):
        """测试创建成功响应"""
        from src.data_sources.models.response import DataResponse
        
        data = pd.DataFrame({'test': [1, 2, 3]})
        response = DataResponse(success=True, data=data)
        
        assert response.success is True
        assert response.data.equals(data)
        assert response.error_message is None
    
    def test_create_error_response(self):
        """测试创建错误响应"""
        from src.data_sources.models.response import DataResponse
        
        response = DataResponse(
            success=False,
            error_message="网络连接失败"
        )
        
        assert response.success is False
        assert response.error_message == "网络连接失败"
        assert response.data is None


class TestIntegration:
    """集成测试类"""
    
    def test_data_source_factory_and_manager_integration(self):
        """测试数据源工厂和管理器集成"""
        # 创建管理器
        manager = DataSourceManager()
        
        # 通过工厂创建数据源
        akshare_source = DataSourceFactory.create_data_source('akshare')
        tushare_source = DataSourceFactory.create_data_source('tushare')
        
        # 添加到管理器
        manager.add_data_source('akshare', akshare_source)
        manager.add_data_source('tushare', tushare_source)
        
        # 验证集成
        assert len(manager.get_all_sources()) == 2
        assert manager.get_data_source('akshare') == akshare_source
        assert manager.get_data_source('tushare') == tushare_source
    
    def test_data_request_response_integration(self):
        """测试数据请求和响应集成"""
        # 创建请求
        request = DataRequest(
            data_type=DataType.STOCK_BASIC
        )
        
        # 模拟数据源处理请求
        mock_data = pd.DataFrame({
            'code': ['000001', '000002'],
            'name': ['平安银行', '万科A']
        })
        
        from src.data_sources.models.response import DataResponse
        response = DataResponse(success=True, data=mock_data)
        
        # 验证集成
        assert request.data_type == DataType.STOCK_BASIC
        assert response.success is True
        assert len(response.data) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 