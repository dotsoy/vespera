"""
数据源综合测试模块
测试AkShare、Tushare等数据源的功能和错误处理
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path
from datetime import datetime, date

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_sources.akshare_data_source import AkShareDataSource
from src.data_sources.tushare_data_source import TushareDataSource
from src.data_sources.data_source_factory import DataSourceFactory
from src.data_sources.data_source_manager import DataSourceManager
from src.data_sources.base_data_source import DataRequest, DataType, DataSourceError


class TestAkShareDataSource:
    """AkShare数据源测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.akshare_source = AkShareDataSource()
    
    def test_initialization(self):
        """测试初始化"""
        assert self.akshare_source is not None
        assert hasattr(self.akshare_source, 'name')
        assert self.akshare_source.name == 'AkShare'
    
    def test_get_stock_list_success(self):
        """测试获取股票列表成功"""
        with patch('akshare.stock_info_a_code_name') as mock_akshare:
            mock_akshare.return_value = pd.DataFrame({
                'code': ['000001', '000002', '000003'],
                'name': ['平安银行', '万科A', '中国神华']
            })
            
            result = self.akshare_source.get_stock_list()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            assert 'code' in result.columns
            assert 'name' in result.columns
    
    def test_get_stock_list_error(self):
        """测试获取股票列表错误"""
        with patch('akshare.stock_info_a_code_name') as mock_akshare:
            mock_akshare.side_effect = Exception("Network error")
            
            result = self.akshare_source.get_stock_list()
            
            assert result is None
    
    def test_get_daily_data_success(self):
        """测试获取日线数据成功"""
        with patch('akshare.stock_zh_a_hist') as mock_akshare:
            mock_akshare.return_value = pd.DataFrame({
                '日期': ['2024-01-01', '2024-01-02', '2024-01-03'],
                '开盘': [15.0, 15.5, 16.0],
                '最高': [15.8, 16.2, 16.5],
                '最低': [14.8, 15.2, 15.8],
                '收盘': [15.5, 16.0, 16.3],
                '成交量': [1000000, 1200000, 1400000],
                '成交额': [15000000, 19200000, 22820000],
                '振幅': [6.67, 6.45, 4.38],
                '涨跌幅': [3.33, 3.23, 1.88],
                '涨跌额': [0.5, 0.5, 0.3],
                '换手率': [0.5, 0.6, 0.7]
            })
            
            result = self.akshare_source.get_daily_data('000001.SZ', '2024-01-01', '2024-01-03')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            assert 'trade_date' in result.columns
            assert 'open_price' in result.columns
            assert 'high_price' in result.columns
            assert 'low_price' in result.columns
            assert 'close_price' in result.columns
            assert 'volume' in result.columns
    
    def test_get_daily_data_error(self):
        """测试获取日线数据错误"""
        with patch('akshare.stock_zh_a_hist') as mock_akshare:
            mock_akshare.side_effect = Exception("Data not available")
            
            result = self.akshare_source.get_daily_data('000001.SZ', '2024-01-01', '2024-01-03')
            
            assert result is None
    
    def test_get_realtime_data_success(self):
        """测试获取实时数据成功"""
        with patch('akshare.stock_zh_a_spot') as mock_akshare:
            mock_akshare.return_value = pd.DataFrame({
                '代码': ['000001'],
                '名称': ['平安银行'],
                '最新价': [15.5],
                '涨跌幅': [2.5],
                '涨跌额': [0.38],
                '成交量': [1000000],
                '成交额': [15500000],
                '振幅': [3.2],
                '最高': [15.8],
                '最低': [15.2],
                '今开': [15.0],
                '昨收': [15.12],
                '量比': [1.2],
                '换手率': [0.5],
                '市盈率-动态': [8.5],
                '市净率': [0.8],
                '总市值': [300000000000],
                '流通市值': [250000000000],
                '涨速': [0.5],
                '5分钟涨跌': [0.2],
                '60日涨跌幅': [10.5],
                '年初至今涨跌幅': [15.2]
            })
            
            result = self.akshare_source.get_realtime_data(['000001.SZ'])
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert 'code' in result.columns
            assert 'name' in result.columns
            assert 'price' in result.columns
    
    def test_get_realtime_data_error(self):
        """测试获取实时数据错误"""
        with patch('akshare.stock_zh_a_spot') as mock_akshare:
            mock_akshare.side_effect = Exception("Real-time data unavailable")
            
            result = self.akshare_source.get_realtime_data(['000001.SZ'])
            
            assert result is None
    
    def test_data_format_conversion(self):
        """测试数据格式转换"""
        # 测试日期格式转换
        test_date = '2024-01-01'
        converted = self.akshare_source._convert_date_format(test_date)
        assert isinstance(converted, str)
        
        # 测试股票代码格式转换
        test_code = '000001'
        converted_code = self.akshare_source._convert_stock_code(test_code)
        assert converted_code == '000001.SZ'
    
    def test_validate_stock_code(self):
        """测试股票代码验证"""
        # 有效代码
        assert self.akshare_source._validate_stock_code('000001.SZ') == True
        assert self.akshare_source._validate_stock_code('000002.SZ') == True
        
        # 无效代码
        assert self.akshare_source._validate_stock_code('invalid') == False
        assert self.akshare_source._validate_stock_code('') == False


class TestTushareDataSource:
    """Tushare数据源测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.tushare_source = TushareDataSource()
    
    def test_initialization(self):
        """测试初始化"""
        assert self.tushare_source is not None
        assert hasattr(self.tushare_source, 'name')
        assert self.tushare_source.name == 'Tushare'
    
    def test_get_stock_list_success(self):
        """测试获取股票列表成功"""
        with patch('tushare.pro.stock_basic') as mock_tushare:
            mock_tushare.return_value = pd.DataFrame({
                'code': ['000001', '000002', '000003'],
                'name': ['平安银行', '万科A', '中国神华']
            })
            
            result = self.tushare_source.get_stock_list()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            assert 'code' in result.columns
            assert 'name' in result.columns
    
    def test_get_stock_list_error(self):
        """测试获取股票列表错误"""
        with patch('tushare.pro.stock_basic') as mock_tushare:
            mock_tushare.side_effect = Exception("Network error")
            
            result = self.tushare_source.get_stock_list()
            
            assert result is None
    
    def test_get_daily_data_success(self):
        """测试获取日线数据成功"""
        with patch('tushare.pro.daily') as mock_tushare:
            mock_tushare.return_value = pd.DataFrame({
                '日期': ['2024-01-01', '2024-01-02', '2024-01-03'],
                '开盘': [15.0, 15.5, 16.0],
                '最高': [15.8, 16.2, 16.5],
                '最低': [14.8, 15.2, 15.8],
                '收盘': [15.5, 16.0, 16.3],
                '成交量': [1000000, 1200000, 1400000],
                '成交额': [15000000, 19200000, 22820000],
                '振幅': [6.67, 6.45, 4.38],
                '涨跌幅': [3.33, 3.23, 1.88],
                '涨跌额': [0.5, 0.5, 0.3],
                '换手率': [0.5, 0.6, 0.7]
            })
            
            result = self.tushare_source.get_daily_data('000001.SZ', '2024-01-01', '2024-01-03')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            assert 'trade_date' in result.columns
            assert 'open_price' in result.columns
            assert 'high_price' in result.columns
            assert 'low_price' in result.columns
            assert 'close_price' in result.columns
            assert 'volume' in result.columns
    
    def test_get_daily_data_error(self):
        """测试获取日线数据错误"""
        with patch('tushare.pro.daily') as mock_tushare:
            mock_tushare.side_effect = Exception("Data not available")
            
            result = self.tushare_source.get_daily_data('000001.SZ', '2024-01-01', '2024-01-03')
            
            assert result is None
    
    def test_get_realtime_data_success(self):
        """测试获取实时数据成功"""
        with patch('tushare.pro.realtime') as mock_tushare:
            mock_tushare.return_value = pd.DataFrame({
                '代码': ['000001'],
                '名称': ['平安银行'],
                '最新价': [15.5],
                '涨跌幅': [2.5],
                '涨跌额': [0.38],
                '成交量': [1000000],
                '成交额': [15500000],
                '振幅': [3.2],
                '最高': [15.8],
                '最低': [15.2],
                '今开': [15.0],
                '昨收': [15.12],
                '量比': [1.2],
                '换手率': [0.5],
                '市盈率-动态': [8.5],
                '市净率': [0.8],
                '总市值': [300000000000],
                '流通市值': [250000000000],
                '涨速': [0.5],
                '5分钟涨跌': [0.2],
                '60日涨跌幅': [10.5],
                '年初至今涨跌幅': [15.2]
            })
            
            result = self.tushare_source.get_realtime_data(['000001.SZ'])
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert 'code' in result.columns
            assert 'name' in result.columns
            assert 'price' in result.columns
    
    def test_get_realtime_data_error(self):
        """测试获取实时数据错误"""
        with patch('tushare.pro.realtime') as mock_tushare:
            mock_tushare.side_effect = Exception("Real-time data unavailable")
            
            result = self.tushare_source.get_realtime_data(['000001.SZ'])
            
            assert result is None
    
    def test_data_format_conversion(self):
        """测试数据格式转换"""
        # 测试日期格式转换
        test_date = '2024-01-01'
        converted = self.tushare_source._convert_date_format(test_date)
        assert isinstance(converted, str)
        
        # 测试股票代码格式转换
        test_code = '000001'
        converted_code = self.tushare_source._convert_stock_code(test_code)
        assert converted_code == '000001.SZ'
    
    def test_validate_stock_code(self):
        """测试股票代码验证"""
        # 有效代码
        assert self.tushare_source._validate_stock_code('000001.SZ') == True
        assert self.tushare_source._validate_stock_code('000002.SZ') == True
        
        # 无效代码
        assert self.tushare_source._validate_stock_code('invalid') == False
        assert self.tushare_source._validate_stock_code('') == False


class TestDataSourceFactory:
    """数据源工厂测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.factory = DataSourceFactory()
    
    def test_create_akshare_source(self):
        """测试创建AkShare数据源"""
        source = self.factory.create_data_source('akshare')
        
        assert source is not None
        assert source.name == 'AkShare'
        assert isinstance(source, AkShareDataSource)
    
    def test_create_tushare_source(self):
        """测试创建Tushare数据源"""
        source = self.factory.create_data_source('tushare')
        
        assert source is not None
        assert source.name == 'Tushare'
        assert isinstance(source, TushareDataSource)
    
    def test_create_invalid_source(self):
        """测试创建无效数据源"""
        source = self.factory.create_data_source('invalid')
        
        assert source is None
    
    def test_get_available_sources(self):
        """测试获取可用数据源列表"""
        sources = self.factory.get_available_sources()
        
        assert isinstance(sources, list)
        assert 'akshare' in sources
        assert 'tushare' in sources
    
    def test_source_configuration(self):
        """测试数据源配置"""
        config = {
            'akshare': {},
            'tushare': {
                'api_key': 'test_key',
                'api_secret': 'test_secret'
            }
        }
        
        sources = self.factory.create_data_sources(config)
        
        assert isinstance(sources, dict)
        assert 'akshare' in sources
        assert 'tushare' in sources
        assert sources['tushare'].api_key == 'test_key'


class TestDataSourceManager:
    """数据源管理器测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.manager = DataSourceManager()
    
    def test_initialization(self):
        """测试初始化"""
        assert self.manager is not None
        assert hasattr(self.manager, 'data_sources')
        assert isinstance(self.manager.data_sources, dict)
    
    def test_add_data_source(self):
        """测试添加数据源"""
        akshare_source = AkShareDataSource()
        
        self.manager.add_data_source('akshare', akshare_source)
        
        assert 'akshare' in self.manager.data_sources
        assert self.manager.data_sources['akshare'] == akshare_source
    
    def test_remove_data_source(self):
        """测试移除数据源"""
        akshare_source = AkShareDataSource()
        self.manager.add_data_source('akshare', akshare_source)
        
        self.manager.remove_data_source('akshare')
        
        assert 'akshare' not in self.manager.data_sources
    
    def test_get_data_source(self):
        """测试获取数据源"""
        akshare_source = AkShareDataSource()
        self.manager.add_data_source('akshare', akshare_source)
        
        source = self.manager.get_data_source('akshare')
        
        assert source == akshare_source
    
    def test_get_data_source_not_found(self):
        """测试获取不存在的数据源"""
        source = self.manager.get_data_source('nonexistent')
        
        assert source is None
    
    def test_get_stock_list_from_all_sources(self):
        """测试从所有数据源获取股票列表"""
        # 模拟AkShare数据源
        akshare_source = Mock()
        akshare_source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001', '000002'],
            'name': ['平安银行', '万科A']
        })
        
        # 模拟Tushare数据源
        tushare_source = Mock()
        tushare_source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001', '000003'],
            'name': ['平安银行', '中国神华']
        })
        
        self.manager.add_data_source('akshare', akshare_source)
        self.manager.add_data_source('tushare', tushare_source)
        
        result = self.manager.get_stock_list_from_all_sources()
        
        assert isinstance(result, dict)
        assert 'akshare' in result
        assert 'tushare' in result
        assert isinstance(result['akshare'], pd.DataFrame)
        assert isinstance(result['tushare'], pd.DataFrame)
    
    def test_get_data_with_fallback(self):
        """测试带降级的数据获取"""
        # 主要数据源失败
        primary_source = Mock()
        primary_source.get_daily_data.side_effect = Exception("Primary source failed")
        
        # 备用数据源成功
        fallback_source = Mock()
        fallback_source.get_daily_data.return_value = pd.DataFrame({
            'trade_date': ['2024-01-01'],
            'close_price': [15.5]
        })
        
        self.manager.add_data_source('primary', primary_source)
        self.manager.add_data_source('fallback', fallback_source)
        
        result = self.manager.get_data_with_fallback(
            '000001.SZ', '2024-01-01', '2024-01-01',
            primary_sources=['primary'],
            fallback_sources=['fallback']
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert 'trade_date' in result.columns
    
    def test_data_source_health_check(self):
        """测试数据源健康检查"""
        # 健康的数据源
        healthy_source = Mock()
        healthy_source.get_stock_list.return_value = pd.DataFrame({'code': ['000001']})
        
        # 不健康的数据源
        unhealthy_source = Mock()
        unhealthy_source.get_stock_list.side_effect = Exception("Connection failed")
        
        self.manager.add_data_source('healthy', healthy_source)
        self.manager.add_data_source('unhealthy', unhealthy_source)
        
        health_status = self.manager.check_data_source_health()
        
        assert isinstance(health_status, dict)
        assert health_status['healthy'] == True
        assert health_status['unhealthy'] == False
    
    def test_data_quality_validation(self):
        """测试数据质量验证"""
        # 高质量数据
        high_quality_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'close_price': np.random.uniform(10, 20, 10),
            'volume': np.random.uniform(1000000, 5000000, 10)
        })
        
        # 低质量数据（包含NaN）
        low_quality_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'close_price': [15.0, np.nan, 16.0, 17.0, np.nan, 18.0, 19.0, 20.0, 21.0, 22.0],
            'volume': [1000000, 1200000, np.nan, 1600000, 1800000, 2000000, 2200000, 2400000, 2600000, 2800000]
        })
        
        high_quality_score = self.manager.validate_data_quality(high_quality_data)
        low_quality_score = self.manager.validate_data_quality(low_quality_data)
        
        assert isinstance(high_quality_score, float)
        assert isinstance(low_quality_score, float)
        assert 0.0 <= high_quality_score <= 1.0
        assert 0.0 <= low_quality_score <= 1.0
        assert high_quality_score > low_quality_score


class TestDataCompatibility:
    """数据兼容性测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.akshare_source = AkShareDataSource()
        self.tushare_source = TushareDataSource()
    
    def test_data_format_standardization(self):
        """测试数据格式标准化"""
        # AkShare格式数据
        akshare_data = pd.DataFrame({
            '日期': ['2024-01-01', '2024-01-02'],
            '开盘': [15.0, 15.5],
            '最高': [15.8, 16.2],
            '最低': [14.8, 15.2],
            '收盘': [15.5, 16.0],
            '成交量': [1000000, 1200000]
        })
        
        # Tushare格式数据
        tushare_data = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'open': [15.0, 15.5],
            'high': [15.8, 16.2],
            'low': [14.8, 15.2],
            'close': [15.5, 16.0],
            'volume': [1000000, 1200000]
        })
        
        # 标准化后应该具有相同的列名
        standardized_akshare = self.akshare_source._standardize_data_format(akshare_data)
        standardized_tushare = self.tushare_source._standardize_data_format(tushare_data)
        
        assert 'trade_date' in standardized_akshare.columns
        assert 'open_price' in standardized_akshare.columns
        assert 'high_price' in standardized_akshare.columns
        assert 'low_price' in standardized_akshare.columns
        assert 'close_price' in standardized_akshare.columns
        assert 'volume' in standardized_akshare.columns
        
        assert 'trade_date' in standardized_tushare.columns
        assert 'open_price' in standardized_tushare.columns
        assert 'high_price' in standardized_tushare.columns
        assert 'low_price' in standardized_tushare.columns
        assert 'close_price' in standardized_tushare.columns
        assert 'volume' in standardized_tushare.columns
    
    def test_data_merging(self):
        """测试数据合并"""
        # 两个数据源的数据
        source1_data = pd.DataFrame({
            'trade_date': ['2024-01-01', '2024-01-02'],
            'close_price': [15.5, 16.0],
            'volume': [1000000, 1200000]
        })
        
        source2_data = pd.DataFrame({
            'trade_date': ['2024-01-01', '2024-01-02'],
            'close_price': [15.6, 16.1],
            'volume': [1100000, 1300000]
        })
        
        # 合并数据
        merged_data = pd.merge(
            source1_data, source2_data,
            on='trade_date',
            suffixes=('_source1', '_source2')
        )
        
        assert len(merged_data) == 2
        assert 'close_price_source1' in merged_data.columns
        assert 'close_price_source2' in merged_data.columns
        assert 'volume_source1' in merged_data.columns
        assert 'volume_source2' in merged_data.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 