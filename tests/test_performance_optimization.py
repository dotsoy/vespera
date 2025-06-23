"""
性能优化测试
测试大数据量处理、内存使用和计算效率
"""
import pytest
import pandas as pd
import numpy as np
import time
import psutil
import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.analyzers.technical_analyzer import TechnicalAnalyzer
from src.analyzers.capital_flow_analyzer import CapitalFlowAnalyzer
from src.strategies.qiming_star.qiming_star_strategy import QimingStarStrategy
from src.utils.technical_indicators import add_all_indicators


class TestDataProcessingPerformance:
    """数据处理性能测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.technical_analyzer = TechnicalAnalyzer()
        self.capital_flow_analyzer = CapitalFlowAnalyzer()
    
    def test_large_dataset_processing(self):
        """测试大数据集处理性能"""
        # 创建大规模测试数据 (10万条记录)
        large_data = pd.DataFrame({
            'trade_date': pd.date_range('2020-01-01', periods=100000, freq='D'),
            'open_price': np.random.uniform(10, 20, 100000),
            'high_price': np.random.uniform(15, 25, 100000),
            'low_price': np.random.uniform(5, 15, 100000),
            'close_price': np.random.uniform(10, 20, 100000),
            'volume': np.random.uniform(1000000, 5000000, 100000),
        })
        
        # 记录开始时间和内存
        start_time = time.time()
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # 执行技术指标计算
        result = self.technical_analyzer.calculate_indicators(large_data)
        
        # 记录结束时间和内存
        end_time = time.time()
        final_memory = process.memory_info().rss
        
        execution_time = end_time - start_time
        memory_usage = (final_memory - initial_memory) / 1024 / 1024  # MB
        
        # 性能验证
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 100000
        assert execution_time < 60  # 10万条记录应该在60秒内完成
        assert memory_usage < 500   # 内存使用应该小于500MB
        
        print(f"大数据集处理性能:")
        print(f"  记录数: {len(result):,}")
        print(f"  执行时间: {execution_time:.2f}秒")
        print(f"  内存使用: {memory_usage:.2f}MB")
        print(f"  处理速度: {len(result)/execution_time:.0f}条/秒")
    
    def test_batch_stock_analysis_performance(self):
        """测试批量股票分析性能"""
        # 创建100只股票的数据
        stock_data_dict = {}
        
        for i in range(100):
            stock_code = f"00000{i:03d}.SZ"
            stock_data_dict[stock_code] = pd.DataFrame({
                'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
                'open_price': np.random.uniform(10, 20, 100),
                'high_price': np.random.uniform(15, 25, 100),
                'low_price': np.random.uniform(5, 15, 100),
                'close_price': np.random.uniform(10, 20, 100),
                'volume': np.random.uniform(1000000, 5000000, 100),
            })
        
        # 记录开始时间
        start_time = time.time()
        
        # 批量分析
        results = {}
        for stock_code, data in stock_data_dict.items():
            try:
                result = self.technical_analyzer.calculate_indicators(data)
                results[stock_code] = result
            except Exception as e:
                pytest.fail(f"处理股票 {stock_code} 时失败: {e}")
        
        # 记录结束时间
        end_time = time.time()
        execution_time = end_time - start_time
        
        # 性能验证
        assert len(results) == 100
        assert execution_time < 30  # 100只股票应该在30秒内完成
        
        print(f"批量股票分析性能:")
        print(f"  股票数量: {len(results)}")
        print(f"  执行时间: {execution_time:.2f}秒")
        print(f"  平均每只股票: {execution_time/len(results):.3f}秒")
    
    def test_memory_efficiency_analysis(self):
        """测试内存效率分析"""
        process = psutil.Process(os.getpid())
        
        # 记录初始内存
        initial_memory = process.memory_info().rss
        
        # 创建中等规模数据
        medium_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=1000, freq='D'),
            'open_price': np.random.uniform(10, 20, 1000),
            'high_price': np.random.uniform(15, 25, 1000),
            'low_price': np.random.uniform(5, 15, 1000),
            'close_price': np.random.uniform(10, 20, 1000),
            'volume': np.random.uniform(1000000, 5000000, 1000),
        })
        
        memory_usage_list = []
        
        # 执行多次计算，监控内存使用
        for i in range(10):
            # 记录计算前内存
            before_memory = process.memory_info().rss
            
            # 执行计算
            result = self.technical_analyzer.calculate_indicators(medium_data)
            
            # 记录计算后内存
            after_memory = process.memory_info().rss
            
            # 计算内存增长
            memory_increase = (after_memory - before_memory) / 1024 / 1024  # MB
            memory_usage_list.append(memory_increase)
            
            # 显式删除结果以释放内存
            del result
        
        # 记录最终内存
        final_memory = process.memory_info().rss
        total_memory_increase = (final_memory - initial_memory) / 1024 / 1024  # MB
        
        # 内存效率验证
        avg_memory_increase = np.mean(memory_usage_list)
        max_memory_increase = np.max(memory_usage_list)
        
        assert avg_memory_increase < 50   # 平均内存增长应该小于50MB
        assert max_memory_increase < 100  # 最大内存增长应该小于100MB
        assert total_memory_increase < 200  # 总内存增长应该小于200MB
        
        print(f"内存效率分析:")
        print(f"  平均内存增长: {avg_memory_increase:.2f}MB")
        print(f"  最大内存增长: {max_memory_increase:.2f}MB")
        print(f"  总内存增长: {total_memory_increase:.2f}MB")
        print(f"  内存使用列表: {[f'{x:.2f}MB' for x in memory_usage_list]}")


class TestComputationalEfficiency:
    """计算效率测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.technical_analyzer = TechnicalAnalyzer()
    
    def test_technical_indicator_calculation_speed(self):
        """测试技术指标计算速度"""
        # 创建标准测试数据
        test_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=1000, freq='D'),
            'open_price': np.random.uniform(10, 20, 1000),
            'high_price': np.random.uniform(15, 25, 1000),
            'low_price': np.random.uniform(5, 15, 1000),
            'close_price': np.random.uniform(10, 20, 1000),
            'volume': np.random.uniform(1000000, 5000000, 1000),
        })
        
        # 测试不同数据大小的计算速度
        data_sizes = [100, 500, 1000, 5000]
        execution_times = []
        
        for size in data_sizes:
            subset_data = test_data.head(size)
            
            start_time = time.time()
            result = self.technical_analyzer.calculate_indicators(subset_data)
            end_time = time.time()
            
            execution_time = end_time - start_time
            execution_times.append(execution_time)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == size
        
        # 验证计算速度的线性增长
        print(f"技术指标计算速度测试:")
        for i, size in enumerate(data_sizes):
            print(f"  {size:4d}条记录: {execution_times[i]:.3f}秒")
        
        # 验证计算时间与数据量基本成正比
        time_per_record = [t/s for t, s in zip(execution_times, data_sizes)]
        avg_time_per_record = np.mean(time_per_record)
        time_variance = np.var(time_per_record)
        
        assert time_variance < 1e-6  # 每记录计算时间应该相对稳定
        print(f"  平均每记录计算时间: {avg_time_per_record:.6f}秒")
    
    def test_trend_score_calculation_efficiency(self):
        """测试趋势评分计算效率"""
        # 创建包含技术指标的测试数据
        test_data = pd.DataFrame({
            'close_price': np.random.uniform(10, 20, 1000),
            'ema_12': np.random.uniform(9, 19, 1000),
            'ema_26': np.random.uniform(8, 18, 1000),
            'ma_5': np.random.uniform(9, 19, 1000),
            'ma_10': np.random.uniform(8, 18, 1000),
            'ma_20': np.random.uniform(7, 17, 1000),
            'bb_upper': np.random.uniform(20, 30, 1000),
            'bb_middle': np.random.uniform(10, 20, 1000),
            'macd': np.random.uniform(-1, 1, 1000),
            'macd_signal': np.random.uniform(-1, 1, 1000),
        })
        
        # 测试趋势评分计算速度
        start_time = time.time()
        
        for _ in range(100):  # 执行100次计算
            score = self.technical_analyzer.calculate_trend_score(test_data)
            assert isinstance(score, float)
            assert 0.0 <= score <= 1.0
        
        end_time = time.time()
        total_time = end_time - start_time
        avg_time_per_calculation = total_time / 100
        
        assert avg_time_per_calculation < 0.01  # 每次计算应该小于10毫秒
        
        print(f"趋势评分计算效率:")
        print(f"  100次计算总时间: {total_time:.3f}秒")
        print(f"  平均每次计算: {avg_time_per_calculation:.6f}秒")
    
    def test_momentum_score_calculation_efficiency(self):
        """测试动量评分计算效率"""
        # 创建包含动量指标的测试数据
        test_data = pd.DataFrame({
            'close_price': np.random.uniform(10, 20, 1000),
            'rsi': np.random.uniform(0, 100, 1000),
            'k': np.random.uniform(0, 100, 1000),
            'd': np.random.uniform(0, 100, 1000),
            'j': np.random.uniform(0, 100, 1000),
            'williams_r': np.random.uniform(-100, 0, 1000),
        })
        
        # 测试动量评分计算速度
        start_time = time.time()
        
        for _ in range(100):  # 执行100次计算
            score = self.technical_analyzer.calculate_momentum_score(test_data)
            assert isinstance(score, float)
            assert 0.0 <= score <= 1.0
        
        end_time = time.time()
        total_time = end_time - start_time
        avg_time_per_calculation = total_time / 100
        
        assert avg_time_per_calculation < 0.01  # 每次计算应该小于10毫秒
        
        print(f"动量评分计算效率:")
        print(f"  100次计算总时间: {total_time:.3f}秒")
        print(f"  平均每次计算: {avg_time_per_calculation:.6f}秒")


class TestStrategyPerformance:
    """策略性能测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.strategy = QimingStarStrategy()
    
    def test_strategy_analysis_performance(self):
        """测试策略分析性能"""
        # 创建测试股票数据
        stock_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'open': np.random.uniform(10, 20, 100),
            'high': np.random.uniform(15, 25, 100),
            'low': np.random.uniform(5, 15, 100),
            'close': np.random.uniform(10, 20, 100),
            'volume': np.random.uniform(1000000, 5000000, 100),
        })
        
        # 创建市场数据
        market_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'close': np.random.uniform(3000, 3500, 100),
            'volume': np.random.uniform(1e10, 2e10, 100),
        })
        
        # 测试策略分析性能
        start_time = time.time()
        
        result = self.strategy.analyze_stock("000001.SZ", stock_data, market_data)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # 性能验证
        assert result is not None
        assert isinstance(result, dict)
        assert execution_time < 5  # 单只股票分析应该在5秒内完成
        
        print(f"策略分析性能:")
        print(f"  执行时间: {execution_time:.3f}秒")
        print(f"  结果类型: {type(result)}")
        print(f"  结果键: {list(result.keys())}")
    
    def test_batch_strategy_analysis_performance(self):
        """测试批量策略分析性能"""
        # 创建多只股票数据
        stock_data_dict = {}
        
        for i in range(50):  # 50只股票
            stock_code = f"00000{i:03d}.SZ"
            stock_data_dict[stock_code] = pd.DataFrame({
                'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
                'open': np.random.uniform(10, 20, 100),
                'high': np.random.uniform(15, 25, 100),
                'low': np.random.uniform(5, 15, 100),
                'close': np.random.uniform(10, 20, 100),
                'volume': np.random.uniform(1000000, 5000000, 100),
            })
        
        # 创建市场数据
        market_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'close': np.random.uniform(3000, 3500, 100),
            'volume': np.random.uniform(1e10, 2e10, 100),
        })
        
        # 测试批量分析性能
        start_time = time.time()
        
        results = self.strategy.batch_analyze(stock_data_dict, market_data)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # 性能验证
        assert isinstance(results, dict)
        assert 's_class' in results
        assert 'a_class' in results
        assert execution_time < 60  # 50只股票批量分析应该在60秒内完成
        
        print(f"批量策略分析性能:")
        print(f"  股票数量: {len(stock_data_dict)}")
        print(f"  执行时间: {execution_time:.3f}秒")
        print(f"  平均每只股票: {execution_time/len(stock_data_dict):.3f}秒")
        print(f"  S级信号: {len(results['s_class'])}")
        print(f"  A级信号: {len(results['a_class'])}")


class TestMemoryOptimization:
    """内存优化测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.technical_analyzer = TechnicalAnalyzer()
    
    def test_memory_cleanup_after_analysis(self):
        """测试分析后内存清理"""
        import gc
        
        process = psutil.Process(os.getpid())
        
        # 记录初始内存
        initial_memory = process.memory_info().rss
        
        # 创建大量数据进行分析
        large_data = pd.DataFrame({
            'trade_date': pd.date_range('2024-01-01', periods=10000, freq='D'),
            'open_price': np.random.uniform(10, 20, 10000),
            'high_price': np.random.uniform(15, 25, 10000),
            'low_price': np.random.uniform(5, 15, 10000),
            'close_price': np.random.uniform(10, 20, 10000),
            'volume': np.random.uniform(1000000, 5000000, 10000),
        })
        
        # 执行分析
        result = self.technical_analyzer.calculate_indicators(large_data)
        
        # 记录分析后内存
        after_analysis_memory = process.memory_info().rss
        
        # 删除结果并强制垃圾回收
        del result
        del large_data
        gc.collect()
        
        # 记录清理后内存
        after_cleanup_memory = process.memory_info().rss
        
        # 计算内存变化
        analysis_memory_increase = (after_analysis_memory - initial_memory) / 1024 / 1024
        cleanup_memory_reduction = (after_analysis_memory - after_cleanup_memory) / 1024 / 1024
        
        print(f"内存清理测试:")
        print(f"  分析时内存增长: {analysis_memory_increase:.2f}MB")
        print(f"  清理后内存减少: {cleanup_memory_reduction:.2f}MB")
        print(f"  清理效率: {cleanup_memory_reduction/analysis_memory_increase*100:.1f}%")
        
        # 验证内存清理效果
        assert cleanup_memory_reduction > 0  # 应该有内存减少
        assert cleanup_memory_reduction / analysis_memory_increase > 0.5  # 清理效率应该大于50%
    
    def test_dataframe_memory_usage_optimization(self):
        """测试DataFrame内存使用优化"""
        # 创建不同数据类型的DataFrame
        data_types = {
            'float64': np.random.uniform(10, 20, 1000),
            'float32': np.random.uniform(10, 20, 1000).astype(np.float32),
            'int64': np.random.randint(1000000, 5000000, 1000),
            'int32': np.random.randint(1000000, 5000000, 1000).astype(np.int32),
        }
        
        memory_usage = {}
        
        for dtype, data in data_types.items():
            df = pd.DataFrame({
                'close_price': data,
                'volume': data,
            })
            
            memory_usage[dtype] = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
        
        print(f"DataFrame内存使用对比:")
        for dtype, memory in memory_usage.items():
            print(f"  {dtype}: {memory:.2f}MB")
        
        # 验证内存优化效果
        assert memory_usage['float32'] < memory_usage['float64']
        assert memory_usage['int32'] < memory_usage['int64']


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 