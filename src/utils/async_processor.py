"""
异步处理器
实现高效的并发数据处理和任务调度
"""
import asyncio
import time
from typing import Any, Dict, List, Optional, Callable, Union, Tuple, Coroutine
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import queue
import multiprocessing as mp
from functools import wraps
import pandas as pd

try:
    from src.utils.logger import get_logger
except ImportError:
    import logging
    def get_logger(name):
        return logging.getLogger(name)

logger = get_logger("async_processor")


class TaskStatus(Enum):
    """任务状态"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ProcessingMode(Enum):
    """处理模式"""
    SEQUENTIAL = "SEQUENTIAL"      # 顺序处理
    CONCURRENT = "CONCURRENT"      # 并发处理
    PARALLEL = "PARALLEL"          # 并行处理
    BATCH = "BATCH"               # 批处理


@dataclass
class TaskResult:
    """任务结果"""
    task_id: str
    status: TaskStatus
    result: Any = None
    error: Optional[Exception] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    execution_time: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[float]:
        """计算执行时长"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return self.execution_time


@dataclass
class ProcessingConfig:
    """处理配置"""
    max_workers: int = 4
    timeout: Optional[float] = None
    chunk_size: int = 100
    retry_attempts: int = 3
    retry_delay: float = 1.0
    enable_progress_tracking: bool = True
    memory_limit_mb: Optional[int] = None


class AsyncTaskManager:
    """异步任务管理器"""
    
    def __init__(self, config: ProcessingConfig = None):
        self.config = config or ProcessingConfig()
        self.tasks: Dict[str, TaskResult] = {}
        self.running_tasks: Dict[str, asyncio.Task] = {}
        self.task_counter = 0
        self.lock = asyncio.Lock()
        
        # 线程池和进程池
        self.thread_pool = ThreadPoolExecutor(max_workers=self.config.max_workers)
        self.process_pool = ProcessPoolExecutor(max_workers=min(self.config.max_workers, mp.cpu_count()))
        
        # 统计信息
        self.stats = {
            "total_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
            "cancelled_tasks": 0,
            "total_execution_time": 0.0
        }
    
    async def submit_task(self, 
                         func: Callable, 
                         *args, 
                         task_id: str = None,
                         mode: ProcessingMode = ProcessingMode.CONCURRENT,
                         **kwargs) -> str:
        """提交任务"""
        if task_id is None:
            self.task_counter += 1
            task_id = f"task_{self.task_counter}_{int(time.time())}"
        
        async with self.lock:
            # 创建任务结果对象
            task_result = TaskResult(
                task_id=task_id,
                status=TaskStatus.PENDING,
                start_time=datetime.now()
            )
            self.tasks[task_id] = task_result
            self.stats["total_tasks"] += 1
        
        # 根据模式执行任务
        if mode == ProcessingMode.CONCURRENT:
            task = asyncio.create_task(self._execute_async_task(task_id, func, *args, **kwargs))
        elif mode == ProcessingMode.PARALLEL:
            task = asyncio.create_task(self._execute_parallel_task(task_id, func, *args, **kwargs))
        else:
            task = asyncio.create_task(self._execute_sequential_task(task_id, func, *args, **kwargs))
        
        self.running_tasks[task_id] = task
        
        logger.info(f"任务已提交: {task_id} (模式: {mode.value})")
        return task_id
    
    async def _execute_async_task(self, task_id: str, func: Callable, *args, **kwargs) -> Any:
        """执行异步任务"""
        try:
            async with self.lock:
                self.tasks[task_id].status = TaskStatus.RUNNING
            
            start_time = time.time()
            
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                # 在线程池中执行同步函数
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(self.thread_pool, func, *args, **kwargs)
            
            execution_time = time.time() - start_time
            
            async with self.lock:
                self.tasks[task_id].status = TaskStatus.COMPLETED
                self.tasks[task_id].result = result
                self.tasks[task_id].end_time = datetime.now()
                self.tasks[task_id].execution_time = execution_time
                self.stats["completed_tasks"] += 1
                self.stats["total_execution_time"] += execution_time
            
            logger.debug(f"任务完成: {task_id} ({execution_time:.3f}s)")
            return result
            
        except Exception as e:
            async with self.lock:
                self.tasks[task_id].status = TaskStatus.FAILED
                self.tasks[task_id].error = e
                self.tasks[task_id].end_time = datetime.now()
                self.stats["failed_tasks"] += 1
            
            logger.error(f"任务失败: {task_id} - {e}")
            raise e
        finally:
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
    
    async def _execute_parallel_task(self, task_id: str, func: Callable, *args, **kwargs) -> Any:
        """在进程池中执行任务"""
        try:
            async with self.lock:
                self.tasks[task_id].status = TaskStatus.RUNNING
            
            start_time = time.time()
            
            # 在进程池中执行
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(self.process_pool, func, *args, **kwargs)
            
            execution_time = time.time() - start_time
            
            async with self.lock:
                self.tasks[task_id].status = TaskStatus.COMPLETED
                self.tasks[task_id].result = result
                self.tasks[task_id].end_time = datetime.now()
                self.tasks[task_id].execution_time = execution_time
                self.stats["completed_tasks"] += 1
                self.stats["total_execution_time"] += execution_time
            
            logger.debug(f"并行任务完成: {task_id} ({execution_time:.3f}s)")
            return result
            
        except Exception as e:
            async with self.lock:
                self.tasks[task_id].status = TaskStatus.FAILED
                self.tasks[task_id].error = e
                self.tasks[task_id].end_time = datetime.now()
                self.stats["failed_tasks"] += 1
            
            logger.error(f"并行任务失败: {task_id} - {e}")
            raise e
        finally:
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
    
    async def _execute_sequential_task(self, task_id: str, func: Callable, *args, **kwargs) -> Any:
        """顺序执行任务"""
        try:
            async with self.lock:
                self.tasks[task_id].status = TaskStatus.RUNNING
            
            start_time = time.time()
            
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            execution_time = time.time() - start_time
            
            async with self.lock:
                self.tasks[task_id].status = TaskStatus.COMPLETED
                self.tasks[task_id].result = result
                self.tasks[task_id].end_time = datetime.now()
                self.tasks[task_id].execution_time = execution_time
                self.stats["completed_tasks"] += 1
                self.stats["total_execution_time"] += execution_time
            
            logger.debug(f"顺序任务完成: {task_id} ({execution_time:.3f}s)")
            return result
            
        except Exception as e:
            async with self.lock:
                self.tasks[task_id].status = TaskStatus.FAILED
                self.tasks[task_id].error = e
                self.tasks[task_id].end_time = datetime.now()
                self.stats["failed_tasks"] += 1
            
            logger.error(f"顺序任务失败: {task_id} - {e}")
            raise e
        finally:
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
    
    async def get_task_result(self, task_id: str, wait: bool = True) -> Optional[TaskResult]:
        """获取任务结果"""
        if task_id not in self.tasks:
            return None
        
        if wait and task_id in self.running_tasks:
            try:
                await self.running_tasks[task_id]
            except Exception:
                pass  # 错误已经记录在task_result中
        
        return self.tasks[task_id]
    
    async def wait_for_tasks(self, task_ids: List[str], timeout: Optional[float] = None) -> List[TaskResult]:
        """等待多个任务完成"""
        tasks_to_wait = []
        for task_id in task_ids:
            if task_id in self.running_tasks:
                tasks_to_wait.append(self.running_tasks[task_id])
        
        if tasks_to_wait:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks_to_wait, return_exceptions=True),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logger.warning(f"等待任务超时: {timeout}s")
        
        return [self.tasks[task_id] for task_id in task_ids if task_id in self.tasks]
    
    async def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        if task_id in self.running_tasks:
            task = self.running_tasks[task_id]
            task.cancel()
            
            async with self.lock:
                if task_id in self.tasks:
                    self.tasks[task_id].status = TaskStatus.CANCELLED
                    self.tasks[task_id].end_time = datetime.now()
                    self.stats["cancelled_tasks"] += 1
            
            logger.info(f"任务已取消: {task_id}")
            return True
        
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        running_count = len(self.running_tasks)
        avg_execution_time = (
            self.stats["total_execution_time"] / self.stats["completed_tasks"]
            if self.stats["completed_tasks"] > 0 else 0
        )
        
        return {
            "total_tasks": self.stats["total_tasks"],
            "running_tasks": running_count,
            "completed_tasks": self.stats["completed_tasks"],
            "failed_tasks": self.stats["failed_tasks"],
            "cancelled_tasks": self.stats["cancelled_tasks"],
            "success_rate": (
                self.stats["completed_tasks"] / self.stats["total_tasks"]
                if self.stats["total_tasks"] > 0 else 0
            ),
            "average_execution_time": avg_execution_time,
            "total_execution_time": self.stats["total_execution_time"]
        }
    
    async def cleanup(self):
        """清理资源"""
        # 取消所有运行中的任务
        for task_id in list(self.running_tasks.keys()):
            await self.cancel_task(task_id)
        
        # 关闭线程池和进程池
        self.thread_pool.shutdown(wait=True)
        self.process_pool.shutdown(wait=True)
        
        logger.info("异步任务管理器已清理")


class BatchProcessor:
    """批处理器"""
    
    def __init__(self, task_manager: AsyncTaskManager = None):
        self.task_manager = task_manager or AsyncTaskManager()
    
    async def process_batch(self, 
                           items: List[Any], 
                           processor_func: Callable,
                           batch_size: int = 100,
                           mode: ProcessingMode = ProcessingMode.CONCURRENT,
                           progress_callback: Optional[Callable] = None) -> List[TaskResult]:
        """批量处理数据"""
        
        # 分批处理
        batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
        task_ids = []
        
        logger.info(f"开始批量处理: {len(items)} 项，分为 {len(batches)} 批")
        
        # 提交所有批次任务
        for i, batch in enumerate(batches):
            task_id = await self.task_manager.submit_task(
                processor_func,
                batch,
                task_id=f"batch_{i}",
                mode=mode
            )
            task_ids.append(task_id)
        
        # 等待所有任务完成
        results = []
        completed_batches = 0
        
        for task_id in task_ids:
            result = await self.task_manager.get_task_result(task_id, wait=True)
            results.append(result)
            completed_batches += 1
            
            # 进度回调
            if progress_callback:
                progress = completed_batches / len(batches)
                progress_callback(progress, completed_batches, len(batches))
        
        logger.info(f"批量处理完成: {completed_batches}/{len(batches)} 批")
        return results
    
    async def process_dataframe_chunks(self,
                                     df: pd.DataFrame,
                                     processor_func: Callable,
                                     chunk_size: int = 1000,
                                     mode: ProcessingMode = ProcessingMode.CONCURRENT) -> pd.DataFrame:
        """分块处理DataFrame"""
        
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
        logger.info(f"DataFrame分块处理: {len(df)} 行，分为 {len(chunks)} 块")
        
        # 处理所有块
        results = await self.process_batch(
            chunks,
            processor_func,
            batch_size=1,  # 每个chunk作为一个任务
            mode=mode
        )
        
        # 合并结果
        processed_chunks = []
        for result in results:
            if result.status == TaskStatus.COMPLETED and result.result is not None:
                processed_chunks.append(result.result)
            else:
                logger.warning(f"块处理失败: {result.error}")
        
        if processed_chunks:
            return pd.concat(processed_chunks, ignore_index=True)
        else:
            return pd.DataFrame()


class ConcurrentDataProcessor:
    """并发数据处理器"""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.task_manager = AsyncTaskManager(
            ProcessingConfig(max_workers=max_workers)
        )
        self.batch_processor = BatchProcessor(self.task_manager)
    
    async def process_stock_list(self, 
                               stock_codes: List[str],
                               processor_func: Callable,
                               batch_size: int = 50) -> Dict[str, Any]:
        """并发处理股票列表"""
        
        start_time = time.time()
        
        # 分批处理股票
        results = await self.batch_processor.process_batch(
            stock_codes,
            processor_func,
            batch_size=batch_size,
            mode=ProcessingMode.CONCURRENT
        )
        
        # 统计结果
        successful_results = []
        failed_results = []
        
        for result in results:
            if result.status == TaskStatus.COMPLETED:
                successful_results.extend(result.result if isinstance(result.result, list) else [result.result])
            else:
                failed_results.append(result)
        
        processing_time = time.time() - start_time
        
        return {
            "successful_count": len(successful_results),
            "failed_count": len(failed_results),
            "total_processing_time": processing_time,
            "results": successful_results,
            "errors": [r.error for r in failed_results if r.error]
        }
    
    async def parallel_data_analysis(self,
                                   data_chunks: List[pd.DataFrame],
                                   analysis_func: Callable) -> List[Any]:
        """并行数据分析"""
        
        # 使用进程池进行CPU密集型分析
        results = await self.batch_processor.process_batch(
            data_chunks,
            analysis_func,
            batch_size=1,
            mode=ProcessingMode.PARALLEL
        )
        
        return [r.result for r in results if r.status == TaskStatus.COMPLETED]
    
    async def concurrent_api_calls(self,
                                 api_requests: List[Dict[str, Any]],
                                 api_func: Callable,
                                 rate_limit: Optional[float] = None) -> List[Any]:
        """并发API调用"""
        
        if rate_limit:
            # 添加速率限制
            semaphore = asyncio.Semaphore(int(1 / rate_limit))
            
            async def rate_limited_api_func(request):
                async with semaphore:
                    result = await api_func(request)
                    if rate_limit:
                        await asyncio.sleep(rate_limit)
                    return result
            
            processor_func = rate_limited_api_func
        else:
            processor_func = api_func
        
        results = await self.batch_processor.process_batch(
            api_requests,
            processor_func,
            batch_size=10,
            mode=ProcessingMode.CONCURRENT
        )
        
        return [r.result for r in results if r.status == TaskStatus.COMPLETED]
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计"""
        return self.task_manager.get_stats()
    
    async def cleanup(self):
        """清理资源"""
        await self.task_manager.cleanup()


# 装饰器
def async_cached(cache_manager, ttl_seconds: Optional[int] = None):
    """异步缓存装饰器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # 生成缓存键
            cache_key = f"{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
            
            # 尝试从缓存获取
            cached_result = cache_manager.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # 执行函数
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # 缓存结果
            cache_manager.set(cache_key, result, ttl_seconds)
            
            return result
        
        return wrapper
    return decorator


def concurrent_task(task_manager: AsyncTaskManager, mode: ProcessingMode = ProcessingMode.CONCURRENT):
    """并发任务装饰器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            task_id = await task_manager.submit_task(func, *args, mode=mode, **kwargs)
            result = await task_manager.get_task_result(task_id, wait=True)
            
            if result.status == TaskStatus.COMPLETED:
                return result.result
            else:
                raise result.error or Exception(f"任务失败: {task_id}")
        
        return wrapper
    return decorator


# 全局处理器实例
global_processor = ConcurrentDataProcessor()


# 便捷函数
async def process_concurrently(items: List[Any], 
                             processor_func: Callable,
                             max_workers: int = 4,
                             batch_size: int = 100) -> List[Any]:
    """并发处理便捷函数"""
    processor = ConcurrentDataProcessor(max_workers=max_workers)
    try:
        if isinstance(items[0], str):
            # 假设是股票代码列表
            result = await processor.process_stock_list(items, processor_func, batch_size)
            return result["results"]
        else:
            # 通用批处理
            results = await processor.batch_processor.process_batch(
                items, processor_func, batch_size
            )
            return [r.result for r in results if r.status == TaskStatus.COMPLETED]
    finally:
        await processor.cleanup()


async def process_dataframe_parallel(df: pd.DataFrame,
                                   processor_func: Callable,
                                   chunk_size: int = 1000,
                                   max_workers: int = 4) -> pd.DataFrame:
    """并行处理DataFrame便捷函数"""
    processor = ConcurrentDataProcessor(max_workers=max_workers)
    try:
        return await processor.batch_processor.process_dataframe_chunks(
            df, processor_func, chunk_size
        )
    finally:
        await processor.cleanup()