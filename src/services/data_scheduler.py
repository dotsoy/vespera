"""
数据更新调度器
实现每日自动增量数据更新
"""
import asyncio
import schedule
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import threading
from pathlib import Path
import sys

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.utils.database import get_db_manager
from src.data_sources.akshare_data_source import AkShareDataSource
from src.data_sources.base_data_source import DataRequest, DataType

logger = get_logger("data_scheduler")


class DataUpdateScheduler:
    """数据更新调度器"""
    
    def __init__(self):
        self.is_running = False
        self.last_update_date = None
        self.update_thread = None
        self.db_manager = get_db_manager()
        self.data_source = AkShareDataSource()
        
    def start(self):
        """启动调度器"""
        if self.is_running:
            logger.warning("调度器已在运行中")
            return
            
        logger.info("启动数据更新调度器")
        self.is_running = True
        
        # 初始化数据源
        if not self.data_source.initialize():
            logger.error("数据源初始化失败，调度器启动失败")
            self.is_running = False
            return
            
        # 设置每日更新时间（交易日早上9:00）
        schedule.every().day.at("09:00").do(self._daily_update_job)
        
        # 启动调度线程
        self.update_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.update_thread.start()
        
        logger.info("数据更新调度器已启动，每日9:00自动更新")
        
    def stop(self):
        """停止调度器"""
        logger.info("停止数据更新调度器")
        self.is_running = False
        schedule.clear()
        
    def _run_scheduler(self):
        """运行调度器主循环"""
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
            except Exception as e:
                logger.error(f"调度器运行错误: {e}")
                time.sleep(60)
                
    def _daily_update_job(self):
        """每日更新任务"""
        try:
            # 检查是否为交易日
            if not self._is_trading_day():
                logger.info("今日非交易日，跳过数据更新")
                return
                
            # 检查今日是否已更新
            if self._is_updated_today():
                logger.info("今日数据已更新，跳过重复更新")
                return
                
            logger.info("开始执行每日数据更新")
            success = self._perform_incremental_update()
            
            if success:
                self._record_update_completion()
                logger.info("每日数据更新完成")
            else:
                logger.error("每日数据更新失败")
                
        except Exception as e:
            logger.error(f"每日更新任务执行失败: {e}")
            
    def _is_trading_day(self) -> bool:
        """检查是否为交易日（简单实现：周一到周五）"""
        today = datetime.now()
        return today.weekday() < 5  # 0-4 表示周一到周五
        
    def _is_updated_today(self) -> bool:
        """检查今日是否已更新"""
        try:
            today = datetime.now().date()
            
            # 检查更新记录表
            query = """
            SELECT update_date FROM data_update_log 
            WHERE update_date = %s AND status = 'completed'
            ORDER BY update_time DESC LIMIT 1
            """
            result = self.db_manager.execute_postgres_query(query, {'update_date': today})
            
            return not result.empty
            
        except Exception as e:
            logger.error(f"检查更新状态失败: {e}")
            return False
            
    def _perform_incremental_update(self) -> bool:
        """执行增量数据更新"""
        try:
            today = datetime.now().date()
            
            # 记录更新开始
            self._record_update_start()
            
            # 1. 更新股票基础信息（每日更新以保持最新）
            logger.info("更新股票基础信息...")
            if not self._update_stock_basic():
                return False
                
            # 2. 增量更新日线行情数据
            logger.info("增量更新日线行情数据...")
            if not self._update_daily_quotes_incremental():
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"增量更新执行失败: {e}")
            return False
            
    def _update_stock_basic(self) -> bool:
        """更新股票基础信息"""
        try:
            request = DataRequest(data_type=DataType.STOCK_BASIC)
            response = self.data_source.fetch_data(request)
            
            if not response.success or response.data.empty:
                logger.error("获取股票基础信息失败")
                return False
                
            stock_basic_df = response.data
            logger.info(f"获取到 {len(stock_basic_df)} 只股票基础信息")
            
            # 使用UPSERT方式更新数据
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
                    is_hs = EXCLUDED.is_hs,
                    updated_at = CURRENT_TIMESTAMP
                    """
                    self.db_manager.execute_postgres_command(insert_sql, {
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
                except Exception as e:
                    logger.warning(f"更新股票 {row['ts_code']} 失败: {e}")
                    
            logger.info(f"成功更新 {insert_count} 只股票基础信息")
            return True
            
        except Exception as e:
            logger.error(f"更新股票基础信息失败: {e}")
            return False
            
    def _update_daily_quotes_incremental(self) -> bool:
        """增量更新日线行情数据"""
        try:
            # 获取需要更新的日期（最近3个交易日）
            update_dates = self._get_update_dates()
            if not update_dates:
                logger.warning("无需要更新的交易日")
                return True
                
            # 获取活跃股票列表（限制数量以控制更新时间）
            stock_list = self._get_active_stocks(limit=100)
            if not stock_list:
                logger.error("获取股票列表失败")
                return False
                
            total_records = 0
            for trade_date in update_dates:
                logger.info(f"更新 {trade_date} 的行情数据...")
                
                date_obj = datetime.strptime(trade_date, '%Y%m%d')
                records = self._update_quotes_for_date(stock_list, date_obj)
                total_records += records
                
                # 控制更新频率，避免API限制
                time.sleep(1)
                
            logger.info(f"增量更新完成，总计 {total_records} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"增量更新日线行情失败: {e}")
            return False
            
    def _get_update_dates(self) -> list:
        """获取需要更新的交易日期"""
        dates = []
        current_date = datetime.now()
        
        # 获取最近3个工作日
        for i in range(3):
            check_date = current_date - timedelta(days=i)
            if check_date.weekday() < 5:  # 周一到周五
                dates.append(check_date.strftime('%Y%m%d'))
                
        return dates
        
    def _get_active_stocks(self, limit: int = 100) -> list:
        """获取活跃股票列表"""
        try:
            query = f"""
            SELECT ts_code FROM stock_basic
            WHERE market IN ('深交所主板', '上交所主板', '创业板', '科创板')
            ORDER BY ts_code LIMIT {limit}
            """
            result = self.db_manager.execute_postgres_query(query)
            return result['ts_code'].tolist() if not result.empty else []

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
            
    def _update_quotes_for_date(self, stock_list: list, date_obj: datetime) -> int:
        """更新指定日期的行情数据"""
        records = 0
        
        for stock_code in stock_list[:10]:  # 限制数量
            try:
                request = DataRequest(
                    data_type=DataType.DAILY_QUOTES,
                    symbol=stock_code,
                    start_date=date_obj,
                    end_date=date_obj
                )
                response = self.data_source.fetch_data(request)
                
                if response.success and not response.data.empty:
                    for _, row in response.data.iterrows():
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
                            amount = EXCLUDED.amount,
                            updated_at = CURRENT_TIMESTAMP
                            """
                            self.db_manager.execute_postgres_command(insert_sql, {
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
                            records += 1
                        except Exception as e:
                            logger.warning(f"插入 {stock_code} {date_obj.strftime('%Y%m%d')} 行情失败: {e}")
                            
                time.sleep(0.1)  # 控制频率
                
            except Exception as e:
                logger.warning(f"获取 {stock_code} 行情失败: {e}")
                
        return records
        
    def _record_update_start(self):
        """记录更新开始"""
        try:
            insert_sql = """
            INSERT INTO data_update_log (update_date, update_time, status, message)
            VALUES (:update_date, :update_time, :status, :message)
            """
            self.db_manager.execute_postgres_command(insert_sql, {
                'update_date': datetime.now().date(),
                'update_time': datetime.now(),
                'status': 'started',
                'message': '开始每日数据更新'
            })
        except Exception as e:
            logger.error(f"记录更新开始失败: {e}")
            
    def _record_update_completion(self):
        """记录更新完成"""
        try:
            insert_sql = """
            INSERT INTO data_update_log (update_date, update_time, status, message)
            VALUES (:update_date, :update_time, :status, :message)
            """
            self.db_manager.execute_postgres_command(insert_sql, {
                'update_date': datetime.now().date(),
                'update_time': datetime.now(),
                'status': 'completed',
                'message': '每日数据更新完成'
            })
        except Exception as e:
            logger.error(f"记录更新完成失败: {e}")
            
    def get_last_update_info(self) -> Optional[Dict[str, Any]]:
        """获取最后更新信息"""
        try:
            query = """
            SELECT update_date, update_time, status, message
            FROM data_update_log 
            ORDER BY update_time DESC LIMIT 1
            """
            result = self.db_manager.execute_postgres_query(query)
            
            if not result.empty:
                row = result.iloc[0]
                return {
                    'update_date': row['update_date'],
                    'update_time': row['update_time'],
                    'status': row['status'],
                    'message': row['message']
                }
            return None
            
        except Exception as e:
            logger.error(f"获取最后更新信息失败: {e}")
            return None


# 全局调度器实例
scheduler = DataUpdateScheduler()


def get_scheduler() -> DataUpdateScheduler:
    """获取调度器实例"""
    return scheduler


if __name__ == "__main__":
    # 测试调度器
    scheduler = get_scheduler()
    scheduler.start()
    
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.stop()
        logger.info("调度器已停止")
