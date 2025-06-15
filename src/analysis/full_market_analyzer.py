"""
A股全市场分析器
支持5000+股票的批量分析和技术指标计算
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime, timedelta
from loguru import logger
import warnings
warnings.filterwarnings('ignore')


class FullMarketAnalyzer:
    """全市场分析器"""
    
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.analysis_results = {}
        
        # 技术指标参数
        self.technical_params = {
            'ma_periods': [5, 10, 20, 60],
            'rsi_period': 14,
            'macd_params': (12, 26, 9),
            'bollinger_period': 20,
            'volume_ma_period': 5
        }
        
        # 选股标准
        self.selection_criteria = {
            'momentum': {
                'min_volume_ratio': 1.5,    # 成交量放大1.5倍
                'min_price_change': 2.0,    # 最小涨幅2%
                'max_price_change': 9.5,    # 最大涨幅9.5%（避免涨停）
                'rsi_range': (30, 70)       # RSI在30-70之间
            },
            'value': {
                'max_pe': 20,               # PE小于20
                'max_pb': 3,                # PB小于3
                'min_roe': 8,               # ROE大于8%
                'max_debt_ratio': 60        # 负债率小于60%
            },
            'technical': {
                'ma_trend': 'up',           # 均线向上
                'macd_signal': 'bullish',   # MACD金叉
                'bollinger_position': 'middle'  # 布林带中轨附近
            }
        }
    
    def create_market_data(self, stock_codes: List[str], days: int = 30) -> pd.DataFrame:
        """创建全市场数据"""
        logger.info(f"📊 创建 {len(stock_codes)} 只股票的市场数据")
        
        all_data = []
        
        # 生成交易日期
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days*2)
        
        trading_dates = []
        current_date = start_date
        while current_date <= end_date and len(trading_dates) < days:
            if current_date.weekday() < 5:  # 排除周末
                trading_dates.append(current_date)
            current_date += timedelta(days=1)
        
        # 为每只股票生成数据
        for i, code in enumerate(stock_codes):
            if i % 500 == 0:
                logger.info(f"处理进度: {i}/{len(stock_codes)} ({i/len(stock_codes)*100:.1f}%)")
            
            stock_data = self._generate_stock_data(code, trading_dates)
            all_data.extend(stock_data)
        
        df = pd.DataFrame(all_data)
        logger.success(f"✅ 生成了 {len(df)} 条市场数据")
        
        return df
    
    def _generate_stock_data(self, code: str, trading_dates: List) -> List[Dict]:
        """为单只股票生成数据"""
        stock_data = []
        
        # 根据股票代码确定基础价格
        base_price = self._get_base_price(code)
        current_price = base_price
        
        for i, trade_date in enumerate(trading_dates):
            # 生成价格波动
            volatility = self._get_stock_volatility(code)
            change_pct = np.random.normal(0, volatility)
            
            # 限制涨跌幅
            if code.startswith(('688', '300')):  # 科创板和创业板
                change_pct = np.clip(change_pct, -0.18, 0.18)
            else:  # 主板
                change_pct = np.clip(change_pct, -0.09, 0.09)
            
            # 计算价格
            open_price = current_price * (1 + np.random.uniform(-0.01, 0.01))
            close_price = current_price * (1 + change_pct)
            high_price = max(open_price, close_price) * (1 + abs(np.random.normal(0, 0.01)))
            low_price = min(open_price, close_price) * (1 - abs(np.random.normal(0, 0.01)))
            
            # 生成成交量
            base_volume = self._get_base_volume(code)
            volume_multiplier = 1 + abs(change_pct) * 2  # 涨跌幅越大，成交量越大
            volume = int(base_volume * volume_multiplier * np.random.uniform(0.5, 2.0))
            
            # 计算成交额
            amount = volume * (high_price + low_price) / 2
            
            record = {
                'ts_code': code,
                'trade_date': trade_date.strftime('%Y-%m-%d'),
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'pre_close': round(current_price, 2),
                'change': round(close_price - current_price, 2),
                'pct_chg': round(change_pct * 100, 2),
                'vol': volume,
                'amount': round(amount, 2),
                'turnover_rate': round(volume / 100000000 * 100, 2)  # 简化换手率
            }
            
            stock_data.append(record)
            current_price = close_price
        
        return stock_data
    
    def _get_base_price(self, code: str) -> float:
        """获取股票基础价格"""
        if code.startswith('600519'):  # 贵州茅台
            return 1700 + np.random.uniform(-100, 100)
        elif code.startswith('000858'):  # 五粮液
            return 130 + np.random.uniform(-20, 20)
        elif code.startswith('300750'):  # 宁德时代
            return 200 + np.random.uniform(-30, 30)
        elif code.startswith('688'):  # 科创板
            return 50 + np.random.uniform(-20, 50)
        elif code.startswith('300'):  # 创业板
            return 25 + np.random.uniform(-10, 25)
        elif code.startswith(('000', '002')):  # 深交所
            return 15 + np.random.uniform(-5, 15)
        else:  # 上交所主板
            return 12 + np.random.uniform(-5, 10)
    
    def _get_stock_volatility(self, code: str) -> float:
        """获取股票波动率"""
        if code.startswith('688'):  # 科创板
            return 0.04
        elif code.startswith('300'):  # 创业板
            return 0.035
        elif code.startswith(('000', '002')):  # 深交所
            return 0.025
        else:  # 上交所主板
            return 0.02
    
    def _get_base_volume(self, code: str) -> int:
        """获取基础成交量"""
        if code.startswith('600519'):  # 贵州茅台
            return 2000000
        elif code.startswith(('000001', '600036', '000858')):  # 大盘股
            return 30000000
        elif code.startswith('688'):  # 科创板
            return 8000000
        elif code.startswith('300'):  # 创业板
            return 15000000
        else:
            return 10000000
    
    def calculate_technical_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算技术指标"""
        logger.info("📈 计算技术指标")
        
        results = []
        
        for code in data['ts_code'].unique():
            stock_data = data[data['ts_code'] == code].sort_values('trade_date').copy()
            
            if len(stock_data) < 20:  # 数据不足，跳过
                continue
            
            # 计算移动平均线
            for period in self.technical_params['ma_periods']:
                stock_data[f'ma{period}'] = stock_data['close'].rolling(period).mean()
            
            # 计算RSI
            stock_data['rsi'] = self._calculate_rsi(stock_data['close'])
            
            # 计算MACD
            macd_data = self._calculate_macd(stock_data['close'])
            stock_data['macd'] = macd_data['macd']
            stock_data['macd_signal'] = macd_data['signal']
            stock_data['macd_hist'] = macd_data['histogram']
            
            # 计算布林带
            bollinger_data = self._calculate_bollinger_bands(stock_data['close'])
            stock_data['bb_upper'] = bollinger_data['upper']
            stock_data['bb_middle'] = bollinger_data['middle']
            stock_data['bb_lower'] = bollinger_data['lower']
            
            # 计算成交量指标
            stock_data['vol_ma'] = stock_data['vol'].rolling(self.technical_params['volume_ma_period']).mean()
            stock_data['vol_ratio'] = stock_data['vol'] / stock_data['vol_ma']
            
            results.append(stock_data)
        
        if results:
            combined_data = pd.concat(results, ignore_index=True)
            logger.success(f"✅ 完成 {len(data['ts_code'].unique())} 只股票的技术指标计算")
            return combined_data
        else:
            logger.warning("⚠️ 没有足够的数据计算技术指标")
            return pd.DataFrame()
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """计算RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _calculate_macd(self, prices: pd.Series) -> Dict[str, pd.Series]:
        """计算MACD"""
        fast, slow, signal_period = self.technical_params['macd_params']
        
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        macd = ema_fast - ema_slow
        signal = macd.ewm(span=signal_period).mean()
        histogram = macd - signal
        
        return {
            'macd': macd,
            'signal': signal,
            'histogram': histogram
        }
    
    def _calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std_dev: int = 2) -> Dict[str, pd.Series]:
        """计算布林带"""
        middle = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        upper = middle + (std * std_dev)
        lower = middle - (std * std_dev)
        
        return {
            'upper': upper,
            'middle': middle,
            'lower': lower
        }
    
    def screen_stocks(self, data: pd.DataFrame, criteria_type: str = 'momentum') -> pd.DataFrame:
        """股票筛选"""
        logger.info(f"🔍 使用 {criteria_type} 策略筛选股票")
        
        if criteria_type not in self.selection_criteria:
            logger.error(f"❌ 未知的筛选策略: {criteria_type}")
            return pd.DataFrame()
        
        criteria = self.selection_criteria[criteria_type]
        
        # 获取最新数据
        latest_data = data.groupby('ts_code').last().reset_index()
        
        selected_stocks = []
        
        for _, stock in latest_data.iterrows():
            if self._meets_criteria(stock, criteria, criteria_type):
                selected_stocks.append(stock)
        
        if selected_stocks:
            result_df = pd.DataFrame(selected_stocks)
            logger.success(f"✅ 筛选出 {len(result_df)} 只符合 {criteria_type} 策略的股票")
            return result_df
        else:
            logger.warning(f"⚠️ 没有股票符合 {criteria_type} 策略")
            return pd.DataFrame()
    
    def _meets_criteria(self, stock: pd.Series, criteria: Dict, criteria_type: str) -> bool:
        """检查是否符合筛选条件"""
        try:
            if criteria_type == 'momentum':
                # 动量策略
                vol_ratio = stock.get('vol_ratio', 1)
                pct_chg = stock.get('pct_chg', 0)
                rsi = stock.get('rsi', 50)
                
                return (
                    vol_ratio >= criteria['min_volume_ratio'] and
                    criteria['min_price_change'] <= pct_chg <= criteria['max_price_change'] and
                    criteria['rsi_range'][0] <= rsi <= criteria['rsi_range'][1]
                )
            
            elif criteria_type == 'technical':
                # 技术策略
                ma5 = stock.get('ma5', 0)
                ma10 = stock.get('ma10', 0)
                close = stock.get('close', 0)
                macd = stock.get('macd', 0)
                macd_signal = stock.get('macd_signal', 0)
                
                return (
                    close > ma5 > ma10 and  # 均线向上
                    macd > macd_signal and  # MACD金叉
                    close > 0
                )
            
            return False
            
        except Exception:
            return False
    
    def generate_market_summary(self, data: pd.DataFrame) -> Dict:
        """生成市场总结"""
        logger.info("📊 生成市场总结")
        
        latest_data = data.groupby('ts_code').last().reset_index()
        
        summary = {
            'total_stocks': len(latest_data),
            'up_stocks': (latest_data['pct_chg'] > 0).sum(),
            'down_stocks': (latest_data['pct_chg'] < 0).sum(),
            'flat_stocks': (latest_data['pct_chg'] == 0).sum(),
            'avg_change': latest_data['pct_chg'].mean(),
            'median_change': latest_data['pct_chg'].median(),
            'max_gain': latest_data['pct_chg'].max(),
            'max_loss': latest_data['pct_chg'].min(),
            'total_volume': latest_data['vol'].sum(),
            'total_amount': latest_data['amount'].sum(),
            'avg_turnover': latest_data['turnover_rate'].mean()
        }
        
        # 涨跌分布
        summary['change_distribution'] = {
            'limit_up': (latest_data['pct_chg'] >= 9.8).sum(),
            'strong_up': ((latest_data['pct_chg'] >= 5) & (latest_data['pct_chg'] < 9.8)).sum(),
            'moderate_up': ((latest_data['pct_chg'] >= 2) & (latest_data['pct_chg'] < 5)).sum(),
            'slight_up': ((latest_data['pct_chg'] > 0) & (latest_data['pct_chg'] < 2)).sum(),
            'slight_down': ((latest_data['pct_chg'] < 0) & (latest_data['pct_chg'] > -2)).sum(),
            'moderate_down': ((latest_data['pct_chg'] <= -2) & (latest_data['pct_chg'] > -5)).sum(),
            'strong_down': ((latest_data['pct_chg'] <= -5) & (latest_data['pct_chg'] > -9.8)).sum(),
            'limit_down': (latest_data['pct_chg'] <= -9.8).sum()
        }
        
        # 市场情绪
        up_ratio = summary['up_stocks'] / summary['total_stocks']
        if up_ratio > 0.7:
            summary['market_sentiment'] = '强势'
        elif up_ratio > 0.6:
            summary['market_sentiment'] = '偏强'
        elif up_ratio > 0.4:
            summary['market_sentiment'] = '震荡'
        elif up_ratio > 0.3:
            summary['market_sentiment'] = '偏弱'
        else:
            summary['market_sentiment'] = '弱势'
        
        logger.success("✅ 市场总结生成完成")
        
        return summary
    
    def get_top_stocks(self, data: pd.DataFrame, criteria: str = 'pct_chg', top_n: int = 50) -> pd.DataFrame:
        """获取排行榜股票"""
        latest_data = data.groupby('ts_code').last().reset_index()
        
        if criteria == 'pct_chg':
            # 涨幅榜
            top_stocks = latest_data.nlargest(top_n, 'pct_chg')
        elif criteria == 'vol':
            # 成交量榜
            top_stocks = latest_data.nlargest(top_n, 'vol')
        elif criteria == 'amount':
            # 成交额榜
            top_stocks = latest_data.nlargest(top_n, 'amount')
        elif criteria == 'turnover_rate':
            # 换手率榜
            top_stocks = latest_data.nlargest(top_n, 'turnover_rate')
        else:
            logger.error(f"❌ 未知的排序标准: {criteria}")
            return pd.DataFrame()
        
        logger.info(f"✅ 获取 {criteria} 排行榜前 {top_n} 只股票")
        
        return top_stocks[['ts_code', 'close', 'pct_chg', 'vol', 'amount', 'turnover_rate']]
