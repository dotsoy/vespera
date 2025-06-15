"""
股票过滤器
用于过滤股票列表，排除ST股票、北交所等
"""
import pandas as pd
import re
from typing import List, Dict, Any
from loguru import logger


class StockFilter:
    """股票过滤器"""
    
    def __init__(self):
        # ST股票关键词
        self.st_keywords = [
            'ST', 'st', '*ST', '*st',
            '退市', '暂停', '终止',
            'PT', 'pt'
        ]
        
        # 北交所股票代码模式
        self.bj_patterns = [
            r'^8\d{5}\.BJ$',  # 8开头的北交所代码
            r'^\d{6}\.BJ$',   # 以.BJ结尾的代码
            r'^43\d{4}$',     # 43开头的新三板代码
            r'^83\d{4}$',     # 83开头的新三板代码
            r'^87\d{4}$'      # 87开头的新三板代码
        ]
        
        # 有效的A股市场代码
        self.valid_markets = ['.SZ', '.SH']
        
        # 有效的A股代码模式
        self.a_share_patterns = [
            r'^00\d{4}\.SZ$',  # 深交所主板
            r'^30\d{4}\.SZ$',  # 创业板
            r'^60\d{4}\.SH$',  # 上交所主板
            r'^68\d{4}\.SH$'   # 科创板
        ]
    
    def is_st_stock(self, stock_name: str) -> bool:
        """判断是否为ST股票"""
        if not stock_name:
            return False
        
        # 检查股票名称中是否包含ST关键词
        for keyword in self.st_keywords:
            if keyword in stock_name:
                return True
        
        return False
    
    def is_bj_stock(self, stock_code: str) -> bool:
        """判断是否为北交所股票"""
        if not stock_code:
            return False
        
        # 检查是否匹配北交所代码模式
        for pattern in self.bj_patterns:
            if re.match(pattern, stock_code):
                return True
        
        return False
    
    def is_valid_a_share(self, stock_code: str) -> bool:
        """判断是否为有效的A股代码"""
        if not stock_code:
            return False
        
        # 检查是否匹配A股代码模式
        for pattern in self.a_share_patterns:
            if re.match(pattern, stock_code):
                return True
        
        return False
    
    def filter_stock_list(self, stock_data: pd.DataFrame, 
                         code_column: str = 'ts_code',
                         name_column: str = 'name') -> pd.DataFrame:
        """
        过滤股票列表
        
        Args:
            stock_data: 股票数据DataFrame
            code_column: 股票代码列名
            name_column: 股票名称列名
            
        Returns:
            过滤后的股票数据
        """
        if stock_data.empty:
            logger.warning("股票数据为空")
            return stock_data
        
        original_count = len(stock_data)
        logger.info(f"开始过滤股票，原始数量: {original_count}")
        
        # 复制数据避免修改原始数据
        filtered_data = stock_data.copy()
        
        # 1. 排除ST股票
        if name_column in filtered_data.columns:
            st_mask = filtered_data[name_column].apply(lambda x: not self.is_st_stock(str(x)))
            filtered_data = filtered_data[st_mask]
            st_excluded = original_count - len(filtered_data)
            logger.info(f"排除ST股票: {st_excluded} 只，剩余: {len(filtered_data)} 只")
        else:
            logger.warning(f"未找到股票名称列: {name_column}")
        
        # 2. 排除北交所股票
        if code_column in filtered_data.columns:
            bj_mask = filtered_data[code_column].apply(lambda x: not self.is_bj_stock(str(x)))
            before_bj = len(filtered_data)
            filtered_data = filtered_data[bj_mask]
            bj_excluded = before_bj - len(filtered_data)
            logger.info(f"排除北交所股票: {bj_excluded} 只，剩余: {len(filtered_data)} 只")
        else:
            logger.warning(f"未找到股票代码列: {code_column}")
        
        # 3. 只保留有效的A股
        if code_column in filtered_data.columns:
            a_share_mask = filtered_data[code_column].apply(lambda x: self.is_valid_a_share(str(x)))
            before_a_share = len(filtered_data)
            filtered_data = filtered_data[a_share_mask]
            invalid_excluded = before_a_share - len(filtered_data)
            logger.info(f"排除无效代码: {invalid_excluded} 只，剩余: {len(filtered_data)} 只")
        
        # 统计最终结果
        final_count = len(filtered_data)
        total_excluded = original_count - final_count
        
        logger.success(f"股票过滤完成:")
        logger.info(f"  原始数量: {original_count}")
        logger.info(f"  最终数量: {final_count}")
        logger.info(f"  排除数量: {total_excluded}")
        logger.info(f"  保留比例: {final_count/original_count*100:.1f}%")
        
        # 显示市场分布
        if code_column in filtered_data.columns and not filtered_data.empty:
            sz_count = filtered_data[code_column].str.endswith('.SZ').sum()
            sh_count = filtered_data[code_column].str.endswith('.SH').sum()
            
            logger.info(f"市场分布:")
            logger.info(f"  深交所: {sz_count} 只")
            logger.info(f"  上交所: {sh_count} 只")
            
            # 详细分布
            if sz_count > 0:
                main_board_sz = filtered_data[code_column].str.match(r'^00\d{4}\.SZ$').sum()
                gem_board = filtered_data[code_column].str.match(r'^30\d{4}\.SZ$').sum()
                logger.info(f"    深交所主板: {main_board_sz} 只")
                logger.info(f"    创业板: {gem_board} 只")
            
            if sh_count > 0:
                main_board_sh = filtered_data[code_column].str.match(r'^60\d{4}\.SH$').sum()
                star_board = filtered_data[code_column].str.match(r'^68\d{4}\.SH$').sum()
                logger.info(f"    上交所主板: {main_board_sh} 只")
                logger.info(f"    科创板: {star_board} 只")
        
        return filtered_data
    
    def get_filter_stats(self, stock_data: pd.DataFrame,
                        code_column: str = 'ts_code',
                        name_column: str = 'name') -> Dict[str, Any]:
        """
        获取过滤统计信息
        
        Returns:
            过滤统计字典
        """
        if stock_data.empty:
            return {}
        
        stats = {
            'total_count': len(stock_data),
            'st_count': 0,
            'bj_count': 0,
            'valid_a_share_count': 0,
            'sz_count': 0,
            'sh_count': 0
        }
        
        # 统计ST股票
        if name_column in stock_data.columns:
            stats['st_count'] = stock_data[name_column].apply(
                lambda x: self.is_st_stock(str(x))
            ).sum()
        
        # 统计北交所股票
        if code_column in stock_data.columns:
            stats['bj_count'] = stock_data[code_column].apply(
                lambda x: self.is_bj_stock(str(x))
            ).sum()
            
            # 统计有效A股
            stats['valid_a_share_count'] = stock_data[code_column].apply(
                lambda x: self.is_valid_a_share(str(x))
            ).sum()
            
            # 统计市场分布
            stats['sz_count'] = stock_data[code_column].str.endswith('.SZ').sum()
            stats['sh_count'] = stock_data[code_column].str.endswith('.SH').sum()
        
        return stats
    
    def validate_stock_code(self, stock_code: str) -> Dict[str, bool]:
        """
        验证单个股票代码
        
        Returns:
            验证结果字典
        """
        return {
            'is_valid_format': bool(re.match(r'^\d{6}\.(SZ|SH|BJ)$', stock_code)),
            'is_a_share': self.is_valid_a_share(stock_code),
            'is_bj_stock': self.is_bj_stock(stock_code),
            'market': stock_code.split('.')[-1] if '.' in stock_code else None
        }
    
    def get_recommended_stocks(self, stock_data: pd.DataFrame,
                              code_column: str = 'ts_code',
                              name_column: str = 'name',
                              limit: int = 100) -> pd.DataFrame:
        """
        获取推荐的股票列表（经过过滤的优质股票）
        
        Args:
            stock_data: 原始股票数据
            limit: 返回数量限制
            
        Returns:
            推荐股票列表
        """
        # 先进行基本过滤
        filtered_data = self.filter_stock_list(stock_data, code_column, name_column)
        
        if filtered_data.empty:
            return filtered_data
        
        # 优先选择主板股票
        if code_column in filtered_data.columns:
            # 主板股票优先级更高
            main_board_mask = (
                filtered_data[code_column].str.match(r'^00\d{4}\.SZ$') |  # 深交所主板
                filtered_data[code_column].str.match(r'^60\d{4}\.SH$')    # 上交所主板
            )
            
            main_board_stocks = filtered_data[main_board_mask]
            other_stocks = filtered_data[~main_board_mask]
            
            # 按主板优先排序
            recommended = pd.concat([main_board_stocks, other_stocks], ignore_index=True)
            
            if len(recommended) > limit:
                recommended = recommended.head(limit)
                logger.info(f"返回推荐股票: {len(recommended)} 只（限制: {limit}）")
            
            return recommended
        
        return filtered_data.head(limit) if len(filtered_data) > limit else filtered_data


# 便捷函数
def filter_a_share_stocks(stock_data: pd.DataFrame,
                         code_column: str = 'ts_code',
                         name_column: str = 'name') -> pd.DataFrame:
    """
    便捷函数：过滤A股股票
    """
    filter_instance = StockFilter()
    return filter_instance.filter_stock_list(stock_data, code_column, name_column)


def get_stock_filter_stats(stock_data: pd.DataFrame,
                          code_column: str = 'ts_code',
                          name_column: str = 'name') -> Dict[str, Any]:
    """
    便捷函数：获取股票过滤统计
    """
    filter_instance = StockFilter()
    return filter_instance.get_filter_stats(stock_data, code_column, name_column)
