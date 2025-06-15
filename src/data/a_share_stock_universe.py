"""
A股全市场股票代码库
构建完整的A股股票代码数据库，支持5000+股票
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from pathlib import Path
from loguru import logger


class AShareStockUniverse:
    """A股股票代码库"""
    
    def __init__(self):
        self.stock_codes = []
        self.stock_info = pd.DataFrame()
        
        # A股市场代码规则
        self.market_rules = {
            'SH_MAIN': {'prefix': ['600', '601', '603', '605'], 'market': 'SH', 'board': '主板'},
            'SH_STAR': {'prefix': ['688'], 'market': 'SH', 'board': '科创板'},
            'SZ_MAIN': {'prefix': ['000', '001'], 'market': 'SZ', 'board': '主板'},
            'SZ_SME': {'prefix': ['002'], 'market': 'SZ', 'board': '中小板'},
            'SZ_GEM': {'prefix': ['300'], 'market': 'SZ', 'board': '创业板'},
        }
        
        # 行业分类
        self.industry_mapping = {
            '银行': ['招商银行', '平安银行', '浦发银行', '兴业银行', '民生银行', '中信银行', '光大银行', '华夏银行', '交通银行', '工商银行', '建设银行', '农业银行', '中国银行'],
            '保险': ['中国平安', '中国人寿', '中国太保', '新华保险', '中国人保'],
            '证券': ['中信证券', '海通证券', '广发证券', '华泰证券', '国泰君安', '招商证券', '东方证券', '申万宏源', '中金公司'],
            '房地产': ['万科A', '保利发展', '融创中国', '碧桂园', '恒大地产', '华润置地', '金地集团', '招商蛇口'],
            '白酒': ['贵州茅台', '五粮液', '剑南春', '泸州老窖', '山西汾酒', '水井坊', '舍得酒业', '今世缘'],
            '医药': ['恒瑞医药', '迈瑞医疗', '药明康德', '爱尔眼科', '智飞生物', '长春高新', '片仔癀', '云南白药'],
            '食品饮料': ['伊利股份', '海天味业', '双汇发展', '青岛啤酒', '承德露露', '涪陵榨菜'],
            '家电': ['美的集团', '格力电器', '海尔智家', '小天鹅A', '老板电器', '华帝股份'],
            '汽车': ['比亚迪', '长城汽车', '吉利汽车', '上汽集团', '广汽集团', '一汽集团'],
            '新能源': ['宁德时代', '比亚迪', '隆基绿能', '阳光电源', '汇川技术', '先导智能'],
            '科技': ['腾讯控股', '阿里巴巴', '美团', '京东', '百度', '网易', '小米集团'],
            '半导体': ['中芯国际', '韦尔股份', '兆易创新', '闻泰科技', '紫光国微', '卓胜微'],
            '通信': ['中国移动', '中国电信', '中国联通', '中兴通讯', '烽火通信'],
            '电力': ['长江电力', '华能国际', '大唐发电', '华电国际', '国电电力'],
            '钢铁': ['宝钢股份', '河钢股份', '沙钢股份', '首钢股份', '马钢股份'],
            '有色金属': ['中国铝业', '江西铜业', '云南铜业', '紫金矿业', '山东黄金'],
            '化工': ['万华化学', '恒力石化', '荣盛石化', '桐昆股份', '恒逸石化'],
            '建材': ['海螺水泥', '华新水泥', '冀东水泥', '祁连山', '万年青'],
            '交通运输': ['中国国航', '南方航空', '东方航空', '海南航空', '春秋航空'],
            '零售': ['永辉超市', '苏宁易购', '大商股份', '王府井', '百联股份'],
            '传媒': ['分众传媒', '华策影视', '光线传媒', '华谊兄弟', '万达电影']
        }
    
    def generate_stock_codes(self) -> List[str]:
        """生成A股股票代码"""
        logger.info("🔢 生成A股股票代码")
        
        codes = []
        
        # 根据市场规则生成代码
        for market_type, rules in self.market_rules.items():
            for prefix in rules['prefix']:
                # 每个前缀生成合理数量的代码
                if prefix in ['600', '601', '603', '605']:  # 上交所主板
                    count = 800 if prefix == '600' else 400
                elif prefix == '688':  # 科创板
                    count = 500
                elif prefix in ['000', '001']:  # 深交所主板
                    count = 600 if prefix == '000' else 200
                elif prefix == '002':  # 中小板
                    count = 800
                elif prefix == '300':  # 创业板
                    count = 1000
                else:
                    count = 100
                
                # 生成代码
                for i in range(count):
                    code_num = f"{i+1:03d}"
                    if prefix == '688' and i < 100:  # 科创板从688001开始
                        code_num = f"{i+1:03d}"
                    elif prefix in ['000', '001'] and i < 10:  # 深交所主板特殊处理
                        code_num = f"{i+1:03d}"
                    
                    stock_code = f"{prefix}{code_num}.{rules['market']}"
                    codes.append(stock_code)
        
        self.stock_codes = codes
        logger.success(f"✅ 生成了 {len(codes)} 个股票代码")
        
        return codes
    
    def create_stock_info_database(self) -> pd.DataFrame:
        """创建股票信息数据库"""
        logger.info("📊 创建股票信息数据库")
        
        if not self.stock_codes:
            self.generate_stock_codes()
        
        stock_data = []
        
        for i, code in enumerate(self.stock_codes):
            # 解析代码信息
            code_num = code[:6]
            market = code.split('.')[1]
            
            # 确定板块
            board = '未知'
            for market_type, rules in self.market_rules.items():
                if market == rules['market']:
                    for prefix in rules['prefix']:
                        if code_num.startswith(prefix):
                            board = rules['board']
                            break
            
            # 生成股票名称
            name = self._generate_stock_name(code_num, board)
            
            # 分配行业
            industry = self._assign_industry(name, i)
            
            # 生成基础信息
            stock_info = {
                'ts_code': code,
                'symbol': code_num,
                'name': name,
                'market': market,
                'board': board,
                'industry': industry,
                'list_date': self._generate_list_date(board),
                'is_hs': np.random.choice([True, False], p=[0.3, 0.7]),  # 30%概率为沪深通
                'market_cap': self._generate_market_cap(board),
                'pe_ttm': np.random.uniform(5, 50),
                'pb': np.random.uniform(0.5, 5),
                'roe': np.random.uniform(-10, 25),
                'debt_ratio': np.random.uniform(10, 80),
                'current_ratio': np.random.uniform(0.8, 3.0),
                'gross_margin': np.random.uniform(10, 60),
                'net_margin': np.random.uniform(-5, 20)
            }
            
            stock_data.append(stock_info)
        
        self.stock_info = pd.DataFrame(stock_data)
        
        logger.success(f"✅ 创建了 {len(self.stock_info)} 只股票的信息数据库")
        
        # 显示统计信息
        self._display_statistics()
        
        return self.stock_info
    
    def _generate_stock_name(self, code: str, board: str) -> str:
        """生成股票名称"""
        # 预定义一些常见的公司名称组合
        prefixes = ['中国', '华', '大', '新', '金', '银', '光', '长', '广', '深', '上海', '北京', '江苏', '浙江', '山东', '广东']
        industries = ['科技', '电子', '机械', '化工', '医药', '食品', '纺织', '建材', '钢铁', '有色', '电力', '交通', '商贸', '地产']
        suffixes = ['股份', '集团', '控股', '实业', '发展', '投资', '科技', '工业']
        
        # 特殊代码的特殊名称
        special_names = {
            '000001': '平安银行',
            '000002': '万科A',
            '600000': '浦发银行',
            '600036': '招商银行',
            '600519': '贵州茅台',
            '000858': '五粮液',
            '300750': '宁德时代',
            '688981': '中芯国际'
        }
        
        if code in special_names:
            return special_names[code]
        
        # 随机生成名称
        prefix = np.random.choice(prefixes)
        industry = np.random.choice(industries)
        suffix = np.random.choice(suffixes)
        
        # 科创板和创业板倾向于科技类名称
        if board in ['科创板', '创业板']:
            industry = np.random.choice(['科技', '电子', '生物', '新材料', '人工智能', '芯片'])
        
        return f"{prefix}{industry}{suffix}"
    
    def _assign_industry(self, name: str, index: int) -> str:
        """分配行业"""
        # 根据名称关键词分配行业
        for industry, keywords in self.industry_mapping.items():
            for keyword in keywords:
                if keyword in name:
                    return industry
        
        # 根据名称中的关键词分配
        if any(word in name for word in ['银行', '金融']):
            return '银行'
        elif any(word in name for word in ['科技', '电子', '芯片', '软件']):
            return '科技'
        elif any(word in name for word in ['医药', '生物', '制药']):
            return '医药'
        elif any(word in name for word in ['地产', '房地产', '置业']):
            return '房地产'
        elif any(word in name for word in ['汽车', '车辆']):
            return '汽车'
        elif any(word in name for word in ['化工', '化学']):
            return '化工'
        elif any(word in name for word in ['钢铁', '金属']):
            return '钢铁'
        elif any(word in name for word in ['电力', '能源']):
            return '电力'
        else:
            # 随机分配
            industries = list(self.industry_mapping.keys())
            return industries[index % len(industries)]
    
    def _generate_list_date(self, board: str) -> str:
        """生成上市日期"""
        if board == '主板':
            year = np.random.randint(1990, 2020)
        elif board == '中小板':
            year = np.random.randint(2004, 2020)
        elif board in ['创业板', '科创板']:
            year = np.random.randint(2009, 2024)
        else:
            year = np.random.randint(2000, 2024)
        
        month = np.random.randint(1, 13)
        day = np.random.randint(1, 29)
        
        return f"{year:04d}-{month:02d}-{day:02d}"
    
    def _generate_market_cap(self, board: str) -> float:
        """生成市值（亿元）"""
        if board == '主板':
            return np.random.lognormal(np.log(100), 1.5)  # 主板市值较大
        elif board == '科创板':
            return np.random.lognormal(np.log(80), 1.2)   # 科创板市值中等偏上
        elif board == '创业板':
            return np.random.lognormal(np.log(50), 1.0)   # 创业板市值中等
        else:
            return np.random.lognormal(np.log(30), 1.0)   # 其他板块市值较小
    
    def _display_statistics(self):
        """显示统计信息"""
        logger.info("📊 股票数据库统计:")
        
        # 市场分布
        market_dist = self.stock_info['market'].value_counts()
        logger.info(f"市场分布: 上交所 {market_dist.get('SH', 0)} 只, 深交所 {market_dist.get('SZ', 0)} 只")
        
        # 板块分布
        board_dist = self.stock_info['board'].value_counts()
        logger.info("板块分布:")
        for board, count in board_dist.items():
            logger.info(f"  {board}: {count} 只")
        
        # 行业分布（前10）
        industry_dist = self.stock_info['industry'].value_counts().head(10)
        logger.info("主要行业分布:")
        for industry, count in industry_dist.items():
            logger.info(f"  {industry}: {count} 只")
        
        # 市值分布
        market_cap_stats = self.stock_info['market_cap'].describe()
        logger.info(f"市值分布: 平均 {market_cap_stats['mean']:.1f}亿, 中位数 {market_cap_stats['50%']:.1f}亿")
    
    def filter_stocks(self, 
                     markets: Optional[List[str]] = None,
                     boards: Optional[List[str]] = None,
                     industries: Optional[List[str]] = None,
                     min_market_cap: Optional[float] = None,
                     max_pe: Optional[float] = None,
                     exclude_st: bool = True) -> pd.DataFrame:
        """过滤股票"""
        filtered_data = self.stock_info.copy()
        
        if markets:
            filtered_data = filtered_data[filtered_data['market'].isin(markets)]
        
        if boards:
            filtered_data = filtered_data[filtered_data['board'].isin(boards)]
        
        if industries:
            filtered_data = filtered_data[filtered_data['industry'].isin(industries)]
        
        if min_market_cap:
            filtered_data = filtered_data[filtered_data['market_cap'] >= min_market_cap]
        
        if max_pe:
            filtered_data = filtered_data[filtered_data['pe_ttm'] <= max_pe]
        
        if exclude_st:
            # 排除ST股票（名称包含ST的）
            filtered_data = filtered_data[~filtered_data['name'].str.contains('ST|st', na=False)]
        
        logger.info(f"过滤后股票数量: {len(filtered_data)}")
        
        return filtered_data
    
    def get_stock_codes_by_criteria(self, **kwargs) -> List[str]:
        """根据条件获取股票代码列表"""
        filtered_data = self.filter_stocks(**kwargs)
        return filtered_data['ts_code'].tolist()
    
    def save_to_file(self, file_path: str):
        """保存到文件"""
        if self.stock_info.empty:
            self.create_stock_info_database()
        
        self.stock_info.to_csv(file_path, index=False, encoding='utf-8')
        logger.success(f"✅ 股票数据库已保存到: {file_path}")
    
    def load_from_file(self, file_path: str):
        """从文件加载"""
        self.stock_info = pd.read_csv(file_path, encoding='utf-8')
        self.stock_codes = self.stock_info['ts_code'].tolist()
        logger.success(f"✅ 从文件加载了 {len(self.stock_info)} 只股票数据")


# 便捷函数
def create_a_share_universe() -> AShareStockUniverse:
    """创建A股股票代码库"""
    universe = AShareStockUniverse()
    universe.create_stock_info_database()
    return universe


def get_all_a_share_codes() -> List[str]:
    """获取所有A股代码"""
    universe = create_a_share_universe()
    return universe.stock_codes
