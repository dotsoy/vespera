"""
基本面分析器 - 识别短期催化剂
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import jieba
from loguru import logger

from config.settings import data_settings
from src.utils.database import get_db_manager


class FundamentalAnalyzer:
    """基本面分析器"""
    
    def __init__(self):
        self.db_manager = get_db_manager()
        
        # 催化剂关键词
        self.positive_keywords = [
            '业绩增长', '盈利', '营收增长', '订单', '合同', '中标', '重组', '并购',
            '分红', '回购', '增持', '利好', '突破', '创新', '专利', '技术',
            '合作', '战略', '投资', '扩产', '新品', '上市', '获批'
        ]
        
        self.negative_keywords = [
            '亏损', '下滑', '减少', '风险', '诉讼', '处罚', '停产', '延期',
            '取消', '终止', '减持', '质押', '违规', '调查', '警告', '退市'
        ]
        
        # 行业热点关键词
        self.industry_hotwords = {
            '人工智能': ['AI', '人工智能', '机器学习', '深度学习', '算法'],
            '新能源': ['新能源', '电池', '锂电', '光伏', '风电', '储能'],
            '半导体': ['芯片', '半导体', '集成电路', 'IC', '晶圆'],
            '生物医药': ['医药', '疫苗', '新药', '临床', '医疗器械'],
            '5G通信': ['5G', '通信', '基站', '物联网', 'IoT'],
            '新材料': ['新材料', '碳纤维', '石墨烯', '纳米材料']
        }
        
        logger.info("基本面分析器初始化完成")
    
    def get_company_announcements(self, ts_code: str, days: int = 30) -> List[Dict]:
        """
        获取公司公告信息 (模拟数据，实际应接入真实数据源)
        
        Args:
            ts_code: 股票代码
            days: 获取天数
            
        Returns:
            公告列表
        """
        try:
            # 这里应该接入真实的公告数据源，如巨潮资讯、上交所、深交所等
            # 目前返回模拟数据
            announcements = [
                {
                    'date': '2024-12-01',
                    'title': '关于签署重大合同的公告',
                    'content': '公司与某大型企业签署了价值10亿元的战略合作协议',
                    'type': '重大合同'
                },
                {
                    'date': '2024-11-28',
                    'title': '2024年第三季度业绩预告',
                    'content': '预计第三季度净利润同比增长30%-40%',
                    'type': '业绩预告'
                }
            ]
            
            logger.info(f"获取到 {len(announcements)} 条公告")
            return announcements
            
        except Exception as e:
            logger.error(f"获取公司公告失败: {e}")
            return []
    
    def get_industry_news(self, industry: str, days: int = 7) -> List[Dict]:
        """
        获取行业新闻 (模拟数据)
        
        Args:
            industry: 行业名称
            days: 获取天数
            
        Returns:
            新闻列表
        """
        try:
            # 模拟行业新闻数据
            news = [
                {
                    'date': '2024-12-01',
                    'title': f'{industry}行业迎来政策利好',
                    'content': f'国家发布新政策支持{industry}行业发展',
                    'sentiment': 'positive'
                },
                {
                    'date': '2024-11-30',
                    'title': f'{industry}技术获得重大突破',
                    'content': f'某公司在{industry}领域取得技术突破',
                    'sentiment': 'positive'
                }
            ]
            
            return news
            
        except Exception as e:
            logger.error(f"获取行业新闻失败: {e}")
            return []
    
    def analyze_text_sentiment(self, text: str) -> float:
        """
        分析文本情感倾向
        
        Args:
            text: 待分析文本
            
        Returns:
            情感评分 (-1到1，负数为负面，正数为正面)
        """
        try:
            # 简单的关键词匹配方法
            words = jieba.lcut(text)
            
            positive_count = 0
            negative_count = 0
            
            for word in words:
                if any(keyword in word for keyword in self.positive_keywords):
                    positive_count += 1
                elif any(keyword in word for keyword in self.negative_keywords):
                    negative_count += 1
            
            total_count = positive_count + negative_count
            if total_count == 0:
                return 0.0
            
            sentiment_score = (positive_count - negative_count) / total_count
            return sentiment_score
            
        except Exception as e:
            logger.error(f"文本情感分析失败: {e}")
            return 0.0
    
    def calculate_catalyst_score(self, announcements: List[Dict]) -> float:
        """
        计算催化剂评分
        
        Args:
            announcements: 公告列表
            
        Returns:
            催化剂评分 (0-1)
        """
        try:
            if not announcements:
                return 0.0
            
            score = 0.0
            total_weight = 0.0
            
            for announcement in announcements:
                # 根据公告类型设置权重
                type_weights = {
                    '重大合同': 0.8,
                    '业绩预告': 0.9,
                    '重组并购': 1.0,
                    '分红回购': 0.6,
                    '技术突破': 0.7,
                    '政策利好': 0.5
                }
                
                weight = type_weights.get(announcement.get('type', ''), 0.3)
                
                # 分析公告内容情感
                content = announcement.get('content', '') + announcement.get('title', '')
                sentiment = self.analyze_text_sentiment(content)
                
                # 时间衰减因子
                try:
                    announcement_date = datetime.strptime(announcement['date'], '%Y-%m-%d')
                    days_ago = (datetime.now() - announcement_date).days
                    time_decay = max(0.1, 1 - days_ago / 30)  # 30天内有效
                except:
                    time_decay = 0.5
                
                # 计算单个公告得分
                announcement_score = max(0, sentiment) * weight * time_decay
                score += announcement_score
                total_weight += weight * time_decay
            
            if total_weight > 0:
                final_score = min(score / total_weight, 1.0)
            else:
                final_score = 0.0
            
            return final_score
            
        except Exception as e:
            logger.error(f"计算催化剂评分失败: {e}")
            return 0.0
    
    def calculate_news_sentiment(self, news_list: List[Dict]) -> float:
        """
        计算新闻情感评分
        
        Args:
            news_list: 新闻列表
            
        Returns:
            新闻情感评分 (0-1)
        """
        try:
            if not news_list:
                return 0.5  # 中性
            
            sentiment_scores = []
            
            for news in news_list:
                content = news.get('content', '') + news.get('title', '')
                sentiment = self.analyze_text_sentiment(content)
                
                # 转换为0-1评分
                normalized_sentiment = (sentiment + 1) / 2
                sentiment_scores.append(normalized_sentiment)
            
            # 计算加权平均（最近的新闻权重更高）
            weights = [1.0 / (i + 1) for i in range(len(sentiment_scores))]
            weighted_sentiment = np.average(sentiment_scores, weights=weights)
            
            return weighted_sentiment
            
        except Exception as e:
            logger.error(f"计算新闻情感评分失败: {e}")
            return 0.5
    
    def calculate_announcement_impact(self, announcements: List[Dict]) -> float:
        """
        计算公告影响力评分
        
        Args:
            announcements: 公告列表
            
        Returns:
            公告影响力评分 (0-1)
        """
        try:
            if not announcements:
                return 0.0
            
            impact_score = 0.0
            
            # 重大事件影响力权重
            major_events = ['重组并购', '重大合同', '业绩预告', '分红回购']
            
            for announcement in announcements:
                announcement_type = announcement.get('type', '')
                
                if announcement_type in major_events:
                    # 分析公告内容的具体影响
                    content = announcement.get('content', '')
                    
                    # 查找数字信息（金额、百分比等）
                    import re
                    numbers = re.findall(r'(\d+(?:\.\d+)?)[%亿万千百]', content)
                    
                    if numbers:
                        # 有具体数字的公告影响力更大
                        impact_score += 0.3
                    else:
                        impact_score += 0.1
                    
                    # 时间新鲜度
                    try:
                        announcement_date = datetime.strptime(announcement['date'], '%Y-%m-%d')
                        days_ago = (datetime.now() - announcement_date).days
                        if days_ago <= 3:
                            impact_score += 0.2
                        elif days_ago <= 7:
                            impact_score += 0.1
                    except:
                        pass
            
            return min(impact_score, 1.0)
            
        except Exception as e:
            logger.error(f"计算公告影响力失败: {e}")
            return 0.0
    
    def calculate_industry_momentum(self, industry: str) -> float:
        """
        计算行业动量评分
        
        Args:
            industry: 行业名称
            
        Returns:
            行业动量评分 (0-1)
        """
        try:
            score = 0.0
            
            # 检查是否为热点行业
            for hot_industry, keywords in self.industry_hotwords.items():
                if any(keyword in industry for keyword in keywords):
                    score += 0.4
                    break
            
            # 获取行业新闻
            industry_news = self.get_industry_news(industry)
            news_sentiment = self.calculate_news_sentiment(industry_news)
            
            # 新闻情感贡献
            if news_sentiment > 0.6:
                score += 0.3
            elif news_sentiment > 0.5:
                score += 0.2
            
            # 行业政策支持度 (模拟)
            policy_support_industries = ['新能源', '人工智能', '半导体', '生物医药']
            if any(policy_industry in industry for policy_industry in policy_support_industries):
                score += 0.3
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"计算行业动量失败: {e}")
            return 0.0
    
    def analyze_stock(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        """
        分析单只股票的基本面
        
        Args:
            ts_code: 股票代码
            trade_date: 分析日期
            
        Returns:
            基本面分析结果
        """
        try:
            # 获取股票基本信息
            stock_info_query = """
            SELECT name, industry FROM stock_basic 
            WHERE ts_code = :ts_code
            """
            
            stock_info = self.db_manager.execute_postgres_query(
                stock_info_query, {'ts_code': ts_code}
            )
            
            if stock_info.empty:
                logger.warning(f"股票 {ts_code} 无基本信息")
                return {}
            
            industry = stock_info.iloc[0]['industry']
            
            # 获取公司公告
            announcements = self.get_company_announcements(ts_code)
            
            # 计算各项评分
            catalyst_score = self.calculate_catalyst_score(announcements)
            news_sentiment = self.calculate_news_sentiment(
                self.get_industry_news(industry)
            )
            announcement_impact = self.calculate_announcement_impact(announcements)
            industry_momentum = self.calculate_industry_momentum(industry)
            
            # 构建基本面数据
            fundamental_data = {
                'industry': industry,
                'recent_announcements': len(announcements),
                'major_events': [ann for ann in announcements 
                               if ann.get('type') in ['重组并购', '重大合同', '业绩预告']],
                'industry_hotwords': [hw for hw, keywords in self.industry_hotwords.items() 
                                    if any(kw in industry for kw in keywords)]
            }
            
            result = {
                'ts_code': ts_code,
                'trade_date': trade_date,
                'catalyst_score': round(catalyst_score, 3),
                'news_sentiment': round(news_sentiment, 3),
                'announcement_impact': round(announcement_impact, 3),
                'industry_momentum': round(industry_momentum, 3),
                # 将字典转换为 JSON 字符串，避免数据库插入问题
                'fundamental_data': str(fundamental_data) if fundamental_data else None
            }
            
            logger.info(f"股票 {ts_code} 基本面分析完成")
            return result
            
        except Exception as e:
            logger.error(f"分析股票 {ts_code} 基本面失败: {e}")
            return {}


if __name__ == "__main__":
    # 测试基本面分析器
    analyzer = FundamentalAnalyzer()
    
    # 测试分析单只股票
    result = analyzer.analyze_stock('000001.SZ', '2024-12-01')
    print("基本面分析结果:", result)
