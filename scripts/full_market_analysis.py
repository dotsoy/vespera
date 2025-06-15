#!/usr/bin/env python3
"""
A股全市场分析脚本
分析5000+A股股票，提供全面的市场分析和选股建议
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.data.a_share_stock_universe import AShareStockUniverse
from src.analysis.full_market_analyzer import FullMarketAnalyzer

logger = get_logger("full_market_analysis")


def initialize_stock_universe():
    """初始化股票代码库"""
    logger.info("🌌 初始化A股股票代码库")
    
    try:
        universe = AShareStockUniverse()
        
        # 检查是否已有保存的数据
        universe_file = project_root / 'data' / 'universe' / 'a_share_universe.csv'
        
        if universe_file.exists():
            logger.info("发现已保存的股票代码库，正在加载...")
            universe.load_from_file(str(universe_file))
        else:
            logger.info("创建新的股票代码库...")
            universe.create_stock_info_database()
            
            # 保存到文件
            universe_file.parent.mkdir(parents=True, exist_ok=True)
            universe.save_to_file(str(universe_file))
        
        logger.success(f"✅ 股票代码库初始化完成: {len(universe.stock_codes)} 只股票")
        
        return universe
        
    except Exception as e:
        logger.error(f"❌ 股票代码库初始化失败: {e}")
        return None


def analyze_full_market(universe: AShareStockUniverse, sample_size: int = 1000):
    """分析全市场"""
    logger.info(f"📊 开始全市场分析 (样本: {sample_size} 只股票)")
    
    try:
        # 获取股票代码（为了演示，使用样本）
        all_codes = universe.stock_codes
        
        if len(all_codes) > sample_size:
            # 分层抽样，确保各板块都有代表
            sample_codes = []
            
            # 按板块分层抽样
            stock_info = universe.stock_info
            boards = stock_info['board'].unique()
            
            for board in boards:
                board_stocks = stock_info[stock_info['board'] == board]['ts_code'].tolist()
                board_sample_size = min(len(board_stocks), sample_size // len(boards))
                board_sample = np.random.choice(board_stocks, board_sample_size, replace=False)
                sample_codes.extend(board_sample)
            
            # 如果还不够，随机补充
            if len(sample_codes) < sample_size:
                remaining = sample_size - len(sample_codes)
                remaining_codes = [code for code in all_codes if code not in sample_codes]
                if remaining_codes:
                    additional = np.random.choice(remaining_codes, 
                                                min(remaining, len(remaining_codes)), 
                                                replace=False)
                    sample_codes.extend(additional)
            
            analysis_codes = sample_codes[:sample_size]
        else:
            analysis_codes = all_codes
        
        logger.info(f"实际分析股票数量: {len(analysis_codes)}")
        
        # 创建分析器
        analyzer = FullMarketAnalyzer(max_workers=10)
        
        # 生成市场数据
        logger.info("📈 生成市场数据...")
        market_data = analyzer.create_market_data(analysis_codes, days=30)
        
        # 计算技术指标
        logger.info("🔢 计算技术指标...")
        technical_data = analyzer.calculate_technical_indicators(market_data)
        
        # 生成市场总结
        logger.info("📊 生成市场总结...")
        market_summary = analyzer.generate_market_summary(technical_data)
        
        # 股票筛选
        logger.info("🔍 进行股票筛选...")
        momentum_stocks = analyzer.screen_stocks(technical_data, 'momentum')
        technical_stocks = analyzer.screen_stocks(technical_data, 'technical')
        
        # 获取排行榜
        logger.info("🏆 生成排行榜...")
        top_gainers = analyzer.get_top_stocks(technical_data, 'pct_chg', 20)
        top_volume = analyzer.get_top_stocks(technical_data, 'vol', 20)
        top_amount = analyzer.get_top_stocks(technical_data, 'amount', 20)
        
        results = {
            'market_data': technical_data,
            'market_summary': market_summary,
            'momentum_stocks': momentum_stocks,
            'technical_stocks': technical_stocks,
            'top_gainers': top_gainers,
            'top_volume': top_volume,
            'top_amount': top_amount,
            'analysis_codes': analysis_codes
        }
        
        logger.success("✅ 全市场分析完成")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ 全市场分析失败: {e}")
        return None


def generate_sector_analysis(universe: AShareStockUniverse, market_data: pd.DataFrame):
    """生成板块分析"""
    logger.info("🏭 生成板块分析")
    
    try:
        # 合并股票信息
        stock_info = universe.stock_info[['ts_code', 'board', 'industry']]
        latest_data = market_data.groupby('ts_code').last().reset_index()
        sector_data = latest_data.merge(stock_info, on='ts_code', how='left')
        
        # 板块分析
        board_analysis = sector_data.groupby('board').agg({
            'pct_chg': ['mean', 'median', 'std', 'count'],
            'vol': 'sum',
            'amount': 'sum',
            'turnover_rate': 'mean'
        }).round(2)
        
        board_analysis.columns = ['avg_change', 'median_change', 'volatility', 'stock_count', 
                                 'total_volume', 'total_amount', 'avg_turnover']
        
        # 行业分析
        industry_analysis = sector_data.groupby('industry').agg({
            'pct_chg': ['mean', 'median', 'count'],
            'vol': 'sum',
            'amount': 'sum'
        }).round(2)
        
        industry_analysis.columns = ['avg_change', 'median_change', 'stock_count', 
                                   'total_volume', 'total_amount']
        
        # 排序
        board_analysis = board_analysis.sort_values('avg_change', ascending=False)
        industry_analysis = industry_analysis.sort_values('avg_change', ascending=False)
        
        logger.success("✅ 板块分析完成")
        
        return {
            'board_analysis': board_analysis,
            'industry_analysis': industry_analysis
        }
        
    except Exception as e:
        logger.error(f"❌ 板块分析失败: {e}")
        return None


def generate_investment_recommendations(analysis_results: dict, sector_analysis: dict):
    """生成投资建议"""
    logger.info("💡 生成投资建议")
    
    try:
        market_summary = analysis_results['market_summary']
        momentum_stocks = analysis_results['momentum_stocks']
        technical_stocks = analysis_results['technical_stocks']
        top_gainers = analysis_results['top_gainers']
        
        recommendations = {
            'market_outlook': '',
            'recommended_stocks': [],
            'sector_recommendations': {},
            'risk_warnings': [],
            'strategy_suggestions': []
        }
        
        # 市场展望
        sentiment = market_summary['market_sentiment']
        up_ratio = market_summary['up_stocks'] / market_summary['total_stocks']
        
        if sentiment == '强势':
            recommendations['market_outlook'] = f"市场呈现强势格局，上涨股票占比{up_ratio:.1%}，建议积极参与"
        elif sentiment == '偏强':
            recommendations['market_outlook'] = f"市场偏强运行，上涨股票占比{up_ratio:.1%}，可适度参与"
        elif sentiment == '震荡':
            recommendations['market_outlook'] = f"市场震荡整理，上涨股票占比{up_ratio:.1%}，建议谨慎操作"
        else:
            recommendations['market_outlook'] = f"市场偏弱运行，上涨股票占比{up_ratio:.1%}，建议控制仓位"
        
        # 推荐股票（结合动量和技术分析）
        if not momentum_stocks.empty and not technical_stocks.empty:
            # 找出同时符合动量和技术条件的股票
            common_stocks = set(momentum_stocks['ts_code']) & set(technical_stocks['ts_code'])
            
            for code in list(common_stocks)[:10]:  # 最多推荐10只
                stock_info = momentum_stocks[momentum_stocks['ts_code'] == code].iloc[0]
                recommendations['recommended_stocks'].append({
                    'code': code,
                    'price': stock_info['close'],
                    'change': stock_info['pct_chg'],
                    'volume_ratio': stock_info.get('vol_ratio', 1),
                    'reason': '同时符合动量和技术条件'
                })
        
        # 如果推荐股票不足，从动量股票中补充
        if len(recommendations['recommended_stocks']) < 5 and not momentum_stocks.empty:
            for _, stock in momentum_stocks.head(10).iterrows():
                if stock['ts_code'] not in [s['code'] for s in recommendations['recommended_stocks']]:
                    recommendations['recommended_stocks'].append({
                        'code': stock['ts_code'],
                        'price': stock['close'],
                        'change': stock['pct_chg'],
                        'volume_ratio': stock.get('vol_ratio', 1),
                        'reason': '符合动量策略'
                    })
                    if len(recommendations['recommended_stocks']) >= 5:
                        break
        
        # 板块建议
        if sector_analysis and 'board_analysis' in sector_analysis:
            board_analysis = sector_analysis['board_analysis']
            
            for board, data in board_analysis.head(3).iterrows():
                if data['avg_change'] > 1:
                    recommendations['sector_recommendations'][board] = '强烈推荐'
                elif data['avg_change'] > 0:
                    recommendations['sector_recommendations'][board] = '推荐关注'
                else:
                    recommendations['sector_recommendations'][board] = '谨慎观望'
        
        # 风险警告
        recommendations['risk_warnings'] = [
            'T+1交易制度：当日买入次日才能卖出',
            '涨跌停限制：注意10%/20%涨跌停限制',
            '市场波动：股市有风险，投资需谨慎',
            '仓位控制：建议分散投资，控制单股仓位'
        ]
        
        # 策略建议
        if sentiment in ['强势', '偏强']:
            recommendations['strategy_suggestions'] = [
                '可适度增加仓位，关注强势板块',
                '重点关注技术突破的个股',
                '注意追高风险，设置止损位'
            ]
        else:
            recommendations['strategy_suggestions'] = [
                '控制仓位，以防守为主',
                '关注超跌反弹机会',
                '等待市场明确方向后再加仓'
            ]
        
        logger.success("✅ 投资建议生成完成")
        
        return recommendations
        
    except Exception as e:
        logger.error(f"❌ 投资建议生成失败: {e}")
        return None


def save_analysis_results(analysis_results: dict, sector_analysis: dict, recommendations: dict):
    """保存分析结果"""
    logger.info("💾 保存分析结果")
    
    try:
        # 创建输出目录
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = project_root / 'analysis' / 'full_market' / timestamp
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存市场数据
        market_data = analysis_results['market_data']
        market_data.to_csv(output_dir / 'market_data.csv', index=False, encoding='utf-8')
        
        # 保存筛选结果
        if not analysis_results['momentum_stocks'].empty:
            analysis_results['momentum_stocks'].to_csv(
                output_dir / 'momentum_stocks.csv', index=False, encoding='utf-8'
            )
        
        if not analysis_results['technical_stocks'].empty:
            analysis_results['technical_stocks'].to_csv(
                output_dir / 'technical_stocks.csv', index=False, encoding='utf-8'
            )
        
        # 保存排行榜
        analysis_results['top_gainers'].to_csv(
            output_dir / 'top_gainers.csv', index=False, encoding='utf-8'
        )
        
        # 保存板块分析
        if sector_analysis:
            sector_analysis['board_analysis'].to_csv(
                output_dir / 'board_analysis.csv', encoding='utf-8'
            )
            sector_analysis['industry_analysis'].to_csv(
                output_dir / 'industry_analysis.csv', encoding='utf-8'
            )
        
        # 保存投资建议
        import json
        with open(output_dir / 'recommendations.json', 'w', encoding='utf-8') as f:
            json.dump(recommendations, f, ensure_ascii=False, indent=2)
        
        logger.success(f"✅ 分析结果已保存到: {output_dir}")
        
        return output_dir
        
    except Exception as e:
        logger.error(f"❌ 保存分析结果失败: {e}")
        return None


def display_analysis_summary(analysis_results: dict, sector_analysis: dict, recommendations: dict):
    """显示分析摘要"""
    logger.info("📋 分析结果摘要")
    
    try:
        market_summary = analysis_results['market_summary']
        
        print("\n" + "=" * 80)
        print("🌟 A股全市场分析报告")
        print("=" * 80)
        print(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"分析股票: {len(analysis_results['analysis_codes'])} 只")
        print()
        
        # 市场概况
        print("📊 市场概况:")
        print(f"  总股票数: {market_summary['total_stocks']}")
        print(f"  上涨股票: {market_summary['up_stocks']} ({market_summary['up_stocks']/market_summary['total_stocks']*100:.1f}%)")
        print(f"  下跌股票: {market_summary['down_stocks']} ({market_summary['down_stocks']/market_summary['total_stocks']*100:.1f}%)")
        print(f"  平均涨跌幅: {market_summary['avg_change']:.2f}%")
        print(f"  市场情绪: {market_summary['market_sentiment']}")
        print()
        
        # 涨跌分布
        print("📈 涨跌分布:")
        dist = market_summary['change_distribution']
        print(f"  涨停: {dist['limit_up']} 只")
        print(f"  大涨(5%+): {dist['strong_up']} 只")
        print(f"  中涨(2-5%): {dist['moderate_up']} 只")
        print(f"  小涨(0-2%): {dist['slight_up']} 只")
        print(f"  小跌(0-2%): {dist['slight_down']} 只")
        print(f"  中跌(2-5%): {dist['moderate_down']} 只")
        print(f"  大跌(5%+): {dist['strong_down']} 只")
        print(f"  跌停: {dist['limit_down']} 只")
        print()
        
        # 推荐股票
        if recommendations['recommended_stocks']:
            print("💡 推荐股票 (前5只):")
            for i, stock in enumerate(recommendations['recommended_stocks'][:5], 1):
                print(f"  {i}. {stock['code']} - 价格: ¥{stock['price']:.2f}, 涨幅: {stock['change']:.2f}%")
                print(f"     理由: {stock['reason']}")
        
        print()
        
        # 板块表现
        if sector_analysis and 'board_analysis' in sector_analysis:
            print("🏭 板块表现 (前5名):")
            board_analysis = sector_analysis['board_analysis']
            for i, (board, data) in enumerate(board_analysis.head(5).iterrows(), 1):
                print(f"  {i}. {board}: 平均涨幅 {data['avg_change']:.2f}%, 股票数 {data['stock_count']}")
        
        print()
        
        # 市场展望
        print("🔮 市场展望:")
        print(f"  {recommendations['market_outlook']}")
        
        print()
        
        # 投资建议
        print("💡 投资建议:")
        for suggestion in recommendations['strategy_suggestions']:
            print(f"  • {suggestion}")
        
        print()
        
        # 风险提示
        print("⚠️ 风险提示:")
        for warning in recommendations['risk_warnings']:
            print(f"  • {warning}")
        
        print("=" * 80)
        
    except Exception as e:
        logger.error(f"❌ 显示分析摘要失败: {e}")


def main():
    """主函数"""
    logger.info("🚀 A股全市场分析系统")
    logger.info("=" * 80)
    
    try:
        # 1. 初始化股票代码库
        logger.info("🎯 步骤1: 初始化股票代码库")
        universe = initialize_stock_universe()
        if not universe:
            return False
        
        # 2. 全市场分析
        logger.info("\n🎯 步骤2: 全市场分析")
        analysis_results = analyze_full_market(universe, sample_size=1000)
        if not analysis_results:
            return False
        
        # 3. 板块分析
        logger.info("\n🎯 步骤3: 板块分析")
        sector_analysis = generate_sector_analysis(universe, analysis_results['market_data'])
        
        # 4. 生成投资建议
        logger.info("\n🎯 步骤4: 生成投资建议")
        recommendations = generate_investment_recommendations(analysis_results, sector_analysis)
        if not recommendations:
            return False
        
        # 5. 保存结果
        logger.info("\n🎯 步骤5: 保存分析结果")
        output_dir = save_analysis_results(analysis_results, sector_analysis, recommendations)
        
        # 6. 显示摘要
        logger.info("\n🎯 步骤6: 显示分析摘要")
        display_analysis_summary(analysis_results, sector_analysis, recommendations)
        
        logger.success("🎉 A股全市场分析完成！")
        
        if output_dir:
            logger.info(f"\n📁 详细结果已保存到: {output_dir}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 全市场分析异常: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
