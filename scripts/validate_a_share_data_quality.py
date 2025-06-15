#!/usr/bin/env python3
"""
A股数据质量验证脚本
验证导入的A股生产数据的完整性、准确性和一致性
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
from src.utils.stock_filter import StockFilter

logger = get_logger("validate_a_share_data_quality")


def validate_data_completeness(data: pd.DataFrame) -> dict:
    """验证数据完整性"""
    logger.info("🔍 验证数据完整性")
    
    try:
        results = {
            'total_records': len(data),
            'unique_stocks': data['ts_code'].nunique(),
            'date_range': {
                'start': data['trade_date'].min(),
                'end': data['trade_date'].max()
            },
            'missing_values': {},
            'completeness_score': 0
        }
        
        # 检查关键字段的缺失值
        key_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
        
        for col in key_columns:
            if col in data.columns:
                missing_count = data[col].isnull().sum()
                missing_pct = (missing_count / len(data)) * 100
                results['missing_values'][col] = {
                    'count': missing_count,
                    'percentage': missing_pct
                }
        
        # 计算完整性评分
        total_missing = sum(item['count'] for item in results['missing_values'].values())
        total_possible = len(data) * len(key_columns)
        results['completeness_score'] = ((total_possible - total_missing) / total_possible) * 100
        
        logger.success(f"✅ 数据完整性验证完成")
        logger.info(f"总记录数: {results['total_records']}")
        logger.info(f"股票数量: {results['unique_stocks']}")
        logger.info(f"日期范围: {results['date_range']['start']} 到 {results['date_range']['end']}")
        logger.info(f"完整性评分: {results['completeness_score']:.2f}%")
        
        # 显示缺失值情况
        for col, missing_info in results['missing_values'].items():
            if missing_info['count'] > 0:
                logger.warning(f"⚠️ {col}: 缺失 {missing_info['count']} 个值 ({missing_info['percentage']:.2f}%)")
            else:
                logger.info(f"✅ {col}: 无缺失值")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ 数据完整性验证失败: {e}")
        return {}


def validate_data_accuracy(data: pd.DataFrame) -> dict:
    """验证数据准确性"""
    logger.info("🎯 验证数据准确性")
    
    try:
        results = {
            'price_logic_errors': 0,
            'volume_anomalies': 0,
            'pct_change_errors': 0,
            'date_format_errors': 0,
            'accuracy_score': 0
        }
        
        total_errors = 0
        total_checks = 0
        
        # 1. 价格逻辑检查
        logger.info("检查价格逻辑...")
        price_logic_mask = (
            (data['high'] >= data['open']) & 
            (data['high'] >= data['close']) & 
            (data['low'] <= data['open']) & 
            (data['low'] <= data['close']) &
            (data['high'] >= data['low'])
        )
        price_errors = (~price_logic_mask).sum()
        results['price_logic_errors'] = price_errors
        total_errors += price_errors
        total_checks += len(data)
        
        if price_errors > 0:
            logger.warning(f"⚠️ 发现 {price_errors} 条价格逻辑错误")
        else:
            logger.success("✅ 价格逻辑检查通过")
        
        # 2. 成交量异常检查
        logger.info("检查成交量异常...")
        volume_anomalies = (data['vol'] < 0).sum()
        results['volume_anomalies'] = volume_anomalies
        total_errors += volume_anomalies
        total_checks += len(data)
        
        if volume_anomalies > 0:
            logger.warning(f"⚠️ 发现 {volume_anomalies} 条负成交量")
        else:
            logger.success("✅ 成交量检查通过")
        
        # 3. 涨跌幅计算检查
        logger.info("检查涨跌幅计算...")
        if 'pct_chg' in data.columns and 'close' in data.columns and 'pre_close' in data.columns:
            calculated_pct_chg = ((data['close'] - data['pre_close']) / data['pre_close'] * 100).round(2)
            pct_chg_errors = (abs(data['pct_chg'] - calculated_pct_chg) > 0.1).sum()
            results['pct_change_errors'] = pct_chg_errors
            total_errors += pct_chg_errors
            total_checks += len(data)
            
            if pct_chg_errors > 0:
                logger.warning(f"⚠️ 发现 {pct_chg_errors} 条涨跌幅计算错误")
            else:
                logger.success("✅ 涨跌幅计算检查通过")
        
        # 4. 日期格式检查
        logger.info("检查日期格式...")
        try:
            pd.to_datetime(data['trade_date'])
            logger.success("✅ 日期格式检查通过")
        except:
            results['date_format_errors'] = len(data)
            total_errors += len(data)
            total_checks += len(data)
            logger.error("❌ 日期格式错误")
        
        # 计算准确性评分
        if total_checks > 0:
            results['accuracy_score'] = ((total_checks - total_errors) / total_checks) * 100
        
        logger.success(f"✅ 数据准确性验证完成")
        logger.info(f"准确性评分: {results['accuracy_score']:.2f}%")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ 数据准确性验证失败: {e}")
        return {}


def validate_a_share_characteristics(data: pd.DataFrame) -> dict:
    """验证A股市场特征"""
    logger.info("🏛️ 验证A股市场特征")
    
    try:
        results = {
            'limit_violations': 0,
            'trading_day_consistency': True,
            'market_distribution': {},
            'industry_coverage': {},
            'a_share_compliance_score': 0
        }
        
        # 1. 涨跌停限制检查
        logger.info("检查涨跌停限制...")
        if 'pct_chg' in data.columns:
            # 普通股票±10%，科创板创业板±20%
            gem_stocks = data['ts_code'].str.startswith(('300', '688'))
            
            # 普通股票涨跌停检查
            normal_stocks = ~gem_stocks
            normal_violations = (
                (data[normal_stocks]['pct_chg'] > 10.1) | 
                (data[normal_stocks]['pct_chg'] < -10.1)
            ).sum()
            
            # 科创板创业板涨跌停检查
            gem_violations = (
                (data[gem_stocks]['pct_chg'] > 20.1) | 
                (data[gem_stocks]['pct_chg'] < -20.1)
            ).sum()
            
            results['limit_violations'] = normal_violations + gem_violations
            
            if results['limit_violations'] > 0:
                logger.warning(f"⚠️ 发现 {results['limit_violations']} 条涨跌停违规")
            else:
                logger.success("✅ 涨跌停限制检查通过")
        
        # 2. 市场分布检查
        logger.info("检查市场分布...")
        if 'ts_code' in data.columns:
            sz_count = data['ts_code'].str.endswith('.SZ').sum()
            sh_count = data['ts_code'].str.endswith('.SH').sum()
            other_count = len(data) - sz_count - sh_count
            
            results['market_distribution'] = {
                'SZ': sz_count,
                'SH': sh_count,
                'Other': other_count
            }
            
            logger.info(f"市场分布: 深交所 {sz_count}, 上交所 {sh_count}, 其他 {other_count}")
            
            if other_count > 0:
                logger.warning(f"⚠️ 发现 {other_count} 条非沪深股票数据")
        
        # 3. 行业覆盖检查
        logger.info("检查行业覆盖...")
        if 'industry' in data.columns:
            industry_counts = data['industry'].value_counts()
            results['industry_coverage'] = industry_counts.to_dict()
            
            logger.info(f"行业覆盖: {len(industry_counts)} 个行业")
            for industry, count in industry_counts.head().items():
                logger.info(f"  {industry}: {count} 条记录")
        
        # 4. 股票代码格式检查
        logger.info("检查股票代码格式...")
        stock_filter = StockFilter()
        valid_codes = 0
        
        for code in data['ts_code'].unique():
            validation = stock_filter.validate_stock_code(code)
            if validation['is_a_share']:
                valid_codes += 1
        
        code_compliance = (valid_codes / data['ts_code'].nunique()) * 100
        
        # 计算A股合规性评分
        compliance_factors = [
            (results['limit_violations'] == 0, 30),  # 涨跌停合规
            (results['market_distribution']['Other'] == 0, 25),  # 市场合规
            (code_compliance > 90, 25),  # 代码格式合规
            (len(results['industry_coverage']) > 5, 20)  # 行业覆盖
        ]
        
        results['a_share_compliance_score'] = sum(
            weight for condition, weight in compliance_factors if condition
        )
        
        logger.success(f"✅ A股市场特征验证完成")
        logger.info(f"A股合规性评分: {results['a_share_compliance_score']}/100")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ A股市场特征验证失败: {e}")
        return {}


def validate_t1_trading_suitability(data: pd.DataFrame) -> dict:
    """验证T+1交易适用性"""
    logger.info("⏰ 验证T+1交易适用性")
    
    try:
        results = {
            'liquidity_score': 0,
            'volatility_distribution': {},
            'gap_risk_assessment': {},
            't1_suitability_score': 0
        }
        
        # 1. 流动性评估
        logger.info("评估流动性...")
        if 'vol' in data.columns:
            avg_volume = data.groupby('ts_code')['vol'].mean()
            high_liquidity_stocks = (avg_volume > 1000000).sum()  # 日均成交量>100万
            total_stocks = len(avg_volume)
            
            results['liquidity_score'] = (high_liquidity_stocks / total_stocks) * 100
            logger.info(f"高流动性股票比例: {results['liquidity_score']:.1f}%")
        
        # 2. 波动率分布
        logger.info("分析波动率分布...")
        if 'pct_chg' in data.columns:
            volatility = data.groupby('ts_code')['pct_chg'].std()
            
            results['volatility_distribution'] = {
                'low_vol': (volatility < 3).sum(),      # 低波动 <3%
                'medium_vol': ((volatility >= 3) & (volatility < 6)).sum(),  # 中波动 3-6%
                'high_vol': (volatility >= 6).sum()     # 高波动 >6%
            }
            
            logger.info("波动率分布:")
            for vol_type, count in results['volatility_distribution'].items():
                logger.info(f"  {vol_type}: {count} 只股票")
        
        # 3. 跳空风险评估
        logger.info("评估跳空风险...")
        gap_risks = []
        
        for ts_code in data['ts_code'].unique():
            stock_data = data[data['ts_code'] == ts_code].sort_values('trade_date')
            if len(stock_data) > 1:
                stock_data = stock_data.copy()
                stock_data['gap'] = (
                    (stock_data['open'] - stock_data['close'].shift(1)) / 
                    stock_data['close'].shift(1) * 100
                ).abs()
                avg_gap = stock_data['gap'].mean()
                gap_risks.append(avg_gap)
        
        if gap_risks:
            results['gap_risk_assessment'] = {
                'avg_gap_risk': np.mean(gap_risks),
                'max_gap_risk': np.max(gap_risks),
                'low_gap_stocks': sum(1 for gap in gap_risks if gap < 1),  # 跳空<1%
                'high_gap_stocks': sum(1 for gap in gap_risks if gap > 3)  # 跳空>3%
            }
            
            logger.info(f"平均跳空风险: {results['gap_risk_assessment']['avg_gap_risk']:.2f}%")
            logger.info(f"低跳空风险股票: {results['gap_risk_assessment']['low_gap_stocks']} 只")
        
        # 4. T+1适用性评分
        suitability_factors = [
            (results['liquidity_score'] > 70, 30),  # 流动性
            (results['volatility_distribution'].get('low_vol', 0) > 5, 25),  # 低波动股票数量
            (results['gap_risk_assessment'].get('avg_gap_risk', 10) < 2, 25),  # 跳空风险
            (len(data['ts_code'].unique()) > 10, 20)  # 股票数量
        ]
        
        results['t1_suitability_score'] = sum(
            weight for condition, weight in suitability_factors if condition
        )
        
        logger.success(f"✅ T+1交易适用性验证完成")
        logger.info(f"T+1适用性评分: {results['t1_suitability_score']}/100")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ T+1交易适用性验证失败: {e}")
        return {}


def generate_quality_report(completeness, accuracy, a_share_features, t1_suitability):
    """生成数据质量报告"""
    logger.info("📋 生成数据质量报告")
    
    try:
        # 计算综合评分
        scores = [
            completeness.get('completeness_score', 0),
            accuracy.get('accuracy_score', 0),
            a_share_features.get('a_share_compliance_score', 0),
            t1_suitability.get('t1_suitability_score', 0)
        ]
        
        overall_score = sum(scores) / len(scores)
        
        # 生成报告
        report = []
        report.append("=" * 80)
        report.append("A股数据质量验证报告")
        report.append("=" * 80)
        report.append(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # 综合评分
        report.append(f"📊 综合质量评分: {overall_score:.1f}/100")
        report.append("")
        
        # 详细评分
        report.append("📋 详细评分:")
        report.append(f"  数据完整性: {completeness.get('completeness_score', 0):.1f}/100")
        report.append(f"  数据准确性: {accuracy.get('accuracy_score', 0):.1f}/100")
        report.append(f"  A股合规性: {a_share_features.get('a_share_compliance_score', 0):.1f}/100")
        report.append(f"  T+1适用性: {t1_suitability.get('t1_suitability_score', 0):.1f}/100")
        report.append("")
        
        # 数据概况
        report.append("📈 数据概况:")
        report.append(f"  总记录数: {completeness.get('total_records', 0):,}")
        report.append(f"  股票数量: {completeness.get('unique_stocks', 0)}")
        
        date_range = completeness.get('date_range', {})
        if date_range:
            report.append(f"  日期范围: {date_range.get('start', 'N/A')} 到 {date_range.get('end', 'N/A')}")
        
        # 质量问题
        report.append("")
        report.append("⚠️ 发现的问题:")
        
        issues = []
        if accuracy.get('price_logic_errors', 0) > 0:
            issues.append(f"价格逻辑错误: {accuracy['price_logic_errors']} 条")
        if accuracy.get('volume_anomalies', 0) > 0:
            issues.append(f"成交量异常: {accuracy['volume_anomalies']} 条")
        if a_share_features.get('limit_violations', 0) > 0:
            issues.append(f"涨跌停违规: {a_share_features['limit_violations']} 条")
        
        if issues:
            for issue in issues:
                report.append(f"  • {issue}")
        else:
            report.append("  ✅ 未发现重大质量问题")
        
        # 建议
        report.append("")
        report.append("💡 建议:")
        
        if overall_score >= 90:
            report.append("  ✅ 数据质量优秀，可以用于生产环境")
        elif overall_score >= 80:
            report.append("  ⚠️ 数据质量良好，建议修复发现的问题")
        elif overall_score >= 70:
            report.append("  ⚠️ 数据质量一般，需要改进数据源或清洗流程")
        else:
            report.append("  ❌ 数据质量较差，不建议用于生产环境")
        
        report.append("")
        report.append("🏛️ A股T+1交易特点:")
        report.append("  • 当日买入次日才能卖出")
        report.append("  • 涨跌停限制保护投资者")
        report.append("  • 需要关注隔夜风险")
        report.append("  • 流动性是关键考虑因素")
        
        report.append("=" * 80)
        
        # 保存报告
        report_content = "\n".join(report)
        
        # 输出到控制台
        print("\n" + report_content)
        
        # 保存到文件
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        logs_dir = project_root / 'logs' / 'production'
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = logs_dir / f'a_share_data_quality_report_{timestamp}.txt'
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.success(f"✅ 数据质量报告已保存: {report_file}")
        
        return overall_score >= 80  # 80分以上认为质量合格
        
    except Exception as e:
        logger.error(f"❌ 生成数据质量报告失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("🔍 A股数据质量验证系统")
    logger.info("=" * 80)
    
    try:
        # 加载数据
        data_file = project_root / 'data' / 'production' / 'a_share' / 'a_share_daily_quotes.csv'
        
        if not data_file.exists():
            logger.error(f"❌ 数据文件不存在: {data_file}")
            return False
        
        logger.info(f"加载数据文件: {data_file}")
        data = pd.read_csv(data_file)
        
        # 执行验证
        logger.info("\n🎯 开始数据质量验证...")
        
        # 1. 完整性验证
        logger.info("\n📊 步骤1: 数据完整性验证")
        completeness = validate_data_completeness(data)
        
        # 2. 准确性验证
        logger.info("\n🎯 步骤2: 数据准确性验证")
        accuracy = validate_data_accuracy(data)
        
        # 3. A股特征验证
        logger.info("\n🏛️ 步骤3: A股市场特征验证")
        a_share_features = validate_a_share_characteristics(data)
        
        # 4. T+1适用性验证
        logger.info("\n⏰ 步骤4: T+1交易适用性验证")
        t1_suitability = validate_t1_trading_suitability(data)
        
        # 5. 生成质量报告
        logger.info("\n📋 步骤5: 生成数据质量报告")
        quality_passed = generate_quality_report(
            completeness, accuracy, a_share_features, t1_suitability
        )
        
        if quality_passed:
            logger.success("🎉 A股数据质量验证通过！")
            logger.info("✅ 数据已准备好用于生产环境")
        else:
            logger.warning("⚠️ 数据质量需要改进")
            logger.info("🔧 请根据报告建议优化数据质量")
        
        return quality_passed
        
    except Exception as e:
        logger.error(f"❌ 数据质量验证异常: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
