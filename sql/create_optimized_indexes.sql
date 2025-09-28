-- 数据库索引优化SQL脚本
-- 为Vespera量化投资分析平台创建优化索引

-- ============================================================================
-- 日线数据表索引 (daily_quotes)
-- ============================================================================

-- 股票代码和交易日期复合索引（最重要的索引）
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_quotes_ts_code_date 
ON daily_quotes(ts_code, trade_date);

-- 交易日期索引，用于按日期范围查询
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_quotes_date 
ON daily_quotes(trade_date);

-- 成交量索引，用于成交量筛选
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_quotes_volume 
ON daily_quotes(volume) WHERE volume > 0;

-- 收盘价索引，用于价格筛选
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_quotes_close_price 
ON daily_quotes(close);

-- 涨跌幅索引，用于涨跌幅筛选
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_quotes_pct_chg 
ON daily_quotes(pct_chg);

-- 市值索引（如果有市值字段）
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_quotes_total_mv 
ON daily_quotes(total_mv) WHERE total_mv IS NOT NULL;

-- ============================================================================
-- 资金流向表索引 (capital_flow_daily)
-- ============================================================================

-- 股票代码和交易日期复合索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_capital_flow_ts_code_date 
ON capital_flow_daily(ts_code, trade_date);

-- 净流入金额索引，用于资金流向分析
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_capital_flow_net_amount 
ON capital_flow_daily(net_amount);

-- 主力净流入索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_capital_flow_main_net_inflow 
ON capital_flow_daily(main_net_inflow) WHERE main_net_inflow IS NOT NULL;

-- 超大单净流入索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_capital_flow_super_net_inflow 
ON capital_flow_daily(super_net_inflow) WHERE super_net_inflow IS NOT NULL;

-- 大单净流入索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_capital_flow_large_net_inflow 
ON capital_flow_daily(large_net_inflow) WHERE large_net_inflow IS NOT NULL;

-- ============================================================================
-- 股票基本信息表索引 (stock_basic)
-- ============================================================================

-- 股票代码唯一索引（主键）
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_basic_ts_code 
ON stock_basic(ts_code);

-- 股票名称索引，用于名称搜索
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_basic_name 
ON stock_basic(name);

-- 行业索引，用于行业分析
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_basic_industry 
ON stock_basic(industry);

-- 市场索引（主板、创业板等）
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_basic_market 
ON stock_basic(market);

-- 上市状态索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_basic_list_status 
ON stock_basic(list_status);

-- 上市日期索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_basic_list_date 
ON stock_basic(list_date);

-- ============================================================================
-- 技术指标表索引 (technical_indicators)
-- ============================================================================

-- 股票代码和交易日期复合索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_technical_ts_code_date 
ON technical_indicators(ts_code, trade_date);

-- RSI指标索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_technical_rsi 
ON technical_indicators(rsi) WHERE rsi IS NOT NULL;

-- MACD指标索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_technical_macd 
ON technical_indicators(macd) WHERE macd IS NOT NULL;

-- KDJ指标索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_technical_kdj_k 
ON technical_indicators(kdj_k) WHERE kdj_k IS NOT NULL;

-- 布林带指标索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_technical_boll_upper 
ON technical_indicators(boll_upper) WHERE boll_upper IS NOT NULL;

-- 移动平均线索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_technical_ma5 
ON technical_indicators(ma5) WHERE ma5 IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_technical_ma20 
ON technical_indicators(ma20) WHERE ma20 IS NOT NULL;

-- ============================================================================
-- 策略信号表索引 (strategy_signals)
-- ============================================================================

-- 股票代码和信号日期复合索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signals_ts_code_date 
ON strategy_signals(ts_code, signal_date);

-- 信号日期索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signals_date 
ON strategy_signals(signal_date);

-- 确定性得分索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signals_score 
ON strategy_signals(conviction_score);

-- 信号等级索引（S级、A级）
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signals_class 
ON strategy_signals(signal_class);

-- 策略名称索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signals_strategy 
ON strategy_signals(strategy_name);

-- 信号状态索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signals_status 
ON strategy_signals(status) WHERE status IS NOT NULL;

-- ============================================================================
-- 回测结果表索引 (backtest_results)
-- ============================================================================

-- 策略名称和回测日期复合索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_backtest_strategy_date 
ON backtest_results(strategy_name, backtest_date);

-- 股票代码索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_backtest_ts_code 
ON backtest_results(ts_code);

-- 收益率索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_backtest_return 
ON backtest_results(total_return) WHERE total_return IS NOT NULL;

-- 最大回撤索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_backtest_max_drawdown 
ON backtest_results(max_drawdown) WHERE max_drawdown IS NOT NULL;

-- ============================================================================
-- 组合索引（复合查询优化）
-- ============================================================================

-- 日线数据：股票代码、日期、成交量复合索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_quotes_ts_code_date_volume 
ON daily_quotes(ts_code, trade_date, volume);

-- 资金流向：股票代码、日期、净流入复合索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_capital_flow_ts_code_date_net 
ON capital_flow_daily(ts_code, trade_date, net_amount);

-- 技术指标：股票代码、日期、RSI复合索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_technical_ts_code_date_rsi 
ON technical_indicators(ts_code, trade_date, rsi);

-- 策略信号：日期、得分、等级复合索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signals_date_score_class 
ON strategy_signals(signal_date, conviction_score, signal_class);

-- ============================================================================
-- 部分索引（条件索引）
-- ============================================================================

-- 只为活跃股票创建索引（成交量大于0）
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_quotes_active_stocks 
ON daily_quotes(ts_code, trade_date) 
WHERE volume > 0;

-- 只为有效信号创建索引（得分大于70）
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signals_valid_signals 
ON strategy_signals(ts_code, signal_date, conviction_score) 
WHERE conviction_score >= 70;

-- 只为主力净流入为正的记录创建索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_capital_flow_positive_inflow 
ON capital_flow_daily(ts_code, trade_date, main_net_inflow) 
WHERE main_net_inflow > 0;

-- ============================================================================
-- 函数索引（表达式索引）
-- ============================================================================

-- 交易日期的年月索引，用于按月统计
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_quotes_year_month 
ON daily_quotes(EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date));

-- 股票代码前缀索引，用于市场分类
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_basic_market_prefix 
ON stock_basic(LEFT(ts_code, 3));

-- 价格变化百分比的绝对值索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_quotes_abs_pct_chg 
ON daily_quotes(ABS(pct_chg)) WHERE pct_chg IS NOT NULL;

-- ============================================================================
-- 全文搜索索引
-- ============================================================================

-- 股票名称全文搜索索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_basic_name_fulltext 
ON stock_basic USING gin(to_tsvector('simple', name));

-- 行业名称全文搜索索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_basic_industry_fulltext 
ON stock_basic USING gin(to_tsvector('simple', industry));

-- ============================================================================
-- 统计信息更新
-- ============================================================================

-- 更新表统计信息以优化查询计划
ANALYZE daily_quotes;
ANALYZE capital_flow_daily;
ANALYZE stock_basic;
ANALYZE technical_indicators;
ANALYZE strategy_signals;
ANALYZE backtest_results;

-- ============================================================================
-- 索引使用情况查询
-- ============================================================================

-- 查看索引使用情况的SQL（用于监控）
/*
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes 
ORDER BY idx_scan DESC;
*/

-- 查看表大小和索引大小
/*
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size(tablename::regclass)) as total_size,
    pg_size_pretty(pg_relation_size(tablename::regclass)) as table_size,
    pg_size_pretty(pg_total_relation_size(tablename::regclass) - pg_relation_size(tablename::regclass)) as index_size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(tablename::regclass) DESC;
*/

-- 查看未使用的索引
/*
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan
FROM pg_stat_user_indexes 
WHERE idx_scan = 0
AND schemaname = 'public';
*/