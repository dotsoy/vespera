-- 启明星项目 PostgreSQL 初始化脚本

-- 创建扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- 创建股票基础信息表
CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code VARCHAR(20) PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    name VARCHAR(50) NOT NULL,
    area VARCHAR(20),
    industry VARCHAR(50),
    market VARCHAR(20),
    list_date DATE,
    is_hs VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建股票日线行情表
CREATE TABLE IF NOT EXISTS stock_daily_quotes (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    open_price DECIMAL(10,3),
    high_price DECIMAL(10,3),
    low_price DECIMAL(10,3),
    close_price DECIMAL(10,3),
    pre_close DECIMAL(10,3),
    change_amount DECIMAL(10,3),
    pct_chg DECIMAL(8,3),
    vol BIGINT,
    amount DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, trade_date)
);

-- 创建资金流向表
CREATE TABLE IF NOT EXISTS money_flow_daily (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    buy_sm_vol BIGINT,
    buy_sm_amount DECIMAL(15,2),
    sell_sm_vol BIGINT,
    sell_sm_amount DECIMAL(15,2),
    buy_md_vol BIGINT,
    buy_md_amount DECIMAL(15,2),
    sell_md_vol BIGINT,
    sell_md_amount DECIMAL(15,2),
    buy_lg_vol BIGINT,
    buy_lg_amount DECIMAL(15,2),
    sell_lg_vol BIGINT,
    sell_lg_amount DECIMAL(15,2),
    buy_elg_vol BIGINT,
    buy_elg_amount DECIMAL(15,2),
    sell_elg_vol BIGINT,
    sell_elg_amount DECIMAL(15,2),
    net_mf_vol BIGINT,
    net_mf_amount DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, trade_date)
);

-- 创建技术分析结果表
CREATE TABLE IF NOT EXISTS technical_daily_profiles (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    trend_score DECIMAL(5,3),
    momentum_score DECIMAL(5,3),
    volume_health_score DECIMAL(5,3),
    pattern_score DECIMAL(5,3),
    support_level DECIMAL(10,3),
    resistance_level DECIMAL(10,3),
    key_patterns JSONB,
    technical_indicators JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, trade_date)
);

-- 创建资金流分析结果表
CREATE TABLE IF NOT EXISTS capital_flow_profiles (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    main_force_score DECIMAL(5,3),
    retail_sentiment_score DECIMAL(5,3),
    institutional_activity DECIMAL(5,3),
    flow_consistency DECIMAL(5,3),
    volume_price_correlation DECIMAL(5,3),
    flow_analysis JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, trade_date)
);

-- 创建基本面分析结果表
CREATE TABLE IF NOT EXISTS fundamental_profiles (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    catalyst_score DECIMAL(5,3),
    news_sentiment DECIMAL(5,3),
    announcement_impact DECIMAL(5,3),
    industry_momentum DECIMAL(5,3),
    fundamental_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, trade_date)
);

-- 创建市场情绪分析结果表
CREATE TABLE IF NOT EXISTS sentiment_profiles (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    social_sentiment DECIMAL(5,3),
    news_sentiment DECIMAL(5,3),
    analyst_sentiment DECIMAL(5,3),
    market_attention DECIMAL(5,3),
    sentiment_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, trade_date)
);

-- 创建宏观环境分析结果表
CREATE TABLE IF NOT EXISTS macro_profiles (
    id SERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    market_regime DECIMAL(5,3),
    sector_rotation DECIMAL(5,3),
    risk_appetite DECIMAL(5,3),
    liquidity_condition DECIMAL(5,3),
    macro_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date)
);

-- 创建最终交易信号表
CREATE TABLE IF NOT EXISTS trading_signals (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    signal_type VARCHAR(20) NOT NULL, -- 'BUY', 'SELL', 'HOLD'
    confidence_score DECIMAL(5,3) NOT NULL,
    technical_score DECIMAL(5,3),
    capital_score DECIMAL(5,3),
    fundamental_score DECIMAL(5,3),
    sentiment_score DECIMAL(5,3),
    macro_score DECIMAL(5,3),
    entry_price DECIMAL(10,3),
    stop_loss DECIMAL(10,3),
    target_price DECIMAL(10,3),
    risk_reward_ratio DECIMAL(5,2),
    position_size DECIMAL(5,3),
    signal_reason TEXT,
    signal_data JSONB,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, trade_date, signal_type)
);

-- 创建系统日志表
CREATE TABLE IF NOT EXISTS system_logs (
    id SERIAL PRIMARY KEY,
    log_level VARCHAR(10) NOT NULL,
    module VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_stock_daily_quotes_ts_code_date ON stock_daily_quotes(ts_code, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_money_flow_daily_ts_code_date ON money_flow_daily(ts_code, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_technical_profiles_ts_code_date ON technical_daily_profiles(ts_code, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_capital_profiles_ts_code_date ON capital_flow_profiles(ts_code, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_trading_signals_date_score ON trading_signals(trade_date DESC, confidence_score DESC);
CREATE INDEX IF NOT EXISTS idx_trading_signals_active ON trading_signals(is_active, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_system_logs_created_at ON system_logs(created_at DESC);

-- 创建更新时间触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为需要的表添加更新时间触发器
CREATE TRIGGER update_stock_basic_updated_at BEFORE UPDATE ON stock_basic
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
