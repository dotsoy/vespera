--
-- PostgreSQL database dump
--

-- Dumped from database version 15.13
-- Dumped by pg_dump version 15.13

-- Started on 2025-06-22 23:42:17 UTC

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

DROP INDEX public.idx_trading_signals_date_score;
DROP INDEX public.idx_trading_signals_active;
DROP INDEX public.idx_technical_profiles_ts_code_date;
DROP INDEX public.idx_system_logs_created_at;
DROP INDEX public.idx_stock_daily_quotes_ts_code_date;
DROP INDEX public.idx_money_flow_daily_ts_code_date;
DROP INDEX public.idx_data_update_log_time;
DROP INDEX public.idx_data_update_log_status;
DROP INDEX public.idx_data_update_log_date;
DROP INDEX public.idx_capital_profiles_ts_code_date;
DROP INDEX public.idx_capital_flow_ts_code_date;
DROP INDEX public.idx_capital_flow_ts_code;
DROP INDEX public.idx_capital_flow_trade_date;
DROP INDEX public.capital_flow_daily_trade_date_ts_code_key;
ALTER TABLE ONLY public.trading_signals DROP CONSTRAINT trading_signals_ts_code_trade_date_signal_type_key;
ALTER TABLE ONLY public.trading_signals DROP CONSTRAINT trading_signals_pkey;
ALTER TABLE ONLY public.technical_indicators_daily DROP CONSTRAINT technical_indicators_daily_ts_code_trade_date_key;
ALTER TABLE ONLY public.technical_indicators_daily DROP CONSTRAINT technical_indicators_daily_pkey;
ALTER TABLE ONLY public.technical_daily_profiles DROP CONSTRAINT technical_daily_profiles_ts_code_trade_date_key;
ALTER TABLE ONLY public.technical_daily_profiles DROP CONSTRAINT technical_daily_profiles_pkey;
ALTER TABLE ONLY public.system_logs DROP CONSTRAINT system_logs_pkey;
ALTER TABLE ONLY public.stock_daily_quotes DROP CONSTRAINT stock_daily_quotes_ts_code_trade_date_key;
ALTER TABLE ONLY public.stock_daily_quotes DROP CONSTRAINT stock_daily_quotes_pkey;
ALTER TABLE ONLY public.sentiment_profiles DROP CONSTRAINT sentiment_profiles_ts_code_trade_date_key;
ALTER TABLE ONLY public.sentiment_profiles DROP CONSTRAINT sentiment_profiles_pkey;
ALTER TABLE ONLY public.money_flow_daily DROP CONSTRAINT money_flow_daily_ts_code_trade_date_key;
ALTER TABLE ONLY public.money_flow_daily DROP CONSTRAINT money_flow_daily_pkey;
ALTER TABLE ONLY public.market_sentiment_daily DROP CONSTRAINT market_sentiment_daily_ts_code_trade_date_key;
ALTER TABLE ONLY public.market_sentiment_daily DROP CONSTRAINT market_sentiment_daily_pkey;
ALTER TABLE ONLY public.macro_profiles DROP CONSTRAINT macro_profiles_trade_date_key;
ALTER TABLE ONLY public.macro_profiles DROP CONSTRAINT macro_profiles_pkey;
ALTER TABLE ONLY public.fundamental_profiles DROP CONSTRAINT fundamental_profiles_ts_code_trade_date_key;
ALTER TABLE ONLY public.fundamental_profiles DROP CONSTRAINT fundamental_profiles_pkey;
ALTER TABLE ONLY public.financial_indicators_quarterly DROP CONSTRAINT financial_indicators_quarterly_ts_code_report_date_key;
ALTER TABLE ONLY public.financial_indicators_quarterly DROP CONSTRAINT financial_indicators_quarterly_pkey;
ALTER TABLE ONLY public.data_update_log DROP CONSTRAINT data_update_log_pkey;
ALTER TABLE ONLY public.capital_flow_profiles DROP CONSTRAINT capital_flow_profiles_ts_code_trade_date_key;
ALTER TABLE ONLY public.capital_flow_profiles DROP CONSTRAINT capital_flow_profiles_pkey;
ALTER TABLE ONLY public.capital_flow_daily DROP CONSTRAINT capital_flow_daily_ts_code_trade_date_key;
ALTER TABLE ONLY public.capital_flow_daily DROP CONSTRAINT capital_flow_daily_pkey;
ALTER TABLE public.trading_signals ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.technical_indicators_daily ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.technical_daily_profiles ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.system_logs ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.stock_daily_quotes ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.sentiment_profiles ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.money_flow_daily ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.market_sentiment_daily ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.macro_profiles ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.fundamental_profiles ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.financial_indicators_quarterly ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.data_update_log ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.capital_flow_profiles ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.capital_flow_daily ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE public.trading_signals_id_seq;
DROP TABLE public.trading_signals;
DROP SEQUENCE public.technical_indicators_daily_id_seq;
DROP TABLE public.technical_indicators_daily;
DROP SEQUENCE public.technical_daily_profiles_id_seq;
DROP TABLE public.technical_daily_profiles;
DROP SEQUENCE public.system_logs_id_seq;
DROP TABLE public.system_logs;
DROP SEQUENCE public.stock_daily_quotes_id_seq;
DROP TABLE public.stock_daily_quotes;
DROP TABLE public.stock_basic;
DROP SEQUENCE public.sentiment_profiles_id_seq;
DROP TABLE public.sentiment_profiles;
DROP SEQUENCE public.money_flow_daily_id_seq;
DROP TABLE public.money_flow_daily;
DROP SEQUENCE public.market_sentiment_daily_id_seq;
DROP TABLE public.market_sentiment_daily;
DROP SEQUENCE public.macro_profiles_id_seq;
DROP TABLE public.macro_profiles;
DROP SEQUENCE public.fundamental_profiles_id_seq;
DROP TABLE public.fundamental_profiles;
DROP SEQUENCE public.financial_indicators_quarterly_id_seq;
DROP TABLE public.financial_indicators_quarterly;
DROP SEQUENCE public.data_update_log_id_seq;
DROP TABLE public.data_update_log;
DROP SEQUENCE public.capital_flow_profiles_id_seq;
DROP TABLE public.capital_flow_profiles;
DROP SEQUENCE public.capital_flow_daily_id_seq;
DROP TABLE public.capital_flow_daily;
DROP FUNCTION public.update_updated_at_column();
DROP EXTENSION "uuid-ossp";
DROP EXTENSION pg_trgm;
--
-- TOC entry 3 (class 3079 OID 16396)
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- TOC entry 3669 (class 0 OID 0)
-- Dependencies: 3
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- TOC entry 2 (class 3079 OID 16385)
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- TOC entry 3670 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- TOC entry 286 (class 1255 OID 16594)
-- Name: update_updated_at_column(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 238 (class 1259 OID 24806)
-- Name: capital_flow_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.capital_flow_daily (
    id integer NOT NULL,
    ts_code character varying(20) NOT NULL,
    trade_date date NOT NULL,
    main_net_inflow numeric(20,2),
    main_net_inflow_rate numeric(10,4),
    super_large_net_inflow numeric(20,2),
    super_large_net_inflow_rate numeric(10,4),
    large_net_inflow numeric(20,2),
    large_net_inflow_rate numeric(10,4),
    medium_net_inflow numeric(20,2),
    medium_net_inflow_rate numeric(10,4),
    small_net_inflow numeric(20,2),
    small_net_inflow_rate numeric(10,4),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    total_amount numeric(20,2)
);


--
-- TOC entry 237 (class 1259 OID 24805)
-- Name: capital_flow_daily_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.capital_flow_daily_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3671 (class 0 OID 0)
-- Dependencies: 237
-- Name: capital_flow_daily_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.capital_flow_daily_id_seq OWNED BY public.capital_flow_daily.id;


--
-- TOC entry 223 (class 1259 OID 16517)
-- Name: capital_flow_profiles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.capital_flow_profiles (
    id integer NOT NULL,
    ts_code character varying(20) NOT NULL,
    trade_date date NOT NULL,
    main_force_score numeric(5,3),
    retail_sentiment_score numeric(5,3),
    institutional_activity numeric(5,3),
    flow_consistency numeric(5,3),
    volume_price_correlation numeric(5,3),
    flow_analysis jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 222 (class 1259 OID 16516)
-- Name: capital_flow_profiles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.capital_flow_profiles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3672 (class 0 OID 0)
-- Dependencies: 222
-- Name: capital_flow_profiles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.capital_flow_profiles_id_seq OWNED BY public.capital_flow_profiles.id;


--
-- TOC entry 236 (class 1259 OID 24789)
-- Name: data_update_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.data_update_log (
    id integer NOT NULL,
    update_date date NOT NULL,
    update_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    status character varying(20) NOT NULL,
    message text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT data_update_log_status_check CHECK (((status)::text = ANY ((ARRAY['started'::character varying, 'completed'::character varying, 'failed'::character varying])::text[])))
);


--
-- TOC entry 235 (class 1259 OID 24788)
-- Name: data_update_log_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.data_update_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3673 (class 0 OID 0)
-- Dependencies: 235
-- Name: data_update_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.data_update_log_id_seq OWNED BY public.data_update_log.id;


--
-- TOC entry 244 (class 1259 OID 24842)
-- Name: financial_indicators_quarterly; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.financial_indicators_quarterly (
    id integer NOT NULL,
    ts_code character varying(20) NOT NULL,
    report_date date NOT NULL,
    pe_ratio numeric(10,4),
    pb_ratio numeric(10,4),
    ps_ratio numeric(10,4),
    roe numeric(10,4),
    roa numeric(10,4),
    debt_ratio numeric(10,4),
    current_ratio numeric(10,4),
    quick_ratio numeric(10,4),
    gross_margin numeric(10,4),
    net_margin numeric(10,4),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 243 (class 1259 OID 24841)
-- Name: financial_indicators_quarterly_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.financial_indicators_quarterly_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3674 (class 0 OID 0)
-- Dependencies: 243
-- Name: financial_indicators_quarterly_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.financial_indicators_quarterly_id_seq OWNED BY public.financial_indicators_quarterly.id;


--
-- TOC entry 225 (class 1259 OID 16529)
-- Name: fundamental_profiles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fundamental_profiles (
    id integer NOT NULL,
    ts_code character varying(20) NOT NULL,
    trade_date date NOT NULL,
    catalyst_score numeric(5,3),
    news_sentiment numeric(5,3),
    announcement_impact numeric(5,3),
    industry_momentum numeric(5,3),
    fundamental_data jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 224 (class 1259 OID 16528)
-- Name: fundamental_profiles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.fundamental_profiles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3675 (class 0 OID 0)
-- Dependencies: 224
-- Name: fundamental_profiles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.fundamental_profiles_id_seq OWNED BY public.fundamental_profiles.id;


--
-- TOC entry 229 (class 1259 OID 16553)
-- Name: macro_profiles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.macro_profiles (
    id integer NOT NULL,
    trade_date date NOT NULL,
    market_regime numeric(5,3),
    sector_rotation numeric(5,3),
    risk_appetite numeric(5,3),
    liquidity_condition numeric(5,3),
    macro_data jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 228 (class 1259 OID 16552)
-- Name: macro_profiles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.macro_profiles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3676 (class 0 OID 0)
-- Dependencies: 228
-- Name: macro_profiles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.macro_profiles_id_seq OWNED BY public.macro_profiles.id;


--
-- TOC entry 242 (class 1259 OID 24831)
-- Name: market_sentiment_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.market_sentiment_daily (
    id integer NOT NULL,
    ts_code character varying(20) NOT NULL,
    trade_date date NOT NULL,
    sentiment_score numeric(10,4),
    news_sentiment numeric(10,4),
    social_sentiment numeric(10,4),
    analyst_rating numeric(10,4),
    institutional_activity numeric(10,4),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 241 (class 1259 OID 24830)
-- Name: market_sentiment_daily_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.market_sentiment_daily_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3677 (class 0 OID 0)
-- Dependencies: 241
-- Name: market_sentiment_daily_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.market_sentiment_daily_id_seq OWNED BY public.market_sentiment_daily.id;


--
-- TOC entry 219 (class 1259 OID 16495)
-- Name: money_flow_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.money_flow_daily (
    id integer NOT NULL,
    ts_code character varying(20) NOT NULL,
    trade_date date NOT NULL,
    buy_sm_vol bigint,
    buy_sm_amount numeric(15,2),
    sell_sm_vol bigint,
    sell_sm_amount numeric(15,2),
    buy_md_vol bigint,
    buy_md_amount numeric(15,2),
    sell_md_vol bigint,
    sell_md_amount numeric(15,2),
    buy_lg_vol bigint,
    buy_lg_amount numeric(15,2),
    sell_lg_vol bigint,
    sell_lg_amount numeric(15,2),
    buy_elg_vol bigint,
    buy_elg_amount numeric(15,2),
    sell_elg_vol bigint,
    sell_elg_amount numeric(15,2),
    net_mf_vol bigint,
    net_mf_amount numeric(15,2),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 218 (class 1259 OID 16494)
-- Name: money_flow_daily_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.money_flow_daily_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3678 (class 0 OID 0)
-- Dependencies: 218
-- Name: money_flow_daily_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.money_flow_daily_id_seq OWNED BY public.money_flow_daily.id;


--
-- TOC entry 227 (class 1259 OID 16541)
-- Name: sentiment_profiles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sentiment_profiles (
    id integer NOT NULL,
    ts_code character varying(20) NOT NULL,
    trade_date date NOT NULL,
    social_sentiment numeric(5,3),
    news_sentiment numeric(5,3),
    analyst_sentiment numeric(5,3),
    market_attention numeric(5,3),
    sentiment_data jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 226 (class 1259 OID 16540)
-- Name: sentiment_profiles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.sentiment_profiles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3679 (class 0 OID 0)
-- Dependencies: 226
-- Name: sentiment_profiles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.sentiment_profiles_id_seq OWNED BY public.sentiment_profiles.id;


--
-- TOC entry 234 (class 1259 OID 16621)
-- Name: stock_basic; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stock_basic (
    ts_code character varying,
    name character varying,
    symbol character varying,
    market character varying,
    exchange character varying,
    list_status character varying,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 217 (class 1259 OID 16485)
-- Name: stock_daily_quotes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stock_daily_quotes (
    id integer NOT NULL,
    ts_code character varying(20) NOT NULL,
    trade_date date NOT NULL,
    open_price numeric(10,3),
    high_price numeric(10,3),
    low_price numeric(10,3),
    close_price numeric(10,3),
    pre_close numeric(10,3),
    change_amount numeric(10,3),
    pct_chg numeric(8,3),
    vol bigint,
    amount numeric(15,2),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 216 (class 1259 OID 16484)
-- Name: stock_daily_quotes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stock_daily_quotes_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3680 (class 0 OID 0)
-- Dependencies: 216
-- Name: stock_daily_quotes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stock_daily_quotes_id_seq OWNED BY public.stock_daily_quotes.id;


--
-- TOC entry 233 (class 1259 OID 16578)
-- Name: system_logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.system_logs (
    id integer NOT NULL,
    log_level character varying(10) NOT NULL,
    module character varying(50) NOT NULL,
    message text NOT NULL,
    details jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 232 (class 1259 OID 16577)
-- Name: system_logs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.system_logs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3681 (class 0 OID 0)
-- Dependencies: 232
-- Name: system_logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.system_logs_id_seq OWNED BY public.system_logs.id;


--
-- TOC entry 221 (class 1259 OID 16505)
-- Name: technical_daily_profiles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.technical_daily_profiles (
    id integer NOT NULL,
    ts_code character varying(20) NOT NULL,
    trade_date date NOT NULL,
    trend_score numeric(5,3),
    momentum_score numeric(5,3),
    volume_health_score numeric(5,3),
    pattern_score numeric(5,3),
    support_level numeric(10,3),
    resistance_level numeric(10,3),
    key_patterns jsonb,
    technical_indicators jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 220 (class 1259 OID 16504)
-- Name: technical_daily_profiles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.technical_daily_profiles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3682 (class 0 OID 0)
-- Dependencies: 220
-- Name: technical_daily_profiles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.technical_daily_profiles_id_seq OWNED BY public.technical_daily_profiles.id;


--
-- TOC entry 240 (class 1259 OID 24820)
-- Name: technical_indicators_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.technical_indicators_daily (
    id integer NOT NULL,
    ts_code character varying(20) NOT NULL,
    trade_date date NOT NULL,
    ma5 numeric(10,4),
    ma10 numeric(10,4),
    ma20 numeric(10,4),
    ma60 numeric(10,4),
    rsi numeric(10,4),
    macd numeric(10,4),
    macd_signal numeric(10,4),
    macd_hist numeric(10,4),
    bollinger_upper numeric(10,4),
    bollinger_middle numeric(10,4),
    bollinger_lower numeric(10,4),
    kdj_k numeric(10,4),
    kdj_d numeric(10,4),
    kdj_j numeric(10,4),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 239 (class 1259 OID 24819)
-- Name: technical_indicators_daily_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.technical_indicators_daily_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3683 (class 0 OID 0)
-- Dependencies: 239
-- Name: technical_indicators_daily_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.technical_indicators_daily_id_seq OWNED BY public.technical_indicators_daily.id;


--
-- TOC entry 231 (class 1259 OID 16565)
-- Name: trading_signals; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trading_signals (
    id integer NOT NULL,
    ts_code character varying(20) NOT NULL,
    trade_date date NOT NULL,
    signal_type character varying(20) NOT NULL,
    confidence_score numeric(5,3) NOT NULL,
    technical_score numeric(5,3),
    capital_score numeric(5,3),
    fundamental_score numeric(5,3),
    sentiment_score numeric(5,3),
    macro_score numeric(5,3),
    entry_price numeric(10,3),
    stop_loss numeric(10,3),
    target_price numeric(10,3),
    risk_reward_ratio numeric(5,2),
    position_size numeric(5,3),
    signal_reason text,
    signal_data jsonb,
    is_active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- TOC entry 230 (class 1259 OID 16564)
-- Name: trading_signals_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.trading_signals_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3684 (class 0 OID 0)
-- Dependencies: 230
-- Name: trading_signals_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.trading_signals_id_seq OWNED BY public.trading_signals.id;


--
-- TOC entry 3414 (class 2604 OID 24809)
-- Name: capital_flow_daily id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.capital_flow_daily ALTER COLUMN id SET DEFAULT nextval('public.capital_flow_daily_id_seq'::regclass);


--
-- TOC entry 3397 (class 2604 OID 16520)
-- Name: capital_flow_profiles id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.capital_flow_profiles ALTER COLUMN id SET DEFAULT nextval('public.capital_flow_profiles_id_seq'::regclass);


--
-- TOC entry 3411 (class 2604 OID 24792)
-- Name: data_update_log id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.data_update_log ALTER COLUMN id SET DEFAULT nextval('public.data_update_log_id_seq'::regclass);


--
-- TOC entry 3423 (class 2604 OID 24845)
-- Name: financial_indicators_quarterly id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.financial_indicators_quarterly ALTER COLUMN id SET DEFAULT nextval('public.financial_indicators_quarterly_id_seq'::regclass);


--
-- TOC entry 3399 (class 2604 OID 16532)
-- Name: fundamental_profiles id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fundamental_profiles ALTER COLUMN id SET DEFAULT nextval('public.fundamental_profiles_id_seq'::regclass);


--
-- TOC entry 3403 (class 2604 OID 16556)
-- Name: macro_profiles id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.macro_profiles ALTER COLUMN id SET DEFAULT nextval('public.macro_profiles_id_seq'::regclass);


--
-- TOC entry 3420 (class 2604 OID 24834)
-- Name: market_sentiment_daily id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_sentiment_daily ALTER COLUMN id SET DEFAULT nextval('public.market_sentiment_daily_id_seq'::regclass);


--
-- TOC entry 3393 (class 2604 OID 16498)
-- Name: money_flow_daily id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.money_flow_daily ALTER COLUMN id SET DEFAULT nextval('public.money_flow_daily_id_seq'::regclass);


--
-- TOC entry 3401 (class 2604 OID 16544)
-- Name: sentiment_profiles id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sentiment_profiles ALTER COLUMN id SET DEFAULT nextval('public.sentiment_profiles_id_seq'::regclass);


--
-- TOC entry 3390 (class 2604 OID 16488)
-- Name: stock_daily_quotes id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stock_daily_quotes ALTER COLUMN id SET DEFAULT nextval('public.stock_daily_quotes_id_seq'::regclass);


--
-- TOC entry 3408 (class 2604 OID 16581)
-- Name: system_logs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_logs ALTER COLUMN id SET DEFAULT nextval('public.system_logs_id_seq'::regclass);


--
-- TOC entry 3395 (class 2604 OID 16508)
-- Name: technical_daily_profiles id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.technical_daily_profiles ALTER COLUMN id SET DEFAULT nextval('public.technical_daily_profiles_id_seq'::regclass);


--
-- TOC entry 3417 (class 2604 OID 24823)
-- Name: technical_indicators_daily id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.technical_indicators_daily ALTER COLUMN id SET DEFAULT nextval('public.technical_indicators_daily_id_seq'::regclass);


--
-- TOC entry 3405 (class 2604 OID 16568)
-- Name: trading_signals id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trading_signals ALTER COLUMN id SET DEFAULT nextval('public.trading_signals_id_seq'::regclass);


--
-- TOC entry 3657 (class 0 OID 24806)
-- Dependencies: 238
-- Data for Name: capital_flow_daily; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.capital_flow_daily (id, ts_code, trade_date, main_net_inflow, main_net_inflow_rate, super_large_net_inflow, super_large_net_inflow_rate, large_net_inflow, large_net_inflow_rate, medium_net_inflow, medium_net_inflow_rate, small_net_inflow, small_net_inflow_rate, created_at, updated_at, total_amount) FROM stdin;
1	000001.SZ	2024-06-17	1000000.00	0.0500	500000.00	0.0250	500000.00	0.0250	-300000.00	-0.0150	-700000.00	-0.0350	2025-06-17 17:02:54.430453	2025-06-17 17:02:54.430453	\N
2	000002.SZ	2024-06-17	-500000.00	-0.0200	-200000.00	-0.0080	-300000.00	-0.0120	100000.00	0.0040	400000.00	0.0160	2025-06-17 17:02:54.430453	2025-06-17 17:02:54.430453	\N
3	600000.SH	2024-06-17	2000000.00	0.0800	1200000.00	0.0480	800000.00	0.0320	-600000.00	-0.0240	-1400000.00	-0.0560	2025-06-17 17:02:54.430453	2025-06-17 17:02:54.430453	\N
4	000001	2025-05-19	-803650.83	-0.0080	1165143.07	-0.0089	2352047.94	-0.0077	-768451.51	-0.0009	-355489.43	-0.0002	2025-06-17 17:06:58.650333	2025-06-17 17:06:58.650333	\N
5	000001	2025-05-20	1417748.71	0.0142	866885.74	0.0133	-2023897.43	0.0063	-701613.24	-0.0064	13520.50	-0.0045	2025-06-17 17:06:58.651535	2025-06-17 17:06:58.651535	\N
6	000001	2025-05-21	958659.31	0.0096	350164.03	0.0079	-653556.49	-0.0260	-995695.16	0.0027	-215932.58	-0.0047	2025-06-17 17:06:58.652537	2025-06-17 17:06:58.652537	\N
7	000001	2025-05-22	3862803.83	0.0386	-1895898.32	0.0157	-2635543.75	-0.0063	944373.21	0.0071	-323027.03	-0.0045	2025-06-17 17:06:58.653551	2025-06-17 17:06:58.653551	\N
8	000001	2025-05-23	3058239.04	0.0306	-1680201.34	0.0030	-825622.13	-0.0245	264443.29	0.0010	191637.20	0.0016	2025-06-17 17:06:58.654558	2025-06-17 17:06:58.654558	\N
9	000001	2025-05-26	-4447870.44	-0.0445	1040764.09	-0.0090	1153215.14	-0.0237	-954461.61	-0.0038	-22498.62	0.0041	2025-06-17 17:06:58.65545	2025-06-17 17:06:58.65545	\N
10	000001	2025-05-27	-4518278.51	-0.0452	-1577056.35	-0.0058	-1621822.73	0.0145	987732.31	0.0010	-110849.67	0.0021	2025-06-17 17:06:58.656919	2025-06-17 17:06:58.656919	\N
11	000001	2025-05-28	-3599998.86	-0.0360	1099460.05	0.0125	573957.46	-0.0111	213159.10	0.0059	-481851.91	0.0004	2025-06-17 17:06:58.658038	2025-06-17 17:06:58.658038	\N
12	000001	2025-05-29	183854.01	0.0018	-799439.88	0.0116	1025706.32	0.0141	-622808.33	0.0047	-263533.53	-0.0047	2025-06-17 17:06:58.659032	2025-06-17 17:06:58.659032	\N
13	000001	2025-05-30	-410774.46	-0.0041	-1410597.40	0.0070	-1043540.12	-0.0195	-256393.81	0.0083	288091.74	0.0011	2025-06-17 17:06:58.660339	2025-06-17 17:06:58.660339	\N
14	000001	2025-06-02	3160240.41	0.0316	572244.41	0.0037	-315254.26	-0.0124	78737.97	-0.0099	195848.71	-0.0036	2025-06-17 17:06:58.661284	2025-06-17 17:06:58.661284	\N
15	000001	2025-06-03	1716695.56	0.0172	800392.11	-0.0120	1132368.07	-0.0200	907191.37	-0.0029	-161881.00	0.0023	2025-06-17 17:06:58.662221	2025-06-17 17:06:58.662221	\N
16	000001	2025-06-04	-2967001.40	-0.0297	1459924.19	0.0144	-2968533.49	-0.0134	663783.91	0.0083	63983.18	0.0023	2025-06-17 17:06:58.663071	2025-06-17 17:06:58.663071	\N
17	000001	2025-06-05	4318300.98	0.0432	1800441.69	0.0044	-1360065.16	0.0146	287313.97	-0.0094	292820.15	0.0028	2025-06-17 17:06:58.66399	2025-06-17 17:06:58.66399	\N
18	000001	2025-06-06	-3221775.63	-0.0322	-757670.23	-0.0015	2651284.17	-0.0144	34080.21	-0.0052	-110434.82	-0.0017	2025-06-17 17:06:58.664911	2025-06-17 17:06:58.664911	\N
19	000001	2025-06-09	4964049.41	0.0496	-764167.25	0.0115	1959151.57	0.0246	-858792.19	-0.0072	-302811.66	-0.0040	2025-06-17 17:06:58.665914	2025-06-17 17:06:58.665914	\N
20	000001	2025-06-10	2209791.39	0.0221	1675568.22	0.0012	-1575611.62	0.0036	209256.22	0.0076	332468.45	-0.0006	2025-06-17 17:06:58.666777	2025-06-17 17:06:58.666777	\N
21	000001	2025-06-11	-3552451.02	-0.0355	-327312.54	0.0170	-1715268.67	-0.0215	-863965.81	-0.0075	-133602.75	0.0026	2025-06-17 17:06:58.667698	2025-06-17 17:06:58.667698	\N
22	000001	2025-06-12	2511996.91	0.0251	876430.30	-0.0098	75470.64	0.0008	336772.85	-0.0029	117801.99	-0.0015	2025-06-17 17:06:58.668633	2025-06-17 17:06:58.668633	\N
23	000001	2025-06-13	-918284.15	-0.0092	172777.74	0.0083	1227039.09	0.0053	742221.72	0.0009	108794.37	-0.0014	2025-06-17 17:06:58.6698	2025-06-17 17:06:58.6698	\N
24	000001	2025-06-16	1676509.20	0.0168	1980444.59	0.0043	-1049771.16	0.0190	169451.47	-0.0058	-482001.28	-0.0012	2025-06-17 17:06:58.670664	2025-06-17 17:06:58.670664	\N
25	000001	2025-06-17	1042328.29	0.0104	863070.39	0.0023	-2078187.24	-0.0105	-502981.37	-0.0025	381573.10	0.0028	2025-06-17 17:06:58.671758	2025-06-17 17:06:58.671758	\N
26	000001	2025-06-18	-396754.53	-0.0040	1061255.71	0.0158	-156250.04	-0.0148	211852.46	-0.0009	487784.89	0.0001	2025-06-17 17:06:58.672673	2025-06-17 17:06:58.672673	\N
27	000002	2025-05-19	4461787.08	0.0446	1003848.85	0.0050	1861008.03	0.0051	-984989.03	-0.0073	461722.79	0.0034	2025-06-17 17:06:58.673595	2025-06-17 17:06:58.673595	\N
28	000002	2025-05-20	3845215.96	0.0385	1159516.21	0.0111	2876695.16	-0.0118	231005.58	0.0051	-102899.28	-0.0014	2025-06-17 17:06:58.674517	2025-06-17 17:06:58.674517	\N
29	000002	2025-05-21	2600274.20	0.0260	1478950.69	-0.0050	713681.26	0.0145	571899.19	0.0020	270392.16	-0.0019	2025-06-17 17:06:58.675445	2025-06-17 17:06:58.675445	\N
30	000002	2025-05-22	525720.90	0.0053	-268805.61	0.0132	-1952471.50	-0.0056	-786282.02	-0.0037	-388323.90	-0.0029	2025-06-17 17:06:58.676321	2025-06-17 17:06:58.676321	\N
31	000002	2025-05-23	-3829134.17	-0.0383	1391546.94	0.0095	1574042.01	0.0211	-185487.72	-0.0028	119136.63	0.0030	2025-06-17 17:06:58.677235	2025-06-17 17:06:58.677235	\N
32	000002	2025-05-26	-4115663.53	-0.0412	1641975.46	-0.0029	-1048516.77	-0.0103	-549576.88	0.0028	-63558.58	0.0040	2025-06-17 17:06:58.678132	2025-06-17 17:06:58.678132	\N
33	000002	2025-05-27	-4112214.91	-0.0411	1664842.57	0.0028	-1081466.07	0.0238	-275648.90	0.0040	459764.62	-0.0015	2025-06-17 17:06:58.679065	2025-06-17 17:06:58.679065	\N
34	000002	2025-05-28	1960444.19	0.0196	1316489.98	0.0051	2606026.21	0.0224	63710.80	-0.0042	-480182.47	0.0009	2025-06-17 17:06:58.679927	2025-06-17 17:06:58.679927	\N
35	000002	2025-05-29	-1421200.75	-0.0142	1624481.19	0.0018	-322254.58	-0.0223	-364686.82	-0.0035	191135.64	-0.0039	2025-06-17 17:06:58.680785	2025-06-17 17:06:58.680785	\N
36	000002	2025-05-30	-1402279.13	-0.0140	1450558.46	0.0018	2415123.81	0.0023	-530101.57	0.0036	146726.04	-0.0019	2025-06-17 17:06:58.681651	2025-06-17 17:06:58.681651	\N
37	000002	2025-06-02	2671324.83	0.0267	749572.94	0.0133	-2832514.72	0.0112	-409605.78	0.0091	-326649.44	0.0033	2025-06-17 17:06:58.682607	2025-06-17 17:06:58.682607	\N
38	000002	2025-06-03	712609.47	0.0071	-31180.46	0.0130	-462686.50	0.0165	-886490.66	0.0011	341139.04	0.0032	2025-06-17 17:06:58.683532	2025-06-17 17:06:58.683532	\N
39	000002	2025-06-04	4239129.56	0.0424	-1657631.04	-0.0084	-1137464.18	-0.0135	-785565.91	-0.0038	-350471.22	-0.0013	2025-06-17 17:06:58.684489	2025-06-17 17:06:58.684489	\N
40	000002	2025-06-05	-405683.49	-0.0041	-435042.17	-0.0154	-1801646.56	-0.0291	-81983.55	-0.0045	-39878.74	-0.0011	2025-06-17 17:06:58.685437	2025-06-17 17:06:58.685437	\N
41	000002	2025-06-06	2106703.45	0.0211	1117031.33	-0.0028	-320866.64	-0.0222	218682.56	0.0014	-348588.81	0.0007	2025-06-17 17:06:58.686357	2025-06-17 17:06:58.686357	\N
42	000002	2025-06-09	-693.63	0.0000	448740.76	0.0042	670322.05	0.0266	577684.11	-0.0028	142001.99	0.0014	2025-06-17 17:06:58.687321	2025-06-17 17:06:58.687321	\N
43	000002	2025-06-10	395900.35	0.0040	-1287545.13	0.0050	-774190.06	0.0053	25326.94	-0.0017	-33927.99	0.0003	2025-06-17 17:06:58.688355	2025-06-17 17:06:58.688355	\N
44	000002	2025-06-11	4471768.95	0.0447	325735.73	0.0169	1395265.42	-0.0069	-760619.41	-0.0053	-390698.36	-0.0031	2025-06-17 17:06:58.68927	2025-06-17 17:06:58.68927	\N
45	000002	2025-06-12	-3711195.82	-0.0371	-704098.13	0.0156	-1815331.17	0.0271	903356.57	0.0036	-349275.30	-0.0019	2025-06-17 17:06:58.690202	2025-06-17 17:06:58.690202	\N
46	000002	2025-06-13	-647231.72	-0.0065	-219871.63	-0.0182	-2749843.34	-0.0251	-759750.84	-0.0029	407340.87	-0.0030	2025-06-17 17:06:58.691105	2025-06-17 17:06:58.691105	\N
47	000002	2025-06-16	-4336437.40	-0.0434	1937034.92	-0.0086	132259.64	-0.0242	159051.59	-0.0055	-66051.65	0.0004	2025-06-17 17:06:58.691972	2025-06-17 17:06:58.691972	\N
48	000002	2025-06-17	4881667.11	0.0488	-1833651.69	0.0088	1386109.61	-0.0064	339447.60	0.0054	413442.51	0.0030	2025-06-17 17:06:58.692896	2025-06-17 17:06:58.692896	\N
49	000002	2025-06-18	2796601.31	0.0280	291398.82	0.0034	-2547809.78	-0.0144	947052.16	0.0082	415471.70	-0.0010	2025-06-17 17:06:58.693835	2025-06-17 17:06:58.693835	\N
50	000006	2025-05-19	4777153.85	0.0478	381435.36	0.0183	-1892115.77	-0.0274	859217.48	0.0041	-207879.46	0.0038	2025-06-17 17:06:58.694947	2025-06-17 17:06:58.694947	\N
51	000006	2025-05-20	-4530900.27	-0.0453	1534850.48	0.0019	-2164745.91	0.0037	132391.81	0.0055	3783.45	0.0009	2025-06-17 17:06:58.695757	2025-06-17 17:06:58.695757	\N
52	000006	2025-05-21	-3706378.66	-0.0371	-119153.87	-0.0079	-2447517.06	-0.0286	97289.20	-0.0056	-385346.36	-0.0023	2025-06-17 17:06:58.696722	2025-06-17 17:06:58.696722	\N
53	000006	2025-05-22	3943378.83	0.0394	-1766263.67	0.0124	2148856.08	-0.0121	748279.26	0.0002	350525.11	0.0026	2025-06-17 17:06:58.69764	2025-06-17 17:06:58.69764	\N
54	000006	2025-05-23	412778.17	0.0041	-913052.22	-0.0055	-773976.32	0.0160	-448146.01	0.0042	-473403.36	-0.0017	2025-06-17 17:06:58.698513	2025-06-17 17:06:58.698513	\N
55	000006	2025-05-26	-1770130.85	-0.0177	1191432.21	0.0160	1894862.25	0.0078	-602681.99	-0.0001	368474.33	-0.0021	2025-06-17 17:06:58.699365	2025-06-17 17:06:58.699365	\N
56	000006	2025-05-27	-1939932.04	-0.0194	244897.35	-0.0032	1067960.11	-0.0274	-348883.53	0.0084	39461.42	-0.0005	2025-06-17 17:06:58.700326	2025-06-17 17:06:58.700326	\N
57	000006	2025-05-28	-205723.60	-0.0021	-1296760.48	-0.0174	-1783539.88	0.0053	725812.87	0.0022	-172400.38	-0.0010	2025-06-17 17:06:58.701239	2025-06-17 17:06:58.701239	\N
58	000006	2025-05-29	-1786784.58	-0.0179	83528.03	-0.0147	-196382.68	-0.0187	-716410.22	0.0059	-433682.43	0.0015	2025-06-17 17:06:58.70218	2025-06-17 17:06:58.70218	\N
59	000006	2025-05-30	4042398.45	0.0404	768483.71	0.0094	-1247237.85	-0.0142	359786.12	-0.0081	390364.91	-0.0002	2025-06-17 17:06:58.703167	2025-06-17 17:06:58.703167	\N
60	000006	2025-06-02	4179185.10	0.0418	-1338104.14	-0.0097	493094.43	0.0173	119475.07	0.0030	-138669.34	0.0038	2025-06-17 17:06:58.70414	2025-06-17 17:06:58.70414	\N
61	000006	2025-06-03	1843817.43	0.0184	-170064.28	0.0018	-853419.97	0.0146	293947.31	-0.0038	-274473.03	0.0024	2025-06-17 17:06:58.705205	2025-06-17 17:06:58.705205	\N
62	000006	2025-06-04	1785242.22	0.0179	666125.37	0.0080	-1909211.18	0.0117	-325389.52	-0.0031	324832.56	-0.0014	2025-06-17 17:06:58.706108	2025-06-17 17:06:58.706108	\N
63	000006	2025-06-05	2297798.71	0.0230	-1887868.25	-0.0078	443074.58	0.0266	915173.72	0.0073	76387.10	-0.0033	2025-06-17 17:06:58.70696	2025-06-17 17:06:58.70696	\N
64	000006	2025-06-06	730958.45	0.0073	-1793886.25	0.0066	1747035.61	0.0039	717246.92	-0.0055	379988.64	0.0016	2025-06-17 17:06:58.707993	2025-06-17 17:06:58.707993	\N
65	000006	2025-06-09	-956139.22	-0.0096	-1613497.76	-0.0065	1725825.68	-0.0065	151052.74	0.0036	371733.85	0.0008	2025-06-17 17:06:58.708914	2025-06-17 17:06:58.708914	\N
66	000006	2025-06-10	4134883.72	0.0413	1145738.07	0.0184	-2906888.99	0.0122	350194.12	0.0075	167885.28	-0.0023	2025-06-17 17:06:58.709864	2025-06-17 17:06:58.709864	\N
67	000006	2025-06-11	2178248.82	0.0218	589520.96	-0.0077	-2343370.01	-0.0126	-339378.53	0.0078	472945.56	-0.0027	2025-06-17 17:06:58.710682	2025-06-17 17:06:58.710682	\N
68	000006	2025-06-12	-185449.25	-0.0019	-1184599.71	-0.0141	2901665.88	-0.0266	-406791.01	0.0046	-343597.66	-0.0002	2025-06-17 17:06:58.711498	2025-06-17 17:06:58.711498	\N
69	000006	2025-06-13	-3023063.39	-0.0302	1635938.79	-0.0044	1950122.88	0.0220	50243.35	0.0033	418114.12	-0.0014	2025-06-17 17:06:58.712415	2025-06-17 17:06:58.712415	\N
70	000006	2025-06-16	-1960100.45	-0.0196	-658535.98	-0.0021	-1204693.00	0.0227	-442079.17	-0.0079	354005.37	0.0020	2025-06-17 17:06:58.713263	2025-06-17 17:06:58.713263	\N
71	000006	2025-06-17	4475179.81	0.0448	1568238.67	0.0032	1354106.78	-0.0093	582862.88	0.0095	111548.96	-0.0016	2025-06-17 17:06:58.714157	2025-06-17 17:06:58.714157	\N
72	000006	2025-06-18	1617497.65	0.0162	-218136.02	0.0136	725848.44	-0.0112	476136.90	-0.0045	-134124.14	-0.0044	2025-06-17 17:06:58.715071	2025-06-17 17:06:58.715071	\N
73	000007	2025-05-19	-1016405.66	-0.0102	-1129651.55	-0.0103	-958088.53	0.0078	-745910.36	-0.0071	-192402.76	-0.0013	2025-06-17 17:06:58.71604	2025-06-17 17:06:58.71604	\N
74	000007	2025-05-20	-1827080.11	-0.0183	287106.72	0.0125	-2793873.45	0.0116	-910904.01	-0.0069	9535.27	-0.0031	2025-06-17 17:06:58.716953	2025-06-17 17:06:58.716953	\N
75	000007	2025-05-21	4635256.47	0.0464	206736.08	-0.0063	1800034.68	-0.0104	-731975.73	0.0037	-21012.85	-0.0023	2025-06-17 17:06:58.717832	2025-06-17 17:06:58.717832	\N
76	000007	2025-05-22	-1298775.58	-0.0130	-1612607.39	0.0119	-2272623.13	-0.0197	-511128.88	-0.0048	-244879.86	-0.0020	2025-06-17 17:06:58.718705	2025-06-17 17:06:58.718705	\N
77	000007	2025-05-23	-4184453.37	-0.0418	819256.14	0.0109	847831.44	0.0007	973927.64	0.0089	83841.98	0.0029	2025-06-17 17:06:58.719605	2025-06-17 17:06:58.719605	\N
78	000007	2025-05-26	3637926.16	0.0364	813355.96	-0.0153	-842176.33	0.0283	-34538.08	0.0016	406827.76	0.0002	2025-06-17 17:06:58.720688	2025-06-17 17:06:58.720688	\N
79	000007	2025-05-27	-2340988.30	-0.0234	1642287.26	-0.0198	2856972.63	0.0020	-344158.48	0.0025	-134148.72	-0.0043	2025-06-17 17:06:58.721645	2025-06-17 17:06:58.721645	\N
80	000007	2025-05-28	-648752.80	-0.0065	1059371.33	0.0064	2930169.31	0.0130	849853.18	0.0071	-379711.76	-0.0024	2025-06-17 17:06:58.722577	2025-06-17 17:06:58.722577	\N
81	000007	2025-05-29	-4924983.24	-0.0492	-314146.61	-0.0108	-166585.94	0.0240	-748306.02	-0.0066	73406.99	0.0035	2025-06-17 17:06:58.72351	2025-06-17 17:06:58.72351	\N
82	000007	2025-05-30	-1641151.73	-0.0164	426973.39	0.0198	414543.27	0.0166	-911351.82	-0.0005	-381659.99	-0.0046	2025-06-17 17:06:58.724392	2025-06-17 17:06:58.724392	\N
83	000007	2025-06-02	3022533.79	0.0302	-330146.05	0.0035	989177.32	-0.0028	-325125.22	-0.0069	420742.22	-0.0047	2025-06-17 17:06:58.72526	2025-06-17 17:06:58.72526	\N
84	000007	2025-06-03	-428785.40	-0.0043	-749329.76	-0.0009	-1395715.52	0.0257	741704.35	0.0075	474964.91	-0.0017	2025-06-17 17:06:58.726175	2025-06-17 17:06:58.726175	\N
85	000007	2025-06-04	2158486.71	0.0216	-514820.80	0.0132	-920581.56	-0.0254	30687.32	0.0086	-318691.97	0.0049	2025-06-17 17:06:58.727186	2025-06-17 17:06:58.727186	\N
86	000007	2025-06-05	1971335.83	0.0197	-1542392.52	0.0066	-759413.81	-0.0251	-291184.80	-0.0078	329950.98	0.0022	2025-06-17 17:06:58.72813	2025-06-17 17:06:58.72813	\N
87	000007	2025-06-06	3349993.54	0.0335	-445373.84	0.0062	-599009.19	-0.0118	-348158.65	0.0007	-426296.02	0.0010	2025-06-17 17:06:58.729101	2025-06-17 17:06:58.729101	\N
88	000007	2025-06-09	-808756.15	-0.0081	1526656.81	-0.0115	-2355736.63	0.0117	92369.29	-0.0099	77727.25	0.0019	2025-06-17 17:06:58.730019	2025-06-17 17:06:58.730019	\N
89	000007	2025-06-10	-2748559.97	-0.0275	1680737.37	-0.0005	541781.75	-0.0042	-489174.85	-0.0040	260633.33	-0.0022	2025-06-17 17:06:58.730916	2025-06-17 17:06:58.730916	\N
90	000007	2025-06-11	-952005.04	-0.0095	485442.47	0.0174	-2029908.47	0.0001	275616.88	-0.0078	495732.72	-0.0037	2025-06-17 17:06:58.73186	2025-06-17 17:06:58.73186	\N
91	000007	2025-06-12	2766328.27	0.0277	-533705.41	-0.0124	1144938.82	-0.0157	558506.52	-0.0052	27197.99	-0.0047	2025-06-17 17:06:58.732804	2025-06-17 17:06:58.732804	\N
92	000007	2025-06-13	478222.76	0.0048	-426178.11	-0.0058	2290080.23	-0.0206	-949118.37	0.0093	454205.69	0.0048	2025-06-17 17:06:58.733764	2025-06-17 17:06:58.733764	\N
93	000007	2025-06-16	4435769.86	0.0444	-1739102.68	-0.0012	-2919074.30	-0.0267	-996387.35	-0.0056	106648.14	-0.0022	2025-06-17 17:06:58.734715	2025-06-17 17:06:58.734715	\N
94	000007	2025-06-17	-4770722.12	-0.0477	-1563478.08	-0.0049	-1700646.79	-0.0169	826116.32	0.0066	-473332.14	0.0038	2025-06-17 17:06:58.735606	2025-06-17 17:06:58.735606	\N
95	000007	2025-06-18	-355741.25	-0.0036	-969264.99	0.0034	279465.58	0.0136	680623.21	-0.0040	82647.66	-0.0007	2025-06-17 17:06:58.736557	2025-06-17 17:06:58.736557	\N
96	000008	2025-05-19	564693.52	0.0056	-403062.14	-0.0140	-2848190.15	0.0001	-194759.55	-0.0094	-411536.26	0.0038	2025-06-17 17:06:58.737634	2025-06-17 17:06:58.737634	\N
97	000008	2025-05-20	-4498886.68	-0.0450	-1447263.64	0.0083	-2624571.33	0.0065	-403210.68	0.0069	364561.17	-0.0043	2025-06-17 17:06:58.738467	2025-06-17 17:06:58.738467	\N
98	000008	2025-05-21	4114133.57	0.0411	1437312.91	0.0124	-861098.77	0.0009	354901.92	-0.0081	254528.65	-0.0008	2025-06-17 17:06:58.739327	2025-06-17 17:06:58.739327	\N
99	000008	2025-05-22	-3193525.90	-0.0319	353790.02	-0.0161	-717253.14	-0.0172	-654515.98	0.0033	-293964.76	-0.0015	2025-06-17 17:06:58.740242	2025-06-17 17:06:58.740242	\N
100	000008	2025-05-23	-3003825.71	-0.0300	1472995.63	0.0164	-1801430.02	0.0181	495494.56	-0.0002	492790.66	-0.0028	2025-06-17 17:06:58.741137	2025-06-17 17:06:58.741137	\N
101	000008	2025-05-26	2101134.71	0.0210	-1882858.59	0.0189	1372736.07	-0.0103	560495.35	-0.0067	-308337.01	0.0034	2025-06-17 17:06:58.741999	2025-06-17 17:06:58.741999	\N
102	000008	2025-05-27	-1410993.11	-0.0141	-1565736.42	-0.0091	-748171.94	0.0000	-548301.49	0.0056	175941.60	-0.0010	2025-06-17 17:06:58.742933	2025-06-17 17:06:58.742933	\N
103	000008	2025-05-28	-660733.22	-0.0066	422243.70	-0.0172	-190226.16	-0.0113	981475.75	0.0015	-376023.32	-0.0023	2025-06-17 17:06:58.743874	2025-06-17 17:06:58.743874	\N
104	000008	2025-05-29	-2733877.31	-0.0273	1077128.34	-0.0093	1237855.40	0.0223	-96474.99	0.0062	-409859.08	0.0046	2025-06-17 17:06:58.744801	2025-06-17 17:06:58.744801	\N
105	000008	2025-05-30	-3356033.19	-0.0336	-1565135.38	-0.0022	-764748.44	0.0228	611059.32	0.0091	272542.08	0.0045	2025-06-17 17:06:58.745719	2025-06-17 17:06:58.745719	\N
106	000008	2025-06-02	1334899.69	0.0133	1290679.74	0.0134	1885109.51	-0.0243	317573.57	0.0089	-206336.42	-0.0045	2025-06-17 17:06:58.746769	2025-06-17 17:06:58.746769	\N
107	000008	2025-06-03	-866720.55	-0.0087	-820169.43	0.0042	-2204139.35	0.0272	462903.35	0.0020	434356.45	0.0021	2025-06-17 17:06:58.747617	2025-06-17 17:06:58.747617	\N
108	000008	2025-06-04	-2767084.04	-0.0277	1831395.12	-0.0172	-96936.54	0.0099	-19653.43	-0.0080	357880.59	0.0044	2025-06-17 17:06:58.748443	2025-06-17 17:06:58.748443	\N
109	000008	2025-06-05	-1941632.66	-0.0194	344239.25	0.0068	1770838.11	0.0038	871726.40	0.0091	-450592.79	-0.0023	2025-06-17 17:06:58.749392	2025-06-17 17:06:58.749392	\N
110	000008	2025-06-06	1030563.36	0.0103	803197.78	-0.0127	-1828652.50	-0.0149	299406.00	0.0099	221621.36	-0.0040	2025-06-17 17:06:58.750316	2025-06-17 17:06:58.750316	\N
111	000008	2025-06-09	2987198.17	0.0299	56903.35	-0.0124	1702471.62	-0.0276	-618132.79	0.0098	493929.56	-0.0046	2025-06-17 17:06:58.751244	2025-06-17 17:06:58.751244	\N
112	000008	2025-06-10	1264822.82	0.0126	1919702.56	-0.0190	-746852.91	0.0050	249212.58	0.0043	-204659.44	-0.0022	2025-06-17 17:06:58.752075	2025-06-17 17:06:58.752075	\N
113	000008	2025-06-11	-4543185.00	-0.0454	320680.78	0.0142	-1785330.47	0.0281	739590.48	-0.0044	-348569.46	0.0027	2025-06-17 17:06:58.752979	2025-06-17 17:06:58.752979	\N
114	000008	2025-06-12	-1963181.40	-0.0196	-1389828.86	0.0110	-2577139.37	0.0003	-152811.14	-0.0031	397367.35	0.0004	2025-06-17 17:06:58.753855	2025-06-17 17:06:58.753855	\N
115	000008	2025-06-13	-3606098.84	-0.0361	257079.42	-0.0146	-2569871.95	-0.0248	781906.14	0.0089	231932.07	0.0026	2025-06-17 17:06:58.754848	2025-06-17 17:06:58.754848	\N
116	000008	2025-06-16	-1915283.61	-0.0192	1838314.44	-0.0004	-2866162.66	-0.0296	-925696.50	-0.0021	-10525.53	0.0003	2025-06-17 17:06:58.755706	2025-06-17 17:06:58.755706	\N
117	000008	2025-06-17	4135468.87	0.0414	-1954557.45	0.0105	2764827.48	-0.0282	822909.05	-0.0095	-474340.83	0.0030	2025-06-17 17:06:58.75663	2025-06-17 17:06:58.75663	\N
118	000008	2025-06-18	4422541.09	0.0442	1876993.09	-0.0200	922479.67	-0.0074	-934394.84	0.0049	338086.60	0.0004	2025-06-17 17:06:58.757539	2025-06-17 17:06:58.757539	\N
\.


--
-- TOC entry 3642 (class 0 OID 16517)
-- Dependencies: 223
-- Data for Name: capital_flow_profiles; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.capital_flow_profiles (id, ts_code, trade_date, main_force_score, retail_sentiment_score, institutional_activity, flow_consistency, volume_price_correlation, flow_analysis, created_at) FROM stdin;
\.


--
-- TOC entry 3655 (class 0 OID 24789)
-- Dependencies: 236
-- Data for Name: data_update_log; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.data_update_log (id, update_date, update_time, status, message, created_at) FROM stdin;
1	2025-06-18	2025-06-18 00:03:37.35193	started	开始每日数据更新	2025-06-17 16:03:37.433904
2	2025-06-18	2025-06-18 00:05:43.989233	started	开始每日数据更新	2025-06-17 16:05:44.063417
3	2025-06-18	2025-06-18 00:05:59.360853	completed	每日数据更新完成	2025-06-17 16:05:59.36276
4	2025-06-18	2025-06-18 00:13:30.46623	started	开始每日数据更新	2025-06-17 16:13:30.465756
5	2025-06-18	2025-06-18 00:13:51.75556	completed	每日数据更新完成	2025-06-17 16:13:51.75575
\.


--
-- TOC entry 3663 (class 0 OID 24842)
-- Dependencies: 244
-- Data for Name: financial_indicators_quarterly; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.financial_indicators_quarterly (id, ts_code, report_date, pe_ratio, pb_ratio, ps_ratio, roe, roa, debt_ratio, current_ratio, quick_ratio, gross_margin, net_margin, created_at, updated_at) FROM stdin;
\.


--
-- TOC entry 3644 (class 0 OID 16529)
-- Dependencies: 225
-- Data for Name: fundamental_profiles; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.fundamental_profiles (id, ts_code, trade_date, catalyst_score, news_sentiment, announcement_impact, industry_momentum, fundamental_data, created_at) FROM stdin;
\.


--
-- TOC entry 3648 (class 0 OID 16553)
-- Dependencies: 229
-- Data for Name: macro_profiles; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.macro_profiles (id, trade_date, market_regime, sector_rotation, risk_appetite, liquidity_condition, macro_data, created_at) FROM stdin;
\.


--
-- TOC entry 3661 (class 0 OID 24831)
-- Dependencies: 242
-- Data for Name: market_sentiment_daily; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.market_sentiment_daily (id, ts_code, trade_date, sentiment_score, news_sentiment, social_sentiment, analyst_rating, institutional_activity, created_at, updated_at) FROM stdin;
1	000001	2025-05-19	4.7599	1.2164	4.5511	8.0273	5.1390	2025-06-17 17:06:58.760418	2025-06-17 17:06:58.760418
2	000001	2025-05-20	9.9799	8.0038	8.8128	1.7922	4.3778	2025-06-17 17:06:58.761599	2025-06-17 17:06:58.761599
3	000001	2025-05-21	4.9109	0.7529	6.4623	3.3344	7.8031	2025-06-17 17:06:58.762521	2025-06-17 17:06:58.762521
4	000001	2025-05-22	2.5951	2.5077	0.7473	7.2248	4.7042	2025-06-17 17:06:58.76341	2025-06-17 17:06:58.76341
5	000001	2025-05-23	3.0866	5.2997	4.4742	0.9389	3.2853	2025-06-17 17:06:58.764522	2025-06-17 17:06:58.764522
6	000001	2025-05-26	6.5420	8.7178	4.0947	9.4567	2.6065	2025-06-17 17:06:58.765707	2025-06-17 17:06:58.765707
7	000001	2025-05-27	5.9502	2.7709	4.4301	6.9466	7.3069	2025-06-17 17:06:58.766761	2025-06-17 17:06:58.766761
8	000001	2025-05-28	2.0313	3.7389	1.7692	0.9988	3.2431	2025-06-17 17:06:58.767968	2025-06-17 17:06:58.767968
9	000001	2025-05-29	0.4059	7.2955	2.9519	9.7319	3.2600	2025-06-17 17:06:58.76896	2025-06-17 17:06:58.76896
10	000001	2025-05-30	3.8680	9.0210	3.2575	3.0855	7.7983	2025-06-17 17:06:58.769882	2025-06-17 17:06:58.769882
11	000001	2025-06-02	9.6440	1.7393	6.5221	5.6506	7.5467	2025-06-17 17:06:58.7708	2025-06-17 17:06:58.7708
12	000001	2025-06-03	7.8709	2.1386	0.3377	8.7838	5.9732	2025-06-17 17:06:58.77228	2025-06-17 17:06:58.77228
13	000001	2025-06-04	5.2015	4.5781	6.2494	2.3309	1.0361	2025-06-17 17:06:58.773188	2025-06-17 17:06:58.773188
14	000001	2025-06-05	8.1521	3.3810	6.4243	8.8515	2.9207	2025-06-17 17:06:58.774024	2025-06-17 17:06:58.774024
15	000001	2025-06-06	6.2625	4.2427	2.5716	9.0797	3.4934	2025-06-17 17:06:58.774936	2025-06-17 17:06:58.774936
16	000001	2025-06-09	8.3919	3.8405	7.2108	9.4490	8.3641	2025-06-17 17:06:58.775762	2025-06-17 17:06:58.775762
17	000001	2025-06-10	5.3490	3.4355	5.4243	8.1641	8.6667	2025-06-17 17:06:58.776654	2025-06-17 17:06:58.776654
18	000001	2025-06-11	2.3991	2.3025	5.6711	6.5577	8.4773	2025-06-17 17:06:58.777553	2025-06-17 17:06:58.777553
19	000001	2025-06-12	9.9044	7.3796	1.7771	5.1117	1.6997	2025-06-17 17:06:58.778464	2025-06-17 17:06:58.778464
20	000001	2025-06-13	3.3863	1.3430	1.2949	8.5461	6.0509	2025-06-17 17:06:58.779354	2025-06-17 17:06:58.779354
21	000001	2025-06-16	3.5393	6.9074	5.3332	8.3152	7.5360	2025-06-17 17:06:58.780307	2025-06-17 17:06:58.780307
22	000001	2025-06-17	1.4213	6.7010	0.8724	4.3880	7.4567	2025-06-17 17:06:58.781259	2025-06-17 17:06:58.781259
23	000001	2025-06-18	6.8652	6.9402	2.8909	5.2344	7.6639	2025-06-17 17:06:58.782225	2025-06-17 17:06:58.782225
24	000002	2025-05-19	6.9559	7.6912	0.8675	7.8013	7.9913	2025-06-17 17:06:58.783174	2025-06-17 17:06:58.783174
25	000002	2025-05-20	5.0338	1.8228	1.0694	4.9981	7.9572	2025-06-17 17:06:58.784158	2025-06-17 17:06:58.784158
26	000002	2025-05-21	4.0480	1.1949	6.9040	0.4268	3.3685	2025-06-17 17:06:58.784999	2025-06-17 17:06:58.784999
27	000002	2025-05-22	4.5784	0.9043	5.7718	1.2850	1.5227	2025-06-17 17:06:58.785878	2025-06-17 17:06:58.785878
28	000002	2025-05-23	5.6720	3.5016	8.8584	9.8288	5.9963	2025-06-17 17:06:58.786798	2025-06-17 17:06:58.786798
29	000002	2025-05-26	0.1150	3.8222	9.8728	2.9170	1.5509	2025-06-17 17:06:58.787931	2025-06-17 17:06:58.787931
30	000002	2025-05-27	1.8395	1.1376	3.4850	3.4188	4.6767	2025-06-17 17:06:58.789942	2025-06-17 17:06:58.789942
31	000002	2025-05-28	4.3611	1.9550	4.6288	5.8691	8.9898	2025-06-17 17:06:58.790883	2025-06-17 17:06:58.790883
32	000002	2025-05-29	8.0526	9.8741	3.1233	9.2872	0.2724	2025-06-17 17:06:58.791787	2025-06-17 17:06:58.791787
33	000002	2025-05-30	3.2683	3.9785	1.6513	5.0891	9.2584	2025-06-17 17:06:58.792626	2025-06-17 17:06:58.792626
34	000002	2025-06-02	6.2355	0.3586	4.7269	2.1923	2.5426	2025-06-17 17:06:58.79351	2025-06-17 17:06:58.79351
35	000002	2025-06-03	8.1603	9.6510	4.0659	5.4741	0.7017	2025-06-17 17:06:58.794431	2025-06-17 17:06:58.794431
36	000002	2025-06-04	3.1007	2.9012	0.9422	4.6425	2.1090	2025-06-17 17:06:58.795358	2025-06-17 17:06:58.795358
37	000002	2025-06-05	6.5636	5.0041	8.7281	9.1870	6.9558	2025-06-17 17:06:58.79626	2025-06-17 17:06:58.79626
38	000002	2025-06-06	4.6290	6.1161	1.9687	7.9064	4.6983	2025-06-17 17:06:58.797176	2025-06-17 17:06:58.797176
39	000002	2025-06-09	6.7990	3.0132	9.4486	5.4709	9.4794	2025-06-17 17:06:58.798084	2025-06-17 17:06:58.798084
40	000002	2025-06-10	8.2505	9.2500	5.4506	6.6239	7.5453	2025-06-17 17:06:58.799018	2025-06-17 17:06:58.799018
41	000002	2025-06-11	3.7829	0.4411	0.1160	3.1826	2.9713	2025-06-17 17:06:58.79994	2025-06-17 17:06:58.79994
42	000002	2025-06-12	1.9007	5.9853	3.0636	7.0707	3.0524	2025-06-17 17:06:58.800876	2025-06-17 17:06:58.800876
43	000002	2025-06-13	4.5196	0.9043	3.7231	3.6007	9.5800	2025-06-17 17:06:58.802925	2025-06-17 17:06:58.802925
44	000002	2025-06-16	6.8784	2.3115	4.9707	4.8984	3.8619	2025-06-17 17:06:58.803823	2025-06-17 17:06:58.803823
45	000002	2025-06-17	5.0739	1.4534	7.5577	2.5628	9.1511	2025-06-17 17:06:58.804892	2025-06-17 17:06:58.804892
46	000002	2025-06-18	7.9475	1.1544	8.9739	1.7925	0.8127	2025-06-17 17:06:58.805753	2025-06-17 17:06:58.805753
47	000006	2025-05-19	1.8272	5.4234	3.3187	3.8691	9.6918	2025-06-17 17:06:58.806611	2025-06-17 17:06:58.806611
48	000006	2025-05-20	3.7116	8.6593	3.8533	1.7767	1.4703	2025-06-17 17:06:58.807503	2025-06-17 17:06:58.807503
49	000006	2025-05-21	8.5527	3.4685	3.0307	5.5773	8.1893	2025-06-17 17:06:58.808414	2025-06-17 17:06:58.808414
50	000006	2025-05-22	3.9668	9.0867	3.7628	8.5077	3.3073	2025-06-17 17:06:58.809329	2025-06-17 17:06:58.809329
51	000006	2025-05-23	0.1146	3.7719	5.2283	6.2128	6.6600	2025-06-17 17:06:58.810222	2025-06-17 17:06:58.810222
52	000006	2025-05-26	4.2075	8.6191	3.6124	1.8980	5.3713	2025-06-17 17:06:58.811199	2025-06-17 17:06:58.811199
53	000006	2025-05-27	9.5940	4.8270	5.0101	9.4873	0.4687	2025-06-17 17:06:58.812089	2025-06-17 17:06:58.812089
54	000006	2025-05-28	2.5843	2.3105	8.1815	2.6015	4.7697	2025-06-17 17:06:58.812928	2025-06-17 17:06:58.812928
55	000006	2025-05-29	0.8533	3.3689	7.6150	1.5215	2.6016	2025-06-17 17:06:58.813812	2025-06-17 17:06:58.813812
56	000006	2025-05-30	3.3443	2.7048	5.0387	6.6811	8.9883	2025-06-17 17:06:58.814745	2025-06-17 17:06:58.814745
57	000006	2025-06-02	0.3044	6.5068	1.0850	8.6606	1.7526	2025-06-17 17:06:58.815598	2025-06-17 17:06:58.815598
58	000006	2025-06-03	1.6727	7.7846	3.6398	5.9586	7.3315	2025-06-17 17:06:58.816549	2025-06-17 17:06:58.816549
59	000006	2025-06-04	3.3280	4.4422	9.6107	6.1654	8.9986	2025-06-17 17:06:58.81746	2025-06-17 17:06:58.81746
60	000006	2025-06-05	5.0472	9.6791	1.4788	0.5846	6.6964	2025-06-17 17:06:58.818383	2025-06-17 17:06:58.818383
61	000006	2025-06-06	3.7542	0.5472	9.2752	1.0885	8.7498	2025-06-17 17:06:58.819276	2025-06-17 17:06:58.819276
62	000006	2025-06-09	6.5637	9.6117	5.1751	7.7026	9.7945	2025-06-17 17:06:58.820177	2025-06-17 17:06:58.820177
63	000006	2025-06-10	7.4245	3.7701	6.3985	9.9653	2.1299	2025-06-17 17:06:58.821206	2025-06-17 17:06:58.821206
64	000006	2025-06-11	5.9479	8.7740	9.2715	0.1106	1.3586	2025-06-17 17:06:58.822073	2025-06-17 17:06:58.822073
65	000006	2025-06-12	4.7370	4.2693	8.1576	9.8457	9.8975	2025-06-17 17:06:58.822957	2025-06-17 17:06:58.822957
66	000006	2025-06-13	6.6042	6.1551	4.2847	2.0917	3.7413	2025-06-17 17:06:58.823896	2025-06-17 17:06:58.823896
67	000006	2025-06-16	5.5481	7.5742	8.3319	0.1304	8.5292	2025-06-17 17:06:58.824701	2025-06-17 17:06:58.824701
68	000006	2025-06-17	3.1918	6.0049	5.5504	4.2502	8.4838	2025-06-17 17:06:58.825515	2025-06-17 17:06:58.825515
69	000006	2025-06-18	6.1691	4.5947	1.4000	5.5649	2.3921	2025-06-17 17:06:58.82633	2025-06-17 17:06:58.82633
70	000007	2025-05-19	7.8995	4.0458	9.4956	9.7545	5.1363	2025-06-17 17:06:58.82721	2025-06-17 17:06:58.82721
71	000007	2025-05-20	4.2464	5.1273	3.8557	2.7870	6.2615	2025-06-17 17:06:58.828316	2025-06-17 17:06:58.828316
72	000007	2025-05-21	4.5268	1.0749	0.6857	5.2791	2.4488	2025-06-17 17:06:58.829205	2025-06-17 17:06:58.829205
73	000007	2025-05-22	2.2158	1.2685	1.1076	8.1874	7.5776	2025-06-17 17:06:58.830112	2025-06-17 17:06:58.830112
74	000007	2025-05-23	3.7473	2.3633	0.7035	3.8701	9.3029	2025-06-17 17:06:58.831023	2025-06-17 17:06:58.831023
75	000007	2025-05-26	6.9207	4.8983	0.7569	0.4551	6.9665	2025-06-17 17:06:58.831914	2025-06-17 17:06:58.831914
76	000007	2025-05-27	3.2800	9.1661	3.8295	3.3273	6.7887	2025-06-17 17:06:58.832838	2025-06-17 17:06:58.832838
77	000007	2025-05-28	4.4285	8.2457	9.9919	5.1137	4.4122	2025-06-17 17:06:58.833685	2025-06-17 17:06:58.833685
78	000007	2025-05-29	8.7978	2.1361	0.9212	0.3564	5.6267	2025-06-17 17:06:58.834498	2025-06-17 17:06:58.834498
79	000007	2025-05-30	5.4881	0.5803	5.2231	2.0456	8.8855	2025-06-17 17:06:58.835338	2025-06-17 17:06:58.835338
80	000007	2025-06-02	6.0376	3.0731	6.8163	7.9835	7.1826	2025-06-17 17:06:58.836213	2025-06-17 17:06:58.836213
81	000007	2025-06-03	3.1310	1.1471	9.7643	7.4293	7.9962	2025-06-17 17:06:58.8371	2025-06-17 17:06:58.8371
82	000007	2025-06-04	2.7032	1.4128	3.3319	6.0998	5.2444	2025-06-17 17:06:58.838091	2025-06-17 17:06:58.838091
83	000007	2025-06-05	4.2722	9.1989	5.0997	3.9034	2.3180	2025-06-17 17:06:58.839077	2025-06-17 17:06:58.839077
84	000007	2025-06-06	8.6844	9.1423	7.2900	6.6239	5.7574	2025-06-17 17:06:58.839884	2025-06-17 17:06:58.839884
85	000007	2025-06-09	4.2991	7.4180	4.9859	4.9660	7.0872	2025-06-17 17:06:58.840773	2025-06-17 17:06:58.840773
86	000007	2025-06-10	6.9943	5.5052	4.5914	2.9798	4.3779	2025-06-17 17:06:58.841639	2025-06-17 17:06:58.841639
87	000007	2025-06-11	4.6142	4.6504	3.9902	3.0402	8.2187	2025-06-17 17:06:58.84252	2025-06-17 17:06:58.84252
88	000007	2025-06-12	6.9447	5.6680	1.3269	9.8268	2.6086	2025-06-17 17:06:58.843348	2025-06-17 17:06:58.843348
89	000007	2025-06-13	4.1168	5.6330	6.6827	8.2300	0.1699	2025-06-17 17:06:58.844136	2025-06-17 17:06:58.844136
90	000007	2025-06-16	9.2595	5.8318	4.1132	4.5869	6.6532	2025-06-17 17:06:58.844955	2025-06-17 17:06:58.844955
91	000007	2025-06-17	8.8335	0.3770	7.5275	4.7714	1.9050	2025-06-17 17:06:58.845768	2025-06-17 17:06:58.845768
92	000007	2025-06-18	3.3068	0.8010	3.3752	7.9117	2.8798	2025-06-17 17:06:58.846597	2025-06-17 17:06:58.846597
93	000008	2025-05-19	7.5908	5.2419	0.1739	6.8541	3.4718	2025-06-17 17:06:58.84746	2025-06-17 17:06:58.84746
94	000008	2025-05-20	2.2637	7.5992	7.1883	1.6252	3.8479	2025-06-17 17:06:58.848335	2025-06-17 17:06:58.848335
95	000008	2025-05-21	3.4775	6.3340	6.4704	8.9484	6.9892	2025-06-17 17:06:58.849203	2025-06-17 17:06:58.849203
96	000008	2025-05-22	1.8297	1.6189	1.9676	7.0019	7.1104	2025-06-17 17:06:58.850047	2025-06-17 17:06:58.850047
97	000008	2025-05-23	2.9409	6.1209	9.9093	3.5696	5.2632	2025-06-17 17:06:58.850862	2025-06-17 17:06:58.850862
98	000008	2025-05-26	1.3355	4.7707	5.0584	0.4157	7.9323	2025-06-17 17:06:58.851711	2025-06-17 17:06:58.851711
99	000008	2025-05-27	5.5987	2.9265	3.0605	1.0489	7.7832	2025-06-17 17:06:58.852527	2025-06-17 17:06:58.852527
100	000008	2025-05-28	8.7687	5.9782	9.2556	3.2369	9.1009	2025-06-17 17:06:58.853368	2025-06-17 17:06:58.853368
101	000008	2025-05-29	2.0258	7.6435	1.9566	4.6739	3.4358	2025-06-17 17:06:58.854252	2025-06-17 17:06:58.854252
102	000008	2025-05-30	2.4739	9.7643	9.8614	4.2435	4.8688	2025-06-17 17:06:58.855198	2025-06-17 17:06:58.855198
103	000008	2025-06-02	8.7761	0.3681	4.4118	2.7748	1.9092	2025-06-17 17:06:58.856062	2025-06-17 17:06:58.856062
104	000008	2025-06-03	0.7877	2.8997	0.3278	3.3317	9.5263	2025-06-17 17:06:58.856924	2025-06-17 17:06:58.856924
105	000008	2025-06-04	8.9532	0.6387	4.4926	8.8369	6.8192	2025-06-17 17:06:58.857755	2025-06-17 17:06:58.857755
106	000008	2025-06-05	0.5142	3.5802	1.8497	2.0512	2.1465	2025-06-17 17:06:58.858585	2025-06-17 17:06:58.858585
107	000008	2025-06-06	4.7551	0.3931	4.0587	6.3741	6.1636	2025-06-17 17:06:58.859419	2025-06-17 17:06:58.859419
108	000008	2025-06-09	7.6315	6.8587	9.3969	6.8461	0.3777	2025-06-17 17:06:58.860247	2025-06-17 17:06:58.860247
109	000008	2025-06-10	3.6132	0.9611	4.3617	5.0202	6.5826	2025-06-17 17:06:58.861088	2025-06-17 17:06:58.861088
110	000008	2025-06-11	5.2247	3.1255	1.6433	3.2583	3.6520	2025-06-17 17:06:58.861948	2025-06-17 17:06:58.861948
111	000008	2025-06-12	5.3632	6.7210	5.3590	1.3800	4.7348	2025-06-17 17:06:58.862738	2025-06-17 17:06:58.862738
112	000008	2025-06-13	6.4704	0.7925	0.0249	1.0516	9.4049	2025-06-17 17:06:58.863598	2025-06-17 17:06:58.863598
113	000008	2025-06-16	5.8460	5.9074	0.9276	7.6356	9.0105	2025-06-17 17:06:58.864464	2025-06-17 17:06:58.864464
114	000008	2025-06-17	8.6043	9.9293	9.2991	8.0919	5.6476	2025-06-17 17:06:58.865372	2025-06-17 17:06:58.865372
115	000008	2025-06-18	1.5144	9.4440	6.8330	7.5257	4.1718	2025-06-17 17:06:58.866234	2025-06-17 17:06:58.866234
\.


--
-- TOC entry 3638 (class 0 OID 16495)
-- Dependencies: 219
-- Data for Name: money_flow_daily; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.money_flow_daily (id, ts_code, trade_date, buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount, buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount, buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount, buy_elg_vol, buy_elg_amount, sell_elg_vol, sell_elg_amount, net_mf_vol, net_mf_amount, created_at) FROM stdin;
\.


--
-- TOC entry 3646 (class 0 OID 16541)
-- Dependencies: 227
-- Data for Name: sentiment_profiles; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.sentiment_profiles (id, ts_code, trade_date, social_sentiment, news_sentiment, analyst_sentiment, market_attention, sentiment_data, created_at) FROM stdin;
\.


--
-- TOC entry 3653 (class 0 OID 16621)
-- Dependencies: 234
-- Data for Name: stock_basic; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.stock_basic (ts_code, name, symbol, market, exchange, list_status, updated_at) FROM stdin;
000001	平安银行	000001	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000002	万  科Ａ	000002	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000006	深振业Ａ	000006	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000007	全新好	000007	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000008	神州高铁	000008	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000009	中国宝安	000009	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000010	美丽生态	000010	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000011	深物业A	000011	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000012	南  玻Ａ	000012	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000014	沙河股份	000014	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000016	深康佳Ａ	000016	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000017	深中华A	000017	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000019	深粮控股	000019	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000020	深华发Ａ	000020	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000021	深科技	000021	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000025	特  力Ａ	000025	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000026	飞亚达	000026	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000027	深圳能源	000027	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000028	国药一致	000028	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000029	深深房Ａ	000029	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000030	富奥股份	000030	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000031	大悦城	000031	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000032	深桑达Ａ	000032	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000034	神州数码	000034	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000035	中国天楹	000035	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000036	华联控股	000036	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000037	深南电A	000037	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000039	中集集团	000039	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000042	中洲控股	000042	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000045	深纺织Ａ	000045	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000048	京基智农	000048	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000049	德赛电池	000049	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000050	深天马Ａ	000050	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000055	方大集团	000055	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000056	皇庭国际	000056	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000058	深 赛 格	000058	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000059	华锦股份	000059	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000060	中金岭南	000060	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000061	农 产 品	000061	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000062	深圳华强	000062	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000063	中兴通讯	000063	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000065	北方国际	000065	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000066	中国长城	000066	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000068	华控赛格	000068	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000069	华侨城Ａ	000069	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000078	海王生物	000078	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000088	盐 田 港	000088	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000089	深圳机场	000089	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000090	天健集团	000090	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000096	广聚能源	000096	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000099	中信海直	000099	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000100	TCL科技	000100	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000151	中成股份	000151	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000153	丰原药业	000153	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000155	川能动力	000155	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000156	华数传媒	000156	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000157	中联重科	000157	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000158	常山北明	000158	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000159	国际实业	000159	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000166	申万宏源	000166	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000301	东方盛虹	000301	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000333	美的集团	000333	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000338	潍柴动力	000338	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000400	许继电气	000400	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000401	冀东水泥	000401	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000402	金 融 街	000402	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000403	派林生物	000403	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000404	长虹华意	000404	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000407	胜利股份	000407	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000408	藏格矿业	000408	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000409	云鼎科技	000409	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000410	沈阳机床	000410	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000411	英特集团	000411	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000415	渤海租赁	000415	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000417	合百集团	000417	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000419	通程控股	000419	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000420	吉林化纤	000420	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000421	南京公用	000421	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000422	湖北宜化	000422	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000423	东阿阿胶	000423	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000425	徐工机械	000425	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000426	兴业银锡	000426	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000428	华天酒店	000428	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000429	粤高速Ａ	000429	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000498	山东路桥	000498	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000501	武商集团	000501	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000503	国新健康	000503	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000505	京粮控股	000505	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000506	中润资源	000506	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000507	珠海港	000507	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000509	华塑控股	000509	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000510	新金路	000510	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000513	丽珠集团	000513	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000514	渝 开 发	000514	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000516	国际医学	000516	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000517	荣安地产	000517	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000519	中兵红箭	000519	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000520	凤凰航运	000520	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000521	长虹美菱	000521	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000523	红棉股份	000523	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000524	岭南控股	000524	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000525	红太阳	000525	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000526	学大教育	000526	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000528	柳    工	000528	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000529	广弘控股	000529	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000530	冰山冷热	000530	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000531	穗恒运Ａ	000531	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000532	华金资本	000532	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000533	顺钠股份	000533	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000534	万泽股份	000534	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000536	华映科技	000536	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000537	中绿电	000537	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000538	云南白药	000538	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000539	粤电力Ａ	000539	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000541	佛山照明	000541	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000543	皖能电力	000543	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000544	中原环保	000544	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000545	金浦钛业	000545	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000546	金圆股份	000546	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000547	航天发展	000547	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000548	湖南投资	000548	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000550	江铃汽车	000550	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000551	创元科技	000551	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000552	甘肃能化	000552	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000553	安道麦A	000553	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000554	泰山石油	000554	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000555	神州信息	000555	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000557	西部创业	000557	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000558	天府文旅	000558	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000559	万向钱潮	000559	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000560	我爱我家	000560	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000561	烽火电子	000561	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000563	陕国投Ａ	000563	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000564	供销大集	000564	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000565	渝三峡Ａ	000565	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000566	海南海药	000566	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000567	海德股份	000567	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000568	泸州老窖	000568	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000570	苏常柴Ａ	000570	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000571	新大洲A	000571	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000572	海马汽车	000572	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000573	粤宏远Ａ	000573	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000576	甘化科工	000576	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000581	威孚高科	000581	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000582	北部湾港	000582	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000586	汇源通信	000586	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000589	贵州轮胎	000589	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000590	启迪药业	000590	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000591	太阳能	000591	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000592	平潭发展	000592	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000593	德龙汇能	000593	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000596	古井贡酒	000596	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000597	东北制药	000597	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000598	兴蓉环境	000598	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000599	青岛双星	000599	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000600	建投能源	000600	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000601	韶能股份	000601	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000603	盛达资源	000603	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000605	渤海股份	000605	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000607	华媒控股	000607	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000610	西安旅游	000610	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000612	焦作万方	000612	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000617	中油资本	000617	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000619	海螺新材	000619	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000620	新华联	000620	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000623	吉林敖东	000623	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000625	长安汽车	000625	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000626	远大控股	000626	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000627	天茂集团	000627	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000628	高新发展	000628	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000629	钒钛股份	000629	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000630	铜陵有色	000630	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000631	顺发恒业	000631	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000632	三木集团	000632	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000633	合金投资	000633	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000635	英 力 特	000635	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000636	风华高科	000636	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000637	茂化实华	000637	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000639	西王食品	000639	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000650	仁和药业	000650	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000651	格力电器	000651	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000652	泰达股份	000652	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000655	金岭矿业	000655	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000657	中钨高新	000657	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000659	珠海中富	000659	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000661	长春高新	000661	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000663	永安林业	000663	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000665	湖北广电	000665	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000670	盈方微	000670	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000672	上峰水泥	000672	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000676	智度股份	000676	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000677	恒天海龙	000677	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000678	襄阳轴承	000678	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000679	大连友谊	000679	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000680	山推股份	000680	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000681	视觉中国	000681	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000682	东方电子	000682	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000683	博源化工	000683	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000685	中山公用	000685	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000686	东北证券	000686	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000688	国城矿业	000688	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000690	宝新能源	000690	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000692	惠天热电	000692	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000695	滨海能源	000695	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000698	沈阳化工	000698	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000700	模塑科技	000700	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000701	厦门信达	000701	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000702	正虹科技	000702	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000703	恒逸石化	000703	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000705	浙江震元	000705	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000707	双环科技	000707	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000708	中信特钢	000708	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000709	河钢股份	000709	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000710	贝瑞基因	000710	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000712	锦龙股份	000712	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000713	丰乐种业	000713	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000715	中兴商业	000715	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000716	黑芝麻	000716	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000717	中南股份	000717	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000718	苏宁环球	000718	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000719	中原传媒	000719	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000720	新能泰山	000720	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000721	西安饮食	000721	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000722	湖南发展	000722	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000723	美锦能源	000723	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000725	京东方Ａ	000725	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000726	鲁  泰Ａ	000726	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000727	冠捷科技	000727	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000728	国元证券	000728	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000729	燕京啤酒	000729	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000731	四川美丰	000731	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000733	振华科技	000733	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000735	罗 牛 山	000735	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000737	北方铜业	000737	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000738	航发控制	000738	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000739	普洛药业	000739	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000750	国海证券	000750	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000751	锌业股份	000751	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000753	漳州发展	000753	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000755	山西高速	000755	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000756	新华制药	000756	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000757	浩物股份	000757	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000758	中色股份	000758	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000759	中百集团	000759	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000761	本钢板材	000761	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000762	西藏矿业	000762	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000766	通化金马	000766	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000767	晋控电力	000767	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000768	中航西飞	000768	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000776	广发证券	000776	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000777	中核科技	000777	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000778	新兴铸管	000778	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000779	甘咨询	000779	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000782	恒申新材	000782	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000783	长江证券	000783	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000785	居然智家	000785	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000786	北新建材	000786	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000788	北大医药	000788	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000789	万年青	000789	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000790	华神科技	000790	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000791	甘肃能源	000791	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000792	盐湖股份	000792	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000795	英洛华	000795	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000796	凯撒旅业	000796	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000797	中国武夷	000797	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000798	中水渔业	000798	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000799	酒鬼酒	000799	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000800	一汽解放	000800	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000801	四川九洲	000801	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000802	北京文化	000802	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000803	山高环能	000803	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000807	云铝股份	000807	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000809	和展能源	000809	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000810	创维数字	000810	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000811	冰轮环境	000811	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000812	陕西金叶	000812	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000813	德展健康	000813	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000815	美利云	000815	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000816	智慧农业	000816	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000818	航锦科技	000818	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000819	岳阳兴长	000819	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000821	京山轻机	000821	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000822	山东海化	000822	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000823	超声电子	000823	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000825	太钢不锈	000825	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000826	启迪环境	000826	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000828	东莞控股	000828	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000829	天音控股	000829	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000830	鲁西化工	000830	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000831	中国稀土	000831	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000833	粤桂股份	000833	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000837	秦川机床	000837	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000838	财信发展	000838	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
688172	燕东微	688172	科创板	SSE	L	2025-06-17 15:57:24.280455
000839	中信国安	000839	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000848	承德露露	000848	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000850	华茂股份	000850	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000852	石化机械	000852	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000856	冀东装备	000856	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000858	五 粮 液	000858	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000859	国风新材	000859	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000860	顺鑫农业	000860	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000862	银星能源	000862	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000863	三湘印象	000863	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000868	安凯客车	000868	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000869	张  裕Ａ	000869	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000875	吉电股份	000875	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000876	新 希 望	000876	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000877	天山股份	000877	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000878	云南铜业	000878	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000880	潍柴重机	000880	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000881	中广核技	000881	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000882	华联股份	000882	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000883	湖北能源	000883	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000885	城发环境	000885	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000886	海南高速	000886	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000887	中鼎股份	000887	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000888	峨眉山Ａ	000888	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000889	中嘉博创	000889	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000890	法尔胜	000890	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000892	欢瑞世纪	000892	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000893	亚钾国际	000893	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000895	双汇发展	000895	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000897	津滨发展	000897	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000898	鞍钢股份	000898	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000899	赣能股份	000899	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000900	现代投资	000900	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000901	航天科技	000901	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000902	新洋丰	000902	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000903	云内动力	000903	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000905	厦门港务	000905	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000906	浙商中拓	000906	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000910	大亚圣象	000910	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000911	广农糖业	000911	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000912	泸天化	000912	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000913	钱江摩托	000913	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000915	华特达因	000915	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000917	电广传媒	000917	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000919	金陵药业	000919	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000920	沃顿科技	000920	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000921	海信家电	000921	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000922	佳电股份	000922	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000923	河钢资源	000923	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000925	众合科技	000925	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000926	福星股份	000926	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000927	中国铁物	000927	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000928	中钢国际	000928	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000930	中粮科技	000930	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000931	中 关 村	000931	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000932	华菱钢铁	000932	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000933	神火股份	000933	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000935	四川双马	000935	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000936	华西股份	000936	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000937	冀中能源	000937	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000938	紫光股份	000938	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000948	南天信息	000948	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000949	新乡化纤	000949	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000950	重药控股	000950	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000951	中国重汽	000951	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000952	广济药业	000952	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000953	河化股份	000953	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000955	欣龙控股	000955	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000957	中通客车	000957	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000958	电投产融	000958	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000959	首钢股份	000959	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000960	锡业股份	000960	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000962	东方钽业	000962	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000963	华东医药	000963	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000965	天保基建	000965	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000966	长源电力	000966	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000967	盈峰环境	000967	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000968	蓝焰控股	000968	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000969	安泰科技	000969	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000970	中科三环	000970	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000973	佛塑科技	000973	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000975	山金国际	000975	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000977	浪潮信息	000977	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000978	桂林旅游	000978	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000980	众泰汽车	000980	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000981	山子高科	000981	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000983	山西焦煤	000983	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000985	大庆华科	000985	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000987	越秀资本	000987	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000988	华工科技	000988	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000989	九芝堂	000989	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000990	诚志股份	000990	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000993	闽东电力	000993	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000995	皇台酒业	000995	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000997	新 大 陆	000997	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000998	隆平高科	000998	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
000999	华润三九	000999	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001201	东瑞股份	001201	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001202	炬申股份	001202	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001203	大中矿业	001203	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001205	盛航股份	001205	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001206	依依股份	001206	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001207	联科科技	001207	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001208	华菱线缆	001208	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001209	洪兴股份	001209	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001210	金房能源	001210	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001211	双枪科技	001211	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001212	中旗新材	001212	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001213	中铁特货	001213	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001215	千味央厨	001215	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001216	华瓷股份	001216	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001217	华尔泰	001217	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001218	丽臣实业	001218	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001219	青岛食品	001219	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001222	源飞宠物	001222	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001223	欧克科技	001223	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001225	和泰机电	001225	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001226	拓山重工	001226	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001227	兰州银行	001227	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001228	永泰运	001228	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001229	魅视科技	001229	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001230	劲旅环境	001230	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001231	农心科技	001231	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001234	泰慕士	001234	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001236	弘业期货	001236	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001238	浙江正特	001238	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001239	永达股份	001239	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001255	博菲电气	001255	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001256	炜冈科技	001256	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001258	立新能源	001258	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001259	利仁科技	001259	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001260	坤泰股份	001260	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001266	宏英智能	001266	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001267	汇绿生态	001267	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001268	联合精密	001268	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001269	欧晶科技	001269	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001277	速达股份	001277	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001278	一彬科技	001278	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001279	强邦新材	001279	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001282	三联锻造	001282	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001283	豪鹏科技	001283	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001286	陕西能源	001286	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001287	中电港	001287	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001288	运机集团	001288	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001289	龙源电力	001289	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001296	长江材料	001296	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001298	好上好	001298	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001299	美能能源	001299	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001300	三柏硕	001300	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001301	尚太科技	001301	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001306	夏厦精密	001306	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001308	康冠科技	001308	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001309	德明利	001309	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001311	多利科技	001311	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001313	粤海饲料	001313	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001314	亿道信息	001314	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001316	润贝航科	001316	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001317	三羊马	001317	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001318	阳光乳业	001318	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001319	铭科精技	001319	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001322	箭牌家居	001322	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001323	慕思股份	001323	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001324	长青科技	001324	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001326	联域股份	001326	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001328	登康口腔	001328	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001330	博纳影业	001330	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001331	胜通能源	001331	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001332	锡装股份	001332	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001333	光华股份	001333	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001335	信凯科技	001335	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001336	楚环科技	001336	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001337	四川黄金	001337	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001338	永顺泰	001338	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001339	智微智能	001339	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001356	富岭股份	001356	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001358	兴欣新材	001358	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001359	平安电工	001359	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001360	南矿集团	001360	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001366	播恩集团	001366	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001367	海森药业	001367	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001368	通达创智	001368	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001373	翔腾新材	001373	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001376	百通能源	001376	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001378	德冠新材	001378	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001379	腾达科技	001379	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001380	华纬科技	001380	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001382	新亚电缆	001382	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001387	雪祺电气	001387	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001389	广合科技	001389	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001390	古麒绒材	001390	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001391	国货航	001391	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001395	亚联机械	001395	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001400	江顺科技	001400	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001696	宗申动力	001696	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001872	招商港口	001872	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001896	豫能控股	001896	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001914	招商积余	001914	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001965	招商公路	001965	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
001979	招商蛇口	001979	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002001	新 和 成	002001	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002003	伟星股份	002003	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002004	华邦健康	002004	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002006	精工科技	002006	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002007	华兰生物	002007	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002008	大族激光	002008	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002009	天奇股份	002009	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002010	传化智联	002010	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002011	盾安环境	002011	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002012	凯恩股份	002012	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002014	永新股份	002014	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002015	协鑫能科	002015	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002016	世荣兆业	002016	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002017	东信和平	002017	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002019	亿帆医药	002019	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002020	京新药业	002020	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002021	中捷资源	002021	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002022	科华生物	002022	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002023	海特高新	002023	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002025	航天电器	002025	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002026	山东威达	002026	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002027	分众传媒	002027	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002028	思源电气	002028	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002029	七 匹 狼	002029	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002030	达安基因	002030	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002031	巨轮智能	002031	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002032	苏 泊 尔	002032	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002033	丽江股份	002033	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002034	旺能环境	002034	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002035	华帝股份	002035	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002036	联创电子	002036	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002037	保利联合	002037	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002038	双鹭药业	002038	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002039	黔源电力	002039	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002040	南 京 港	002040	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002041	登海种业	002041	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002042	华孚时尚	002042	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002043	兔 宝 宝	002043	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002044	美年健康	002044	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002045	国光电器	002045	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002046	国机精工	002046	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002048	宁波华翔	002048	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002049	紫光国微	002049	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002050	三花智控	002050	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002051	中工国际	002051	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002053	云南能投	002053	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002054	德美化工	002054	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002055	得润电子	002055	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002056	横店东磁	002056	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002057	中钢天源	002057	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002059	云南旅游	002059	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002060	广东建工	002060	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002061	浙江交科	002061	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002062	宏润建设	002062	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002063	远光软件	002063	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002064	华峰化学	002064	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002065	东华软件	002065	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002066	瑞泰科技	002066	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002067	景兴纸业	002067	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002068	黑猫股份	002068	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002069	獐子岛	002069	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002072	凯瑞德	002072	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002073	软控股份	002073	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002074	国轩高科	002074	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002075	沙钢股份	002075	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002077	大港股份	002077	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002078	太阳纸业	002078	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002079	苏州固锝	002079	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002080	中材科技	002080	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002081	金 螳 螂	002081	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002082	万邦德	002082	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002083	孚日股份	002083	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002084	海鸥住工	002084	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002085	万丰奥威	002085	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002086	东方海洋	002086	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002088	鲁阳节能	002088	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002090	金智科技	002090	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002091	江苏国泰	002091	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002092	中泰化学	002092	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002093	国脉科技	002093	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002094	青岛金王	002094	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002095	生 意 宝	002095	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002096	易普力	002096	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002097	山河智能	002097	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002098	浔兴股份	002098	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002099	海翔药业	002099	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002100	天康生物	002100	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002101	广东鸿图	002101	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002102	能特科技	002102	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002103	广博股份	002103	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002104	恒宝股份	002104	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002105	信隆健康	002105	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002106	莱宝高科	002106	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002107	沃华医药	002107	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002108	沧州明珠	002108	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002109	兴化股份	002109	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002110	三钢闽光	002110	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002111	威海广泰	002111	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002112	三变科技	002112	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002114	罗平锌电	002114	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002115	三维通信	002115	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002116	中国海诚	002116	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002117	东港股份	002117	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002119	康强电子	002119	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002120	韵达股份	002120	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002121	科陆电子	002121	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002122	汇洲智能	002122	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002123	梦网科技	002123	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002124	天邦食品	002124	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002125	湘潭电化	002125	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002126	银轮股份	002126	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002127	南极电商	002127	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002128	电投能源	002128	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002129	TCL中环	002129	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002130	沃尔核材	002130	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002131	利欧股份	002131	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002132	恒星科技	002132	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002133	广宇集团	002133	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002134	天津普林	002134	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002135	东南网架	002135	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002136	安 纳 达	002136	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002137	实益达	002137	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002138	顺络电子	002138	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002139	拓邦股份	002139	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002140	东华科技	002140	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002142	宁波银行	002142	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002144	宏达高科	002144	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002145	中核钛白	002145	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002146	荣盛发展	002146	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002148	北纬科技	002148	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002149	西部材料	002149	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002150	通润装备	002150	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002151	北斗星通	002151	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002152	广电运通	002152	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002153	石基信息	002153	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002154	报 喜 鸟	002154	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002155	湖南黄金	002155	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002156	通富微电	002156	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002157	正邦科技	002157	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002158	汉钟精机	002158	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002159	三特索道	002159	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002160	常铝股份	002160	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002161	远 望 谷	002161	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002162	悦心健康	002162	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002163	海南发展	002163	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002164	宁波东力	002164	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002165	红 宝 丽	002165	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002166	莱茵生物	002166	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002167	东方锆业	002167	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002169	智光电气	002169	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002170	芭田股份	002170	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002171	楚江新材	002171	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002172	澳洋健康	002172	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002173	创新医疗	002173	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002174	游族网络	002174	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002175	东方智造	002175	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002176	江特电机	002176	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002177	御银股份	002177	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002178	延华智能	002178	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002179	中航光电	002179	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002180	纳思达	002180	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002181	粤 传 媒	002181	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002182	宝武镁业	002182	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002183	怡 亚 通	002183	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002184	海得控制	002184	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002185	华天科技	002185	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002186	全 聚 德	002186	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002187	广百股份	002187	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002188	中天服务	002188	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002189	中光学	002189	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002190	成飞集成	002190	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002191	劲嘉股份	002191	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002192	融捷股份	002192	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002193	如意集团	002193	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002194	武汉凡谷	002194	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002195	岩山科技	002195	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002196	方正电机	002196	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002198	嘉应制药	002198	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002201	九鼎新材	002201	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002202	金风科技	002202	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002203	海亮股份	002203	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002204	大连重工	002204	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002205	国统股份	002205	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002206	海 利 得	002206	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002207	准油股份	002207	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002208	合肥城建	002208	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002209	达 意 隆	002209	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002210	飞马国际	002210	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002212	天融信	002212	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002213	大为股份	002213	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002215	诺 普 信	002215	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002216	三全食品	002216	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002218	拓日新能	002218	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002219	新里程	002219	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002221	东华能源	002221	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002222	福晶科技	002222	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002223	鱼跃医疗	002223	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002224	三 力 士	002224	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002225	濮耐股份	002225	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002226	江南化工	002226	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002227	奥 特 迅	002227	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002228	合兴包装	002228	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002229	鸿博股份	002229	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002230	科大讯飞	002230	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002232	启明信息	002232	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002233	塔牌集团	002233	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002234	民和股份	002234	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002235	安妮股份	002235	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002236	大华股份	002236	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002237	恒邦股份	002237	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002238	天威视讯	002238	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002239	奥特佳	002239	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002240	盛新锂能	002240	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002241	歌尔股份	002241	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002242	九阳股份	002242	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002243	力合科创	002243	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002244	滨江集团	002244	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002245	蔚蓝锂芯	002245	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002246	北化股份	002246	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002247	聚力文化	002247	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002248	华东数控	002248	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002249	大洋电机	002249	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002250	联化科技	002250	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002251	步步高	002251	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002252	上海莱士	002252	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002254	泰和新材	002254	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002255	海陆重工	002255	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002256	兆新股份	002256	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002258	利尔化学	002258	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002259	升达林业	002259	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002261	拓维信息	002261	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002262	恩华药业	002262	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002263	大东南	002263	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002264	新 华 都	002264	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002265	建设工业	002265	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002266	浙富控股	002266	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002267	陕天然气	002267	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002268	电科网安	002268	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002269	美邦服饰	002269	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002270	华明装备	002270	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002271	东方雨虹	002271	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002272	川润股份	002272	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002273	水晶光电	002273	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002274	华昌化工	002274	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002275	桂林三金	002275	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002276	万马股份	002276	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002277	友阿股份	002277	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002278	神开股份	002278	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002279	久其软件	002279	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002281	光迅科技	002281	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002282	博深股份	002282	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002283	天润工业	002283	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002284	亚太股份	002284	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002285	世联行	002285	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002286	保龄宝	002286	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002287	奇正藏药	002287	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002290	禾盛新材	002290	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002291	遥望科技	002291	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002292	奥飞娱乐	002292	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002293	罗莱生活	002293	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002294	信立泰	002294	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002295	精艺股份	002295	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002296	辉煌科技	002296	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002297	博云新材	002297	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002298	中电鑫龙	002298	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002299	圣农发展	002299	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002300	太阳电缆	002300	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002301	齐心集团	002301	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002302	西部建设	002302	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002303	美盈森	002303	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002304	洋河股份	002304	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002307	北新路桥	002307	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002310	东方园林	002310	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002311	海大集团	002311	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002312	川发龙蟒	002312	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002313	日海智能	002313	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002314	南山控股	002314	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002315	焦点科技	002315	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002317	众生药业	002317	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002318	久立特材	002318	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002319	乐通股份	002319	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002320	海峡股份	002320	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002321	华英农业	002321	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002322	理工能科	002322	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002323	雅博股份	002323	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002324	普利特	002324	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002326	永太科技	002326	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002327	富安娜	002327	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002328	新朋股份	002328	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002329	皇氏集团	002329	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002330	得利斯	002330	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002331	皖通科技	002331	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002332	仙琚制药	002332	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002333	罗普斯金	002333	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002334	英威腾	002334	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002335	科华数据	002335	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002337	赛象科技	002337	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002338	奥普光电	002338	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002339	积成电子	002339	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002340	格林美	002340	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002342	巨力索具	002342	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002343	慈文传媒	002343	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002344	海宁皮城	002344	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002345	潮宏基	002345	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002346	柘中股份	002346	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002347	泰尔股份	002347	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002348	高乐股份	002348	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002349	精华制药	002349	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002350	北京科锐	002350	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002351	漫步者	002351	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002352	顺丰控股	002352	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002353	杰瑞股份	002353	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002354	天娱数科	002354	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002355	兴民智通	002355	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002356	赫美集团	002356	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002357	富临运业	002357	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002358	森源电气	002358	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002360	同德化工	002360	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002361	神剑股份	002361	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002362	汉王科技	002362	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002363	隆基机械	002363	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002364	中恒电气	002364	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002365	永安药业	002365	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002366	融发核电	002366	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002367	康力电梯	002367	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002368	太极股份	002368	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002369	卓翼科技	002369	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002370	亚太药业	002370	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002371	北方华创	002371	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002372	伟星新材	002372	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002373	千方科技	002373	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002374	中锐股份	002374	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002375	亚厦股份	002375	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002376	新北洋	002376	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002377	国创高新	002377	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002378	章源钨业	002378	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002379	宏创控股	002379	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002380	科远智慧	002380	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002381	双箭股份	002381	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002382	蓝帆医疗	002382	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002383	合众思壮	002383	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002384	东山精密	002384	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002385	大北农	002385	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002386	天原股份	002386	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002387	维信诺	002387	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002389	航天彩虹	002389	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002390	信邦制药	002390	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002391	长青股份	002391	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002392	北京利尔	002392	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002393	力生制药	002393	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002394	联发股份	002394	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002395	双象股份	002395	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002396	星网锐捷	002396	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002397	梦洁股份	002397	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002398	垒知集团	002398	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002399	海普瑞	002399	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002400	省广集团	002400	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002401	中远海科	002401	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002402	和而泰	002402	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002403	爱仕达	002403	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002404	嘉欣丝绸	002404	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002405	四维图新	002405	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002406	远东传动	002406	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002407	多氟多	002407	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002408	齐翔腾达	002408	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002409	雅克科技	002409	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002410	广联达	002410	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002412	汉森制药	002412	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002413	雷科防务	002413	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002414	高德红外	002414	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002415	海康威视	002415	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002416	爱施德	002416	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002418	康盛股份	002418	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002419	天虹股份	002419	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002420	毅昌科技	002420	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002421	达实智能	002421	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002422	科伦药业	002422	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002423	中粮资本	002423	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002426	胜利精密	002426	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002427	尤夫股份	002427	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002428	云南锗业	002428	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002429	兆驰股份	002429	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002430	杭氧股份	002430	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002431	棕榈股份	002431	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002432	九安医疗	002432	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002434	万里扬	002434	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002436	兴森科技	002436	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002437	誉衡药业	002437	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002438	江苏神通	002438	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002439	启明星辰	002439	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002440	闰土股份	002440	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002441	众业达	002441	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002442	龙星科技	002442	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002443	金洲管道	002443	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002444	巨星科技	002444	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002445	中南文化	002445	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002446	盛路通信	002446	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002448	中原内配	002448	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002449	国星光电	002449	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002451	摩恩电气	002451	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002452	长高电新	002452	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002453	华软科技	002453	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002454	松芝股份	002454	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002455	百川股份	002455	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002456	欧菲光	002456	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002457	青龙管业	002457	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002458	益生股份	002458	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002459	晶澳科技	002459	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002460	赣锋锂业	002460	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002461	珠江啤酒	002461	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002462	嘉事堂	002462	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002463	沪电股份	002463	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002465	海格通信	002465	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002466	天齐锂业	002466	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002467	二六三	002467	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002468	申通快递	002468	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002469	三维化学	002469	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002470	金正大	002470	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002471	中超控股	002471	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002472	双环传动	002472	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002474	榕基软件	002474	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002475	立讯精密	002475	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002476	宝莫股份	002476	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002478	常宝股份	002478	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002479	富春环保	002479	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002480	新筑股份	002480	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002481	双塔食品	002481	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002482	广田集团	002482	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002483	润邦股份	002483	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002484	江海股份	002484	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002486	嘉麟杰	002486	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002487	大金重工	002487	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002488	金固股份	002488	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002489	浙江永强	002489	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002490	山东墨龙	002490	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002491	通鼎互联	002491	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002492	恒基达鑫	002492	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002493	荣盛石化	002493	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002494	华斯股份	002494	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002495	佳隆股份	002495	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002497	雅化集团	002497	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002498	汉缆股份	002498	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002500	山西证券	002500	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002501	利源股份	002501	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002506	协鑫集成	002506	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002507	涪陵榨菜	002507	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002508	老板电器	002508	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002510	天汽模	002510	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002511	中顺洁柔	002511	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002512	达华智能	002512	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002513	蓝丰生化	002513	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002514	宝馨科技	002514	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002515	金字火腿	002515	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002516	旷达科技	002516	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002517	恺英网络	002517	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002518	科士达	002518	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002519	银河电子	002519	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002520	日发精机	002520	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002521	齐峰新材	002521	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002522	浙江众成	002522	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002523	天桥起重	002523	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002524	光正眼科	002524	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002526	山东矿机	002526	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002527	新时达	002527	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002530	金财互联	002530	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002531	天顺风能	002531	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002532	天山铝业	002532	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002533	金杯电工	002533	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002534	西子洁能	002534	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002535	林州重机	002535	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002536	飞龙股份	002536	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002537	海联金汇	002537	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002538	司尔特	002538	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002539	云图控股	002539	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002540	亚太科技	002540	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002541	鸿路钢构	002541	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002542	中化岩土	002542	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002543	万和电气	002543	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002544	普天科技	002544	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002545	东方铁塔	002545	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002546	新联电子	002546	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002547	春兴精工	002547	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002548	金新农	002548	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002549	凯美特气	002549	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002550	千红制药	002550	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002551	尚荣医疗	002551	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002552	宝鼎科技	002552	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002553	南方精工	002553	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002554	惠博普	002554	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002555	三七互娱	002555	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002556	辉隆股份	002556	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002557	洽洽食品	002557	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002558	巨人网络	002558	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002559	亚威股份	002559	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002560	通达股份	002560	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002561	徐家汇	002561	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002562	兄弟科技	002562	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002563	森马服饰	002563	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002564	天沃科技	002564	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002565	顺灏股份	002565	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002566	益盛药业	002566	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002567	唐人神	002567	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002568	百润股份	002568	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002570	贝因美	002570	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002571	德力股份	002571	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002572	索菲亚	002572	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002573	清新环境	002573	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002574	明牌珠宝	002574	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002575	群兴玩具	002575	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002576	通达动力	002576	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002577	雷柏科技	002577	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002578	闽发铝业	002578	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002579	中京电子	002579	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002580	圣阳股份	002580	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002581	未名医药	002581	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002582	好想你	002582	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002583	海能达	002583	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002584	西陇科学	002584	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002585	双星新材	002585	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002587	奥拓电子	002587	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002588	史丹利	002588	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002589	瑞康医药	002589	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002590	万安科技	002590	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002591	恒大高新	002591	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002593	日上集团	002593	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002594	比亚迪	002594	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002595	豪迈科技	002595	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002596	海南瑞泽	002596	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002597	金禾实业	002597	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002598	山东章鼓	002598	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002599	盛通股份	002599	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002600	领益智造	002600	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002601	龙佰集团	002601	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002603	以岭药业	002603	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002605	姚记科技	002605	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002606	大连电瓷	002606	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002607	中公教育	002607	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002608	江苏国信	002608	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002609	捷顺科技	002609	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002611	东方精工	002611	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002612	朗姿股份	002612	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002613	北玻股份	002613	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002614	奥佳华	002614	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002615	哈尔斯	002615	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002616	长青集团	002616	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002617	露笑科技	002617	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002622	皓宸医疗	002622	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002623	亚玛顿	002623	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002624	完美世界	002624	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002625	光启技术	002625	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002626	金达威	002626	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002627	三峡旅游	002627	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002628	成都路桥	002628	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002629	仁智股份	002629	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002631	德尔未来	002631	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002632	道明光学	002632	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002633	申科股份	002633	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002634	棒杰股份	002634	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002635	安洁科技	002635	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002636	金安国纪	002636	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002637	赞宇科技	002637	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002638	勤上股份	002638	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002639	雪人股份	002639	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002640	跨境通	002640	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002641	公元股份	002641	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002642	荣联科技	002642	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002643	万润股份	002643	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002644	佛慈制药	002644	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002645	华宏科技	002645	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002646	天佑德酒	002646	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002648	卫星化学	002648	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002649	博彦科技	002649	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002651	利君股份	002651	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002652	扬子新材	002652	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002653	海思科	002653	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002654	万润科技	002654	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002655	共达电声	002655	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002657	中科金财	002657	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002658	雪迪龙	002658	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002659	凯文教育	002659	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002660	茂硕电源	002660	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002661	克明食品	002661	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002662	峰璟股份	002662	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002663	普邦股份	002663	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002664	信质集团	002664	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002666	德联集团	002666	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002667	威领股份	002667	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002668	TCL智家	002668	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002669	康达新材	002669	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002670	国盛金控	002670	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002671	龙泉股份	002671	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002672	东江环保	002672	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002673	西部证券	002673	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002674	兴业科技	002674	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002675	东诚药业	002675	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002676	顺威股份	002676	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002677	浙江美大	002677	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002678	珠江钢琴	002678	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002679	福建金森	002679	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002681	奋达科技	002681	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002682	龙洲股份	002682	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002683	广东宏大	002683	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002685	华东重机	002685	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002686	亿利达	002686	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002687	乔治白	002687	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002688	金河生物	002688	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002689	远大智能	002689	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002690	美亚光电	002690	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002691	冀凯股份	002691	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002692	远程股份	002692	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002694	顾地科技	002694	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002695	煌上煌	002695	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002696	百洋股份	002696	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002697	红旗连锁	002697	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002698	博实股份	002698	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002700	新疆浩源	002700	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002701	奥瑞金	002701	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002702	海欣食品	002702	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002703	浙江世宝	002703	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002705	新宝股份	002705	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002706	良信股份	002706	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002707	众信旅游	002707	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002708	光洋股份	002708	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002709	天赐材料	002709	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002712	思美传媒	002712	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002714	牧原股份	002714	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002715	登云股份	002715	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002716	湖南白银	002716	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002718	友邦吊顶	002718	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002719	麦趣尔	002719	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002721	金一文化	002721	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002722	物产金轮	002722	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002723	小崧股份	002723	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002724	海洋王	002724	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002725	跃岭股份	002725	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002726	龙大美食	002726	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002727	一心堂	002727	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002728	特一药业	002728	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002729	好利科技	002729	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002730	电光科技	002730	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002731	萃华珠宝	002731	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002732	燕塘乳业	002732	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002733	雄韬股份	002733	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002734	利民股份	002734	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002735	王子新材	002735	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002736	国信证券	002736	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002737	葵花药业	002737	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002738	中矿资源	002738	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002739	万达电影	002739	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002741	光华科技	002741	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002743	富煌钢构	002743	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002745	木林森	002745	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002746	仙坛股份	002746	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002747	埃斯顿	002747	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002748	世龙实业	002748	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002749	国光股份	002749	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002752	昇兴股份	002752	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002753	永东股份	002753	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002755	奥赛康	002755	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002756	永兴材料	002756	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002757	南兴股份	002757	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002758	浙农股份	002758	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002759	天际股份	002759	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002760	凤形股份	002760	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002761	浙江建投	002761	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002763	汇洁股份	002763	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002765	蓝黛科技	002765	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002766	索菱股份	002766	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002767	先锋电子	002767	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002768	国恩股份	002768	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002769	普路通	002769	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002771	真视通	002771	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002772	众兴菌业	002772	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002773	康弘药业	002773	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002774	快意电梯	002774	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002775	文科股份	002775	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002777	久远银海	002777	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002778	中晟高科	002778	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002779	中坚科技	002779	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002780	三夫户外	002780	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002782	可立克	002782	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002783	凯龙股份	002783	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002785	万里石	002785	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002786	银宝山新	002786	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002787	华源控股	002787	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002788	鹭燕医药	002788	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002790	瑞尔特	002790	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002791	坚朗五金	002791	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002792	通宇通讯	002792	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002793	罗欣药业	002793	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002795	永和智控	002795	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002796	世嘉科技	002796	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002797	第一创业	002797	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002798	帝欧家居	002798	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002799	环球印务	002799	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002800	天顺股份	002800	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002801	微光股份	002801	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002802	洪汇新材	002802	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002803	吉宏股份	002803	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002805	丰元股份	002805	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002806	华锋股份	002806	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002807	江阴银行	002807	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002809	红墙股份	002809	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002810	山东赫达	002810	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002811	郑中设计	002811	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002812	恩捷股份	002812	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002813	路畅科技	002813	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002815	崇达技术	002815	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002817	黄山胶囊	002817	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002818	富森美	002818	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002819	东方中科	002819	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002820	桂发祥	002820	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002821	凯莱英	002821	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002823	凯中精密	002823	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002824	和胜股份	002824	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002825	纳尔股份	002825	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002826	易明医药	002826	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002827	高争民爆	002827	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002828	贝肯能源	002828	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002829	星网宇达	002829	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002830	名雕股份	002830	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002831	裕同科技	002831	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002832	比音勒芬	002832	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002833	弘亚数控	002833	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002835	同为股份	002835	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002836	新宏泽	002836	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002837	英维克	002837	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002838	道恩股份	002838	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002839	张家港行	002839	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002840	华统股份	002840	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002841	视源股份	002841	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002842	翔鹭钨业	002842	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002843	泰嘉股份	002843	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002845	同兴达	002845	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002846	英联股份	002846	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002847	盐津铺子	002847	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002849	威星智能	002849	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002850	科达利	002850	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002851	麦格米特	002851	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002852	道道全	002852	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002853	皮阿诺	002853	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002855	捷荣技术	002855	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002856	美芝股份	002856	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002857	三晖电气	002857	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002858	力盛体育	002858	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002859	洁美科技	002859	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002860	星帅尔	002860	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002861	瀛通通讯	002861	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002862	实丰文化	002862	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002863	今飞凯达	002863	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002864	盘龙药业	002864	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002865	钧达股份	002865	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002866	传艺科技	002866	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002867	周大生	002867	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002869	金溢科技	002869	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002870	香山股份	002870	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002871	伟隆股份	002871	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002873	新天药业	002873	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002875	安奈儿	002875	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002876	三利谱	002876	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002877	智能自控	002877	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002878	元隆雅图	002878	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002879	长缆科技	002879	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002880	卫光生物	002880	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002881	美格智能	002881	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002882	金龙羽	002882	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002883	中设股份	002883	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002884	凌霄泵业	002884	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002885	京泉华	002885	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002886	沃特股份	002886	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002887	绿茵生态	002887	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002888	惠威科技	002888	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002889	东方嘉盛	002889	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002890	弘宇股份	002890	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002891	中宠股份	002891	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002892	科力尔	002892	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002893	京能热力	002893	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002895	川恒股份	002895	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002896	中大力德	002896	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002897	意华股份	002897	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002899	英派斯	002899	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002900	哈三联	002900	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002901	大博医疗	002901	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002902	铭普光磁	002902	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002903	宇环数控	002903	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002905	金逸影视	002905	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002906	华阳集团	002906	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002907	华森制药	002907	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002908	德生科技	002908	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002909	集泰股份	002909	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002910	庄园牧场	002910	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002911	佛燃能源	002911	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002912	中新赛克	002912	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002913	奥士康	002913	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002915	中欣氟材	002915	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002916	深南电路	002916	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002917	金奥博	002917	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002918	蒙娜丽莎	002918	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002919	名臣健康	002919	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002920	德赛西威	002920	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002921	联诚精密	002921	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002922	伊戈尔	002922	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002923	润都股份	002923	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002925	盈趣科技	002925	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002926	华西证券	002926	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002927	泰永长征	002927	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002928	华夏航空	002928	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002929	润建股份	002929	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002930	宏川智慧	002930	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002931	锋龙股份	002931	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002932	明德生物	002932	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002933	新兴装备	002933	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002935	天奥电子	002935	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002936	郑州银行	002936	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002937	兴瑞科技	002937	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002938	鹏鼎控股	002938	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002939	长城证券	002939	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002940	昂利康	002940	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002941	新疆交建	002941	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002942	新农股份	002942	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002943	宇晶股份	002943	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002945	华林证券	002945	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002946	新乳业	002946	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002947	恒铭达	002947	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002948	青岛银行	002948	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002949	华阳国际	002949	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002950	奥美医疗	002950	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002951	金时科技	002951	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002952	亚世光电	002952	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002953	日丰股份	002953	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002955	鸿合科技	002955	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002956	西麦食品	002956	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002957	科瑞技术	002957	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002958	青农商行	002958	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002959	小熊电器	002959	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002960	青鸟消防	002960	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002961	瑞达期货	002961	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002962	五方光电	002962	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002963	豪尔赛	002963	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002965	祥鑫科技	002965	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002966	苏州银行	002966	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002967	广电计量	002967	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002968	新大正	002968	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002969	嘉美包装	002969	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002970	锐明技术	002970	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002971	和远气体	002971	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002972	科安达	002972	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002973	侨银股份	002973	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002975	博杰股份	002975	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002976	瑞玛精密	002976	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002977	天箭科技	002977	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002978	安宁股份	002978	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002979	雷赛智能	002979	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002980	华盛昌	002980	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002981	朝阳科技	002981	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002982	湘佳股份	002982	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002983	芯瑞达	002983	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002984	森麒麟	002984	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002985	北摩高科	002985	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002986	宇新股份	002986	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002987	京北方	002987	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002988	豪美新材	002988	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002989	中天精装	002989	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002990	盛视科技	002990	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002991	甘源食品	002991	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002992	宝明科技	002992	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002993	奥海科技	002993	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002995	天地在线	002995	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002996	顺博合金	002996	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002997	瑞鹄模具	002997	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002998	优彩资源	002998	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
002999	天禾股份	002999	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003000	劲仔食品	003000	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003001	中岩大地	003001	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003002	壶化股份	003002	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003003	天元股份	003003	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003005	竞业达	003005	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003006	百亚股份	003006	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003007	直真科技	003007	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003008	开普检测	003008	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003009	中天火箭	003009	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003010	若羽臣	003010	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003011	海象新材	003011	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003012	东鹏控股	003012	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003013	地铁设计	003013	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003015	日久光电	003015	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003016	欣贺股份	003016	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003017	大洋生物	003017	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003018	金富科技	003018	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003019	宸展光电	003019	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003020	立方制药	003020	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003021	兆威机电	003021	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003022	联泓新科	003022	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003023	彩虹集团	003023	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003025	思进智能	003025	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003026	中晶科技	003026	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003027	同兴科技	003027	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003028	振邦智能	003028	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003029	吉大正元	003029	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003030	祖名股份	003030	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003031	中瓷电子	003031	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003033	征和工业	003033	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003035	南网能源	003035	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003036	泰坦股份	003036	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003037	三和管桩	003037	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003038	鑫铂股份	003038	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003039	顺控发展	003039	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003040	楚天龙	003040	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003041	真爱美家	003041	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003042	中农联合	003042	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003043	华亚智能	003043	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
003816	中国广核	003816	深交所主板	SZSE	L	2025-06-17 15:57:24.280455
300001	特锐德	300001	创业板	SZSE	L	2025-06-17 15:57:24.280455
300002	神州泰岳	300002	创业板	SZSE	L	2025-06-17 15:57:24.280455
300003	乐普医疗	300003	创业板	SZSE	L	2025-06-17 15:57:24.280455
300004	南风股份	300004	创业板	SZSE	L	2025-06-17 15:57:24.280455
300005	探路者	300005	创业板	SZSE	L	2025-06-17 15:57:24.280455
300006	莱美药业	300006	创业板	SZSE	L	2025-06-17 15:57:24.280455
300007	汉威科技	300007	创业板	SZSE	L	2025-06-17 15:57:24.280455
300008	天海防务	300008	创业板	SZSE	L	2025-06-17 15:57:24.280455
300009	安科生物	300009	创业板	SZSE	L	2025-06-17 15:57:24.280455
300010	豆神教育	300010	创业板	SZSE	L	2025-06-17 15:57:24.280455
300011	鼎汉技术	300011	创业板	SZSE	L	2025-06-17 15:57:24.280455
300012	华测检测	300012	创业板	SZSE	L	2025-06-17 15:57:24.280455
300013	新宁物流	300013	创业板	SZSE	L	2025-06-17 15:57:24.280455
300014	亿纬锂能	300014	创业板	SZSE	L	2025-06-17 15:57:24.280455
300015	爱尔眼科	300015	创业板	SZSE	L	2025-06-17 15:57:24.280455
300016	北陆药业	300016	创业板	SZSE	L	2025-06-17 15:57:24.280455
300017	网宿科技	300017	创业板	SZSE	L	2025-06-17 15:57:24.280455
300018	中元股份	300018	创业板	SZSE	L	2025-06-17 15:57:24.280455
300019	硅宝科技	300019	创业板	SZSE	L	2025-06-17 15:57:24.280455
300021	大禹节水	300021	创业板	SZSE	L	2025-06-17 15:57:24.280455
300022	吉峰科技	300022	创业板	SZSE	L	2025-06-17 15:57:24.280455
300024	机器人	300024	创业板	SZSE	L	2025-06-17 15:57:24.280455
300025	华星创业	300025	创业板	SZSE	L	2025-06-17 15:57:24.280455
300026	红日药业	300026	创业板	SZSE	L	2025-06-17 15:57:24.280455
300027	华谊兄弟	300027	创业板	SZSE	L	2025-06-17 15:57:24.280455
300030	阳普医疗	300030	创业板	SZSE	L	2025-06-17 15:57:24.280455
300031	宝通科技	300031	创业板	SZSE	L	2025-06-17 15:57:24.280455
300032	金龙机电	300032	创业板	SZSE	L	2025-06-17 15:57:24.280455
300033	同花顺	300033	创业板	SZSE	L	2025-06-17 15:57:24.280455
300034	钢研高纳	300034	创业板	SZSE	L	2025-06-17 15:57:24.280455
300035	中科电气	300035	创业板	SZSE	L	2025-06-17 15:57:24.280455
300036	超图软件	300036	创业板	SZSE	L	2025-06-17 15:57:24.280455
300037	新宙邦	300037	创业板	SZSE	L	2025-06-17 15:57:24.280455
300039	上海凯宝	300039	创业板	SZSE	L	2025-06-17 15:57:24.280455
300040	九洲集团	300040	创业板	SZSE	L	2025-06-17 15:57:24.280455
300041	回天新材	300041	创业板	SZSE	L	2025-06-17 15:57:24.280455
300042	朗科科技	300042	创业板	SZSE	L	2025-06-17 15:57:24.280455
300043	星辉娱乐	300043	创业板	SZSE	L	2025-06-17 15:57:24.280455
300045	华力创通	300045	创业板	SZSE	L	2025-06-17 15:57:24.280455
300046	台基股份	300046	创业板	SZSE	L	2025-06-17 15:57:24.280455
300047	天源迪科	300047	创业板	SZSE	L	2025-06-17 15:57:24.280455
300048	合康新能	300048	创业板	SZSE	L	2025-06-17 15:57:24.280455
300049	福瑞股份	300049	创业板	SZSE	L	2025-06-17 15:57:24.280455
300050	世纪鼎利	300050	创业板	SZSE	L	2025-06-17 15:57:24.280455
300051	琏升科技	300051	创业板	SZSE	L	2025-06-17 15:57:24.280455
300053	航宇微	300053	创业板	SZSE	L	2025-06-17 15:57:24.280455
300054	鼎龙股份	300054	创业板	SZSE	L	2025-06-17 15:57:24.280455
300055	万邦达	300055	创业板	SZSE	L	2025-06-17 15:57:24.280455
300056	中创环保	300056	创业板	SZSE	L	2025-06-17 15:57:24.280455
300057	万顺新材	300057	创业板	SZSE	L	2025-06-17 15:57:24.280455
300058	蓝色光标	300058	创业板	SZSE	L	2025-06-17 15:57:24.280455
300059	东方财富	300059	创业板	SZSE	L	2025-06-17 15:57:24.280455
300061	旗天科技	300061	创业板	SZSE	L	2025-06-17 15:57:24.280455
300062	中能电气	300062	创业板	SZSE	L	2025-06-17 15:57:24.280455
300063	天龙集团	300063	创业板	SZSE	L	2025-06-17 15:57:24.280455
300065	海兰信	300065	创业板	SZSE	L	2025-06-17 15:57:24.280455
300066	三川智慧	300066	创业板	SZSE	L	2025-06-17 15:57:24.280455
300067	安诺其	300067	创业板	SZSE	L	2025-06-17 15:57:24.280455
300068	南都电源	300068	创业板	SZSE	L	2025-06-17 15:57:24.280455
300069	金利华电	300069	创业板	SZSE	L	2025-06-17 15:57:24.280455
300070	碧水源	300070	创业板	SZSE	L	2025-06-17 15:57:24.280455
300071	福石控股	300071	创业板	SZSE	L	2025-06-17 15:57:24.280455
300072	海新能科	300072	创业板	SZSE	L	2025-06-17 15:57:24.280455
300073	当升科技	300073	创业板	SZSE	L	2025-06-17 15:57:24.280455
300074	华平股份	300074	创业板	SZSE	L	2025-06-17 15:57:24.280455
300075	数字政通	300075	创业板	SZSE	L	2025-06-17 15:57:24.280455
300076	GQY视讯	300076	创业板	SZSE	L	2025-06-17 15:57:24.280455
300077	国民技术	300077	创业板	SZSE	L	2025-06-17 15:57:24.280455
300078	思创医惠	300078	创业板	SZSE	L	2025-06-17 15:57:24.280455
300079	数码视讯	300079	创业板	SZSE	L	2025-06-17 15:57:24.280455
300080	易成新能	300080	创业板	SZSE	L	2025-06-17 15:57:24.280455
300081	恒信东方	300081	创业板	SZSE	L	2025-06-17 15:57:24.280455
300082	奥克股份	300082	创业板	SZSE	L	2025-06-17 15:57:24.280455
300083	创世纪	300083	创业板	SZSE	L	2025-06-17 15:57:24.280455
300084	海默科技	300084	创业板	SZSE	L	2025-06-17 15:57:24.280455
300085	银之杰	300085	创业板	SZSE	L	2025-06-17 15:57:24.280455
300086	康芝药业	300086	创业板	SZSE	L	2025-06-17 15:57:24.280455
300087	荃银高科	300087	创业板	SZSE	L	2025-06-17 15:57:24.280455
300088	长信科技	300088	创业板	SZSE	L	2025-06-17 15:57:24.280455
300091	金通灵	300091	创业板	SZSE	L	2025-06-17 15:57:24.280455
300092	科新机电	300092	创业板	SZSE	L	2025-06-17 15:57:24.280455
300094	国联水产	300094	创业板	SZSE	L	2025-06-17 15:57:24.280455
300095	华伍股份	300095	创业板	SZSE	L	2025-06-17 15:57:24.280455
300098	高新兴	300098	创业板	SZSE	L	2025-06-17 15:57:24.280455
300099	尤洛卡	300099	创业板	SZSE	L	2025-06-17 15:57:24.280455
300100	双林股份	300100	创业板	SZSE	L	2025-06-17 15:57:24.280455
300101	振芯科技	300101	创业板	SZSE	L	2025-06-17 15:57:24.280455
300102	乾照光电	300102	创业板	SZSE	L	2025-06-17 15:57:24.280455
300103	达刚控股	300103	创业板	SZSE	L	2025-06-17 15:57:24.280455
300105	龙源技术	300105	创业板	SZSE	L	2025-06-17 15:57:24.280455
300106	西部牧业	300106	创业板	SZSE	L	2025-06-17 15:57:24.280455
300107	建新股份	300107	创业板	SZSE	L	2025-06-17 15:57:24.280455
300109	新开源	300109	创业板	SZSE	L	2025-06-17 15:57:24.280455
300110	华仁药业	300110	创业板	SZSE	L	2025-06-17 15:57:24.280455
300111	向日葵	300111	创业板	SZSE	L	2025-06-17 15:57:24.280455
300112	万讯自控	300112	创业板	SZSE	L	2025-06-17 15:57:24.280455
300113	顺网科技	300113	创业板	SZSE	L	2025-06-17 15:57:24.280455
300115	长盈精密	300115	创业板	SZSE	L	2025-06-17 15:57:24.280455
300118	东方日升	300118	创业板	SZSE	L	2025-06-17 15:57:24.280455
300119	瑞普生物	300119	创业板	SZSE	L	2025-06-17 15:57:24.280455
300120	经纬辉开	300120	创业板	SZSE	L	2025-06-17 15:57:24.280455
300121	阳谷华泰	300121	创业板	SZSE	L	2025-06-17 15:57:24.280455
300122	智飞生物	300122	创业板	SZSE	L	2025-06-17 15:57:24.280455
300123	亚光科技	300123	创业板	SZSE	L	2025-06-17 15:57:24.280455
300124	汇川技术	300124	创业板	SZSE	L	2025-06-17 15:57:24.280455
300126	锐奇股份	300126	创业板	SZSE	L	2025-06-17 15:57:24.280455
300127	银河磁体	300127	创业板	SZSE	L	2025-06-17 15:57:24.280455
300128	锦富技术	300128	创业板	SZSE	L	2025-06-17 15:57:24.280455
300129	泰胜风能	300129	创业板	SZSE	L	2025-06-17 15:57:24.280455
300130	新国都	300130	创业板	SZSE	L	2025-06-17 15:57:24.280455
300131	英唐智控	300131	创业板	SZSE	L	2025-06-17 15:57:24.280455
300132	青松股份	300132	创业板	SZSE	L	2025-06-17 15:57:24.280455
300133	华策影视	300133	创业板	SZSE	L	2025-06-17 15:57:24.280455
300134	大富科技	300134	创业板	SZSE	L	2025-06-17 15:57:24.280455
300135	宝利国际	300135	创业板	SZSE	L	2025-06-17 15:57:24.280455
300136	信维通信	300136	创业板	SZSE	L	2025-06-17 15:57:24.280455
300138	晨光生物	300138	创业板	SZSE	L	2025-06-17 15:57:24.280455
300139	晓程科技	300139	创业板	SZSE	L	2025-06-17 15:57:24.280455
300140	节能环境	300140	创业板	SZSE	L	2025-06-17 15:57:24.280455
300141	和顺电气	300141	创业板	SZSE	L	2025-06-17 15:57:24.280455
300142	沃森生物	300142	创业板	SZSE	L	2025-06-17 15:57:24.280455
300143	盈康生命	300143	创业板	SZSE	L	2025-06-17 15:57:24.280455
300144	宋城演艺	300144	创业板	SZSE	L	2025-06-17 15:57:24.280455
300145	南方泵业	300145	创业板	SZSE	L	2025-06-17 15:57:24.280455
300146	汤臣倍健	300146	创业板	SZSE	L	2025-06-17 15:57:24.280455
300148	天舟文化	300148	创业板	SZSE	L	2025-06-17 15:57:24.280455
300149	睿智医药	300149	创业板	SZSE	L	2025-06-17 15:57:24.280455
300150	世纪瑞尔	300150	创业板	SZSE	L	2025-06-17 15:57:24.280455
300151	昌红科技	300151	创业板	SZSE	L	2025-06-17 15:57:24.280455
300153	科泰电源	300153	创业板	SZSE	L	2025-06-17 15:57:24.280455
300154	瑞凌股份	300154	创业板	SZSE	L	2025-06-17 15:57:24.280455
300155	安居宝	300155	创业板	SZSE	L	2025-06-17 15:57:24.280455
300157	新锦动力	300157	创业板	SZSE	L	2025-06-17 15:57:24.280455
300158	振东制药	300158	创业板	SZSE	L	2025-06-17 15:57:24.280455
300160	秀强股份	300160	创业板	SZSE	L	2025-06-17 15:57:24.280455
300161	华中数控	300161	创业板	SZSE	L	2025-06-17 15:57:24.280455
300162	雷曼光电	300162	创业板	SZSE	L	2025-06-17 15:57:24.280455
300163	先锋新材	300163	创业板	SZSE	L	2025-06-17 15:57:24.280455
300164	通源石油	300164	创业板	SZSE	L	2025-06-17 15:57:24.280455
300166	东方国信	300166	创业板	SZSE	L	2025-06-17 15:57:24.280455
300168	万达信息	300168	创业板	SZSE	L	2025-06-17 15:57:24.280455
300169	天晟新材	300169	创业板	SZSE	L	2025-06-17 15:57:24.280455
300170	汉得信息	300170	创业板	SZSE	L	2025-06-17 15:57:24.280455
300171	东富龙	300171	创业板	SZSE	L	2025-06-17 15:57:24.280455
300172	中电环保	300172	创业板	SZSE	L	2025-06-17 15:57:24.280455
300173	福能东方	300173	创业板	SZSE	L	2025-06-17 15:57:24.280455
300174	元力股份	300174	创业板	SZSE	L	2025-06-17 15:57:24.280455
300176	鸿特科技	300176	创业板	SZSE	L	2025-06-17 15:57:24.280455
300177	中海达	300177	创业板	SZSE	L	2025-06-17 15:57:24.280455
300179	四方达	300179	创业板	SZSE	L	2025-06-17 15:57:24.280455
300180	华峰超纤	300180	创业板	SZSE	L	2025-06-17 15:57:24.280455
300181	佐力药业	300181	创业板	SZSE	L	2025-06-17 15:57:24.280455
300182	捷成股份	300182	创业板	SZSE	L	2025-06-17 15:57:24.280455
300183	东软载波	300183	创业板	SZSE	L	2025-06-17 15:57:24.280455
300184	力源信息	300184	创业板	SZSE	L	2025-06-17 15:57:24.280455
300185	通裕重工	300185	创业板	SZSE	L	2025-06-17 15:57:24.280455
300187	永清环保	300187	创业板	SZSE	L	2025-06-17 15:57:24.280455
300188	国投智能	300188	创业板	SZSE	L	2025-06-17 15:57:24.280455
300189	神农种业	300189	创业板	SZSE	L	2025-06-17 15:57:24.280455
300190	维尔利	300190	创业板	SZSE	L	2025-06-17 15:57:24.280455
300191	潜能恒信	300191	创业板	SZSE	L	2025-06-17 15:57:24.280455
300192	科德教育	300192	创业板	SZSE	L	2025-06-17 15:57:24.280455
300193	佳士科技	300193	创业板	SZSE	L	2025-06-17 15:57:24.280455
300194	福安药业	300194	创业板	SZSE	L	2025-06-17 15:57:24.280455
300195	长荣股份	300195	创业板	SZSE	L	2025-06-17 15:57:24.280455
300196	长海股份	300196	创业板	SZSE	L	2025-06-17 15:57:24.280455
300197	节能铁汉	300197	创业板	SZSE	L	2025-06-17 15:57:24.280455
300199	翰宇药业	300199	创业板	SZSE	L	2025-06-17 15:57:24.280455
300200	高盟新材	300200	创业板	SZSE	L	2025-06-17 15:57:24.280455
300201	海伦哲	300201	创业板	SZSE	L	2025-06-17 15:57:24.280455
300203	聚光科技	300203	创业板	SZSE	L	2025-06-17 15:57:24.280455
300204	舒泰神	300204	创业板	SZSE	L	2025-06-17 15:57:24.280455
300206	理邦仪器	300206	创业板	SZSE	L	2025-06-17 15:57:24.280455
300207	欣旺达	300207	创业板	SZSE	L	2025-06-17 15:57:24.280455
300209	有棵树	300209	创业板	SZSE	L	2025-06-17 15:57:24.280455
300210	森远股份	300210	创业板	SZSE	L	2025-06-17 15:57:24.280455
300212	易华录	300212	创业板	SZSE	L	2025-06-17 15:57:24.280455
300213	佳讯飞鸿	300213	创业板	SZSE	L	2025-06-17 15:57:24.280455
300214	日科化学	300214	创业板	SZSE	L	2025-06-17 15:57:24.280455
300215	电科院	300215	创业板	SZSE	L	2025-06-17 15:57:24.280455
300217	东方电热	300217	创业板	SZSE	L	2025-06-17 15:57:24.280455
300218	安利股份	300218	创业板	SZSE	L	2025-06-17 15:57:24.280455
300219	鸿利智汇	300219	创业板	SZSE	L	2025-06-17 15:57:24.280455
300220	金运激光	300220	创业板	SZSE	L	2025-06-17 15:57:24.280455
300221	银禧科技	300221	创业板	SZSE	L	2025-06-17 15:57:24.280455
300222	科大智能	300222	创业板	SZSE	L	2025-06-17 15:57:24.280455
300223	北京君正	300223	创业板	SZSE	L	2025-06-17 15:57:24.280455
300224	正海磁材	300224	创业板	SZSE	L	2025-06-17 15:57:24.280455
300225	金力泰	300225	创业板	SZSE	L	2025-06-17 15:57:24.280455
300226	上海钢联	300226	创业板	SZSE	L	2025-06-17 15:57:24.280455
300227	光韵达	300227	创业板	SZSE	L	2025-06-17 15:57:24.280455
300228	富瑞特装	300228	创业板	SZSE	L	2025-06-17 15:57:24.280455
300229	拓尔思	300229	创业板	SZSE	L	2025-06-17 15:57:24.280455
300230	永利股份	300230	创业板	SZSE	L	2025-06-17 15:57:24.280455
300231	银信科技	300231	创业板	SZSE	L	2025-06-17 15:57:24.280455
300232	洲明科技	300232	创业板	SZSE	L	2025-06-17 15:57:24.280455
300233	金城医药	300233	创业板	SZSE	L	2025-06-17 15:57:24.280455
300234	开尔新材	300234	创业板	SZSE	L	2025-06-17 15:57:24.280455
300235	方直科技	300235	创业板	SZSE	L	2025-06-17 15:57:24.280455
300236	上海新阳	300236	创业板	SZSE	L	2025-06-17 15:57:24.280455
300237	美晨科技	300237	创业板	SZSE	L	2025-06-17 15:57:24.280455
300238	冠昊生物	300238	创业板	SZSE	L	2025-06-17 15:57:24.280455
300239	东宝生物	300239	创业板	SZSE	L	2025-06-17 15:57:24.280455
300240	飞力达	300240	创业板	SZSE	L	2025-06-17 15:57:24.280455
300241	瑞丰光电	300241	创业板	SZSE	L	2025-06-17 15:57:24.280455
300242	佳云科技	300242	创业板	SZSE	L	2025-06-17 15:57:24.280455
300243	瑞丰高材	300243	创业板	SZSE	L	2025-06-17 15:57:24.280455
300244	迪安诊断	300244	创业板	SZSE	L	2025-06-17 15:57:24.280455
300245	天玑科技	300245	创业板	SZSE	L	2025-06-17 15:57:24.280455
300246	宝莱特	300246	创业板	SZSE	L	2025-06-17 15:57:24.280455
300247	融捷健康	300247	创业板	SZSE	L	2025-06-17 15:57:24.280455
300248	新开普	300248	创业板	SZSE	L	2025-06-17 15:57:24.280455
300249	依米康	300249	创业板	SZSE	L	2025-06-17 15:57:24.280455
300250	初灵信息	300250	创业板	SZSE	L	2025-06-17 15:57:24.280455
300251	光线传媒	300251	创业板	SZSE	L	2025-06-17 15:57:24.280455
300252	金信诺	300252	创业板	SZSE	L	2025-06-17 15:57:24.280455
300253	卫宁健康	300253	创业板	SZSE	L	2025-06-17 15:57:24.280455
300254	仟源医药	300254	创业板	SZSE	L	2025-06-17 15:57:24.280455
300255	常山药业	300255	创业板	SZSE	L	2025-06-17 15:57:24.280455
300256	星星科技	300256	创业板	SZSE	L	2025-06-17 15:57:24.280455
300257	开山股份	300257	创业板	SZSE	L	2025-06-17 15:57:24.280455
300258	精锻科技	300258	创业板	SZSE	L	2025-06-17 15:57:24.280455
300259	新天科技	300259	创业板	SZSE	L	2025-06-17 15:57:24.280455
300260	新莱应材	300260	创业板	SZSE	L	2025-06-17 15:57:24.280455
300261	雅本化学	300261	创业板	SZSE	L	2025-06-17 15:57:24.280455
300263	隆华科技	300263	创业板	SZSE	L	2025-06-17 15:57:24.280455
300264	佳创视讯	300264	创业板	SZSE	L	2025-06-17 15:57:24.280455
300265	通光线缆	300265	创业板	SZSE	L	2025-06-17 15:57:24.280455
300266	兴源环境	300266	创业板	SZSE	L	2025-06-17 15:57:24.280455
300267	尔康制药	300267	创业板	SZSE	L	2025-06-17 15:57:24.280455
300269	联建光电	300269	创业板	SZSE	L	2025-06-17 15:57:24.280455
300270	中威电子	300270	创业板	SZSE	L	2025-06-17 15:57:24.280455
300271	华宇软件	300271	创业板	SZSE	L	2025-06-17 15:57:24.280455
300272	开能健康	300272	创业板	SZSE	L	2025-06-17 15:57:24.280455
300274	阳光电源	300274	创业板	SZSE	L	2025-06-17 15:57:24.280455
300275	梅安森	300275	创业板	SZSE	L	2025-06-17 15:57:24.280455
300276	三丰智能	300276	创业板	SZSE	L	2025-06-17 15:57:24.280455
300277	海联讯	300277	创业板	SZSE	L	2025-06-17 15:57:24.280455
300278	华昌达	300278	创业板	SZSE	L	2025-06-17 15:57:24.280455
300279	和晶科技	300279	创业板	SZSE	L	2025-06-17 15:57:24.280455
300281	金明精机	300281	创业板	SZSE	L	2025-06-17 15:57:24.280455
300283	温州宏丰	300283	创业板	SZSE	L	2025-06-17 15:57:24.280455
300284	苏交科	300284	创业板	SZSE	L	2025-06-17 15:57:24.280455
300285	国瓷材料	300285	创业板	SZSE	L	2025-06-17 15:57:24.280455
300286	安科瑞	300286	创业板	SZSE	L	2025-06-17 15:57:24.280455
300287	飞利信	300287	创业板	SZSE	L	2025-06-17 15:57:24.280455
300288	朗玛信息	300288	创业板	SZSE	L	2025-06-17 15:57:24.280455
300289	利德曼	300289	创业板	SZSE	L	2025-06-17 15:57:24.280455
300290	荣科科技	300290	创业板	SZSE	L	2025-06-17 15:57:24.280455
300291	百纳千成	300291	创业板	SZSE	L	2025-06-17 15:57:24.280455
300292	吴通控股	300292	创业板	SZSE	L	2025-06-17 15:57:24.280455
300293	蓝英装备	300293	创业板	SZSE	L	2025-06-17 15:57:24.280455
300294	博雅生物	300294	创业板	SZSE	L	2025-06-17 15:57:24.280455
300295	三六五网	300295	创业板	SZSE	L	2025-06-17 15:57:24.280455
300296	利亚德	300296	创业板	SZSE	L	2025-06-17 15:57:24.280455
300298	三诺生物	300298	创业板	SZSE	L	2025-06-17 15:57:24.280455
300299	富春股份	300299	创业板	SZSE	L	2025-06-17 15:57:24.280455
300302	同有科技	300302	创业板	SZSE	L	2025-06-17 15:57:24.280455
300303	聚飞光电	300303	创业板	SZSE	L	2025-06-17 15:57:24.280455
300304	云意电气	300304	创业板	SZSE	L	2025-06-17 15:57:24.280455
300305	裕兴股份	300305	创业板	SZSE	L	2025-06-17 15:57:24.280455
300306	远方信息	300306	创业板	SZSE	L	2025-06-17 15:57:24.280455
300307	慈星股份	300307	创业板	SZSE	L	2025-06-17 15:57:24.280455
300308	中际旭创	300308	创业板	SZSE	L	2025-06-17 15:57:24.280455
300310	宜通世纪	300310	创业板	SZSE	L	2025-06-17 15:57:24.280455
300311	任子行	300311	创业板	SZSE	L	2025-06-17 15:57:24.280455
300314	戴维医疗	300314	创业板	SZSE	L	2025-06-17 15:57:24.280455
300315	掌趣科技	300315	创业板	SZSE	L	2025-06-17 15:57:24.280455
300316	晶盛机电	300316	创业板	SZSE	L	2025-06-17 15:57:24.280455
300317	珈伟新能	300317	创业板	SZSE	L	2025-06-17 15:57:24.280455
300318	博晖创新	300318	创业板	SZSE	L	2025-06-17 15:57:24.280455
300319	麦捷科技	300319	创业板	SZSE	L	2025-06-17 15:57:24.280455
300320	海达股份	300320	创业板	SZSE	L	2025-06-17 15:57:24.280455
300321	同大股份	300321	创业板	SZSE	L	2025-06-17 15:57:24.280455
300322	硕贝德	300322	创业板	SZSE	L	2025-06-17 15:57:24.280455
300323	华灿光电	300323	创业板	SZSE	L	2025-06-17 15:57:24.280455
300324	旋极信息	300324	创业板	SZSE	L	2025-06-17 15:57:24.280455
300327	中颖电子	300327	创业板	SZSE	L	2025-06-17 15:57:24.280455
300328	宜安科技	300328	创业板	SZSE	L	2025-06-17 15:57:24.280455
300329	海伦钢琴	300329	创业板	SZSE	L	2025-06-17 15:57:24.280455
300331	苏大维格	300331	创业板	SZSE	L	2025-06-17 15:57:24.280455
300332	天壕能源	300332	创业板	SZSE	L	2025-06-17 15:57:24.280455
300333	兆日科技	300333	创业板	SZSE	L	2025-06-17 15:57:24.280455
300334	津膜科技	300334	创业板	SZSE	L	2025-06-17 15:57:24.280455
300335	迪森股份	300335	创业板	SZSE	L	2025-06-17 15:57:24.280455
300337	银邦股份	300337	创业板	SZSE	L	2025-06-17 15:57:24.280455
300339	润和软件	300339	创业板	SZSE	L	2025-06-17 15:57:24.280455
300340	科恒股份	300340	创业板	SZSE	L	2025-06-17 15:57:24.280455
300341	麦克奥迪	300341	创业板	SZSE	L	2025-06-17 15:57:24.280455
300342	天银机电	300342	创业板	SZSE	L	2025-06-17 15:57:24.280455
300345	华民股份	300345	创业板	SZSE	L	2025-06-17 15:57:24.280455
300346	南大光电	300346	创业板	SZSE	L	2025-06-17 15:57:24.280455
300347	泰格医药	300347	创业板	SZSE	L	2025-06-17 15:57:24.280455
300348	长亮科技	300348	创业板	SZSE	L	2025-06-17 15:57:24.280455
300349	金卡智能	300349	创业板	SZSE	L	2025-06-17 15:57:24.280455
300350	华鹏飞	300350	创业板	SZSE	L	2025-06-17 15:57:24.280455
300351	永贵电器	300351	创业板	SZSE	L	2025-06-17 15:57:24.280455
300352	北信源	300352	创业板	SZSE	L	2025-06-17 15:57:24.280455
300353	东土科技	300353	创业板	SZSE	L	2025-06-17 15:57:24.280455
300354	东华测试	300354	创业板	SZSE	L	2025-06-17 15:57:24.280455
300355	蒙草生态	300355	创业板	SZSE	L	2025-06-17 15:57:24.280455
300357	我武生物	300357	创业板	SZSE	L	2025-06-17 15:57:24.280455
300358	楚天科技	300358	创业板	SZSE	L	2025-06-17 15:57:24.280455
300359	全通教育	300359	创业板	SZSE	L	2025-06-17 15:57:24.280455
300360	炬华科技	300360	创业板	SZSE	L	2025-06-17 15:57:24.280455
300363	博腾股份	300363	创业板	SZSE	L	2025-06-17 15:57:24.280455
300364	中文在线	300364	创业板	SZSE	L	2025-06-17 15:57:24.280455
300365	恒华科技	300365	创业板	SZSE	L	2025-06-17 15:57:24.280455
300366	创意信息	300366	创业板	SZSE	L	2025-06-17 15:57:24.280455
300368	汇金股份	300368	创业板	SZSE	L	2025-06-17 15:57:24.280455
300369	绿盟科技	300369	创业板	SZSE	L	2025-06-17 15:57:24.280455
300370	安控科技	300370	创业板	SZSE	L	2025-06-17 15:57:24.280455
300371	汇中股份	300371	创业板	SZSE	L	2025-06-17 15:57:24.280455
300373	扬杰科技	300373	创业板	SZSE	L	2025-06-17 15:57:24.280455
300374	中铁装配	300374	创业板	SZSE	L	2025-06-17 15:57:24.280455
300375	鹏翎股份	300375	创业板	SZSE	L	2025-06-17 15:57:24.280455
300377	赢时胜	300377	创业板	SZSE	L	2025-06-17 15:57:24.280455
300378	鼎捷数智	300378	创业板	SZSE	L	2025-06-17 15:57:24.280455
300380	安硕信息	300380	创业板	SZSE	L	2025-06-17 15:57:24.280455
300381	溢多利	300381	创业板	SZSE	L	2025-06-17 15:57:24.280455
300382	斯莱克	300382	创业板	SZSE	L	2025-06-17 15:57:24.280455
300383	光环新网	300383	创业板	SZSE	L	2025-06-17 15:57:24.280455
300384	三联虹普	300384	创业板	SZSE	L	2025-06-17 15:57:24.280455
300385	雪浪环境	300385	创业板	SZSE	L	2025-06-17 15:57:24.280455
300386	飞天诚信	300386	创业板	SZSE	L	2025-06-17 15:57:24.280455
300387	富邦科技	300387	创业板	SZSE	L	2025-06-17 15:57:24.280455
300388	节能国祯	300388	创业板	SZSE	L	2025-06-17 15:57:24.280455
300389	艾比森	300389	创业板	SZSE	L	2025-06-17 15:57:24.280455
300390	天华新能	300390	创业板	SZSE	L	2025-06-17 15:57:24.280455
300393	中来股份	300393	创业板	SZSE	L	2025-06-17 15:57:24.280455
300394	天孚通信	300394	创业板	SZSE	L	2025-06-17 15:57:24.280455
300395	菲利华	300395	创业板	SZSE	L	2025-06-17 15:57:24.280455
300396	迪瑞医疗	300396	创业板	SZSE	L	2025-06-17 15:57:24.280455
300397	天和防务	300397	创业板	SZSE	L	2025-06-17 15:57:24.280455
300398	飞凯材料	300398	创业板	SZSE	L	2025-06-17 15:57:24.280455
300399	天利科技	300399	创业板	SZSE	L	2025-06-17 15:57:24.280455
300400	劲拓股份	300400	创业板	SZSE	L	2025-06-17 15:57:24.280455
300401	花园生物	300401	创业板	SZSE	L	2025-06-17 15:57:24.280455
300402	宝色股份	300402	创业板	SZSE	L	2025-06-17 15:57:24.280455
300403	汉宇集团	300403	创业板	SZSE	L	2025-06-17 15:57:24.280455
300404	博济医药	300404	创业板	SZSE	L	2025-06-17 15:57:24.280455
300405	科隆股份	300405	创业板	SZSE	L	2025-06-17 15:57:24.280455
300406	九强生物	300406	创业板	SZSE	L	2025-06-17 15:57:24.280455
300407	凯发电气	300407	创业板	SZSE	L	2025-06-17 15:57:24.280455
300408	三环集团	300408	创业板	SZSE	L	2025-06-17 15:57:24.280455
300409	道氏技术	300409	创业板	SZSE	L	2025-06-17 15:57:24.280455
300410	正业科技	300410	创业板	SZSE	L	2025-06-17 15:57:24.280455
300411	金盾股份	300411	创业板	SZSE	L	2025-06-17 15:57:24.280455
300412	迦南科技	300412	创业板	SZSE	L	2025-06-17 15:57:24.280455
300413	芒果超媒	300413	创业板	SZSE	L	2025-06-17 15:57:24.280455
300414	中光防雷	300414	创业板	SZSE	L	2025-06-17 15:57:24.280455
300415	伊之密	300415	创业板	SZSE	L	2025-06-17 15:57:24.280455
300416	苏试试验	300416	创业板	SZSE	L	2025-06-17 15:57:24.280455
300417	南华仪器	300417	创业板	SZSE	L	2025-06-17 15:57:24.280455
300418	昆仑万维	300418	创业板	SZSE	L	2025-06-17 15:57:24.280455
300420	五洋自控	300420	创业板	SZSE	L	2025-06-17 15:57:24.280455
300421	力星股份	300421	创业板	SZSE	L	2025-06-17 15:57:24.280455
300422	博世科	300422	创业板	SZSE	L	2025-06-17 15:57:24.280455
300423	昇辉科技	300423	创业板	SZSE	L	2025-06-17 15:57:24.280455
300424	航新科技	300424	创业板	SZSE	L	2025-06-17 15:57:24.280455
300425	中建环能	300425	创业板	SZSE	L	2025-06-17 15:57:24.280455
300426	华智数媒	300426	创业板	SZSE	L	2025-06-17 15:57:24.280455
300427	红相股份	300427	创业板	SZSE	L	2025-06-17 15:57:24.280455
300428	立中集团	300428	创业板	SZSE	L	2025-06-17 15:57:24.280455
300429	强力新材	300429	创业板	SZSE	L	2025-06-17 15:57:24.280455
300430	诚益通	300430	创业板	SZSE	L	2025-06-17 15:57:24.280455
300432	富临精工	300432	创业板	SZSE	L	2025-06-17 15:57:24.280455
300433	蓝思科技	300433	创业板	SZSE	L	2025-06-17 15:57:24.280455
300434	金石亚药	300434	创业板	SZSE	L	2025-06-17 15:57:24.280455
300435	中泰股份	300435	创业板	SZSE	L	2025-06-17 15:57:24.280455
300436	广生堂	300436	创业板	SZSE	L	2025-06-17 15:57:24.280455
300437	清水源	300437	创业板	SZSE	L	2025-06-17 15:57:24.280455
300438	鹏辉能源	300438	创业板	SZSE	L	2025-06-17 15:57:24.280455
300439	美康生物	300439	创业板	SZSE	L	2025-06-17 15:57:24.280455
300440	运达科技	300440	创业板	SZSE	L	2025-06-17 15:57:24.280455
300441	鲍斯股份	300441	创业板	SZSE	L	2025-06-17 15:57:24.280455
300442	润泽科技	300442	创业板	SZSE	L	2025-06-17 15:57:24.280455
300443	金雷股份	300443	创业板	SZSE	L	2025-06-17 15:57:24.280455
300444	双杰电气	300444	创业板	SZSE	L	2025-06-17 15:57:24.280455
300445	康斯特	300445	创业板	SZSE	L	2025-06-17 15:57:24.280455
300446	航天智造	300446	创业板	SZSE	L	2025-06-17 15:57:24.280455
300447	全信股份	300447	创业板	SZSE	L	2025-06-17 15:57:24.280455
300448	浩云科技	300448	创业板	SZSE	L	2025-06-17 15:57:24.280455
300449	汉邦高科	300449	创业板	SZSE	L	2025-06-17 15:57:24.280455
300450	先导智能	300450	创业板	SZSE	L	2025-06-17 15:57:24.280455
300451	创业慧康	300451	创业板	SZSE	L	2025-06-17 15:57:24.280455
300452	山河药辅	300452	创业板	SZSE	L	2025-06-17 15:57:24.280455
300453	三鑫医疗	300453	创业板	SZSE	L	2025-06-17 15:57:24.280455
300454	深信服	300454	创业板	SZSE	L	2025-06-17 15:57:24.280455
300455	航天智装	300455	创业板	SZSE	L	2025-06-17 15:57:24.280455
300456	赛微电子	300456	创业板	SZSE	L	2025-06-17 15:57:24.280455
300457	赢合科技	300457	创业板	SZSE	L	2025-06-17 15:57:24.280455
300458	全志科技	300458	创业板	SZSE	L	2025-06-17 15:57:24.280455
300459	汤姆猫	300459	创业板	SZSE	L	2025-06-17 15:57:24.280455
300460	惠伦晶体	300460	创业板	SZSE	L	2025-06-17 15:57:24.280455
300461	田中精机	300461	创业板	SZSE	L	2025-06-17 15:57:24.280455
300462	华铭智能	300462	创业板	SZSE	L	2025-06-17 15:57:24.280455
300463	迈克生物	300463	创业板	SZSE	L	2025-06-17 15:57:24.280455
300464	星徽股份	300464	创业板	SZSE	L	2025-06-17 15:57:24.280455
300465	高伟达	300465	创业板	SZSE	L	2025-06-17 15:57:24.280455
300466	赛摩智能	300466	创业板	SZSE	L	2025-06-17 15:57:24.280455
300467	迅游科技	300467	创业板	SZSE	L	2025-06-17 15:57:24.280455
300468	四方精创	300468	创业板	SZSE	L	2025-06-17 15:57:24.280455
300469	信息发展	300469	创业板	SZSE	L	2025-06-17 15:57:24.280455
300470	中密控股	300470	创业板	SZSE	L	2025-06-17 15:57:24.280455
300471	厚普股份	300471	创业板	SZSE	L	2025-06-17 15:57:24.280455
300473	德尔股份	300473	创业板	SZSE	L	2025-06-17 15:57:24.280455
300474	景嘉微	300474	创业板	SZSE	L	2025-06-17 15:57:24.280455
300475	香农芯创	300475	创业板	SZSE	L	2025-06-17 15:57:24.280455
300476	胜宏科技	300476	创业板	SZSE	L	2025-06-17 15:57:24.280455
300478	杭州高新	300478	创业板	SZSE	L	2025-06-17 15:57:24.280455
300479	神思电子	300479	创业板	SZSE	L	2025-06-17 15:57:24.280455
300480	光力科技	300480	创业板	SZSE	L	2025-06-17 15:57:24.280455
300481	濮阳惠成	300481	创业板	SZSE	L	2025-06-17 15:57:24.280455
300482	万孚生物	300482	创业板	SZSE	L	2025-06-17 15:57:24.280455
300483	首华燃气	300483	创业板	SZSE	L	2025-06-17 15:57:24.280455
300484	蓝海华腾	300484	创业板	SZSE	L	2025-06-17 15:57:24.280455
300485	赛升药业	300485	创业板	SZSE	L	2025-06-17 15:57:24.280455
300486	东杰智能	300486	创业板	SZSE	L	2025-06-17 15:57:24.280455
300487	蓝晓科技	300487	创业板	SZSE	L	2025-06-17 15:57:24.280455
300488	恒锋工具	300488	创业板	SZSE	L	2025-06-17 15:57:24.280455
300489	光智科技	300489	创业板	SZSE	L	2025-06-17 15:57:24.280455
300490	华自科技	300490	创业板	SZSE	L	2025-06-17 15:57:24.280455
300491	通合科技	300491	创业板	SZSE	L	2025-06-17 15:57:24.280455
300492	华图山鼎	300492	创业板	SZSE	L	2025-06-17 15:57:24.280455
300493	润欣科技	300493	创业板	SZSE	L	2025-06-17 15:57:24.280455
300494	盛天网络	300494	创业板	SZSE	L	2025-06-17 15:57:24.280455
300496	中科创达	300496	创业板	SZSE	L	2025-06-17 15:57:24.280455
300497	富祥药业	300497	创业板	SZSE	L	2025-06-17 15:57:24.280455
300498	温氏股份	300498	创业板	SZSE	L	2025-06-17 15:57:24.280455
300499	高澜股份	300499	创业板	SZSE	L	2025-06-17 15:57:24.280455
300500	启迪设计	300500	创业板	SZSE	L	2025-06-17 15:57:24.280455
300501	海顺新材	300501	创业板	SZSE	L	2025-06-17 15:57:24.280455
300502	新易盛	300502	创业板	SZSE	L	2025-06-17 15:57:24.280455
300503	昊志机电	300503	创业板	SZSE	L	2025-06-17 15:57:24.280455
300504	天邑股份	300504	创业板	SZSE	L	2025-06-17 15:57:24.280455
300505	川金诺	300505	创业板	SZSE	L	2025-06-17 15:57:24.280455
300507	苏奥传感	300507	创业板	SZSE	L	2025-06-17 15:57:24.280455
300508	维宏股份	300508	创业板	SZSE	L	2025-06-17 15:57:24.280455
300509	新美星	300509	创业板	SZSE	L	2025-06-17 15:57:24.280455
300510	金冠股份	300510	创业板	SZSE	L	2025-06-17 15:57:24.280455
300511	雪榕生物	300511	创业板	SZSE	L	2025-06-17 15:57:24.280455
300512	中亚股份	300512	创业板	SZSE	L	2025-06-17 15:57:24.280455
300513	恒实科技	300513	创业板	SZSE	L	2025-06-17 15:57:24.280455
300514	友讯达	300514	创业板	SZSE	L	2025-06-17 15:57:24.280455
300515	三德科技	300515	创业板	SZSE	L	2025-06-17 15:57:24.280455
300516	久之洋	300516	创业板	SZSE	L	2025-06-17 15:57:24.280455
300517	海波重科	300517	创业板	SZSE	L	2025-06-17 15:57:24.280455
300518	新迅达	300518	创业板	SZSE	L	2025-06-17 15:57:24.280455
300519	新光药业	300519	创业板	SZSE	L	2025-06-17 15:57:24.280455
300520	科大国创	300520	创业板	SZSE	L	2025-06-17 15:57:24.280455
300521	爱司凯	300521	创业板	SZSE	L	2025-06-17 15:57:24.280455
300522	世名科技	300522	创业板	SZSE	L	2025-06-17 15:57:24.280455
300523	辰安科技	300523	创业板	SZSE	L	2025-06-17 15:57:24.280455
300525	博思软件	300525	创业板	SZSE	L	2025-06-17 15:57:24.280455
300527	中船应急	300527	创业板	SZSE	L	2025-06-17 15:57:24.280455
300528	幸福蓝海	300528	创业板	SZSE	L	2025-06-17 15:57:24.280455
300529	健帆生物	300529	创业板	SZSE	L	2025-06-17 15:57:24.280455
300530	领湃科技	300530	创业板	SZSE	L	2025-06-17 15:57:24.280455
300531	优博讯	300531	创业板	SZSE	L	2025-06-17 15:57:24.280455
300532	今天国际	300532	创业板	SZSE	L	2025-06-17 15:57:24.280455
300533	冰川网络	300533	创业板	SZSE	L	2025-06-17 15:57:24.280455
300534	陇神戎发	300534	创业板	SZSE	L	2025-06-17 15:57:24.280455
300535	达威股份	300535	创业板	SZSE	L	2025-06-17 15:57:24.280455
300536	农尚环境	300536	创业板	SZSE	L	2025-06-17 15:57:24.280455
300537	广信材料	300537	创业板	SZSE	L	2025-06-17 15:57:24.280455
300538	同益股份	300538	创业板	SZSE	L	2025-06-17 15:57:24.280455
300539	横河精密	300539	创业板	SZSE	L	2025-06-17 15:57:24.280455
300540	蜀道装备	300540	创业板	SZSE	L	2025-06-17 15:57:24.280455
300541	先进数通	300541	创业板	SZSE	L	2025-06-17 15:57:24.280455
300542	新晨科技	300542	创业板	SZSE	L	2025-06-17 15:57:24.280455
300543	朗科智能	300543	创业板	SZSE	L	2025-06-17 15:57:24.280455
300545	联得装备	300545	创业板	SZSE	L	2025-06-17 15:57:24.280455
300546	雄帝科技	300546	创业板	SZSE	L	2025-06-17 15:57:24.280455
300547	川环科技	300547	创业板	SZSE	L	2025-06-17 15:57:24.280455
300548	博创科技	300548	创业板	SZSE	L	2025-06-17 15:57:24.280455
300549	优德精密	300549	创业板	SZSE	L	2025-06-17 15:57:24.280455
300550	和仁科技	300550	创业板	SZSE	L	2025-06-17 15:57:24.280455
300551	古鳌科技	300551	创业板	SZSE	L	2025-06-17 15:57:24.280455
300552	万集科技	300552	创业板	SZSE	L	2025-06-17 15:57:24.280455
300553	集智股份	300553	创业板	SZSE	L	2025-06-17 15:57:24.280455
300554	三超新材	300554	创业板	SZSE	L	2025-06-17 15:57:24.280455
300556	丝路视觉	300556	创业板	SZSE	L	2025-06-17 15:57:24.280455
300557	理工光科	300557	创业板	SZSE	L	2025-06-17 15:57:24.280455
300558	贝达药业	300558	创业板	SZSE	L	2025-06-17 15:57:24.280455
300559	佳发教育	300559	创业板	SZSE	L	2025-06-17 15:57:24.280455
300560	中富通	300560	创业板	SZSE	L	2025-06-17 15:57:24.280455
300562	乐心医疗	300562	创业板	SZSE	L	2025-06-17 15:57:24.280455
300563	神宇股份	300563	创业板	SZSE	L	2025-06-17 15:57:24.280455
300564	筑博设计	300564	创业板	SZSE	L	2025-06-17 15:57:24.280455
300565	科信技术	300565	创业板	SZSE	L	2025-06-17 15:57:24.280455
300566	激智科技	300566	创业板	SZSE	L	2025-06-17 15:57:24.280455
300567	精测电子	300567	创业板	SZSE	L	2025-06-17 15:57:24.280455
300568	星源材质	300568	创业板	SZSE	L	2025-06-17 15:57:24.280455
300569	天能重工	300569	创业板	SZSE	L	2025-06-17 15:57:24.280455
300570	太辰光	300570	创业板	SZSE	L	2025-06-17 15:57:24.280455
300571	平治信息	300571	创业板	SZSE	L	2025-06-17 15:57:24.280455
300572	安车检测	300572	创业板	SZSE	L	2025-06-17 15:57:24.280455
300573	兴齐眼药	300573	创业板	SZSE	L	2025-06-17 15:57:24.280455
300575	中旗股份	300575	创业板	SZSE	L	2025-06-17 15:57:24.280455
300576	容大感光	300576	创业板	SZSE	L	2025-06-17 15:57:24.280455
300577	开润股份	300577	创业板	SZSE	L	2025-06-17 15:57:24.280455
300578	会畅通讯	300578	创业板	SZSE	L	2025-06-17 15:57:24.280455
300579	数字认证	300579	创业板	SZSE	L	2025-06-17 15:57:24.280455
300580	贝斯特	300580	创业板	SZSE	L	2025-06-17 15:57:24.280455
300581	晨曦航空	300581	创业板	SZSE	L	2025-06-17 15:57:24.280455
300582	英飞特	300582	创业板	SZSE	L	2025-06-17 15:57:24.280455
300583	赛托生物	300583	创业板	SZSE	L	2025-06-17 15:57:24.280455
300584	海辰药业	300584	创业板	SZSE	L	2025-06-17 15:57:24.280455
300585	奥联电子	300585	创业板	SZSE	L	2025-06-17 15:57:24.280455
300586	美联新材	300586	创业板	SZSE	L	2025-06-17 15:57:24.280455
300587	天铁科技	300587	创业板	SZSE	L	2025-06-17 15:57:24.280455
300588	熙菱信息	300588	创业板	SZSE	L	2025-06-17 15:57:24.280455
300589	江龙船艇	300589	创业板	SZSE	L	2025-06-17 15:57:24.280455
300590	移为通信	300590	创业板	SZSE	L	2025-06-17 15:57:24.280455
300591	万里马	300591	创业板	SZSE	L	2025-06-17 15:57:24.280455
300592	华凯易佰	300592	创业板	SZSE	L	2025-06-17 15:57:24.280455
300593	新雷能	300593	创业板	SZSE	L	2025-06-17 15:57:24.280455
300594	朗进科技	300594	创业板	SZSE	L	2025-06-17 15:57:24.280455
300595	欧普康视	300595	创业板	SZSE	L	2025-06-17 15:57:24.280455
300596	利安隆	300596	创业板	SZSE	L	2025-06-17 15:57:24.280455
300597	吉大通信	300597	创业板	SZSE	L	2025-06-17 15:57:24.280455
300598	诚迈科技	300598	创业板	SZSE	L	2025-06-17 15:57:24.280455
300599	雄塑科技	300599	创业板	SZSE	L	2025-06-17 15:57:24.280455
300600	国瑞科技	300600	创业板	SZSE	L	2025-06-17 15:57:24.280455
300601	康泰生物	300601	创业板	SZSE	L	2025-06-17 15:57:24.280455
300602	飞荣达	300602	创业板	SZSE	L	2025-06-17 15:57:24.280455
300603	立昂技术	300603	创业板	SZSE	L	2025-06-17 15:57:24.280455
300604	长川科技	300604	创业板	SZSE	L	2025-06-17 15:57:24.280455
300605	恒锋信息	300605	创业板	SZSE	L	2025-06-17 15:57:24.280455
300606	金太阳	300606	创业板	SZSE	L	2025-06-17 15:57:24.280455
300607	拓斯达	300607	创业板	SZSE	L	2025-06-17 15:57:24.280455
300608	思特奇	300608	创业板	SZSE	L	2025-06-17 15:57:24.280455
300609	汇纳科技	300609	创业板	SZSE	L	2025-06-17 15:57:24.280455
300610	晨化股份	300610	创业板	SZSE	L	2025-06-17 15:57:24.280455
300611	美力科技	300611	创业板	SZSE	L	2025-06-17 15:57:24.280455
300612	宣亚国际	300612	创业板	SZSE	L	2025-06-17 15:57:24.280455
300613	富瀚微	300613	创业板	SZSE	L	2025-06-17 15:57:24.280455
300614	百川畅银	300614	创业板	SZSE	L	2025-06-17 15:57:24.280455
300615	欣天科技	300615	创业板	SZSE	L	2025-06-17 15:57:24.280455
300616	尚品宅配	300616	创业板	SZSE	L	2025-06-17 15:57:24.280455
300617	安靠智电	300617	创业板	SZSE	L	2025-06-17 15:57:24.280455
300618	寒锐钴业	300618	创业板	SZSE	L	2025-06-17 15:57:24.280455
300619	金银河	300619	创业板	SZSE	L	2025-06-17 15:57:24.280455
300620	光库科技	300620	创业板	SZSE	L	2025-06-17 15:57:24.280455
300621	维业股份	300621	创业板	SZSE	L	2025-06-17 15:57:24.280455
300622	博士眼镜	300622	创业板	SZSE	L	2025-06-17 15:57:24.280455
300623	捷捷微电	300623	创业板	SZSE	L	2025-06-17 15:57:24.280455
300624	万兴科技	300624	创业板	SZSE	L	2025-06-17 15:57:24.280455
300625	三雄极光	300625	创业板	SZSE	L	2025-06-17 15:57:24.280455
300626	华瑞股份	300626	创业板	SZSE	L	2025-06-17 15:57:24.280455
300627	华测导航	300627	创业板	SZSE	L	2025-06-17 15:57:24.280455
300628	亿联网络	300628	创业板	SZSE	L	2025-06-17 15:57:24.280455
300629	新劲刚	300629	创业板	SZSE	L	2025-06-17 15:57:24.280455
300631	久吾高科	300631	创业板	SZSE	L	2025-06-17 15:57:24.280455
300632	光莆股份	300632	创业板	SZSE	L	2025-06-17 15:57:24.280455
300633	开立医疗	300633	创业板	SZSE	L	2025-06-17 15:57:24.280455
300634	彩讯股份	300634	创业板	SZSE	L	2025-06-17 15:57:24.280455
300635	中达安	300635	创业板	SZSE	L	2025-06-17 15:57:24.280455
300636	同和药业	300636	创业板	SZSE	L	2025-06-17 15:57:24.280455
300637	扬帆新材	300637	创业板	SZSE	L	2025-06-17 15:57:24.280455
300638	广和通	300638	创业板	SZSE	L	2025-06-17 15:57:24.280455
300639	凯普生物	300639	创业板	SZSE	L	2025-06-17 15:57:24.280455
300640	德艺文创	300640	创业板	SZSE	L	2025-06-17 15:57:24.280455
300641	正丹股份	300641	创业板	SZSE	L	2025-06-17 15:57:24.280455
300642	透景生命	300642	创业板	SZSE	L	2025-06-17 15:57:24.280455
300643	万通智控	300643	创业板	SZSE	L	2025-06-17 15:57:24.280455
300644	南京聚隆	300644	创业板	SZSE	L	2025-06-17 15:57:24.280455
300645	正元智慧	300645	创业板	SZSE	L	2025-06-17 15:57:24.280455
300647	超频三	300647	创业板	SZSE	L	2025-06-17 15:57:24.280455
300648	星云股份	300648	创业板	SZSE	L	2025-06-17 15:57:24.280455
300649	杭州园林	300649	创业板	SZSE	L	2025-06-17 15:57:24.280455
300650	太龙股份	300650	创业板	SZSE	L	2025-06-17 15:57:24.280455
300651	金陵体育	300651	创业板	SZSE	L	2025-06-17 15:57:24.280455
300652	雷迪克	300652	创业板	SZSE	L	2025-06-17 15:57:24.280455
300653	正海生物	300653	创业板	SZSE	L	2025-06-17 15:57:24.280455
300654	世纪天鸿	300654	创业板	SZSE	L	2025-06-17 15:57:24.280455
300655	晶瑞电材	300655	创业板	SZSE	L	2025-06-17 15:57:24.280455
300656	民德电子	300656	创业板	SZSE	L	2025-06-17 15:57:24.280455
300657	弘信电子	300657	创业板	SZSE	L	2025-06-17 15:57:24.280455
300658	延江股份	300658	创业板	SZSE	L	2025-06-17 15:57:24.280455
300659	中孚信息	300659	创业板	SZSE	L	2025-06-17 15:57:24.280455
300660	江苏雷利	300660	创业板	SZSE	L	2025-06-17 15:57:24.280455
300661	圣邦股份	300661	创业板	SZSE	L	2025-06-17 15:57:24.280455
300662	科锐国际	300662	创业板	SZSE	L	2025-06-17 15:57:24.280455
300663	科蓝软件	300663	创业板	SZSE	L	2025-06-17 15:57:24.280455
300664	鹏鹞环保	300664	创业板	SZSE	L	2025-06-17 15:57:24.280455
300665	飞鹿股份	300665	创业板	SZSE	L	2025-06-17 15:57:24.280455
300666	江丰电子	300666	创业板	SZSE	L	2025-06-17 15:57:24.280455
300667	必创科技	300667	创业板	SZSE	L	2025-06-17 15:57:24.280455
300668	杰恩设计	300668	创业板	SZSE	L	2025-06-17 15:57:24.280455
300669	沪宁股份	300669	创业板	SZSE	L	2025-06-17 15:57:24.280455
300670	大烨智能	300670	创业板	SZSE	L	2025-06-17 15:57:24.280455
300671	富满微	300671	创业板	SZSE	L	2025-06-17 15:57:24.280455
300672	国科微	300672	创业板	SZSE	L	2025-06-17 15:57:24.280455
300673	佩蒂股份	300673	创业板	SZSE	L	2025-06-17 15:57:24.280455
300674	宇信科技	300674	创业板	SZSE	L	2025-06-17 15:57:24.280455
300675	建科院	300675	创业板	SZSE	L	2025-06-17 15:57:24.280455
300676	华大基因	300676	创业板	SZSE	L	2025-06-17 15:57:24.280455
300677	英科医疗	300677	创业板	SZSE	L	2025-06-17 15:57:24.280455
300678	中科信息	300678	创业板	SZSE	L	2025-06-17 15:57:24.280455
300679	电连技术	300679	创业板	SZSE	L	2025-06-17 15:57:24.280455
300680	隆盛科技	300680	创业板	SZSE	L	2025-06-17 15:57:24.280455
300681	英搏尔	300681	创业板	SZSE	L	2025-06-17 15:57:24.280455
300682	朗新集团	300682	创业板	SZSE	L	2025-06-17 15:57:24.280455
300683	海特生物	300683	创业板	SZSE	L	2025-06-17 15:57:24.280455
300684	中石科技	300684	创业板	SZSE	L	2025-06-17 15:57:24.280455
300685	艾德生物	300685	创业板	SZSE	L	2025-06-17 15:57:24.280455
300686	智动力	300686	创业板	SZSE	L	2025-06-17 15:57:24.280455
300687	赛意信息	300687	创业板	SZSE	L	2025-06-17 15:57:24.280455
300688	创业黑马	300688	创业板	SZSE	L	2025-06-17 15:57:24.280455
300689	澄天伟业	300689	创业板	SZSE	L	2025-06-17 15:57:24.280455
300690	双一科技	300690	创业板	SZSE	L	2025-06-17 15:57:24.280455
300691	联合光电	300691	创业板	SZSE	L	2025-06-17 15:57:24.280455
300692	中环环保	300692	创业板	SZSE	L	2025-06-17 15:57:24.280455
300693	盛弘股份	300693	创业板	SZSE	L	2025-06-17 15:57:24.280455
300694	蠡湖股份	300694	创业板	SZSE	L	2025-06-17 15:57:24.280455
300695	兆丰股份	300695	创业板	SZSE	L	2025-06-17 15:57:24.280455
300696	爱乐达	300696	创业板	SZSE	L	2025-06-17 15:57:24.280455
300697	电工合金	300697	创业板	SZSE	L	2025-06-17 15:57:24.280455
300698	万马科技	300698	创业板	SZSE	L	2025-06-17 15:57:24.280455
300699	光威复材	300699	创业板	SZSE	L	2025-06-17 15:57:24.280455
300700	岱勒新材	300700	创业板	SZSE	L	2025-06-17 15:57:24.280455
300701	森霸传感	300701	创业板	SZSE	L	2025-06-17 15:57:24.280455
300702	天宇股份	300702	创业板	SZSE	L	2025-06-17 15:57:24.280455
300703	创源股份	300703	创业板	SZSE	L	2025-06-17 15:57:24.280455
300705	九典制药	300705	创业板	SZSE	L	2025-06-17 15:57:24.280455
300706	阿石创	300706	创业板	SZSE	L	2025-06-17 15:57:24.280455
300707	威唐工业	300707	创业板	SZSE	L	2025-06-17 15:57:24.280455
300708	聚灿光电	300708	创业板	SZSE	L	2025-06-17 15:57:24.280455
300709	精研科技	300709	创业板	SZSE	L	2025-06-17 15:57:24.280455
300710	万隆光电	300710	创业板	SZSE	L	2025-06-17 15:57:24.280455
300711	广哈通信	300711	创业板	SZSE	L	2025-06-17 15:57:24.280455
300712	永福股份	300712	创业板	SZSE	L	2025-06-17 15:57:24.280455
300713	英可瑞	300713	创业板	SZSE	L	2025-06-17 15:57:24.280455
300715	凯伦股份	300715	创业板	SZSE	L	2025-06-17 15:57:24.280455
300717	华信新材	300717	创业板	SZSE	L	2025-06-17 15:57:24.280455
300718	长盛轴承	300718	创业板	SZSE	L	2025-06-17 15:57:24.280455
300719	安达维尔	300719	创业板	SZSE	L	2025-06-17 15:57:24.280455
300720	海川智能	300720	创业板	SZSE	L	2025-06-17 15:57:24.280455
300721	怡达股份	300721	创业板	SZSE	L	2025-06-17 15:57:24.280455
300722	新余国科	300722	创业板	SZSE	L	2025-06-17 15:57:24.280455
300723	一品红	300723	创业板	SZSE	L	2025-06-17 15:57:24.280455
300724	捷佳伟创	300724	创业板	SZSE	L	2025-06-17 15:57:24.280455
300725	药石科技	300725	创业板	SZSE	L	2025-06-17 15:57:24.280455
300726	宏达电子	300726	创业板	SZSE	L	2025-06-17 15:57:24.280455
300727	润禾材料	300727	创业板	SZSE	L	2025-06-17 15:57:24.280455
300729	乐歌股份	300729	创业板	SZSE	L	2025-06-17 15:57:24.280455
300730	科创信息	300730	创业板	SZSE	L	2025-06-17 15:57:24.280455
300731	科创新源	300731	创业板	SZSE	L	2025-06-17 15:57:24.280455
300732	设研院	300732	创业板	SZSE	L	2025-06-17 15:57:24.280455
300733	西菱动力	300733	创业板	SZSE	L	2025-06-17 15:57:24.280455
300735	光弘科技	300735	创业板	SZSE	L	2025-06-17 15:57:24.280455
300736	百邦科技	300736	创业板	SZSE	L	2025-06-17 15:57:24.280455
300737	科顺股份	300737	创业板	SZSE	L	2025-06-17 15:57:24.280455
300738	奥飞数据	300738	创业板	SZSE	L	2025-06-17 15:57:24.280455
300739	明阳电路	300739	创业板	SZSE	L	2025-06-17 15:57:24.280455
300740	水羊股份	300740	创业板	SZSE	L	2025-06-17 15:57:24.280455
300741	华宝股份	300741	创业板	SZSE	L	2025-06-17 15:57:24.280455
300743	天地数码	300743	创业板	SZSE	L	2025-06-17 15:57:24.280455
300745	欣锐科技	300745	创业板	SZSE	L	2025-06-17 15:57:24.280455
300746	汉嘉设计	300746	创业板	SZSE	L	2025-06-17 15:57:24.280455
300747	锐科激光	300747	创业板	SZSE	L	2025-06-17 15:57:24.280455
300748	金力永磁	300748	创业板	SZSE	L	2025-06-17 15:57:24.280455
300749	顶固集创	300749	创业板	SZSE	L	2025-06-17 15:57:24.280455
300750	宁德时代	300750	创业板	SZSE	L	2025-06-17 15:57:24.280455
300751	迈为股份	300751	创业板	SZSE	L	2025-06-17 15:57:24.280455
300752	隆利科技	300752	创业板	SZSE	L	2025-06-17 15:57:24.280455
300753	爱朋医疗	300753	创业板	SZSE	L	2025-06-17 15:57:24.280455
300755	华致酒行	300755	创业板	SZSE	L	2025-06-17 15:57:24.280455
300756	金马游乐	300756	创业板	SZSE	L	2025-06-17 15:57:24.280455
300757	罗博特科	300757	创业板	SZSE	L	2025-06-17 15:57:24.280455
300758	七彩化学	300758	创业板	SZSE	L	2025-06-17 15:57:24.280455
300759	康龙化成	300759	创业板	SZSE	L	2025-06-17 15:57:24.280455
300760	迈瑞医疗	300760	创业板	SZSE	L	2025-06-17 15:57:24.280455
300761	立华股份	300761	创业板	SZSE	L	2025-06-17 15:57:24.280455
300762	上海瀚讯	300762	创业板	SZSE	L	2025-06-17 15:57:24.280455
300763	锦浪科技	300763	创业板	SZSE	L	2025-06-17 15:57:24.280455
300765	新诺威	300765	创业板	SZSE	L	2025-06-17 15:57:24.280455
300766	每日互动	300766	创业板	SZSE	L	2025-06-17 15:57:24.280455
300767	震安科技	300767	创业板	SZSE	L	2025-06-17 15:57:24.280455
300768	迪普科技	300768	创业板	SZSE	L	2025-06-17 15:57:24.280455
300769	德方纳米	300769	创业板	SZSE	L	2025-06-17 15:57:24.280455
300770	新媒股份	300770	创业板	SZSE	L	2025-06-17 15:57:24.280455
300771	智莱科技	300771	创业板	SZSE	L	2025-06-17 15:57:24.280455
300772	运达股份	300772	创业板	SZSE	L	2025-06-17 15:57:24.280455
300773	拉卡拉	300773	创业板	SZSE	L	2025-06-17 15:57:24.280455
300774	倍杰特	300774	创业板	SZSE	L	2025-06-17 15:57:24.280455
300775	三角防务	300775	创业板	SZSE	L	2025-06-17 15:57:24.280455
300776	帝尔激光	300776	创业板	SZSE	L	2025-06-17 15:57:24.280455
300777	中简科技	300777	创业板	SZSE	L	2025-06-17 15:57:24.280455
300778	新城市	300778	创业板	SZSE	L	2025-06-17 15:57:24.280455
300779	惠城环保	300779	创业板	SZSE	L	2025-06-17 15:57:24.280455
300780	德恩精工	300780	创业板	SZSE	L	2025-06-17 15:57:24.280455
300781	因赛集团	300781	创业板	SZSE	L	2025-06-17 15:57:24.280455
300782	卓胜微	300782	创业板	SZSE	L	2025-06-17 15:57:24.280455
300783	三只松鼠	300783	创业板	SZSE	L	2025-06-17 15:57:24.280455
300784	利安科技	300784	创业板	SZSE	L	2025-06-17 15:57:24.280455
300785	值得买	300785	创业板	SZSE	L	2025-06-17 15:57:24.280455
300786	国林科技	300786	创业板	SZSE	L	2025-06-17 15:57:24.280455
300787	海能实业	300787	创业板	SZSE	L	2025-06-17 15:57:24.280455
300788	中信出版	300788	创业板	SZSE	L	2025-06-17 15:57:24.280455
300789	唐源电气	300789	创业板	SZSE	L	2025-06-17 15:57:24.280455
300790	宇瞳光学	300790	创业板	SZSE	L	2025-06-17 15:57:24.280455
300791	仙乐健康	300791	创业板	SZSE	L	2025-06-17 15:57:24.280455
300792	壹网壹创	300792	创业板	SZSE	L	2025-06-17 15:57:24.280455
300793	佳禾智能	300793	创业板	SZSE	L	2025-06-17 15:57:24.280455
300795	米奥会展	300795	创业板	SZSE	L	2025-06-17 15:57:24.280455
300796	贝斯美	300796	创业板	SZSE	L	2025-06-17 15:57:24.280455
300797	钢研纳克	300797	创业板	SZSE	L	2025-06-17 15:57:24.280455
300798	锦鸡股份	300798	创业板	SZSE	L	2025-06-17 15:57:24.280455
300800	力合科技	300800	创业板	SZSE	L	2025-06-17 15:57:24.280455
300801	泰和科技	300801	创业板	SZSE	L	2025-06-17 15:57:24.280455
300802	矩子科技	300802	创业板	SZSE	L	2025-06-17 15:57:24.280455
300803	指南针	300803	创业板	SZSE	L	2025-06-17 15:57:24.280455
300804	广康生化	300804	创业板	SZSE	L	2025-06-17 15:57:24.280455
300805	电声股份	300805	创业板	SZSE	L	2025-06-17 15:57:24.280455
300806	斯迪克	300806	创业板	SZSE	L	2025-06-17 15:57:24.280455
300807	天迈科技	300807	创业板	SZSE	L	2025-06-17 15:57:24.280455
300808	久量股份	300808	创业板	SZSE	L	2025-06-17 15:57:24.280455
300809	华辰装备	300809	创业板	SZSE	L	2025-06-17 15:57:24.280455
300810	中科海讯	300810	创业板	SZSE	L	2025-06-17 15:57:24.280455
300811	铂科新材	300811	创业板	SZSE	L	2025-06-17 15:57:24.280455
300812	易天股份	300812	创业板	SZSE	L	2025-06-17 15:57:24.280455
300813	泰林生物	300813	创业板	SZSE	L	2025-06-17 15:57:24.280455
300814	中富电路	300814	创业板	SZSE	L	2025-06-17 15:57:24.280455
300815	玉禾田	300815	创业板	SZSE	L	2025-06-17 15:57:24.280455
300816	艾可蓝	300816	创业板	SZSE	L	2025-06-17 15:57:24.280455
300817	双飞集团	300817	创业板	SZSE	L	2025-06-17 15:57:24.280455
300818	耐普矿机	300818	创业板	SZSE	L	2025-06-17 15:57:24.280455
300819	聚杰微纤	300819	创业板	SZSE	L	2025-06-17 15:57:24.280455
300820	英杰电气	300820	创业板	SZSE	L	2025-06-17 15:57:24.280455
300821	东岳硅材	300821	创业板	SZSE	L	2025-06-17 15:57:24.280455
300822	贝仕达克	300822	创业板	SZSE	L	2025-06-17 15:57:24.280455
300823	建科智能	300823	创业板	SZSE	L	2025-06-17 15:57:24.280455
300824	北鼎股份	300824	创业板	SZSE	L	2025-06-17 15:57:24.280455
300825	阿尔特	300825	创业板	SZSE	L	2025-06-17 15:57:24.280455
300826	测绘股份	300826	创业板	SZSE	L	2025-06-17 15:57:24.280455
300827	上能电气	300827	创业板	SZSE	L	2025-06-17 15:57:24.280455
300828	锐新科技	300828	创业板	SZSE	L	2025-06-17 15:57:24.280455
300829	金丹科技	300829	创业板	SZSE	L	2025-06-17 15:57:24.280455
300830	金现代	300830	创业板	SZSE	L	2025-06-17 15:57:24.280455
300831	派瑞股份	300831	创业板	SZSE	L	2025-06-17 15:57:24.280455
300832	新产业	300832	创业板	SZSE	L	2025-06-17 15:57:24.280455
300833	浩洋股份	300833	创业板	SZSE	L	2025-06-17 15:57:24.280455
300834	星辉环材	300834	创业板	SZSE	L	2025-06-17 15:57:24.280455
300835	龙磁科技	300835	创业板	SZSE	L	2025-06-17 15:57:24.280455
300836	佰奥智能	300836	创业板	SZSE	L	2025-06-17 15:57:24.280455
300837	浙矿股份	300837	创业板	SZSE	L	2025-06-17 15:57:24.280455
300838	浙江力诺	300838	创业板	SZSE	L	2025-06-17 15:57:24.280455
300839	博汇股份	300839	创业板	SZSE	L	2025-06-17 15:57:24.280455
300840	酷特智能	300840	创业板	SZSE	L	2025-06-17 15:57:24.280455
300841	康华生物	300841	创业板	SZSE	L	2025-06-17 15:57:24.280455
300842	帝科股份	300842	创业板	SZSE	L	2025-06-17 15:57:24.280455
300843	胜蓝股份	300843	创业板	SZSE	L	2025-06-17 15:57:24.280455
300844	山水比德	300844	创业板	SZSE	L	2025-06-17 15:57:24.280455
300845	捷安高科	300845	创业板	SZSE	L	2025-06-17 15:57:24.280455
300846	首都在线	300846	创业板	SZSE	L	2025-06-17 15:57:24.280455
300847	中船汉光	300847	创业板	SZSE	L	2025-06-17 15:57:24.280455
300848	美瑞新材	300848	创业板	SZSE	L	2025-06-17 15:57:24.280455
300849	锦盛新材	300849	创业板	SZSE	L	2025-06-17 15:57:24.280455
300850	新强联	300850	创业板	SZSE	L	2025-06-17 15:57:24.280455
300851	交大思诺	300851	创业板	SZSE	L	2025-06-17 15:57:24.280455
300852	四会富仕	300852	创业板	SZSE	L	2025-06-17 15:57:24.280455
300853	申昊科技	300853	创业板	SZSE	L	2025-06-17 15:57:24.280455
300854	中兰环保	300854	创业板	SZSE	L	2025-06-17 15:57:24.280455
300855	图南股份	300855	创业板	SZSE	L	2025-06-17 15:57:24.280455
300856	科思股份	300856	创业板	SZSE	L	2025-06-17 15:57:24.280455
300857	协创数据	300857	创业板	SZSE	L	2025-06-17 15:57:24.280455
300858	科拓生物	300858	创业板	SZSE	L	2025-06-17 15:57:24.280455
300859	西域旅游	300859	创业板	SZSE	L	2025-06-17 15:57:24.280455
300860	锋尚文化	300860	创业板	SZSE	L	2025-06-17 15:57:24.280455
300861	美畅股份	300861	创业板	SZSE	L	2025-06-17 15:57:24.280455
300862	蓝盾光电	300862	创业板	SZSE	L	2025-06-17 15:57:24.280455
300863	卡倍亿	300863	创业板	SZSE	L	2025-06-17 15:57:24.280455
300864	南大环境	300864	创业板	SZSE	L	2025-06-17 15:57:24.280455
300865	大宏立	300865	创业板	SZSE	L	2025-06-17 15:57:24.280455
300866	安克创新	300866	创业板	SZSE	L	2025-06-17 15:57:24.280455
300867	圣元环保	300867	创业板	SZSE	L	2025-06-17 15:57:24.280455
300868	杰美特	300868	创业板	SZSE	L	2025-06-17 15:57:24.280455
300869	康泰医学	300869	创业板	SZSE	L	2025-06-17 15:57:24.280455
300870	欧陆通	300870	创业板	SZSE	L	2025-06-17 15:57:24.280455
300871	回盛生物	300871	创业板	SZSE	L	2025-06-17 15:57:24.280455
300872	天阳科技	300872	创业板	SZSE	L	2025-06-17 15:57:24.280455
300873	海晨股份	300873	创业板	SZSE	L	2025-06-17 15:57:24.280455
300875	捷强装备	300875	创业板	SZSE	L	2025-06-17 15:57:24.280455
300876	蒙泰高新	300876	创业板	SZSE	L	2025-06-17 15:57:24.280455
300877	金春股份	300877	创业板	SZSE	L	2025-06-17 15:57:24.280455
300878	维康药业	300878	创业板	SZSE	L	2025-06-17 15:57:24.280455
300879	大叶股份	300879	创业板	SZSE	L	2025-06-17 15:57:24.280455
300880	迦南智能	300880	创业板	SZSE	L	2025-06-17 15:57:24.280455
300881	盛德鑫泰	300881	创业板	SZSE	L	2025-06-17 15:57:24.280455
300882	万胜智能	300882	创业板	SZSE	L	2025-06-17 15:57:24.280455
300883	龙利得	300883	创业板	SZSE	L	2025-06-17 15:57:24.280455
300884	狄耐克	300884	创业板	SZSE	L	2025-06-17 15:57:24.280455
300885	海昌新材	300885	创业板	SZSE	L	2025-06-17 15:57:24.280455
300886	华业香料	300886	创业板	SZSE	L	2025-06-17 15:57:24.280455
300887	谱尼测试	300887	创业板	SZSE	L	2025-06-17 15:57:24.280455
300888	稳健医疗	300888	创业板	SZSE	L	2025-06-17 15:57:24.280455
300889	爱克股份	300889	创业板	SZSE	L	2025-06-17 15:57:24.280455
300890	翔丰华	300890	创业板	SZSE	L	2025-06-17 15:57:24.280455
300891	惠云钛业	300891	创业板	SZSE	L	2025-06-17 15:57:24.280455
300892	品渥食品	300892	创业板	SZSE	L	2025-06-17 15:57:24.280455
300893	松原安全	300893	创业板	SZSE	L	2025-06-17 15:57:24.280455
300894	火星人	300894	创业板	SZSE	L	2025-06-17 15:57:24.280455
300895	铜牛信息	300895	创业板	SZSE	L	2025-06-17 15:57:24.280455
300896	爱美客	300896	创业板	SZSE	L	2025-06-17 15:57:24.280455
300897	山科智能	300897	创业板	SZSE	L	2025-06-17 15:57:24.280455
300898	熊猫乳品	300898	创业板	SZSE	L	2025-06-17 15:57:24.280455
300900	广联航空	300900	创业板	SZSE	L	2025-06-17 15:57:24.280455
300901	中胤时尚	300901	创业板	SZSE	L	2025-06-17 15:57:24.280455
300902	国安达	300902	创业板	SZSE	L	2025-06-17 15:57:24.280455
300903	科翔股份	300903	创业板	SZSE	L	2025-06-17 15:57:24.280455
300904	威力传动	300904	创业板	SZSE	L	2025-06-17 15:57:24.280455
300905	宝丽迪	300905	创业板	SZSE	L	2025-06-17 15:57:24.280455
300906	日月明	300906	创业板	SZSE	L	2025-06-17 15:57:24.280455
300907	康平科技	300907	创业板	SZSE	L	2025-06-17 15:57:24.280455
300908	仲景食品	300908	创业板	SZSE	L	2025-06-17 15:57:24.280455
300909	汇创达	300909	创业板	SZSE	L	2025-06-17 15:57:24.280455
300910	瑞丰新材	300910	创业板	SZSE	L	2025-06-17 15:57:24.280455
300911	亿田智能	300911	创业板	SZSE	L	2025-06-17 15:57:24.280455
300912	凯龙高科	300912	创业板	SZSE	L	2025-06-17 15:57:24.280455
300913	兆龙互连	300913	创业板	SZSE	L	2025-06-17 15:57:24.280455
300915	海融科技	300915	创业板	SZSE	L	2025-06-17 15:57:24.280455
300916	朗特智能	300916	创业板	SZSE	L	2025-06-17 15:57:24.280455
300917	特发服务	300917	创业板	SZSE	L	2025-06-17 15:57:24.280455
300918	南山智尚	300918	创业板	SZSE	L	2025-06-17 15:57:24.280455
300919	中伟股份	300919	创业板	SZSE	L	2025-06-17 15:57:24.280455
300920	润阳科技	300920	创业板	SZSE	L	2025-06-17 15:57:24.280455
300921	南凌科技	300921	创业板	SZSE	L	2025-06-17 15:57:24.280455
300922	天秦装备	300922	创业板	SZSE	L	2025-06-17 15:57:24.280455
300923	研奥股份	300923	创业板	SZSE	L	2025-06-17 15:57:24.280455
300925	法本信息	300925	创业板	SZSE	L	2025-06-17 15:57:24.280455
300926	博俊科技	300926	创业板	SZSE	L	2025-06-17 15:57:24.280455
300927	江天化学	300927	创业板	SZSE	L	2025-06-17 15:57:24.280455
300928	华安鑫创	300928	创业板	SZSE	L	2025-06-17 15:57:24.280455
300929	华骐环保	300929	创业板	SZSE	L	2025-06-17 15:57:24.280455
300930	屹通新材	300930	创业板	SZSE	L	2025-06-17 15:57:24.280455
300931	通用电梯	300931	创业板	SZSE	L	2025-06-17 15:57:24.280455
300932	三友联众	300932	创业板	SZSE	L	2025-06-17 15:57:24.280455
300933	中辰股份	300933	创业板	SZSE	L	2025-06-17 15:57:24.280455
300935	盈建科	300935	创业板	SZSE	L	2025-06-17 15:57:24.280455
300936	中英科技	300936	创业板	SZSE	L	2025-06-17 15:57:24.280455
300937	药易购	300937	创业板	SZSE	L	2025-06-17 15:57:24.280455
300938	信测标准	300938	创业板	SZSE	L	2025-06-17 15:57:24.280455
300939	秋田微	300939	创业板	SZSE	L	2025-06-17 15:57:24.280455
300940	南极光	300940	创业板	SZSE	L	2025-06-17 15:57:24.280455
300941	创识科技	300941	创业板	SZSE	L	2025-06-17 15:57:24.280455
300942	易瑞生物	300942	创业板	SZSE	L	2025-06-17 15:57:24.280455
300943	春晖智控	300943	创业板	SZSE	L	2025-06-17 15:57:24.280455
300945	曼卡龙	300945	创业板	SZSE	L	2025-06-17 15:57:24.280455
300946	恒而达	300946	创业板	SZSE	L	2025-06-17 15:57:24.280455
300947	德必集团	300947	创业板	SZSE	L	2025-06-17 15:57:24.280455
300948	冠中生态	300948	创业板	SZSE	L	2025-06-17 15:57:24.280455
300949	奥雅股份	300949	创业板	SZSE	L	2025-06-17 15:57:24.280455
300950	德固特	300950	创业板	SZSE	L	2025-06-17 15:57:24.280455
300951	博硕科技	300951	创业板	SZSE	L	2025-06-17 15:57:24.280455
300952	恒辉安防	300952	创业板	SZSE	L	2025-06-17 15:57:24.280455
300953	震裕科技	300953	创业板	SZSE	L	2025-06-17 15:57:24.280455
300955	嘉亨家化	300955	创业板	SZSE	L	2025-06-17 15:57:24.280455
300956	英力股份	300956	创业板	SZSE	L	2025-06-17 15:57:24.280455
300957	贝泰妮	300957	创业板	SZSE	L	2025-06-17 15:57:24.280455
300958	建工修复	300958	创业板	SZSE	L	2025-06-17 15:57:24.280455
300959	线上线下	300959	创业板	SZSE	L	2025-06-17 15:57:24.280455
300960	通业科技	300960	创业板	SZSE	L	2025-06-17 15:57:24.280455
300961	深水海纳	300961	创业板	SZSE	L	2025-06-17 15:57:24.280455
300962	中金辐照	300962	创业板	SZSE	L	2025-06-17 15:57:24.280455
300963	中洲特材	300963	创业板	SZSE	L	2025-06-17 15:57:24.280455
300964	本川智能	300964	创业板	SZSE	L	2025-06-17 15:57:24.280455
300965	恒宇信通	300965	创业板	SZSE	L	2025-06-17 15:57:24.280455
300966	共同药业	300966	创业板	SZSE	L	2025-06-17 15:57:24.280455
300967	晓鸣股份	300967	创业板	SZSE	L	2025-06-17 15:57:24.280455
300968	格林精密	300968	创业板	SZSE	L	2025-06-17 15:57:24.280455
300969	恒帅股份	300969	创业板	SZSE	L	2025-06-17 15:57:24.280455
300970	华绿生物	300970	创业板	SZSE	L	2025-06-17 15:57:24.280455
300971	博亚精工	300971	创业板	SZSE	L	2025-06-17 15:57:24.280455
300972	万辰集团	300972	创业板	SZSE	L	2025-06-17 15:57:24.280455
300973	立高食品	300973	创业板	SZSE	L	2025-06-17 15:57:24.280455
300975	商络电子	300975	创业板	SZSE	L	2025-06-17 15:57:24.280455
300976	达瑞电子	300976	创业板	SZSE	L	2025-06-17 15:57:24.280455
300977	深圳瑞捷	300977	创业板	SZSE	L	2025-06-17 15:57:24.280455
300978	东箭科技	300978	创业板	SZSE	L	2025-06-17 15:57:24.280455
300979	华利集团	300979	创业板	SZSE	L	2025-06-17 15:57:24.280455
300980	祥源新材	300980	创业板	SZSE	L	2025-06-17 15:57:24.280455
300981	中红医疗	300981	创业板	SZSE	L	2025-06-17 15:57:24.280455
300982	苏文电能	300982	创业板	SZSE	L	2025-06-17 15:57:24.280455
300983	尤安设计	300983	创业板	SZSE	L	2025-06-17 15:57:24.280455
300984	金沃股份	300984	创业板	SZSE	L	2025-06-17 15:57:24.280455
300985	致远新能	300985	创业板	SZSE	L	2025-06-17 15:57:24.280455
300986	志特新材	300986	创业板	SZSE	L	2025-06-17 15:57:24.280455
300987	川网传媒	300987	创业板	SZSE	L	2025-06-17 15:57:24.280455
300988	津荣天宇	300988	创业板	SZSE	L	2025-06-17 15:57:24.280455
300989	蕾奥规划	300989	创业板	SZSE	L	2025-06-17 15:57:24.280455
300990	同飞股份	300990	创业板	SZSE	L	2025-06-17 15:57:24.280455
300991	创益通	300991	创业板	SZSE	L	2025-06-17 15:57:24.280455
300992	泰福泵业	300992	创业板	SZSE	L	2025-06-17 15:57:24.280455
300993	玉马科技	300993	创业板	SZSE	L	2025-06-17 15:57:24.280455
300994	久祺股份	300994	创业板	SZSE	L	2025-06-17 15:57:24.280455
300995	奇德新材	300995	创业板	SZSE	L	2025-06-17 15:57:24.280455
300996	普联软件	300996	创业板	SZSE	L	2025-06-17 15:57:24.280455
300997	欢乐家	300997	创业板	SZSE	L	2025-06-17 15:57:24.280455
300998	宁波方正	300998	创业板	SZSE	L	2025-06-17 15:57:24.280455
300999	金龙鱼	300999	创业板	SZSE	L	2025-06-17 15:57:24.280455
301000	肇民科技	301000	创业板	SZSE	L	2025-06-17 15:57:24.280455
301001	凯淳股份	301001	创业板	SZSE	L	2025-06-17 15:57:24.280455
301002	崧盛股份	301002	创业板	SZSE	L	2025-06-17 15:57:24.280455
301003	江苏博云	301003	创业板	SZSE	L	2025-06-17 15:57:24.280455
301004	嘉益股份	301004	创业板	SZSE	L	2025-06-17 15:57:24.280455
301005	超捷股份	301005	创业板	SZSE	L	2025-06-17 15:57:24.280455
301006	迈拓股份	301006	创业板	SZSE	L	2025-06-17 15:57:24.280455
301007	德迈仕	301007	创业板	SZSE	L	2025-06-17 15:57:24.280455
301008	宏昌科技	301008	创业板	SZSE	L	2025-06-17 15:57:24.280455
301009	可靠股份	301009	创业板	SZSE	L	2025-06-17 15:57:24.280455
301010	晶雪节能	301010	创业板	SZSE	L	2025-06-17 15:57:24.280455
301011	华立科技	301011	创业板	SZSE	L	2025-06-17 15:57:24.280455
301012	扬电科技	301012	创业板	SZSE	L	2025-06-17 15:57:24.280455
301013	利和兴	301013	创业板	SZSE	L	2025-06-17 15:57:24.280455
301015	百洋医药	301015	创业板	SZSE	L	2025-06-17 15:57:24.280455
301016	雷尔伟	301016	创业板	SZSE	L	2025-06-17 15:57:24.280455
301017	漱玉平民	301017	创业板	SZSE	L	2025-06-17 15:57:24.280455
301018	申菱环境	301018	创业板	SZSE	L	2025-06-17 15:57:24.280455
301019	宁波色母	301019	创业板	SZSE	L	2025-06-17 15:57:24.280455
301020	密封科技	301020	创业板	SZSE	L	2025-06-17 15:57:24.280455
301021	英诺激光	301021	创业板	SZSE	L	2025-06-17 15:57:24.280455
301022	海泰科	301022	创业板	SZSE	L	2025-06-17 15:57:24.280455
301023	江南奕帆	301023	创业板	SZSE	L	2025-06-17 15:57:24.280455
301024	霍普股份	301024	创业板	SZSE	L	2025-06-17 15:57:24.280455
301025	读客文化	301025	创业板	SZSE	L	2025-06-17 15:57:24.280455
301026	浩通科技	301026	创业板	SZSE	L	2025-06-17 15:57:24.280455
301027	华蓝集团	301027	创业板	SZSE	L	2025-06-17 15:57:24.280455
301028	东亚机械	301028	创业板	SZSE	L	2025-06-17 15:57:24.280455
301029	怡合达	301029	创业板	SZSE	L	2025-06-17 15:57:24.280455
301030	仕净科技	301030	创业板	SZSE	L	2025-06-17 15:57:24.280455
301031	中熔电气	301031	创业板	SZSE	L	2025-06-17 15:57:24.280455
301032	新柴股份	301032	创业板	SZSE	L	2025-06-17 15:57:24.280455
301033	迈普医学	301033	创业板	SZSE	L	2025-06-17 15:57:24.280455
301035	润丰股份	301035	创业板	SZSE	L	2025-06-17 15:57:24.280455
301036	双乐股份	301036	创业板	SZSE	L	2025-06-17 15:57:24.280455
301037	保立佳	301037	创业板	SZSE	L	2025-06-17 15:57:24.280455
301038	深水规院	301038	创业板	SZSE	L	2025-06-17 15:57:24.280455
301039	中集车辆	301039	创业板	SZSE	L	2025-06-17 15:57:24.280455
301040	中环海陆	301040	创业板	SZSE	L	2025-06-17 15:57:24.280455
301041	金百泽	301041	创业板	SZSE	L	2025-06-17 15:57:24.280455
301042	安联锐视	301042	创业板	SZSE	L	2025-06-17 15:57:24.280455
301043	绿岛风	301043	创业板	SZSE	L	2025-06-17 15:57:24.280455
301045	天禄科技	301045	创业板	SZSE	L	2025-06-17 15:57:24.280455
301046	能辉科技	301046	创业板	SZSE	L	2025-06-17 15:57:24.280455
301047	义翘神州	301047	创业板	SZSE	L	2025-06-17 15:57:24.280455
301048	金鹰重工	301048	创业板	SZSE	L	2025-06-17 15:57:24.280455
301049	超越科技	301049	创业板	SZSE	L	2025-06-17 15:57:24.280455
301050	雷电微力	301050	创业板	SZSE	L	2025-06-17 15:57:24.280455
301051	信濠光电	301051	创业板	SZSE	L	2025-06-17 15:57:24.280455
301052	果麦文化	301052	创业板	SZSE	L	2025-06-17 15:57:24.280455
301053	远信工业	301053	创业板	SZSE	L	2025-06-17 15:57:24.280455
301055	张小泉	301055	创业板	SZSE	L	2025-06-17 15:57:24.280455
301056	森赫股份	301056	创业板	SZSE	L	2025-06-17 15:57:24.280455
301057	汇隆新材	301057	创业板	SZSE	L	2025-06-17 15:57:24.280455
301058	中粮科工	301058	创业板	SZSE	L	2025-06-17 15:57:24.280455
301059	金三江	301059	创业板	SZSE	L	2025-06-17 15:57:24.280455
301060	兰卫医学	301060	创业板	SZSE	L	2025-06-17 15:57:24.280455
301061	匠心家居	301061	创业板	SZSE	L	2025-06-17 15:57:24.280455
301062	上海艾录	301062	创业板	SZSE	L	2025-06-17 15:57:24.280455
301063	海锅股份	301063	创业板	SZSE	L	2025-06-17 15:57:24.280455
301065	本立科技	301065	创业板	SZSE	L	2025-06-17 15:57:24.280455
301066	万事利	301066	创业板	SZSE	L	2025-06-17 15:57:24.280455
301067	显盈科技	301067	创业板	SZSE	L	2025-06-17 15:57:24.280455
301068	大地海洋	301068	创业板	SZSE	L	2025-06-17 15:57:24.280455
301069	凯盛新材	301069	创业板	SZSE	L	2025-06-17 15:57:24.280455
301070	开勒股份	301070	创业板	SZSE	L	2025-06-17 15:57:24.280455
301071	力量钻石	301071	创业板	SZSE	L	2025-06-17 15:57:24.280455
301072	中捷精工	301072	创业板	SZSE	L	2025-06-17 15:57:24.280455
301073	君亭酒店	301073	创业板	SZSE	L	2025-06-17 15:57:24.280455
301075	多瑞医药	301075	创业板	SZSE	L	2025-06-17 15:57:24.280455
301076	新瀚新材	301076	创业板	SZSE	L	2025-06-17 15:57:24.280455
301077	星华新材	301077	创业板	SZSE	L	2025-06-17 15:57:24.280455
301078	孩子王	301078	创业板	SZSE	L	2025-06-17 15:57:24.280455
301079	邵阳液压	301079	创业板	SZSE	L	2025-06-17 15:57:24.280455
301080	百普赛斯	301080	创业板	SZSE	L	2025-06-17 15:57:24.280455
301081	严牌股份	301081	创业板	SZSE	L	2025-06-17 15:57:24.280455
301082	久盛电气	301082	创业板	SZSE	L	2025-06-17 15:57:24.280455
301083	百胜智能	301083	创业板	SZSE	L	2025-06-17 15:57:24.280455
301085	亚康股份	301085	创业板	SZSE	L	2025-06-17 15:57:24.280455
301086	鸿富瀚	301086	创业板	SZSE	L	2025-06-17 15:57:24.280455
301087	可孚医疗	301087	创业板	SZSE	L	2025-06-17 15:57:24.280455
301088	戎美股份	301088	创业板	SZSE	L	2025-06-17 15:57:24.280455
301089	拓新药业	301089	创业板	SZSE	L	2025-06-17 15:57:24.280455
301090	华润材料	301090	创业板	SZSE	L	2025-06-17 15:57:24.280455
301091	深城交	301091	创业板	SZSE	L	2025-06-17 15:57:24.280455
301092	争光股份	301092	创业板	SZSE	L	2025-06-17 15:57:24.280455
301093	华兰股份	301093	创业板	SZSE	L	2025-06-17 15:57:24.280455
301095	广立微	301095	创业板	SZSE	L	2025-06-17 15:57:24.280455
301096	百诚医药	301096	创业板	SZSE	L	2025-06-17 15:57:24.280455
301097	天益医疗	301097	创业板	SZSE	L	2025-06-17 15:57:24.280455
301098	金埔园林	301098	创业板	SZSE	L	2025-06-17 15:57:24.280455
301099	雅创电子	301099	创业板	SZSE	L	2025-06-17 15:57:24.280455
301100	风光股份	301100	创业板	SZSE	L	2025-06-17 15:57:24.280455
301101	明月镜片	301101	创业板	SZSE	L	2025-06-17 15:57:24.280455
301102	兆讯传媒	301102	创业板	SZSE	L	2025-06-17 15:57:24.280455
301103	何氏眼科	301103	创业板	SZSE	L	2025-06-17 15:57:24.280455
301105	鸿铭股份	301105	创业板	SZSE	L	2025-06-17 15:57:24.280455
301106	骏成科技	301106	创业板	SZSE	L	2025-06-17 15:57:24.280455
301107	瑜欣电子	301107	创业板	SZSE	L	2025-06-17 15:57:24.280455
301108	洁雅股份	301108	创业板	SZSE	L	2025-06-17 15:57:24.280455
301109	军信股份	301109	创业板	SZSE	L	2025-06-17 15:57:24.280455
301110	青木科技	301110	创业板	SZSE	L	2025-06-17 15:57:24.280455
301111	粤万年青	301111	创业板	SZSE	L	2025-06-17 15:57:24.280455
301112	信邦智能	301112	创业板	SZSE	L	2025-06-17 15:57:24.280455
301113	雅艺科技	301113	创业板	SZSE	L	2025-06-17 15:57:24.280455
301115	联检科技	301115	创业板	SZSE	L	2025-06-17 15:57:24.280455
301116	益客食品	301116	创业板	SZSE	L	2025-06-17 15:57:24.280455
301117	佳缘科技	301117	创业板	SZSE	L	2025-06-17 15:57:24.280455
301118	恒光股份	301118	创业板	SZSE	L	2025-06-17 15:57:24.280455
301119	正强股份	301119	创业板	SZSE	L	2025-06-17 15:57:24.280455
301120	新特电气	301120	创业板	SZSE	L	2025-06-17 15:57:24.280455
301121	紫建电子	301121	创业板	SZSE	L	2025-06-17 15:57:24.280455
301122	采纳股份	301122	创业板	SZSE	L	2025-06-17 15:57:24.280455
301123	奕东电子	301123	创业板	SZSE	L	2025-06-17 15:57:24.280455
301125	腾亚精工	301125	创业板	SZSE	L	2025-06-17 15:57:24.280455
301126	达嘉维康	301126	创业板	SZSE	L	2025-06-17 15:57:24.280455
301127	武汉天源	301127	创业板	SZSE	L	2025-06-17 15:57:24.280455
301128	强瑞技术	301128	创业板	SZSE	L	2025-06-17 15:57:24.280455
301129	瑞纳智能	301129	创业板	SZSE	L	2025-06-17 15:57:24.280455
301130	西点药业	301130	创业板	SZSE	L	2025-06-17 15:57:24.280455
301131	聚赛龙	301131	创业板	SZSE	L	2025-06-17 15:57:24.280455
301132	满坤科技	301132	创业板	SZSE	L	2025-06-17 15:57:24.280455
301133	金钟股份	301133	创业板	SZSE	L	2025-06-17 15:57:24.280455
301135	瑞德智能	301135	创业板	SZSE	L	2025-06-17 15:57:24.280455
301136	招标股份	301136	创业板	SZSE	L	2025-06-17 15:57:24.280455
301137	哈焊华通	301137	创业板	SZSE	L	2025-06-17 15:57:24.280455
301138	华研精机	301138	创业板	SZSE	L	2025-06-17 15:57:24.280455
301139	元道通信	301139	创业板	SZSE	L	2025-06-17 15:57:24.280455
301141	中科磁业	301141	创业板	SZSE	L	2025-06-17 15:57:24.280455
301148	嘉戎技术	301148	创业板	SZSE	L	2025-06-17 15:57:24.280455
301149	隆华新材	301149	创业板	SZSE	L	2025-06-17 15:57:24.280455
301150	中一科技	301150	创业板	SZSE	L	2025-06-17 15:57:24.280455
301151	冠龙节能	301151	创业板	SZSE	L	2025-06-17 15:57:24.280455
301152	天力锂能	301152	创业板	SZSE	L	2025-06-17 15:57:24.280455
301153	中科江南	301153	创业板	SZSE	L	2025-06-17 15:57:24.280455
301155	海力风电	301155	创业板	SZSE	L	2025-06-17 15:57:24.280455
301156	美农生物	301156	创业板	SZSE	L	2025-06-17 15:57:24.280455
301157	华塑科技	301157	创业板	SZSE	L	2025-06-17 15:57:24.280455
301158	德石股份	301158	创业板	SZSE	L	2025-06-17 15:57:24.280455
301159	三维天地	301159	创业板	SZSE	L	2025-06-17 15:57:24.280455
301160	翔楼新材	301160	创业板	SZSE	L	2025-06-17 15:57:24.280455
301161	唯万密封	301161	创业板	SZSE	L	2025-06-17 15:57:24.280455
301162	国能日新	301162	创业板	SZSE	L	2025-06-17 15:57:24.280455
301163	宏德股份	301163	创业板	SZSE	L	2025-06-17 15:57:24.280455
301165	锐捷网络	301165	创业板	SZSE	L	2025-06-17 15:57:24.280455
301166	优宁维	301166	创业板	SZSE	L	2025-06-17 15:57:24.280455
301167	建研设计	301167	创业板	SZSE	L	2025-06-17 15:57:24.280455
301168	通灵股份	301168	创业板	SZSE	L	2025-06-17 15:57:24.280455
301169	零点有数	301169	创业板	SZSE	L	2025-06-17 15:57:24.280455
301170	锡南科技	301170	创业板	SZSE	L	2025-06-17 15:57:24.280455
301171	易点天下	301171	创业板	SZSE	L	2025-06-17 15:57:24.280455
301172	君逸数码	301172	创业板	SZSE	L	2025-06-17 15:57:24.280455
301173	毓恬冠佳	301173	创业板	SZSE	L	2025-06-17 15:57:24.280455
301175	中科环保	301175	创业板	SZSE	L	2025-06-17 15:57:24.280455
301176	逸豪新材	301176	创业板	SZSE	L	2025-06-17 15:57:24.280455
301177	迪阿股份	301177	创业板	SZSE	L	2025-06-17 15:57:24.280455
301178	天亿马	301178	创业板	SZSE	L	2025-06-17 15:57:24.280455
301179	泽宇智能	301179	创业板	SZSE	L	2025-06-17 15:57:24.280455
301180	万祥科技	301180	创业板	SZSE	L	2025-06-17 15:57:24.280455
301181	标榜股份	301181	创业板	SZSE	L	2025-06-17 15:57:24.280455
301182	凯旺科技	301182	创业板	SZSE	L	2025-06-17 15:57:24.280455
301183	东田微	301183	创业板	SZSE	L	2025-06-17 15:57:24.280455
301185	鸥玛软件	301185	创业板	SZSE	L	2025-06-17 15:57:24.280455
301186	超达装备	301186	创业板	SZSE	L	2025-06-17 15:57:24.280455
301187	欧圣电气	301187	创业板	SZSE	L	2025-06-17 15:57:24.280455
301188	力诺药包	301188	创业板	SZSE	L	2025-06-17 15:57:24.280455
301189	奥尼电子	301189	创业板	SZSE	L	2025-06-17 15:57:24.280455
301190	善水科技	301190	创业板	SZSE	L	2025-06-17 15:57:24.280455
301191	菲菱科思	301191	创业板	SZSE	L	2025-06-17 15:57:24.280455
301192	泰祥股份	301192	创业板	SZSE	L	2025-06-17 15:57:24.280455
301193	家联科技	301193	创业板	SZSE	L	2025-06-17 15:57:24.280455
301195	北路智控	301195	创业板	SZSE	L	2025-06-17 15:57:24.280455
301196	唯科科技	301196	创业板	SZSE	L	2025-06-17 15:57:24.280455
301197	工大科雅	301197	创业板	SZSE	L	2025-06-17 15:57:24.280455
301198	喜悦智行	301198	创业板	SZSE	L	2025-06-17 15:57:24.280455
301199	迈赫股份	301199	创业板	SZSE	L	2025-06-17 15:57:24.280455
301200	大族数控	301200	创业板	SZSE	L	2025-06-17 15:57:24.280455
301201	诚达药业	301201	创业板	SZSE	L	2025-06-17 15:57:24.280455
301202	朗威股份	301202	创业板	SZSE	L	2025-06-17 15:57:24.280455
301203	国泰环保	301203	创业板	SZSE	L	2025-06-17 15:57:24.280455
301205	联特科技	301205	创业板	SZSE	L	2025-06-17 15:57:24.280455
301206	三元生物	301206	创业板	SZSE	L	2025-06-17 15:57:24.280455
301207	华兰疫苗	301207	创业板	SZSE	L	2025-06-17 15:57:24.280455
301208	中亦科技	301208	创业板	SZSE	L	2025-06-17 15:57:24.280455
301209	联合化学	301209	创业板	SZSE	L	2025-06-17 15:57:24.280455
301210	金杨股份	301210	创业板	SZSE	L	2025-06-17 15:57:24.280455
301211	亨迪药业	301211	创业板	SZSE	L	2025-06-17 15:57:24.280455
301212	联盛化学	301212	创业板	SZSE	L	2025-06-17 15:57:24.280455
301213	观想科技	301213	创业板	SZSE	L	2025-06-17 15:57:24.280455
301215	中汽股份	301215	创业板	SZSE	L	2025-06-17 15:57:24.280455
301216	万凯新材	301216	创业板	SZSE	L	2025-06-17 15:57:24.280455
301217	铜冠铜箔	301217	创业板	SZSE	L	2025-06-17 15:57:24.280455
301218	华是科技	301218	创业板	SZSE	L	2025-06-17 15:57:24.280455
301219	腾远钴业	301219	创业板	SZSE	L	2025-06-17 15:57:24.280455
301220	亚香股份	301220	创业板	SZSE	L	2025-06-17 15:57:24.280455
301221	光庭信息	301221	创业板	SZSE	L	2025-06-17 15:57:24.280455
301222	浙江恒威	301222	创业板	SZSE	L	2025-06-17 15:57:24.280455
301223	中荣股份	301223	创业板	SZSE	L	2025-06-17 15:57:24.280455
301225	恒勃股份	301225	创业板	SZSE	L	2025-06-17 15:57:24.280455
301226	祥明智能	301226	创业板	SZSE	L	2025-06-17 15:57:24.280455
301227	森鹰窗业	301227	创业板	SZSE	L	2025-06-17 15:57:24.280455
301228	实朴检测	301228	创业板	SZSE	L	2025-06-17 15:57:24.280455
301229	纽泰格	301229	创业板	SZSE	L	2025-06-17 15:57:24.280455
301230	泓博医药	301230	创业板	SZSE	L	2025-06-17 15:57:24.280455
301231	荣信文化	301231	创业板	SZSE	L	2025-06-17 15:57:24.280455
301232	飞沃科技	301232	创业板	SZSE	L	2025-06-17 15:57:24.280455
301233	盛帮股份	301233	创业板	SZSE	L	2025-06-17 15:57:24.280455
301234	五洲医疗	301234	创业板	SZSE	L	2025-06-17 15:57:24.280455
301235	华康洁净	301235	创业板	SZSE	L	2025-06-17 15:57:24.280455
301236	软通动力	301236	创业板	SZSE	L	2025-06-17 15:57:24.280455
301237	和顺科技	301237	创业板	SZSE	L	2025-06-17 15:57:24.280455
301238	瑞泰新材	301238	创业板	SZSE	L	2025-06-17 15:57:24.280455
301239	普瑞眼科	301239	创业板	SZSE	L	2025-06-17 15:57:24.280455
301246	宏源药业	301246	创业板	SZSE	L	2025-06-17 15:57:24.280455
301248	杰创智能	301248	创业板	SZSE	L	2025-06-17 15:57:24.280455
301251	威尔高	301251	创业板	SZSE	L	2025-06-17 15:57:24.280455
301252	同星科技	301252	创业板	SZSE	L	2025-06-17 15:57:24.280455
301255	通力科技	301255	创业板	SZSE	L	2025-06-17 15:57:24.280455
301256	华融化学	301256	创业板	SZSE	L	2025-06-17 15:57:24.280455
301257	普蕊斯	301257	创业板	SZSE	L	2025-06-17 15:57:24.280455
301258	富士莱	301258	创业板	SZSE	L	2025-06-17 15:57:24.280455
301259	艾布鲁	301259	创业板	SZSE	L	2025-06-17 15:57:24.280455
301260	格力博	301260	创业板	SZSE	L	2025-06-17 15:57:24.280455
301261	恒工精密	301261	创业板	SZSE	L	2025-06-17 15:57:24.280455
301262	海看股份	301262	创业板	SZSE	L	2025-06-17 15:57:24.280455
301263	泰恩康	301263	创业板	SZSE	L	2025-06-17 15:57:24.280455
301265	华新环保	301265	创业板	SZSE	L	2025-06-17 15:57:24.280455
301266	宇邦新材	301266	创业板	SZSE	L	2025-06-17 15:57:24.280455
301267	华厦眼科	301267	创业板	SZSE	L	2025-06-17 15:57:24.280455
301268	铭利达	301268	创业板	SZSE	L	2025-06-17 15:57:24.280455
301269	华大九天	301269	创业板	SZSE	L	2025-06-17 15:57:24.280455
301270	汉仪股份	301270	创业板	SZSE	L	2025-06-17 15:57:24.280455
301272	英华特	301272	创业板	SZSE	L	2025-06-17 15:57:24.280455
301273	瑞晨环保	301273	创业板	SZSE	L	2025-06-17 15:57:24.280455
301275	汉朔科技	301275	创业板	SZSE	L	2025-06-17 15:57:24.280455
301276	嘉曼服饰	301276	创业板	SZSE	L	2025-06-17 15:57:24.280455
301277	新天地	301277	创业板	SZSE	L	2025-06-17 15:57:24.280455
301278	快可电子	301278	创业板	SZSE	L	2025-06-17 15:57:24.280455
301279	金道科技	301279	创业板	SZSE	L	2025-06-17 15:57:24.280455
301280	珠城科技	301280	创业板	SZSE	L	2025-06-17 15:57:24.280455
301281	科源制药	301281	创业板	SZSE	L	2025-06-17 15:57:24.280455
301282	金禄电子	301282	创业板	SZSE	L	2025-06-17 15:57:24.280455
301283	聚胶股份	301283	创业板	SZSE	L	2025-06-17 15:57:24.280455
301285	鸿日达	301285	创业板	SZSE	L	2025-06-17 15:57:24.280455
301286	侨源股份	301286	创业板	SZSE	L	2025-06-17 15:57:24.280455
301287	康力源	301287	创业板	SZSE	L	2025-06-17 15:57:24.280455
301289	国缆检测	301289	创业板	SZSE	L	2025-06-17 15:57:24.280455
301290	东星医疗	301290	创业板	SZSE	L	2025-06-17 15:57:24.280455
301291	明阳电气	301291	创业板	SZSE	L	2025-06-17 15:57:24.280455
301292	海科新源	301292	创业板	SZSE	L	2025-06-17 15:57:24.280455
301293	三博脑科	301293	创业板	SZSE	L	2025-06-17 15:57:24.280455
301295	美硕科技	301295	创业板	SZSE	L	2025-06-17 15:57:24.280455
301296	新巨丰	301296	创业板	SZSE	L	2025-06-17 15:57:24.280455
301297	富乐德	301297	创业板	SZSE	L	2025-06-17 15:57:24.280455
301298	东利机械	301298	创业板	SZSE	L	2025-06-17 15:57:24.280455
301299	卓创资讯	301299	创业板	SZSE	L	2025-06-17 15:57:24.280455
301300	远翔新材	301300	创业板	SZSE	L	2025-06-17 15:57:24.280455
301301	川宁生物	301301	创业板	SZSE	L	2025-06-17 15:57:24.280455
301302	华如科技	301302	创业板	SZSE	L	2025-06-17 15:57:24.280455
301303	真兰仪表	301303	创业板	SZSE	L	2025-06-17 15:57:24.280455
301305	朗坤科技	301305	创业板	SZSE	L	2025-06-17 15:57:24.280455
301306	西测测试	301306	创业板	SZSE	L	2025-06-17 15:57:24.280455
301307	美利信	301307	创业板	SZSE	L	2025-06-17 15:57:24.280455
301308	江波龙	301308	创业板	SZSE	L	2025-06-17 15:57:24.280455
301309	万得凯	301309	创业板	SZSE	L	2025-06-17 15:57:24.280455
301310	鑫宏业	301310	创业板	SZSE	L	2025-06-17 15:57:24.280455
301311	昆船智能	301311	创业板	SZSE	L	2025-06-17 15:57:24.280455
301312	智立方	301312	创业板	SZSE	L	2025-06-17 15:57:24.280455
301313	凡拓数创	301313	创业板	SZSE	L	2025-06-17 15:57:24.280455
301314	科瑞思	301314	创业板	SZSE	L	2025-06-17 15:57:24.280455
301315	威士顿	301315	创业板	SZSE	L	2025-06-17 15:57:24.280455
301316	慧博云通	301316	创业板	SZSE	L	2025-06-17 15:57:24.280455
301317	鑫磊股份	301317	创业板	SZSE	L	2025-06-17 15:57:24.280455
301318	维海德	301318	创业板	SZSE	L	2025-06-17 15:57:24.280455
301319	唯特偶	301319	创业板	SZSE	L	2025-06-17 15:57:24.280455
301320	豪江智能	301320	创业板	SZSE	L	2025-06-17 15:57:24.280455
301321	翰博高新	301321	创业板	SZSE	L	2025-06-17 15:57:24.280455
301322	绿通科技	301322	创业板	SZSE	L	2025-06-17 15:57:24.280455
301323	新莱福	301323	创业板	SZSE	L	2025-06-17 15:57:24.280455
301325	曼恩斯特	301325	创业板	SZSE	L	2025-06-17 15:57:24.280455
301326	捷邦科技	301326	创业板	SZSE	L	2025-06-17 15:57:24.280455
301327	华宝新能	301327	创业板	SZSE	L	2025-06-17 15:57:24.280455
301328	维峰电子	301328	创业板	SZSE	L	2025-06-17 15:57:24.280455
301329	信音电子	301329	创业板	SZSE	L	2025-06-17 15:57:24.280455
301330	熵基科技	301330	创业板	SZSE	L	2025-06-17 15:57:24.280455
301331	恩威医药	301331	创业板	SZSE	L	2025-06-17 15:57:24.280455
301332	德尔玛	301332	创业板	SZSE	L	2025-06-17 15:57:24.280455
301333	诺思格	301333	创业板	SZSE	L	2025-06-17 15:57:24.280455
301335	天元宠物	301335	创业板	SZSE	L	2025-06-17 15:57:24.280455
301336	趣睡科技	301336	创业板	SZSE	L	2025-06-17 15:57:24.280455
301337	亚华电子	301337	创业板	SZSE	L	2025-06-17 15:57:24.280455
301338	凯格精机	301338	创业板	SZSE	L	2025-06-17 15:57:24.280455
301339	通行宝	301339	创业板	SZSE	L	2025-06-17 15:57:24.280455
301345	涛涛车业	301345	创业板	SZSE	L	2025-06-17 15:57:24.280455
301348	蓝箭电子	301348	创业板	SZSE	L	2025-06-17 15:57:24.280455
301349	信德新材	301349	创业板	SZSE	L	2025-06-17 15:57:24.280455
301353	普莱得	301353	创业板	SZSE	L	2025-06-17 15:57:24.280455
301355	南王科技	301355	创业板	SZSE	L	2025-06-17 15:57:24.280455
301356	天振股份	301356	创业板	SZSE	L	2025-06-17 15:57:24.280455
301357	北方长龙	301357	创业板	SZSE	L	2025-06-17 15:57:24.280455
301358	湖南裕能	301358	创业板	SZSE	L	2025-06-17 15:57:24.280455
301359	东南电子	301359	创业板	SZSE	L	2025-06-17 15:57:24.280455
301360	荣旗科技	301360	创业板	SZSE	L	2025-06-17 15:57:24.280455
301361	众智科技	301361	创业板	SZSE	L	2025-06-17 15:57:24.280455
301362	民爆光电	301362	创业板	SZSE	L	2025-06-17 15:57:24.280455
301363	美好医疗	301363	创业板	SZSE	L	2025-06-17 15:57:24.280455
301365	矩阵股份	301365	创业板	SZSE	L	2025-06-17 15:57:24.280455
301366	一博科技	301366	创业板	SZSE	L	2025-06-17 15:57:24.280455
301367	瑞迈特	301367	创业板	SZSE	L	2025-06-17 15:57:24.280455
301368	丰立智能	301368	创业板	SZSE	L	2025-06-17 15:57:24.280455
301369	联动科技	301369	创业板	SZSE	L	2025-06-17 15:57:24.280455
301370	国科恒泰	301370	创业板	SZSE	L	2025-06-17 15:57:24.280455
301371	敷尔佳	301371	创业板	SZSE	L	2025-06-17 15:57:24.280455
301372	科净源	301372	创业板	SZSE	L	2025-06-17 15:57:24.280455
301373	凌玮科技	301373	创业板	SZSE	L	2025-06-17 15:57:24.280455
301376	致欧科技	301376	创业板	SZSE	L	2025-06-17 15:57:24.280455
301377	鼎泰高科	301377	创业板	SZSE	L	2025-06-17 15:57:24.280455
301378	通达海	301378	创业板	SZSE	L	2025-06-17 15:57:24.280455
301379	天山电子	301379	创业板	SZSE	L	2025-06-17 15:57:24.280455
301380	挖金客	301380	创业板	SZSE	L	2025-06-17 15:57:24.280455
301381	赛维时代	301381	创业板	SZSE	L	2025-06-17 15:57:24.280455
301382	蜂助手	301382	创业板	SZSE	L	2025-06-17 15:57:24.280455
301383	天键股份	301383	创业板	SZSE	L	2025-06-17 15:57:24.280455
301386	未来电器	301386	创业板	SZSE	L	2025-06-17 15:57:24.280455
301387	光大同创	301387	创业板	SZSE	L	2025-06-17 15:57:24.280455
301388	欣灵电气	301388	创业板	SZSE	L	2025-06-17 15:57:24.280455
301389	隆扬电子	301389	创业板	SZSE	L	2025-06-17 15:57:24.280455
301390	经纬股份	301390	创业板	SZSE	L	2025-06-17 15:57:24.280455
301391	卡莱特	301391	创业板	SZSE	L	2025-06-17 15:57:24.280455
301392	汇成真空	301392	创业板	SZSE	L	2025-06-17 15:57:24.280455
301393	昊帆生物	301393	创业板	SZSE	L	2025-06-17 15:57:24.280455
301395	仁信新材	301395	创业板	SZSE	L	2025-06-17 15:57:24.280455
301396	宏景科技	301396	创业板	SZSE	L	2025-06-17 15:57:24.280455
301397	溯联股份	301397	创业板	SZSE	L	2025-06-17 15:57:24.280455
301398	星源卓镁	301398	创业板	SZSE	L	2025-06-17 15:57:24.280455
301399	英特科技	301399	创业板	SZSE	L	2025-06-17 15:57:24.280455
301408	华人健康	301408	创业板	SZSE	L	2025-06-17 15:57:24.280455
301413	安培龙	301413	创业板	SZSE	L	2025-06-17 15:57:24.280455
301418	协昌科技	301418	创业板	SZSE	L	2025-06-17 15:57:24.280455
301419	阿莱德	301419	创业板	SZSE	L	2025-06-17 15:57:24.280455
301421	波长光电	301421	创业板	SZSE	L	2025-06-17 15:57:24.280455
301428	世纪恒通	301428	创业板	SZSE	L	2025-06-17 15:57:24.280455
301429	森泰股份	301429	创业板	SZSE	L	2025-06-17 15:57:24.280455
301439	泓淋电力	301439	创业板	SZSE	L	2025-06-17 15:57:24.280455
301446	福事特	301446	创业板	SZSE	L	2025-06-17 15:57:24.280455
301448	开创电气	301448	创业板	SZSE	L	2025-06-17 15:57:24.280455
301456	盘古智能	301456	创业板	SZSE	L	2025-06-17 15:57:24.280455
301458	钧崴电子	301458	创业板	SZSE	L	2025-06-17 15:57:24.280455
301459	丰茂股份	301459	创业板	SZSE	L	2025-06-17 15:57:24.280455
301468	博盈特焊	301468	创业板	SZSE	L	2025-06-17 15:57:24.280455
301469	恒达新材	301469	创业板	SZSE	L	2025-06-17 15:57:24.280455
301479	弘景光电	301479	创业板	SZSE	L	2025-06-17 15:57:24.280455
301486	致尚科技	301486	创业板	SZSE	L	2025-06-17 15:57:24.280455
301487	盟固利	301487	创业板	SZSE	L	2025-06-17 15:57:24.280455
301488	豪恩汽电	301488	创业板	SZSE	L	2025-06-17 15:57:24.280455
301489	思泉新材	301489	创业板	SZSE	L	2025-06-17 15:57:24.280455
301498	乖宝宠物	301498	创业板	SZSE	L	2025-06-17 15:57:24.280455
301499	维科精密	301499	创业板	SZSE	L	2025-06-17 15:57:24.280455
301500	飞南资源	301500	创业板	SZSE	L	2025-06-17 15:57:24.280455
301501	恒鑫生活	301501	创业板	SZSE	L	2025-06-17 15:57:24.280455
301502	华阳智能	301502	创业板	SZSE	L	2025-06-17 15:57:24.280455
301503	智迪科技	301503	创业板	SZSE	L	2025-06-17 15:57:24.280455
301505	苏州规划	301505	创业板	SZSE	L	2025-06-17 15:57:24.280455
301507	民生健康	301507	创业板	SZSE	L	2025-06-17 15:57:24.280455
301508	中机认检	301508	创业板	SZSE	L	2025-06-17 15:57:24.280455
301509	金凯生科	301509	创业板	SZSE	L	2025-06-17 15:57:24.280455
301510	固高科技	301510	创业板	SZSE	L	2025-06-17 15:57:24.280455
301511	德福科技	301511	创业板	SZSE	L	2025-06-17 15:57:24.280455
301512	智信精密	301512	创业板	SZSE	L	2025-06-17 15:57:24.280455
301515	港通医疗	301515	创业板	SZSE	L	2025-06-17 15:57:24.280455
301516	中远通	301516	创业板	SZSE	L	2025-06-17 15:57:24.280455
301517	陕西华达	301517	创业板	SZSE	L	2025-06-17 15:57:24.280455
301518	长华化学	301518	创业板	SZSE	L	2025-06-17 15:57:24.280455
301519	舜禹股份	301519	创业板	SZSE	L	2025-06-17 15:57:24.280455
301520	万邦医药	301520	创业板	SZSE	L	2025-06-17 15:57:24.280455
301522	上大股份	301522	创业板	SZSE	L	2025-06-17 15:57:24.280455
301525	儒竞科技	301525	创业板	SZSE	L	2025-06-17 15:57:24.280455
301526	国际复材	301526	创业板	SZSE	L	2025-06-17 15:57:24.280455
301528	多浦乐	301528	创业板	SZSE	L	2025-06-17 15:57:24.280455
301529	福赛科技	301529	创业板	SZSE	L	2025-06-17 15:57:24.280455
301533	威马农机	301533	创业板	SZSE	L	2025-06-17 15:57:24.280455
301535	浙江华远	301535	创业板	SZSE	L	2025-06-17 15:57:24.280455
301536	星宸科技	301536	创业板	SZSE	L	2025-06-17 15:57:24.280455
301538	骏鼎达	301538	创业板	SZSE	L	2025-06-17 15:57:24.280455
301539	宏鑫科技	301539	创业板	SZSE	L	2025-06-17 15:57:24.280455
301548	崇德科技	301548	创业板	SZSE	L	2025-06-17 15:57:24.280455
301550	斯菱股份	301550	创业板	SZSE	L	2025-06-17 15:57:24.280455
301551	无线传媒	301551	创业板	SZSE	L	2025-06-17 15:57:24.280455
301552	科力装备	301552	创业板	SZSE	L	2025-06-17 15:57:24.280455
301555	惠柏新材	301555	创业板	SZSE	L	2025-06-17 15:57:24.280455
301556	托普云农	301556	创业板	SZSE	L	2025-06-17 15:57:24.280455
301557	常友科技	301557	创业板	SZSE	L	2025-06-17 15:57:24.280455
301558	三态股份	301558	创业板	SZSE	L	2025-06-17 15:57:24.280455
301559	中集环科	301559	创业板	SZSE	L	2025-06-17 15:57:24.280455
301560	众捷汽车	301560	创业板	SZSE	L	2025-06-17 15:57:24.280455
301565	中仑新材	301565	创业板	SZSE	L	2025-06-17 15:57:24.280455
301566	达利凯普	301566	创业板	SZSE	L	2025-06-17 15:57:24.280455
301567	贝隆精密	301567	创业板	SZSE	L	2025-06-17 15:57:24.280455
301568	思泰克	301568	创业板	SZSE	L	2025-06-17 15:57:24.280455
301571	国科天成	301571	创业板	SZSE	L	2025-06-17 15:57:24.280455
301577	美信科技	301577	创业板	SZSE	L	2025-06-17 15:57:24.280455
301578	辰奕智能	301578	创业板	SZSE	L	2025-06-17 15:57:24.280455
301580	爱迪特	301580	创业板	SZSE	L	2025-06-17 15:57:24.280455
301581	黄山谷捷	301581	创业板	SZSE	L	2025-06-17 15:57:24.280455
301585	蓝宇股份	301585	创业板	SZSE	L	2025-06-17 15:57:24.280455
301586	佳力奇	301586	创业板	SZSE	L	2025-06-17 15:57:24.280455
301587	中瑞股份	301587	创业板	SZSE	L	2025-06-17 15:57:24.280455
301588	美新科技	301588	创业板	SZSE	L	2025-06-17 15:57:24.280455
301589	诺瓦星云	301589	创业板	SZSE	L	2025-06-17 15:57:24.280455
301590	优优绿能	301590	创业板	SZSE	L	2025-06-17 15:57:24.280455
301591	肯特股份	301591	创业板	SZSE	L	2025-06-17 15:57:24.280455
301592	六九一二	301592	创业板	SZSE	L	2025-06-17 15:57:24.280455
301595	太力科技	301595	创业板	SZSE	L	2025-06-17 15:57:24.280455
301596	瑞迪智驱	301596	创业板	SZSE	L	2025-06-17 15:57:24.280455
301598	博科测试	301598	创业板	SZSE	L	2025-06-17 15:57:24.280455
301600	慧翰股份	301600	创业板	SZSE	L	2025-06-17 15:57:24.280455
301601	惠通科技	301601	创业板	SZSE	L	2025-06-17 15:57:24.280455
301602	超研股份	301602	创业板	SZSE	L	2025-06-17 15:57:24.280455
301603	乔锋智能	301603	创业板	SZSE	L	2025-06-17 15:57:24.280455
301606	绿联科技	301606	创业板	SZSE	L	2025-06-17 15:57:24.280455
301607	富特科技	301607	创业板	SZSE	L	2025-06-17 15:57:24.280455
301608	博实结	301608	创业板	SZSE	L	2025-06-17 15:57:24.280455
301611	珂玛科技	301611	创业板	SZSE	L	2025-06-17 15:57:24.280455
301613	新铝时代	301613	创业板	SZSE	L	2025-06-17 15:57:24.280455
301616	浙江华业	301616	创业板	SZSE	L	2025-06-17 15:57:24.280455
301617	博苑股份	301617	创业板	SZSE	L	2025-06-17 15:57:24.280455
301618	长联科技	301618	创业板	SZSE	L	2025-06-17 15:57:24.280455
301622	英思特	301622	创业板	SZSE	L	2025-06-17 15:57:24.280455
301626	苏州天脉	301626	创业板	SZSE	L	2025-06-17 15:57:24.280455
301628	强达电路	301628	创业板	SZSE	L	2025-06-17 15:57:24.280455
301629	矽电股份	301629	创业板	SZSE	L	2025-06-17 15:57:24.280455
301631	壹连科技	301631	创业板	SZSE	L	2025-06-17 15:57:24.280455
301633	港迪技术	301633	创业板	SZSE	L	2025-06-17 15:57:24.280455
301636	泽润新能	301636	创业板	SZSE	L	2025-06-17 15:57:24.280455
301658	首航新能	301658	创业板	SZSE	L	2025-06-17 15:57:24.280455
301662	宏工科技	301662	创业板	SZSE	L	2025-06-17 15:57:24.280455
301665	泰禾股份	301665	创业板	SZSE	L	2025-06-17 15:57:24.280455
302132	中航成飞	302132	创业板	SZSE	L	2025-06-17 15:57:24.280455
600000	浦发银行	600000	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600004	白云机场	600004	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600006	东风股份	600006	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600007	中国国贸	600007	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600008	首创环保	600008	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600009	上海机场	600009	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600010	包钢股份	600010	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600011	华能国际	600011	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600012	皖通高速	600012	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600015	华夏银行	600015	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600016	民生银行	600016	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600017	日照港	600017	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600018	上港集团	600018	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600019	宝钢股份	600019	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600020	中原高速	600020	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600021	上海电力	600021	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600022	山东钢铁	600022	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600023	浙能电力	600023	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600025	华能水电	600025	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600026	中远海能	600026	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600027	华电国际	600027	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600028	中国石化	600028	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600029	南方航空	600029	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600030	中信证券	600030	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600031	三一重工	600031	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600032	浙江新能	600032	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600033	福建高速	600033	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600035	楚天高速	600035	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600036	招商银行	600036	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600037	歌华有线	600037	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600038	中直股份	600038	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600039	四川路桥	600039	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600048	保利发展	600048	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600050	中国联通	600050	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600051	宁波联合	600051	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600052	东望时代	600052	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600053	九鼎投资	600053	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600054	黄山旅游	600054	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600055	万东医疗	600055	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600056	中国医药	600056	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600057	厦门象屿	600057	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600058	五矿发展	600058	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600059	古越龙山	600059	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600060	海信视像	600060	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600061	国投资本	600061	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600062	华润双鹤	600062	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600063	皖维高新	600063	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600064	南京高科	600064	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600066	宇通客车	600066	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600067	冠城新材	600067	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600071	凤凰光学	600071	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600072	中船科技	600072	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600073	光明肉业	600073	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600075	新疆天业	600075	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600076	康欣新材	600076	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600078	澄星股份	600078	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600079	人福医药	600079	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600080	金花股份	600080	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600081	东风科技	600081	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600082	海泰发展	600082	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600084	中信尼雅	600084	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600085	同仁堂	600085	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600088	中视传媒	600088	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600089	特变电工	600089	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600094	大名城	600094	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600095	湘财股份	600095	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600096	云天化	600096	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600097	开创国际	600097	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600098	广州发展	600098	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600099	林海股份	600099	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600100	同方股份	600100	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600101	明星电力	600101	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600103	青山纸业	600103	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600104	上汽集团	600104	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600105	永鼎股份	600105	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600106	重庆路桥	600106	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600108	亚盛集团	600108	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600109	国金证券	600109	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600110	诺德股份	600110	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600111	北方稀土	600111	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600113	浙江东日	600113	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600114	东睦股份	600114	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600115	中国东航	600115	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600116	三峡水利	600116	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600117	西宁特钢	600117	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600118	中国卫星	600118	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600119	长江投资	600119	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600120	浙江东方	600120	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600121	郑州煤电	600121	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600123	兰花科创	600123	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600125	铁龙物流	600125	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600126	杭钢股份	600126	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600127	金健米业	600127	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600128	苏豪弘业	600128	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600129	太极集团	600129	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600131	国网信通	600131	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600132	重庆啤酒	600132	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600133	东湖高新	600133	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600135	乐凯胶片	600135	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600137	浪莎股份	600137	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600138	中青旅	600138	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600141	兴发集团	600141	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600143	金发科技	600143	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600148	长春一东	600148	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600149	廊坊发展	600149	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600150	中国船舶	600150	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600151	航天机电	600151	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600152	维科技术	600152	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600153	建发股份	600153	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600155	华创云信	600155	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600156	华升股份	600156	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600157	永泰能源	600157	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600158	中体产业	600158	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600159	大龙地产	600159	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600160	巨化股份	600160	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600161	天坛生物	600161	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600162	香江控股	600162	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600163	中闽能源	600163	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600166	福田汽车	600166	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600167	联美控股	600167	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600168	XD武汉控	600168	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600169	太原重工	600169	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600170	上海建工	600170	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600171	上海贝岭	600171	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600172	黄河旋风	600172	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600173	卧龙新能	600173	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600176	中国巨石	600176	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600177	雅戈尔	600177	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600178	东安动力	600178	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600179	安通控股	600179	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600180	瑞茂通	600180	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600182	S佳通	600182	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600183	生益科技	600183	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600184	光电股份	600184	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600185	珠免集团	600185	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600186	莲花控股	600186	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600187	国中水务	600187	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600188	兖矿能源	600188	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600189	泉阳泉	600189	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600191	华资实业	600191	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600192	长城电工	600192	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600195	中牧股份	600195	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600196	复星医药	600196	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600197	伊力特	600197	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600198	大唐电信	600198	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600199	金种子酒	600199	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600201	生物股份	600201	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600202	哈空调	600202	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600203	福日电子	600203	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600206	有研新材	600206	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600207	安彩高科	600207	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600208	衢州发展	600208	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600210	紫江企业	600210	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600211	西藏药业	600211	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600212	绿能慧充	600212	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600215	派斯林	600215	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600216	浙江医药	600216	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600217	中再资环	600217	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600218	全柴动力	600218	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600219	南山铝业	600219	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600221	海航控股	600221	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600222	太龙药业	600222	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600223	福瑞达	600223	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600226	亨通股份	600226	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600227	赤天化	600227	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600229	城市传媒	600229	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600230	沧州大化	600230	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600231	凌钢股份	600231	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600232	XD金鹰股	600232	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600233	圆通速递	600233	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600234	科新发展	600234	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600235	民丰特纸	600235	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600236	桂冠电力	600236	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600237	铜峰电子	600237	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600239	云南城投	600239	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600241	时代万恒	600241	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600246	万通发展	600246	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600248	陕建股份	600248	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600249	两面针	600249	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600250	南京商旅	600250	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600251	冠农股份	600251	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600252	中恒集团	600252	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600255	鑫科材料	600255	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600256	广汇能源	600256	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600257	大湖股份	600257	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600258	首旅酒店	600258	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600259	广晟有色	600259	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600261	阳光照明	600261	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600262	北方股份	600262	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600266	城建发展	600266	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600267	海正药业	600267	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600268	国电南自	600268	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600269	赣粤高速	600269	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600271	航天信息	600271	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600272	开开实业	600272	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600273	嘉化能源	600273	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600276	恒瑞医药	600276	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600278	东方创业	600278	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600279	重庆港	600279	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600280	中央商场	600280	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600281	华阳新材	600281	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600282	南钢股份	600282	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600283	钱江水利	600283	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600284	浦东建设	600284	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600285	羚锐制药	600285	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600288	大恒科技	600288	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600292	远达环保	600292	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600293	三峡新材	600293	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600295	鄂尔多斯	600295	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600298	安琪酵母	600298	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600299	安迪苏	600299	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600300	维维股份	600300	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600301	华锡有色	600301	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600302	标准股份	600302	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600303	曙光股份	600303	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600305	恒顺醋业	600305	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600307	酒钢宏兴	600307	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600308	华泰股份	600308	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600309	万华化学	600309	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600310	广西能源	600310	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600312	平高电气	600312	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600313	农发种业	600313	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600315	上海家化	600315	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600316	洪都航空	600316	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600318	新力金融	600318	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600319	亚星化学	600319	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600320	振华重工	600320	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600322	津投城开	600322	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600323	瀚蓝环境	600323	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600325	华发股份	600325	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600326	西藏天路	600326	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600327	大东方	600327	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600328	中盐化工	600328	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600329	达仁堂	600329	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600330	天通股份	600330	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600331	宏达股份	600331	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600332	白云山	600332	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600333	长春燃气	600333	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600335	国机汽车	600335	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600336	澳柯玛	600336	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600337	美克家居	600337	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600338	西藏珠峰	600338	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600339	中油工程	600339	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600340	华夏幸福	600340	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600343	航天动力	600343	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600345	长江通信	600345	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600346	恒力石化	600346	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600348	华阳股份	600348	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600350	山东高速	600350	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600351	亚宝药业	600351	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600352	浙江龙盛	600352	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600353	旭光电子	600353	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600354	敦煌种业	600354	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600356	恒丰纸业	600356	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600359	新农开发	600359	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600361	创新新材	600361	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600362	江西铜业	600362	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600363	联创光电	600363	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600366	宁波韵升	600366	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600367	红星发展	600367	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600368	五洲交通	600368	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600369	西南证券	600369	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600370	三房巷	600370	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600371	万向德农	600371	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600372	中航机载	600372	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600373	中文传媒	600373	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600375	汉马科技	600375	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600376	首开股份	600376	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600377	宁沪高速	600377	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600378	昊华科技	600378	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600379	宝光股份	600379	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600380	健康元	600380	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600382	广东明珠	600382	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600383	金地集团	600383	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600386	北巴传媒	600386	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600388	龙净环保	600388	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600389	江山股份	600389	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600390	五矿资本	600390	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600391	航发科技	600391	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600392	盛和资源	600392	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600395	盘江股份	600395	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600396	华电辽能	600396	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600397	安源煤业	600397	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600398	海澜之家	600398	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600399	抚顺特钢	600399	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600400	红豆股份	600400	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600403	大有能源	600403	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600405	动力源	600405	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600406	国电南瑞	600406	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600408	安泰集团	600408	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600409	三友化工	600409	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600410	华胜天成	600410	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600415	小商品城	600415	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600416	湘电股份	600416	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600418	江淮汽车	600418	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600419	天润乳业	600419	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600420	国药现代	600420	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600422	昆药集团	600422	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600423	柳化股份	600423	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600425	青松建化	600425	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600426	华鲁恒升	600426	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600428	中远海特	600428	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600429	三元股份	600429	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600433	冠豪高新	600433	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600435	北方导航	600435	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600436	片仔癀	600436	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600438	通威股份	600438	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600439	瑞贝卡	600439	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600444	国机通用	600444	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600446	金证股份	600446	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600448	华纺股份	600448	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600449	宁夏建材	600449	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600452	涪陵电力	600452	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600455	博通股份	600455	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600456	宝钛股份	600456	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600458	时代新材	600458	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600459	贵研铂业	600459	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600460	士兰微	600460	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600461	洪城环境	600461	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600463	空港股份	600463	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600467	好当家	600467	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600468	百利电气	600468	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600469	风神股份	600469	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600470	六国化工	600470	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600475	华光环能	600475	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600476	湘邮科技	600476	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600477	杭萧钢构	600477	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600478	科力远	600478	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600479	千金药业	600479	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600480	凌云股份	600480	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600481	双良节能	600481	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600482	中国动力	600482	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600483	福能股份	600483	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600486	扬农化工	600486	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600487	亨通光电	600487	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600488	津药药业	600488	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600489	中金黄金	600489	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600490	鹏欣资源	600490	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600491	龙元建设	600491	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600493	凤竹纺织	600493	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600495	晋西车轴	600495	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600496	精工钢构	600496	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600497	驰宏锌锗	600497	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600498	烽火通信	600498	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600499	科达制造	600499	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600500	中化国际	600500	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600501	航天晨光	600501	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600502	安徽建工	600502	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600503	华丽家族	600503	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600505	西昌电力	600505	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600506	统一股份	600506	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600507	方大特钢	600507	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600508	上海能源	600508	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600509	天富能源	600509	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600510	黑牡丹	600510	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600511	国药股份	600511	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600512	腾达建设	600512	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600513	联环药业	600513	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600515	海南机场	600515	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600516	方大炭素	600516	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600517	国网英大	600517	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600518	康美药业	600518	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600519	贵州茅台	600519	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600520	三佳科技	600520	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600521	华海药业	600521	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600522	中天科技	600522	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600523	XD贵航股	600523	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600526	菲达环保	600526	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600527	江南高纤	600527	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600528	中铁工业	600528	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600529	山东药玻	600529	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600530	交大昂立	600530	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600531	豫光金铅	600531	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600533	栖霞建设	600533	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600535	天士力	600535	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600536	中国软件	600536	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600537	亿晶光电	600537	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600538	国发股份	600538	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600539	狮头股份	600539	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600540	新赛股份	600540	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600543	莫高股份	600543	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600545	卓郎智能	600545	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600546	山煤国际	600546	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600547	山东黄金	600547	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600548	深高速	600548	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600549	厦门钨业	600549	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600550	保变电气	600550	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600551	时代出版	600551	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600552	凯盛科技	600552	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600556	天下秀	600556	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600557	康缘药业	600557	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600558	大西洋	600558	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600559	老白干酒	600559	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600560	金自天正	600560	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600561	江西长运	600561	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600562	国睿科技	600562	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600563	法拉电子	600563	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600566	济川药业	600566	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600567	山鹰国际	600567	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600569	安阳钢铁	600569	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600570	恒生电子	600570	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600571	信雅达	600571	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600572	康恩贝	600572	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600573	惠泉啤酒	600573	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600575	淮河能源	600575	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600576	祥源文旅	600576	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600577	精达股份	600577	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600578	京能电力	600578	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600579	中化装备	600579	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600580	卧龙电驱	600580	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600581	八一钢铁	600581	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600582	天地科技	600582	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600583	海油工程	600583	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600584	长电科技	600584	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600585	海螺水泥	600585	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600586	金晶科技	600586	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600587	新华医疗	600587	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600588	用友网络	600588	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600589	大位科技	600589	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600590	泰豪科技	600590	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600592	龙溪股份	600592	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600593	大连圣亚	600593	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600594	益佰制药	600594	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600595	中孚实业	600595	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600596	新安股份	600596	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600597	光明乳业	600597	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600598	北大荒	600598	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600600	青岛啤酒	600600	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600601	方正科技	600601	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600602	云赛智联	600602	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600604	市北高新	600604	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600605	汇通能源	600605	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600606	绿地控股	600606	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600609	金杯汽车	600609	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600610	中毅达	600610	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600611	大众交通	600611	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600612	老凤祥	600612	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600613	神奇制药	600613	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600615	丰华股份	600615	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600616	金枫酒业	600616	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600617	国新能源	600617	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600618	氯碱化工	600618	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600619	海立股份	600619	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600620	天宸股份	600620	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600621	华鑫股份	600621	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600622	光大嘉宝	600622	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600623	华谊集团	600623	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600624	复旦复华	600624	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600626	申达股份	600626	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600628	新世界	600628	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600629	华建集团	600629	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600630	龙头股份	600630	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600633	浙数文化	600633	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600635	大众公用	600635	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600637	东方明珠	600637	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600638	新黄浦	600638	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600639	浦东金桥	600639	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600640	国脉文化	600640	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600641	万业企业	600641	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600642	申能股份	600642	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600643	爱建集团	600643	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600644	乐山电力	600644	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600645	中源协和	600645	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600648	外高桥	600648	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600649	城投控股	600649	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600650	锦江在线	600650	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600651	飞乐音响	600651	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600653	申华控股	600653	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600654	中安科	600654	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600655	豫园股份	600655	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600657	信达地产	600657	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600658	电子城	600658	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600660	福耀玻璃	600660	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600661	昂立教育	600661	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600662	外服控股	600662	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600663	陆家嘴	600663	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600664	哈药股份	600664	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600665	天地源	600665	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600666	奥瑞德	600666	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600667	太极实业	600667	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600668	尖峰集团	600668	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600671	天目药业	600671	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600673	东阳光	600673	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600674	川投能源	600674	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600675	中华企业	600675	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600676	交运股份	600676	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600678	四川金顶	600678	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600679	上海凤凰	600679	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600681	百川能源	600681	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600682	南京新百	600682	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600683	京投发展	600683	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600684	珠江股份	600684	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600685	中船防务	600685	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600686	金龙汽车	600686	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600688	上海石化	600688	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600689	上海三毛	600689	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600690	海尔智家	600690	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600691	阳煤化工	600691	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600692	亚通股份	600692	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600693	东百集团	600693	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600694	大商股份	600694	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600697	欧亚集团	600697	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600698	湖南天雁	600698	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600699	均胜电子	600699	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600702	舍得酒业	600702	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600703	三安光电	600703	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600704	物产中大	600704	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600706	曲江文旅	600706	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600707	彩虹股份	600707	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600708	光明地产	600708	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600710	苏美达	600710	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600712	南宁百货	600712	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600713	南京医药	600713	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600714	金瑞矿业	600714	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600715	文投控股	600715	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600716	凤凰股份	600716	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600717	天津港	600717	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600718	东软集团	600718	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600719	大连热电	600719	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600720	中交设计	600720	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600721	百花医药	600721	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600722	金牛化工	600722	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600724	宁波富达	600724	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600725	云维股份	600725	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600726	华电能源	600726	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600727	鲁北化工	600727	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600728	佳都科技	600728	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600729	重庆百货	600729	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600730	中国高科	600730	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600731	湖南海利	600731	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600732	爱旭股份	600732	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600733	北汽蓝谷	600733	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600734	实达集团	600734	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600735	新华锦	600735	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600736	苏州高新	600736	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600737	中粮糖业	600737	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600738	丽尚国潮	600738	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600739	辽宁成大	600739	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600740	山西焦化	600740	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600741	华域汽车	600741	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600742	富维股份	600742	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600743	华远控股	600743	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600744	华银电力	600744	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600745	闻泰科技	600745	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600746	江苏索普	600746	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600748	上实发展	600748	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600749	西藏旅游	600749	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600750	江中药业	600750	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600751	海航科技	600751	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600754	锦江酒店	600754	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600755	厦门国贸	600755	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600756	浪潮软件	600756	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600757	长江传媒	600757	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600758	辽宁能源	600758	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600759	洲际油气	600759	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600760	中航沈飞	600760	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600761	安徽合力	600761	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600763	通策医疗	600763	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600764	中国海防	600764	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600765	中航重机	600765	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600768	宁波富邦	600768	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600769	祥龙电业	600769	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600770	综艺股份	600770	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600771	广誉远	600771	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600773	西藏城投	600773	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600774	汉商集团	600774	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600775	南京熊猫	600775	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600776	东方通信	600776	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600778	友好集团	600778	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600779	水井坊	600779	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600780	通宝能源	600780	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600782	新钢股份	600782	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600783	鲁信创投	600783	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600784	鲁银投资	600784	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600785	新华百货	600785	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600787	中储股份	600787	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600789	鲁抗医药	600789	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600790	轻纺城	600790	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600791	京能置业	600791	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600792	云煤能源	600792	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600793	宜宾纸业	600793	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600794	保税科技	600794	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600795	国电电力	600795	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600796	钱江生化	600796	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600797	浙大网新	600797	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600798	宁波海运	600798	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600800	渤海化学	600800	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600801	华新水泥	600801	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600802	福建水泥	600802	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600803	新奥股份	600803	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600805	悦达投资	600805	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600807	济高发展	600807	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600808	马钢股份	600808	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600809	山西汾酒	600809	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600810	神马股份	600810	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600812	华北制药	600812	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600814	杭州解百	600814	上交所主板	SSE	L	2025-06-17 15:57:24.280455
688173	希荻微	688173	科创板	SSE	L	2025-06-17 15:57:24.280455
600815	厦工股份	600815	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600816	建元信托	600816	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600817	宇通重工	600817	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600818	中路股份	600818	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600819	耀皮玻璃	600819	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600820	隧道股份	600820	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600821	金开新能	600821	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600822	上海物贸	600822	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600824	益民集团	600824	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600825	新华传媒	600825	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600826	兰生股份	600826	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600827	百联股份	600827	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600828	茂业商业	600828	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600829	人民同泰	600829	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600830	香溢融通	600830	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600833	第一医药	600833	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600834	申通地铁	600834	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600835	上海机电	600835	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600838	上海九百	600838	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600839	四川长虹	600839	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600841	动力新科	600841	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600843	上工申贝	600843	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600844	丹化科技	600844	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600845	宝信软件	600845	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600846	同济科技	600846	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600847	万里股份	600847	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600848	上海临港	600848	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600850	电科数字	600850	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600851	海欣股份	600851	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600853	龙建股份	600853	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600854	春兰股份	600854	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600855	航天长峰	600855	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600857	宁波中百	600857	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600858	银座股份	600858	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600859	王府井	600859	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600860	京城股份	600860	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600861	北京人力	600861	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600862	中航高科	600862	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600863	内蒙华电	600863	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600864	哈投股份	600864	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600865	百大集团	600865	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600866	星湖科技	600866	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600867	通化东宝	600867	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600868	梅雁吉祥	600868	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600869	远东股份	600869	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600871	石化油服	600871	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600872	中炬高新	600872	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600873	梅花生物	600873	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600874	创业环保	600874	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600875	东方电气	600875	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600876	凯盛新能	600876	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600877	电科芯片	600877	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600879	航天电子	600879	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600880	博瑞传播	600880	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600881	亚泰集团	600881	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600882	妙可蓝多	600882	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600883	博闻科技	600883	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600884	杉杉股份	600884	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600885	宏发股份	600885	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600886	国投电力	600886	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600887	伊利股份	600887	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600888	新疆众和	600888	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600889	南京化纤	600889	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600893	航发动力	600893	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600894	广日股份	600894	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600895	张江高科	600895	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600897	厦门空港	600897	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600900	长江电力	600900	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600901	江苏金租	600901	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600903	XD贵州燃	600903	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600905	三峡能源	600905	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600906	财达证券	600906	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600908	无锡银行	600908	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600909	华安证券	600909	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600916	中国黄金	600916	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600917	重庆燃气	600917	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600918	中泰证券	600918	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600919	江苏银行	600919	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600925	苏能股份	600925	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600926	杭州银行	600926	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600927	永安期货	600927	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600928	西安银行	600928	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600929	雪天盐业	600929	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600933	爱柯迪	600933	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600935	华塑股份	600935	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600936	广西广电	600936	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600938	中国海油	600938	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600939	重庆建工	600939	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600941	中国移动	600941	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600955	维远股份	600955	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600956	新天绿能	600956	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600958	东方证券	600958	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600959	江苏有线	600959	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600960	渤海汽车	600960	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600961	株冶集团	600961	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600962	国投中鲁	600962	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600963	岳阳林纸	600963	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600965	福成股份	600965	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600966	博汇纸业	600966	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600967	内蒙一机	600967	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600968	海油发展	600968	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600969	郴电国际	600969	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600970	中材国际	600970	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600971	恒源煤电	600971	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600973	宝胜股份	600973	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600975	新五丰	600975	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600976	健民集团	600976	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600977	中国电影	600977	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600979	广安爱众	600979	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600980	北矿科技	600980	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600981	汇鸿集团	600981	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600982	宁波能源	600982	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600983	惠而浦	600983	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600984	建设机械	600984	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600985	淮北矿业	600985	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600986	浙文互联	600986	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600987	航民股份	600987	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600988	赤峰黄金	600988	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600989	宝丰能源	600989	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600990	四创电子	600990	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600992	贵绳股份	600992	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600993	马应龙	600993	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600995	南网储能	600995	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600996	贵广网络	600996	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600997	开滦股份	600997	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600998	九州通	600998	上交所主板	SSE	L	2025-06-17 15:57:24.280455
600999	招商证券	600999	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601000	唐山港	601000	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601001	晋控煤业	601001	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601002	晋亿实业	601002	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601003	柳钢股份	601003	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601005	重庆钢铁	601005	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601006	大秦铁路	601006	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601007	金陵饭店	601007	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601008	连云港	601008	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601009	南京银行	601009	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601010	文峰股份	601010	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601011	宝泰隆	601011	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601012	隆基绿能	601012	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601015	陕西黑猫	601015	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601016	节能风电	601016	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601018	宁波港	601018	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601019	山东出版	601019	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601020	华钰矿业	601020	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601021	春秋航空	601021	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601022	宁波远洋	601022	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601033	永兴股份	601033	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601038	一拖股份	601038	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601058	赛轮轮胎	601058	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601059	信达证券	601059	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601061	中信金属	601061	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601065	江盐集团	601065	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601066	中信建投	601066	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601068	中铝国际	601068	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601069	西部黄金	601069	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601077	渝农商行	601077	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601083	锦江航运	601083	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601086	国芳集团	601086	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601088	中国神华	601088	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601089	福元医药	601089	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601096	宏盛华源	601096	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601098	中南传媒	601098	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601099	太平洋	601099	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601100	恒立液压	601100	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601101	昊华能源	601101	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601106	中国一重	601106	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601107	四川成渝	601107	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601108	财通证券	601108	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601111	中国国航	601111	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601113	华鼎股份	601113	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601116	三江购物	601116	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601117	中国化学	601117	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601118	海南橡胶	601118	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601121	宝地矿业	601121	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601126	四方股份	601126	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601127	赛力斯	601127	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601128	常熟银行	601128	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601133	柏诚股份	601133	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601136	首创证券	601136	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601137	博威合金	601137	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601138	工业富联	601138	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601139	深圳燃气	601139	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601155	新城控股	601155	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601156	东航物流	601156	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601158	重庆水务	601158	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601162	天风证券	601162	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601163	三角轮胎	601163	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601166	兴业银行	601166	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601168	西部矿业	601168	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601169	北京银行	601169	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601177	杭齿前进	601177	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601179	中国西电	601179	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601186	中国铁建	601186	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601187	厦门银行	601187	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601188	龙江交通	601188	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601198	东兴证券	601198	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601199	江南水务	601199	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601200	上海环境	601200	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601208	东材科技	601208	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601211	国泰海通	601211	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601212	白银有色	601212	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601216	君正集团	601216	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601218	吉鑫科技	601218	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601222	林洋能源	601222	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601225	陕西煤业	601225	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601226	华电科工	601226	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601228	广州港	601228	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601229	上海银行	601229	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601231	环旭电子	601231	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601233	桐昆股份	601233	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601236	红塔证券	601236	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601238	广汽集团	601238	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601279	英利汽车	601279	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601288	农业银行	601288	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601298	青岛港	601298	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601311	骆驼股份	601311	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601318	中国平安	601318	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601319	中国人保	601319	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601326	秦港股份	601326	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601328	交通银行	601328	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601330	绿色动力	601330	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601333	广深铁路	601333	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601336	新华保险	601336	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601339	百隆东方	601339	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601360	三六零	601360	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601366	利群股份	601366	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601368	绿城水务	601368	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601369	陕鼓动力	601369	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601375	中原证券	601375	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601377	兴业证券	601377	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601388	怡球资源	601388	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601390	中国中铁	601390	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601398	工商银行	601398	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601399	国机重装	601399	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601456	国联民生	601456	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601500	通用股份	601500	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601512	中新集团	601512	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601515	东峰集团	601515	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601518	吉林高速	601518	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601519	大智慧	601519	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601528	瑞丰银行	601528	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601555	东吴证券	601555	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601566	九牧王	601566	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601567	三星医疗	601567	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601568	北元集团	601568	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601577	长沙银行	601577	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601579	会稽山	601579	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601588	北辰实业	601588	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601595	上海电影	601595	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601598	中国外运	601598	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601599	浙文影业	601599	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601600	中国铝业	601600	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601601	中国太保	601601	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601606	长城军工	601606	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601607	上海医药	601607	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601608	中信重工	601608	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601609	金田股份	601609	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601611	中国核建	601611	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601615	明阳智能	601615	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601616	广电电气	601616	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601618	中国中冶	601618	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601619	嘉泽新能	601619	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601628	中国人寿	601628	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601633	长城汽车	601633	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601636	旗滨集团	601636	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601658	邮储银行	601658	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601665	齐鲁银行	601665	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601666	平煤股份	601666	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601668	中国建筑	601668	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601669	中国电建	601669	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601677	明泰铝业	601677	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601678	滨化股份	601678	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601686	友发集团	601686	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601688	华泰证券	601688	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601689	拓普集团	601689	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601696	中银证券	601696	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601698	中国卫通	601698	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601699	潞安环能	601699	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601700	风范股份	601700	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601702	华峰铝业	601702	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601717	郑煤机	601717	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601718	际华集团	601718	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601727	上海电气	601727	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601728	中国电信	601728	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601766	中国中车	601766	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601777	千里科技	601777	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601778	晶科科技	601778	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601788	光大证券	601788	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601789	宁波建工	601789	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601798	蓝科高新	601798	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601799	星宇股份	601799	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601800	中国交建	601800	上交所主板	SSE	L	2025-06-17 15:57:24.280455
688612	威迈斯	688612	科创板	SSE	L	2025-06-17 15:57:24.280455
601801	皖新传媒	601801	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601808	中海油服	601808	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601811	新华文轩	601811	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601816	京沪高铁	601816	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601818	光大银行	601818	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601825	沪农商行	601825	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601827	三峰环境	601827	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601828	美凯龙	601828	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601838	成都银行	601838	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601857	中国石油	601857	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601858	中国科传	601858	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601860	紫金银行	601860	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601865	福莱特	601865	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601866	中远海发	601866	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601868	中国能建	601868	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601869	长飞光纤	601869	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601872	招商轮船	601872	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601877	正泰电器	601877	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601878	浙商证券	601878	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601880	辽港股份	601880	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601881	中国银河	601881	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601882	海天精工	601882	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601886	江河集团	601886	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601888	中国中免	601888	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601890	亚星锚链	601890	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601898	中煤能源	601898	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601899	紫金矿业	601899	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601900	南方传媒	601900	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601901	方正证券	601901	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601908	京运通	601908	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601916	浙商银行	601916	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601918	新集能源	601918	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601919	中远海控	601919	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601921	浙版传媒	601921	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601928	凤凰传媒	601928	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601929	吉视传媒	601929	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601933	永辉超市	601933	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601939	建设银行	601939	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601949	中国出版	601949	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601952	苏垦农发	601952	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601956	东贝集团	601956	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601958	金钼股份	601958	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601963	重庆银行	601963	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601965	中国汽研	601965	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601966	玲珑轮胎	601966	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601968	宝钢包装	601968	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601969	海南矿业	601969	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601975	招商南油	601975	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601985	中国核电	601985	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601988	中国银行	601988	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601989	中国重工	601989	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601990	南京证券	601990	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601991	大唐发电	601991	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601992	金隅集团	601992	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601995	中金公司	601995	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601996	丰林集团	601996	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601997	贵阳银行	601997	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601998	中信银行	601998	上交所主板	SSE	L	2025-06-17 15:57:24.280455
601999	出版传媒	601999	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603000	人民网	603000	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603001	奥康国际	603001	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603002	宏昌电子	603002	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603004	鼎龙科技	603004	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603005	晶方科技	603005	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603006	联明股份	603006	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603008	喜临门	603008	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603009	北特科技	603009	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603010	万盛股份	603010	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603011	合锻智能	603011	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603012	创力集团	603012	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603013	亚普股份	603013	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603014	威高血净	603014	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603015	弘讯科技	603015	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603016	新宏泰	603016	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603017	中衡设计	603017	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603018	华设集团	603018	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603019	中科曙光	603019	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603020	爱普股份	603020	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603022	新通联	603022	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603023	威帝股份	603023	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603025	大豪科技	603025	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603026	石大胜华	603026	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603027	千禾味业	603027	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603028	赛福天	603028	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603029	天鹅股份	603029	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603030	全筑股份	603030	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603031	安孚科技	603031	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603032	德新科技	603032	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603033	三维股份	603033	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603035	常熟汽饰	603035	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603036	如通股份	603036	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603037	凯众股份	603037	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603038	华立股份	603038	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603039	泛微网络	603039	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603040	新坐标	603040	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603041	美思德	603041	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603042	华脉科技	603042	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603043	广州酒家	603043	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603045	福达合金	603045	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603048	浙江黎明	603048	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603049	中策橡胶	603049	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603050	科林电气	603050	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603051	鹿山新材	603051	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603052	可川科技	603052	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603053	成都燃气	603053	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603055	台华新材	603055	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603056	德邦股份	603056	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603057	紫燕食品	603057	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603058	永吉股份	603058	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603059	倍加洁	603059	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603060	国检集团	603060	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603061	金海通	603061	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603062	麦加芯彩	603062	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603063	禾望电气	603063	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603065	宿迁联盛	603065	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603066	音飞储存	603066	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603067	振华股份	603067	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603068	博通集成	603068	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603069	海汽集团	603069	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603070	万控智造	603070	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603071	物产环能	603071	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603072	天和磁材	603072	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603073	彩蝶实业	603073	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603075	热威股份	603075	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603076	乐惠国际	603076	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603077	和邦生物	603077	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603078	江化微	603078	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603079	圣达生物	603079	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603080	新疆火炬	603080	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603081	大丰实业	603081	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603082	北自科技	603082	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603083	剑桥科技	603083	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603085	天成自控	603085	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603086	先达股份	603086	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603087	甘李药业	603087	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603088	宁波精达	603088	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603089	正裕工业	603089	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603090	宏盛股份	603090	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603091	众鑫股份	603091	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603093	南华期货	603093	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603095	越剑智能	603095	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603096	新经典	603096	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603097	江苏华辰	603097	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603098	森特股份	603098	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603099	长白山	603099	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603100	川仪股份	603100	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603101	汇嘉时代	603101	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603102	百合股份	603102	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603103	横店影视	603103	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603105	芯能科技	603105	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603106	恒银科技	603106	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603107	上海汽配	603107	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603108	润达医疗	603108	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603109	神驰机电	603109	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603110	东方材料	603110	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603111	康尼机电	603111	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603112	华翔股份	603112	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603113	金能科技	603113	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603115	海星股份	603115	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603116	红蜻蜓	603116	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603117	万林物流	603117	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603118	共进股份	603118	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603119	浙江荣泰	603119	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603120	肯特催化	603120	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603121	华培动力	603121	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603122	合富中国	603122	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603123	翠微股份	603123	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603124	江南新材	603124	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603125	常青科技	603125	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603126	中材节能	603126	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603127	昭衍新药	603127	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603128	华贸物流	603128	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603129	春风动力	603129	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603130	云中马	603130	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603131	上海沪工	603131	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603132	金徽股份	603132	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603135	中重科技	603135	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603136	天目湖	603136	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603137	恒尚节能	603137	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603138	海量数据	603138	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603139	康惠制药	603139	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603150	万朗磁塑	603150	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603151	邦基科技	603151	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603153	上海建科	603153	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603155	新亚强	603155	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603156	养元饮品	603156	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603158	腾龙股份	603158	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603159	上海亚虹	603159	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603160	汇顶科技	603160	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603161	科华控股	603161	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603162	海通发展	603162	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603163	圣晖集成	603163	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603165	荣晟环保	603165	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603166	福达股份	603166	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603167	渤海轮渡	603167	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603168	莎普爱思	603168	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603169	兰石重装	603169	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603170	宝立食品	603170	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603171	税友股份	603171	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603172	XD万丰股	603172	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603173	福斯达	603173	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603176	汇通集团	603176	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603177	德创环保	603177	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603178	圣龙股份	603178	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603179	新泉股份	603179	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603180	金牌家居	603180	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603181	皇马科技	603181	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603182	嘉华股份	603182	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603183	建研院	603183	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603185	弘元绿能	603185	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603186	华正新材	603186	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603187	海容冷链	603187	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603188	亚邦股份	603188	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603189	网达软件	603189	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603190	亚通精工	603190	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603191	望变电气	603191	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603192	汇得科技	603192	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603193	润本股份	603193	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603194	XD中力股	603194	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603195	公牛集团	603195	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603196	日播时尚	603196	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603197	保隆科技	603197	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603198	迎驾贡酒	603198	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603199	九华旅游	603199	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603200	上海洗霸	603200	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603201	常润股份	603201	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603202	天有为	603202	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603203	快克智能	603203	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603205	健尔康	603205	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603206	嘉环科技	603206	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603207	小方制药	603207	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603208	江山欧派	603208	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603209	兴通股份	603209	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603210	泰鸿万立	603210	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603211	晋拓股份	603211	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603212	赛伍技术	603212	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603213	镇洋发展	603213	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603214	爱婴室	603214	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603215	比依股份	603215	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603216	梦天家居	603216	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603217	元利科技	603217	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603218	日月股份	603218	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603219	富佳股份	603219	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603220	中贝通信	603220	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603221	爱丽家居	603221	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603222	济民健康	603222	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603223	恒通股份	603223	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603225	新凤鸣	603225	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603226	菲林格尔	603226	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603227	雪峰科技	603227	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603228	景旺电子	603228	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603229	奥翔药业	603229	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603230	内蒙新华	603230	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603231	索宝蛋白	603231	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603232	格尔软件	603232	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603233	大参林	603233	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603235	天新药业	603235	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603236	移远通信	603236	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603237	五芳斋	603237	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603238	诺邦股份	603238	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603239	浙江仙通	603239	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603255	鼎际得	603255	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603256	宏和科技	603256	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603257	中国瑞林	603257	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603258	电魂网络	603258	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603259	药明康德	603259	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603260	合盛硅业	603260	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603266	天龙股份	603266	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603267	鸿远电子	603267	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603269	海鸥股份	603269	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603270	金帝股份	603270	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603271	永杰新材	603271	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603272	联翔股份	603272	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603273	天元智能	603273	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603275	众辰科技	603275	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603276	恒兴新材	603276	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603277	银都股份	603277	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603278	大业股份	603278	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603279	景津装备	603279	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603280	南方路机	603280	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603281	江瀚新材	603281	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603282	亚光股份	603282	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603283	赛腾股份	603283	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603285	键邦股份	603285	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603286	日盈电子	603286	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603288	海天味业	603288	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603289	泰瑞机器	603289	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603290	斯达半导	603290	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603291	联合水务	603291	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603296	华勤技术	603296	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603297	XD永新光	603297	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603298	杭叉集团	603298	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603299	苏盐井神	603299	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603300	海南华铁	603300	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603301	振德医疗	603301	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603303	得邦照明	603303	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603305	旭升集团	603305	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603306	华懋科技	603306	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603307	扬州金泉	603307	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603308	应流股份	603308	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603309	维力医疗	603309	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603310	巍华新材	603310	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603311	金海高科	603311	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603312	西典新能	603312	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603313	梦百合	603313	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603315	福鞍股份	603315	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603316	诚邦股份	603316	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603317	天味食品	603317	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603318	水发燃气	603318	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603319	美湖股份	603319	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603320	迪贝电气	603320	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603321	梅轮电梯	603321	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603322	超讯通信	603322	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603323	苏农银行	603323	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603324	盛剑科技	603324	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603325	博隆技术	603325	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603326	我乐家居	603326	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603327	福蓉科技	603327	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603328	依顿电子	603328	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603329	上海雅仕	603329	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603330	天洋新材	603330	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603331	百达精工	603331	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603332	苏州龙杰	603332	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603333	尚纬股份	603333	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603335	迪生力	603335	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603336	宏辉果蔬	603336	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603337	杰克股份	603337	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603338	浙江鼎力	603338	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603339	四方科技	603339	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603341	龙旗科技	603341	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603344	星德胜	603344	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603345	安井食品	603345	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603348	文灿股份	603348	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603350	安乃达	603350	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603351	威尔药业	603351	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603353	和顺石油	603353	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603355	莱克电气	603355	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603356	华菱精工	603356	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603357	设计总院	603357	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603358	华达科技	603358	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603359	东珠生态	603359	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603360	百傲化学	603360	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603363	傲农生物	603363	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603365	水星家纺	603365	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603366	日出东方	603366	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603367	辰欣药业	603367	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603368	柳药集团	603368	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603369	今世缘	603369	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603373	XD安邦护	603373	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603375	盛景微	603375	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603378	亚士创能	603378	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603379	三美股份	603379	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603380	易德龙	603380	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603381	永臻股份	603381	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603382	C海阳	603382	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603383	顶点软件	603383	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603385	惠达卫浴	603385	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603386	骏亚科技	603386	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603387	基蛋生物	603387	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603390	通达电气	603390	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603391	力聚热能	603391	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603392	万泰生物	603392	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603393	新天然气	603393	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603395	红四方	603395	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603396	金辰股份	603396	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603399	永杉锂业	603399	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603408	建霖家居	603408	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603409	汇通控股	603409	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603416	信捷电气	603416	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603421	鼎信通讯	603421	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603429	集友股份	603429	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603439	贵州三力	603439	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603444	吉比特	603444	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603456	九洲药业	603456	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603458	勘设股份	603458	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603466	风语筑	603466	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603477	巨星农牧	603477	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603486	科沃斯	603486	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603488	展鹏科技	603488	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603489	八方股份	603489	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603496	恒为科技	603496	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603499	翔港科技	603499	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603500	祥和实业	603500	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603501	韦尔股份	603501	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603505	金石资源	603505	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603506	南都物业	603506	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603507	振江股份	603507	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603508	思维列控	603508	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603511	爱慕股份	603511	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603515	欧普照明	603515	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603516	淳中科技	603516	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603517	绝味食品	603517	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603518	锦泓集团	603518	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603519	立霸股份	603519	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603520	司太立	603520	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603527	众源新材	603527	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603528	多伦科技	603528	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603529	爱玛科技	603529	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603530	神马电力	603530	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603533	掌阅科技	603533	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603535	嘉诚国际	603535	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603536	惠发食品	603536	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603538	美诺华	603538	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603551	奥普科技	603551	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603556	海兴电力	603556	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603558	健盛集团	603558	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603565	中谷物流	603565	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603566	普莱柯	603566	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603567	珍宝岛	603567	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603568	伟明环保	603568	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603569	长久物流	603569	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603577	汇金通	603577	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603578	三星新材	603578	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603579	荣泰健康	603579	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603583	捷昌驱动	603583	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603585	苏利股份	603585	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603586	金麒麟	603586	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603587	地素时尚	603587	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603588	高能环境	603588	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603589	口子窖	603589	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603590	康辰药业	603590	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603595	东尼电子	603595	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603596	伯特利	603596	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603598	引力传媒	603598	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603599	广信股份	603599	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603600	永艺股份	603600	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603601	再升科技	603601	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603602	纵横通信	603602	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603605	珀莱雅	603605	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603606	东方电缆	603606	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603607	京华激光	603607	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603608	天创时尚	603608	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603609	禾丰股份	603609	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603610	麒盛科技	603610	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603611	诺力股份	603611	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603612	索通发展	603612	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603613	国联股份	603613	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603615	茶花股份	603615	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603616	韩建河山	603616	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603617	君禾股份	603617	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603618	杭电股份	603618	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603619	中曼石油	603619	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603626	科森科技	603626	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603628	清源股份	603628	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603629	利通电子	603629	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603630	拉芳家化	603630	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603633	徕木股份	603633	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603636	南威软件	603636	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603637	镇海股份	603637	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603638	艾迪精密	603638	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603639	海利尔	603639	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603648	畅联股份	603648	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603650	彤程新材	603650	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603655	朗博科技	603655	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603656	泰禾智能	603656	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603657	春光科技	603657	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603658	安图生物	603658	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603659	璞泰来	603659	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603660	苏州科达	603660	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603661	XD恒林股	603661	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603662	柯力传感	603662	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603663	三祥新材	603663	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603665	康隆达	603665	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603666	亿嘉和	603666	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603667	五洲新春	603667	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603668	天马科技	603668	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603669	灵康药业	603669	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603676	卫信康	603676	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603677	奇精机械	603677	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603678	火炬电子	603678	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603679	华体科技	603679	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603680	今创集团	603680	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603681	永冠新材	603681	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603682	锦和商管	603682	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603683	晶华新材	603683	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603685	晨丰科技	603685	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603686	福龙马	603686	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603687	大胜达	603687	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603688	石英股份	603688	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603689	皖天然气	603689	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603690	至纯科技	603690	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603693	江苏新能	603693	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603696	安记食品	603696	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603697	有友食品	603697	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603698	航天工程	603698	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603699	纽威股份	603699	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603700	宁水集团	603700	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603701	德宏股份	603701	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603703	盛洋科技	603703	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603706	东方环宇	603706	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603707	健友股份	603707	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603708	家家悦	603708	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603709	中源家居	603709	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603711	香飘飘	603711	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603712	七一二	603712	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603713	密尔克卫	603713	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603716	塞力医疗	603716	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603717	天域生物	603717	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603718	海利生物	603718	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603719	良品铺子	603719	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603722	阿科力	603722	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603725	天安新材	603725	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603726	朗迪集团	603726	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603727	博迈科	603727	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603728	鸣志电器	603728	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603729	龙韵股份	603729	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603730	岱美股份	603730	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603733	仙鹤股份	603733	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603737	三棵树	603737	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603738	泰晶科技	603738	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603739	蔚蓝生物	603739	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603755	日辰股份	603755	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603757	XD大元泵	603757	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603758	秦安股份	603758	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603759	海天股份	603759	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603766	隆鑫通用	603766	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603767	中马传动	603767	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603768	常青股份	603768	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603773	沃格光电	603773	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603776	永安行	603776	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603777	来伊份	603777	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603778	国晟科技	603778	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603779	威龙股份	603779	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603786	科博达	603786	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603787	新日股份	603787	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603788	宁波高发	603788	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603790	雅运股份	603790	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603797	联泰环保	603797	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603798	康普顿	603798	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603799	华友钴业	603799	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603800	洪田股份	603800	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603801	志邦家居	603801	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603803	瑞斯康达	603803	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603806	福斯特	603806	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603808	歌力思	603808	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603809	豪能股份	603809	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603810	丰山集团	603810	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603811	诚意药业	603811	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603815	交建股份	603815	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603816	顾家家居	603816	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603817	海峡环保	603817	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603818	曲美家居	603818	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603819	XD神力股	603819	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603822	嘉澳环保	603822	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603823	百合花	603823	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603825	华扬联众	603825	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603826	坤彩科技	603826	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603829	洛凯股份	603829	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603833	欧派家居	603833	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603836	海程邦达	603836	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603839	安正时尚	603839	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603848	好太太	603848	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603855	华荣股份	603855	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603856	东宏股份	603856	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603858	步长制药	603858	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603859	能科科技	603859	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603860	中公高科	603860	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603861	白云电器	603861	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603863	松炀资源	603863	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603866	桃李面包	603866	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603867	新化股份	603867	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603868	飞科电器	603868	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603871	嘉友国际	603871	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603876	鼎胜新材	603876	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603877	太平鸟	603877	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603878	武进不锈	603878	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603879	永悦科技	603879	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603880	南卫股份	603880	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603881	数据港	603881	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603882	金域医学	603882	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603883	老百姓	603883	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603885	吉祥航空	603885	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603886	元祖股份	603886	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603887	城地香江	603887	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603888	新华网	603888	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603889	新澳股份	603889	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603890	春秋电子	603890	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603893	瑞芯微	603893	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603895	天永智能	603895	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603896	寿仙谷	603896	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603897	长城科技	603897	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603898	好莱客	603898	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603899	晨光股份	603899	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603900	莱绅通灵	603900	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603901	永创智能	603901	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603903	中持股份	603903	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603906	龙蟠科技	603906	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603908	牧高笛	603908	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603909	建发合诚	603909	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603912	佳力图	603912	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603915	国茂股份	603915	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603916	苏博特	603916	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603917	合力科技	603917	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603918	金桥信息	603918	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603919	金徽酒	603919	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603920	世运电路	603920	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603922	金鸿顺	603922	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603926	铁流股份	603926	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603927	中科软	603927	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603928	兴业股份	603928	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603929	亚翔集成	603929	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603931	格林达	603931	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603933	睿能科技	603933	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603936	博敏电子	603936	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603937	丽岛新材	603937	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603938	三孚股份	603938	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603939	益丰药房	603939	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603948	建业股份	603948	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603949	雪龙集团	603949	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603950	长源东谷	603950	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603955	大千生态	603955	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603956	威派格	603956	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603958	哈森股份	603958	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603960	克来机电	603960	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603966	法兰泰克	603966	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603967	中创物流	603967	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603968	醋化股份	603968	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603969	银龙股份	603969	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603970	中农立华	603970	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603976	正川股份	603976	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603977	国泰集团	603977	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603978	深圳新星	603978	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603979	金诚信	603979	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603980	吉华集团	603980	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603982	泉峰汽车	603982	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603983	丸美生物	603983	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603985	恒润股份	603985	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603986	兆易创新	603986	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603987	康德莱	603987	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603988	中电电机	603988	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603989	艾华集团	603989	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603990	麦迪科技	603990	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603991	至正股份	603991	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603992	松霖科技	603992	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603993	洛阳钼业	603993	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603995	甬金股份	603995	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603997	继峰股份	603997	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603998	方盛制药	603998	上交所主板	SSE	L	2025-06-17 15:57:24.280455
603999	读者传媒	603999	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605001	威奥股份	605001	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605003	众望布艺	605003	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605005	合兴股份	605005	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605006	山东玻纤	605006	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605007	五洲特纸	605007	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605008	长鸿高科	605008	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605009	豪悦护理	605009	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605011	杭州热电	605011	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605016	百龙创园	605016	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605018	XD长华集	605018	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605020	永和股份	605020	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605028	世茂能源	605028	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605033	美邦股份	605033	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605050	福然德	605050	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605055	迎丰股份	605055	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605056	咸亨国际	605056	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605058	澳弘电子	605058	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605060	联德股份	605060	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605066	天正电气	605066	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605068	明新旭腾	605068	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605069	正和生态	605069	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605077	华康股份	605077	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605080	浙江自然	605080	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605086	龙高股份	605086	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605088	冠盛股份	605088	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605089	味知香	605089	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605090	九丰能源	605090	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605098	行动教育	605098	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605099	共创草坪	605099	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605100	华丰股份	605100	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605108	同庆楼	605108	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605111	新洁能	605111	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605116	奥锐特	605116	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605117	德业股份	605117	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605118	力鼎光电	605118	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605122	四方新材	605122	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605123	派克新材	605123	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605128	上海沿浦	605128	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605133	嵘泰股份	605133	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605136	丽人丽妆	605136	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605138	盛泰集团	605138	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605151	西上海	605151	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605155	西大门	605155	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605158	华达新材	605158	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605162	新中港	605162	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605166	聚合顺	605166	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605167	利柏特	605167	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605168	三人行	605168	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605169	洪通燃气	605169	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605177	东亚药业	605177	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605178	时空科技	605178	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605179	一鸣食品	605179	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605180	华生科技	605180	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605183	确成股份	605183	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605186	健麾信息	605186	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605188	国光连锁	605188	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605189	富春染织	605189	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605196	华通线缆	605196	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605198	安德利	605198	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605208	永茂泰	605208	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605218	伟时电子	605218	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605222	起帆电缆	605222	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605228	神通科技	605228	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605255	天普股份	605255	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605258	协和电子	605258	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605259	绿田机械	605259	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605266	健之佳	605266	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605268	王力安防	605268	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605277	XD新亚电	605277	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605286	同力日升	605286	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605287	德才股份	605287	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605288	凯迪股份	605288	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605289	罗曼股份	605289	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605296	神农集团	605296	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605298	必得科技	605298	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605299	舒华体育	605299	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605300	佳禾食品	605300	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605303	园林股份	605303	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605305	中际联合	605305	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605318	法狮龙	605318	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605319	无锡振华	605319	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605333	沪光股份	605333	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605336	帅丰电器	605336	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605337	XD李子园	605337	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605338	巴比食品	605338	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605339	南侨食品	605339	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605358	立昂微	605358	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605365	立达信	605365	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605366	宏柏新材	605366	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605368	蓝天燃气	605368	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605369	拱东医疗	605369	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605376	博迁新材	605376	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605377	华旺科技	605377	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605378	野马电池	605378	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605388	均瑶健康	605388	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605389	长龄液压	605389	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605398	新炬网络	605398	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605399	晨光新材	605399	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605488	福莱新材	605488	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605499	东鹏饮料	605499	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605500	森林包装	605500	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605507	国邦医药	605507	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605555	德昌股份	605555	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605566	福莱蒽特	605566	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605567	春雪食品	605567	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605577	龙版传媒	605577	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605580	恒盛能源	605580	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605588	冠石科技	605588	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605589	圣泉集团	605589	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605598	上海港湾	605598	上交所主板	SSE	L	2025-06-17 15:57:24.280455
605599	XD菜百股	605599	上交所主板	SSE	L	2025-06-17 15:57:24.280455
688001	华兴源创	688001	科创板	SSE	L	2025-06-17 15:57:24.280455
688002	睿创微纳	688002	科创板	SSE	L	2025-06-17 15:57:24.280455
688003	天准科技	688003	科创板	SSE	L	2025-06-17 15:57:24.280455
688004	博汇科技	688004	科创板	SSE	L	2025-06-17 15:57:24.280455
688005	容百科技	688005	科创板	SSE	L	2025-06-17 15:57:24.280455
688006	杭可科技	688006	科创板	SSE	L	2025-06-17 15:57:24.280455
688007	XD光峰科	688007	科创板	SSE	L	2025-06-17 15:57:24.280455
688008	澜起科技	688008	科创板	SSE	L	2025-06-17 15:57:24.280455
688009	中国通号	688009	科创板	SSE	L	2025-06-17 15:57:24.280455
688010	福光股份	688010	科创板	SSE	L	2025-06-17 15:57:24.280455
688011	新光光电	688011	科创板	SSE	L	2025-06-17 15:57:24.280455
688012	中微公司	688012	科创板	SSE	L	2025-06-17 15:57:24.280455
688013	天臣医疗	688013	科创板	SSE	L	2025-06-17 15:57:24.280455
688015	交控科技	688015	科创板	SSE	L	2025-06-17 15:57:24.280455
688016	心脉医疗	688016	科创板	SSE	L	2025-06-17 15:57:24.280455
688017	绿的谐波	688017	科创板	SSE	L	2025-06-17 15:57:24.280455
688018	乐鑫科技	688018	科创板	SSE	L	2025-06-17 15:57:24.280455
688019	DR安集科	688019	科创板	SSE	L	2025-06-17 15:57:24.280455
688020	方邦股份	688020	科创板	SSE	L	2025-06-17 15:57:24.280455
688021	奥福科技	688021	科创板	SSE	L	2025-06-17 15:57:24.280455
688022	瀚川智能	688022	科创板	SSE	L	2025-06-17 15:57:24.280455
688023	安恒信息	688023	科创板	SSE	L	2025-06-17 15:57:24.280455
688025	杰普特	688025	科创板	SSE	L	2025-06-17 15:57:24.280455
688026	洁特生物	688026	科创板	SSE	L	2025-06-17 15:57:24.280455
688027	国盾量子	688027	科创板	SSE	L	2025-06-17 15:57:24.280455
688028	沃尔德	688028	科创板	SSE	L	2025-06-17 15:57:24.280455
688029	南微医学	688029	科创板	SSE	L	2025-06-17 15:57:24.280455
688030	山石网科	688030	科创板	SSE	L	2025-06-17 15:57:24.280455
688031	星环科技	688031	科创板	SSE	L	2025-06-17 15:57:24.280455
688032	禾迈股份	688032	科创板	SSE	L	2025-06-17 15:57:24.280455
688033	天宜新材	688033	科创板	SSE	L	2025-06-17 15:57:24.280455
688035	德邦科技	688035	科创板	SSE	L	2025-06-17 15:57:24.280455
688036	传音控股	688036	科创板	SSE	L	2025-06-17 15:57:24.280455
688037	芯源微	688037	科创板	SSE	L	2025-06-17 15:57:24.280455
688038	中科通达	688038	科创板	SSE	L	2025-06-17 15:57:24.280455
688039	当虹科技	688039	科创板	SSE	L	2025-06-17 15:57:24.280455
688041	海光信息	688041	科创板	SSE	L	2025-06-17 15:57:24.280455
688045	必易微	688045	科创板	SSE	L	2025-06-17 15:57:24.280455
688046	药康生物	688046	科创板	SSE	L	2025-06-17 15:57:24.280455
688047	龙芯中科	688047	科创板	SSE	L	2025-06-17 15:57:24.280455
688048	长光华芯	688048	科创板	SSE	L	2025-06-17 15:57:24.280455
688049	炬芯科技	688049	科创板	SSE	L	2025-06-17 15:57:24.280455
688050	爱博医疗	688050	科创板	SSE	L	2025-06-17 15:57:24.280455
688051	佳华科技	688051	科创板	SSE	L	2025-06-17 15:57:24.280455
688052	纳芯微	688052	科创板	SSE	L	2025-06-17 15:57:24.280455
688053	思科瑞	688053	科创板	SSE	L	2025-06-17 15:57:24.280455
688055	龙腾光电	688055	科创板	SSE	L	2025-06-17 15:57:24.280455
688056	莱伯泰科	688056	科创板	SSE	L	2025-06-17 15:57:24.280455
688057	金达莱	688057	科创板	SSE	L	2025-06-17 15:57:24.280455
688058	宝兰德	688058	科创板	SSE	L	2025-06-17 15:57:24.280455
688059	华锐精密	688059	科创板	SSE	L	2025-06-17 15:57:24.280455
688060	云涌科技	688060	科创板	SSE	L	2025-06-17 15:57:24.280455
688061	灿瑞科技	688061	科创板	SSE	L	2025-06-17 15:57:24.280455
688062	迈威生物	688062	科创板	SSE	L	2025-06-17 15:57:24.280455
688063	派能科技	688063	科创板	SSE	L	2025-06-17 15:57:24.280455
688065	凯赛生物	688065	科创板	SSE	L	2025-06-17 15:57:24.280455
688066	航天宏图	688066	科创板	SSE	L	2025-06-17 15:57:24.280455
688067	爱威科技	688067	科创板	SSE	L	2025-06-17 15:57:24.280455
688068	热景生物	688068	科创板	SSE	L	2025-06-17 15:57:24.280455
688069	德林海	688069	科创板	SSE	L	2025-06-17 15:57:24.280455
688070	纵横股份	688070	科创板	SSE	L	2025-06-17 15:57:24.280455
688071	华依科技	688071	科创板	SSE	L	2025-06-17 15:57:24.280455
688072	拓荆科技	688072	科创板	SSE	L	2025-06-17 15:57:24.280455
688073	毕得医药	688073	科创板	SSE	L	2025-06-17 15:57:24.280455
688075	安旭生物	688075	科创板	SSE	L	2025-06-17 15:57:24.280455
688076	诺泰生物	688076	科创板	SSE	L	2025-06-17 15:57:24.280455
688077	大地熊	688077	科创板	SSE	L	2025-06-17 15:57:24.280455
688078	龙软科技	688078	科创板	SSE	L	2025-06-17 15:57:24.280455
688079	美迪凯	688079	科创板	SSE	L	2025-06-17 15:57:24.280455
688080	映翰通	688080	科创板	SSE	L	2025-06-17 15:57:24.280455
688081	兴图新科	688081	科创板	SSE	L	2025-06-17 15:57:24.280455
688082	盛美上海	688082	科创板	SSE	L	2025-06-17 15:57:24.280455
688083	中望软件	688083	科创板	SSE	L	2025-06-17 15:57:24.280455
688084	晶品特装	688084	科创板	SSE	L	2025-06-17 15:57:24.280455
688085	三友医疗	688085	科创板	SSE	L	2025-06-17 15:57:24.280455
688087	英科再生	688087	科创板	SSE	L	2025-06-17 15:57:24.280455
688088	虹软科技	688088	科创板	SSE	L	2025-06-17 15:57:24.280455
688089	嘉必优	688089	科创板	SSE	L	2025-06-17 15:57:24.280455
688090	瑞松科技	688090	科创板	SSE	L	2025-06-17 15:57:24.280455
688091	上海谊众	688091	科创板	SSE	L	2025-06-17 15:57:24.280455
688092	爱科科技	688092	科创板	SSE	L	2025-06-17 15:57:24.280455
688093	世华科技	688093	科创板	SSE	L	2025-06-17 15:57:24.280455
688095	福昕软件	688095	科创板	SSE	L	2025-06-17 15:57:24.280455
688096	京源环保	688096	科创板	SSE	L	2025-06-17 15:57:24.280455
688097	博众精工	688097	科创板	SSE	L	2025-06-17 15:57:24.280455
688098	申联生物	688098	科创板	SSE	L	2025-06-17 15:57:24.280455
688099	晶晨股份	688099	科创板	SSE	L	2025-06-17 15:57:24.280455
688100	威胜信息	688100	科创板	SSE	L	2025-06-17 15:57:24.280455
688101	三达膜	688101	科创板	SSE	L	2025-06-17 15:57:24.280455
688102	斯瑞新材	688102	科创板	SSE	L	2025-06-17 15:57:24.280455
688103	国力股份	688103	科创板	SSE	L	2025-06-17 15:57:24.280455
688105	诺唯赞	688105	科创板	SSE	L	2025-06-17 15:57:24.280455
688106	金宏气体	688106	科创板	SSE	L	2025-06-17 15:57:24.280455
688107	安路科技	688107	科创板	SSE	L	2025-06-17 15:57:24.280455
688108	赛诺医疗	688108	科创板	SSE	L	2025-06-17 15:57:24.280455
688109	品茗科技	688109	科创板	SSE	L	2025-06-17 15:57:24.280455
688110	东芯股份	688110	科创板	SSE	L	2025-06-17 15:57:24.280455
688111	金山办公	688111	科创板	SSE	L	2025-06-17 15:57:24.280455
688112	鼎阳科技	688112	科创板	SSE	L	2025-06-17 15:57:24.280455
688113	联测科技	688113	科创板	SSE	L	2025-06-17 15:57:24.280455
688114	华大智造	688114	科创板	SSE	L	2025-06-17 15:57:24.280455
688115	思林杰	688115	科创板	SSE	L	2025-06-17 15:57:24.280455
688116	天奈科技	688116	科创板	SSE	L	2025-06-17 15:57:24.280455
688117	圣诺生物	688117	科创板	SSE	L	2025-06-17 15:57:24.280455
688118	普元信息	688118	科创板	SSE	L	2025-06-17 15:57:24.280455
688119	中钢洛耐	688119	科创板	SSE	L	2025-06-17 15:57:24.280455
688120	华海清科	688120	科创板	SSE	L	2025-06-17 15:57:24.280455
688121	卓然股份	688121	科创板	SSE	L	2025-06-17 15:57:24.280455
688122	西部超导	688122	科创板	SSE	L	2025-06-17 15:57:24.280455
688123	聚辰股份	688123	科创板	SSE	L	2025-06-17 15:57:24.280455
688125	安达智能	688125	科创板	SSE	L	2025-06-17 15:57:24.280455
688126	沪硅产业	688126	科创板	SSE	L	2025-06-17 15:57:24.280455
688127	蓝特光学	688127	科创板	SSE	L	2025-06-17 15:57:24.280455
688128	中国电研	688128	科创板	SSE	L	2025-06-17 15:57:24.280455
688129	东来技术	688129	科创板	SSE	L	2025-06-17 15:57:24.280455
688130	晶华微	688130	科创板	SSE	L	2025-06-17 15:57:24.280455
688131	皓元医药	688131	科创板	SSE	L	2025-06-17 15:57:24.280455
688132	邦彦技术	688132	科创板	SSE	L	2025-06-17 15:57:24.280455
688133	泰坦科技	688133	科创板	SSE	L	2025-06-17 15:57:24.280455
688135	利扬芯片	688135	科创板	SSE	L	2025-06-17 15:57:24.280455
688136	科兴制药	688136	科创板	SSE	L	2025-06-17 15:57:24.280455
688137	近岸蛋白	688137	科创板	SSE	L	2025-06-17 15:57:24.280455
688138	清溢光电	688138	科创板	SSE	L	2025-06-17 15:57:24.280455
688139	海尔生物	688139	科创板	SSE	L	2025-06-17 15:57:24.280455
688141	杰华特	688141	科创板	SSE	L	2025-06-17 15:57:24.280455
688143	长盈通	688143	科创板	SSE	L	2025-06-17 15:57:24.280455
688146	中船特气	688146	科创板	SSE	L	2025-06-17 15:57:24.280455
688147	微导纳米	688147	科创板	SSE	L	2025-06-17 15:57:24.280455
688148	芳源股份	688148	科创板	SSE	L	2025-06-17 15:57:24.280455
688150	莱特光电	688150	科创板	SSE	L	2025-06-17 15:57:24.280455
688151	华强科技	688151	科创板	SSE	L	2025-06-17 15:57:24.280455
688152	麒麟信安	688152	科创板	SSE	L	2025-06-17 15:57:24.280455
688153	唯捷创芯	688153	科创板	SSE	L	2025-06-17 15:57:24.280455
688155	先惠技术	688155	科创板	SSE	L	2025-06-17 15:57:24.280455
688156	路德环境	688156	科创板	SSE	L	2025-06-17 15:57:24.280455
688157	松井股份	688157	科创板	SSE	L	2025-06-17 15:57:24.280455
688158	优刻得	688158	科创板	SSE	L	2025-06-17 15:57:24.280455
688159	有方科技	688159	科创板	SSE	L	2025-06-17 15:57:24.280455
688160	步科股份	688160	科创板	SSE	L	2025-06-17 15:57:24.280455
688161	威高骨科	688161	科创板	SSE	L	2025-06-17 15:57:24.280455
688162	巨一科技	688162	科创板	SSE	L	2025-06-17 15:57:24.280455
688163	赛伦生物	688163	科创板	SSE	L	2025-06-17 15:57:24.280455
688165	埃夫特	688165	科创板	SSE	L	2025-06-17 15:57:24.280455
688166	博瑞医药	688166	科创板	SSE	L	2025-06-17 15:57:24.280455
688167	炬光科技	688167	科创板	SSE	L	2025-06-17 15:57:24.280455
688168	安博通	688168	科创板	SSE	L	2025-06-17 15:57:24.280455
688169	石头科技	688169	科创板	SSE	L	2025-06-17 15:57:24.280455
688170	德龙激光	688170	科创板	SSE	L	2025-06-17 15:57:24.280455
688171	纬德信息	688171	科创板	SSE	L	2025-06-17 15:57:24.280455
688175	高凌信息	688175	科创板	SSE	L	2025-06-17 15:57:24.280455
688176	亚虹医药	688176	科创板	SSE	L	2025-06-17 15:57:24.280455
688177	百奥泰	688177	科创板	SSE	L	2025-06-17 15:57:24.280455
688178	万德斯	688178	科创板	SSE	L	2025-06-17 15:57:24.280455
688179	阿拉丁	688179	科创板	SSE	L	2025-06-17 15:57:24.280455
688180	君实生物	688180	科创板	SSE	L	2025-06-17 15:57:24.280455
688181	八亿时空	688181	科创板	SSE	L	2025-06-17 15:57:24.280455
688182	灿勤科技	688182	科创板	SSE	L	2025-06-17 15:57:24.280455
688183	生益电子	688183	科创板	SSE	L	2025-06-17 15:57:24.280455
688185	康希诺	688185	科创板	SSE	L	2025-06-17 15:57:24.280455
688186	广大特材	688186	科创板	SSE	L	2025-06-17 15:57:24.280455
688187	时代电气	688187	科创板	SSE	L	2025-06-17 15:57:24.280455
688188	柏楚电子	688188	科创板	SSE	L	2025-06-17 15:57:24.280455
688189	南新制药	688189	科创板	SSE	L	2025-06-17 15:57:24.280455
688190	云路股份	688190	科创板	SSE	L	2025-06-17 15:57:24.280455
688191	智洋创新	688191	科创板	SSE	L	2025-06-17 15:57:24.280455
688192	迪哲医药	688192	科创板	SSE	L	2025-06-17 15:57:24.280455
688193	仁度生物	688193	科创板	SSE	L	2025-06-17 15:57:24.280455
688195	腾景科技	688195	科创板	SSE	L	2025-06-17 15:57:24.280455
688196	卓越新能	688196	科创板	SSE	L	2025-06-17 15:57:24.280455
688197	首药控股	688197	科创板	SSE	L	2025-06-17 15:57:24.280455
688198	佰仁医疗	688198	科创板	SSE	L	2025-06-17 15:57:24.280455
688199	久日新材	688199	科创板	SSE	L	2025-06-17 15:57:24.280455
688200	华峰测控	688200	科创板	SSE	L	2025-06-17 15:57:24.280455
688201	信安世纪	688201	科创板	SSE	L	2025-06-17 15:57:24.280455
688202	美迪西	688202	科创板	SSE	L	2025-06-17 15:57:24.280455
688203	海正生材	688203	科创板	SSE	L	2025-06-17 15:57:24.280455
688205	德科立	688205	科创板	SSE	L	2025-06-17 15:57:24.280455
688206	概伦电子	688206	科创板	SSE	L	2025-06-17 15:57:24.280455
688207	格灵深瞳	688207	科创板	SSE	L	2025-06-17 15:57:24.280455
688208	道通科技	688208	科创板	SSE	L	2025-06-17 15:57:24.280455
688209	英集芯	688209	科创板	SSE	L	2025-06-17 15:57:24.280455
688210	统联精密	688210	科创板	SSE	L	2025-06-17 15:57:24.280455
688211	中科微至	688211	科创板	SSE	L	2025-06-17 15:57:24.280455
688212	澳华内镜	688212	科创板	SSE	L	2025-06-17 15:57:24.280455
688213	思特威	688213	科创板	SSE	L	2025-06-17 15:57:24.280455
688215	瑞晟智能	688215	科创板	SSE	L	2025-06-17 15:57:24.280455
688216	气派科技	688216	科创板	SSE	L	2025-06-17 15:57:24.280455
688217	睿昂基因	688217	科创板	SSE	L	2025-06-17 15:57:24.280455
688218	江苏北人	688218	科创板	SSE	L	2025-06-17 15:57:24.280455
688219	会通股份	688219	科创板	SSE	L	2025-06-17 15:57:24.280455
688220	翱捷科技	688220	科创板	SSE	L	2025-06-17 15:57:24.280455
688221	前沿生物	688221	科创板	SSE	L	2025-06-17 15:57:24.280455
688222	成都先导	688222	科创板	SSE	L	2025-06-17 15:57:24.280455
688223	晶科能源	688223	科创板	SSE	L	2025-06-17 15:57:24.280455
688225	亚信安全	688225	科创板	SSE	L	2025-06-17 15:57:24.280455
688226	威腾电气	688226	科创板	SSE	L	2025-06-17 15:57:24.280455
688227	品高股份	688227	科创板	SSE	L	2025-06-17 15:57:24.280455
688228	开普云	688228	科创板	SSE	L	2025-06-17 15:57:24.280455
688229	博睿数据	688229	科创板	SSE	L	2025-06-17 15:57:24.280455
688230	芯导科技	688230	科创板	SSE	L	2025-06-17 15:57:24.280455
688231	隆达股份	688231	科创板	SSE	L	2025-06-17 15:57:24.280455
688232	新点软件	688232	科创板	SSE	L	2025-06-17 15:57:24.280455
688233	神工股份	688233	科创板	SSE	L	2025-06-17 15:57:24.280455
688234	天岳先进	688234	科创板	SSE	L	2025-06-17 15:57:24.280455
688235	百济神州	688235	科创板	SSE	L	2025-06-17 15:57:24.280455
688236	春立医疗	688236	科创板	SSE	L	2025-06-17 15:57:24.280455
688237	超卓航科	688237	科创板	SSE	L	2025-06-17 15:57:24.280455
688238	和元生物	688238	科创板	SSE	L	2025-06-17 15:57:24.280455
688239	航宇科技	688239	科创板	SSE	L	2025-06-17 15:57:24.280455
688244	永信至诚	688244	科创板	SSE	L	2025-06-17 15:57:24.280455
688246	嘉和美康	688246	科创板	SSE	L	2025-06-17 15:57:24.280455
688247	宣泰医药	688247	科创板	SSE	L	2025-06-17 15:57:24.280455
688248	南网科技	688248	科创板	SSE	L	2025-06-17 15:57:24.280455
688249	晶合集成	688249	科创板	SSE	L	2025-06-17 15:57:24.280455
688251	井松智能	688251	科创板	SSE	L	2025-06-17 15:57:24.280455
688252	天德钰	688252	科创板	SSE	L	2025-06-17 15:57:24.280455
688253	英诺特	688253	科创板	SSE	L	2025-06-17 15:57:24.280455
688255	凯尔达	688255	科创板	SSE	L	2025-06-17 15:57:24.280455
688256	寒武纪	688256	科创板	SSE	L	2025-06-17 15:57:24.280455
688257	新锐股份	688257	科创板	SSE	L	2025-06-17 15:57:24.280455
688258	卓易信息	688258	科创板	SSE	L	2025-06-17 15:57:24.280455
688259	创耀科技	688259	科创板	SSE	L	2025-06-17 15:57:24.280455
688260	昀冢科技	688260	科创板	SSE	L	2025-06-17 15:57:24.280455
688261	东微半导	688261	科创板	SSE	L	2025-06-17 15:57:24.280455
688262	国芯科技	688262	科创板	SSE	L	2025-06-17 15:57:24.280455
688265	南模生物	688265	科创板	SSE	L	2025-06-17 15:57:24.280455
688266	泽璟制药	688266	科创板	SSE	L	2025-06-17 15:57:24.280455
688267	XD中触媒	688267	科创板	SSE	L	2025-06-17 15:57:24.280455
688268	华特气体	688268	科创板	SSE	L	2025-06-17 15:57:24.280455
688269	凯立新材	688269	科创板	SSE	L	2025-06-17 15:57:24.280455
688270	臻镭科技	688270	科创板	SSE	L	2025-06-17 15:57:24.280455
688271	联影医疗	688271	科创板	SSE	L	2025-06-17 15:57:24.280455
688272	富吉瑞	688272	科创板	SSE	L	2025-06-17 15:57:24.280455
688273	麦澜德	688273	科创板	SSE	L	2025-06-17 15:57:24.280455
688275	万润新能	688275	科创板	SSE	L	2025-06-17 15:57:24.280455
688276	百克生物	688276	科创板	SSE	L	2025-06-17 15:57:24.280455
688277	天智航	688277	科创板	SSE	L	2025-06-17 15:57:24.280455
688278	特宝生物	688278	科创板	SSE	L	2025-06-17 15:57:24.280455
688279	峰岹科技	688279	科创板	SSE	L	2025-06-17 15:57:24.280455
688280	精进电动	688280	科创板	SSE	L	2025-06-17 15:57:24.280455
688281	华秦科技	688281	科创板	SSE	L	2025-06-17 15:57:24.280455
688282	理工导航	688282	科创板	SSE	L	2025-06-17 15:57:24.280455
688283	坤恒顺维	688283	科创板	SSE	L	2025-06-17 15:57:24.280455
688285	高铁电气	688285	科创板	SSE	L	2025-06-17 15:57:24.280455
688286	敏芯股份	688286	科创板	SSE	L	2025-06-17 15:57:24.280455
688288	鸿泉物联	688288	科创板	SSE	L	2025-06-17 15:57:24.280455
688289	圣湘生物	688289	科创板	SSE	L	2025-06-17 15:57:24.280455
688290	景业智能	688290	科创板	SSE	L	2025-06-17 15:57:24.280455
688291	金橙子	688291	科创板	SSE	L	2025-06-17 15:57:24.280455
688292	浩瀚深度	688292	科创板	SSE	L	2025-06-17 15:57:24.280455
688293	奥浦迈	688293	科创板	SSE	L	2025-06-17 15:57:24.280455
688295	中复神鹰	688295	科创板	SSE	L	2025-06-17 15:57:24.280455
688296	和达科技	688296	科创板	SSE	L	2025-06-17 15:57:24.280455
688297	中无人机	688297	科创板	SSE	L	2025-06-17 15:57:24.280455
688298	东方生物	688298	科创板	SSE	L	2025-06-17 15:57:24.280455
688299	长阳科技	688299	科创板	SSE	L	2025-06-17 15:57:24.280455
688300	联瑞新材	688300	科创板	SSE	L	2025-06-17 15:57:24.280455
688301	奕瑞科技	688301	科创板	SSE	L	2025-06-17 15:57:24.280455
688302	海创药业	688302	科创板	SSE	L	2025-06-17 15:57:24.280455
688303	大全能源	688303	科创板	SSE	L	2025-06-17 15:57:24.280455
688305	科德数控	688305	科创板	SSE	L	2025-06-17 15:57:24.280455
688306	均普智能	688306	科创板	SSE	L	2025-06-17 15:57:24.280455
688307	中润光学	688307	科创板	SSE	L	2025-06-17 15:57:24.280455
688308	欧科亿	688308	科创板	SSE	L	2025-06-17 15:57:24.280455
688309	恒誉环保	688309	科创板	SSE	L	2025-06-17 15:57:24.280455
688310	迈得医疗	688310	科创板	SSE	L	2025-06-17 15:57:24.280455
688311	盟升电子	688311	科创板	SSE	L	2025-06-17 15:57:24.280455
688312	燕麦科技	688312	科创板	SSE	L	2025-06-17 15:57:24.280455
688313	仕佳光子	688313	科创板	SSE	L	2025-06-17 15:57:24.280455
688314	康拓医疗	688314	科创板	SSE	L	2025-06-17 15:57:24.280455
688315	诺禾致源	688315	科创板	SSE	L	2025-06-17 15:57:24.280455
688316	青云科技	688316	科创板	SSE	L	2025-06-17 15:57:24.280455
688317	之江生物	688317	科创板	SSE	L	2025-06-17 15:57:24.280455
688318	财富趋势	688318	科创板	SSE	L	2025-06-17 15:57:24.280455
688319	欧林生物	688319	科创板	SSE	L	2025-06-17 15:57:24.280455
688320	禾川科技	688320	科创板	SSE	L	2025-06-17 15:57:24.280455
688321	微芯生物	688321	科创板	SSE	L	2025-06-17 15:57:24.280455
688322	奥比中光	688322	科创板	SSE	L	2025-06-17 15:57:24.280455
688323	瑞华泰	688323	科创板	SSE	L	2025-06-17 15:57:24.280455
688325	赛微微电	688325	科创板	SSE	L	2025-06-17 15:57:24.280455
688326	经纬恒润	688326	科创板	SSE	L	2025-06-17 15:57:24.280455
688327	云从科技	688327	科创板	SSE	L	2025-06-17 15:57:24.280455
688328	深科达	688328	科创板	SSE	L	2025-06-17 15:57:24.280455
688329	艾隆科技	688329	科创板	SSE	L	2025-06-17 15:57:24.280455
688330	宏力达	688330	科创板	SSE	L	2025-06-17 15:57:24.280455
688331	荣昌生物	688331	科创板	SSE	L	2025-06-17 15:57:24.280455
688332	中科蓝讯	688332	科创板	SSE	L	2025-06-17 15:57:24.280455
688333	铂力特	688333	科创板	SSE	L	2025-06-17 15:57:24.280455
688334	西高院	688334	科创板	SSE	L	2025-06-17 15:57:24.280455
688335	复洁环保	688335	科创板	SSE	L	2025-06-17 15:57:24.280455
688336	三生国健	688336	科创板	SSE	L	2025-06-17 15:57:24.280455
688337	普源精电	688337	科创板	SSE	L	2025-06-17 15:57:24.280455
688338	赛科希德	688338	科创板	SSE	L	2025-06-17 15:57:24.280455
688339	亿华通	688339	科创板	SSE	L	2025-06-17 15:57:24.280455
688343	云天励飞	688343	科创板	SSE	L	2025-06-17 15:57:24.280455
688345	博力威	688345	科创板	SSE	L	2025-06-17 15:57:24.280455
688347	华虹公司	688347	科创板	SSE	L	2025-06-17 15:57:24.280455
688348	昱能科技	688348	科创板	SSE	L	2025-06-17 15:57:24.280455
688349	XD三一重	688349	科创板	SSE	L	2025-06-17 15:57:24.280455
688350	富淼科技	688350	科创板	SSE	L	2025-06-17 15:57:24.280455
688351	微电生理	688351	科创板	SSE	L	2025-06-17 15:57:24.280455
688352	颀中科技	688352	科创板	SSE	L	2025-06-17 15:57:24.280455
688353	华盛锂电	688353	科创板	SSE	L	2025-06-17 15:57:24.280455
688355	明志科技	688355	科创板	SSE	L	2025-06-17 15:57:24.280455
688356	键凯科技	688356	科创板	SSE	L	2025-06-17 15:57:24.280455
688357	建龙微纳	688357	科创板	SSE	L	2025-06-17 15:57:24.280455
688358	祥生医疗	688358	科创板	SSE	L	2025-06-17 15:57:24.280455
688359	三孚新科	688359	科创板	SSE	L	2025-06-17 15:57:24.280455
688360	德马科技	688360	科创板	SSE	L	2025-06-17 15:57:24.280455
688361	中科飞测	688361	科创板	SSE	L	2025-06-17 15:57:24.280455
688362	甬矽电子	688362	科创板	SSE	L	2025-06-17 15:57:24.280455
688363	华熙生物	688363	科创板	SSE	L	2025-06-17 15:57:24.280455
688365	光云科技	688365	科创板	SSE	L	2025-06-17 15:57:24.280455
688366	昊海生科	688366	科创板	SSE	L	2025-06-17 15:57:24.280455
688367	工大高科	688367	科创板	SSE	L	2025-06-17 15:57:24.280455
688368	晶丰明源	688368	科创板	SSE	L	2025-06-17 15:57:24.280455
688369	致远互联	688369	科创板	SSE	L	2025-06-17 15:57:24.280455
688370	丛麟科技	688370	科创板	SSE	L	2025-06-17 15:57:24.280455
688371	菲沃泰	688371	科创板	SSE	L	2025-06-17 15:57:24.280455
688372	伟测科技	688372	科创板	SSE	L	2025-06-17 15:57:24.280455
688373	盟科药业	688373	科创板	SSE	L	2025-06-17 15:57:24.280455
688375	国博电子	688375	科创板	SSE	L	2025-06-17 15:57:24.280455
688376	美埃科技	688376	科创板	SSE	L	2025-06-17 15:57:24.280455
688377	迪威尔	688377	科创板	SSE	L	2025-06-17 15:57:24.280455
688378	奥来德	688378	科创板	SSE	L	2025-06-17 15:57:24.280455
688379	华光新材	688379	科创板	SSE	L	2025-06-17 15:57:24.280455
688380	中微半导	688380	科创板	SSE	L	2025-06-17 15:57:24.280455
688381	帝奥微	688381	科创板	SSE	L	2025-06-17 15:57:24.280455
688382	益方生物	688382	科创板	SSE	L	2025-06-17 15:57:24.280455
688383	新益昌	688383	科创板	SSE	L	2025-06-17 15:57:24.280455
688385	复旦微电	688385	科创板	SSE	L	2025-06-17 15:57:24.280455
688386	泛亚微透	688386	科创板	SSE	L	2025-06-17 15:57:24.280455
688387	信科移动	688387	科创板	SSE	L	2025-06-17 15:57:24.280455
688388	嘉元科技	688388	科创板	SSE	L	2025-06-17 15:57:24.280455
688389	普门科技	688389	科创板	SSE	L	2025-06-17 15:57:24.280455
688390	固德威	688390	科创板	SSE	L	2025-06-17 15:57:24.280455
688391	钜泉科技	688391	科创板	SSE	L	2025-06-17 15:57:24.280455
688392	骄成超声	688392	科创板	SSE	L	2025-06-17 15:57:24.280455
688393	安必平	688393	科创板	SSE	L	2025-06-17 15:57:24.280455
688395	正弦电气	688395	科创板	SSE	L	2025-06-17 15:57:24.280455
688396	华润微	688396	科创板	SSE	L	2025-06-17 15:57:24.280455
688398	赛特新材	688398	科创板	SSE	L	2025-06-17 15:57:24.280455
688399	硕世生物	688399	科创板	SSE	L	2025-06-17 15:57:24.280455
688400	凌云光	688400	科创板	SSE	L	2025-06-17 15:57:24.280455
688401	路维光电	688401	科创板	SSE	L	2025-06-17 15:57:24.280455
688403	汇成股份	688403	科创板	SSE	L	2025-06-17 15:57:24.280455
688408	中信博	688408	科创板	SSE	L	2025-06-17 15:57:24.280455
688409	富创精密	688409	科创板	SSE	L	2025-06-17 15:57:24.280455
688410	山外山	688410	科创板	SSE	L	2025-06-17 15:57:24.280455
688411	海博思创	688411	科创板	SSE	L	2025-06-17 15:57:24.280455
688416	恒烁股份	688416	科创板	SSE	L	2025-06-17 15:57:24.280455
688418	震有科技	688418	科创板	SSE	L	2025-06-17 15:57:24.280455
688419	耐科装备	688419	科创板	SSE	L	2025-06-17 15:57:24.280455
688420	美腾科技	688420	科创板	SSE	L	2025-06-17 15:57:24.280455
688425	铁建重工	688425	科创板	SSE	L	2025-06-17 15:57:24.280455
688426	康为世纪	688426	科创板	SSE	L	2025-06-17 15:57:24.280455
688428	诺诚健华	688428	科创板	SSE	L	2025-06-17 15:57:24.280455
688429	时创能源	688429	科创板	SSE	L	2025-06-17 15:57:24.280455
688432	有研硅	688432	科创板	SSE	L	2025-06-17 15:57:24.280455
688433	华曙高科	688433	科创板	SSE	L	2025-06-17 15:57:24.280455
688435	英方软件	688435	科创板	SSE	L	2025-06-17 15:57:24.280455
688439	振华风光	688439	科创板	SSE	L	2025-06-17 15:57:24.280455
688443	智翔金泰	688443	科创板	SSE	L	2025-06-17 15:57:24.280455
688448	磁谷科技	688448	科创板	SSE	L	2025-06-17 15:57:24.280455
688449	联芸科技	688449	科创板	SSE	L	2025-06-17 15:57:24.280455
688450	光格科技	688450	科创板	SSE	L	2025-06-17 15:57:24.280455
688455	科捷智能	688455	科创板	SSE	L	2025-06-17 15:57:24.280455
688456	有研粉材	688456	科创板	SSE	L	2025-06-17 15:57:24.280455
688458	美芯晟	688458	科创板	SSE	L	2025-06-17 15:57:24.280455
688459	哈铁科技	688459	科创板	SSE	L	2025-06-17 15:57:24.280455
688466	金科环境	688466	科创板	SSE	L	2025-06-17 15:57:24.280455
688468	科美诊断	688468	科创板	SSE	L	2025-06-17 15:57:24.280455
688469	芯联集成	688469	科创板	SSE	L	2025-06-17 15:57:24.280455
688472	阿特斯	688472	科创板	SSE	L	2025-06-17 15:57:24.280455
688475	萤石网络	688475	科创板	SSE	L	2025-06-17 15:57:24.280455
688478	晶升股份	688478	科创板	SSE	L	2025-06-17 15:57:24.280455
688479	友车科技	688479	科创板	SSE	L	2025-06-17 15:57:24.280455
688480	赛恩斯	688480	科创板	SSE	L	2025-06-17 15:57:24.280455
688484	南芯科技	688484	科创板	SSE	L	2025-06-17 15:57:24.280455
688485	九州一轨	688485	科创板	SSE	L	2025-06-17 15:57:24.280455
688486	龙迅股份	688486	科创板	SSE	L	2025-06-17 15:57:24.280455
688488	艾迪药业	688488	科创板	SSE	L	2025-06-17 15:57:24.280455
688489	三未信安	688489	科创板	SSE	L	2025-06-17 15:57:24.280455
688496	清越科技	688496	科创板	SSE	L	2025-06-17 15:57:24.280455
688498	源杰科技	688498	科创板	SSE	L	2025-06-17 15:57:24.280455
688499	利元亨	688499	科创板	SSE	L	2025-06-17 15:57:24.280455
688500	慧辰股份	688500	科创板	SSE	L	2025-06-17 15:57:24.280455
688501	青达环保	688501	科创板	SSE	L	2025-06-17 15:57:24.280455
688502	茂莱光学	688502	科创板	SSE	L	2025-06-17 15:57:24.280455
688503	聚和材料	688503	科创板	SSE	L	2025-06-17 15:57:24.280455
688505	复旦张江	688505	科创板	SSE	L	2025-06-17 15:57:24.280455
688506	百利天恒	688506	科创板	SSE	L	2025-06-17 15:57:24.280455
688507	索辰科技	688507	科创板	SSE	L	2025-06-17 15:57:24.280455
688508	芯朋微	688508	科创板	SSE	L	2025-06-17 15:57:24.280455
688509	正元地信	688509	科创板	SSE	L	2025-06-17 15:57:24.280455
688510	航亚科技	688510	科创板	SSE	L	2025-06-17 15:57:24.280455
688512	慧智微	688512	科创板	SSE	L	2025-06-17 15:57:24.280455
688513	苑东生物	688513	科创板	SSE	L	2025-06-17 15:57:24.280455
688515	裕太微	688515	科创板	SSE	L	2025-06-17 15:57:24.280455
688516	奥特维	688516	科创板	SSE	L	2025-06-17 15:57:24.280455
688517	金冠电气	688517	科创板	SSE	L	2025-06-17 15:57:24.280455
688518	联赢激光	688518	科创板	SSE	L	2025-06-17 15:57:24.280455
688519	南亚新材	688519	科创板	SSE	L	2025-06-17 15:57:24.280455
688520	神州细胞	688520	科创板	SSE	L	2025-06-17 15:57:24.280455
688521	芯原股份	688521	科创板	SSE	L	2025-06-17 15:57:24.280455
688522	纳睿雷达	688522	科创板	SSE	L	2025-06-17 15:57:24.280455
688523	航天环宇	688523	科创板	SSE	L	2025-06-17 15:57:24.280455
688525	佰维存储	688525	科创板	SSE	L	2025-06-17 15:57:24.280455
688526	XD科前生	688526	科创板	SSE	L	2025-06-17 15:57:24.280455
688528	秦川物联	688528	科创板	SSE	L	2025-06-17 15:57:24.280455
688529	豪森智能	688529	科创板	SSE	L	2025-06-17 15:57:24.280455
688530	欧莱新材	688530	科创板	SSE	L	2025-06-17 15:57:24.280455
688531	日联科技	688531	科创板	SSE	L	2025-06-17 15:57:24.280455
688533	上声电子	688533	科创板	SSE	L	2025-06-17 15:57:24.280455
688535	华海诚科	688535	科创板	SSE	L	2025-06-17 15:57:24.280455
688536	思瑞浦	688536	科创板	SSE	L	2025-06-17 15:57:24.280455
688538	和辉光电	688538	科创板	SSE	L	2025-06-17 15:57:24.280455
688539	高华科技	688539	科创板	SSE	L	2025-06-17 15:57:24.280455
688543	国科军工	688543	科创板	SSE	L	2025-06-17 15:57:24.280455
688545	兴福电子	688545	科创板	SSE	L	2025-06-17 15:57:24.280455
688548	广钢气体	688548	科创板	SSE	L	2025-06-17 15:57:24.280455
688549	中巨芯	688549	科创板	SSE	L	2025-06-17 15:57:24.280455
688550	瑞联新材	688550	科创板	SSE	L	2025-06-17 15:57:24.280455
688551	科威尔	688551	科创板	SSE	L	2025-06-17 15:57:24.280455
688552	航天南湖	688552	科创板	SSE	L	2025-06-17 15:57:24.280455
688553	汇宇制药	688553	科创板	SSE	L	2025-06-17 15:57:24.280455
688556	高测股份	688556	科创板	SSE	L	2025-06-17 15:57:24.280455
688557	兰剑智能	688557	科创板	SSE	L	2025-06-17 15:57:24.280455
688558	国盛智科	688558	科创板	SSE	L	2025-06-17 15:57:24.280455
688559	海目星	688559	科创板	SSE	L	2025-06-17 15:57:24.280455
688560	明冠新材	688560	科创板	SSE	L	2025-06-17 15:57:24.280455
688561	奇安信	688561	科创板	SSE	L	2025-06-17 15:57:24.280455
688562	航天软件	688562	科创板	SSE	L	2025-06-17 15:57:24.280455
688563	航材股份	688563	科创板	SSE	L	2025-06-17 15:57:24.280455
688565	力源科技	688565	科创板	SSE	L	2025-06-17 15:57:24.280455
688566	吉贝尔	688566	科创板	SSE	L	2025-06-17 15:57:24.280455
688567	孚能科技	688567	科创板	SSE	L	2025-06-17 15:57:24.280455
688568	中科星图	688568	科创板	SSE	L	2025-06-17 15:57:24.280455
688569	铁科轨道	688569	科创板	SSE	L	2025-06-17 15:57:24.280455
688570	天玛智控	688570	科创板	SSE	L	2025-06-17 15:57:24.280455
688571	杭华股份	688571	科创板	SSE	L	2025-06-17 15:57:24.280455
688573	信宇人	688573	科创板	SSE	L	2025-06-17 15:57:24.280455
688575	亚辉龙	688575	科创板	SSE	L	2025-06-17 15:57:24.280455
688576	西山科技	688576	科创板	SSE	L	2025-06-17 15:57:24.280455
688577	浙海德曼	688577	科创板	SSE	L	2025-06-17 15:57:24.280455
688578	艾力斯	688578	科创板	SSE	L	2025-06-17 15:57:24.280455
688579	山大地纬	688579	科创板	SSE	L	2025-06-17 15:57:24.280455
688580	伟思医疗	688580	科创板	SSE	L	2025-06-17 15:57:24.280455
688581	安杰思	688581	科创板	SSE	L	2025-06-17 15:57:24.280455
688582	芯动联科	688582	科创板	SSE	L	2025-06-17 15:57:24.280455
688583	思看科技	688583	科创板	SSE	L	2025-06-17 15:57:24.280455
688584	上海合晶	688584	科创板	SSE	L	2025-06-17 15:57:24.280455
688585	上纬新材	688585	科创板	SSE	L	2025-06-17 15:57:24.280455
688586	江航装备	688586	科创板	SSE	L	2025-06-17 15:57:24.280455
688588	凌志软件	688588	科创板	SSE	L	2025-06-17 15:57:24.280455
688589	力合微	688589	科创板	SSE	L	2025-06-17 15:57:24.280455
688590	新致软件	688590	科创板	SSE	L	2025-06-17 15:57:24.280455
688591	泰凌微	688591	科创板	SSE	L	2025-06-17 15:57:24.280455
688592	司南导航	688592	科创板	SSE	L	2025-06-17 15:57:24.280455
688593	新相微	688593	科创板	SSE	L	2025-06-17 15:57:24.280455
688595	芯海科技	688595	科创板	SSE	L	2025-06-17 15:57:24.280455
688596	正帆科技	688596	科创板	SSE	L	2025-06-17 15:57:24.280455
688597	煜邦电力	688597	科创板	SSE	L	2025-06-17 15:57:24.280455
688598	金博股份	688598	科创板	SSE	L	2025-06-17 15:57:24.280455
688599	天合光能	688599	科创板	SSE	L	2025-06-17 15:57:24.280455
688600	皖仪科技	688600	科创板	SSE	L	2025-06-17 15:57:24.280455
688601	力芯微	688601	科创板	SSE	L	2025-06-17 15:57:24.280455
688602	康鹏科技	688602	科创板	SSE	L	2025-06-17 15:57:24.280455
688603	天承科技	688603	科创板	SSE	L	2025-06-17 15:57:24.280455
688605	先锋精科	688605	科创板	SSE	L	2025-06-17 15:57:24.280455
688606	奥泰生物	688606	科创板	SSE	L	2025-06-17 15:57:24.280455
688607	康众医疗	688607	科创板	SSE	L	2025-06-17 15:57:24.280455
688608	恒玄科技	688608	科创板	SSE	L	2025-06-17 15:57:24.280455
688609	九联科技	688609	科创板	SSE	L	2025-06-17 15:57:24.280455
688610	埃科光电	688610	科创板	SSE	L	2025-06-17 15:57:24.280455
688611	杭州柯林	688611	科创板	SSE	L	2025-06-17 15:57:24.280455
688613	奥精医疗	688613	科创板	SSE	L	2025-06-17 15:57:24.280455
688615	合合信息	688615	科创板	SSE	L	2025-06-17 15:57:24.280455
688616	西力科技	688616	科创板	SSE	L	2025-06-17 15:57:24.280455
688617	惠泰医疗	688617	科创板	SSE	L	2025-06-17 15:57:24.280455
688618	三旺通信	688618	科创板	SSE	L	2025-06-17 15:57:24.280455
688619	罗普特	688619	科创板	SSE	L	2025-06-17 15:57:24.280455
688620	安凯微	688620	科创板	SSE	L	2025-06-17 15:57:24.280455
688621	阳光诺和	688621	科创板	SSE	L	2025-06-17 15:57:24.280455
688622	禾信仪器	688622	科创板	SSE	L	2025-06-17 15:57:24.280455
688623	双元科技	688623	科创板	SSE	L	2025-06-17 15:57:24.280455
688625	呈和科技	688625	科创板	SSE	L	2025-06-17 15:57:24.280455
688626	翔宇医疗	688626	科创板	SSE	L	2025-06-17 15:57:24.280455
688627	精智达	688627	科创板	SSE	L	2025-06-17 15:57:24.280455
688628	优利德	688628	科创板	SSE	L	2025-06-17 15:57:24.280455
688629	华丰科技	688629	科创板	SSE	L	2025-06-17 15:57:24.280455
688630	芯碁微装	688630	科创板	SSE	L	2025-06-17 15:57:24.280455
688631	莱斯信息	688631	科创板	SSE	L	2025-06-17 15:57:24.280455
688633	星球石墨	688633	科创板	SSE	L	2025-06-17 15:57:24.280455
688636	智明达	688636	科创板	SSE	L	2025-06-17 15:57:24.280455
688638	誉辰智能	688638	科创板	SSE	L	2025-06-17 15:57:24.280455
688639	华恒生物	688639	科创板	SSE	L	2025-06-17 15:57:24.280455
688648	中邮科技	688648	科创板	SSE	L	2025-06-17 15:57:24.280455
688651	盛邦安全	688651	科创板	SSE	L	2025-06-17 15:57:24.280455
688652	京仪装备	688652	科创板	SSE	L	2025-06-17 15:57:24.280455
688653	康希通信	688653	科创板	SSE	L	2025-06-17 15:57:24.280455
688655	迅捷兴	688655	科创板	SSE	L	2025-06-17 15:57:24.280455
688656	浩欧博	688656	科创板	SSE	L	2025-06-17 15:57:24.280455
688657	浩辰软件	688657	科创板	SSE	L	2025-06-17 15:57:24.280455
688658	悦康药业	688658	科创板	SSE	L	2025-06-17 15:57:24.280455
688659	元琛科技	688659	科创板	SSE	L	2025-06-17 15:57:24.280455
688660	电气风电	688660	科创板	SSE	L	2025-06-17 15:57:24.280455
688661	和林微纳	688661	科创板	SSE	L	2025-06-17 15:57:24.280455
688662	富信科技	688662	科创板	SSE	L	2025-06-17 15:57:24.280455
688663	新风光	688663	科创板	SSE	L	2025-06-17 15:57:24.280455
688665	四方光电	688665	科创板	SSE	L	2025-06-17 15:57:24.280455
688667	菱电电控	688667	科创板	SSE	L	2025-06-17 15:57:24.280455
688668	鼎通科技	688668	科创板	SSE	L	2025-06-17 15:57:24.280455
688669	聚石化学	688669	科创板	SSE	L	2025-06-17 15:57:24.280455
688670	金迪克	688670	科创板	SSE	L	2025-06-17 15:57:24.280455
688671	碧兴物联	688671	科创板	SSE	L	2025-06-17 15:57:24.280455
688676	金盘科技	688676	科创板	SSE	L	2025-06-17 15:57:24.280455
688677	海泰新光	688677	科创板	SSE	L	2025-06-17 15:57:24.280455
688678	福立旺	688678	科创板	SSE	L	2025-06-17 15:57:24.280455
688679	通源环境	688679	科创板	SSE	L	2025-06-17 15:57:24.280455
688680	海优新材	688680	科创板	SSE	L	2025-06-17 15:57:24.280455
688681	科汇股份	688681	科创板	SSE	L	2025-06-17 15:57:24.280455
688682	霍莱沃	688682	科创板	SSE	L	2025-06-17 15:57:24.280455
688683	XD莱尔科	688683	科创板	SSE	L	2025-06-17 15:57:24.280455
688685	迈信林	688685	科创板	SSE	L	2025-06-17 15:57:24.280455
688686	XD奥普特	688686	科创板	SSE	L	2025-06-17 15:57:24.280455
688687	凯因科技	688687	科创板	SSE	L	2025-06-17 15:57:24.280455
688689	银河微电	688689	科创板	SSE	L	2025-06-17 15:57:24.280455
688690	纳微科技	688690	科创板	SSE	L	2025-06-17 15:57:24.280455
688691	灿芯股份	688691	科创板	SSE	L	2025-06-17 15:57:24.280455
688692	达梦数据	688692	科创板	SSE	L	2025-06-17 15:57:24.280455
688693	锴威特	688693	科创板	SSE	L	2025-06-17 15:57:24.280455
688695	中创股份	688695	科创板	SSE	L	2025-06-17 15:57:24.280455
688696	极米科技	688696	科创板	SSE	L	2025-06-17 15:57:24.280455
688697	纽威数控	688697	科创板	SSE	L	2025-06-17 15:57:24.280455
688698	伟创电气	688698	科创板	SSE	L	2025-06-17 15:57:24.280455
688699	明微电子	688699	科创板	SSE	L	2025-06-17 15:57:24.280455
688700	东威科技	688700	科创板	SSE	L	2025-06-17 15:57:24.280455
688701	卓锦股份	688701	科创板	SSE	L	2025-06-17 15:57:24.280455
688702	盛科通信	688702	科创板	SSE	L	2025-06-17 15:57:24.280455
688707	振华新材	688707	科创板	SSE	L	2025-06-17 15:57:24.280455
688708	佳驰科技	688708	科创板	SSE	L	2025-06-17 15:57:24.280455
688709	成都华微	688709	科创板	SSE	L	2025-06-17 15:57:24.280455
688710	益诺思	688710	科创板	SSE	L	2025-06-17 15:57:24.280455
688711	宏微科技	688711	科创板	SSE	L	2025-06-17 15:57:24.280455
688716	中研股份	688716	科创板	SSE	L	2025-06-17 15:57:24.280455
688717	艾罗能源	688717	科创板	SSE	L	2025-06-17 15:57:24.280455
688718	唯赛勃	688718	科创板	SSE	L	2025-06-17 15:57:24.280455
688719	爱科赛博	688719	科创板	SSE	L	2025-06-17 15:57:24.280455
688720	艾森股份	688720	科创板	SSE	L	2025-06-17 15:57:24.280455
688721	龙图光罩	688721	科创板	SSE	L	2025-06-17 15:57:24.280455
688722	同益中	688722	科创板	SSE	L	2025-06-17 15:57:24.280455
688726	拉普拉斯	688726	科创板	SSE	L	2025-06-17 15:57:24.280455
688728	格科微	688728	科创板	SSE	L	2025-06-17 15:57:24.280455
688733	XD壹石通	688733	科创板	SSE	L	2025-06-17 15:57:24.280455
688737	中自科技	688737	科创板	SSE	L	2025-06-17 15:57:24.280455
688739	成大生物	688739	科创板	SSE	L	2025-06-17 15:57:24.280455
688750	金天钛业	688750	科创板	SSE	L	2025-06-17 15:57:24.280455
688755	汉邦科技	688755	科创板	SSE	L	2025-06-17 15:57:24.280455
688757	胜科纳米	688757	科创板	SSE	L	2025-06-17 15:57:24.280455
688758	赛分科技	688758	科创板	SSE	L	2025-06-17 15:57:24.280455
688766	普冉股份	688766	科创板	SSE	L	2025-06-17 15:57:24.280455
688767	博拓生物	688767	科创板	SSE	L	2025-06-17 15:57:24.280455
688768	容知日新	688768	科创板	SSE	L	2025-06-17 15:57:24.280455
688772	珠海冠宇	688772	科创板	SSE	L	2025-06-17 15:57:24.280455
688775	C影石	688775	科创板	SSE	L	2025-06-17 15:57:24.280455
688776	国光电气	688776	科创板	SSE	L	2025-06-17 15:57:24.280455
688777	中控技术	688777	科创板	SSE	L	2025-06-17 15:57:24.280455
688778	厦钨新能	688778	科创板	SSE	L	2025-06-17 15:57:24.280455
688779	五矿新能	688779	科创板	SSE	L	2025-06-17 15:57:24.280455
688786	悦安新材	688786	科创板	SSE	L	2025-06-17 15:57:24.280455
688787	海天瑞声	688787	科创板	SSE	L	2025-06-17 15:57:24.280455
688788	科思科技	688788	科创板	SSE	L	2025-06-17 15:57:24.280455
688789	宏华数科	688789	科创板	SSE	L	2025-06-17 15:57:24.280455
688793	倍轻松	688793	科创板	SSE	L	2025-06-17 15:57:24.280455
688798	艾为电子	688798	科创板	SSE	L	2025-06-17 15:57:24.280455
688799	华纳药厂	688799	科创板	SSE	L	2025-06-17 15:57:24.280455
688800	瑞可达	688800	科创板	SSE	L	2025-06-17 15:57:24.280455
688819	天能股份	688819	科创板	SSE	L	2025-06-17 15:57:24.280455
688981	中芯国际	688981	科创板	SSE	L	2025-06-17 15:57:24.280455
689009	九号公司	689009	科创板	SSE	L	2025-06-17 15:57:24.280455
430017	星昊医药	430017	北交所	BSE	L	2025-06-17 15:57:24.280455
430047	诺思兰德	430047	北交所	BSE	L	2025-06-17 15:57:24.280455
430090	同辉信息	430090	北交所	BSE	L	2025-06-17 15:57:24.280455
430139	华岭股份	430139	北交所	BSE	L	2025-06-17 15:57:24.280455
430198	微创光电	430198	北交所	BSE	L	2025-06-17 15:57:24.280455
430300	辰光医疗	430300	北交所	BSE	L	2025-06-17 15:57:24.280455
430418	苏轴股份	430418	北交所	BSE	L	2025-06-17 15:57:24.280455
430425	乐创技术	430425	北交所	BSE	L	2025-06-17 15:57:24.280455
430476	海能技术	430476	北交所	BSE	L	2025-06-17 15:57:24.280455
430478	峆一药业	430478	北交所	BSE	L	2025-06-17 15:57:24.280455
430510	丰光精密	430510	北交所	BSE	L	2025-06-17 15:57:24.280455
430556	雅达股份	430556	北交所	BSE	L	2025-06-17 15:57:24.280455
430564	天润科技	430564	北交所	BSE	L	2025-06-17 15:57:24.280455
430685	新芝生物	430685	北交所	BSE	L	2025-06-17 15:57:24.280455
430718	合肥高科	430718	北交所	BSE	L	2025-06-17 15:57:24.280455
830779	武汉蓝电	830779	北交所	BSE	L	2025-06-17 15:57:24.280455
830809	安达科技	830809	北交所	BSE	L	2025-06-17 15:57:24.280455
830832	齐鲁华信	830832	北交所	BSE	L	2025-06-17 15:57:24.280455
830839	万通液压	830839	北交所	BSE	L	2025-06-17 15:57:24.280455
830879	基康仪器	830879	北交所	BSE	L	2025-06-17 15:57:24.280455
830896	旺成科技	830896	北交所	BSE	L	2025-06-17 15:57:24.280455
830946	森萱医药	830946	北交所	BSE	L	2025-06-17 15:57:24.280455
830964	润农节水	830964	北交所	BSE	L	2025-06-17 15:57:24.280455
830974	凯大催化	830974	北交所	BSE	L	2025-06-17 15:57:24.280455
831010	凯添燃气	831010	北交所	BSE	L	2025-06-17 15:57:24.280455
831039	国义招标	831039	北交所	BSE	L	2025-06-17 15:57:24.280455
831087	秋乐种业	831087	北交所	BSE	L	2025-06-17 15:57:24.280455
831152	昆工科技	831152	北交所	BSE	L	2025-06-17 15:57:24.280455
831167	鑫汇科	831167	北交所	BSE	L	2025-06-17 15:57:24.280455
831175	派诺科技	831175	北交所	BSE	L	2025-06-17 15:57:24.280455
831195	三祥科技	831195	北交所	BSE	L	2025-06-17 15:57:24.280455
831278	泰德股份	831278	北交所	BSE	L	2025-06-17 15:57:24.280455
831304	迪尔化工	831304	北交所	BSE	L	2025-06-17 15:57:24.280455
831305	海希通讯	831305	北交所	BSE	L	2025-06-17 15:57:24.280455
831370	新安洁	831370	北交所	BSE	L	2025-06-17 15:57:24.280455
831396	许昌智能	831396	北交所	BSE	L	2025-06-17 15:57:24.280455
831526	凯华材料	831526	北交所	BSE	L	2025-06-17 15:57:24.280455
831627	力王股份	831627	北交所	BSE	L	2025-06-17 15:57:24.280455
831641	格利尔	831641	北交所	BSE	L	2025-06-17 15:57:24.280455
831689	克莱特	831689	北交所	BSE	L	2025-06-17 15:57:24.280455
831726	朱老六	831726	北交所	BSE	L	2025-06-17 15:57:24.280455
831768	拾比佰	831768	北交所	BSE	L	2025-06-17 15:57:24.280455
831832	科达自控	831832	北交所	BSE	L	2025-06-17 15:57:24.280455
831834	三维股份	831834	北交所	BSE	L	2025-06-17 15:57:24.280455
831855	浙江大农	831855	北交所	BSE	L	2025-06-17 15:57:24.280455
831856	浩淼科技	831856	北交所	BSE	L	2025-06-17 15:57:24.280455
831906	舜宇精工	831906	北交所	BSE	L	2025-06-17 15:57:24.280455
831961	创远信科	831961	北交所	BSE	L	2025-06-17 15:57:24.280455
832000	安徽凤凰	832000	北交所	BSE	L	2025-06-17 15:57:24.280455
832023	田野股份	832023	北交所	BSE	L	2025-06-17 15:57:24.280455
832089	禾昌聚合	832089	北交所	BSE	L	2025-06-17 15:57:24.280455
832110	雷特科技	832110	北交所	BSE	L	2025-06-17 15:57:24.280455
832145	恒合股份	832145	北交所	BSE	L	2025-06-17 15:57:24.280455
832149	利尔达	832149	北交所	BSE	L	2025-06-17 15:57:24.280455
832171	志晟信息	832171	北交所	BSE	L	2025-06-17 15:57:24.280455
832175	东方碳素	832175	北交所	BSE	L	2025-06-17 15:57:24.280455
832225	利通科技	832225	北交所	BSE	L	2025-06-17 15:57:24.280455
832278	鹿得医疗	832278	北交所	BSE	L	2025-06-17 15:57:24.280455
832419	路斯股份	832419	北交所	BSE	L	2025-06-17 15:57:24.280455
832469	富恒新材	832469	北交所	BSE	L	2025-06-17 15:57:24.280455
832471	美邦科技	832471	北交所	BSE	L	2025-06-17 15:57:24.280455
832491	奥迪威	832491	北交所	BSE	L	2025-06-17 15:57:24.280455
832522	纳科诺尔	832522	北交所	BSE	L	2025-06-17 15:57:24.280455
832566	梓橦宫	832566	北交所	BSE	L	2025-06-17 15:57:24.280455
832651	天罡股份	832651	北交所	BSE	L	2025-06-17 15:57:24.280455
832662	方盛股份	832662	北交所	BSE	L	2025-06-17 15:57:24.280455
832735	德源药业	832735	北交所	BSE	L	2025-06-17 15:57:24.280455
832786	骑士乳业	832786	北交所	BSE	L	2025-06-17 15:57:24.280455
832802	保丽洁	832802	北交所	BSE	L	2025-06-17 15:57:24.280455
832876	慧为智能	832876	北交所	BSE	L	2025-06-17 15:57:24.280455
832885	星辰科技	832885	北交所	BSE	L	2025-06-17 15:57:24.280455
832978	开特股份	832978	北交所	BSE	L	2025-06-17 15:57:24.280455
832982	锦波生物	832982	北交所	BSE	L	2025-06-17 15:57:24.280455
833030	立方控股	833030	北交所	BSE	L	2025-06-17 15:57:24.280455
833075	柏星龙	833075	北交所	BSE	L	2025-06-17 15:57:24.280455
833171	国航远洋	833171	北交所	BSE	L	2025-06-17 15:57:24.280455
833230	欧康医药	833230	北交所	BSE	L	2025-06-17 15:57:24.280455
833266	生物谷	833266	北交所	BSE	L	2025-06-17 15:57:24.280455
833284	灵鸽科技	833284	北交所	BSE	L	2025-06-17 15:57:24.280455
833346	XD威贸电	833346	北交所	BSE	L	2025-06-17 15:57:24.280455
833394	民士达	833394	北交所	BSE	L	2025-06-17 15:57:24.280455
833427	华维设计	833427	北交所	BSE	L	2025-06-17 15:57:24.280455
833429	康比特	833429	北交所	BSE	L	2025-06-17 15:57:24.280455
833454	同心传动	833454	北交所	BSE	L	2025-06-17 15:57:24.280455
833455	汇隆活塞	833455	北交所	BSE	L	2025-06-17 15:57:24.280455
833509	同惠电子	833509	北交所	BSE	L	2025-06-17 15:57:24.280455
833523	德瑞锂电	833523	北交所	BSE	L	2025-06-17 15:57:24.280455
833533	骏创科技	833533	北交所	BSE	L	2025-06-17 15:57:24.280455
833575	康乐卫士	833575	北交所	BSE	L	2025-06-17 15:57:24.280455
833580	科创新材	833580	北交所	BSE	L	2025-06-17 15:57:24.280455
833751	惠同新材	833751	北交所	BSE	L	2025-06-17 15:57:24.280455
833781	瑞奇智造	833781	北交所	BSE	L	2025-06-17 15:57:24.280455
833873	中设咨询	833873	北交所	BSE	L	2025-06-17 15:57:24.280455
833914	XD远航精	833914	北交所	BSE	L	2025-06-17 15:57:24.280455
833943	优机股份	833943	北交所	BSE	L	2025-06-17 15:57:24.280455
834014	特瑞斯	834014	北交所	BSE	L	2025-06-17 15:57:24.280455
834021	流金科技	834021	北交所	BSE	L	2025-06-17 15:57:24.280455
834033	康普化学	834033	北交所	BSE	L	2025-06-17 15:57:24.280455
834058	华洋赛车	834058	北交所	BSE	L	2025-06-17 15:57:24.280455
834062	科润智控	834062	北交所	BSE	L	2025-06-17 15:57:24.280455
834261	一诺威	834261	北交所	BSE	L	2025-06-17 15:57:24.280455
834407	驰诚股份	834407	北交所	BSE	L	2025-06-17 15:57:24.280455
834415	恒拓开源	834415	北交所	BSE	L	2025-06-17 15:57:24.280455
834475	三友科技	834475	北交所	BSE	L	2025-06-17 15:57:24.280455
834599	同力股份	834599	北交所	BSE	L	2025-06-17 15:57:24.280455
834639	晨光电缆	834639	北交所	BSE	L	2025-06-17 15:57:24.280455
834765	美之高	834765	北交所	BSE	L	2025-06-17 15:57:24.280455
834770	艾能聚	834770	北交所	BSE	L	2025-06-17 15:57:24.280455
834950	迅安科技	834950	北交所	BSE	L	2025-06-17 15:57:24.280455
835174	五新隧装	835174	北交所	BSE	L	2025-06-17 15:57:24.280455
835179	凯德石英	835179	北交所	BSE	L	2025-06-17 15:57:24.280455
835184	国源科技	835184	北交所	BSE	L	2025-06-17 15:57:24.280455
835185	贝特瑞	835185	北交所	BSE	L	2025-06-17 15:57:24.280455
835207	众诚科技	835207	北交所	BSE	L	2025-06-17 15:57:24.280455
835237	力佳科技	835237	北交所	BSE	L	2025-06-17 15:57:24.280455
835368	连城数控	835368	北交所	BSE	L	2025-06-17 15:57:24.280455
835438	戈碧迦	835438	北交所	BSE	L	2025-06-17 15:57:24.280455
835508	殷图网联	835508	北交所	BSE	L	2025-06-17 15:57:24.280455
835579	机科股份	835579	北交所	BSE	L	2025-06-17 15:57:24.280455
835640	富士达	835640	北交所	BSE	L	2025-06-17 15:57:24.280455
835670	数字人	835670	北交所	BSE	L	2025-06-17 15:57:24.280455
835857	百甲科技	835857	北交所	BSE	L	2025-06-17 15:57:24.280455
835892	中科美菱	835892	北交所	BSE	L	2025-06-17 15:57:24.280455
835985	海泰新能	835985	北交所	BSE	L	2025-06-17 15:57:24.280455
836077	吉林碳谷	836077	北交所	BSE	L	2025-06-17 15:57:24.280455
836149	旭杰科技	836149	北交所	BSE	L	2025-06-17 15:57:24.280455
836208	青矩技术	836208	北交所	BSE	L	2025-06-17 15:57:24.280455
836221	易实精密	836221	北交所	BSE	L	2025-06-17 15:57:24.280455
836239	长虹能源	836239	北交所	BSE	L	2025-06-17 15:57:24.280455
836247	华密新材	836247	北交所	BSE	L	2025-06-17 15:57:24.280455
836260	中寰股份	836260	北交所	BSE	L	2025-06-17 15:57:24.280455
836263	中航泰达	836263	北交所	BSE	L	2025-06-17 15:57:24.280455
836270	天铭科技	836270	北交所	BSE	L	2025-06-17 15:57:24.280455
836395	朗鸿科技	836395	北交所	BSE	L	2025-06-17 15:57:24.280455
836414	欧普泰	836414	北交所	BSE	L	2025-06-17 15:57:24.280455
836419	万德股份	836419	北交所	BSE	L	2025-06-17 15:57:24.280455
836422	润普食品	836422	北交所	BSE	L	2025-06-17 15:57:24.280455
836433	大唐药业	836433	北交所	BSE	L	2025-06-17 15:57:24.280455
836504	博迅生物	836504	北交所	BSE	L	2025-06-17 15:57:24.280455
836547	无锡晶海	836547	北交所	BSE	L	2025-06-17 15:57:24.280455
836675	秉扬科技	836675	北交所	BSE	L	2025-06-17 15:57:24.280455
836699	海达尔	836699	北交所	BSE	L	2025-06-17 15:57:24.280455
836717	瑞星股份	836717	北交所	BSE	L	2025-06-17 15:57:24.280455
836720	吉冈精密	836720	北交所	BSE	L	2025-06-17 15:57:24.280455
836807	奔朗新材	836807	北交所	BSE	L	2025-06-17 15:57:24.280455
836826	盖世食品	836826	北交所	BSE	L	2025-06-17 15:57:24.280455
836871	派特尔	836871	北交所	BSE	L	2025-06-17 15:57:24.280455
836892	广咨国际	836892	北交所	BSE	L	2025-06-17 15:57:24.280455
836942	恒立钻具	836942	北交所	BSE	L	2025-06-17 15:57:24.280455
836957	汉维科技	836957	北交所	BSE	L	2025-06-17 15:57:24.280455
836961	西磁科技	836961	北交所	BSE	L	2025-06-17 15:57:24.280455
837006	晟楠科技	837006	北交所	BSE	L	2025-06-17 15:57:24.280455
837023	芭薇股份	837023	北交所	BSE	L	2025-06-17 15:57:24.280455
837046	亿能电力	837046	北交所	BSE	L	2025-06-17 15:57:24.280455
837092	汉鑫科技	837092	北交所	BSE	L	2025-06-17 15:57:24.280455
837174	宏裕包材	837174	北交所	BSE	L	2025-06-17 15:57:24.280455
837212	智新电子	837212	北交所	BSE	L	2025-06-17 15:57:24.280455
837242	建邦科技	837242	北交所	BSE	L	2025-06-17 15:57:24.280455
837344	三元基因	837344	北交所	BSE	L	2025-06-17 15:57:24.280455
837403	康农种业	837403	北交所	BSE	L	2025-06-17 15:57:24.280455
837592	华信永道	837592	北交所	BSE	L	2025-06-17 15:57:24.280455
837663	明阳科技	837663	北交所	BSE	L	2025-06-17 15:57:24.280455
837748	路桥信息	837748	北交所	BSE	L	2025-06-17 15:57:24.280455
837821	则成电子	837821	北交所	BSE	L	2025-06-17 15:57:24.280455
838030	德众汽车	838030	北交所	BSE	L	2025-06-17 15:57:24.280455
838163	方大新材	838163	北交所	BSE	L	2025-06-17 15:57:24.280455
838171	邦德股份	838171	北交所	BSE	L	2025-06-17 15:57:24.280455
838227	美登科技	838227	北交所	BSE	L	2025-06-17 15:57:24.280455
838262	太湖雪	838262	北交所	BSE	L	2025-06-17 15:57:24.280455
838275	驱动力	838275	北交所	BSE	L	2025-06-17 15:57:24.280455
838402	硅烷科技	838402	北交所	BSE	L	2025-06-17 15:57:24.280455
838670	恒进感应	838670	北交所	BSE	L	2025-06-17 15:57:24.280455
838701	豪声电子	838701	北交所	BSE	L	2025-06-17 15:57:24.280455
838810	春光智能	838810	北交所	BSE	L	2025-06-17 15:57:24.280455
838837	华原股份	838837	北交所	BSE	L	2025-06-17 15:57:24.280455
838924	广脉科技	838924	北交所	BSE	L	2025-06-17 15:57:24.280455
838971	天马新材	838971	北交所	BSE	L	2025-06-17 15:57:24.280455
839273	一致魔芋	839273	北交所	BSE	L	2025-06-17 15:57:24.280455
839371	欧福蛋业	839371	北交所	BSE	L	2025-06-17 15:57:24.280455
839493	并行科技	839493	北交所	BSE	L	2025-06-17 15:57:24.280455
839719	宁新新材	839719	北交所	BSE	L	2025-06-17 15:57:24.280455
839725	惠丰钻石	839725	北交所	BSE	L	2025-06-17 15:57:24.280455
839729	永顺生物	839729	北交所	BSE	L	2025-06-17 15:57:24.280455
839790	联迪信息	839790	北交所	BSE	L	2025-06-17 15:57:24.280455
839792	东和新材	839792	北交所	BSE	L	2025-06-17 15:57:24.280455
839946	华阳变速	839946	北交所	BSE	L	2025-06-17 15:57:24.280455
870199	倍益康	870199	北交所	BSE	L	2025-06-17 15:57:24.280455
870204	沪江材料	870204	北交所	BSE	L	2025-06-17 15:57:24.280455
870299	灿能电力	870299	北交所	BSE	L	2025-06-17 15:57:24.280455
870357	雅葆轩	870357	北交所	BSE	L	2025-06-17 15:57:24.280455
870436	大地电气	870436	北交所	BSE	L	2025-06-17 15:57:24.280455
870508	丰安股份	870508	北交所	BSE	L	2025-06-17 15:57:24.280455
870656	海昇药业	870656	北交所	BSE	L	2025-06-17 15:57:24.280455
870726	鸿智科技	870726	北交所	BSE	L	2025-06-17 15:57:24.280455
870866	绿亨科技	870866	北交所	BSE	L	2025-06-17 15:57:24.280455
870976	视声智能	870976	北交所	BSE	L	2025-06-17 15:57:24.280455
871245	威博液压	871245	北交所	BSE	L	2025-06-17 15:57:24.280455
871263	莱赛激光	871263	北交所	BSE	L	2025-06-17 15:57:24.280455
871396	常辅股份	871396	北交所	BSE	L	2025-06-17 15:57:24.280455
871478	巨能股份	871478	北交所	BSE	L	2025-06-17 15:57:24.280455
871553	凯腾精工	871553	北交所	BSE	L	2025-06-17 15:57:24.280455
871634	新威凌	871634	北交所	BSE	L	2025-06-17 15:57:24.280455
871642	通易航天	871642	北交所	BSE	L	2025-06-17 15:57:24.280455
871694	中裕科技	871694	北交所	BSE	L	2025-06-17 15:57:24.280455
871753	天纺标	871753	北交所	BSE	L	2025-06-17 15:57:24.280455
871857	泓禧科技	871857	北交所	BSE	L	2025-06-17 15:57:24.280455
871970	大禹生物	871970	北交所	BSE	L	2025-06-17 15:57:24.280455
871981	晶赛科技	871981	北交所	BSE	L	2025-06-17 15:57:24.280455
872190	雷神科技	872190	北交所	BSE	L	2025-06-17 15:57:24.280455
872351	华光源海	872351	北交所	BSE	L	2025-06-17 15:57:24.280455
872374	云里物里	872374	北交所	BSE	L	2025-06-17 15:57:24.280455
872392	佳合科技	872392	北交所	BSE	L	2025-06-17 15:57:24.280455
872541	铁大科技	872541	北交所	BSE	L	2025-06-17 15:57:24.280455
872808	曙光数创	872808	北交所	BSE	L	2025-06-17 15:57:24.280455
872895	花溪科技	872895	北交所	BSE	L	2025-06-17 15:57:24.280455
872925	锦好医疗	872925	北交所	BSE	L	2025-06-17 15:57:24.280455
872931	无锡鼎邦	872931	北交所	BSE	L	2025-06-17 15:57:24.280455
872953	国子软件	872953	北交所	BSE	L	2025-06-17 15:57:24.280455
873001	纬达光电	873001	北交所	BSE	L	2025-06-17 15:57:24.280455
873122	中纺标	873122	北交所	BSE	L	2025-06-17 15:57:24.280455
873132	泰鹏智能	873132	北交所	BSE	L	2025-06-17 15:57:24.280455
873152	天宏锂电	873152	北交所	BSE	L	2025-06-17 15:57:24.280455
873167	新赣江	873167	北交所	BSE	L	2025-06-17 15:57:24.280455
873169	七丰精工	873169	北交所	BSE	L	2025-06-17 15:57:24.280455
873223	荣亿精密	873223	北交所	BSE	L	2025-06-17 15:57:24.280455
873305	九菱科技	873305	北交所	BSE	L	2025-06-17 15:57:24.280455
873339	恒太照明	873339	北交所	BSE	L	2025-06-17 15:57:24.280455
873527	夜光明	873527	北交所	BSE	L	2025-06-17 15:57:24.280455
873570	坤博精工	873570	北交所	BSE	L	2025-06-17 15:57:24.280455
873576	天力复合	873576	北交所	BSE	L	2025-06-17 15:57:24.280455
873593	鼎智科技	873593	北交所	BSE	L	2025-06-17 15:57:24.280455
873665	科强股份	873665	北交所	BSE	L	2025-06-17 15:57:24.280455
873679	前进科技	873679	北交所	BSE	L	2025-06-17 15:57:24.280455
873690	捷众科技	873690	北交所	BSE	L	2025-06-17 15:57:24.280455
873693	阿为特	873693	北交所	BSE	L	2025-06-17 15:57:24.280455
873703	广厦环能	873703	北交所	BSE	L	2025-06-17 15:57:24.280455
873706	铁拓机械	873706	北交所	BSE	L	2025-06-17 15:57:24.280455
873726	卓兆点胶	873726	北交所	BSE	L	2025-06-17 15:57:24.280455
873806	云星宇	873806	北交所	BSE	L	2025-06-17 15:57:24.280455
873833	美心翼申	873833	北交所	BSE	L	2025-06-17 15:57:24.280455
920002	万达轴承	920002	其他	OTHER	L	2025-06-17 15:57:24.280455
920008	成电光信	920008	其他	OTHER	L	2025-06-17 15:57:24.280455
920016	中草香料	920016	其他	OTHER	L	2025-06-17 15:57:24.280455
920019	铜冠矿建	920019	其他	OTHER	L	2025-06-17 15:57:24.280455
920027	交大铁发	920027	其他	OTHER	L	2025-06-17 15:57:24.280455
920029	开发科技	920029	其他	OTHER	L	2025-06-17 15:57:24.280455
920060	万源通	920060	其他	OTHER	L	2025-06-17 15:57:24.280455
920066	科拜尔	920066	其他	OTHER	L	2025-06-17 15:57:24.280455
920068	天工股份	920068	其他	OTHER	L	2025-06-17 15:57:24.280455
920082	方正阀门	920082	其他	OTHER	L	2025-06-17 15:57:24.280455
920088	科力股份	920088	其他	OTHER	L	2025-06-17 15:57:24.280455
920098	科隆新材	920098	其他	OTHER	L	2025-06-17 15:57:24.280455
920099	瑞华技术	920099	其他	OTHER	L	2025-06-17 15:57:24.280455
920106	林泰新材	920106	其他	OTHER	L	2025-06-17 15:57:24.280455
920108	宏海科技	920108	其他	OTHER	L	2025-06-17 15:57:24.280455
920111	聚星科技	920111	其他	OTHER	L	2025-06-17 15:57:24.280455
920116	星图测控	920116	其他	OTHER	L	2025-06-17 15:57:24.280455
920118	太湖远大	920118	其他	OTHER	L	2025-06-17 15:57:24.280455
920128	胜业电气	920128	其他	OTHER	L	2025-06-17 15:57:24.280455
920167	同享科技	920167	其他	OTHER	L	2025-06-17 15:57:24.280455
920445	龙竹科技	920445	其他	OTHER	L	2025-06-17 15:57:24.280455
920489	佳先股份	920489	其他	OTHER	L	2025-06-17 15:57:24.280455
920682	球冠电缆	920682	其他	OTHER	L	2025-06-17 15:57:24.280455
920799	艾融软件	920799	其他	OTHER	L	2025-06-17 15:57:24.280455
920819	颖泰生物	920819	其他	OTHER	L	2025-06-17 15:57:24.280455
\.


--
-- TOC entry 3636 (class 0 OID 16485)
-- Dependencies: 217
-- Data for Name: stock_daily_quotes; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.stock_daily_quotes (id, ts_code, trade_date, open_price, high_price, low_price, close_price, pre_close, change_amount, pct_chg, vol, amount, created_at, updated_at) FROM stdin;
1	000001	2025-06-17	11.790	11.880	11.710	11.760	0.000	-0.030	-0.250	890027	1048960748.80	2025-06-17 16:05:53.82243	2025-06-17 16:13:45.19649
2	000002	2025-06-17	6.570	6.580	6.510	6.530	0.000	-0.040	-0.610	582391	380340184.44	2025-06-17 16:05:54.017319	2025-06-17 16:13:45.373043
3	000006	2025-06-17	6.780	6.920	6.750	6.820	0.000	0.030	0.440	197343	134946850.20	2025-06-17 16:05:54.202183	2025-06-17 16:13:45.549719
4	000007	2025-06-17	7.890	7.990	7.840	7.930	0.000	0.070	0.890	37560	29713008.00	2025-06-17 16:05:54.390908	2025-06-17 16:13:45.727671
5	000008	2025-06-17	2.760	2.830	2.750	2.820	0.000	0.050	1.810	486010	135417055.40	2025-06-17 16:05:54.581642	2025-06-17 16:13:45.911743
6	000009	2025-06-17	8.110	8.130	8.020	8.100	0.000	0.010	0.120	114753	92846294.09	2025-06-17 16:05:54.771357	2025-06-17 16:13:46.098471
7	000010	2025-06-17	3.140	3.150	3.050	3.080	0.000	-0.060	-1.910	207592	63826850.00	2025-06-17 16:05:54.950393	2025-06-17 16:13:46.276643
8	000011	2025-06-17	8.500	8.540	8.460	8.500	0.000	0.010	0.120	21442	18220415.00	2025-06-17 16:05:55.117496	2025-06-17 16:13:46.452331
9	000012	2025-06-17	4.600	4.620	4.580	4.600	0.000	0.010	0.220	47189	21689614.24	2025-06-17 16:05:55.301927	2025-06-17 16:13:46.638206
10	000014	2025-06-17	11.520	11.770	11.430	11.620	0.000	0.040	0.350	108954	126419297.20	2025-06-17 16:05:55.502047	2025-06-17 16:13:46.82373
11	000001	2025-06-16	11.570	11.790	11.530	11.790	0.000	0.210	1.810	1174438	1371041708.41	2025-06-17 16:05:56.679637	2025-06-17 16:13:48.013538
12	000002	2025-06-16	6.460	6.630	6.450	6.570	0.000	0.130	2.020	1302348	852638648.59	2025-06-17 16:05:56.852647	2025-06-17 16:13:48.196477
13	000006	2025-06-16	6.910	6.910	6.750	6.790	0.000	-0.080	-1.160	231796	157604199.74	2025-06-17 16:05:57.030684	2025-06-17 16:13:48.37059
14	000007	2025-06-16	7.930	8.060	7.840	7.860	0.000	-0.080	-1.010	44910	35655636.37	2025-06-17 16:05:57.198229	2025-06-17 16:13:48.545669
15	000008	2025-06-16	2.750	2.770	2.750	2.770	0.000	0.010	0.360	262828	72570160.00	2025-06-17 16:05:57.372058	2025-06-17 16:13:48.726021
16	000009	2025-06-16	8.050	8.100	7.990	8.090	0.000	-0.030	-0.370	125415	101174853.65	2025-06-17 16:05:57.55111	2025-06-17 16:13:48.916024
17	000010	2025-06-16	3.130	3.180	3.110	3.140	0.000	-0.010	-0.320	208664	65645446.13	2025-06-17 16:05:57.720284	2025-06-17 16:13:50.092669
18	000011	2025-06-16	8.400	8.630	8.360	8.490	0.000	0.040	0.470	38693	32939092.00	2025-06-17 16:05:57.8939	2025-06-17 16:13:50.266998
19	000012	2025-06-16	4.550	4.600	4.540	4.590	0.000	0.020	0.440	76600	35070845.87	2025-06-17 16:05:58.071807	2025-06-17 16:13:50.450817
20	000014	2025-06-16	11.650	11.770	11.510	11.580	0.000	-0.060	-0.520	109964	127597646.00	2025-06-17 16:05:58.246558	2025-06-17 16:13:50.643582
\.


--
-- TOC entry 3652 (class 0 OID 16578)
-- Dependencies: 233
-- Data for Name: system_logs; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.system_logs (id, log_level, module, message, details, created_at) FROM stdin;
\.


--
-- TOC entry 3640 (class 0 OID 16505)
-- Dependencies: 221
-- Data for Name: technical_daily_profiles; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.technical_daily_profiles (id, ts_code, trade_date, trend_score, momentum_score, volume_health_score, pattern_score, support_level, resistance_level, key_patterns, technical_indicators, created_at) FROM stdin;
\.


--
-- TOC entry 3659 (class 0 OID 24820)
-- Dependencies: 240
-- Data for Name: technical_indicators_daily; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.technical_indicators_daily (id, ts_code, trade_date, ma5, ma10, ma20, ma60, rsi, macd, macd_signal, macd_hist, bollinger_upper, bollinger_middle, bollinger_lower, kdj_k, kdj_d, kdj_j, created_at, updated_at) FROM stdin;
1	000001	2025-05-19	10.4164	43.9904	16.8035	21.7882	74.6242	-0.8719	-1.3030	-0.0638	59.7424	41.3249	39.9291	46.8765	58.1555	5.0272	2025-06-17 17:06:58.521124	2025-06-17 17:06:58.521124
2	000001	2025-05-20	13.9170	27.2386	27.0141	19.5730	20.3983	-0.9111	0.9405	-0.2950	55.2921	48.2913	30.6612	64.5053	66.9692	50.9028	2025-06-17 17:06:58.52558	2025-06-17 17:06:58.52558
3	000001	2025-05-21	18.9074	44.9916	18.2043	21.8984	37.0528	-1.8435	-1.7089	0.3842	59.8181	49.8549	34.8156	72.3761	24.9193	9.9067	2025-06-17 17:06:58.528458	2025-06-17 17:06:58.528458
4	000001	2025-05-22	19.6324	32.5906	46.7153	32.6114	37.7873	0.3695	1.5244	0.0990	53.2711	48.0920	32.2842	34.9178	70.7226	48.9500	2025-06-17 17:06:58.530932	2025-06-17 17:06:58.530932
5	000001	2025-05-23	15.2148	38.6544	22.6979	14.5081	21.0504	-1.7091	0.6178	-0.8302	59.8452	43.2589	36.2501	79.1752	48.7167	0.9422	2025-06-17 17:06:58.534598	2025-06-17 17:06:58.534598
6	000001	2025-05-26	46.3270	37.8034	18.6370	17.3479	62.9947	1.7390	-1.0911	-0.7347	59.6432	44.1576	34.6203	76.5207	55.8437	67.3668	2025-06-17 17:06:58.536582	2025-06-17 17:06:58.536582
7	000001	2025-05-27	37.0803	27.0072	30.5443	11.5074	72.5829	1.7630	1.6114	-0.2970	57.4098	43.7599	35.7747	70.0316	43.0258	42.8379	2025-06-17 17:06:58.538466	2025-06-17 17:06:58.538466
8	000001	2025-05-28	44.0422	26.2470	16.6036	10.4485	65.0900	-0.5480	0.9966	0.4744	57.3678	44.4971	33.8209	41.9013	75.7890	99.5555	2025-06-17 17:06:58.539764	2025-06-17 17:06:58.539764
9	000001	2025-05-29	37.8424	26.3271	17.1278	22.0695	66.0866	0.6830	-1.4052	0.1311	57.4023	47.4860	36.5090	45.5115	74.0698	84.1382	2025-06-17 17:06:58.540942	2025-06-17 17:06:58.540942
10	000001	2025-05-30	35.0709	14.4018	31.6836	42.5860	53.1560	-0.8117	-1.9825	-0.4138	57.2131	45.2929	34.2591	42.8304	36.1079	64.7193	2025-06-17 17:06:58.541919	2025-06-17 17:06:58.541919
11	000001	2025-06-02	15.0579	33.2253	23.6532	26.9481	29.1145	0.2161	0.5518	-0.2779	52.5304	40.9881	32.9662	56.2310	53.9424	41.8647	2025-06-17 17:06:58.542937	2025-06-17 17:06:58.542937
12	000001	2025-06-03	42.4619	25.4469	49.9711	31.7535	34.2571	-0.2857	-0.8146	0.8985	53.4311	42.4275	39.9391	43.0145	45.0881	2.7150	2025-06-17 17:06:58.543895	2025-06-17 17:06:58.543895
13	000001	2025-06-04	29.9390	17.2776	41.2856	49.4004	42.9442	1.4072	0.6631	-0.7935	57.4521	40.8997	35.2515	49.2015	39.7810	81.7905	2025-06-17 17:06:58.544884	2025-06-17 17:06:58.544884
14	000001	2025-06-05	48.9712	42.4246	17.2664	18.9211	61.6625	-0.1622	1.4043	0.0792	51.0037	42.4845	37.5024	43.0979	75.1124	25.7255	2025-06-17 17:06:58.54585	2025-06-17 17:06:58.54585
15	000001	2025-06-06	21.7815	20.4971	25.1286	12.4715	46.0358	1.1613	-1.7608	0.3081	50.4687	42.6100	36.1882	47.0972	49.7901	10.9245	2025-06-17 17:06:58.546829	2025-06-17 17:06:58.546829
16	000001	2025-06-09	32.0557	11.7012	23.3343	48.4532	53.5393	-0.3454	1.6603	0.7741	50.6835	40.6748	30.3465	69.5115	66.4813	81.2157	2025-06-17 17:06:58.547753	2025-06-17 17:06:58.547753
17	000001	2025-06-10	11.2457	26.6654	36.4914	30.9220	69.6680	0.6785	0.3184	-0.5492	53.3955	41.3638	38.4134	22.6570	20.0290	1.6055	2025-06-17 17:06:58.548654	2025-06-17 17:06:58.548654
18	000001	2025-06-11	43.8923	28.0292	27.0190	37.7254	20.8720	-1.1526	-1.1127	0.1780	51.8481	43.0014	32.8389	58.4609	69.7015	37.1111	2025-06-17 17:06:58.549567	2025-06-17 17:06:58.549567
19	000001	2025-06-12	12.3902	33.9533	27.1930	48.9856	21.0802	-0.6822	-1.5499	0.4934	51.7740	45.2210	35.2738	20.9405	60.1849	19.8439	2025-06-17 17:06:58.550419	2025-06-17 17:06:58.550419
20	000001	2025-06-13	24.9422	22.4900	29.9872	49.0368	57.4836	0.3656	0.3957	-0.0491	57.2937	44.2884	39.5813	67.0552	52.1250	61.9217	2025-06-17 17:06:58.552586	2025-06-17 17:06:58.552586
21	000001	2025-06-16	49.3418	29.5424	47.7699	44.0793	25.6089	1.2214	-0.4948	0.8396	58.0003	48.6997	32.7639	21.2437	41.3304	24.5400	2025-06-17 17:06:58.553572	2025-06-17 17:06:58.553572
22	000001	2025-06-17	16.5019	37.8211	21.8034	37.4392	69.8239	-0.6891	0.0741	0.4315	54.3452	43.0546	33.7288	30.1376	57.3295	76.2352	2025-06-17 17:06:58.554612	2025-06-17 17:06:58.554612
23	000001	2025-06-18	13.6279	24.3983	27.9419	10.3184	23.8238	-0.2300	-0.2111	-0.2361	53.4333	41.4094	39.9226	75.9457	27.6861	57.3533	2025-06-17 17:06:58.555933	2025-06-17 17:06:58.555933
24	000002	2025-05-19	14.0458	20.9197	38.3607	24.4956	44.1322	-1.9312	0.9078	-0.1474	58.6840	49.7750	33.0251	32.0583	46.0750	27.7828	2025-06-17 17:06:58.556907	2025-06-17 17:06:58.556907
25	000002	2025-05-20	13.8225	29.8122	46.5572	25.1330	33.7562	-1.0951	-1.7008	0.1253	57.2387	44.8896	32.3465	47.7503	30.8530	29.2107	2025-06-17 17:06:58.557878	2025-06-17 17:06:58.557878
26	000002	2025-05-21	47.6287	13.3322	24.4795	37.6578	61.9486	1.9747	0.9509	0.6962	58.3544	49.7582	39.9646	57.7028	50.2858	67.8318	2025-06-17 17:06:58.558875	2025-06-17 17:06:58.558875
27	000002	2025-05-22	25.2999	49.3890	36.5454	34.6856	67.3770	-1.9366	-0.3420	0.4077	52.6372	43.4448	32.0869	44.9751	37.1665	83.1052	2025-06-17 17:06:58.55986	2025-06-17 17:06:58.55986
28	000002	2025-05-23	15.4887	11.5014	20.5530	30.9161	50.8130	0.2102	0.5354	-0.3366	52.5536	44.7530	39.8307	78.3808	61.3247	23.7594	2025-06-17 17:06:58.560771	2025-06-17 17:06:58.560771
29	000002	2025-05-26	19.3480	46.5143	14.7202	49.4457	25.9794	-0.8602	1.1268	-0.8022	58.6291	44.5327	37.0599	77.3577	20.6564	4.5390	2025-06-17 17:06:58.561724	2025-06-17 17:06:58.561724
30	000002	2025-05-27	29.0719	25.7971	31.3174	24.4488	28.9438	-1.1148	1.3550	0.3473	55.5567	49.5459	30.0470	21.8364	67.8275	74.3145	2025-06-17 17:06:58.562672	2025-06-17 17:06:58.562672
31	000002	2025-05-28	24.1251	17.7786	16.3889	17.7757	71.4767	1.6142	0.5006	0.4009	56.6310	41.0521	39.9664	44.0466	70.8716	67.3066	2025-06-17 17:06:58.563568	2025-06-17 17:06:58.563568
32	000002	2025-05-29	31.8945	17.1844	35.1688	44.9449	26.8607	-0.9569	-0.3076	-0.9282	50.5839	49.4577	37.2328	20.0545	41.4016	58.0188	2025-06-17 17:06:58.564473	2025-06-17 17:06:58.564473
33	000002	2025-05-30	21.1072	20.1878	34.0831	14.7619	59.3309	0.8759	1.0370	-0.6967	57.0299	42.6703	34.8742	61.2254	73.5200	52.5823	2025-06-17 17:06:58.565448	2025-06-17 17:06:58.565448
34	000002	2025-06-02	23.3562	38.1212	40.2973	15.5673	76.6986	-1.1812	-1.1530	0.3165	52.8039	45.7829	32.6943	48.3251	31.3434	65.5646	2025-06-17 17:06:58.566387	2025-06-17 17:06:58.566387
35	000002	2025-06-03	10.6433	41.3150	38.0968	15.2320	55.0077	-0.4342	1.7956	-0.0674	53.0932	44.8432	32.7950	31.4357	67.6660	27.8774	2025-06-17 17:06:58.567318	2025-06-17 17:06:58.567318
36	000002	2025-06-04	37.0487	38.9391	47.7269	33.5972	25.1057	-1.5026	-1.0014	-0.9221	50.7694	43.2348	35.1673	35.0231	27.2242	43.8770	2025-06-17 17:06:58.568247	2025-06-17 17:06:58.568247
37	000002	2025-06-05	40.1442	34.9675	49.2709	49.7002	37.9948	-1.2532	-0.3675	-0.4856	55.0918	43.6847	30.2317	66.9964	35.0995	95.4076	2025-06-17 17:06:58.569141	2025-06-17 17:06:58.569141
38	000002	2025-06-06	20.8673	14.5398	16.3612	48.8614	40.8229	1.2249	0.6015	-0.7848	59.7909	44.2190	39.4037	78.2175	68.8618	57.5282	2025-06-17 17:06:58.570082	2025-06-17 17:06:58.570082
39	000002	2025-06-09	40.8718	18.0858	48.1629	17.6230	64.9113	-1.6269	1.9291	0.2929	59.4807	47.8276	35.6639	75.0525	76.0568	47.3074	2025-06-17 17:06:58.571184	2025-06-17 17:06:58.571184
40	000002	2025-06-10	38.3139	16.8531	40.8797	21.1348	49.7107	-1.5425	-0.8592	-0.8003	53.5008	45.7386	38.1520	39.2289	67.7005	48.0968	2025-06-17 17:06:58.572385	2025-06-17 17:06:58.572385
41	000002	2025-06-11	15.3449	40.0545	30.5800	44.2012	57.0213	-1.0071	1.7728	-0.8880	59.0427	49.8798	34.2633	65.7185	31.4781	18.6329	2025-06-17 17:06:58.573388	2025-06-17 17:06:58.573388
42	000002	2025-06-12	38.3801	47.5334	47.7726	41.3282	38.6607	0.2143	-0.7213	-0.7800	52.9113	42.0235	36.4026	40.5638	37.7853	55.0549	2025-06-17 17:06:58.574243	2025-06-17 17:06:58.574243
43	000002	2025-06-13	14.3441	21.2306	34.7452	46.0249	55.3283	-1.5117	-1.0329	-0.8074	52.1615	41.8463	31.9556	39.4934	74.7075	80.8637	2025-06-17 17:06:58.575111	2025-06-17 17:06:58.575111
44	000002	2025-06-16	18.2876	41.6896	33.7091	48.6308	63.6163	-0.0638	0.2052	-0.1226	55.3560	47.2164	32.4849	76.2769	57.9302	82.9224	2025-06-17 17:06:58.576042	2025-06-17 17:06:58.576042
45	000002	2025-06-17	36.3510	30.8200	36.9037	17.5030	41.5011	1.9396	0.5252	-0.6394	52.5107	46.0078	30.1969	71.6118	67.0736	27.0050	2025-06-17 17:06:58.576965	2025-06-17 17:06:58.576965
46	000002	2025-06-18	25.5690	45.8745	43.6712	28.4074	21.2140	-0.5159	-1.9131	0.1160	51.8006	46.9633	31.2160	46.7461	21.5529	96.3586	2025-06-17 17:06:58.577886	2025-06-17 17:06:58.577886
47	000006	2025-05-19	13.4290	22.6963	20.3928	44.8464	48.9831	0.7118	1.5321	0.2533	50.7766	43.9786	36.5278	45.5864	28.4824	50.9679	2025-06-17 17:06:58.578791	2025-06-17 17:06:58.578791
48	000006	2025-05-20	49.5431	42.6794	43.0280	27.1789	52.8305	-1.0037	-0.3759	-0.7991	51.9198	41.0962	36.8015	47.5202	28.7473	78.8829	2025-06-17 17:06:58.57972	2025-06-17 17:06:58.57972
49	000006	2025-05-21	22.5585	19.2352	13.3650	17.4998	78.5510	-0.0660	0.7648	0.2598	52.5010	46.6602	33.0632	52.2982	45.3738	95.5685	2025-06-17 17:06:58.580682	2025-06-17 17:06:58.580682
50	000006	2025-05-22	29.3235	34.7061	33.7417	23.2247	63.2869	-0.7152	-0.7239	-0.4021	57.0348	43.9271	31.3386	32.5872	73.6883	9.7481	2025-06-17 17:06:58.58163	2025-06-17 17:06:58.58163
51	000006	2025-05-23	19.7284	35.2680	16.9901	40.0988	71.8140	-0.6297	-1.0827	0.2108	55.2899	48.1949	33.7644	71.8747	36.3063	63.2623	2025-06-17 17:06:58.582521	2025-06-17 17:06:58.582521
52	000006	2025-05-26	11.1018	15.5034	15.3987	20.7503	30.9134	0.3325	-1.9785	0.6760	53.0436	42.2781	31.7905	45.8826	65.0124	41.0837	2025-06-17 17:06:58.583467	2025-06-17 17:06:58.583467
53	000006	2025-05-27	36.3623	44.5711	44.4703	36.7392	69.1865	0.8688	1.8914	-0.8251	51.4767	43.9384	30.4844	31.5025	64.1565	84.7752	2025-06-17 17:06:58.584404	2025-06-17 17:06:58.584404
54	000006	2025-05-28	26.9767	27.3530	24.4304	31.7638	44.3478	-0.8491	1.8360	-0.0507	52.7088	40.5811	39.1311	55.5855	61.3327	73.2618	2025-06-17 17:06:58.585424	2025-06-17 17:06:58.585424
55	000006	2025-05-29	16.3733	41.3260	32.9557	29.5621	59.4613	1.7871	-0.9182	0.7809	50.0649	49.2126	34.5181	48.5908	63.0053	9.7125	2025-06-17 17:06:58.586323	2025-06-17 17:06:58.586323
56	000006	2025-05-30	26.1891	49.9837	10.6711	11.9158	46.0704	0.1869	-0.1143	-0.9749	50.8520	44.9066	32.5987	42.9579	70.8427	35.1675	2025-06-17 17:06:58.587231	2025-06-17 17:06:58.587231
57	000006	2025-06-02	42.9210	15.9555	12.9946	34.2693	49.0018	-1.5478	-1.3584	0.6570	57.1437	46.6914	30.6676	52.5374	58.1933	8.7058	2025-06-17 17:06:58.588307	2025-06-17 17:06:58.588307
58	000006	2025-06-03	45.4236	27.6599	13.6562	25.5172	24.4890	-0.2346	1.3487	0.8382	56.5193	43.8494	33.1141	49.5586	36.7195	70.7618	2025-06-17 17:06:58.589242	2025-06-17 17:06:58.589242
59	000006	2025-06-04	18.3305	30.0189	12.9067	28.4564	79.3688	0.6298	-0.4038	0.5193	55.8543	41.7802	30.5518	44.0858	43.1056	52.0112	2025-06-17 17:06:58.591699	2025-06-17 17:06:58.591699
60	000006	2025-06-05	16.8188	12.5961	44.4997	25.1435	39.9173	-1.2504	-0.0094	-0.4116	50.7157	49.5791	38.2607	59.1787	48.8108	9.9831	2025-06-17 17:06:58.592967	2025-06-17 17:06:58.592967
61	000006	2025-06-06	43.0917	25.8304	10.5737	26.7431	76.8538	1.0447	0.6443	0.4412	50.6193	42.4752	39.0873	68.0701	70.1993	6.0967	2025-06-17 17:06:58.593819	2025-06-17 17:06:58.593819
62	000006	2025-06-09	18.0465	15.4329	19.0059	11.7871	71.0725	1.7196	1.4276	-0.6487	54.6480	41.2467	30.4207	21.4110	65.9661	77.8955	2025-06-17 17:06:58.594708	2025-06-17 17:06:58.594708
63	000006	2025-06-10	38.4506	17.6467	37.9730	30.5010	24.3359	-1.7398	1.8471	-0.8624	50.8444	49.6029	35.4846	25.8006	77.0347	68.6091	2025-06-17 17:06:58.595817	2025-06-17 17:06:58.595817
64	000006	2025-06-11	19.0381	38.5052	34.6913	10.6887	33.6140	1.6725	-0.5565	-0.7370	56.9609	42.3466	32.4128	78.2584	22.9237	47.1502	2025-06-17 17:06:58.596672	2025-06-17 17:06:58.596672
65	000006	2025-06-12	17.5356	27.3986	46.6097	30.1574	60.3646	1.3069	-1.5594	0.7033	52.6863	43.4498	31.5426	27.1164	74.2177	22.3623	2025-06-17 17:06:58.597561	2025-06-17 17:06:58.597561
66	000006	2025-06-13	15.0026	39.6178	37.1638	19.2997	77.4926	-0.5429	-1.5814	-0.3232	51.7672	45.2154	39.0187	29.6991	62.9031	52.1561	2025-06-17 17:06:58.598525	2025-06-17 17:06:58.598525
67	000006	2025-06-16	24.3890	34.4262	43.7691	44.4787	76.6446	1.9211	-1.0431	0.4152	50.3977	43.7597	30.5005	70.4639	26.4571	54.9198	2025-06-17 17:06:58.599466	2025-06-17 17:06:58.599466
68	000006	2025-06-17	13.9322	32.1495	30.0242	24.5755	30.4858	0.6923	0.0044	-0.5760	57.1557	40.8797	39.3047	36.8193	44.5098	66.0348	2025-06-17 17:06:58.6004	2025-06-17 17:06:58.6004
69	000006	2025-06-18	11.3778	46.0446	22.7390	45.4950	34.7304	0.2960	0.6431	0.6540	59.3401	41.0974	38.2314	34.9821	48.6204	51.8471	2025-06-17 17:06:58.601316	2025-06-17 17:06:58.601316
70	000007	2025-05-19	23.2564	22.8222	14.9394	44.4527	25.3355	-1.9753	-1.9024	0.7749	57.7482	42.3961	32.5349	27.9120	48.1054	72.6028	2025-06-17 17:06:58.602244	2025-06-17 17:06:58.602244
71	000007	2025-05-20	47.2159	24.7665	41.1389	26.5418	58.5720	1.9778	-1.7981	-0.7287	50.4917	44.2615	35.4542	37.3662	62.1044	23.1664	2025-06-17 17:06:58.603164	2025-06-17 17:06:58.603164
72	000007	2025-05-21	20.2509	32.7457	16.5927	10.2613	67.1643	-1.4056	1.3390	-0.9455	52.6675	42.5138	31.5460	49.4314	42.8895	40.6396	2025-06-17 17:06:58.604146	2025-06-17 17:06:58.604146
73	000007	2025-05-22	32.9485	44.5000	33.9218	28.1736	76.2399	-1.2653	-0.2099	-0.7860	52.6899	44.7024	34.6909	31.0064	21.1656	32.9396	2025-06-17 17:06:58.605134	2025-06-17 17:06:58.605134
74	000007	2025-05-23	33.7860	31.6056	36.6631	21.7484	31.3901	0.7043	-1.0478	0.3324	54.2167	43.2315	34.0678	22.0030	41.9171	23.0781	2025-06-17 17:06:58.606056	2025-06-17 17:06:58.606056
75	000007	2025-05-26	15.6367	20.8787	15.4044	10.7325	42.5211	-1.8651	0.3808	-0.6832	58.9502	43.2080	34.7637	64.9129	42.1192	31.8399	2025-06-17 17:06:58.606985	2025-06-17 17:06:58.606985
76	000007	2025-05-27	45.1329	24.5028	13.5777	11.7677	38.2180	-0.2192	-0.5329	0.1995	51.2929	45.0347	38.7960	25.8107	49.0762	75.3162	2025-06-17 17:06:58.607906	2025-06-17 17:06:58.607906
77	000007	2025-05-28	35.4302	32.5942	25.0271	16.7860	43.5404	0.9746	1.6526	-0.6880	58.6580	40.7732	30.7827	29.2471	44.5865	28.9653	2025-06-17 17:06:58.60883	2025-06-17 17:06:58.60883
78	000007	2025-05-29	22.9619	10.7590	30.8085	31.4035	36.9729	-1.9405	1.4388	-0.6499	59.9946	44.6405	34.7079	79.6954	60.3276	35.7399	2025-06-17 17:06:58.609609	2025-06-17 17:06:58.609609
79	000007	2025-05-30	20.1734	19.9083	30.2046	46.2966	30.0116	0.8974	-0.9428	0.6069	56.8250	42.9333	39.1144	68.9810	49.1686	94.7922	2025-06-17 17:06:58.610472	2025-06-17 17:06:58.610472
80	000007	2025-06-02	22.7464	46.8030	44.7836	44.4441	23.4297	0.4965	-1.4663	0.9572	56.5865	41.5093	37.2146	25.3539	78.7628	17.4617	2025-06-17 17:06:58.611299	2025-06-17 17:06:58.611299
81	000007	2025-06-03	32.5609	12.2975	30.7658	18.6714	42.7142	1.5114	0.9164	-0.1779	59.5173	49.6812	35.9250	28.0657	44.4696	29.5217	2025-06-17 17:06:58.612162	2025-06-17 17:06:58.612162
82	000007	2025-06-04	12.3394	16.3488	37.2788	37.1227	76.4205	0.4371	1.8681	-0.4726	55.1345	40.4146	36.6783	74.5090	52.1606	58.9289	2025-06-17 17:06:58.613213	2025-06-17 17:06:58.613213
83	000007	2025-06-05	36.7970	43.6521	22.8630	17.5935	56.5163	-1.2129	-1.4748	0.2852	59.1719	47.4857	34.3213	34.1667	29.9048	27.6349	2025-06-17 17:06:58.61407	2025-06-17 17:06:58.61407
84	000007	2025-06-06	24.2245	45.5365	28.3610	47.1571	48.1414	0.3220	-1.2402	-0.9818	54.5677	44.9355	39.7598	32.7021	33.9666	39.5624	2025-06-17 17:06:58.61499	2025-06-17 17:06:58.61499
85	000007	2025-06-09	28.1530	25.9194	22.5364	24.2346	43.8617	1.4134	1.9923	0.0637	56.9036	48.8469	32.6555	39.5473	77.0918	60.5854	2025-06-17 17:06:58.615891	2025-06-17 17:06:58.615891
86	000007	2025-06-10	39.5745	43.4676	32.8167	44.5276	66.3240	0.5619	-0.7519	-0.4305	51.4071	47.6431	39.5977	59.7893	70.9918	81.3196	2025-06-17 17:06:58.616737	2025-06-17 17:06:58.616737
87	000007	2025-06-11	17.2633	31.1409	25.6369	44.5099	22.2013	0.8898	-1.0651	0.8361	52.2536	48.2222	33.4947	68.3001	49.7001	91.3926	2025-06-17 17:06:58.617714	2025-06-17 17:06:58.617714
88	000007	2025-06-12	22.7390	14.2674	26.9856	36.8500	69.1811	1.1227	-0.1321	-0.6493	59.1934	40.4596	34.3387	43.6915	73.2372	24.3769	2025-06-17 17:06:58.618642	2025-06-17 17:06:58.618642
89	000007	2025-06-13	27.0798	32.5084	27.5478	44.1988	71.2577	-1.9850	-1.8902	-0.4797	53.5108	45.0448	36.1894	31.6598	20.0686	81.3856	2025-06-17 17:06:58.619467	2025-06-17 17:06:58.619467
90	000007	2025-06-16	36.9224	24.2851	10.8713	13.6215	67.4741	-0.5386	1.3673	0.3041	53.7081	45.3030	37.7687	23.5138	63.6885	49.5245	2025-06-17 17:06:58.62234	2025-06-17 17:06:58.62234
91	000007	2025-06-17	35.6242	45.3184	40.7008	48.5314	54.8943	-0.7609	0.0887	0.2131	59.8644	43.4614	34.8683	54.5105	73.8206	21.1810	2025-06-17 17:06:58.623284	2025-06-17 17:06:58.623284
92	000007	2025-06-18	48.0668	19.0670	23.5100	16.5734	45.5024	-0.6224	0.8557	-0.6463	51.6227	47.8428	37.7360	64.3905	59.3840	4.8985	2025-06-17 17:06:58.624229	2025-06-17 17:06:58.624229
93	000008	2025-05-19	40.8751	28.3869	16.9883	28.2963	52.6887	-1.9986	1.3814	-0.6717	55.9925	46.1721	36.1894	55.4025	26.9797	91.7065	2025-06-17 17:06:58.625155	2025-06-17 17:06:58.625155
94	000008	2025-05-20	13.8214	29.8338	18.1335	46.8367	58.0763	-0.7131	1.4655	0.0701	51.2340	47.1015	30.0172	59.9109	32.4944	33.0029	2025-06-17 17:06:58.62607	2025-06-17 17:06:58.62607
95	000008	2025-05-21	29.8157	44.7142	16.6838	37.1660	69.9335	1.2039	-1.0722	0.9286	57.2796	47.3496	38.5618	42.8830	75.5455	4.0262	2025-06-17 17:06:58.626926	2025-06-17 17:06:58.626926
96	000008	2025-05-22	24.4035	37.5464	18.4810	38.0904	47.6757	-0.4251	0.5950	0.6257	54.3952	45.3183	30.7545	63.1234	63.8151	83.1084	2025-06-17 17:06:58.627859	2025-06-17 17:06:58.627859
97	000008	2025-05-23	28.6256	44.7973	42.6951	24.0443	63.7155	-0.0042	-1.0712	-0.2968	53.6585	43.6530	34.4900	41.2260	53.5137	45.8332	2025-06-17 17:06:58.628771	2025-06-17 17:06:58.628771
98	000008	2025-05-26	22.5854	21.6353	31.5838	27.9519	57.1344	-0.9537	1.6992	-0.4539	58.6248	47.0170	35.3509	34.6117	70.6022	36.3337	2025-06-17 17:06:58.629693	2025-06-17 17:06:58.629693
99	000008	2025-05-27	49.2030	45.9200	30.4488	49.8179	64.1588	-0.5977	1.2414	-0.8661	55.9466	48.6344	36.4588	44.5469	50.9723	49.5662	2025-06-17 17:06:58.630634	2025-06-17 17:06:58.630634
100	000008	2025-05-28	11.3301	21.3120	27.0022	25.9992	58.6894	-0.2670	-1.4281	-0.0793	52.1908	47.5897	31.8275	75.6195	63.9051	13.5607	2025-06-17 17:06:58.631571	2025-06-17 17:06:58.631571
101	000008	2025-05-29	22.3474	10.0676	24.1252	29.5506	66.7193	-0.7306	-1.1039	0.5757	57.0503	42.8425	39.5404	57.6264	26.2805	76.0306	2025-06-17 17:06:58.632562	2025-06-17 17:06:58.632562
102	000008	2025-05-30	42.0250	45.7668	12.9835	45.2664	41.5459	0.8102	0.2297	0.7968	53.6127	48.1640	36.5946	21.9231	55.1180	18.0719	2025-06-17 17:06:58.633677	2025-06-17 17:06:58.633677
103	000008	2025-06-02	43.2489	24.9888	10.0132	27.6603	41.2509	-1.0112	0.3706	0.6672	53.2041	47.7948	32.1687	53.4118	73.2417	13.1320	2025-06-17 17:06:58.634612	2025-06-17 17:06:58.634612
104	000008	2025-06-03	20.8401	37.7440	33.5908	20.3547	32.4846	0.6051	0.8904	0.1178	53.9977	48.9749	35.2309	24.7588	50.5124	79.0053	2025-06-17 17:06:58.635503	2025-06-17 17:06:58.635503
105	000008	2025-06-04	34.7944	15.9380	47.4950	16.8232	73.1915	-1.5870	1.8977	-0.5817	53.7205	40.6354	33.9602	41.8175	48.7623	13.3219	2025-06-17 17:06:58.636437	2025-06-17 17:06:58.636437
106	000008	2025-06-05	24.0951	14.2949	18.3018	44.4257	66.6952	-0.2905	1.2204	0.8771	56.4897	46.4777	36.3448	75.8205	41.4864	47.5875	2025-06-17 17:06:58.638709	2025-06-17 17:06:58.638709
107	000008	2025-06-06	36.6842	47.5096	49.3248	27.6486	75.0364	0.8099	0.9085	0.4653	58.6110	42.0342	39.3041	31.2352	59.7834	36.2787	2025-06-17 17:06:58.639683	2025-06-17 17:06:58.639683
108	000008	2025-06-09	14.0066	33.7879	10.1591	39.5624	31.0295	1.4562	-0.8989	0.7069	52.4946	47.2500	38.7981	26.7311	33.4400	9.7034	2025-06-17 17:06:58.640573	2025-06-17 17:06:58.640573
109	000008	2025-06-10	49.3131	31.9917	27.5221	39.9132	66.8726	1.6176	0.9660	0.4702	53.3068	42.0519	39.1525	61.6662	61.7634	94.7939	2025-06-17 17:06:58.641532	2025-06-17 17:06:58.641532
110	000008	2025-06-11	41.6913	12.0382	36.9597	17.7129	49.9798	-0.5367	0.6596	-0.8502	55.6039	47.4100	31.0506	49.5612	23.4726	73.4478	2025-06-17 17:06:58.642421	2025-06-17 17:06:58.642421
111	000008	2025-06-12	14.7301	35.2311	33.1181	45.6613	56.6095	-0.7893	0.7992	-0.3521	56.0131	47.9836	31.1050	74.2800	20.6583	84.0909	2025-06-17 17:06:58.643373	2025-06-17 17:06:58.643373
112	000008	2025-06-13	43.4723	30.6337	22.7566	37.6896	73.9971	-0.9527	-1.4505	0.6744	57.2391	47.5488	30.3434	59.2487	47.2198	84.3970	2025-06-17 17:06:58.644306	2025-06-17 17:06:58.644306
113	000008	2025-06-16	15.1643	27.2873	43.9587	38.0484	68.1794	0.9664	-0.9863	-0.9737	54.2601	46.8581	38.2625	21.7962	33.8992	3.5475	2025-06-17 17:06:58.645236	2025-06-17 17:06:58.645236
114	000008	2025-06-17	45.8032	43.5301	25.1288	12.9766	66.9128	-0.2763	1.7504	-0.0451	58.0928	47.3182	33.5725	72.6163	47.3393	45.6579	2025-06-17 17:06:58.646077	2025-06-17 17:06:58.646077
115	000008	2025-06-18	48.3903	12.2569	34.3667	40.6026	39.2130	0.1116	-0.8568	-0.6397	57.8398	48.0016	31.1346	38.6346	29.6252	75.2846	2025-06-17 17:06:58.647026	2025-06-17 17:06:58.647026
\.


--
-- TOC entry 3650 (class 0 OID 16565)
-- Dependencies: 231
-- Data for Name: trading_signals; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.trading_signals (id, ts_code, trade_date, signal_type, confidence_score, technical_score, capital_score, fundamental_score, sentiment_score, macro_score, entry_price, stop_loss, target_price, risk_reward_ratio, position_size, signal_reason, signal_data, is_active, created_at) FROM stdin;
\.


--
-- TOC entry 3685 (class 0 OID 0)
-- Dependencies: 237
-- Name: capital_flow_daily_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.capital_flow_daily_id_seq', 118, true);


--
-- TOC entry 3686 (class 0 OID 0)
-- Dependencies: 222
-- Name: capital_flow_profiles_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.capital_flow_profiles_id_seq', 1, false);


--
-- TOC entry 3687 (class 0 OID 0)
-- Dependencies: 235
-- Name: data_update_log_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.data_update_log_id_seq', 5, true);


--
-- TOC entry 3688 (class 0 OID 0)
-- Dependencies: 243
-- Name: financial_indicators_quarterly_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.financial_indicators_quarterly_id_seq', 1, false);


--
-- TOC entry 3689 (class 0 OID 0)
-- Dependencies: 224
-- Name: fundamental_profiles_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.fundamental_profiles_id_seq', 1, false);


--
-- TOC entry 3690 (class 0 OID 0)
-- Dependencies: 228
-- Name: macro_profiles_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.macro_profiles_id_seq', 1, false);


--
-- TOC entry 3691 (class 0 OID 0)
-- Dependencies: 241
-- Name: market_sentiment_daily_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.market_sentiment_daily_id_seq', 115, true);


--
-- TOC entry 3692 (class 0 OID 0)
-- Dependencies: 218
-- Name: money_flow_daily_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.money_flow_daily_id_seq', 1, false);


--
-- TOC entry 3693 (class 0 OID 0)
-- Dependencies: 226
-- Name: sentiment_profiles_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.sentiment_profiles_id_seq', 1, false);


--
-- TOC entry 3694 (class 0 OID 0)
-- Dependencies: 216
-- Name: stock_daily_quotes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.stock_daily_quotes_id_seq', 40, true);


--
-- TOC entry 3695 (class 0 OID 0)
-- Dependencies: 232
-- Name: system_logs_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.system_logs_id_seq', 1, false);


--
-- TOC entry 3696 (class 0 OID 0)
-- Dependencies: 220
-- Name: technical_daily_profiles_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.technical_daily_profiles_id_seq', 1, false);


--
-- TOC entry 3697 (class 0 OID 0)
-- Dependencies: 239
-- Name: technical_indicators_daily_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.technical_indicators_daily_id_seq', 115, true);


--
-- TOC entry 3698 (class 0 OID 0)
-- Dependencies: 230
-- Name: trading_signals_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.trading_signals_id_seq', 1, false);


--
-- TOC entry 3474 (class 2606 OID 24813)
-- Name: capital_flow_daily capital_flow_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.capital_flow_daily
    ADD CONSTRAINT capital_flow_daily_pkey PRIMARY KEY (id);


--
-- TOC entry 3477 (class 2606 OID 24815)
-- Name: capital_flow_daily capital_flow_daily_ts_code_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.capital_flow_daily
    ADD CONSTRAINT capital_flow_daily_ts_code_trade_date_key UNIQUE (ts_code, trade_date);


--
-- TOC entry 3443 (class 2606 OID 16525)
-- Name: capital_flow_profiles capital_flow_profiles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.capital_flow_profiles
    ADD CONSTRAINT capital_flow_profiles_pkey PRIMARY KEY (id);


--
-- TOC entry 3445 (class 2606 OID 16527)
-- Name: capital_flow_profiles capital_flow_profiles_ts_code_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.capital_flow_profiles
    ADD CONSTRAINT capital_flow_profiles_ts_code_trade_date_key UNIQUE (ts_code, trade_date);


--
-- TOC entry 3469 (class 2606 OID 24799)
-- Name: data_update_log data_update_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.data_update_log
    ADD CONSTRAINT data_update_log_pkey PRIMARY KEY (id);


--
-- TOC entry 3490 (class 2606 OID 24849)
-- Name: financial_indicators_quarterly financial_indicators_quarterly_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.financial_indicators_quarterly
    ADD CONSTRAINT financial_indicators_quarterly_pkey PRIMARY KEY (id);


--
-- TOC entry 3492 (class 2606 OID 24851)
-- Name: financial_indicators_quarterly financial_indicators_quarterly_ts_code_report_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.financial_indicators_quarterly
    ADD CONSTRAINT financial_indicators_quarterly_ts_code_report_date_key UNIQUE (ts_code, report_date);


--
-- TOC entry 3448 (class 2606 OID 16537)
-- Name: fundamental_profiles fundamental_profiles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fundamental_profiles
    ADD CONSTRAINT fundamental_profiles_pkey PRIMARY KEY (id);


--
-- TOC entry 3450 (class 2606 OID 16539)
-- Name: fundamental_profiles fundamental_profiles_ts_code_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fundamental_profiles
    ADD CONSTRAINT fundamental_profiles_ts_code_trade_date_key UNIQUE (ts_code, trade_date);


--
-- TOC entry 3456 (class 2606 OID 16561)
-- Name: macro_profiles macro_profiles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.macro_profiles
    ADD CONSTRAINT macro_profiles_pkey PRIMARY KEY (id);


--
-- TOC entry 3458 (class 2606 OID 16563)
-- Name: macro_profiles macro_profiles_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.macro_profiles
    ADD CONSTRAINT macro_profiles_trade_date_key UNIQUE (trade_date);


--
-- TOC entry 3486 (class 2606 OID 24838)
-- Name: market_sentiment_daily market_sentiment_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_sentiment_daily
    ADD CONSTRAINT market_sentiment_daily_pkey PRIMARY KEY (id);


--
-- TOC entry 3488 (class 2606 OID 24840)
-- Name: market_sentiment_daily market_sentiment_daily_ts_code_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_sentiment_daily
    ADD CONSTRAINT market_sentiment_daily_ts_code_trade_date_key UNIQUE (ts_code, trade_date);


--
-- TOC entry 3434 (class 2606 OID 16501)
-- Name: money_flow_daily money_flow_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.money_flow_daily
    ADD CONSTRAINT money_flow_daily_pkey PRIMARY KEY (id);


--
-- TOC entry 3436 (class 2606 OID 16503)
-- Name: money_flow_daily money_flow_daily_ts_code_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.money_flow_daily
    ADD CONSTRAINT money_flow_daily_ts_code_trade_date_key UNIQUE (ts_code, trade_date);


--
-- TOC entry 3452 (class 2606 OID 16549)
-- Name: sentiment_profiles sentiment_profiles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sentiment_profiles
    ADD CONSTRAINT sentiment_profiles_pkey PRIMARY KEY (id);


--
-- TOC entry 3454 (class 2606 OID 16551)
-- Name: sentiment_profiles sentiment_profiles_ts_code_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sentiment_profiles
    ADD CONSTRAINT sentiment_profiles_ts_code_trade_date_key UNIQUE (ts_code, trade_date);


--
-- TOC entry 3429 (class 2606 OID 16491)
-- Name: stock_daily_quotes stock_daily_quotes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stock_daily_quotes
    ADD CONSTRAINT stock_daily_quotes_pkey PRIMARY KEY (id);


--
-- TOC entry 3431 (class 2606 OID 16493)
-- Name: stock_daily_quotes stock_daily_quotes_ts_code_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stock_daily_quotes
    ADD CONSTRAINT stock_daily_quotes_ts_code_trade_date_key UNIQUE (ts_code, trade_date);


--
-- TOC entry 3467 (class 2606 OID 16586)
-- Name: system_logs system_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_logs
    ADD CONSTRAINT system_logs_pkey PRIMARY KEY (id);


--
-- TOC entry 3439 (class 2606 OID 16513)
-- Name: technical_daily_profiles technical_daily_profiles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.technical_daily_profiles
    ADD CONSTRAINT technical_daily_profiles_pkey PRIMARY KEY (id);


--
-- TOC entry 3441 (class 2606 OID 16515)
-- Name: technical_daily_profiles technical_daily_profiles_ts_code_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.technical_daily_profiles
    ADD CONSTRAINT technical_daily_profiles_ts_code_trade_date_key UNIQUE (ts_code, trade_date);


--
-- TOC entry 3482 (class 2606 OID 24827)
-- Name: technical_indicators_daily technical_indicators_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.technical_indicators_daily
    ADD CONSTRAINT technical_indicators_daily_pkey PRIMARY KEY (id);


--
-- TOC entry 3484 (class 2606 OID 24829)
-- Name: technical_indicators_daily technical_indicators_daily_ts_code_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.technical_indicators_daily
    ADD CONSTRAINT technical_indicators_daily_ts_code_trade_date_key UNIQUE (ts_code, trade_date);


--
-- TOC entry 3462 (class 2606 OID 16574)
-- Name: trading_signals trading_signals_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trading_signals
    ADD CONSTRAINT trading_signals_pkey PRIMARY KEY (id);


--
-- TOC entry 3464 (class 2606 OID 16576)
-- Name: trading_signals trading_signals_ts_code_trade_date_signal_type_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trading_signals
    ADD CONSTRAINT trading_signals_ts_code_trade_date_signal_type_key UNIQUE (ts_code, trade_date, signal_type);


--
-- TOC entry 3475 (class 1259 OID 32980)
-- Name: capital_flow_daily_trade_date_ts_code_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX capital_flow_daily_trade_date_ts_code_key ON public.capital_flow_daily USING btree (trade_date, ts_code);


--
-- TOC entry 3478 (class 1259 OID 24817)
-- Name: idx_capital_flow_trade_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_capital_flow_trade_date ON public.capital_flow_daily USING btree (trade_date);


--
-- TOC entry 3479 (class 1259 OID 24816)
-- Name: idx_capital_flow_ts_code; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_capital_flow_ts_code ON public.capital_flow_daily USING btree (ts_code);


--
-- TOC entry 3480 (class 1259 OID 24818)
-- Name: idx_capital_flow_ts_code_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_capital_flow_ts_code_date ON public.capital_flow_daily USING btree (ts_code, trade_date);


--
-- TOC entry 3446 (class 1259 OID 16590)
-- Name: idx_capital_profiles_ts_code_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_capital_profiles_ts_code_date ON public.capital_flow_profiles USING btree (ts_code, trade_date DESC);


--
-- TOC entry 3470 (class 1259 OID 24800)
-- Name: idx_data_update_log_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_data_update_log_date ON public.data_update_log USING btree (update_date);


--
-- TOC entry 3471 (class 1259 OID 24801)
-- Name: idx_data_update_log_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_data_update_log_status ON public.data_update_log USING btree (status);


--
-- TOC entry 3472 (class 1259 OID 24802)
-- Name: idx_data_update_log_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_data_update_log_time ON public.data_update_log USING btree (update_time);


--
-- TOC entry 3432 (class 1259 OID 16588)
-- Name: idx_money_flow_daily_ts_code_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_money_flow_daily_ts_code_date ON public.money_flow_daily USING btree (ts_code, trade_date DESC);


--
-- TOC entry 3427 (class 1259 OID 16587)
-- Name: idx_stock_daily_quotes_ts_code_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stock_daily_quotes_ts_code_date ON public.stock_daily_quotes USING btree (ts_code, trade_date DESC);


--
-- TOC entry 3465 (class 1259 OID 16593)
-- Name: idx_system_logs_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_system_logs_created_at ON public.system_logs USING btree (created_at DESC);


--
-- TOC entry 3437 (class 1259 OID 16589)
-- Name: idx_technical_profiles_ts_code_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_technical_profiles_ts_code_date ON public.technical_daily_profiles USING btree (ts_code, trade_date DESC);


--
-- TOC entry 3459 (class 1259 OID 16592)
-- Name: idx_trading_signals_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trading_signals_active ON public.trading_signals USING btree (is_active, trade_date DESC);


--
-- TOC entry 3460 (class 1259 OID 16591)
-- Name: idx_trading_signals_date_score; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trading_signals_date_score ON public.trading_signals USING btree (trade_date DESC, confidence_score DESC);


-- Completed on 2025-06-22 23:42:17 UTC

--
-- PostgreSQL database dump complete
--

