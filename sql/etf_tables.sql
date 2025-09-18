-- ETF Data Tables for UK Market Analysis
-- Supporting ETF arbitrage strategies from Gilt Market Analysis Guide

-- ETF NAV Historical Data
CREATE TABLE IF NOT EXISTS etf_nav_history (
    date DATE NOT NULL,
    etf_ticker VARCHAR(10) NOT NULL,
    nav DECIMAL(12, 6) NOT NULL,
    currency VARCHAR(3) DEFAULT 'GBP',
    data_source VARCHAR(50) DEFAULT 'iShares',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (date, etf_ticker),
    
    -- Indexes for performance
    INDEX idx_etf_nav_ticker (etf_ticker),
    INDEX idx_etf_nav_date (date DESC),
    INDEX idx_etf_nav_source (data_source)
);

-- ETF Current Prices (for premium/discount analysis)
CREATE TABLE IF NOT EXISTS etf_current_prices (
    etf_ticker VARCHAR(10) NOT NULL,
    price DECIMAL(12, 6) NOT NULL,
    currency VARCHAR(3) DEFAULT 'GBP',
    exchange VARCHAR(10) DEFAULT 'LSE',
    price_timestamp TIMESTAMP NOT NULL,
    data_source VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (etf_ticker, price_timestamp),
    
    -- Indexes
    INDEX idx_etf_price_ticker (etf_ticker),
    INDEX idx_etf_price_timestamp (price_timestamp DESC)
);

-- ETF Premium/Discount Analysis
CREATE TABLE IF NOT EXISTS etf_premium_discount (
    date DATE NOT NULL,
    etf_ticker VARCHAR(10) NOT NULL,
    nav DECIMAL(12, 6) NOT NULL,
    market_price DECIMAL(12, 6) NOT NULL,
    premium_discount DECIMAL(8, 4) NOT NULL, -- Percentage as decimal (0.05 = 5%)
    signal VARCHAR(10), -- BUY/SELL/HOLD based on thresholds
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (date, etf_ticker),
    
    -- Indexes
    INDEX idx_etf_premium_ticker (etf_ticker),
    INDEX idx_etf_premium_date (date DESC),
    INDEX idx_etf_premium_signal (signal)
);

-- ETF Holdings Data
CREATE TABLE IF NOT EXISTS etf_holdings (
    etf_ticker VARCHAR(10) NOT NULL,
    as_of_date DATE NOT NULL,
    holding_ticker VARCHAR(20),
    holding_name VARCHAR(200),
    sector VARCHAR(100),
    asset_class VARCHAR(50),
    market_value DECIMAL(18, 2),
    weight_percent DECIMAL(8, 4), -- Percentage as decimal (0.05 = 5%)
    shares DECIMAL(18, 2),
    par_value DECIMAL(18, 2),
    maturity_date DATE,
    currency VARCHAR(3) DEFAULT 'GBP',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (etf_ticker, as_of_date, holding_ticker),
    
    -- Indexes
    INDEX idx_etf_holdings_ticker (etf_ticker),
    INDEX idx_etf_holdings_date (as_of_date DESC),
    INDEX idx_etf_holdings_asset_class (asset_class)
);

-- ETF Metadata
CREATE TABLE IF NOT EXISTS etf_metadata (
    etf_ticker VARCHAR(10) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    currency VARCHAR(3) DEFAULT 'GBP',
    inception_date DATE,
    expense_ratio DECIMAL(6, 4), -- Percentage as decimal (0.0005 = 0.05%)
    benchmark VARCHAR(200),
    fund_size DECIMAL(18, 2), -- AUM in base currency
    provider VARCHAR(50) DEFAULT 'iShares',
    isin VARCHAR(12),
    sedol VARCHAR(7),
    exchange VARCHAR(10) DEFAULT 'LSE',
    strategy_category VARCHAR(100), -- 'Government Bonds', 'Index-Linked', etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert metadata for initial ETFs
INSERT INTO etf_metadata (etf_ticker, name, description, inception_date, provider, strategy_category) VALUES
('IGLT', 'iShares Core UK Gilts UCITS ETF', 'Broad UK government bond exposure for gilt market analysis', '2009-09-25', 'iShares', 'Government Bonds'),
('INXG', 'iShares UK Index-Linked Gilts UCITS ETF', 'UK inflation-linked government bonds for breakeven analysis', '2009-09-25', 'iShares', 'Index-Linked Bonds'),
('VGOV', 'Vanguard U.K. Gilt UCITS ETF (GBP) Distributing', 'UK government bond ETF with historical price data for arbitrage analysis', '2012-05-22', 'Vanguard', 'Government Bonds')
ON CONFLICT (etf_ticker) DO NOTHING;

-- Comments explaining the premium/discount analysis
COMMENT ON TABLE etf_premium_discount IS 'ETF premium/discount analysis supporting arbitrage strategies from Gilt Market Analysis Guide. Premium >0.5% may indicate selling opportunity, discount <-0.5% may indicate buying opportunity.';

COMMENT ON COLUMN etf_premium_discount.premium_discount IS 'Premium/discount as decimal: (ETF_Price - NAV) / NAV. Positive = premium, negative = discount.';

COMMENT ON COLUMN etf_premium_discount.signal IS 'Trading signal based on thresholds: BUY (discount >0.5%), SELL (premium >0.3%), HOLD (within range).';