CREATE TABLE IF NOT EXISTS portfolios (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  UNIQUE(user_id, name),
  FOREIGN KEY (user_id) REFERENCES users (id)
);

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  is_admin INTEGER NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT,
  last_login_at TEXT
);

CREATE TABLE IF NOT EXISTS assets (
  ticker TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  sector TEXT NOT NULL,
  logo_url TEXT NOT NULL DEFAULT '',
  market_data_status TEXT NOT NULL DEFAULT 'unknown',
  market_data_source TEXT NOT NULL DEFAULT '',
  market_data_updated_at TEXT,
  market_data_last_attempt_at TEXT,
  market_data_last_error TEXT NOT NULL DEFAULT '',
  price REAL NOT NULL,
  dy REAL NOT NULL DEFAULT 0,
  pl REAL NOT NULL DEFAULT 0,
  pvp REAL NOT NULL DEFAULT 0,
  variation_day REAL NOT NULL DEFAULT 0,
  variation_7d REAL NOT NULL DEFAULT 0,
  variation_30d REAL NOT NULL DEFAULT 0,
  market_cap_bi REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS asset_enrichments (
  ticker TEXT PRIMARY KEY,
  payload_json TEXT NOT NULL,
  raw_reply TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (ticker) REFERENCES assets (ticker)
);

CREATE TABLE IF NOT EXISTS asset_enrichment_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticker TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  raw_reply TEXT NOT NULL DEFAULT '',
  price_at_update REAL NOT NULL DEFAULT 0,
  mood TEXT NOT NULL DEFAULT '',
  suggested_action TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (ticker) REFERENCES assets (ticker)
);

CREATE INDEX IF NOT EXISTS idx_asset_enrichment_history_ticker_created_at
  ON asset_enrichment_history (ticker, created_at DESC);

CREATE TABLE IF NOT EXISTS transactions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  portfolio_id INTEGER NOT NULL,
  ticker TEXT NOT NULL,
  tx_type TEXT NOT NULL CHECK(tx_type IN ('buy', 'sell')),
  shares REAL NOT NULL CHECK(shares > 0),
  price REAL NOT NULL CHECK(price > 0),
  date TEXT NOT NULL,
  FOREIGN KEY (portfolio_id) REFERENCES portfolios (id),
  FOREIGN KEY (ticker) REFERENCES assets (ticker)
);

CREATE TABLE IF NOT EXISTS incomes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  portfolio_id INTEGER NOT NULL,
  ticker TEXT NOT NULL,
  income_type TEXT NOT NULL CHECK(income_type IN ('dividendo', 'jcp', 'aluguel')),
  amount REAL NOT NULL CHECK(amount > 0),
  date TEXT NOT NULL,
  FOREIGN KEY (portfolio_id) REFERENCES portfolios (id),
  FOREIGN KEY (ticker) REFERENCES assets (ticker)
);

CREATE TABLE IF NOT EXISTS fixed_incomes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  portfolio_id INTEGER NOT NULL,
  distributor TEXT NOT NULL,
  issuer TEXT NOT NULL,
  investment_type TEXT NOT NULL,
  rate_type TEXT NOT NULL CHECK(rate_type IN ('FIXO', 'FIXO+IPCA', 'IPCA', 'CDI', 'FIXO+CDI')),
  annual_rate REAL NOT NULL CHECK(annual_rate >= 0),
  rate_fixed REAL NOT NULL DEFAULT 0 CHECK(rate_fixed >= 0),
  rate_ipca REAL NOT NULL DEFAULT 0 CHECK(rate_ipca >= 0),
  rate_cdi REAL NOT NULL DEFAULT 0 CHECK(rate_cdi >= 0),
  date_aporte TEXT NOT NULL,
  aporte REAL NOT NULL CHECK(aporte > 0),
  reinvested REAL NOT NULL DEFAULT 0 CHECK(reinvested >= 0),
  maturity_date TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (portfolio_id) REFERENCES portfolios (id)
);

CREATE TABLE IF NOT EXISTS fixed_income_snapshot_items (
  portfolio_id INTEGER NOT NULL,
  fixed_income_id INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (portfolio_id, fixed_income_id),
  FOREIGN KEY (portfolio_id) REFERENCES portfolios (id),
  FOREIGN KEY (fixed_income_id) REFERENCES fixed_incomes (id)
);

CREATE TABLE IF NOT EXISTS fixed_income_snapshot_summary (
  portfolio_id INTEGER PRIMARY KEY,
  payload_json TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (portfolio_id) REFERENCES portfolios (id)
);

CREATE TABLE IF NOT EXISTS chart_snapshot_monthly_class (
  portfolio_id INTEGER PRIMARY KEY,
  payload_json TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (portfolio_id) REFERENCES portfolios (id)
);

CREATE TABLE IF NOT EXISTS chart_snapshot_monthly_ticker (
  portfolio_id INTEGER PRIMARY KEY,
  payload_json TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (portfolio_id) REFERENCES portfolios (id)
);
