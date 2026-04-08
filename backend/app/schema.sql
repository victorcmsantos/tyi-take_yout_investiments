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
  role TEXT NOT NULL DEFAULT 'trader',
  is_admin INTEGER NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT,
  last_login_at TEXT
);

CREATE TABLE IF NOT EXISTS scanner_trade_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  action TEXT NOT NULL,
  user_id INTEGER,
  username TEXT NOT NULL DEFAULT '',
  trade_id INTEGER,
  ticker TEXT,
  request_payload_json TEXT NOT NULL DEFAULT '{}',
  response_payload_json TEXT NOT NULL DEFAULT '{}',
  success INTEGER NOT NULL DEFAULT 0,
  upstream_status INTEGER,
  error_message TEXT NOT NULL DEFAULT '',
  remote_addr TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users (id)
);

CREATE INDEX IF NOT EXISTS idx_scanner_trade_audit_created_at
ON scanner_trade_audit (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_scanner_trade_audit_user_id
ON scanner_trade_audit (user_id);

CREATE TABLE IF NOT EXISTS scanner_trade_close_notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  trade_id INTEGER NOT NULL,
  close_status TEXT NOT NULL,
  exit_reason TEXT NOT NULL DEFAULT '',
  ticker TEXT,
  notified_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users (id),
  UNIQUE (user_id, trade_id, close_status)
);

CREATE INDEX IF NOT EXISTS idx_scanner_trade_close_notifications_user_created
ON scanner_trade_close_notifications (user_id, notified_at DESC);

CREATE TABLE IF NOT EXISTS scanner_manual_scan_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  status TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed')),
  requested_by_user_id INTEGER,
  requested_by_username TEXT NOT NULL DEFAULT '',
  started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at TEXT,
  total_tickers INTEGER NOT NULL DEFAULT 0,
  processed_tickers INTEGER NOT NULL DEFAULT 0,
  triggered_signals INTEGER NOT NULL DEFAULT 0,
  upstream_status INTEGER,
  error_message TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (requested_by_user_id) REFERENCES users (id)
);

CREATE INDEX IF NOT EXISTS idx_scanner_manual_scan_runs_started_at
ON scanner_manual_scan_runs (started_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_scanner_manual_scan_single_running
ON scanner_manual_scan_runs (status)
WHERE status = 'running';

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
  market_data_provider_trace TEXT NOT NULL DEFAULT '',
  market_data_fallback_used INTEGER NOT NULL DEFAULT 0,
  price REAL NOT NULL,
  dy REAL NOT NULL DEFAULT 0,
  pl REAL NOT NULL DEFAULT 0,
  pvp REAL NOT NULL DEFAULT 0,
  variation_day REAL NOT NULL DEFAULT 0,
  variation_7d REAL NOT NULL DEFAULT 0,
  variation_30d REAL NOT NULL DEFAULT 0,
  market_cap_bi REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS metric_formulas (
  metric_key TEXT PRIMARY KEY,
  formula TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS asset_metric_baselines (
  ticker TEXT PRIMARY KEY,
  price REAL NOT NULL DEFAULT 0,
  dy REAL NOT NULL DEFAULT 0,
  pl REAL NOT NULL DEFAULT 0,
  pvp REAL NOT NULL DEFAULT 0,
  variation_day REAL NOT NULL DEFAULT 0,
  variation_7d REAL NOT NULL DEFAULT 0,
  variation_30d REAL NOT NULL DEFAULT 0,
  market_cap_bi REAL NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (ticker) REFERENCES assets (ticker)
);

CREATE TABLE IF NOT EXISTS api_provider_circuit_state (
  provider TEXT PRIMARY KEY,
  disabled_until REAL NOT NULL DEFAULT 0,
  status_code INTEGER,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS api_provider_usage_window (
  provider TEXT NOT NULL,
  window TEXT NOT NULL,
  bucket TEXT NOT NULL,
  request_count INTEGER NOT NULL DEFAULT 0,
  success_count INTEGER NOT NULL DEFAULT 0,
  error_count INTEGER NOT NULL DEFAULT 0,
  status_429_count INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (provider, window, bucket)
);

CREATE INDEX IF NOT EXISTS idx_api_provider_usage_window_updated_at
ON api_provider_usage_window (updated_at DESC);

CREATE TABLE IF NOT EXISTS background_job_status (
  job_name TEXT PRIMARY KEY,
  configured_enabled INTEGER NOT NULL DEFAULT 0,
  enabled INTEGER NOT NULL DEFAULT 0,
  running INTEGER NOT NULL DEFAULT 0,
  interval_seconds INTEGER NOT NULL DEFAULT 0,
  max_age_seconds INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_started_at TEXT,
  last_finished_at TEXT,
  last_success_at TEXT,
  last_error_at TEXT,
  last_duration_ms REAL,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  last_result_json TEXT,
  last_error TEXT,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS market_data_sync_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticker TEXT NOT NULL,
  attempted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  success INTEGER NOT NULL DEFAULT 0,
  scope TEXT NOT NULL DEFAULT 'asset',
  providers_tried TEXT NOT NULL DEFAULT '',
  metrics_source TEXT NOT NULL DEFAULT '',
  profile_source TEXT NOT NULL DEFAULT '',
  fallback_used INTEGER NOT NULL DEFAULT 0,
  market_data_status TEXT NOT NULL DEFAULT 'unknown',
  error_message TEXT NOT NULL DEFAULT '',
  price REAL,
  FOREIGN KEY (ticker) REFERENCES assets (ticker)
);

CREATE INDEX IF NOT EXISTS idx_market_data_sync_audit_ticker_attempted_at
ON market_data_sync_audit (ticker, attempted_at DESC);

CREATE TABLE IF NOT EXISTS trade_pnl_reconciliation_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id INTEGER NOT NULL,
  ticker TEXT NOT NULL,
  trade_status TEXT NOT NULL DEFAULT '',
  divergence_pct REAL NOT NULL DEFAULT 0,
  divergence_amount REAL NOT NULL DEFAULT 0,
  payload_json TEXT NOT NULL DEFAULT '{}',
  detected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trade_pnl_reconciliation_audit_trade_id
ON trade_pnl_reconciliation_audit (trade_id, detected_at DESC);

CREATE TABLE IF NOT EXISTS upcoming_income_cache_state (
  ticker TEXT PRIMARY KEY,
  fetched_at TEXT NOT NULL,
  has_events INTEGER NOT NULL DEFAULT 0 CHECK(has_events IN (0, 1)),
  FOREIGN KEY (ticker) REFERENCES assets (ticker)
);

CREATE TABLE IF NOT EXISTS upcoming_income_cache_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticker TEXT NOT NULL,
  symbol TEXT NOT NULL DEFAULT '',
  income_type TEXT NOT NULL DEFAULT 'dividendo',
  ex_date TEXT,
  payment_date TEXT,
  amount REAL,
  currency TEXT NOT NULL DEFAULT 'BRL',
  source TEXT NOT NULL DEFAULT '',
  fetched_at TEXT NOT NULL,
  FOREIGN KEY (ticker) REFERENCES assets (ticker)
);

CREATE INDEX IF NOT EXISTS idx_upcoming_income_cache_events_ticker_ex_date
ON upcoming_income_cache_events (ticker, ex_date, payment_date);

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
  aporte REAL NOT NULL CHECK(aporte >= 0),
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
