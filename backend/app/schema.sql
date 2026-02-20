CREATE TABLE IF NOT EXISTS portfolios (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS assets (
  ticker TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  sector TEXT NOT NULL,
  logo_url TEXT NOT NULL DEFAULT '',
  price REAL NOT NULL,
  dy REAL NOT NULL DEFAULT 0,
  pl REAL NOT NULL DEFAULT 0,
  pvp REAL NOT NULL DEFAULT 0,
  variation_day REAL NOT NULL DEFAULT 0,
  variation_7d REAL NOT NULL DEFAULT 0,
  variation_30d REAL NOT NULL DEFAULT 0,
  market_cap_bi REAL NOT NULL DEFAULT 0
);

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
