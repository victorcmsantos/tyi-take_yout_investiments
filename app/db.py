import sqlite3
from pathlib import Path

from flask import current_app, g

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(current_app.config["DATABASE"])
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(_=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    with current_app.open_resource("schema.sql") as schema:
        db.executescript(schema.read().decode("utf-8"))
    db.commit()


def seed_db():
    db = get_db()
    db.execute("INSERT OR IGNORE INTO portfolios (id, name) VALUES (1, 'Carteira Principal')")
    db.commit()


def init_app(app):
    db_path = Path(app.root_path).parent / "investments.db"
    app.config.setdefault("DATABASE", str(db_path))

    app.teardown_appcontext(close_db)
    with app.app_context():
        if not db_path.exists():
            db_path.parent.mkdir(parents=True, exist_ok=True)
            init_db()
            seed_db()
        else:
            ensure_schema_upgrades()


def ensure_schema_upgrades():
    db = get_db()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolios (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL UNIQUE
        )
        """
    )
    portfolios_count = db.execute("SELECT COUNT(*) AS total FROM portfolios").fetchone()
    if not portfolios_count or int(portfolios_count["total"]) == 0:
        db.execute("INSERT INTO portfolios (name) VALUES ('Carteira Principal')")

    tx_cols = [row["name"] for row in db.execute("PRAGMA table_info(transactions)").fetchall()]
    if "portfolio_id" not in tx_cols:
        db.execute("ALTER TABLE transactions ADD COLUMN portfolio_id INTEGER")
        db.execute("UPDATE transactions SET portfolio_id = 1 WHERE portfolio_id IS NULL")

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS incomes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          portfolio_id INTEGER,
          ticker TEXT NOT NULL,
          income_type TEXT NOT NULL CHECK(income_type IN ('dividendo', 'jcp', 'aluguel')),
          amount REAL NOT NULL CHECK(amount > 0),
          date TEXT NOT NULL,
          FOREIGN KEY (ticker) REFERENCES assets (ticker)
        )
        """
    )

    income_cols = [row["name"] for row in db.execute("PRAGMA table_info(incomes)").fetchall()]
    if "portfolio_id" not in income_cols:
        db.execute("ALTER TABLE incomes ADD COLUMN portfolio_id INTEGER")
    db.execute("UPDATE incomes SET portfolio_id = 1 WHERE portfolio_id IS NULL")

    asset_cols = [row["name"] for row in db.execute("PRAGMA table_info(assets)").fetchall()]
    if "variation_7d" not in asset_cols:
        db.execute("ALTER TABLE assets ADD COLUMN variation_7d REAL NOT NULL DEFAULT 0")
    if "variation_30d" not in asset_cols:
        db.execute("ALTER TABLE assets ADD COLUMN variation_30d REAL NOT NULL DEFAULT 0")

    db.execute(
        """
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
        )
        """
    )

    fi_sql_row = db.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'fixed_incomes'"
    ).fetchone()
    fi_sql = (fi_sql_row["sql"] if fi_sql_row else "") or ""
    needs_recreate = (
        "'FIXO+IPCA'" not in fi_sql
        or "'FIXO+CDI'" not in fi_sql
        or "rate_fixed" not in fi_sql
        or "rate_ipca" not in fi_sql
        or "rate_cdi" not in fi_sql
    )
    if needs_recreate:
        db.execute(
            """
            CREATE TABLE fixed_incomes_new (
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
            )
            """
        )
        db.execute(
            """
            INSERT INTO fixed_incomes_new (
                id, portfolio_id, distributor, issuer, investment_type, rate_type,
                annual_rate, rate_fixed, rate_ipca, rate_cdi, date_aporte, aporte, reinvested, maturity_date, created_at
            )
            SELECT
                id, portfolio_id, distributor, issuer, investment_type, rate_type,
                annual_rate,
                CASE
                  WHEN rate_type IN ('FIXO', 'FIXO+IPCA', 'FIXO+CDI') THEN annual_rate
                  ELSE 0
                END AS rate_fixed,
                CASE
                  WHEN rate_type = 'IPCA' THEN annual_rate
                  ELSE 0
                END AS rate_ipca,
                CASE
                  WHEN rate_type = 'CDI' THEN annual_rate
                  ELSE 0
                END AS rate_cdi,
                date_aporte, aporte, reinvested, maturity_date, created_at
            FROM fixed_incomes
            """
        )
        db.execute("DROP TABLE fixed_incomes")
        db.execute("ALTER TABLE fixed_incomes_new RENAME TO fixed_incomes")

    # Corrige legados onde taxa hibrida foi salva em dobro por migracao antiga.
    db.execute(
        """
        UPDATE fixed_incomes
        SET rate_ipca = 0
        WHERE rate_type = 'FIXO+IPCA'
          AND ABS(rate_fixed - annual_rate) < 0.000001
          AND ABS(rate_ipca - annual_rate) < 0.000001
          AND ABS(rate_cdi) < 0.000001
        """
    )
    db.execute(
        """
        UPDATE fixed_incomes
        SET rate_cdi = 0
        WHERE rate_type = 'FIXO+CDI'
          AND ABS(rate_fixed - annual_rate) < 0.000001
          AND ABS(rate_cdi - annual_rate) < 0.000001
          AND ABS(rate_ipca) < 0.000001
        """
    )
    db.commit()
