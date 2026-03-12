"""Portfolio services."""

import csv
import io
import json
from datetime import datetime

from flask import current_app
from flask import has_request_context

from ..db import get_db
from . import _legacy as legacy


def _row_to_dict(row):
    return dict(row) if row else None


def _current_user_id():
    return legacy._current_user_id()


def _to_number(value):
    return legacy._to_number(value)


def _parse_float(value: str):
    return legacy._parse_float(value)


def _parse_date(value: str):
    return legacy._parse_date(value)


def _current_shares(ticker: str, portfolio_id: int):
    db = get_db()
    row = db.execute(
        """
        SELECT COALESCE(SUM(CASE WHEN tx_type = 'buy' THEN shares ELSE -shares END), 0) AS shares
        FROM transactions
        WHERE ticker = ? AND portfolio_id = ?
        """,
        (ticker, portfolio_id),
    ).fetchone()
    return float(row["shares"] or 0.0)


def _transaction_exists(portfolio_id: int, ticker: str, tx_type: str, shares: float, price: float, date: str):
    db = get_db()
    row = db.execute(
        """
        SELECT id
        FROM transactions
        WHERE portfolio_id = ?
          AND ticker = ?
          AND tx_type = ?
          AND ABS(shares - ?) < 0.000000001
          AND ABS(price - ?) < 0.000001
          AND date = ?
        LIMIT 1
        """,
        (portfolio_id, ticker, tx_type, shares, price, date),
    ).fetchone()
    return row is not None


def _income_exists(portfolio_id: int, ticker: str, income_type: str, amount: float, date: str):
    db = get_db()
    row = db.execute(
        """
        SELECT id
        FROM incomes
        WHERE portfolio_id = ?
          AND ticker = ?
          AND income_type = ?
          AND ABS(amount - ?) < 0.000001
          AND date = ?
        LIMIT 1
        """,
        (portfolio_id, ticker, income_type, amount, date),
    ).fetchone()
    return row is not None


def _fixed_income_exists(
    portfolio_id: int,
    distributor: str,
    issuer: str,
    investment_type: str,
    rate_type: str,
    annual_rate: float,
    rate_fixed: float,
    rate_ipca: float,
    rate_cdi: float,
    date_aporte: str,
    aporte: float,
    reinvested: float,
    maturity_date: str,
):
    db = get_db()
    row = db.execute(
        """
        SELECT id
        FROM fixed_incomes
        WHERE portfolio_id = ?
          AND distributor = ?
          AND issuer = ?
          AND investment_type = ?
          AND rate_type = ?
          AND ABS(annual_rate - ?) < 0.000001
          AND ABS(rate_fixed - ?) < 0.000001
          AND ABS(rate_ipca - ?) < 0.000001
          AND ABS(rate_cdi - ?) < 0.000001
          AND date_aporte = ?
          AND ABS(aporte - ?) < 0.000001
          AND ABS(reinvested - ?) < 0.000001
          AND maturity_date = ?
        LIMIT 1
        """,
        (
            portfolio_id,
            distributor,
            issuer,
            investment_type,
            rate_type,
            annual_rate,
            rate_fixed,
            rate_ipca,
            rate_cdi,
            date_aporte,
            aporte,
            reinvested,
            maturity_date,
        ),
    ).fetchone()
    return row is not None


def get_portfolios():
    db = get_db()
    current_user_id = _current_user_id()
    if current_user_id is None and not has_request_context():
        rows = db.execute("SELECT id, name FROM portfolios ORDER BY id ASC").fetchall()
    elif current_user_id is None:
        return []
    else:
        rows = db.execute(
            "SELECT id, name FROM portfolios WHERE user_id = ? ORDER BY id ASC",
            (current_user_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def _get_portfolio(portfolio_id: int):
    db = get_db()
    current_user_id = _current_user_id()
    if current_user_id is None and not has_request_context():
        row = db.execute("SELECT id, name FROM portfolios WHERE id = ?", (portfolio_id,)).fetchone()
    elif current_user_id is None:
        return None
    else:
        row = db.execute(
            "SELECT id, name FROM portfolios WHERE id = ? AND user_id = ?",
            (portfolio_id, current_user_id),
        ).fetchone()
    return _row_to_dict(row)


def get_default_portfolio_id():
    db = get_db()
    current_user_id = _current_user_id()
    if current_user_id is None and not has_request_context():
        row = db.execute("SELECT id FROM portfolios ORDER BY id ASC LIMIT 1").fetchone()
    elif current_user_id is None:
        return None
    else:
        row = db.execute(
            "SELECT id FROM portfolios WHERE user_id = ? ORDER BY id ASC LIMIT 1",
            (current_user_id,),
        ).fetchone()
    return int(row["id"]) if row else None


def normalize_portfolio_ids(raw_ids):
    if isinstance(raw_ids, int):
        raw_values = [raw_ids]
    elif isinstance(raw_ids, (list, tuple, set)):
        raw_values = list(raw_ids)
    else:
        raw_values = [raw_ids]

    portfolios = get_portfolios()
    valid_ids = {int(item["id"]) for item in portfolios}
    result = []

    for value in raw_values:
        if value in (None, ""):
            continue
        try:
            pid = int(value)
        except (TypeError, ValueError):
            continue
        if pid in valid_ids and pid not in result:
            result.append(pid)

    if not result:
        default_portfolio_id = get_default_portfolio_id()
        result = [default_portfolio_id] if default_portfolio_id is not None else []
    return result


def resolve_portfolio_id(raw_portfolio_id):
    default_portfolio_id = get_default_portfolio_id()
    if raw_portfolio_id in (None, ""):
        return default_portfolio_id
    try:
        pid = int(raw_portfolio_id)
    except (TypeError, ValueError):
        return default_portfolio_id
    return pid if _get_portfolio(pid) else default_portfolio_id


def create_portfolio(name: str):
    clean_name = (name or "").strip()
    if not clean_name:
        return False, "Nome da carteira e obrigatorio."
    current_user_id = _current_user_id()
    if current_user_id is None:
        return False, "Usuario sem contexto de carteira."
    db = get_db()
    existing = db.execute(
        "SELECT id FROM portfolios WHERE user_id = ? AND LOWER(name) = LOWER(?)",
        (current_user_id, clean_name),
    ).fetchone()
    if existing:
        return False, "Ja existe uma carteira com esse nome."
    db.execute("INSERT INTO portfolios (user_id, name) VALUES (?, ?)", (current_user_id, clean_name))
    db.commit()
    row = db.execute(
        "SELECT id FROM portfolios WHERE user_id = ? AND name = ?",
        (current_user_id, clean_name),
    ).fetchone()
    return True, int(row["id"])


def delete_portfolio(portfolio_id):
    try:
        pid = int(portfolio_id)
    except (TypeError, ValueError):
        return False, "Carteira invalida."

    db = get_db()
    current_user_id = _current_user_id()
    if current_user_id is None:
        return False, "Usuario sem contexto de carteira."
    portfolio = db.execute(
        "SELECT id, name FROM portfolios WHERE id = ? AND user_id = ?",
        (pid, current_user_id),
    ).fetchone()
    if not portfolio:
        return False, "Carteira nao encontrada."

    total_row = db.execute(
        "SELECT COUNT(*) AS total FROM portfolios WHERE user_id = ?",
        (current_user_id,),
    ).fetchone()
    total_portfolios = int(total_row["total"]) if total_row else 0
    if total_portfolios <= 1:
        return False, "Nao e possivel remover a unica carteira. Crie outra primeiro."

    tx_row = db.execute(
        "SELECT COUNT(*) AS total FROM transactions WHERE portfolio_id = ?",
        (pid,),
    ).fetchone()
    in_row = db.execute(
        "SELECT COUNT(*) AS total FROM incomes WHERE portfolio_id = ?",
        (pid,),
    ).fetchone()
    tx_total = int(tx_row["total"]) if tx_row else 0
    in_total = int(in_row["total"]) if in_row else 0
    if tx_total > 0 or in_total > 0:
        return (
            False,
            "Carteira com lancamentos nao pode ser removida. Remova transacoes/proventos primeiro.",
        )

    db.execute("DELETE FROM chart_snapshot_monthly_class WHERE portfolio_id = ?", (pid,))
    db.execute("DELETE FROM chart_snapshot_monthly_ticker WHERE portfolio_id = ?", (pid,))
    db.execute("DELETE FROM fixed_income_snapshot_items WHERE portfolio_id = ?", (pid,))
    db.execute("DELETE FROM fixed_income_snapshot_summary WHERE portfolio_id = ?", (pid,))
    db.execute("DELETE FROM portfolios WHERE id = ?", (pid,))
    db.commit()
    legacy._clear_benchmark_cache()
    return True, portfolio["name"]


def add_transaction(form_data: dict):
    portfolio_id = resolve_portfolio_id(
        form_data.get("target_portfolio_id") or form_data.get("portfolio_id")
    )
    ticker = (form_data.get("ticker") or "").strip().upper()
    tx_type = (form_data.get("tx_type") or "").strip().lower()

    if not ticker:
        return False, "Ticker e obrigatorio."
    if tx_type not in {"buy", "sell"}:
        return False, "Tipo de transacao invalido."

    shares = _parse_float(form_data.get("shares"))
    if shares is None:
        return False, "Quantidade precisa ser numerica."
    if shares <= 0:
        return False, "Quantidade precisa ser maior que zero."

    price = _parse_float(form_data.get("price"))
    if price is None or price <= 0:
        return False, "Preco precisa ser numerico e maior que zero."
    ok_conversion, converted_price, conversion_error = legacy._convert_usd_to_brl_if_needed(ticker, price)
    if not ok_conversion:
        return False, conversion_error
    price = converted_price

    transaction_date = _parse_date(form_data.get("date"))
    if transaction_date is None:
        return False, "Data invalida. Use o formato YYYY-MM-DD."

    if _transaction_exists(portfolio_id, ticker, tx_type, shares, price, transaction_date):
        return False, "Transacao duplicada: ja existe um registro com esses mesmos dados."

    db = get_db()
    if tx_type == "sell":
        if shares - _current_shares(ticker, portfolio_id) > 0.000000001:
            return False, "Venda maior que a quantidade em carteira."

    from . import market_data

    asset = market_data.get_asset(ticker)
    if not asset:
        if tx_type == "sell":
            return False, "Nao existe posicao para esse ticker."
        profile, _ = legacy._fetch_market_profile(ticker)
        name = profile.get("name") or (form_data.get("name") or "").strip() or ticker
        sector = profile.get("sector") or (form_data.get("sector") or "").strip() or "Nao informado"
        db.execute(
            """
            INSERT INTO assets (
                ticker, name, sector, price, dy, pl, pvp, variation_day, market_cap_bi
            ) VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0)
            """,
            (ticker, name, sector, price),
        )
    else:
        should_update_profile = asset["name"] == ticker or asset["sector"] == "Nao informado"
        if should_update_profile:
            profile, _ = legacy._fetch_market_profile(ticker)
            name = profile.get("name") or asset["name"]
            sector = profile.get("sector") or asset["sector"]
            db.execute(
                "UPDATE assets SET name = ?, sector = ? WHERE ticker = ?",
                (name, sector, ticker),
            )

    db.execute(
        """
        INSERT INTO transactions (portfolio_id, ticker, tx_type, shares, price, date)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (portfolio_id, ticker, tx_type, shares, price, transaction_date),
    )
    db.commit()
    legacy.invalidate_chart_snapshots([portfolio_id])

    return True, "Transacao registrada com sucesso."


def import_transactions_csv(file_bytes, target_portfolio_id: int):
    if not file_bytes:
        return False, "Arquivo CSV vazio.", 0, []

    try:
        text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        return False, "Nao foi possivel ler o CSV (use UTF-8).", 0, []

    sample = text[:2048]
    delimiter = ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        delimiter = dialect.delimiter
    except Exception:
        if "\t" in sample:
            delimiter = "\t"
        else:
            delimiter = ";" if ";" in sample and "," not in sample else ","

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    if not reader.fieldnames:
        return False, "CSV sem cabecalho.", 0, []

    header_map = {
        "ticker": "ticker",
        "ativo": "ticker",
        "tx_type": "tx_type",
        "tipo/tx_type": "tx_type",
        "tipo": "tx_type",
        "shares": "shares",
        "quantidade/shares": "shares",
        "quantidade": "shares",
        "qtd": "shares",
        "price": "price",
        "preco/price": "price",
        "preço/price": "price",
        "preco": "price",
        "preço": "price",
        "date": "date",
        "data/date": "date",
        "data": "date",
        "name": "name",
        "nome": "name",
        "sector": "sector",
        "setor": "sector",
        "amount": "amount",
        "valor": "amount",
        "valor/amount": "amount",
        "provento": "amount",
    }

    normalized_fields = {}
    for field in reader.fieldnames:
        key = (field or "").strip().lower()
        mapped = header_map.get(key)
        if mapped:
            normalized_fields[field] = mapped

    required = {"ticker", "tx_type", "date"}
    if not required.issubset(set(normalized_fields.values())):
        return (
            False,
            "CSV precisa ter colunas: ticker, tipo/tx_type e data/date.",
            0,
            [],
        )

    imported = 0
    errors = []
    csv_tickers = set()
    line_number = 1

    for row in reader:
        line_number += 1
        payload = {"target_portfolio_id": str(target_portfolio_id)}
        for original, mapped in normalized_fields.items():
            payload[mapped] = (row.get(original) or "").strip()
        ticker = (payload.get("ticker") or "").strip().upper()
        if ticker:
            csv_tickers.add(ticker)

        tx_type = payload.get("tx_type", "").lower()
        if tx_type == "compra":
            payload["tx_type"] = "buy"
        elif tx_type == "venda":
            payload["tx_type"] = "sell"

        tx_type = payload.get("tx_type", "").lower()
        if tx_type in {"dividendo", "jcp", "aluguel"}:
            income_payload = {
                "target_portfolio_id": str(target_portfolio_id),
                "ticker": payload.get("ticker"),
                "income_type": tx_type,
                "amount": payload.get("amount") or payload.get("price"),
                "date": payload.get("date"),
            }
            ok, message = add_income(income_payload)
        else:
            ok, message = add_transaction(payload)

        if ok:
            imported += 1
        else:
            errors.append(f"Linha {line_number}: {message}")

    from . import market_data

    failed_refresh = market_data.refresh_market_data_for_tickers(sorted(csv_tickers), attempts=2)
    for ticker in failed_refresh:
        errors.append(f"Aviso: nao foi possivel atualizar Yahoo para {ticker}.")

    return True, "Importacao concluida.", imported, errors


def add_income(form_data: dict):
    portfolio_id = resolve_portfolio_id(
        form_data.get("target_portfolio_id") or form_data.get("portfolio_id")
    )
    ticker = (form_data.get("ticker") or "").strip().upper()
    income_type = (form_data.get("income_type") or "").strip().lower()

    if not ticker:
        return False, "Ticker e obrigatorio."
    if income_type not in {"dividendo", "jcp", "aluguel"}:
        return False, "Tipo de provento invalido."

    amount = _parse_float(form_data.get("amount"))
    if amount is None or amount <= 0:
        return False, "Valor do provento precisa ser numerico e maior que zero."
    ok_conversion, converted_amount, conversion_error = legacy._convert_usd_to_brl_if_needed(ticker, amount)
    if not ok_conversion:
        return False, conversion_error
    amount = converted_amount

    income_date = _parse_date(form_data.get("date"))
    if income_date is None:
        return False, "Data invalida. Use o formato YYYY-MM-DD."

    if _income_exists(portfolio_id, ticker, income_type, amount, income_date):
        return False, "Provento duplicado: ja existe um registro com esses mesmos dados."

    from . import market_data

    if not market_data.get_asset(ticker):
        return False, "Ticker nao cadastrado. Lance uma transacao primeiro."

    db = get_db()
    db.execute(
        """
        INSERT INTO incomes (portfolio_id, ticker, income_type, amount, date)
        VALUES (?, ?, ?, ?, ?)
        """,
        (portfolio_id, ticker, income_type, amount, income_date),
    )
    db.commit()
    legacy.invalidate_chart_snapshots([portfolio_id])
    return True, "Provento registrado com sucesso."


def add_fixed_income(form_data: dict):
    portfolio_id = resolve_portfolio_id(
        form_data.get("target_portfolio_id") or form_data.get("portfolio_id")
    )
    distributor = (form_data.get("distributor") or "").strip()
    issuer = (form_data.get("issuer") or "").strip()
    investment_type = (form_data.get("investment_type") or "").strip().upper()
    rate_type = (form_data.get("rate_type") or "").strip().upper()
    annual_rate_legacy = _parse_float(form_data.get("annual_rate"))
    rate_fixed = _parse_float(form_data.get("juros_fixo"))
    rate_ipca = _parse_float(form_data.get("ipca"))
    rate_cdi = _parse_float(form_data.get("cdi"))
    rate_fixed = 0.0 if rate_fixed is None else rate_fixed
    rate_ipca = 0.0 if rate_ipca is None else rate_ipca
    rate_cdi = 0.0 if rate_cdi is None else rate_cdi
    date_aporte = _parse_date(form_data.get("date_aporte"))
    maturity_date = _parse_date(form_data.get("maturity_date"))
    aporte = _parse_float(form_data.get("aporte"))
    reinvested = _parse_float(form_data.get("reinvested"))

    if not distributor:
        return False, "Distribuidor e obrigatorio."
    if not issuer:
        return False, "Emissor e obrigatorio."
    if not investment_type:
        return False, "Investimento e obrigatorio."
    if rate_type not in {"FIXO", "FIXO+IPCA", "IPCA", "CDI", "FIXO+CDI"}:
        return False, "Tipo de taxa invalido."
    expected_sets = {
        "FIXO": {"FIXO"},
        "IPCA": {"IPCA"},
        "CDI": {"CDI"},
        "FIXO+IPCA": {"FIXO", "IPCA"},
        "FIXO+CDI": {"FIXO", "CDI"},
    }
    positive_set = set()
    if rate_fixed > 0:
        positive_set.add("FIXO")
    if rate_ipca > 0:
        positive_set.add("IPCA")
    if rate_cdi > 0:
        positive_set.add("CDI")

    expected = expected_sets[rate_type]
    annual_rate = None
    if positive_set:
        if positive_set != expected:
            return (
                False,
                (
                    f"Para o tipo {rate_type}, preencha somente: "
                    f"{', '.join(sorted(expected))}."
                ),
            )
        component_rates = {"FIXO": rate_fixed, "IPCA": rate_ipca, "CDI": rate_cdi}
        annual_rate = sum(component_rates[key] for key in expected)
    else:
        if rate_type in {"FIXO+IPCA", "FIXO+CDI"}:
            return False, f"Para o tipo {rate_type}, informe os percentuais de cada componente."
        if annual_rate_legacy is None or annual_rate_legacy < 0:
            return False, "Taxa anual invalida."
        annual_rate = annual_rate_legacy

    if annual_rate is None or annual_rate < 0:
        return False, "Taxa anual invalida."
    if aporte is None or aporte <= 0:
        return False, "Aporte invalido."
    if reinvested is None:
        reinvested = 0.0
    if reinvested < 0:
        return False, "Reinvestido nao pode ser negativo."
    if not date_aporte:
        return False, "Data de aporte invalida."
    if not maturity_date:
        return False, "Data final invalida."
    if maturity_date < date_aporte:
        return False, "Data final nao pode ser menor que data de aporte."
    if _fixed_income_exists(
        portfolio_id,
        distributor,
        issuer,
        investment_type,
        rate_type,
        annual_rate,
        rate_fixed,
        rate_ipca,
        rate_cdi,
        date_aporte,
        aporte,
        reinvested,
        maturity_date,
    ):
        return False, "Registro duplicado: ja existe uma renda fixa com os mesmos dados."

    db = get_db()
    db.execute(
        """
        INSERT INTO fixed_incomes (
            portfolio_id, distributor, issuer, investment_type, rate_type, annual_rate,
            rate_fixed, rate_ipca, rate_cdi,
            date_aporte, aporte, reinvested, maturity_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            portfolio_id,
            distributor,
            issuer,
            investment_type,
            rate_type,
            annual_rate,
            rate_fixed,
            rate_ipca,
            rate_cdi,
            date_aporte,
            aporte,
            reinvested,
            maturity_date,
        ),
    )
    db.commit()
    invalidate_fixed_income_snapshot([portfolio_id])
    legacy.invalidate_chart_snapshots([portfolio_id])
    return True, "Renda fixa cadastrada com sucesso."


def delete_fixed_incomes(fixed_income_ids, portfolio_ids):
    ids = []
    for raw_id in fixed_income_ids:
        try:
            parsed = int(raw_id)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in ids:
            ids.append(parsed)

    if not ids:
        return 0

    pids = normalize_portfolio_ids(portfolio_ids)
    ids_placeholders = ",".join(["?"] * len(ids))
    pids_placeholders = ",".join(["?"] * len(pids))

    db = get_db()
    cursor = db.execute(
        """
        DELETE FROM fixed_incomes
        WHERE id IN ("""
        + ids_placeholders
        + """)
          AND portfolio_id IN ("""
        + pids_placeholders
        + """)
        """,
        tuple(ids + pids),
    )
    db.commit()
    invalidate_fixed_income_snapshot(pids)
    legacy.invalidate_chart_snapshots(pids)
    return cursor.rowcount or 0


def _fixed_income_projection(item):
    # Reusa a regra consolidada existente enquanto a extração do cálculo
    # atuarial completo ainda está em andamento.
    return legacy._fixed_income_projection(item)


def get_fixed_incomes(portfolio_ids, sort_by: str = "date_aporte", sort_dir: str = "desc"):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            fi.id,
            fi.portfolio_id,
            fi.distributor,
            fi.issuer,
            fi.investment_type,
            fi.rate_type,
            fi.annual_rate,
            fi.rate_fixed,
            fi.rate_ipca,
            fi.rate_cdi,
            fi.date_aporte,
            fi.aporte,
            fi.reinvested,
            fi.maturity_date,
            p.name AS portfolio_name
        FROM fixed_incomes fi
        JOIN portfolios p ON p.id = fi.portfolio_id
        WHERE fi.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY fi.date_aporte DESC, fi.id DESC
        """,
        tuple(pids),
    ).fetchall()
    items = [_fixed_income_projection(dict(row)) for row in rows]
    return _sort_fixed_income_items(items, sort_by=sort_by, sort_dir=sort_dir)


def _sort_fixed_income_items(items, sort_by: str = "date_aporte", sort_dir: str = "desc"):
    valid_dirs = {"asc", "desc"}
    direction = sort_dir if sort_dir in valid_dirs else "desc"
    key_name = (sort_by or "date_aporte").strip()
    if key_name not in {
        "portfolio_name",
        "distributor",
        "issuer",
        "investment_type",
        "rate_type",
        "annual_rate",
        "date_aporte",
        "maturity_date",
        "active_applied_value",
        "elapsed_days",
        "total_days",
        "current_gross_value",
        "total_received",
        "rendimento",
        "final_gross_value",
    }:
        key_name = "date_aporte"

    def _sort_key(item):
        value = item.get(key_name)
        if value is None:
            return (1, "")
        if isinstance(value, (int, float)):
            return (0, float(value))
        return (0, str(value).lower())

    sorted_items = list(items or [])
    sorted_items.sort(key=_sort_key, reverse=(direction == "desc"))
    return sorted_items


def get_fixed_income_summary(portfolio_ids):
    payload = get_fixed_income_payload_cached(portfolio_ids)
    return payload.get("summary", get_fixed_income_summary_from_items(payload.get("items") or []))


def get_fixed_income_summary_from_items(items):
    items = items or []
    return {
        "applied_total": round(sum(item["active_applied_value"] for item in items), 2),
        "current_total": round(sum(item["current_gross_value"] for item in items), 2),
        "income_total": round(sum(item["current_income"] for item in items), 2),
        "final_total": round(
            sum(item["final_gross_value"] for item in items if not item["is_matured"]),
            2,
        ),
        "total_received": round(sum(item["total_received"] for item in items), 2),
        "rendimento_recebido_total": round(sum(item["rendimento"] for item in items), 2),
        "count": len(items),
    }


def _snapshot_now():
    return datetime.now().isoformat(timespec="seconds")


def _snapshot_age_seconds(iso_text: str):
    try:
        created = datetime.fromisoformat((iso_text or "").strip())
    except (TypeError, ValueError):
        return None
    return max((datetime.now() - created).total_seconds(), 0.0)


def invalidate_fixed_income_snapshot(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    try:
        db.execute(
            "DELETE FROM fixed_income_snapshot_items WHERE portfolio_id IN (" + placeholders + ")",
            tuple(pids),
        )
        db.execute(
            "DELETE FROM fixed_income_snapshot_summary WHERE portfolio_id IN (" + placeholders + ")",
            tuple(pids),
        )
        db.commit()
    except Exception:
        db.rollback()


def rebuild_fixed_income_snapshots(portfolio_ids=None):
    if portfolio_ids is None:
        pids = legacy._all_portfolio_ids()
    else:
        pids = normalize_portfolio_ids(portfolio_ids)
    if not pids:
        return {"portfolios": 0, "items": 0}

    db = get_db()
    total_items = 0
    stamp = _snapshot_now()
    for pid in pids:
        items = get_fixed_incomes([pid], sort_by="date_aporte", sort_dir="desc")
        summary = get_fixed_income_summary_from_items(items)
        total_items += len(items)
        try:
            db.execute(
                "DELETE FROM fixed_income_snapshot_items WHERE portfolio_id = ?",
                (pid,),
            )
            db.execute(
                """
                INSERT INTO fixed_income_snapshot_summary (portfolio_id, payload_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(portfolio_id) DO UPDATE SET
                  payload_json = excluded.payload_json,
                  updated_at = excluded.updated_at
                """,
                (pid, json.dumps(summary, ensure_ascii=False), stamp),
            )
            for item in items:
                db.execute(
                    """
                    INSERT INTO fixed_income_snapshot_items (portfolio_id, fixed_income_id, payload_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(portfolio_id, fixed_income_id) DO UPDATE SET
                      payload_json = excluded.payload_json,
                      updated_at = excluded.updated_at
                    """,
                    (pid, int(item["id"]), json.dumps(item, ensure_ascii=False), stamp),
                )
        except Exception:
            db.rollback()
            raise
    db.commit()
    return {"portfolios": len(pids), "items": total_items}


def get_fixed_income_payload_cached(portfolio_ids, sort_by: str = "date_aporte", sort_dir: str = "desc"):
    pids = normalize_portfolio_ids(portfolio_ids)
    max_age_seconds = int(current_app.config.get("FIXED_INCOME_SNAPSHOT_MAX_AGE_SECONDS", 900))
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()

    try:
        summary_rows = db.execute(
            """
            SELECT portfolio_id, payload_json, updated_at
            FROM fixed_income_snapshot_summary
            WHERE portfolio_id IN ("""
            + placeholders
            + """)
            """,
            tuple(pids),
        ).fetchall()
        if len(summary_rows) != len(pids):
            raise RuntimeError("snapshot_miss")

        summary_map = {}
        for row in summary_rows:
            age = _snapshot_age_seconds(row["updated_at"])
            if age is None or age > max_age_seconds:
                raise RuntimeError("snapshot_stale")
            summary_map[int(row["portfolio_id"])] = json.loads(row["payload_json"] or "{}")

        item_rows = db.execute(
            """
            SELECT portfolio_id, payload_json, updated_at
            FROM fixed_income_snapshot_items
            WHERE portfolio_id IN ("""
            + placeholders
            + """)
            """,
            tuple(pids),
        ).fetchall()

        items = []
        for row in item_rows:
            age = _snapshot_age_seconds(row["updated_at"])
            if age is None or age > max_age_seconds:
                raise RuntimeError("snapshot_stale")
            items.append(json.loads(row["payload_json"] or "{}"))

        summary = {
            "applied_total": 0.0,
            "current_total": 0.0,
            "income_total": 0.0,
            "final_total": 0.0,
            "total_received": 0.0,
            "rendimento_recebido_total": 0.0,
            "count": 0,
        }
        for pid in pids:
            part = summary_map.get(int(pid), {})
            summary["applied_total"] += float(part.get("applied_total", 0.0))
            summary["current_total"] += float(part.get("current_total", 0.0))
            summary["income_total"] += float(part.get("income_total", 0.0))
            summary["final_total"] += float(part.get("final_total", 0.0))
            summary["total_received"] += float(part.get("total_received", 0.0))
            summary["rendimento_recebido_total"] += float(part.get("rendimento_recebido_total", 0.0))
            summary["count"] += int(part.get("count", 0))
        for key in (
            "applied_total",
            "current_total",
            "income_total",
            "final_total",
            "total_received",
            "rendimento_recebido_total",
        ):
            summary[key] = round(summary[key], 2)

        return {
            "items": _sort_fixed_income_items(items, sort_by=sort_by, sort_dir=sort_dir),
            "summary": summary,
            "snapshot": True,
        }
    except Exception:
        items = get_fixed_incomes(pids, sort_by=sort_by, sort_dir=sort_dir)
        summary = get_fixed_income_summary_from_items(items)
        return {"items": items, "summary": summary, "snapshot": False}


def import_fixed_incomes_csv(file_bytes, target_portfolio_id: int):
    if not file_bytes:
        return False, "Arquivo CSV vazio.", 0, []

    try:
        text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        return False, "Nao foi possivel ler o CSV (use UTF-8).", 0, []

    sample = text[:2048]
    delimiter = ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        delimiter = dialect.delimiter
    except Exception:
        if "\t" in sample:
            delimiter = "\t"
        else:
            delimiter = ";" if ";" in sample and "," not in sample else ","

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    if not reader.fieldnames:
        return False, "CSV sem cabecalho.", 0, []

    header_map = {
        "distributor": "distributor",
        "distribuidor": "distributor",
        "issuer": "issuer",
        "emissor": "issuer",
        "investment_type": "investment_type",
        "investimento": "investment_type",
        "rate_type": "rate_type",
        "tipo_taxa": "rate_type",
        "tipo taxa": "rate_type",
        "tax_type": "rate_type",
        "tipo": "rate_type",
        "annual_rate": "annual_rate",
        "taxa_anual": "annual_rate",
        "taxa anual": "annual_rate",
        "juros_fixo": "annual_rate",
        "juros fixo": "annual_rate",
        "jurosfixo": "annual_rate",
        "juros_fixo_%": "annual_rate",
        "juros fixo %": "annual_rate",
        "juros_fixo_csv": "juros_fixo",
        "juros fixo csv": "juros_fixo",
        "juros_fixo_col": "juros_fixo",
        "juros fixo col": "juros_fixo",
        "juros_fixo_valor": "juros_fixo",
        "juros fixo valor": "juros_fixo",
        "jurosfixocsv": "juros_fixo",
        "jurosfixocol": "juros_fixo",
        "jurosfixovalor": "juros_fixo",
        "juros_fixo": "juros_fixo",
        "juros fixo": "juros_fixo",
        "ipca": "ipca",
        "cdi": "cdi",
        "date_aporte": "date_aporte",
        "data_aporte": "date_aporte",
        "data aporte": "date_aporte",
        "aporte_date": "date_aporte",
        "maturity_date": "maturity_date",
        "data_final": "maturity_date",
        "data final": "maturity_date",
        "vencimento": "maturity_date",
        "aporte": "aporte",
        "applied": "aporte",
        "reinvested": "reinvested",
        "reinvestido": "reinvested",
    }

    normalized_fields = {}
    for field in reader.fieldnames:
        key = (field or "").strip().lower()
        mapped = header_map.get(key)
        if mapped:
            normalized_fields[field] = mapped

    required = {
        "distributor",
        "issuer",
        "investment_type",
        "rate_type",
        "date_aporte",
        "maturity_date",
        "aporte",
        "reinvested",
    }
    if not required.issubset(set(normalized_fields.values())):
        return (
            False,
            (
                "CSV de renda fixa precisa ter colunas: Distribuidor, Emissor, Investimento, "
                "tipo, data aporte, aporte, Reinvestido, data final, Juros Fixo, IPCA e CDI."
            ),
            0,
            [],
        )

    has_rate_cols = {"juros_fixo", "ipca", "cdi"}.issubset(set(normalized_fields.values()))
    has_legacy_rate = {"rate_type", "annual_rate"}.issubset(set(normalized_fields.values()))
    if not has_rate_cols and not has_legacy_rate:
        return (
            False,
            "CSV precisa informar as colunas de taxa (Juros Fixo, IPCA, CDI) ou (tipo taxa, taxa anual).",
            0,
            [],
        )

    imported = 0
    errors = []
    line_number = 1
    for row in reader:
        line_number += 1
        payload = {"target_portfolio_id": str(target_portfolio_id)}
        for original, mapped in normalized_fields.items():
            payload[mapped] = (row.get(original) or "").strip()

        rate_type_raw = (payload.get("rate_type") or "").strip().upper()
        rate_type_map = {
            "FIXO": "FIXO",
            "FIXO+IPCA": "FIXO+IPCA",
            "FIXO + IPCA": "FIXO+IPCA",
            "CDI": "CDI",
            "IPCA": "IPCA",
            "FIXO+CDI": "FIXO+CDI",
            "FIXO + CDI": "FIXO+CDI",
        }
        payload["rate_type"] = rate_type_map.get(rate_type_raw, rate_type_raw)
        if payload["rate_type"] not in {"FIXO", "FIXO+IPCA", "IPCA", "CDI", "FIXO+CDI"}:
            errors.append(
                f"Linha {line_number}: tipo invalido. Use FIXO, FIXO+IPCA, IPCA, CDI ou FIXO+CDI."
            )
            continue

        juros_fixo = _parse_float(payload.get("juros_fixo"))
        ipca = _parse_float(payload.get("ipca"))
        cdi = _parse_float(payload.get("cdi"))
        rate_candidates = [
            ("FIXO", juros_fixo if juros_fixo is not None else 0.0),
            ("IPCA", ipca if ipca is not None else 0.0),
            ("CDI", cdi if cdi is not None else 0.0),
        ]
        positive_rates = {rtype for rtype, rate in rate_candidates if rate > 0}
        if positive_rates:
            expected_sets = {
                "FIXO": {"FIXO"},
                "IPCA": {"IPCA"},
                "CDI": {"CDI"},
                "FIXO+IPCA": {"FIXO", "IPCA"},
                "FIXO+CDI": {"FIXO", "CDI"},
            }
            expected = expected_sets[payload["rate_type"]]
            if positive_rates != expected:
                errors.append(
                    (
                        f"Linha {line_number}: tipo '{payload['rate_type']}' nao bate com as colunas de taxa preenchidas "
                        f"(esperado {', '.join(sorted(expected))})."
                    )
                )
                continue

            rate_values = {
                "FIXO": juros_fixo if juros_fixo is not None else 0.0,
                "IPCA": ipca if ipca is not None else 0.0,
                "CDI": cdi if cdi is not None else 0.0,
            }
            payload["annual_rate"] = sum(rate_values[key] for key in expected)
        elif "annual_rate" in payload and "rate_type" in payload:
            pass
        else:
            errors.append(
                f"Linha {line_number}: informe a taxa correspondente ao tipo em Juros Fixo, IPCA ou CDI."
            )
            continue

        ok, message = add_fixed_income(payload)
        if not ok:
            errors.append(f"Linha {line_number}: {message}")
            continue

        imported += 1

    return True, "Importacao concluida.", imported, errors


def get_transactions(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            t.id,
            t.ticker,
            t.tx_type,
            t.shares,
            t.price,
            t.date,
            (t.shares * t.price) AS total_value,
            p.name AS portfolio_name
        FROM transactions t
        JOIN portfolios p ON p.id = t.portfolio_id
        WHERE t.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY t.date DESC, t.id DESC
        """,
        tuple(pids),
    ).fetchall()
    return [dict(row) for row in rows]


def delete_transactions(transaction_ids, portfolio_ids):
    ids = []
    for raw_id in transaction_ids:
        try:
            parsed = int(raw_id)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in ids:
            ids.append(parsed)

    if not ids:
        return 0

    pids = normalize_portfolio_ids(portfolio_ids)
    ids_placeholders = ",".join(["?"] * len(ids))
    pids_placeholders = ",".join(["?"] * len(pids))

    db = get_db()
    cursor = db.execute(
        """
        DELETE FROM transactions
        WHERE id IN ("""
        + ids_placeholders
        + """)
          AND portfolio_id IN ("""
        + pids_placeholders
        + """)
        """,
        tuple(ids + pids),
    )
    db.commit()
    legacy.invalidate_chart_snapshots(pids)
    return cursor.rowcount


def get_incomes(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            i.id,
            i.ticker,
            i.income_type,
            i.amount,
            i.date,
            p.name AS portfolio_name
        FROM incomes i
        JOIN portfolios p ON p.id = i.portfolio_id
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY i.date DESC, i.id DESC
        """,
        tuple(pids),
    ).fetchall()
    return [dict(row) for row in rows]


def delete_incomes(income_ids, portfolio_ids):
    ids = []
    for raw_id in income_ids:
        try:
            parsed = int(raw_id)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in ids:
            ids.append(parsed)

    if not ids:
        return 0

    pids = normalize_portfolio_ids(portfolio_ids)
    ids_placeholders = ",".join(["?"] * len(ids))
    pids_placeholders = ",".join(["?"] * len(pids))

    db = get_db()
    cursor = db.execute(
        """
        DELETE FROM incomes
        WHERE id IN ("""
        + ids_placeholders
        + """)
          AND portfolio_id IN ("""
        + pids_placeholders
        + """)
        """,
        tuple(ids + pids),
    )
    db.commit()
    legacy.invalidate_chart_snapshots(pids)
    return cursor.rowcount or 0


def get_asset_transactions(ticker: str, portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            t.ticker,
            t.tx_type,
            t.shares,
            t.price,
            t.date,
            (t.shares * t.price) AS total_value,
            p.name AS portfolio_name
        FROM transactions t
        JOIN portfolios p ON p.id = t.portfolio_id
        WHERE t.ticker = ? AND t.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY t.date DESC, t.id DESC
        """,
        tuple([ticker.upper()] + pids),
    ).fetchall()
    return [dict(row) for row in rows]


def get_asset_incomes(ticker: str, portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            i.ticker,
            i.income_type,
            i.amount,
            i.date,
            p.name AS portfolio_name
        FROM incomes i
        JOIN portfolios p ON p.id = i.portfolio_id
        WHERE i.ticker = ? AND i.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY i.date DESC, i.id DESC
        """,
        tuple([ticker.upper()] + pids),
    ).fetchall()
    return [dict(row) for row in rows]


def get_asset_position_summary(ticker: str, portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    from . import market_data

    asset = market_data.get_asset(ticker)
    if not asset:
        return {
            "shares": 0,
            "avg_price": 0.0,
            "total_value": 0.0,
            "market_value": 0.0,
            "open_pnl_value": 0.0,
            "open_pnl_pct": 0.0,
            "total_incomes": 0.0,
            "incomes_current_month": 0.0,
            "incomes_3m": 0.0,
            "incomes_12m": 0.0,
        }

    rows = db.execute(
        """
        SELECT tx_type, shares, price
        FROM transactions
        WHERE ticker = ? AND portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY date ASC, id ASC
        """,
        tuple([ticker.upper()] + pids),
    ).fetchall()

    shares = 0
    total_cost = 0.0

    for row in rows:
        tx_type = row["tx_type"]
        tx_shares = row["shares"]
        tx_price = row["price"]

        if tx_type == "buy":
            total_cost += tx_shares * tx_price
            shares += tx_shares
            continue

        if shares <= 0:
            continue

        avg_price = total_cost / shares
        sell_shares = min(tx_shares, shares)
        total_cost -= avg_price * sell_shares
        shares -= sell_shares

    avg_price = (total_cost / shares) if shares > 0 else 0.0
    total_value = total_cost
    market_value = shares * asset["price"]
    open_pnl_value = market_value - total_value
    open_pnl_pct = (open_pnl_value / total_value) * 100 if total_value > 0 else 0.0
    today = datetime.now().date()
    current_month_start = today.replace(day=1)

    def _subtract_months(date_value, months_back):
        year = date_value.year
        month = date_value.month - months_back
        while month <= 0:
            month += 12
            year -= 1
        return date_value.replace(year=year, month=month, day=1)

    start_3m = _subtract_months(current_month_start, 2)
    start_12m = _subtract_months(current_month_start, 11)

    income_row = db.execute(
        """
        SELECT
            COALESCE(SUM(amount), 0) AS total_incomes,
            COALESCE(SUM(CASE WHEN date >= ? THEN amount ELSE 0 END), 0) AS incomes_current_month,
            COALESCE(SUM(CASE WHEN date >= ? THEN amount ELSE 0 END), 0) AS incomes_3m,
            COALESCE(SUM(CASE WHEN date >= ? THEN amount ELSE 0 END), 0) AS incomes_12m
        FROM incomes
        WHERE ticker = ? AND portfolio_id IN ("""
        + placeholders
        + """)
        """,
        tuple(
            [
                current_month_start.strftime("%Y-%m-%d"),
                start_3m.strftime("%Y-%m-%d"),
                start_12m.strftime("%Y-%m-%d"),
                ticker.upper(),
            ]
            + pids
        ),
    ).fetchone()
    total_incomes = float(income_row["total_incomes"]) if income_row else 0.0
    incomes_current_month = float(income_row["incomes_current_month"]) if income_row else 0.0
    incomes_3m = float(income_row["incomes_3m"]) if income_row else 0.0
    incomes_12m = float(income_row["incomes_12m"]) if income_row else 0.0

    return {
        "shares": shares,
        "avg_price": round(avg_price, 2),
        "total_value": round(total_value, 2),
        "market_value": round(market_value, 2),
        "open_pnl_value": round(open_pnl_value, 2),
        "open_pnl_pct": round(open_pnl_pct, 2),
        "total_incomes": round(total_incomes, 2),
        "incomes_current_month": round(incomes_current_month, 2),
        "incomes_3m": round(incomes_3m, 2),
        "incomes_12m": round(incomes_12m, 2),
    }


def get_sectors_summary():
    db = get_db()
    rows = db.execute(
        """
        SELECT
            sector,
            COUNT(*) AS assets_count,
            ROUND(AVG(dy), 2) AS avg_dy,
            ROUND(SUM(market_cap_bi), 2) AS market_cap_bi
        FROM assets
        GROUP BY sector
        ORDER BY market_cap_bi DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def get_benchmark_comparison(portfolio_ids, range_key: str = "12m", scope_key: str = "all"):
    return legacy.get_benchmark_comparison(portfolio_ids, range_key=range_key, scope_key=scope_key)


def get_variable_income_value_daily_series(portfolio_ids, range_key: str = "90d"):
    return legacy.get_variable_income_value_daily_series(portfolio_ids, range_key=range_key)


def get_portfolio_snapshot(portfolio_ids, sort_by: str = "name", sort_dir: str = "asc"):
    return legacy.get_portfolio_snapshot(portfolio_ids, sort_by=sort_by, sort_dir=sort_dir)


def get_monthly_class_summary(portfolio_ids):
    return legacy.get_monthly_class_summary(portfolio_ids)


def get_monthly_ticker_summary(portfolio_ids, months=8):
    return legacy.get_monthly_ticker_summary(portfolio_ids, months=months)


def rebuild_chart_snapshots(portfolio_ids=None):
    return legacy.rebuild_chart_snapshots(portfolio_ids=portfolio_ids)

__all__ = [
    "add_fixed_income",
    "add_income",
    "add_transaction",
    "create_portfolio",
    "delete_fixed_incomes",
    "delete_incomes",
    "delete_portfolio",
    "delete_transactions",
    "get_asset_incomes",
    "get_asset_position_summary",
    "get_asset_transactions",
    "get_benchmark_comparison",
    "get_fixed_income_payload_cached",
    "get_fixed_income_summary",
    "get_fixed_incomes",
    "get_incomes",
    "get_monthly_class_summary",
    "get_monthly_ticker_summary",
    "get_portfolio_snapshot",
    "get_portfolios",
    "get_sectors_summary",
    "get_transactions",
    "get_variable_income_value_daily_series",
    "import_fixed_incomes_csv",
    "import_transactions_csv",
    "normalize_portfolio_ids",
    "rebuild_chart_snapshots",
    "rebuild_fixed_income_snapshots",
    "resolve_portfolio_id",
]
