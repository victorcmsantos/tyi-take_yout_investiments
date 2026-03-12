import json
import math
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Thread
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

from flask import Blueprint, current_app, has_request_context, jsonify, request, send_file

from .auth import (
    can_user_write,
    create_user_account,
    get_current_user,
    list_users,
    login_user,
    logout_current_user,
    require_admin_user,
    set_user_role,
    set_user_active_state,
)
from .db import create_database_backups, get_db, list_database_backups, resolve_database_backup_path
from .notifications import notify_event, send_telegram_text, telegram_status_payload
from .observability import build_health_payload, get_route_metrics
from .services import (
    add_fixed_income,
    add_income,
    add_transaction,
    create_portfolio,
    delete_portfolio,
    delete_fixed_incomes,
    delete_incomes,
    delete_transactions,
    get_asset,
    get_asset_enrichment,
    get_asset_enrichment_history,
    get_asset_incomes,
    get_asset_position_summary,
    get_asset_price_history,
    get_asset_upcoming_incomes,
    get_asset_transactions,
    get_benchmark_comparison,
    get_fixed_income_summary,
    get_fixed_income_payload_cached,
    get_fixed_incomes,
    get_incomes,
    get_monthly_class_summary,
    get_monthly_ticker_summary,
    get_metric_formulas_catalog,
    get_portfolio_snapshot,
    get_portfolios,
    get_sectors_summary,
    get_top_assets,
    get_transactions,
    get_variable_income_value_daily_series,
    import_fixed_incomes_csv,
    import_transactions_csv,
    normalize_portfolio_ids,
    refresh_assets_market_data,
    refresh_asset_market_data,
    resolve_portfolio_id,
    enrich_asset_with_openclaw,
    enrich_assets_with_openclaw_batch,
    update_metric_formula,
)
from .services import _legacy as legacy_market
api_bp = Blueprint("api", __name__)
_SCANNER_USER_NOTE_PATTERN = re.compile(r"^\[\[TYI_UID:(\d+)\]\]\s*")
_SAFE_SQL_IDENT_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SCANNER_TIMESTAMP_CANDIDATE_COLUMNS = (
    "created_at",
    "updated_at",
    "timestamp",
    "opened_at",
    "closed_at",
    "last_checked_at",
    "last_scan_at",
    "last_verified_at",
    "discovered_at",
    "signal_timestamp",
)


def _selected_portfolio_ids_from_request():
    raw_ids = request.args.getlist("portfolio_id")
    if not raw_ids:
        single = request.args.get("portfolio_id")
        if single:
            raw_ids = [single]
    return normalize_portfolio_ids(raw_ids)


def _json_ok(payload=None, status=200):
    return jsonify({"ok": True, "data": payload}), status


def _json_error(message, status=400, details=None):
    payload = {"ok": False, "error": message}
    if details is not None:
        payload["details"] = details
    return jsonify(payload), status


def _as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _now_iso_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_sql_ident(name: str):
    value = str(name or "").strip()
    if not _SAFE_SQL_IDENT_PATTERN.match(value):
        return None
    return value


def _json_compact(payload, limit: int = 4000):
    try:
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        text = str(payload)
    if len(text) <= limit:
        return text
    return f"{text[: max(limit - 3, 0)]}..."


def _notify_sync_event(
    event_key: str,
    title: str,
    *,
    details: dict | None = None,
    dedupe_key: str | None = None,
    min_interval_seconds: int | None = None,
):
    try:
        notify_event(
            event_key,
            title,
            details=details,
            dedupe_key=dedupe_key,
            min_interval_seconds=min_interval_seconds,
        )
    except Exception:
        current_app.logger.exception("Falha ao enviar notificacao de sync (event=%s).", event_key)


def _trade_field_from_sources(field: str, *sources):
    for source in sources:
        if isinstance(source, dict) and source.get(field) is not None:
            return source.get(field)
    return None


def _notify_swing_trade_event(
    *,
    event_key: str,
    title: str,
    user: dict | None,
    trade_id=None,
    ticker: str | None = None,
    payload: dict | None = None,
    target: dict | None = None,
    response_payload: dict | None = None,
    upstream_status: int | None = None,
    error_message: str | None = None,
):
    normalized_ticker = str(
        ticker
        or _trade_field_from_sources("ticker", payload, target, response_payload)
        or ""
    ).strip().upper()
    details = {
        "requested_by": str((user or {}).get("username") or ""),
        "trade_id": int(trade_id) if trade_id is not None else (
            int(response_payload.get("id")) if isinstance(response_payload, dict) and response_payload.get("id") is not None else None
        ),
        "ticker": normalized_ticker or None,
        "quantity": _safe_float(_trade_field_from_sources("quantity", payload, target, response_payload)),
        "entry_price": _safe_float(_trade_field_from_sources("entry_price", payload, target, response_payload)),
        "target_price": _safe_float(_trade_field_from_sources("target_price", payload, target, response_payload)),
        "stop_price": _safe_float(_trade_field_from_sources("stop_price", payload, target, response_payload)),
        "last_price": _safe_float(_trade_field_from_sources("last_price", payload, target, response_payload)),
        "exit_price": _safe_float(_trade_field_from_sources("exit_price", payload, target, response_payload)),
        "invested_amount": _safe_float(_trade_field_from_sources("invested_amount", payload, target, response_payload)),
        "pnl_pct": _safe_float(
            _trade_field_from_sources(
                "current_pnl_pct",
                response_payload,
                target,
            )
        ),
        "pnl_amount": _safe_float(
            _trade_field_from_sources(
                "current_pnl_amount",
                response_payload,
                target,
            )
        ),
        "status": str(_trade_field_from_sources("status", response_payload, target) or "").strip().upper() or None,
        "upstream_status": int(upstream_status) if upstream_status is not None else None,
        "error": str(error_message or "").strip() or None,
    }
    clean_details = {key: value for key, value in details.items() if value is not None and value != ""}
    dedupe_identity = str(clean_details.get("trade_id") or clean_details.get("ticker") or "na")
    _notify_sync_event(
        event_key,
        title,
        details=clean_details,
        dedupe_key=f"swing:{event_key}:{dedupe_identity}",
        min_interval_seconds=10 if event_key != "swing_trade_failed" else 60,
    )


def _parse_datetime_like(value):
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    numeric = raw.replace(".", "", 1)
    if numeric.isdigit():
        try:
            number = float(raw)
            if number > 1e12:
                number = number / 1000.0
            if number > 0:
                return datetime.fromtimestamp(number, tz=timezone.utc)
        except Exception:
            pass

    normalized = raw
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _safe_float(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _safe_date(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    base = raw.split("T", 1)[0].split(" ", 1)[0]
    try:
        return datetime.strptime(base, "%Y-%m-%d").date()
    except ValueError:
        return None


def _build_history_income_estimates(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids or [])
    if not pids:
        return {}

    placeholders = ",".join(["?"] * len(pids))
    rows = get_db().execute(
        """
        SELECT ticker, income_type, amount, date
        FROM incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY ticker ASC, date ASC
        """,
        tuple(pids),
    ).fetchall()

    by_ticker = {}
    for row in rows:
        ticker = str(row["ticker"] or "").strip().upper()
        if not ticker:
            continue
        if not legacy_market._is_brazilian_market_ticker(ticker):
            continue
        income_date = _safe_date(row["date"])
        amount = _safe_float(row["amount"])
        if income_date is None or amount is None or amount <= 0:
            continue
        by_ticker.setdefault(ticker, []).append(
            {
                "date": income_date,
                "amount": round(float(amount), 2),
                "income_type": str(row["income_type"] or "dividendo").strip().lower() or "dividendo",
            }
        )

    today = datetime.now(timezone.utc).date()
    estimates = {}
    for ticker, records in by_ticker.items():
        if len(records) < 2:
            continue
        unique_dates = sorted({item["date"] for item in records})
        if len(unique_dates) < 2:
            continue
        intervals = []
        for idx in range(1, len(unique_dates)):
            delta_days = int((unique_dates[idx] - unique_dates[idx - 1]).days)
            if delta_days > 0:
                intervals.append(delta_days)
        if not intervals:
            continue
        ordered = sorted(intervals)
        middle = len(ordered) // 2
        if len(ordered) % 2 == 0:
            cadence_days = int(round((ordered[middle - 1] + ordered[middle]) / 2.0))
        else:
            cadence_days = int(ordered[middle])
        if cadence_days < 20 or cadence_days > 130:
            continue

        projected = unique_dates[-1]
        for _ in range(24):
            projected = projected + timedelta(days=cadence_days)
            if projected >= today:
                break
        if projected < today:
            continue
        if (projected - today).days > 180:
            continue

        recent = records[-3:] if len(records) >= 3 else records
        amounts = [float(item["amount"]) for item in recent if float(item["amount"]) > 0]
        if not amounts:
            continue

        estimates[ticker] = {
            "income_type": recent[-1]["income_type"] if recent else "dividendo",
            "ex_date": projected.isoformat(),
            "payment_date": None,
            "estimated_total": round(sum(amounts) / len(amounts), 2),
            "source": "history_estimate",
        }
    return estimates


def _build_upcoming_incomes_payload(portfolio_ids, limit: int = 30):
    snapshot = get_portfolio_snapshot(portfolio_ids, sort_by="value", sort_dir="desc")
    positions = [item for item in (snapshot or {}).get("positions", []) if isinstance(item, dict)]
    include_history_estimates = _as_bool(os.getenv("UPCOMING_INCOME_HISTORY_ESTIMATE_ENABLED") or "1")
    history_estimates = _build_history_income_estimates(portfolio_ids) if include_history_estimates else {}

    all_items = []
    tickers_with_events = set()
    totals_by_currency = {}
    unknown_amount_count = 0
    history_estimated_count = 0

    for position in positions:
        ticker = str(position.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        if not legacy_market._is_brazilian_market_ticker(ticker):
            continue
        shares = _safe_float(position.get("shares")) or 0.0
        events = get_asset_upcoming_incomes(ticker, allow_live_fetch=False)
        if not events:
            estimate = history_estimates.get(ticker)
            if isinstance(estimate, dict):
                estimated_total = _safe_float(estimate.get("estimated_total"))
                if estimated_total is None:
                    unknown_amount_count += 1
                else:
                    totals_by_currency["BRL"] = round(
                        float(totals_by_currency.get("BRL") or 0.0) + estimated_total,
                        2,
                    )
                tickers_with_events.add(ticker)
                history_estimated_count += 1
                all_items.append(
                    {
                        "ticker": ticker,
                        "name": str(position.get("name") or "").strip(),
                        "shares": round(float(shares), 6),
                        "income_type": str(estimate.get("income_type") or "dividendo").strip().lower(),
                        "ex_date": estimate.get("ex_date"),
                        "payment_date": estimate.get("payment_date"),
                        "amount_per_share": None,
                        "currency": "BRL",
                        "estimated_total": estimated_total,
                        "source": str(estimate.get("source") or "history_estimate").strip(),
                    }
                )
            continue
        tickers_with_events.add(ticker)
        for event in events:
            if not isinstance(event, dict):
                continue
            amount = _safe_float(event.get("amount"))
            currency = str(event.get("currency") or "BRL").strip().upper() or "BRL"
            estimated_total = round(amount * shares, 2) if amount is not None and shares > 0 else None
            if estimated_total is None:
                unknown_amount_count += 1
            else:
                totals_by_currency[currency] = round(
                    float(totals_by_currency.get(currency) or 0.0) + estimated_total,
                    2,
                )
            all_items.append(
                {
                    "ticker": ticker,
                    "name": str(position.get("name") or "").strip(),
                    "shares": round(float(shares), 6),
                    "income_type": str(event.get("income_type") or "dividendo").strip().lower(),
                    "ex_date": event.get("ex_date"),
                    "payment_date": event.get("payment_date"),
                    "amount_per_share": round(float(amount), 6) if amount is not None else None,
                    "currency": currency,
                    "estimated_total": estimated_total,
                    "source": str(event.get("source") or "").strip(),
                }
            )

    all_items.sort(
        key=lambda item: (
            str(item.get("ex_date") or "9999-12-31"),
            str(item.get("payment_date") or "9999-12-31"),
            str(item.get("ticker") or ""),
        )
    )
    limited_items = list(all_items[: max(1, min(int(limit or 30), 200))])
    next_ex_date = limited_items[0].get("ex_date") if limited_items else None

    return {
        "generated_at": _now_iso_utc(),
        "portfolio_ids": list(portfolio_ids or []),
        "summary": {
            "positions_count": len(positions),
            "tickers_with_events": len(tickers_with_events),
            "events_count": len(all_items),
            "unknown_amount_count": int(unknown_amount_count),
            "history_estimated_count": int(history_estimated_count),
            "estimated_totals": totals_by_currency,
            "next_ex_date": next_ex_date,
        },
        "items": limited_items,
    }


def _scanner_db_status_payload():
    configured_path = str(current_app.config.get("MARKET_SCANNER_DATABASE_PATH") or "").strip()
    payload = {
        "configured_path": configured_path,
        "exists": False,
        "db_accessible": False,
        "size_bytes": 0,
        "file_modified_at": None,
        "last_data_update_at": None,
        "table_count": 0,
        "tables": [],
        "row_counts": {},
        "error": None,
    }
    if not configured_path:
        payload["error"] = "MARKET_SCANNER_DATABASE_PATH nao configurado."
        return payload

    db_path = Path(configured_path)
    payload["exists"] = db_path.exists()
    if not db_path.exists():
        payload["error"] = f"Banco do scanner nao encontrado em {configured_path}."
        return payload

    try:
        stat = db_path.stat()
        payload["size_bytes"] = int(stat.st_size)
        payload["file_modified_at"] = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(
            timespec="seconds"
        )
    except OSError as exc:
        payload["error"] = f"Falha ao ler metadados do banco: {exc}"
        return payload

    try:
        connection = sqlite3.connect(str(db_path), timeout=2)
        connection.row_factory = sqlite3.Row
        try:
            table_rows = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            ).fetchall()
            table_names = [
                row["name"]
                for row in table_rows
                if _safe_sql_ident(row["name"])
            ]
            payload["tables"] = table_names
            payload["table_count"] = len(table_names)
            payload["db_accessible"] = True

            latest_dt = None
            latest_raw = None
            for table_name in table_names:
                safe_table = _safe_sql_ident(table_name)
                if not safe_table:
                    continue
                row_count = connection.execute(f'SELECT COUNT(*) AS total FROM "{safe_table}"').fetchone()
                payload["row_counts"][safe_table] = int((row_count["total"] if row_count else 0) or 0)

                columns = [
                    row["name"]
                    for row in connection.execute(f'PRAGMA table_info("{safe_table}")').fetchall()
                    if _safe_sql_ident(row["name"])
                ]
                for column in columns:
                    if column not in _SCANNER_TIMESTAMP_CANDIDATE_COLUMNS:
                        continue
                    row = connection.execute(
                        f'SELECT MAX("{column}") AS value FROM "{safe_table}" WHERE "{column}" IS NOT NULL'
                    ).fetchone()
                    raw_value = row["value"] if row else None
                    if raw_value is None:
                        continue
                    raw_text = str(raw_value).strip()
                    if not raw_text:
                        continue
                    parsed_dt = _parse_datetime_like(raw_text)
                    if parsed_dt is not None:
                        if latest_dt is None or parsed_dt > latest_dt:
                            latest_dt = parsed_dt
                    elif latest_raw is None or raw_text > latest_raw:
                        latest_raw = raw_text

            if latest_dt is not None:
                payload["last_data_update_at"] = latest_dt.isoformat(timespec="seconds")
            elif latest_raw is not None:
                payload["last_data_update_at"] = latest_raw
        finally:
            connection.close()
    except Exception as exc:
        payload["db_accessible"] = False
        payload["error"] = str(exc)
    return payload


def _log_scanner_trade_audit(
    *,
    action: str,
    user: dict | None,
    trade_id=None,
    ticker: str | None = None,
    request_payload=None,
    response_payload=None,
    success: bool,
    upstream_status: int | None = None,
    error_message: str | None = None,
):
    try:
        actor_id = None
        if user and user.get("id") is not None:
            actor_id = int(user["id"])
        actor_username = str((user or {}).get("username") or "")
        remote_addr = ""
        if has_request_context():
            remote_addr = request.headers.get("X-Forwarded-For") or request.remote_addr or ""
        db = get_db()
        db.execute(
            """
            INSERT INTO scanner_trade_audit (
              action,
              user_id,
              username,
              trade_id,
              ticker,
              request_payload_json,
              response_payload_json,
              success,
              upstream_status,
              error_message,
              remote_addr,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(action or "").strip().lower(),
                actor_id,
                actor_username,
                int(trade_id) if trade_id is not None else None,
                str(ticker or "").strip().upper() or None,
                _json_compact(request_payload),
                _json_compact(response_payload),
                1 if success else 0,
                int(upstream_status) if upstream_status is not None else None,
                str(error_message or "").strip(),
                remote_addr,
                _now_iso_utc(),
            ),
        )
        db.commit()
    except Exception:
        current_app.logger.exception("Falha ao registrar auditoria de scanner trade.")


def _scanner_note_with_user(notes, user_id: int):
    clean = str(notes or "").strip()
    # Remove qualquer marcador anterior para evitar spoof de ownership.
    clean = _SCANNER_USER_NOTE_PATTERN.sub("", clean).strip()
    if clean:
        return f"[[TYI_UID:{int(user_id)}]] {clean}"
    return f"[[TYI_UID:{int(user_id)}]]"


def _scanner_user_id_from_notes(notes):
    match = _SCANNER_USER_NOTE_PATTERN.match(str(notes or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _scanner_strip_user_marker(notes):
    return _SCANNER_USER_NOTE_PATTERN.sub("", str(notes or "")).strip()


def _scanner_trade_visible_for_user(trade: dict, user: dict):
    owner_id = _scanner_user_id_from_notes((trade or {}).get("notes"))
    if owner_id is None:
        # Trades antigos sem marcador ficam acessiveis apenas para admin.
        return bool(user.get("is_admin"))
    return int(owner_id) == int(user["id"])


def _scanner_trade_to_client(trade: dict):
    payload = dict(trade or {})
    payload["notes"] = _scanner_strip_user_marker(payload.get("notes"))
    return payload


def _scanner_summary_from_trades(active, history):
    all_trades = list(active or []) + list(history or [])
    status_counts = {
        "open": sum(1 for trade in all_trades if str(trade.get("status")) == "OPEN"),
        "success": sum(1 for trade in all_trades if str(trade.get("status")) == "TARGET_HIT"),
        "failure": sum(1 for trade in all_trades if str(trade.get("status")) == "STOP_HIT"),
        "closed_profit": sum(1 for trade in all_trades if str(trade.get("status")) == "CLOSED_PROFIT"),
        "closed_loss": sum(1 for trade in all_trades if str(trade.get("status")) == "CLOSED_LOSS"),
    }
    open_invested_amount = round(
        sum(float((trade or {}).get("invested_amount") or 0.0) for trade in (active or [])),
        2,
    )
    open_pnl_amount = round(
        sum(float((trade or {}).get("current_pnl_amount") or 0.0) for trade in (active or [])),
        2,
    )
    return {
        "tracked_count": len(active or []),
        "history_count": len(history or []),
        "open_invested_amount": open_invested_amount,
        "open_pnl_amount": open_pnl_amount,
        **status_counts,
    }


def _scanner_filter_trades_payload_for_user(payload, user: dict):
    source = payload if isinstance(payload, dict) else {}
    raw_active = source.get("active") if isinstance(source.get("active"), list) else []
    raw_history = source.get("history") if isinstance(source.get("history"), list) else []
    visible_active = [
        _scanner_trade_to_client(item)
        for item in raw_active
        if isinstance(item, dict) and _scanner_trade_visible_for_user(item, user)
    ]
    visible_history = [
        _scanner_trade_to_client(item)
        for item in raw_history
        if isinstance(item, dict) and _scanner_trade_visible_for_user(item, user)
    ]
    visible_active = _scanner_attach_market_data(visible_active)
    visible_history = _scanner_attach_market_data(visible_history)
    return {
        "active": visible_active,
        "history": visible_history,
        "tracked_tickers": sorted({str(item.get("ticker") or "").upper() for item in visible_active if item.get("ticker")}),
        "summary": _scanner_summary_from_trades(visible_active, visible_history),
    }


def _log_trade_pnl_reconciliation_audit(
    *,
    trade_id: int,
    ticker: str,
    trade_status: str,
    divergence_pct: float,
    divergence_amount: float,
    payload: dict,
):
    try:
        db = get_db()
        db.execute(
            """
            INSERT INTO trade_pnl_reconciliation_audit (
                trade_id,
                ticker,
                trade_status,
                divergence_pct,
                divergence_amount,
                payload_json,
                detected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(trade_id),
                str(ticker or "").strip().upper(),
                str(trade_status or "").strip().upper(),
                float(divergence_pct or 0.0),
                float(divergence_amount or 0.0),
                _json_compact(payload, limit=2000),
                _now_iso_utc(),
            ),
        )
        db.commit()
    except Exception:
        current_app.logger.exception("Falha ao registrar auditoria de reconciliacao de PnL.")


def _scanner_reconcile_trade_item(trade: dict):
    item = dict(trade or {})
    quantity = _safe_float(item.get("quantity")) or 0.0
    invested_amount = _safe_float(item.get("invested_amount")) or 0.0
    entry_price = _safe_float(item.get("entry_price")) or 0.0
    last_price = _safe_float(item.get("last_price"))
    exit_price = _safe_float(item.get("exit_price"))
    status = str(item.get("status") or "").strip().upper()
    is_open = status == "OPEN"

    if quantity <= 0:
        quantity = 0.0
    if invested_amount <= 0 and quantity > 0 and entry_price > 0:
        invested_amount = quantity * entry_price

    reference_price = last_price if is_open else (exit_price if exit_price is not None else last_price)
    if reference_price is None:
        reference_price = entry_price if entry_price > 0 else 0.0

    market_value = float(reference_price) * quantity if quantity > 0 else 0.0
    pnl_amount = market_value - invested_amount
    pnl_pct = ((market_value / invested_amount) - 1.0) * 100.0 if invested_amount > 0 else 0.0

    provided_amount = _safe_float(item.get("current_pnl_amount" if is_open else "realized_pnl_amount"))
    provided_pct = _safe_float(item.get("current_pnl_pct" if is_open else "realized_pnl_pct"))
    delta_amount = 0.0 if provided_amount is None else (pnl_amount - provided_amount)
    delta_pct = 0.0 if provided_pct is None else (pnl_pct - provided_pct)
    divergence = bool(abs(delta_pct) >= 0.5 or abs(delta_amount) >= 0.01)

    item["current_market_value"] = round(float(market_value), 2)
    if is_open:
        item["current_pnl_amount"] = round(float(pnl_amount), 2)
        item["current_pnl_pct"] = round(float(pnl_pct), 2)
    else:
        item["realized_pnl_amount"] = round(float(pnl_amount), 2)
        item["realized_pnl_pct"] = round(float(pnl_pct), 2)

    item["pnl_reconciliation"] = {
        "checked_at": _now_iso_utc(),
        "divergence": divergence,
        "delta_pct": round(float(delta_pct), 4),
        "delta_amount": round(float(delta_amount), 4),
        "reference_price": round(float(reference_price), 6),
        "source_value_kind": "last_price" if is_open else ("exit_price" if exit_price is not None else "last_price"),
    }
    return item


def _scanner_reconcile_trades_payload(payload: dict):
    source = payload if isinstance(payload, dict) else {}
    active_rows = [item for item in (source.get("active") or []) if isinstance(item, dict)]
    history_rows = [item for item in (source.get("history") or []) if isinstance(item, dict)]

    active = []
    history = []
    for item in active_rows:
        reconciled = _scanner_reconcile_trade_item(item)
        if reconciled.get("pnl_reconciliation", {}).get("divergence"):
            _log_trade_pnl_reconciliation_audit(
                trade_id=int(reconciled.get("id") or 0),
                ticker=str(reconciled.get("ticker") or ""),
                trade_status=str(reconciled.get("status") or ""),
                divergence_pct=float((reconciled.get("pnl_reconciliation") or {}).get("delta_pct") or 0.0),
                divergence_amount=float((reconciled.get("pnl_reconciliation") or {}).get("delta_amount") or 0.0),
                payload=reconciled,
            )
        active.append(reconciled)

    for item in history_rows:
        reconciled = _scanner_reconcile_trade_item(item)
        if reconciled.get("pnl_reconciliation", {}).get("divergence"):
            _log_trade_pnl_reconciliation_audit(
                trade_id=int(reconciled.get("id") or 0),
                ticker=str(reconciled.get("ticker") or ""),
                trade_status=str(reconciled.get("status") or ""),
                divergence_pct=float((reconciled.get("pnl_reconciliation") or {}).get("delta_pct") or 0.0),
                divergence_amount=float((reconciled.get("pnl_reconciliation") or {}).get("delta_amount") or 0.0),
                payload=reconciled,
            )
        history.append(reconciled)

    return {
        **source,
        "active": active,
        "history": history,
        "summary": _scanner_summary_from_trades(active, history),
    }


def _scanner_trade_auto_close_metadata(trade: dict):
    status = str((trade or {}).get("status") or "").strip().upper()
    if status == "TARGET_HIT":
        return {
            "status": status,
            "exit_reason": "target_hit",
            "close_label": "alvo atingido",
        }
    if status == "STOP_HIT":
        return {
            "status": status,
            "exit_reason": "stop_hit",
            "close_label": "stop atingido",
        }
    return None


def _notify_scanner_auto_closed_trades(*, user: dict, trades_payload: dict):
    if not isinstance(user, dict) or user.get("id") is None:
        return

    history_rows = [
        item
        for item in ((trades_payload or {}).get("history") or [])
        if isinstance(item, dict)
    ]
    if not history_rows:
        return

    candidates = []
    for trade in history_rows:
        metadata = _scanner_trade_auto_close_metadata(trade)
        if not metadata:
            continue
        try:
            trade_id = int(trade.get("id") or 0)
        except (TypeError, ValueError):
            continue
        if trade_id <= 0:
            continue
        candidates.append((trade_id, trade, metadata))

    if not candidates:
        return

    user_id = int(user["id"])
    db = get_db()
    pending_notifications = []
    try:
        for trade_id, trade, metadata in candidates:
            ticker = str(trade.get("ticker") or "").strip().upper() or None
            cursor = db.execute(
                """
                INSERT OR IGNORE INTO scanner_trade_close_notifications (
                  user_id,
                  trade_id,
                  close_status,
                  exit_reason,
                  ticker,
                  notified_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    int(trade_id),
                    str(metadata["status"]),
                    str(metadata["exit_reason"]),
                    ticker,
                    _now_iso_utc(),
                ),
            )
            if int(cursor.rowcount or 0) > 0:
                pending_notifications.append((trade_id, trade, metadata))
        if pending_notifications:
            db.commit()
    except Exception:
        db.rollback()
        current_app.logger.exception(
            "Falha ao registrar fechamento automatico de swing trade para notificacao."
        )
        return

    for trade_id, trade, metadata in pending_notifications:
        _notify_swing_trade_event(
            event_key="swing_trade_closed",
            title=f"Swing trade encerrada automaticamente ({metadata['close_label']})",
            user=user,
            trade_id=trade_id,
            ticker=trade.get("ticker"),
            target=trade,
            response_payload=trade,
            upstream_status=200,
        )


def _scanner_prepare_trade_payload(payload, user: dict, force_notes: bool = False):
    prepared = dict(payload or {})
    prepared["user_id"] = int(user["id"])
    if force_notes or "notes" in prepared:
        prepared["notes"] = _scanner_note_with_user(prepared.get("notes"), int(user["id"]))
    return prepared


def _scanner_base_ticker(value):
    ticker = str(value or "").strip().upper()
    if not ticker:
        return ""
    return ticker.removesuffix(".SA")


def _scanner_market_data_map_for_tickers(tickers):
    base_tickers = sorted({_scanner_base_ticker(item) for item in (tickers or []) if _scanner_base_ticker(item)})
    if not base_tickers:
        return {}
    placeholders = ",".join(["?"] * len(base_tickers))
    try:
        rows = get_db().execute(
            f"""
            SELECT ticker, market_data_source, market_data_updated_at
            FROM assets
            WHERE ticker IN ({placeholders})
            """,
            tuple(base_tickers),
        ).fetchall()
    except Exception:
        return {}
    payload = {}
    for row in rows:
        base = _scanner_base_ticker(row["ticker"])
        payload[base] = {
            "source": str(row["market_data_source"] or "").strip() or "market_scanner",
            "updated_at": row["market_data_updated_at"],
        }
    return payload


def _scanner_attach_market_data(items):
    source_items = [dict(item) for item in (items or []) if isinstance(item, dict)]
    if not source_items:
        return []
    by_ticker = _scanner_market_data_map_for_tickers([item.get("ticker") for item in source_items])
    output = []
    for item in source_items:
        base = _scanner_base_ticker(item.get("ticker"))
        market_data = by_ticker.get(base, {})
        output.append(
            {
                **item,
                "market_data": {
                    "source": str(market_data.get("source") or "").strip() or "market_scanner",
                    "updated_at": market_data.get("updated_at"),
                },
            }
        )
    return output


def _market_scanner_base_url():
    return (os.getenv("MARKET_SCANNER_BASE_URL") or "http://market-scanner:8000").rstrip("/")


def _market_scanner_timeout_seconds():
    raw = (os.getenv("MARKET_SCANNER_TIMEOUT_SECONDS") or "8").strip()
    try:
        return max(float(raw), 1.0)
    except (TypeError, ValueError):
        return 8.0


def _market_scanner_scan_timeout_seconds():
    raw = (os.getenv("MARKET_SCANNER_SCAN_TIMEOUT_SECONDS") or "600").strip()
    try:
        return max(float(raw), _market_scanner_timeout_seconds())
    except (TypeError, ValueError):
        return 600.0


def _market_scanner_get(path: str):
    return _market_scanner_request("GET", path)


def _market_scanner_request(method: str, path: str, payload=None, timeout_seconds=None):
    user = get_current_user() if has_request_context() else None
    query_items = []
    if has_request_context():
        query_items = [
            (key, value)
            for key, value in request.args.items(multi=True)
            if key not in {"user_id", "scanner_user_id"}
        ]
    if user and user.get("id") is not None:
        query_items.append(("user_id", str(int(user["id"]))))
    query = urlparse.urlencode(query_items, doseq=True)
    url = f"{_market_scanner_base_url()}{path}"
    if query:
        url = f"{url}?{query}"

    body = None
    headers = {"Accept": "application/json"}
    if user and user.get("id") is not None:
        headers["X-TYI-User-Id"] = str(int(user["id"]))
        headers["X-TYI-Username"] = str(user.get("username") or "")
    if payload is not None:
        if user and isinstance(payload, dict):
            payload = {**payload, "user_id": int(user["id"])}
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urlrequest.Request(url, data=body, headers=headers, method=method.upper())
    timeout_value = float(timeout_seconds) if timeout_seconds is not None else _market_scanner_timeout_seconds()
    try:
        with urlrequest.urlopen(req, timeout=timeout_value) as response:
            status = int(response.getcode() or 200)
            raw_body = response.read().decode("utf-8", errors="replace")
    except urlerror.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        return False, int(exc.code or 502), _market_scanner_parse_body(raw_body)
    except Exception as exc:
        return False, 503, str(exc)

    return True, status, _market_scanner_parse_body(raw_body)


def _market_scanner_parse_body(raw_body: str):
    try:
        return json.loads(raw_body) if raw_body else None
    except json.JSONDecodeError:
        return {"raw": raw_body}


def _market_scanner_proxy_get(path: str):
    return _market_scanner_proxy("GET", path)


def _market_scanner_result_to_api_response(ok: bool, status: int, response_payload):
    if not ok:
        message = _market_scanner_error_message(response_payload, status)
        return _json_error(
            message,
            status=status,
            details={"upstream": response_payload},
        )
    return _json_ok(response_payload, status=status)


def _market_scanner_proxy(method: str, path: str, payload=None):
    ok, status, response_payload = _market_scanner_request(method, path, payload=payload)
    return _market_scanner_result_to_api_response(ok, status, response_payload)


def _market_scanner_error_message(payload, status: int):
    if isinstance(payload, dict):
        detail = payload.get("detail") or payload.get("error")
        if isinstance(detail, str) and detail.strip():
            return detail.strip()
    if isinstance(payload, str) and payload.strip():
        return payload.strip()[:300]
    if status >= 500:
        return "Market Scanner indisponivel."
    return "Falha ao processar requisicao no Market Scanner."


def _scanner_ticker_catalog_summary():
    payload = {
        "available": False,
        "total_tickers": 0,
        "active_tickers": 0,
        "yahoo_supported_tickers": 0,
        "last_scan_at": None,
        "error": None,
    }
    db = get_db()
    try:
        row = db.execute(
            """
            SELECT
              COUNT(*) AS total_tickers,
              SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_tickers,
              SUM(CASE WHEN yahoo_supported = 1 THEN 1 ELSE 0 END) AS yahoo_supported_tickers,
              MAX(last_scan_at) AS last_scan_at
            FROM tickers
            """
        ).fetchone()
    except Exception as exc:
        payload["error"] = str(exc)
        return payload

    payload["available"] = True
    payload["total_tickers"] = int((row["total_tickers"] if row else 0) or 0)
    payload["active_tickers"] = int((row["active_tickers"] if row else 0) or 0)
    payload["yahoo_supported_tickers"] = int((row["yahoo_supported_tickers"] if row else 0) or 0)
    payload["last_scan_at"] = row["last_scan_at"] if row else None
    return payload


def _scanner_assets_sync_summary():
    payload = {
        "available": False,
        "assets_total": 0,
        "assets_market_scanner_source": 0,
        "last_market_data_updated_at": None,
        "error": None,
    }
    db = get_db()
    try:
        row = db.execute(
            """
            SELECT
              COUNT(*) AS assets_total,
              SUM(CASE WHEN market_data_source = 'market_scanner' THEN 1 ELSE 0 END) AS assets_market_scanner_source,
              MAX(market_data_updated_at) AS last_market_data_updated_at
            FROM assets
            """
        ).fetchone()
    except Exception as exc:
        payload["error"] = str(exc)
        return payload

    payload["available"] = True
    payload["assets_total"] = int((row["assets_total"] if row else 0) or 0)
    payload["assets_market_scanner_source"] = int(
        (row["assets_market_scanner_source"] if row else 0) or 0
    )
    payload["last_market_data_updated_at"] = row["last_market_data_updated_at"] if row else None
    return payload


def _scanner_manual_scan_row_to_payload(row):
    if not row:
        return None
    payload = dict(row)
    payload["id"] = int(payload.get("id") or 0)
    payload["total_tickers"] = int(payload.get("total_tickers") or 0)
    payload["processed_tickers"] = int(payload.get("processed_tickers") or 0)
    payload["triggered_signals"] = int(payload.get("triggered_signals") or 0)
    payload["requested_by_user_id"] = (
        int(payload["requested_by_user_id"]) if payload.get("requested_by_user_id") is not None else None
    )
    payload["upstream_status"] = int(payload["upstream_status"]) if payload.get("upstream_status") is not None else None
    started_dt = _parse_datetime_like(payload.get("started_at"))
    finished_dt = _parse_datetime_like(payload.get("finished_at"))
    payload["duration_ms"] = (
        round((finished_dt - started_dt).total_seconds() * 1000.0, 2)
        if started_dt is not None and finished_dt is not None and finished_dt >= started_dt
        else None
    )
    return payload


def _scanner_manual_scan_live_progress(started_at, planned_total: int):
    output = {
        "processed_tickers_live": 0,
        "progress_percent": None,
    }
    started_dt = _parse_datetime_like(started_at)
    if started_dt is None:
        return output

    db = get_db()
    try:
        rows = db.execute("SELECT last_scan_at FROM tickers WHERE last_scan_at IS NOT NULL").fetchall()
    except Exception:
        return output

    processed = 0
    for row in rows:
        dt = _parse_datetime_like(row["last_scan_at"])
        if dt is not None and dt >= started_dt:
            processed += 1

    total = int(planned_total or 0)
    if total <= 0:
        total = len(rows)

    output["processed_tickers_live"] = int(processed)
    if total > 0:
        output["progress_percent"] = round(max(0.0, min((processed / total) * 100.0, 100.0)), 2)
    return output


def _scanner_upstream_scan_status_payload():
    ok, status, response_payload = _market_scanner_request("GET", "/scan/status")
    if not ok:
        return {
            "available": False,
            "running": False,
            "upstream_status": int(status or 0),
            "error": _market_scanner_error_message(response_payload, status),
        }

    payload = response_payload if isinstance(response_payload, dict) else {}
    return {
        "available": True,
        "upstream_status": int(status or 0),
        "running": bool(payload.get("running")),
        "active_run_id": payload.get("active_run_id"),
        "active_scope": payload.get("active_scope"),
        "active_requested_symbols": payload.get("active_requested_symbols") or [],
        "active_requested_count": int(payload.get("active_requested_count") or 0),
        "active_force": bool(payload.get("active_force")),
        "active_started_at": payload.get("active_started_at"),
        "last_summary": payload.get("last_summary") if isinstance(payload.get("last_summary"), dict) else {},
        "last_finished_at": payload.get("last_finished_at"),
        "error": None,
    }


def _scanner_manual_scan_status_payload():
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT
              id,
              status,
              requested_by_user_id,
              requested_by_username,
              started_at,
              finished_at,
              total_tickers,
              processed_tickers,
              triggered_signals,
              upstream_status,
              error_message
            FROM scanner_manual_scan_runs
            ORDER BY id DESC
            LIMIT 8
            """
        ).fetchall()
    except Exception as exc:
        return {
            "scanner_db": _scanner_db_status_payload(),
            "catalog": _scanner_ticker_catalog_summary(),
            "assets_sync": _scanner_assets_sync_summary(),
            "current_run": None,
            "last_run": None,
            "history": [],
            "error": str(exc),
        }
    items = [_scanner_manual_scan_row_to_payload(row) for row in rows]
    current_run = next((item for item in items if (item or {}).get("status") == "running"), None)
    last_run = next((item for item in items if (item or {}).get("status") != "running"), None)

    if current_run:
        live = _scanner_manual_scan_live_progress(
            current_run.get("started_at"),
            int(current_run.get("total_tickers") or 0),
        )
        current_run = {
            **current_run,
            **live,
            "processed_tickers": max(
                int(current_run.get("processed_tickers") or 0),
                int(live.get("processed_tickers_live") or 0),
            ),
        }

    if last_run:
        total = int(last_run.get("total_tickers") or 0)
        done = int(last_run.get("processed_tickers") or 0)
        last_run = dict(last_run)
        last_run["progress_percent"] = (
            round(max(0.0, min((done / total) * 100.0, 100.0)), 2) if total > 0 else None
        )

    return {
        "scanner_db": _scanner_db_status_payload(),
        "catalog": _scanner_ticker_catalog_summary(),
        "assets_sync": _scanner_assets_sync_summary(),
        "upstream_scan": _scanner_upstream_scan_status_payload(),
        "current_run": current_run,
        "last_run": last_run,
        "history": items[:5],
    }


def _scanner_manual_scan_worker(app, run_id: int):
    with app.app_context():
        ok = False
        status = 503
        response_payload = None
        error_message = ""
        try:
            ok, status, response_payload = _market_scanner_request(
                "POST",
                "/scan",
                payload={},
                timeout_seconds=_market_scanner_scan_timeout_seconds(),
            )
            if not ok:
                error_message = _market_scanner_error_message(response_payload, status)
        except Exception as exc:
            ok = False
            status = 503
            response_payload = str(exc)
            error_message = str(exc)

        summary = {}
        if isinstance(response_payload, dict) and isinstance(response_payload.get("scan_summary"), dict):
            summary = dict(response_payload.get("scan_summary") or {})
        processed_tickers = int(
            summary.get("tickers_processed")
            or summary.get("tickers_loaded")
            or 0
        )
        triggered_signals = int(summary.get("signals_triggered") or 0)

        finished_at = _now_iso_utc()
        try:
            timeout_seconds = float(current_app.config.get("SQLITE_TIMEOUT_SECONDS", 30))
            connection = sqlite3.connect(current_app.config["DATABASE"], timeout=timeout_seconds)
            try:
                connection.row_factory = sqlite3.Row
                connection.execute(f"PRAGMA busy_timeout = {int(timeout_seconds * 1000)}")
                connection.execute(
                    """
                    UPDATE scanner_manual_scan_runs
                    SET
                      status = ?,
                      finished_at = ?,
                      processed_tickers = CASE
                        WHEN ? > processed_tickers THEN ?
                        ELSE processed_tickers
                      END,
                      triggered_signals = ?,
                      upstream_status = ?,
                      error_message = ?
                    WHERE id = ?
                    """,
                    (
                        "success" if ok else "failed",
                        finished_at,
                        processed_tickers,
                        processed_tickers,
                        triggered_signals,
                        int(status or 0),
                        "" if ok else str(error_message or "Falha ao executar scan manual."),
                        int(run_id),
                    ),
                )
                connection.commit()
            finally:
                connection.close()
        except Exception:
            current_app.logger.exception(
                "Falha ao persistir resultado do scan manual do scanner (run_id=%s).",
                int(run_id),
            )
        if ok:
            _notify_sync_event(
                "manual_scan_success",
                "Scan manual finalizado com sucesso",
                details={
                    "run_id": int(run_id),
                    "upstream_status": int(status or 0),
                    "processed_tickers": int(processed_tickers),
                    "triggered_signals": int(triggered_signals),
                },
                dedupe_key=f"scan:manual:success:{int(run_id)}",
                min_interval_seconds=5,
            )
        else:
            _notify_sync_event(
                "manual_scan_failed",
                "Falha no scan manual",
                details={
                    "run_id": int(run_id),
                    "upstream_status": int(status or 0),
                    "processed_tickers": int(processed_tickers),
                    "triggered_signals": int(triggered_signals),
                    "error": str(error_message or "Falha ao executar scan manual."),
                },
                dedupe_key=f"scan:manual:failed:{int(run_id)}",
                min_interval_seconds=5,
            )


def _start_scanner_manual_scan(user: dict):
    db = get_db()
    running_row = db.execute(
        """
        SELECT
          id,
          status,
          requested_by_user_id,
          requested_by_username,
          started_at,
          finished_at,
          total_tickers,
          processed_tickers,
          triggered_signals,
          upstream_status,
          error_message
        FROM scanner_manual_scan_runs
        WHERE status = 'running'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if running_row is not None:
        started_dt = _parse_datetime_like(running_row["started_at"])
        timeout_seconds = _market_scanner_scan_timeout_seconds()
        timed_out = (
            started_dt is not None
            and (datetime.now(timezone.utc) - started_dt).total_seconds() > (timeout_seconds + 120.0)
        )
        if timed_out:
            db.execute(
                """
                UPDATE scanner_manual_scan_runs
                SET
                  status = 'failed',
                  finished_at = ?,
                  upstream_status = 504,
                  error_message = ?
                WHERE id = ?
                """,
                (
                    _now_iso_utc(),
                    "Execucao manual marcada como timeout por exceder o limite.",
                    int(running_row["id"]),
                ),
            )
            db.commit()
            running_row = None
    if running_row is not None:
        return {
            "started": False,
            "run": _scanner_manual_scan_row_to_payload(running_row),
        }

    catalog = _scanner_ticker_catalog_summary()
    planned_total = int(catalog.get("active_tickers") or 0) or int(catalog.get("total_tickers") or 0)
    started_at = _now_iso_utc()
    try:
        db.execute(
            """
            INSERT INTO scanner_manual_scan_runs (
              status,
              requested_by_user_id,
              requested_by_username,
              started_at,
              total_tickers,
              processed_tickers,
              triggered_signals,
              error_message
            )
            VALUES (?, ?, ?, ?, ?, 0, 0, '')
            """,
            (
                "running",
                int(user.get("id")) if user and user.get("id") is not None else None,
                str((user or {}).get("username") or ""),
                started_at,
                planned_total,
            ),
        )
    except sqlite3.IntegrityError:
        running_row = db.execute(
            """
            SELECT
              id,
              status,
              requested_by_user_id,
              requested_by_username,
              started_at,
              finished_at,
              total_tickers,
              processed_tickers,
              triggered_signals,
              upstream_status,
              error_message
            FROM scanner_manual_scan_runs
            WHERE status = 'running'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        return {
            "started": False,
            "run": _scanner_manual_scan_row_to_payload(running_row),
        }
    run_id = int(db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
    db.commit()

    app_obj = current_app._get_current_object()
    Thread(
        target=_scanner_manual_scan_worker,
        args=(app_obj, run_id),
        daemon=True,
        name=f"scanner-manual-scan-{run_id}",
    ).start()
    _notify_sync_event(
        "manual_scan_started",
        "Scan manual iniciado",
        details={
            "run_id": int(run_id),
            "requested_by": str((user or {}).get("username") or ""),
            "planned_total": int(planned_total),
        },
        dedupe_key=f"scan:manual:started:{int(run_id)}",
        min_interval_seconds=5,
    )

    run_row = db.execute(
        """
        SELECT
          id,
          status,
          requested_by_user_id,
          requested_by_username,
          started_at,
          finished_at,
          total_tickers,
          processed_tickers,
          triggered_signals,
          upstream_status,
          error_message
        FROM scanner_manual_scan_runs
        WHERE id = ?
        """,
        (run_id,),
    ).fetchone()
    return {
        "started": True,
        "run": _scanner_manual_scan_row_to_payload(run_row),
    }


def _build_charts_core_payload(portfolio_ids):
    portfolio = get_portfolio_snapshot(portfolio_ids, sort_by="name", sort_dir="asc")
    fixed_income_payload = get_fixed_income_payload_cached(
        portfolio_ids, sort_by="date_aporte", sort_dir="desc"
    )
    fixed_income = fixed_income_payload["summary"]
    fixed_income_items = fixed_income_payload["items"]
    monthly_class_summary = get_monthly_class_summary(portfolio_ids)

    category_labels = {
        "br_stocks": "Acoes BR",
        "us_stocks": "Acoes US",
        "crypto": "Cripto",
        "fiis": "FIIs",
    }
    category_chart = {"labels": [], "values": []}
    for key in ("br_stocks", "us_stocks", "crypto", "fiis"):
        value = float(portfolio["group_totals"].get(key, 0.0))
        if value <= 0:
            continue
        category_chart["labels"].append(category_labels[key])
        category_chart["values"].append(round(value, 2))

    top_positions = sorted(
        portfolio["positions"], key=lambda item: item.get("value", 0.0), reverse=True
    )[:10]
    top_assets_chart = {
        "labels": [item["ticker"] for item in top_positions],
        "values": [round(float(item.get("value", 0.0)), 2) for item in top_positions],
    }

    allocation_by_group_charts = []
    for key in ("br_stocks", "us_stocks", "fiis", "crypto"):
        items = portfolio["grouped_positions"].get(key, [])
        if not items:
            continue
        group_total = sum(float(item.get("value", 0.0)) for item in items)
        if group_total <= 0:
            continue
        allocation_by_group_charts.append(
            {
                "id": f"chart-allocation-{key}",
                "title": f"Composicao por ativo - {category_labels[key]}",
                "labels": [item["ticker"] for item in items],
                "values": [round(float(item.get("value", 0.0)), 2) for item in items],
                "weights": [
                    round((float(item.get("value", 0.0)) / group_total) * 100, 2)
                    for item in items
                ],
            }
        )

    cards_chart = {
        "labels": ["Patrimonio", "Investido em aberto", "Proventos totais"],
        "values": [
            round(float(portfolio["total_value"]), 2),
            round(float(portfolio["invested_value"]), 2),
            round(float(portfolio["total_incomes"]), 2),
        ],
    }

    cri_keywords = ("CRI", "CRA", "DEB")
    cri_result = 0.0
    for item in fixed_income_items:
        investment_name = (item.get("investment_type") or "").upper()
        if any(key in investment_name for key in cri_keywords):
            cri_result += float(item.get("current_income", 0.0))

    result_by_category_chart = {
        "labels": ["US", "FIIs", "BR", "Cripto", "CRI/CRA/DEB"],
        "values": [
            round(float(portfolio["group_summaries"]["us_stocks"]["open_pnl_value"]), 2),
            round(float(portfolio["group_summaries"]["fiis"]["open_pnl_value"]), 2),
            round(float(portfolio["group_summaries"]["br_stocks"]["open_pnl_value"]), 2),
            round(float(portfolio["group_summaries"]["crypto"]["open_pnl_value"]), 2),
            round(float(cri_result), 2),
        ],
    }

    fixed_income_by_investment = {}
    fixed_income_by_distributor = {}
    fixed_income_by_issuer = {}
    for item in fixed_income_items:
        value = float(item.get("active_applied_value", 0.0))
        income_value = float(item.get("current_income", 0.0))
        investment_type = (item.get("investment_type") or "Nao informado").strip()
        distributor = (item.get("distributor") or "Nao informado").strip()
        issuer = (item.get("issuer") or "Nao informado").strip()
        if value > 0:
            fixed_income_by_investment[investment_type] = (
                fixed_income_by_investment.get(investment_type, 0.0) + value
            )
            fixed_income_by_distributor[distributor] = (
                fixed_income_by_distributor.get(distributor, 0.0) + value
            )
        if value > 0 or income_value != 0:
            state = fixed_income_by_issuer.get(issuer, {"investment": 0.0, "income": 0.0})
            state["investment"] += max(value, 0.0)
            state["income"] += income_value
            fixed_income_by_issuer[issuer] = state

    investment_pairs = sorted(
        fixed_income_by_investment.items(), key=lambda pair: pair[1], reverse=True
    )
    distributor_pairs = sorted(
        fixed_income_by_distributor.items(), key=lambda pair: pair[1], reverse=True
    )
    issuer_pairs = sorted(
        fixed_income_by_issuer.items(), key=lambda pair: pair[1]["investment"], reverse=True
    )

    monthly_income_rows = list(monthly_class_summary)
    last_income_idx = -1
    for idx, row in enumerate(monthly_income_rows):
        fii_income = float(row.get("fii_incomes", 0.0))
        acoes_income = float(row.get("br_incomes", 0.0))
        if (fii_income + acoes_income) != 0:
            last_income_idx = idx
    if last_income_idx >= 0:
        monthly_income_rows = monthly_income_rows[: last_income_idx + 1]
        monthly_income_rows = [
            row
            for row in monthly_income_rows
            if (float(row.get("fii_incomes", 0.0)) + float(row.get("br_incomes", 0.0))) != 0
        ]

    return {
        "category_chart": category_chart,
        "top_assets_chart": top_assets_chart,
        "allocation_by_group_charts": allocation_by_group_charts,
        "cards_chart": cards_chart,
        "result_by_category_chart": result_by_category_chart,
        "classes_chart": {
            "labels": ["Renda Variavel", "Renda Fixa"],
            "values": [
                round(float(portfolio["total_value"]), 2),
                round(float(fixed_income["current_total"]), 2),
            ],
        },
        "fixed_income_investment_chart": {
            "labels": [label for label, _ in investment_pairs],
            "values": [round(value, 2) for _, value in investment_pairs],
        },
        "fixed_income_distributor_chart": {
            "labels": [label for label, _ in distributor_pairs],
            "values": [round(value, 2) for _, value in distributor_pairs],
        },
        "fixed_income_issuer_chart": {
            "labels": [label for label, _ in issuer_pairs],
            "investment_values": [
                round(float(payload["investment"]), 2) for _, payload in issuer_pairs
            ],
            "income_values": [
                round(float(payload["income"]), 2) for _, payload in issuer_pairs
            ],
        },
        "monthly_class_summary": monthly_class_summary,
        "monthly_income_chart": {
            "labels": [row.get("label", "") for row in monthly_income_rows],
            "fii_values": [
                round(float(row.get("fii_incomes", 0.0)), 2) for row in monthly_income_rows
            ],
            "acoes_values": [
                round(float(row.get("br_incomes", 0.0)), 2) for row in monthly_income_rows
            ],
        },
        "snapshot": {
            "fixed_income": bool(fixed_income_payload.get("snapshot")),
        },
    }


@api_bp.route("/health", methods=["GET"])
def health():
    payload = build_health_payload()
    status_code = 200 if payload["status"] == "ok" else 503
    return _json_ok(payload, status=status_code)


@api_bp.route("/scanner/health", methods=["GET"])
def scanner_health():
    return _market_scanner_proxy_get("/signal-matrix")


@api_bp.route("/scanner/signals", methods=["GET"])
def scanner_signals():
    ok, status, response_payload = _market_scanner_request("GET", "/signals")
    if not ok:
        return _market_scanner_result_to_api_response(ok, status, response_payload)
    payload = response_payload if isinstance(response_payload, list) else []
    return _json_ok(_scanner_attach_market_data(payload), status=status)


@api_bp.route("/scanner/signal-matrix", methods=["GET"])
def scanner_signal_matrix():
    ok, status, response_payload = _market_scanner_request("GET", "/signal-matrix")
    return _market_scanner_result_to_api_response(ok, status, response_payload)


@api_bp.route("/scanner/trades", methods=["GET"])
def scanner_trades():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    ok, status, response_payload = _market_scanner_request("GET", "/trades")
    if not ok:
        message = _market_scanner_error_message(response_payload, status)
        return _json_error(message, status=status, details={"upstream": response_payload})
    filtered = _scanner_filter_trades_payload_for_user(response_payload, user)
    reconciled = _scanner_reconcile_trades_payload(filtered)
    _notify_scanner_auto_closed_trades(user=user, trades_payload=reconciled)
    return _json_ok(reconciled, status=status)


@api_bp.route("/scanner/trades", methods=["POST"])
def scanner_create_trade():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    incoming_payload = request.get_json(silent=True) or request.form.to_dict()
    payload = _scanner_prepare_trade_payload(incoming_payload, user, force_notes=True)
    ok, status, response_payload = _market_scanner_request("POST", "/trades", payload=payload)
    error_message = None if ok else _market_scanner_error_message(response_payload, status)
    _log_scanner_trade_audit(
        action="create",
        user=user,
        trade_id=(response_payload or {}).get("id") if isinstance(response_payload, dict) else None,
        ticker=payload.get("ticker"),
        request_payload=payload,
        response_payload=response_payload,
        success=ok,
        upstream_status=status,
        error_message=error_message,
    )
    if ok:
        _notify_swing_trade_event(
            event_key="swing_trade_opened",
            title="Swing trade aberta",
            user=user,
            trade_id=(response_payload or {}).get("id") if isinstance(response_payload, dict) else None,
            ticker=payload.get("ticker"),
            payload=payload,
            response_payload=response_payload if isinstance(response_payload, dict) else None,
            upstream_status=status,
        )
    else:
        _notify_swing_trade_event(
            event_key="swing_trade_failed",
            title="Falha ao abrir swing trade",
            user=user,
            ticker=payload.get("ticker"),
            payload=payload,
            response_payload=response_payload if isinstance(response_payload, dict) else None,
            upstream_status=status,
            error_message=error_message,
        )
    return _market_scanner_result_to_api_response(ok, status, response_payload)


@api_bp.route("/scanner/trades/<int:trade_id>", methods=["PATCH"])
def scanner_update_trade(trade_id: int):
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)

    ok, status, trades_payload = _market_scanner_request("GET", "/trades")
    if not ok:
        message = _market_scanner_error_message(trades_payload, status)
        _log_scanner_trade_audit(
            action="update",
            user=user,
            trade_id=trade_id,
            request_payload=None,
            response_payload=trades_payload,
            success=False,
            upstream_status=status,
            error_message=message,
        )
        _notify_swing_trade_event(
            event_key="swing_trade_failed",
            title="Falha ao ajustar swing trade",
            user=user,
            trade_id=trade_id,
            upstream_status=status,
            error_message=message,
        )
        return _json_error(message, status=status, details={"upstream": trades_payload})

    all_trades = []
    if isinstance(trades_payload, dict):
        all_trades.extend(item for item in (trades_payload.get("active") or []) if isinstance(item, dict))
        all_trades.extend(item for item in (trades_payload.get("history") or []) if isinstance(item, dict))
    target = next((item for item in all_trades if int(item.get("id") or 0) == int(trade_id)), None)
    if target is None:
        _log_scanner_trade_audit(
            action="update",
            user=user,
            trade_id=trade_id,
            request_payload=None,
            response_payload=None,
            success=False,
            upstream_status=404,
            error_message="Trade nao encontrado.",
        )
        _notify_swing_trade_event(
            event_key="swing_trade_failed",
            title="Falha ao ajustar swing trade",
            user=user,
            trade_id=trade_id,
            upstream_status=404,
            error_message="Trade nao encontrado.",
        )
        return _json_error("Trade nao encontrado.", status=404)
    if not _scanner_trade_visible_for_user(target, user):
        _log_scanner_trade_audit(
            action="update",
            user=user,
            trade_id=trade_id,
            ticker=target.get("ticker"),
            request_payload=None,
            response_payload=None,
            success=False,
            upstream_status=403,
            error_message="Trade nao pertence ao usuario atual.",
        )
        _notify_swing_trade_event(
            event_key="swing_trade_failed",
            title="Falha ao ajustar swing trade",
            user=user,
            trade_id=trade_id,
            ticker=target.get("ticker"),
            target=target,
            upstream_status=403,
            error_message="Trade nao pertence ao usuario atual.",
        )
        return _json_error("Trade nao pertence ao usuario atual.", status=403)

    payload = request.get_json(silent=True) or request.form.to_dict()
    payload = _scanner_prepare_trade_payload(payload, user, force_notes=("notes" in payload))
    ok, status, response_payload = _market_scanner_request("PATCH", f"/trades/{trade_id}", payload=payload)
    error_message = None if ok else _market_scanner_error_message(response_payload, status)
    _log_scanner_trade_audit(
        action="update",
        user=user,
        trade_id=trade_id,
        ticker=target.get("ticker"),
        request_payload=payload,
        response_payload=response_payload,
        success=ok,
        upstream_status=status,
        error_message=error_message,
    )
    if ok:
        _notify_swing_trade_event(
            event_key="swing_trade_updated",
            title="Swing trade ajustada",
            user=user,
            trade_id=trade_id,
            ticker=target.get("ticker"),
            payload=payload,
            target=target,
            response_payload=response_payload if isinstance(response_payload, dict) else None,
            upstream_status=status,
        )
    else:
        _notify_swing_trade_event(
            event_key="swing_trade_failed",
            title="Falha ao ajustar swing trade",
            user=user,
            trade_id=trade_id,
            ticker=target.get("ticker"),
            payload=payload,
            target=target,
            response_payload=response_payload if isinstance(response_payload, dict) else None,
            upstream_status=status,
            error_message=error_message,
        )
    return _market_scanner_result_to_api_response(ok, status, response_payload)


@api_bp.route("/scanner/trades/<int:trade_id>/close", methods=["POST"])
def scanner_close_trade(trade_id: int):
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)

    ok, status, trades_payload = _market_scanner_request("GET", "/trades")
    if not ok:
        message = _market_scanner_error_message(trades_payload, status)
        _log_scanner_trade_audit(
            action="close",
            user=user,
            trade_id=trade_id,
            request_payload=None,
            response_payload=trades_payload,
            success=False,
            upstream_status=status,
            error_message=message,
        )
        _notify_swing_trade_event(
            event_key="swing_trade_failed",
            title="Falha ao encerrar swing trade",
            user=user,
            trade_id=trade_id,
            upstream_status=status,
            error_message=message,
        )
        return _json_error(message, status=status, details={"upstream": trades_payload})

    all_trades = []
    if isinstance(trades_payload, dict):
        all_trades.extend(item for item in (trades_payload.get("active") or []) if isinstance(item, dict))
        all_trades.extend(item for item in (trades_payload.get("history") or []) if isinstance(item, dict))
    target = next((item for item in all_trades if int(item.get("id") or 0) == int(trade_id)), None)
    if target is None:
        _log_scanner_trade_audit(
            action="close",
            user=user,
            trade_id=trade_id,
            request_payload=None,
            response_payload=None,
            success=False,
            upstream_status=404,
            error_message="Trade nao encontrado.",
        )
        _notify_swing_trade_event(
            event_key="swing_trade_failed",
            title="Falha ao encerrar swing trade",
            user=user,
            trade_id=trade_id,
            upstream_status=404,
            error_message="Trade nao encontrado.",
        )
        return _json_error("Trade nao encontrado.", status=404)
    if not _scanner_trade_visible_for_user(target, user):
        _log_scanner_trade_audit(
            action="close",
            user=user,
            trade_id=trade_id,
            ticker=target.get("ticker"),
            request_payload=None,
            response_payload=None,
            success=False,
            upstream_status=403,
            error_message="Trade nao pertence ao usuario atual.",
        )
        _notify_swing_trade_event(
            event_key="swing_trade_failed",
            title="Falha ao encerrar swing trade",
            user=user,
            trade_id=trade_id,
            ticker=target.get("ticker"),
            target=target,
            upstream_status=403,
            error_message="Trade nao pertence ao usuario atual.",
        )
        return _json_error("Trade nao pertence ao usuario atual.", status=403)

    payload = request.get_json(silent=True) or request.form.to_dict()
    payload = _scanner_prepare_trade_payload(payload, user, force_notes=False)
    ok, status, response_payload = _market_scanner_request("POST", f"/trades/{trade_id}/close", payload=payload)
    error_message = None if ok else _market_scanner_error_message(response_payload, status)
    _log_scanner_trade_audit(
        action="close",
        user=user,
        trade_id=trade_id,
        ticker=target.get("ticker"),
        request_payload=payload,
        response_payload=response_payload,
        success=ok,
        upstream_status=status,
        error_message=error_message,
    )
    if ok:
        _notify_swing_trade_event(
            event_key="swing_trade_closed",
            title="Swing trade encerrada",
            user=user,
            trade_id=trade_id,
            ticker=target.get("ticker"),
            payload=payload,
            target=target,
            response_payload=response_payload if isinstance(response_payload, dict) else None,
            upstream_status=status,
        )
    else:
        _notify_swing_trade_event(
            event_key="swing_trade_failed",
            title="Falha ao encerrar swing trade",
            user=user,
            trade_id=trade_id,
            ticker=target.get("ticker"),
            payload=payload,
            target=target,
            response_payload=response_payload if isinstance(response_payload, dict) else None,
            upstream_status=status,
            error_message=error_message,
        )
    return _market_scanner_result_to_api_response(ok, status, response_payload)


@api_bp.route("/scanner/ticker/<symbol>", methods=["GET"])
def scanner_ticker(symbol: str):
    safe_symbol = urlparse.quote(symbol.upper(), safe="")
    ok, status, response_payload = _market_scanner_request("GET", f"/ticker/{safe_symbol}")
    return _market_scanner_result_to_api_response(ok, status, response_payload)


@api_bp.route("/scanner/scan/status", methods=["GET"])
def scanner_manual_scan_status():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    return _json_ok(_scanner_manual_scan_status_payload())


@api_bp.route("/scanner/scan/start", methods=["POST"])
def scanner_manual_scan_start():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    if not can_user_write(user):
        return _json_error("Perfil viewer possui acesso somente leitura.", status=403)
    payload = _start_scanner_manual_scan(user)
    if not payload.get("started"):
        run = payload.get("run") or {}
        _notify_sync_event(
            "manual_scan_started",
            "Scan manual deduplicado (ja em andamento)",
            details={
                "requested_by": str((user or {}).get("username") or ""),
                "running_run_id": run.get("id"),
            },
            dedupe_key="scan:manual:already-running",
            min_interval_seconds=60,
        )
    status = 202 if payload.get("started") else 200
    return _json_ok(payload, status=status)


@api_bp.route("/scanner/scan", methods=["POST"])
def scanner_manual_scan():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    if not can_user_write(user):
        return _json_error("Perfil viewer possui acesso somente leitura.", status=403)
    upstream_scan = _scanner_upstream_scan_status_payload()
    if bool(upstream_scan.get("running")):
        _notify_sync_event(
            "manual_scan_started",
            "Scan manual deduplicado (upstream em execucao)",
            details={
                "requested_by": str((user or {}).get("username") or ""),
                "active_run_id": upstream_scan.get("active_run_id"),
            },
            dedupe_key="scanner:scan:already-running",
            min_interval_seconds=60,
        )
        return _json_ok(
            {
                "started": False,
                "deduplicated": True,
                "message": "Ja existe scan ativo no scanner.",
                "upstream_scan": upstream_scan,
            },
            status=202,
        )
    ok, status, response_payload = _market_scanner_request(
        "POST",
        "/scan",
        payload={},
        timeout_seconds=_market_scanner_scan_timeout_seconds(),
    )
    if ok:
        summary = response_payload.get("scan_summary") if isinstance(response_payload, dict) else {}
        _notify_sync_event(
            "manual_scan_success",
            "Scan manual acionado via API",
            details={
                "requested_by": str((user or {}).get("username") or ""),
                "upstream_status": int(status or 0),
                "tickers_processed": int((summary or {}).get("tickers_processed") or (summary or {}).get("tickers_loaded") or 0),
                "signals_triggered": int((summary or {}).get("signals_triggered") or 0),
            },
            dedupe_key="scanner:scan:success",
            min_interval_seconds=15,
        )
    else:
        _notify_sync_event(
            "manual_scan_failed",
            "Falha no scan manual via API",
            details={
                "requested_by": str((user or {}).get("username") or ""),
                "upstream_status": int(status or 0),
                "error": _market_scanner_error_message(response_payload, status),
            },
            dedupe_key=f"scanner:scan:failed:{int(status or 0)}",
            min_interval_seconds=120,
        )
    return _market_scanner_result_to_api_response(ok, status, response_payload)


@api_bp.route("/scanner/scan/<symbol>", methods=["POST"])
def scanner_manual_scan_ticker(symbol: str):
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    if not can_user_write(user):
        return _json_error("Perfil viewer possui acesso somente leitura.", status=403)
    normalized_symbol = str(symbol or "").strip().upper()
    upstream_scan = _scanner_upstream_scan_status_payload()
    if bool(upstream_scan.get("running")):
        if _scanner_upstream_covers_ticker(upstream_scan, normalized_symbol):
            return _json_ok(
                {
                    "ticker": normalized_symbol,
                    "started": False,
                    "deduplicated": True,
                    "message": "Ticker ja coberto pelo scan ativo.",
                    "upstream_scan": upstream_scan,
                },
                status=202,
            )
        return _json_ok(
            {
                "ticker": normalized_symbol,
                "started": False,
                "deduplicated": False,
                "message": "Existe scan ativo. Aguarde concluir para iniciar outro ticker.",
                "upstream_scan": upstream_scan,
            },
            status=202,
        )
    safe_symbol = urlparse.quote(str(symbol or "").strip().upper(), safe="")
    ok, status, response_payload = _market_scanner_request(
        "POST",
        f"/scan/ticker/{safe_symbol}",
        payload={},
        timeout_seconds=_market_scanner_scan_timeout_seconds(),
    )
    if ok:
        _notify_sync_event(
            "sync_ticker_success",
            "Scan manual por ticker acionado",
            details={
                "ticker": normalized_symbol,
                "requested_by": str((user or {}).get("username") or ""),
                "upstream_status": int(status or 0),
            },
            dedupe_key=f"scanner:scan:ticker:success:{normalized_symbol}",
            min_interval_seconds=30,
        )
    else:
        _notify_sync_event(
            "sync_ticker_failed",
            "Falha no scan manual por ticker",
            details={
                "ticker": normalized_symbol,
                "requested_by": str((user or {}).get("username") or ""),
                "upstream_status": int(status or 0),
                "error": _market_scanner_error_message(response_payload, status),
            },
            dedupe_key=f"scanner:scan:ticker:failed:{normalized_symbol}:{int(status or 0)}",
            min_interval_seconds=120,
        )
    return _market_scanner_result_to_api_response(ok, status, response_payload)


@api_bp.route("/scanner/metrics/catalog", methods=["GET"])
def scanner_metrics_catalog():
    return _market_scanner_proxy_get("/metrics/catalog")


@api_bp.route("/scanner/metrics/catalog/<metric_key>", methods=["PATCH"])
def scanner_metrics_update(metric_key: str):
    safe_key = urlparse.quote(str(metric_key or "").strip(), safe="")
    payload = request.get_json(silent=True) or request.form.to_dict()
    return _market_scanner_proxy("PATCH", f"/metrics/catalog/{safe_key}", payload=payload)


@api_bp.route("/auth/me", methods=["GET"])
def auth_me():
    user = get_current_user()
    return _json_ok({"authenticated": bool(user), "user": user})


@api_bp.route("/auth/login", methods=["POST"])
def auth_login():
    payload = request.get_json(silent=True) or request.form.to_dict()
    user = login_user(payload.get("username", ""), payload.get("password", ""))
    if not user:
        return _json_error("Usuario ou senha invalidos.", status=401)
    return _json_ok({"authenticated": True, "user": user})


@api_bp.route("/auth/logout", methods=["POST"])
def auth_logout():
    logout_current_user()
    return _json_ok({"authenticated": False})


@api_bp.route("/admin/users", methods=["GET", "POST"])
def admin_users():
    admin = require_admin_user()
    if request.method == "GET":
        return _json_ok({"users": list_users(), "current_user_id": admin["id"]})

    payload = request.get_json(silent=True) or request.form.to_dict()
    ok, message, user = create_user_account(
        payload.get("username", ""),
        payload.get("password", ""),
        _as_bool(payload.get("is_admin")),
        role=payload.get("role"),
    )
    if not ok:
        return _json_error(message, status=400)
    return _json_ok({"message": message, "user": user}, status=201)


@api_bp.route("/admin/users/<int:user_id>/status", methods=["POST"])
def admin_user_status(user_id: int):
    admin = require_admin_user()
    payload = request.get_json(silent=True) or request.form.to_dict()
    ok, message, user = set_user_active_state(
        user_id,
        _as_bool(payload.get("is_active")),
        acting_user_id=admin["id"],
    )
    if not ok:
        return _json_error(message, status=400)
    return _json_ok({"message": message, "user": user})


@api_bp.route("/admin/users/<int:user_id>/role", methods=["POST"])
def admin_user_role(user_id: int):
    admin = require_admin_user()
    payload = request.get_json(silent=True) or request.form.to_dict()
    ok, message, user = set_user_role(
        user_id,
        str(payload.get("role") or ""),
        acting_user_id=admin["id"],
    )
    if not ok:
        return _json_error(message, status=400)
    return _json_ok({"message": message, "user": user})


@api_bp.route("/admin/scanner/status", methods=["GET"])
def admin_scanner_status():
    require_admin_user()
    return _json_ok(_scanner_db_status_payload())


@api_bp.route("/admin/scanner/audit", methods=["GET"])
def admin_scanner_audit():
    require_admin_user()
    raw_limit = request.args.get("limit", "100")
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        limit = 100
    limit = max(1, min(limit, 500))

    rows = get_db().execute(
        """
        SELECT
          id,
          action,
          user_id,
          username,
          trade_id,
          ticker,
          success,
          upstream_status,
          error_message,
          remote_addr,
          created_at
        FROM scanner_trade_audit
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    items = [dict(row) for row in rows]
    return _json_ok({"items": items, "limit": limit})


@api_bp.route("/admin/telegram/status", methods=["GET"])
def admin_telegram_status():
    require_admin_user()
    return _json_ok(telegram_status_payload())


@api_bp.route("/admin/telegram/test", methods=["POST"])
def admin_telegram_test():
    admin = require_admin_user()
    payload = request.get_json(silent=True) or request.form.to_dict()
    custom_text = str(payload.get("message") or "").strip()
    test_message = custom_text or (
        f"[TYI] Teste de notificacao Telegram\n"
        f"Evento: admin_test\n"
        f"Quando: {_now_iso_utc()}\n"
        f"Usuario: {str(admin.get('username') or '')}"
    )
    result = send_telegram_text(
        test_message,
        event_key="admin_test",
        dedupe_key=None,
        min_interval_seconds=0,
        asynchronous=False,
        force=True,
    )
    if result.get("sent"):
        return _json_ok({"message": "Mensagem de teste enviada.", "result": result})
    return _json_error(
        "Nao foi possivel enviar a mensagem de teste no Telegram.",
        status=503,
        details=result,
    )


@api_bp.route("/admin/openclaw/enrich-assets", methods=["POST"])
def admin_openclaw_enrich_assets():
    require_admin_user()
    payload = request.get_json(silent=True) or request.form.to_dict()
    tickers = payload.get("tickers") or []
    if isinstance(tickers, str):
        tickers = [item.strip() for item in tickers.split(",")]
    elif not isinstance(tickers, list):
        tickers = []

    raw_limit = payload.get("limit")
    limit = None
    try:
        parsed_limit = int(raw_limit)
        if parsed_limit > 0:
            limit = parsed_limit
    except (TypeError, ValueError):
        limit = None

    result = enrich_assets_with_openclaw_batch(
        tickers=tickers,
        only_missing=_as_bool(payload.get("only_missing", True)),
        limit=limit,
    )
    return _json_ok(result)


@api_bp.route("/admin/metric-formulas", methods=["GET"])
def admin_metric_formulas():
    require_admin_user()
    return _json_ok(get_metric_formulas_catalog())


@api_bp.route("/admin/metric-formulas/<metric_key>", methods=["POST"])
def admin_metric_formula_save(metric_key: str):
    require_admin_user()
    payload = request.get_json(silent=True) or request.form.to_dict()
    ok, message, result = update_metric_formula(metric_key, payload.get("formula"))
    if not ok:
        return _json_error(message, status=400)
    return _json_ok({"message": message, "result": result, "catalog": get_metric_formulas_catalog()})


@api_bp.route("/metrics", methods=["GET"])
def metrics():
    return _json_ok({"routes": get_route_metrics(current_app)})


@api_bp.route("/backup/database", methods=["GET", "POST"])
def backup_database_endpoint():
    require_admin_user()
    if request.method == "GET":
        return _json_ok({"backups": list_database_backups()})

    try:
        result = create_database_backups(reason="api")
    except Exception as exc:
        return _json_error(f"Nao foi possivel gerar backup: {exc}", status=500)
    backups = result.get("backups") or []
    primary = next((item for item in backups if item.get("database_key") == "backend"), None)
    return _json_ok({"backup": primary, **result}, status=201)


@api_bp.route("/backup/database/<path:filename>", methods=["GET"])
def backup_database_download(filename: str):
    require_admin_user()
    path = resolve_database_backup_path(filename)
    if not path:
        return _json_error("Backup nao encontrado.", status=404)
    return send_file(
        path,
        as_attachment=True,
        download_name=path.name,
        mimetype="application/x-sqlite3",
    )


@api_bp.route("/portfolios", methods=["GET", "POST", "DELETE"])
def portfolios():
    if request.method == "GET":
        return _json_ok(get_portfolios())

    if request.method == "POST":
        payload = request.get_json(silent=True) or request.form.to_dict()
        ok, result = create_portfolio(payload.get("name"))
        if not ok:
            return _json_error(result, status=400)
        return _json_ok({"portfolio_id": result}, status=201)

    payload = request.get_json(silent=True) or request.form.to_dict()
    ok, result = delete_portfolio(payload.get("portfolio_id"))
    if not ok:
        return _json_error(result, status=400)
    return _json_ok({"removed_name": result})


@api_bp.route("/assets", methods=["GET"])
def assets():
    return _json_ok(get_top_assets())


@api_bp.route("/assets/<ticker>", methods=["GET"])
def asset_detail(ticker):
    portfolio_ids = _selected_portfolio_ids_from_request()
    asset = get_asset(ticker)
    if not asset:
        return _json_error("Ativo nao encontrado.", status=404)
    payload = {
        "asset": asset,
        "enrichment": get_asset_enrichment(ticker),
        "enrichment_history": get_asset_enrichment_history(ticker),
        "position": get_asset_position_summary(ticker, portfolio_ids),
        "transactions": get_asset_transactions(ticker, portfolio_ids),
        "incomes": get_asset_incomes(ticker, portfolio_ids),
        "upcoming_incomes": get_asset_upcoming_incomes(ticker),
    }
    return _json_ok(payload)


@api_bp.route("/assets/<ticker>/price-history", methods=["GET"])
def asset_price_history(ticker: str):
    chart_range = (request.args.get("range") or "1y").lower()
    asset = get_asset(ticker)
    if not asset:
        return _json_error("Ativo nao encontrado.", status=404)
    return _json_ok(get_asset_price_history(ticker, chart_range))


@api_bp.route("/assets/<ticker>/enrich/openclaw", methods=["POST"])
def asset_enrich_openclaw(ticker: str):
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)

    ok, message, enrichment = enrich_asset_with_openclaw(ticker)
    if not ok:
        return _json_error(message, status=502)
    return _json_ok(
        {
            "message": message,
            "enrichment": enrichment,
            "enrichment_history": get_asset_enrichment_history(ticker),
        }
    )


@api_bp.route("/portfolio/snapshot", methods=["GET"])
def portfolio_snapshot():
    portfolio_ids = _selected_portfolio_ids_from_request()
    sort_by = request.args.get("sort_by", "name")
    sort_dir = request.args.get("sort_dir", "asc")
    payload = get_portfolio_snapshot(portfolio_ids, sort_by=sort_by, sort_dir=sort_dir)
    return _json_ok(payload)


@api_bp.route("/sectors", methods=["GET"])
def sectors():
    return _json_ok(get_sectors_summary())


@api_bp.route("/transactions", methods=["GET", "POST", "DELETE"])
def transactions():
    if request.method == "GET":
        portfolio_ids = _selected_portfolio_ids_from_request()
        return _json_ok(get_transactions(portfolio_ids))

    if request.method == "POST":
        payload = request.get_json(silent=True) or request.form.to_dict()
        ok, message = add_transaction(payload)
        if not ok:
            return _json_error(message, status=400)
        return _json_ok({"message": message}, status=201)

    ids = request.get_json(silent=True) or {}
    transaction_ids = ids.get("transaction_ids") or []
    portfolio_ids = _selected_portfolio_ids_from_request()
    removed = delete_transactions(transaction_ids, portfolio_ids)
    if removed <= 0:
        return _json_error("Nenhuma transacao removida.", status=400)
    return _json_ok({"removed": removed})


@api_bp.route("/incomes", methods=["GET", "POST", "DELETE"])
def incomes():
    if request.method == "GET":
        portfolio_ids = _selected_portfolio_ids_from_request()
        return _json_ok(get_incomes(portfolio_ids))

    if request.method == "POST":
        payload = request.get_json(silent=True) or request.form.to_dict()
        ok, message = add_income(payload)
        if not ok:
            return _json_error(message, status=400)
        return _json_ok({"message": message}, status=201)

    ids = request.get_json(silent=True) or {}
    income_ids = ids.get("income_ids") or []
    portfolio_ids = _selected_portfolio_ids_from_request()
    removed = delete_incomes(income_ids, portfolio_ids)
    if removed <= 0:
        return _json_error("Nenhum provento removido.", status=400)
    return _json_ok({"removed": removed})


@api_bp.route("/incomes/upcoming", methods=["GET"])
def incomes_upcoming():
    portfolio_ids = _selected_portfolio_ids_from_request()
    raw_limit = request.args.get("limit")
    try:
        limit = max(1, min(int(raw_limit or 30), 200))
    except (TypeError, ValueError):
        limit = 30
    return _json_ok(_build_upcoming_incomes_payload(portfolio_ids, limit=limit))


@api_bp.route("/fixed-incomes", methods=["GET", "POST", "DELETE"])
def fixed_incomes():
    if request.method == "GET":
        portfolio_ids = _selected_portfolio_ids_from_request()
        sort_by = request.args.get("sort_by", "date_aporte")
        sort_dir = request.args.get("sort_dir", "desc")
        return _json_ok(get_fixed_income_payload_cached(portfolio_ids, sort_by=sort_by, sort_dir=sort_dir))

    if request.method == "POST":
        payload = request.get_json(silent=True) or request.form.to_dict()
        ok, message = add_fixed_income(payload)
        if not ok:
            return _json_error(message, status=400)
        return _json_ok({"message": message}, status=201)

    ids = request.get_json(silent=True) or {}
    fixed_income_ids = ids.get("fixed_income_ids") or []
    portfolio_ids = _selected_portfolio_ids_from_request()
    removed = delete_fixed_incomes(fixed_income_ids, portfolio_ids)
    if removed <= 0:
        return _json_error("Nenhum registro removido.", status=400)
    return _json_ok({"removed": removed})


@api_bp.route("/imports/transactions-csv", methods=["POST"])
def import_transactions_csv_endpoint():
    target_portfolio_id = resolve_portfolio_id(request.form.get("target_portfolio_id"))
    file = request.files.get("csv_file")
    file_bytes = file.read() if file else b""
    ok, message, imported, errors = import_transactions_csv(file_bytes, target_portfolio_id)
    if not ok:
        return _json_error(message, status=400, details={"imported": imported, "errors": errors})
    return _json_ok(
        {
            "message": message,
            "imported": imported,
            "errors": errors,
            "target_portfolio_id": target_portfolio_id,
        }
    )


@api_bp.route("/imports/fixed-incomes-csv", methods=["POST"])
def import_fixed_incomes_csv_endpoint():
    target_portfolio_id = resolve_portfolio_id(request.form.get("target_portfolio_id"))
    file = request.files.get("fixed_income_csv_file")
    file_bytes = file.read() if file else b""
    ok, message, imported, errors = import_fixed_incomes_csv(file_bytes, target_portfolio_id)
    if not ok:
        return _json_error(message, status=400, details={"imported": imported, "errors": errors})
    return _json_ok(
        {
            "message": message,
            "imported": imported,
            "errors": errors,
            "target_portfolio_id": target_portfolio_id,
        }
    )


@api_bp.route("/charts/monthly-class-summary", methods=["GET"])
def charts_monthly_class_summary():
    portfolio_ids = _selected_portfolio_ids_from_request()
    return _json_ok(get_monthly_class_summary(portfolio_ids))


@api_bp.route("/charts/benchmark", methods=["GET"])
def charts_benchmark():
    portfolio_ids = _selected_portfolio_ids_from_request()
    range_key = (request.args.get("range") or "12m").strip().lower()
    scope_key = (request.args.get("scope") or "all").strip().lower()
    return _json_ok(get_benchmark_comparison(portfolio_ids, range_key=range_key, scope_key=scope_key))


@api_bp.route("/charts/core", methods=["GET"])
def charts_core():
    portfolio_ids = _selected_portfolio_ids_from_request()
    return _json_ok(_build_charts_core_payload(portfolio_ids))


@api_bp.route("/charts/ticker-summary", methods=["GET"])
def charts_ticker_summary():
    portfolio_ids = _selected_portfolio_ids_from_request()
    months = request.args.get("months", 8)
    return _json_ok(get_monthly_ticker_summary(portfolio_ids, months=months))


@api_bp.route("/charts/variable-income-value-daily", methods=["GET"])
def charts_variable_income_value_daily():
    portfolio_ids = _selected_portfolio_ids_from_request()
    range_key = (request.args.get("range") or "90d").strip().lower()
    return _json_ok(get_variable_income_value_daily_series(portfolio_ids, range_key=range_key))


@api_bp.route("/charts/dashboard", methods=["GET"])
def charts_dashboard():
    portfolio_ids = _selected_portfolio_ids_from_request()
    benchmark_range = (request.args.get("range") or "12m").strip().lower()
    benchmark_scope = (request.args.get("scope") or "all").strip().lower()
    payload = _build_charts_core_payload(portfolio_ids)
    payload["monthly_ticker_summary"] = get_monthly_ticker_summary(portfolio_ids, months=8)
    payload["benchmark_chart"] = get_benchmark_comparison(
        portfolio_ids,
        range_key=benchmark_range,
        scope_key=benchmark_scope,
    )
    payload["benchmark_range"] = payload["benchmark_chart"].get("range_key", "12m")
    payload["benchmark_scope"] = payload["benchmark_chart"].get("scope_key", "all")
    return _json_ok(payload)


def _scanner_upstream_covers_ticker(upstream_scan: dict, ticker: str):
    if not isinstance(upstream_scan, dict):
        return False
    if not bool(upstream_scan.get("running")):
        return False
    scope = str(upstream_scan.get("active_scope") or "").strip().lower()
    if scope == "full":
        return True
    requested = upstream_scan.get("active_requested_symbols") or []
    normalized_ticker = str(ticker or "").strip().upper()
    normalized_symbol = f"{normalized_ticker.removesuffix('.SA')}.SA" if normalized_ticker else ""
    requested_upper = {str(item or "").strip().upper() for item in requested}
    return normalized_ticker in requested_upper or normalized_symbol in requested_upper


def _sync_status_payload():
    health_payload = build_health_payload()
    scanner_status_payload = _scanner_manual_scan_status_payload()
    stale_row = get_db().execute(
        """
        SELECT COUNT(*) AS total
        FROM assets
        WHERE market_data_status IN ('stale', 'failed', 'unknown')
        """
    ).fetchone()
    stale_assets_total = int((stale_row["total"] if stale_row else 0) or 0)
    return {
        "generated_at": _now_iso_utc(),
        "health": health_payload,
        "scanner": scanner_status_payload,
        "stale_assets_total": stale_assets_total,
    }


def _sync_queue_payload(limit: int = 20):
    safe_limit = max(1, min(int(limit or 20), 200))
    rows = get_db().execute(
        """
        SELECT
          id,
          status,
          requested_by_user_id,
          requested_by_username,
          started_at,
          finished_at,
          total_tickers,
          processed_tickers,
          triggered_signals,
          upstream_status,
          error_message
        FROM scanner_manual_scan_runs
        ORDER BY id DESC
        LIMIT ?
        """,
        (safe_limit,),
    ).fetchall()
    items = [_scanner_manual_scan_row_to_payload(row) for row in rows]
    return {
        "generated_at": _now_iso_utc(),
        "upstream_scan": _scanner_upstream_scan_status_payload(),
        "items": items,
        "running_count": sum(1 for item in items if str(item.get("status")) == "running"),
        "failed_count": sum(1 for item in items if str(item.get("status")) == "failed"),
        "success_count": sum(1 for item in items if str(item.get("status")) == "success"),
    }


@api_bp.route("/sync/market-data", methods=["POST"])
def sync_market_data_all():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    if not can_user_write(user):
        return _json_error("Perfil viewer possui acesso somente leitura.", status=403)

    upstream_scan = _scanner_upstream_scan_status_payload()
    if bool(upstream_scan.get("running")):
        _notify_sync_event(
            "manual_scan_started",
            "Sync geral deduplicado (scan ja em andamento)",
            details={
                "requested_by": str((user or {}).get("username") or ""),
                "active_run_id": upstream_scan.get("active_run_id"),
            },
            dedupe_key="sync:full:already-running",
            min_interval_seconds=60,
        )
        return _json_ok(
            {
                "mode": "manual_full_scan",
                "source": "market_scanner",
                "started": False,
                "deduplicated": True,
                "upstream_scan": upstream_scan,
            },
            status=202,
        )

    ok, status, response_payload = _market_scanner_request(
        "POST",
        "/scan",
        payload={},
        timeout_seconds=_market_scanner_scan_timeout_seconds(),
    )
    if not ok:
        _notify_sync_event(
            "manual_scan_failed",
            "Falha no sync geral manual",
            details={
                "requested_by": str((user or {}).get("username") or ""),
                "upstream_status": int(status or 0),
                "error": _market_scanner_error_message(response_payload, status),
            },
            dedupe_key=f"sync:full:failed:{int(status or 0)}",
            min_interval_seconds=120,
        )
        return _market_scanner_result_to_api_response(ok, status, response_payload)

    summary = {}
    if isinstance(response_payload, dict) and isinstance(response_payload.get("scan_summary"), dict):
        summary = dict(response_payload.get("scan_summary") or {})
    _notify_sync_event(
        "manual_scan_success",
        "Sync geral manual concluido",
        details={
            "requested_by": str((user or {}).get("username") or ""),
            "upstream_status": int(status or 0),
            "tickers_processed": int(summary.get("tickers_processed") or summary.get("tickers_loaded") or 0),
            "signals_triggered": int(summary.get("signals_triggered") or 0),
        },
        dedupe_key="sync:full:success",
        min_interval_seconds=15,
    )
    return _json_ok(
        {
            "mode": "manual_full_scan",
            "source": "market_scanner",
            "started": True,
            "deduplicated": False,
            "upstream_scan": _scanner_upstream_scan_status_payload(),
            "scan_summary": summary,
        },
        status=status,
    )


@api_bp.route("/sync/market-data/stale", methods=["POST"])
def sync_market_data_stale():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    if not can_user_write(user):
        return _json_error("Perfil viewer possui acesso somente leitura.", status=403)

    payload = request.get_json(silent=True) or request.form.to_dict()
    scope_key = str(payload.get("scope") or "all").strip().lower() or "all"
    raw_attempts = payload.get("attempts")
    try:
        attempts = int(raw_attempts) if raw_attempts is not None else 2
    except (TypeError, ValueError):
        return _json_error("Parametro attempts invalido.", status=400)
    attempts = max(1, min(attempts, 5))
    include_scanner_br = not bool(current_app.config.get("MARKET_SYNC_FORCE_LIVE_BR", False))

    try:
        result = refresh_assets_market_data(
            scope_key=scope_key,
            stale_only=True,
            attempts=attempts,
            include_scanner_br=include_scanner_br,
        )
    except ValueError as exc:
        return _json_error(str(exc), status=400)
    except Exception:
        current_app.logger.exception("Falha no sync manual de ativos desatualizados.")
        return _json_error("Falha ao atualizar ativos desatualizados.", status=503)

    failed = [str(item or "").strip().upper() for item in (result.get("failed") or []) if str(item or "").strip()]
    selected_count = int(result.get("selected_count") or 0)
    failed_count = len(failed)
    updated_count = max(selected_count - failed_count, 0)
    output = {
        "mode": "manual_stale_sync",
        "scope": str(result.get("scope") or scope_key),
        "stale_only": True,
        "selected_count": selected_count,
        "updated_count": updated_count,
        "failed_count": failed_count,
        "failed": failed,
    }
    if failed_count == 0:
        _notify_sync_event(
            "manual_scan_success",
            "Sync de ativos desatualizados concluido",
            details={
                "requested_by": str((user or {}).get("username") or ""),
                "scope": output["scope"],
                "selected_count": selected_count,
                "updated_count": updated_count,
                "failed_count": failed_count,
            },
            dedupe_key=f"sync:stale:success:{output['scope']}",
            min_interval_seconds=15,
        )
    else:
        _notify_sync_event(
            "manual_scan_failed",
            "Sync de ativos desatualizados com falhas",
            details={
                "requested_by": str((user or {}).get("username") or ""),
                "scope": output["scope"],
                "selected_count": selected_count,
                "updated_count": updated_count,
                "failed_count": failed_count,
                "failed_sample": failed[:10],
            },
            dedupe_key=f"sync:stale:failed:{output['scope']}",
            min_interval_seconds=30,
        )
    return _json_ok(output)


@api_bp.route("/sync/market-data/<ticker>", methods=["POST"])
def sync_market_data_ticker(ticker):
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    if not can_user_write(user):
        return _json_error("Perfil viewer possui acesso somente leitura.", status=403)

    normalized_ticker = str(ticker or "").strip().upper()
    if not normalized_ticker:
        return _json_error("Ticker invalido.", status=400)

    scan_payload = None
    use_scanner = legacy_market._is_brazilian_market_ticker(normalized_ticker)
    if use_scanner:
        upstream_scan = _scanner_upstream_scan_status_payload()
        if bool(upstream_scan.get("running")):
            if _scanner_upstream_covers_ticker(upstream_scan, normalized_ticker):
                return _json_ok(
                    {
                        "ticker": normalized_ticker,
                        "success": True,
                        "started": False,
                        "deduplicated": True,
                        "upstream_scan": upstream_scan,
                    },
                    status=202,
                )
            return _json_ok(
                {
                    "ticker": normalized_ticker,
                    "success": False,
                    "started": False,
                    "deduplicated": False,
                    "queued": False,
                    "message": "Ja existe scan ativo no scanner. Aguarde concluir para evitar concorrencia.",
                    "upstream_scan": upstream_scan,
                },
                status=202,
            )
        safe_symbol = urlparse.quote(normalized_ticker, safe="")
        scan_ok, scan_status, scan_payload = _market_scanner_request(
            "POST",
            f"/scan/ticker/{safe_symbol}",
            payload={},
            timeout_seconds=_market_scanner_scan_timeout_seconds(),
        )
        if not scan_ok:
            _notify_sync_event(
                "sync_ticker_failed",
                "Falha no sync por ticker",
                details={
                    "ticker": normalized_ticker,
                    "requested_by": str((user or {}).get("username") or ""),
                    "upstream_status": int(scan_status or 0),
                    "error": _market_scanner_error_message(scan_payload, scan_status),
                },
                dedupe_key=f"sync:ticker:failed:{normalized_ticker}:{int(scan_status or 0)}",
                min_interval_seconds=120,
            )
            return _market_scanner_result_to_api_response(scan_ok, scan_status, scan_payload)
    else:
        ok = False
        try:
            ok = refresh_asset_market_data(
                normalized_ticker,
                include_scanner_br=False,
                preferred_provider=None,
            )
        except Exception:
            ok = False
        if not ok:
            _notify_sync_event(
                "sync_ticker_failed",
                "Falha no sync por ticker",
                details={
                    "ticker": normalized_ticker,
                    "requested_by": str((user or {}).get("username") or ""),
                    "source": "backend_live_fetch",
                },
                dedupe_key=f"sync:ticker:failed:{normalized_ticker}:backend",
                min_interval_seconds=120,
            )
            return _json_error("Nao foi possivel atualizar este ticker agora.", status=503)

    asset_payload = get_asset(normalized_ticker)
    market_data_payload = ((asset_payload or {}).get("market_data") or {})
    _notify_sync_event(
        "sync_ticker_success",
        "Sync por ticker concluido",
        details={
            "ticker": normalized_ticker,
            "requested_by": str((user or {}).get("username") or ""),
            "source": market_data_payload.get("source"),
            "updated_at": market_data_payload.get("updated_at"),
        },
        dedupe_key=f"sync:ticker:success:{normalized_ticker}",
        min_interval_seconds=30,
    )
    return _json_ok(
        {
            "ticker": normalized_ticker,
            "success": True,
            "started": True,
            "deduplicated": False,
            "force_live": False,
            "source": market_data_payload.get("source"),
            "updated_at": market_data_payload.get("updated_at"),
            "scan_summary": (scan_payload or {}).get("scan_summary")
            if isinstance(scan_payload, dict)
            else None,
        }
    )


@api_bp.route("/sync/status", methods=["GET"])
def sync_status():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    return _json_ok(_sync_status_payload())


@api_bp.route("/sync/queue", methods=["GET"])
def sync_queue():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    limit = request.args.get("limit", 20)
    return _json_ok(_sync_queue_payload(limit=limit))


@api_bp.route("/sync/cancel/<int:run_id>", methods=["POST"])
def sync_cancel(run_id: int):
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    if not can_user_write(user):
        return _json_error("Perfil viewer possui acesso somente leitura.", status=403)

    db = get_db()
    row = db.execute(
        """
        SELECT
          id,
          status,
          requested_by_user_id,
          requested_by_username,
          started_at,
          finished_at,
          total_tickers,
          processed_tickers,
          triggered_signals,
          upstream_status,
          error_message
        FROM scanner_manual_scan_runs
        WHERE id = ?
        LIMIT 1
        """,
        (int(run_id),),
    ).fetchone()
    if not row:
        return _json_error("Execucao nao encontrada.", status=404)

    status_text = str(row["status"] or "").strip().lower()
    if status_text != "running":
        return _json_ok(
            {
                "id": int(run_id),
                "canceled": False,
                "effective_cancel": False,
                "message": "Execucao nao esta em andamento.",
                "run": _scanner_manual_scan_row_to_payload(row),
            }
        )

    db.execute(
        """
        UPDATE scanner_manual_scan_runs
        SET
          status = 'failed',
          finished_at = ?,
          upstream_status = 499,
          error_message = ?
        WHERE id = ? AND status = 'running'
        """,
        (_now_iso_utc(), "Cancelado manualmente via API.", int(run_id)),
    )
    db.commit()
    _notify_sync_event(
        "manual_scan_failed",
        "Run manual marcado como cancelado",
        details={
            "run_id": int(run_id),
            "requested_by": str((user or {}).get("username") or ""),
        },
        dedupe_key=f"sync:cancel:{int(run_id)}",
        min_interval_seconds=10,
    )
    updated = db.execute(
        """
        SELECT
          id,
          status,
          requested_by_user_id,
          requested_by_username,
          started_at,
          finished_at,
          total_tickers,
          processed_tickers,
          triggered_signals,
          upstream_status,
          error_message
        FROM scanner_manual_scan_runs
        WHERE id = ?
        LIMIT 1
        """,
        (int(run_id),),
    ).fetchone()
    return _json_ok(
        {
            "id": int(run_id),
            "canceled": True,
            "effective_cancel": False,
            "message": "Run local marcado como cancelado. Se o scanner upstream ja iniciou, ele pode concluir em paralelo.",
            "run": _scanner_manual_scan_row_to_payload(updated),
            "upstream_scan": _scanner_upstream_scan_status_payload(),
        }
    )


@api_bp.route("/sync/audit", methods=["GET"])
def sync_audit():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)

    ticker = str(request.args.get("ticker") or "").strip().upper()
    limit_raw = request.args.get("limit", 100)
    try:
        limit = max(1, min(int(limit_raw), 500))
    except (TypeError, ValueError):
        limit = 100

    params = []
    where = ""
    if ticker:
        where = "WHERE ticker = ?"
        params.append(ticker)
    rows = get_db().execute(
        f"""
        SELECT
          id,
          ticker,
          attempted_at,
          success,
          scope,
          providers_tried,
          metrics_source,
          profile_source,
          fallback_used,
          market_data_status,
          error_message,
          price
        FROM market_data_sync_audit
        {where}
        ORDER BY id DESC
        LIMIT ?
        """,
        tuple(params + [limit]),
    ).fetchall()
    items = []
    for row in rows:
        items.append(
            {
                "id": int(row["id"]),
                "ticker": str(row["ticker"] or "").strip().upper(),
                "attempted_at": row["attempted_at"],
                "success": bool(int(row["success"] or 0)),
                "scope": str(row["scope"] or "").strip().lower(),
                "providers_tried": [item.strip() for item in str(row["providers_tried"] or "").split(",") if item.strip()],
                "metrics_source": str(row["metrics_source"] or "").strip(),
                "profile_source": str(row["profile_source"] or "").strip(),
                "fallback_used": bool(int(row["fallback_used"] or 0)),
                "market_data_status": str(row["market_data_status"] or "").strip().lower(),
                "error_message": str(row["error_message"] or "").strip(),
                "price": _safe_float(row["price"]),
            }
        )
    return _json_ok(
        {
            "generated_at": _now_iso_utc(),
            "ticker": ticker or None,
            "count": len(items),
            "items": items,
        }
    )
