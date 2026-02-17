import csv
import io
import json
import random
import time
from datetime import datetime, timedelta
from urllib.error import URLError
from urllib.request import Request, urlopen

from .db import get_db

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

_FX_CACHE = {"usdbrl": None, "expires_at": 0.0}
_BCB_SERIES_CACHE = {}


def _row_to_dict(row):
    return dict(row) if row else None


def get_top_assets():
    db = get_db()
    rows = db.execute("SELECT * FROM assets ORDER BY market_cap_bi DESC").fetchall()
    return [dict(row) for row in rows]


def get_asset(ticker: str):
    db = get_db()
    row = db.execute(
        "SELECT * FROM assets WHERE ticker = ?",
        (ticker.upper(),),
    ).fetchone()
    return _row_to_dict(row)


def get_portfolios():
    db = get_db()
    rows = db.execute("SELECT id, name FROM portfolios ORDER BY id ASC").fetchall()
    return [dict(row) for row in rows]


def get_portfolio(portfolio_id: int):
    db = get_db()
    row = db.execute("SELECT id, name FROM portfolios WHERE id = ?", (portfolio_id,)).fetchone()
    return _row_to_dict(row)


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
        result = [get_default_portfolio_id()]
    return result


def get_default_portfolio_id():
    db = get_db()
    row = db.execute("SELECT id FROM portfolios ORDER BY id ASC LIMIT 1").fetchone()
    return int(row["id"]) if row else 1


def resolve_portfolio_id(raw_portfolio_id):
    if raw_portfolio_id in (None, ""):
        return get_default_portfolio_id()
    try:
        pid = int(raw_portfolio_id)
    except (TypeError, ValueError):
        return get_default_portfolio_id()
    return pid if get_portfolio(pid) else get_default_portfolio_id()


def create_portfolio(name: str):
    clean_name = (name or "").strip()
    if not clean_name:
        return False, "Nome da carteira e obrigatorio."
    db = get_db()
    existing = db.execute(
        "SELECT id FROM portfolios WHERE LOWER(name) = LOWER(?)",
        (clean_name,),
    ).fetchone()
    if existing:
        return False, "Ja existe uma carteira com esse nome."
    db.execute("INSERT INTO portfolios (name) VALUES (?)", (clean_name,))
    db.commit()
    row = db.execute("SELECT id FROM portfolios WHERE name = ?", (clean_name,)).fetchone()
    return True, int(row["id"])


def delete_portfolio(portfolio_id):
    try:
        pid = int(portfolio_id)
    except (TypeError, ValueError):
        return False, "Carteira invalida."

    db = get_db()
    portfolio = db.execute("SELECT id, name FROM portfolios WHERE id = ?", (pid,)).fetchone()
    if not portfolio:
        return False, "Carteira nao encontrada."

    total_row = db.execute("SELECT COUNT(*) AS total FROM portfolios").fetchone()
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

    db.execute("DELETE FROM portfolios WHERE id = ?", (pid,))
    db.commit()
    return True, portfolio["name"]


def _parse_float(value: str):
    if isinstance(value, (int, float)):
        return float(value)

    raw_value = (value or "").strip()
    raw_value = (
        raw_value.replace("R$", "")
        .replace("r$", "")
        .replace("US$", "")
        .replace("us$", "")
        .replace("$", "")
        .replace("%", "")
        .replace(" ", "")
    )

    if "," in raw_value and "." in raw_value:
        # Ex.: 1.234,56 -> 1234.56
        raw_value = raw_value.replace(".", "").replace(",", ".")
    elif "," in raw_value:
        # Ex.: 26,76 -> 26.76
        raw_value = raw_value.replace(",", ".")
    # Se vier somente com ponto decimal (ex.: 31.50), mantem como esta.

    if raw_value == "":
        return None
    try:
        return float(raw_value)
    except ValueError:
        return None


def _to_yahoo_symbol(ticker: str):
    symbol = (ticker or "").strip().upper()
    if "." in symbol or "-" in symbol:
        return symbol
    return f"{symbol}.SA"


def _to_number(value):
    try:
        if value is None:
            return None
        if hasattr(value, "iloc"):
            try:
                if len(value) == 0:
                    return None
                value = value.iloc[0]
            except Exception:
                return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_get(container, key):
    if container is None:
        return None
    try:
        return container.get(key)
    except Exception:
        return None


def _candidate_yahoo_symbols(ticker: str):
    raw_ticker = (ticker or "").strip().upper()
    if not raw_ticker:
        return []

    # Ativos dos EUA e cripto em USD devem consultar o simbolo original.
    if _is_us_stock_ticker(raw_ticker) or raw_ticker.endswith("-USD"):
        return [raw_ticker]

    symbols = [_to_yahoo_symbol(raw_ticker)]
    if "." not in raw_ticker and raw_ticker not in symbols:
        symbols.append(raw_ticker)
    return symbols


def _http_get_json(url: str):
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
        },
    )
    for _ in range(2):
        try:
            with urlopen(request, timeout=8) as response:
                body = response.read()
            if not body:
                continue
            return json.loads(body.decode("utf-8"))
        except (URLError, TimeoutError, ValueError):
            time.sleep(0.15)
            continue
        except Exception:
            time.sleep(0.15)
            continue
    return None


def _fetch_bcb_series(series_code: int, date_start: str, date_end: str):
    cache_key = (int(series_code), date_start, date_end)
    if cache_key in _BCB_SERIES_CACHE:
        return _BCB_SERIES_CACHE[cache_key]

    start_dt = datetime.strptime(date_start, "%Y-%m-%d")
    end_dt = datetime.strptime(date_end, "%Y-%m-%d")
    data_inicial = start_dt.strftime("%d/%m/%Y")
    data_final = end_dt.strftime("%d/%m/%Y")
    url = (
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs."
        f"{series_code}/dados?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
    )
    payload = _http_get_json(url) or []
    parsed = []
    for item in payload:
        raw_date = (item.get("data") or "").strip()
        raw_value = item.get("valor")
        try:
            date_value = datetime.strptime(raw_date, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            continue
        numeric = _parse_float(raw_value)
        if numeric is None:
            continue
        parsed.append((date_value, float(numeric)))

    _BCB_SERIES_CACHE[cache_key] = parsed
    return parsed


def _compound_from_bcb_series(
    series_code: int,
    start_date,
    end_date,
    multiplier: float = 1.0,
    extrapolation_step_days: float = 1.0,
):
    if start_date > end_date:
        return 1.0, True
    start_iso = start_date.strftime("%Y-%m-%d")
    end_iso = end_date.strftime("%Y-%m-%d")
    try:
        series = _fetch_bcb_series(series_code, start_iso, end_iso)
    except Exception:
        return 1.0, False
    if not series:
        return 1.0, False

    factor = 1.0
    for _, pct_value in series:
        factor *= 1 + ((pct_value / 100.0) * multiplier)
    try:
        last_series_date = datetime.strptime(series[-1][0], "%Y-%m-%d").date()
    except Exception:
        last_series_date = end_date
    missing_days = max((end_date - last_series_date).days, 0)
    if missing_days > 0 and extrapolation_step_days > 0:
        last_pct_value = float(series[-1][1])
        step_factor = 1 + ((last_pct_value / 100.0) * multiplier)
        factor *= step_factor ** (missing_days / extrapolation_step_days)
    return factor, True


def _fetch_yahoo_quote(symbol: str):
    payload = _http_get_json(f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}")
    if not payload:
        return {}
    try:
        result = payload.get("quoteResponse", {}).get("result", [])
        if result:
            return result[0] or {}
    except Exception:
        return {}
    return {}


def _fetch_yahoo_quote_summary(symbol: str):
    modules = "assetProfile,summaryDetail,defaultKeyStatistics,price"
    payload = _http_get_json(
        f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules={modules}"
    )
    if not payload:
        return {}
    try:
        result = payload.get("quoteSummary", {}).get("result", [])
        if result:
            return result[0] or {}
    except Exception:
        return {}
    return {}


def _metrics_from_quote(quote: dict):
    if not quote:
        return None

    quote_price = _to_number(quote.get("regularMarketPrice")) or _to_number(quote.get("postMarketPrice"))
    quote_prev_close = _to_number(quote.get("regularMarketPreviousClose"))
    quote_change_pct = _to_number(quote.get("regularMarketChangePercent"))
    quote_pl = _to_number(quote.get("trailingPE")) or _to_number(quote.get("forwardPE"))
    quote_pvp = _to_number(quote.get("priceToBook"))
    quote_cap = _to_number(quote.get("marketCap"))
    quote_dy_raw = _to_number(quote.get("trailingAnnualDividendYield")) or _to_number(
        quote.get("dividendYield")
    )
    quote_dy = None if quote_dy_raw is None else (quote_dy_raw * 100 if quote_dy_raw <= 1.5 else quote_dy_raw)

    quote_variation = None
    if quote_price is not None and quote_prev_close not in (None, 0):
        quote_variation = ((quote_price / quote_prev_close) - 1) * 100
    elif quote_change_pct is not None:
        quote_variation = quote_change_pct

    if any(value is not None for value in [quote_price, quote_pl, quote_pvp, quote_dy]):
        return {
            "price": quote_price,
            "pl": quote_pl,
            "pvp": quote_pvp,
            "dy": quote_dy,
            "variation_day": quote_variation,
            "variation_7d": None,
            "variation_30d": None,
            "market_cap_bi": (quote_cap / 1_000_000_000) if quote_cap is not None else None,
        }

    return None


def _is_us_stock_ticker(ticker: str):
    ticker_up = (ticker or "").strip().upper()
    if not ticker_up:
        return False
    if ticker_up.endswith("11"):
        return False
    if ticker_up.endswith("USDT") or ticker_up.endswith("-USD"):
        return False
    clean = ticker_up.replace(".", "")
    return clean.isalpha() and len(clean) <= 6


def _is_usd_quoted_ticker(ticker: str):
    ticker_up = (ticker or "").strip().upper()
    return ticker_up.endswith("-USD") or _is_us_stock_ticker(ticker_up)


def _get_usdbrl_rate():
    now = time.time()
    stale_rate = _FX_CACHE["usdbrl"]
    if _FX_CACHE["usdbrl"] is not None and now < _FX_CACHE["expires_at"]:
        return _FX_CACHE["usdbrl"]

    for symbol in ("BRL=X", "USDBRL=X"):
        quote = _fetch_yahoo_quote(symbol)
        rate = _to_number(quote.get("regularMarketPrice")) or _to_number(quote.get("postMarketPrice"))
        if rate is not None and rate > 0:
            _FX_CACHE["usdbrl"] = rate
            _FX_CACHE["expires_at"] = now + 300
            return rate

    # Fallback HTTP fora do Yahoo (mais resiliencia quando Yahoo oscila).
    awesome = _http_get_json("https://economia.awesomeapi.com.br/json/last/USD-BRL")
    try:
        awesome_rate = _to_number((awesome or {}).get("USDBRL", {}).get("bid"))
    except Exception:
        awesome_rate = None
    if awesome_rate is not None and awesome_rate > 0:
        _FX_CACHE["usdbrl"] = awesome_rate
        _FX_CACHE["expires_at"] = now + 300
        return awesome_rate

    erapi = _http_get_json("https://open.er-api.com/v6/latest/USD")
    try:
        erapi_rate = _to_number((erapi or {}).get("rates", {}).get("BRL"))
    except Exception:
        erapi_rate = None
    if erapi_rate is not None and erapi_rate > 0:
        _FX_CACHE["usdbrl"] = erapi_rate
        _FX_CACHE["expires_at"] = now + 300
        return erapi_rate

    if yf is not None:
        for symbol in ("BRL=X", "USDBRL=X"):
            try:
                ticker = yf.Ticker(symbol)
                fast = ticker.fast_info or {}
                info = ticker.info or {}
                rate = (
                    _to_number(_safe_get(fast, "lastPrice"))
                    or _to_number(_safe_get(info, "regularMarketPrice"))
                    or _to_number(_safe_get(info, "currentPrice"))
                )
                if rate is not None and rate > 0:
                    _FX_CACHE["usdbrl"] = rate
                    _FX_CACHE["expires_at"] = now + 300
                    return rate
            except Exception:
                continue

        # Fallback final via historico diario.
        for symbol in ("BRL=X", "USDBRL=X"):
            try:
                hist = yf.download(symbol, period="5d", interval="1d", progress=False, threads=False)
                if hist is not None and not hist.empty:
                    rate = _to_number(hist["Close"].dropna().iloc[-1])
                    if rate is not None and rate > 0:
                        _FX_CACHE["usdbrl"] = rate
                        _FX_CACHE["expires_at"] = now + 300
                        return rate
            except Exception:
                continue
    # Se nada respondeu agora, usa ultimo valor em cache para evitar falhas em lote.
    return stale_rate


def _metrics_in_brl_if_needed(ticker: str, metrics: dict):
    if not metrics or not _is_usd_quoted_ticker(ticker):
        return metrics

    usdbrl = _get_usdbrl_rate()
    if usdbrl is None:
        return metrics

    updated = dict(metrics)
    if updated.get("price") is not None:
        updated["price"] = updated["price"] * usdbrl
    if updated.get("market_cap_bi") is not None:
        updated["market_cap_bi"] = updated["market_cap_bi"] * usdbrl
    return updated


def _convert_usd_to_brl_if_needed(ticker: str, amount: float):
    if amount is None:
        return True, None, None
    if not _is_us_stock_ticker(ticker):
        return True, amount, None

    usdbrl = _get_usdbrl_rate()
    if usdbrl is None:
        return False, None, "Nao foi possivel obter cotacao USD/BRL para converter ativo dos EUA."
    return True, amount * usdbrl, None


def _history_variations(hist, current_price=None):
    if hist is None or hist.empty:
        return {"variation_7d": None, "variation_30d": None}

    try:
        closes = hist["Close"].dropna()
    except Exception:
        return {"variation_7d": None, "variation_30d": None}

    if closes.empty:
        return {"variation_7d": None, "variation_30d": None}

    last = _to_number(current_price) or _to_number(closes.iloc[-1])
    if last is None or last == 0:
        return {"variation_7d": None, "variation_30d": None}

    var_7d = None
    var_30d = None

    if len(closes) >= 8:
        base_7 = _to_number(closes.iloc[-8])
        if base_7 not in (None, 0):
            var_7d = ((last / base_7) - 1) * 100

    if len(closes) >= 31:
        base_30 = _to_number(closes.iloc[-31])
        if base_30 not in (None, 0):
            var_30d = ((last / base_30) - 1) * 100

    return {"variation_7d": var_7d, "variation_30d": var_30d}


def _history_config(range_key: str):
    key = (range_key or "1y").lower()
    configs = {
        "1d": {"period": "5d", "interval": "30m", "date_fmt": "%H:%M"},
        "7d": {"period": "10d", "interval": "1h", "date_fmt": "%d/%m"},
        "30d": {"period": "2mo", "interval": "1d", "date_fmt": "%d/%m"},
        "6m": {"period": "6mo", "interval": "1d", "date_fmt": "%d/%m"},
        "1y": {"period": "1y", "interval": "1d", "date_fmt": "%d/%m/%y"},
        "5y": {"period": "5y", "interval": "1wk", "date_fmt": "%d/%m/%y"},
    }
    return key if key in configs else "1y", configs.get(key, configs["1y"])


def _extract_close_series(hist):
    if hist is None:
        return None
    try:
        if "Close" in hist:
            closes = hist["Close"]
        else:
            return None
    except Exception:
        return None

    # Em algumas versoes/formatos, o "Close" pode vir como DataFrame.
    try:
        if hasattr(closes, "columns"):
            columns = list(getattr(closes, "columns", []))
            if not columns:
                return None
            closes = closes[columns[0]]
    except Exception:
        return None
    return closes


def _fetch_chart_points(symbol: str, range_key: str):
    range_map = {
        "1d": ("5d", "30m"),
        "7d": ("10d", "1h"),
        "30d": ("1mo", "1d"),
        "6m": ("6mo", "1d"),
        "1y": ("1y", "1d"),
        "5y": ("5y", "1wk"),
    }
    r, i = range_map.get(range_key, ("1y", "1d"))
    payload = _http_get_json(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={r}&interval={i}"
    )
    if not payload:
        return []
    try:
        result = payload.get("chart", {}).get("result", [])
        if not result:
            return []
        item = result[0] or {}
        timestamps = item.get("timestamp") or []
        quote = ((item.get("indicators") or {}).get("quote") or [{}])[0]
        closes = quote.get("close") or []
    except Exception:
        return []

    points = []
    for ts, close in zip(timestamps, closes):
        close_value = _to_number(close)
        if close_value is None:
            continue
        try:
            dt = datetime.fromtimestamp(int(ts))
        except Exception:
            continue
        points.append((dt, float(close_value)))
    return points


def get_asset_price_history(ticker: str, range_key: str = "1y"):
    normalized_key, cfg = _history_config(range_key)
    result = {
        "range_key": normalized_key,
        "labels": [],
        "prices": [],
        "change_pct": None,
    }

    usdbrl = _get_usdbrl_rate() if _is_usd_quoted_ticker(ticker) else None

    for symbol in _candidate_yahoo_symbols(ticker):
        points = []

        if yf is not None:
            try:
                hist = yf.download(
                    symbol,
                    period=cfg["period"],
                    interval=cfg["interval"],
                    progress=False,
                    threads=False,
                    auto_adjust=False,
                )
            except Exception:
                hist = None

            closes = _extract_close_series(hist)
            if closes is not None:
                try:
                    close_series = closes.dropna()
                except Exception:
                    close_series = closes
                try:
                    for idx, value in close_series.items():
                        close_value = _to_number(value)
                        if close_value is None:
                            continue
                        dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                        points.append((dt, float(close_value)))
                except Exception:
                    points = []

        if not points:
            points = _fetch_chart_points(symbol, normalized_key)

        if not points:
            continue

        prices = []
        labels = []
        for dt, price in points:
            price_value = float(price)
            if usdbrl is not None and usdbrl > 0:
                price_value *= usdbrl
            prices.append(round(price_value, 2))
            try:
                labels.append(dt.strftime(cfg["date_fmt"]))
            except Exception:
                labels.append(str(dt))

        if not prices:
            continue

        first = prices[0]
        last = prices[-1]
        change_pct = ((last / first) - 1) * 100 if first not in (None, 0) else None
        return {
            "range_key": normalized_key,
            "labels": labels,
            "prices": prices,
            "change_pct": change_pct,
        }

    return result


def _fetch_yahoo_info(ticker: str):
    if yf is None:
        return {}

    for symbol in _candidate_yahoo_symbols(ticker):
        for _ in range(2):
            try:
                info = yf.Ticker(symbol).info or {}
            except Exception:
                info = {}
            if info:
                return info
    return {}


def _fetch_yahoo_profile(ticker: str):
    info = _fetch_yahoo_info(ticker)
    name = (info.get("longName") or info.get("shortName") or info.get("displayName") or "").strip()
    sector = (info.get("sectorDisp") or info.get("sector") or "").strip()

    if not name or not sector:
        for symbol in _candidate_yahoo_symbols(ticker):
            quote = _fetch_yahoo_quote(symbol)
            summary = _fetch_yahoo_quote_summary(symbol)

            if not name:
                name = (
                    (quote.get("longName") or quote.get("shortName") or "").strip()
                    or (
                        ((summary.get("price") or {}).get("longName"))
                        or ((summary.get("price") or {}).get("shortName"))
                        or ""
                    ).strip()
                )

            if not sector:
                sector = (
                    ((summary.get("assetProfile") or {}).get("sector") or "").strip()
                    or (quote.get("sectorDisp") or quote.get("sector") or "").strip()
                )

            if name and sector:
                break

    # Fallback para ativos que o Yahoo nao classifica em setor (ex.: alguns ETFs/Fundos).
    if not sector:
        name_upper = name.upper()
        short_name = (info.get("shortName") or "").upper()
        if "ETF" in name_upper or "ETF" in short_name:
            sector = "ETF"
        elif ticker.upper().endswith("11"):
            sector = "Fundos/ETFs"

    return {"name": name, "sector": sector}


def _fetch_yahoo_metrics(ticker: str):
    if yf is None:
        for symbol in _candidate_yahoo_symbols(ticker):
            quote_metrics = _metrics_from_quote(_fetch_yahoo_quote(symbol))
            if quote_metrics:
                return _metrics_in_brl_if_needed(ticker, quote_metrics)
        return None

    for symbol in _candidate_yahoo_symbols(ticker):
        for _ in range(2):
            yf_ticker = yf.Ticker(symbol)

            try:
                fast = yf_ticker.fast_info or {}
            except Exception:
                fast = {}

            try:
                info = yf_ticker.info or {}
            except Exception:
                info = {}

            price = (
                _to_number(_safe_get(fast, "lastPrice"))
                or _to_number(_safe_get(info, "regularMarketPrice"))
                or _to_number(_safe_get(info, "currentPrice"))
            )
            previous_close = _to_number(_safe_get(fast, "previousClose")) or _to_number(
                _safe_get(info, "regularMarketPreviousClose")
            )

            variation_day = None
            if price is not None and previous_close not in (None, 0):
                variation_day = ((price / previous_close) - 1) * 100
            else:
                raw_change = _to_number(_safe_get(info, "regularMarketChangePercent"))
                if raw_change is not None:
                    variation_day = raw_change * 100 if -1 <= raw_change <= 1 else raw_change

            dy_raw = _to_number(_safe_get(info, "dividendYield"))
            if dy_raw is None:
                dy_raw = _to_number(_safe_get(info, "trailingAnnualDividendYield"))
            dy = None if dy_raw is None else (dy_raw * 100 if dy_raw <= 1.5 else dy_raw)

            market_cap = _to_number(_safe_get(info, "marketCap"))
            pl = _to_number(_safe_get(info, "trailingPE")) or _to_number(_safe_get(info, "forwardPE"))
            pvp = _to_number(_safe_get(info, "priceToBook"))

            if any(value is not None for value in [price, pl, pvp, dy]):
                history_variations = {"variation_7d": None, "variation_30d": None}
                try:
                    hist = yf.download(symbol, period="3mo", interval="1d", progress=False, threads=False)
                    history_variations = _history_variations(hist, current_price=price)
                except Exception:
                    pass

                return _metrics_in_brl_if_needed(ticker, {
                    "price": price,
                    "pl": pl,
                    "pvp": pvp,
                    "dy": dy,
                    "variation_day": variation_day,
                    "variation_7d": history_variations["variation_7d"],
                    "variation_30d": history_variations["variation_30d"],
                    "market_cap_bi": (market_cap / 1_000_000_000) if market_cap is not None else None,
                })

            # Fallback mais estavel em momentos de intermitencia do yfinance.
            quote_metrics = _metrics_from_quote(_fetch_yahoo_quote(symbol))
            if quote_metrics:
                return _metrics_in_brl_if_needed(ticker, quote_metrics)

            # Fallback: algumas series retornam vazio em fast_info/info, mas possuem historico.
            try:
                hist = yf.download(symbol, period="5d", interval="1d", progress=False, threads=False)
            except Exception:
                hist = None
            if hist is not None and not hist.empty:
                close_value = _to_number(hist["Close"].dropna().iloc[-1])
                if close_value is not None:
                    history_variations = _history_variations(hist, current_price=close_value)
                    return _metrics_in_brl_if_needed(ticker, {
                        "price": close_value,
                        "pl": pl,
                        "pvp": pvp,
                        "dy": dy,
                        "variation_day": variation_day,
                        "variation_7d": history_variations["variation_7d"],
                        "variation_30d": history_variations["variation_30d"],
                        "market_cap_bi": (market_cap / 1_000_000_000)
                        if market_cap is not None
                        else None,
                    })

    return None


def refresh_asset_market_data(ticker: str):
    asset = get_asset(ticker)
    if not asset:
        return False

    profile = _fetch_yahoo_profile(ticker)
    metrics = _fetch_yahoo_metrics(ticker) or {}
    if not metrics and not profile:
        return False
    has_market_metrics = any(
        metrics.get(field) is not None
        for field in (
            "price",
            "dy",
            "pl",
            "pvp",
            "variation_day",
            "variation_7d",
            "variation_30d",
            "market_cap_bi",
        )
    )

    db = get_db()
    name = asset["name"]
    sector = asset["sector"]
    if profile:
        # Sempre prioriza perfil do Yahoo quando houver valor.
        # Isso evita ativo ficar preso com nome/setor antigo apos importacoes.
        name = profile.get("name") or name
        sector = profile.get("sector") or sector

    db.execute(
        """
        UPDATE assets
        SET
            name = ?,
            sector = ?,
            price = ?,
            dy = ?,
            pl = ?,
            pvp = ?,
            variation_day = ?,
            variation_7d = ?,
            variation_30d = ?,
            market_cap_bi = ?
        WHERE ticker = ?
        """,
        (
            name,
            sector,
            metrics.get("price") if metrics.get("price") is not None else asset["price"],
            metrics.get("dy") if metrics.get("dy") is not None else asset["dy"],
            metrics.get("pl") if metrics.get("pl") is not None else asset["pl"],
            metrics.get("pvp") if metrics.get("pvp") is not None else asset["pvp"],
            metrics.get("variation_day")
            if metrics.get("variation_day") is not None
            else asset["variation_day"],
            metrics.get("variation_7d")
            if metrics.get("variation_7d") is not None
            else asset.get("variation_7d", 0.0),
            metrics.get("variation_30d")
            if metrics.get("variation_30d") is not None
            else asset.get("variation_30d", 0.0),
            metrics.get("market_cap_bi")
            if metrics.get("market_cap_bi") is not None
            else asset["market_cap_bi"],
            ticker.upper(),
        ),
    )
    db.commit()
    # Sucesso de "atualizacao Yahoo" significa ter recebido cotacao/indicadores.
    # Atualizacao apenas de nome/setor nao conta como sync completo de mercado.
    return has_market_metrics


def refresh_all_assets_market_data(attempts: int = 3):
    db = get_db()
    rows = db.execute("SELECT ticker FROM assets").fetchall()
    tickers = [row["ticker"] for row in rows]
    if not tickers:
        return []
    return refresh_market_data_for_tickers(tickers, attempts=attempts)


def refresh_market_data_for_tickers(tickers, attempts: int = 2):
    unique_tickers = []
    for ticker in tickers:
        clean = (ticker or "").strip().upper()
        if clean and clean not in unique_tickers:
            unique_tickers.append(clean)

    failed = set(unique_tickers)
    for attempt in range(attempts):
        if not failed:
            break
        next_failed = set()
        for ticker in list(failed):
            try:
                ok = refresh_asset_market_data(ticker)
            except Exception:
                ok = False
            if not ok:
                next_failed.add(ticker)
            # Pequeno jitter reduz chance de bloqueio/rate-limit em lote.
            time.sleep(0.12 + random.random() * 0.12)
        failed = next_failed
        if failed and attempt < attempts - 1:
            # Backoff progressivo entre rodadas.
            time.sleep(0.5 * (attempt + 1))
    return sorted(failed)


def _parse_date(value: str):
    raw_value = (value or "").strip().replace("\u00a0", " ")
    if not raw_value:
        return None

    month_map = {
        "jan": "01",
        "fev": "02",
        "feb": "02",
        "mar": "03",
        "abr": "04",
        "apr": "04",
        "mai": "05",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "ago": "08",
        "aug": "08",
        "set": "09",
        "sep": "09",
        "out": "10",
        "oct": "10",
        "nov": "11",
        "dez": "12",
        "dec": "12",
    }

    raw_lower = raw_value.lower().replace(".", "")
    for name, number in month_map.items():
        raw_lower = raw_lower.replace(f"/{name}/", f"/{number}/")
        raw_lower = raw_lower.replace(f"-{name}-", f"-{number}-")
    raw_value = raw_lower

    # Excel/planilha pode exportar numero de serie de data.
    if raw_value.isdigit():
        try:
            base = datetime(1899, 12, 30)
            parsed = base + timedelta(days=int(raw_value))
            return parsed.strftime("%Y-%m-%d")
        except Exception:
            pass

    date_formats = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%d-%m-%y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
    )
    for fmt in date_formats:
        try:
            parsed = datetime.strptime(raw_value, fmt)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _position_category(ticker: str, name: str, sector: str):
    ticker_up = (ticker or "").upper()
    name_up = (name or "").upper()
    sector_up = (sector or "").upper()

    if (
        "-USD" in ticker_up
        or ticker_up.endswith("USDT")
        or "CRYPTO" in sector_up
        or "CRYPTO" in name_up
    ):
        return "crypto"

    is_fii = (
        ticker_up.endswith("11")
        and (
            "FII" in name_up
            or "IMOBILI" in name_up
            or "REIT" in name_up
            or sector_up in {"REAL ESTATE", "FUNDOS IMOBILIARIOS"}
        )
    )
    if is_fii:
        return "fiis"

    # Heuristica simples para ticker americano (ex.: AAPL, MSFT, GOOGL).
    clean = ticker_up.replace(".", "")
    if clean.isalpha() and len(clean) <= 6:
        return "us_stocks"

    return "br_stocks"


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
    ok_conversion, converted_price, conversion_error = _convert_usd_to_brl_if_needed(ticker, price)
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

    asset = get_asset(ticker)
    if not asset:
        if tx_type == "sell":
            return False, "Nao existe posicao para esse ticker."
        profile = _fetch_yahoo_profile(ticker)
        name = (
            profile["name"]
            or (form_data.get("name") or "").strip()
            or ticker
        )
        sector = (
            profile["sector"]
            or (form_data.get("sector") or "").strip()
            or "Nao informado"
        )
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
            profile = _fetch_yahoo_profile(ticker)
            name = profile["name"] or asset["name"]
            sector = profile["sector"] or asset["sector"]
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

    failed_refresh = refresh_market_data_for_tickers(sorted(csv_tickers), attempts=2)
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
    ok_conversion, converted_amount, conversion_error = _convert_usd_to_brl_if_needed(ticker, amount)
    if not ok_conversion:
        return False, conversion_error
    amount = converted_amount

    income_date = _parse_date(form_data.get("date"))
    if income_date is None:
        return False, "Data invalida. Use o formato YYYY-MM-DD."

    if _income_exists(portfolio_id, ticker, income_type, amount, income_date):
        return False, "Provento duplicado: ja existe um registro com esses mesmos dados."

    if not get_asset(ticker):
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
        # Compatibilidade para layout antigo (sem componentes) apenas para tipos simples.
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
    return True, "Renda fixa cadastrada com sucesso."


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

        # Novo padrao: escolhe automaticamente o tipo de taxa pela coluna preenchida.
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
            # Compatibilidade com layout antigo.
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


def _fixed_income_projection(item):
    aporte_date = datetime.strptime(item["date_aporte"], "%Y-%m-%d").date()
    maturity_date = datetime.strptime(item["maturity_date"], "%Y-%m-%d").date()
    today = datetime.now().date()

    principal = float(item["aporte"]) + float(item["reinvested"])
    total_days = max((maturity_date - aporte_date).days, 1)
    elapsed_days = max(min((today - aporte_date).days, total_days), 0)

    rate_fixed = max(float(item.get("rate_fixed", 0.0)), 0.0)
    rate_ipca = max(float(item.get("rate_ipca", 0.0)), 0.0)
    rate_cdi = max(float(item.get("rate_cdi", 0.0)), 0.0)
    rate_type = (item.get("rate_type") or "").upper()

    # Compatibilidade para registros antigos sem componentes separados.
    if rate_fixed == 0 and rate_ipca == 0 and rate_cdi == 0:
        legacy_rate = max(float(item.get("annual_rate", 0.0)), 0.0)
        if legacy_rate > 0:
            if rate_type == "FIXO":
                rate_fixed = legacy_rate
            elif rate_type == "CDI":
                rate_cdi = legacy_rate
            elif rate_type == "IPCA":
                rate_ipca = legacy_rate
            elif rate_type in {"FIXO+IPCA", "FIXO+CDI"}:
                rate_fixed = legacy_rate

    def _fixed_factor(days: int):
        if days <= 0 or rate_fixed <= 0:
            return 1.0
        return (1 + (rate_fixed / 100.0)) ** (days / 365.0)

    def _annualized_factor(rate: float, days: int):
        if days <= 0 or rate <= 0:
            return 1.0
        return (1 + (rate / 100.0)) ** (days / 365.0)

    def _cdi_factor(start_date, end_date):
        if rate_cdi <= 0 or start_date > end_date:
            return 1.0
        factor, has_data = _compound_from_bcb_series(
            11,
            start_date,
            end_date,
            multiplier=(rate_cdi / 100.0),
            extrapolation_step_days=1.0,
        )
        if has_data:
            return factor
        days = max((end_date - start_date).days, 0)
        return _annualized_factor(rate_cdi, days)

    def _ipca_factor(start_date, end_date):
        if rate_ipca <= 0 or start_date > end_date:
            return 1.0
        factor, has_data = _compound_from_bcb_series(
            433,
            start_date,
            end_date,
            multiplier=(rate_ipca / 100.0),
            extrapolation_step_days=30.0,
        )
        if has_data:
            return factor
        days = max((end_date - start_date).days, 0)
        return _annualized_factor(rate_ipca, days)

    def _factor_for_period(start_date, end_date, days: int):
        fixed_factor = _fixed_factor(days)
        cdi_factor = _cdi_factor(start_date, end_date)
        ipca_factor = _ipca_factor(start_date, end_date)

        if rate_type == "FIXO":
            return fixed_factor
        if rate_type == "CDI":
            return cdi_factor
        if rate_type == "IPCA":
            return ipca_factor
        if rate_type == "FIXO+IPCA":
            return fixed_factor * ipca_factor
        if rate_type == "FIXO+CDI":
            return fixed_factor * cdi_factor
        return fixed_factor * cdi_factor * ipca_factor

    current_end = aporte_date + timedelta(days=elapsed_days)
    final_end = aporte_date + timedelta(days=total_days)
    current_factor = _factor_for_period(aporte_date, current_end, elapsed_days)
    final_factor = _factor_for_period(aporte_date, final_end, total_days)
    current_value = principal * current_factor
    final_value = principal * final_factor
    is_matured = today >= maturity_date
    active_applied_value = 0.0 if is_matured else principal
    active_current_value = 0.0 if is_matured else current_value
    active_current_income = 0.0 if is_matured else (current_value - principal)
    total_received = final_value if is_matured else 0.0
    rendimento = (final_value - principal) if is_matured else 0.0

    projected = dict(item)
    projected["applied_value"] = round(principal, 2)
    projected["active_applied_value"] = round(active_applied_value, 2)
    projected["elapsed_days"] = int(elapsed_days)
    projected["total_days"] = int(total_days)
    projected["is_matured"] = is_matured
    projected["current_gross_value"] = round(active_current_value, 2)
    projected["current_income"] = round(active_current_income, 2)
    projected["final_gross_value"] = round(final_value, 2)
    projected["final_income"] = round(final_value - principal, 2)
    projected["total_received"] = round(total_received, 2)
    projected["rendimento"] = round(rendimento, 2)
    return projected


def get_fixed_incomes(portfolio_ids, sort_by: str = "date_aporte", sort_dir: str = "desc"):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            fi.id,
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

    items.sort(key=_sort_key, reverse=(direction == "desc"))
    return items


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
    return cursor.rowcount or 0


def get_fixed_income_summary(portfolio_ids):
    items = get_fixed_incomes(portfolio_ids)
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
    return cursor.rowcount


def get_incomes(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT ticker, income_type, amount, date
        FROM incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY date DESC, id DESC
        """,
        tuple(pids),
    ).fetchall()
    return [dict(row) for row in rows]


def get_income_totals_by_ticker(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT ticker, COALESCE(SUM(amount), 0) AS total_incomes
        FROM incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        GROUP BY ticker
        """,
        tuple(pids),
    ).fetchall()

    by_ticker = {row["ticker"]: float(row["total_incomes"]) for row in rows}
    total = round(sum(by_ticker.values()), 2)
    return by_ticker, total


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
    asset = get_asset(ticker)
    if not asset:
        return {
            "shares": 0,
            "avg_price": 0.0,
            "total_value": 0.0,
            "market_value": 0.0,
            "open_pnl_value": 0.0,
            "open_pnl_pct": 0.0,
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
    income_row = db.execute(
        """
        SELECT COALESCE(SUM(amount), 0) AS total_incomes
        FROM incomes
        WHERE ticker = ? AND portfolio_id IN ("""
        + placeholders
        + """)
        """,
        tuple([ticker.upper()] + pids),
    ).fetchone()
    total_incomes = float(income_row["total_incomes"]) if income_row else 0.0

    return {
        "shares": shares,
        "avg_price": round(avg_price, 2),
        "total_value": round(total_value, 2),
        "market_value": round(market_value, 2),
        "open_pnl_value": round(open_pnl_value, 2),
        "open_pnl_pct": round(open_pnl_pct, 2),
        "total_incomes": round(total_incomes, 2),
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


def get_portfolio_snapshot(portfolio_ids, sort_by: str = "value", sort_dir: str = "desc"):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            a.ticker,
            a.name,
            a.sector,
            b.shares,
            a.price,
            a.dy,
            (a.price * b.shares) AS value
        FROM assets a
        JOIN (
            SELECT
                ticker,
                SUM(CASE WHEN tx_type = 'buy' THEN shares ELSE -shares END) AS shares
            FROM transactions
            WHERE portfolio_id IN ("""
        + placeholders
        + """)
            GROUP BY ticker
            HAVING shares > 0
        ) b ON b.ticker = a.ticker
        ORDER BY value DESC
        """,
        tuple(pids),
    ).fetchall()

    positions = []
    total = 0.0
    monthly_dividends = 0.0
    invested_total = 0.0
    incomes_total = 0.0

    for row in rows:
        item = dict(row)
        total += item["value"]
        monthly_dividends += item["value"] * (item["dy"] / 100) / 12
        positions.append(
            {
                "ticker": item["ticker"],
                "name": item["name"],
                "sector": item["sector"],
                "shares": item["shares"],
                "price": item["price"],
                "value": item["value"],
            }
        )

    # Custo em aberto por ticker (media movel), para calcular resultado em aberto da carteira.
    tx_rows = db.execute(
        """
        SELECT ticker, tx_type, shares, price
        FROM transactions
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY ticker ASC, date ASC, id ASC
        """,
        tuple(pids),
    ).fetchall()
    cost_state = {}
    for tx in tx_rows:
        ticker = tx["ticker"]
        current = cost_state.get(ticker, {"shares": 0, "cost": 0.0})
        shares = current["shares"]
        cost = current["cost"]

        if tx["tx_type"] == "buy":
            shares += tx["shares"]
            cost += tx["shares"] * tx["price"]
        else:
            if shares > 0:
                avg_price = cost / shares
                sell_shares = min(tx["shares"], shares)
                shares -= sell_shares
                cost -= avg_price * sell_shares
                if shares == 0:
                    cost = 0.0

        cost_state[ticker] = {"shares": shares, "cost": cost}

    # Proventos por ticker para as carteiras selecionadas.
    income_rows = db.execute(
        """
        SELECT ticker, COALESCE(SUM(amount), 0) AS total_incomes
        FROM incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        GROUP BY ticker
        """,
        tuple(pids),
    ).fetchall()
    incomes_by_ticker = {row["ticker"]: float(row["total_incomes"]) for row in income_rows}
    incomes_total = sum(incomes_by_ticker.values())

    for item in positions:
        invested_total += cost_state.get(item["ticker"], {"cost": 0.0})["cost"]

    grouped_positions = {"br_stocks": [], "us_stocks": [], "crypto": [], "fiis": []}

    for item in positions:
        invested_item = cost_state.get(item["ticker"], {"cost": 0.0})["cost"]
        open_pnl_item = item["value"] - invested_item
        open_pnl_pct_item = (open_pnl_item / invested_item) * 100 if invested_item > 0 else 0.0
        avg_price_item = (invested_item / item["shares"]) if item["shares"] > 0 else 0.0

        item["invested_value"] = round(invested_item, 2)
        item["avg_price"] = round(avg_price_item, 2)
        item["open_pnl_value"] = round(open_pnl_item, 2)
        item["open_pnl_pct"] = round(open_pnl_pct_item, 2)
        item["total_incomes"] = round(incomes_by_ticker.get(item["ticker"], 0.0), 2)
        item["weight"] = round((item["value"] / total) * 100, 2) if total else 0.0
        item["category"] = _position_category(item["ticker"], item["name"], item["sector"])
        grouped_positions[item["category"]].append(item)

    sort_key_map = {
        "ticker": "ticker",
        "name": "name",
        "shares": "shares",
        "price": "price",
        "avg_price": "avg_price",
        "invested_value": "invested_value",
        "value": "value",
        "total_incomes": "total_incomes",
        "open_pnl_value": "open_pnl_value",
        "open_pnl_pct": "open_pnl_pct",
        "weight": "weight",
    }
    safe_sort_by = sort_key_map.get((sort_by or "").strip().lower(), "value")
    safe_sort_dir = "asc" if (sort_dir or "").strip().lower() == "asc" else "desc"
    reverse = safe_sort_dir == "desc"

    def _sort_value(item):
        value = item.get(safe_sort_by)
        if isinstance(value, str):
            return value.upper()
        return value if value is not None else 0

    for key in grouped_positions:
        grouped_positions[key] = sorted(grouped_positions[key], key=_sort_value, reverse=reverse)

    open_pnl_value = total - invested_total
    open_pnl_pct = (open_pnl_value / invested_total) * 100 if invested_total > 0 else 0.0
    group_totals = {
        key: round(sum(item["value"] for item in items), 2)
        for key, items in grouped_positions.items()
    }
    group_summaries = {}
    for key, items in grouped_positions.items():
        group_total = sum(item["value"] for item in items)
        group_invested = sum(item["invested_value"] for item in items)
        group_open_pnl = group_total - group_invested
        group_open_pnl_pct = (group_open_pnl / group_invested) * 100 if group_invested > 0 else 0.0
        group_incomes = sum(item["total_incomes"] for item in items)
        group_summaries[key] = {
            "total_value": round(group_total, 2),
            "invested_value": round(group_invested, 2),
            "open_pnl_value": round(group_open_pnl, 2),
            "open_pnl_pct": round(group_open_pnl_pct, 2),
            "total_incomes": round(group_incomes, 2),
        }

    return {
        "total_value": round(total, 2),
        "invested_value": round(invested_total, 2),
        "monthly_dividends": round(monthly_dividends, 2),
        "total_incomes": round(incomes_total, 2),
        "open_pnl_value": round(open_pnl_value, 2),
        "open_pnl_pct": round(open_pnl_pct, 2),
        "positions": positions,
        "grouped_positions": grouped_positions,
        "group_totals": group_totals,
        "group_summaries": group_summaries,
        "sort_by": safe_sort_by,
        "sort_dir": safe_sort_dir,
    }


def get_monthly_class_summary(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()

    month_names = {
        1: "jan",
        2: "fev",
        3: "mar",
        4: "abr",
        5: "mai",
        6: "jun",
        7: "jul",
        8: "ago",
        9: "set",
        10: "out",
        11: "nov",
        12: "dez",
    }

    def _month_key(date_text: str):
        try:
            parsed = datetime.strptime((date_text or "")[:10], "%Y-%m-%d")
            return parsed.year, parsed.month
        except ValueError:
            return None

    def _category_bucket(ticker: str, name: str, sector: str):
        category = _position_category(ticker, name, sector)
        if category == "br_stocks":
            return "br"
        if category == "fiis":
            return "fii"
        if category == "crypto":
            return "cripto"
        return None

    rows_map = {}
    month_set = set()

    def _ensure_month_entry(month_key):
        if month_key not in rows_map:
            rows_map[month_key] = {
                "br_invested": 0.0,
                "br_incomes": 0.0,
                "fii_invested": 0.0,
                "fii_incomes": 0.0,
                "fixa_invested": 0.0,
                "fixa_incomes": 0.0,
                "cripto_invested": 0.0,
                "cripto_incomes": 0.0,
            }
        month_set.add(month_key)

    tx_rows = db.execute(
        """
        SELECT
            t.date,
            t.tx_type,
            (t.shares * t.price) AS amount,
            a.ticker,
            a.name,
            a.sector
        FROM transactions t
        JOIN assets a ON a.ticker = t.ticker
        WHERE t.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY t.date ASC, t.id ASC
        """,
        tuple(pids),
    ).fetchall()
    for row in tx_rows:
        month_key = _month_key(row["date"])
        if not month_key:
            continue
        bucket = _category_bucket(row["ticker"], row["name"], row["sector"])
        if not bucket:
            continue
        _ensure_month_entry(month_key)
        # "Investidos" segue aporte de compras no mes.
        if row["tx_type"] == "buy":
            rows_map[month_key][f"{bucket}_invested"] += float(row["amount"] or 0.0)

    income_rows = db.execute(
        """
        SELECT
            i.date,
            i.amount,
            a.ticker,
            a.name,
            a.sector
        FROM incomes i
        JOIN assets a ON a.ticker = i.ticker
        WHERE i.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY i.date ASC, i.id ASC
        """,
        tuple(pids),
    ).fetchall()
    for row in income_rows:
        month_key = _month_key(row["date"])
        if not month_key:
            continue
        bucket = _category_bucket(row["ticker"], row["name"], row["sector"])
        if not bucket:
            continue
        _ensure_month_entry(month_key)
        rows_map[month_key][f"{bucket}_incomes"] += float(row["amount"] or 0.0)

    fixed_rows = db.execute(
        """
        SELECT
            id,
            rate_type,
            annual_rate,
            rate_fixed,
            rate_ipca,
            rate_cdi,
            date_aporte,
            maturity_date,
            aporte,
            reinvested
        FROM fixed_incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY date_aporte ASC, id ASC
        """,
        tuple(pids),
    ).fetchall()
    for row in fixed_rows:
        item = dict(row)
        aporte_month = _month_key(item["date_aporte"])
        if aporte_month:
            _ensure_month_entry(aporte_month)
            rows_map[aporte_month]["fixa_invested"] += float(item.get("aporte") or 0.0) + float(
                item.get("reinvested") or 0.0
            )

        maturity_month = _month_key(item["maturity_date"])
        if maturity_month:
            projected = _fixed_income_projection(item)
            _ensure_month_entry(maturity_month)
            rows_map[maturity_month]["fixa_incomes"] += float(projected.get("final_income") or 0.0)

    if not month_set:
        return []

    ordered_months = sorted(month_set)
    result = []
    for year, month in ordered_months:
        values = rows_map[(year, month)]
        total_invested = (
            values["br_invested"]
            + values["fii_invested"]
            + values["fixa_invested"]
            + values["cripto_invested"]
        )
        total_incomes = (
            values["br_incomes"]
            + values["fii_incomes"]
            + values["fixa_incomes"]
            + values["cripto_incomes"]
        )
        result.append(
            {
                "label": f"{month_names[month]}/{str(year)[2:]}",
                "br_invested": round(values["br_invested"], 2),
                "br_incomes": round(values["br_incomes"], 2),
                "fii_invested": round(values["fii_invested"], 2),
                "fii_incomes": round(values["fii_incomes"], 2),
                "fixa_invested": round(values["fixa_invested"], 2),
                "fixa_incomes": round(values["fixa_incomes"], 2),
                "cripto_invested": round(values["cripto_invested"], 2),
                "cripto_incomes": round(values["cripto_incomes"], 2),
                "total_invested": round(total_invested, 2),
                "total_incomes": round(total_incomes, 2),
            }
        )
    return result
