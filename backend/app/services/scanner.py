"""Scanner and metric-formula services."""

import ast
from datetime import datetime

from ..db import get_db
from . import _legacy as legacy

_METRIC_FORMULA_FIELDS = tuple(legacy._METRIC_FORMULA_FIELDS)
_METRIC_FORMULA_CATALOG = dict(legacy._METRIC_FORMULA_CATALOG)
_METRIC_FORMULA_ALLOWED_FUNCS = dict(legacy._METRIC_FORMULA_ALLOWED_FUNCS)


def _now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _to_number(value):
    return legacy._to_number(value)


def _metric_formula_catalog_items():
    items = []
    for key in _METRIC_FORMULA_FIELDS:
        meta = _METRIC_FORMULA_CATALOG.get(key, {})
        items.append(
            {
                "key": key,
                "title": str(meta.get("title") or key).strip(),
                "description": str(meta.get("description") or "").strip(),
                "default_formula": str(meta.get("formula") or "value").strip() or "value",
            }
        )
    return items


def _normalize_metric_formula(formula: str):
    text = str(formula or "").strip()
    return text or "value"


def _normalize_metric_formula_value(value, fallback=0.0):
    numeric = _to_number(value)
    if numeric is None:
        return float(_to_number(fallback) or 0.0)
    return float(numeric)


def _metric_formula_context(values: dict):
    context = {}
    for field in _METRIC_FORMULA_FIELDS:
        context[field] = _normalize_metric_formula_value((values or {}).get(field), fallback=0.0)
    return context


def _validate_metric_formula_expression(formula: str):
    expression = _normalize_metric_formula(formula)
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"Formula invalida: {exc.msg}.") from exc

    allowed_names = set(_METRIC_FORMULA_FIELDS) | {"value"} | set(_METRIC_FORMULA_ALLOWED_FUNCS.keys())
    allowed_node_types = (
        ast.Expression,
        ast.BinOp,
        ast.UnaryOp,
        ast.Constant,
        ast.Name,
        ast.Load,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.Mod,
        ast.Pow,
        ast.FloorDiv,
        ast.UAdd,
        ast.USub,
        ast.Call,
        ast.Compare,
        ast.Eq,
        ast.NotEq,
        ast.Lt,
        ast.LtE,
        ast.Gt,
        ast.GtE,
        ast.BoolOp,
        ast.And,
        ast.Or,
        ast.IfExp,
        ast.Not,
    )

    for node in ast.walk(tree):
        if not isinstance(node, allowed_node_types):
            raise ValueError("Formula contem operacao nao permitida.")
        if isinstance(node, ast.Name) and node.id not in allowed_names:
            raise ValueError(f"Variavel/fucao nao permitida: {node.id}.")
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in _METRIC_FORMULA_ALLOWED_FUNCS:
                raise ValueError("Chamada de funcao nao permitida.")
    return tree


def _evaluate_metric_formula(formula: str, context: dict):
    expression = _normalize_metric_formula(formula)
    tree = _validate_metric_formula_expression(expression)
    compiled = compile(tree, "<metric-formula>", "eval")
    safe_globals = {"__builtins__": {}}
    safe_globals.update(_METRIC_FORMULA_ALLOWED_FUNCS)
    return eval(compiled, safe_globals, dict(context or {}))


def _ensure_metric_formula_rows(db):
    now_iso = _now_iso()
    for item in _metric_formula_catalog_items():
        db.execute(
            """
            INSERT INTO metric_formulas (metric_key, formula, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(metric_key) DO NOTHING
            """,
            (item["key"], item["default_formula"], now_iso),
        )


def _get_metric_formula_map(db):
    _ensure_metric_formula_rows(db)
    rows = db.execute("SELECT metric_key, formula FROM metric_formulas").fetchall()
    payload = {}
    for row in rows:
        key = str(row["metric_key"] or "").strip()
        if key in _METRIC_FORMULA_FIELDS:
            payload[key] = _normalize_metric_formula(row["formula"])
    return payload


def _seed_missing_metric_baselines(db):
    rows = db.execute(
        """
        SELECT
            a.ticker,
            a.price,
            a.dy,
            a.pl,
            a.pvp,
            a.variation_day,
            a.variation_7d,
            a.variation_30d,
            a.market_cap_bi
        FROM assets a
        LEFT JOIN asset_metric_baselines b ON b.ticker = a.ticker
        WHERE b.ticker IS NULL
        """
    ).fetchall()
    for row in rows:
        legacy._upsert_asset_metric_baseline(
            db,
            row["ticker"],
            {
                "price": row["price"],
                "dy": row["dy"],
                "pl": row["pl"],
                "pvp": row["pvp"],
                "variation_day": row["variation_day"],
                "variation_7d": row["variation_7d"],
                "variation_30d": row["variation_30d"],
                "market_cap_bi": row["market_cap_bi"],
            },
        )


def _apply_metric_formulas_to_values(values: dict, formula_map: dict):
    base_context = _metric_formula_context(values)
    output = {}
    for field in _METRIC_FORMULA_FIELDS:
        formula = _normalize_metric_formula((formula_map or {}).get(field, "value"))
        scoped_context = dict(base_context)
        scoped_context["value"] = base_context[field]
        try:
            evaluated = _evaluate_metric_formula(formula, scoped_context)
        except Exception:
            evaluated = base_context[field]
        output[field] = _normalize_metric_formula_value(evaluated, fallback=base_context[field])
    return output


def get_metric_formulas_catalog():
    db = get_db()
    formula_map = _get_metric_formula_map(db)
    rows = db.execute("SELECT metric_key, updated_at FROM metric_formulas").fetchall()
    updated_map = {str(row["metric_key"]): row["updated_at"] for row in rows}
    items = []
    for item in _metric_formula_catalog_items():
        key = item["key"]
        items.append(
            {
                "key": key,
                "title": item["title"],
                "description": item["description"],
                "formula": formula_map.get(key, item["default_formula"]),
                "updated_at": updated_map.get(key),
                "example": f"{key} = {item['default_formula']}",
            }
        )
    return {
        "metrics": items,
        "allowed_variables": ["value"] + list(_METRIC_FORMULA_FIELDS),
        "allowed_functions": sorted(_METRIC_FORMULA_ALLOWED_FUNCS.keys()),
    }


def recalculate_metric_formulas_for_all_assets():
    db = get_db()
    _seed_missing_metric_baselines(db)
    formula_map = _get_metric_formula_map(db)
    rows = db.execute(
        """
        SELECT ticker, price, dy, pl, pvp, variation_day, variation_7d, variation_30d, market_cap_bi
        FROM asset_metric_baselines
        ORDER BY ticker ASC
        """
    ).fetchall()
    updated = 0
    for row in rows:
        ticker = str(row["ticker"] or "").strip().upper()
        if not ticker:
            continue
        applied = _apply_metric_formulas_to_values(
            {
                "price": row["price"],
                "dy": row["dy"],
                "pl": row["pl"],
                "pvp": row["pvp"],
                "variation_day": row["variation_day"],
                "variation_7d": row["variation_7d"],
                "variation_30d": row["variation_30d"],
                "market_cap_bi": row["market_cap_bi"],
            },
            formula_map,
        )
        cursor = db.execute(
            """
            UPDATE assets
            SET
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
                applied["price"],
                applied["dy"],
                applied["pl"],
                applied["pvp"],
                applied["variation_day"],
                applied["variation_7d"],
                applied["variation_30d"],
                applied["market_cap_bi"],
                ticker,
            ),
        )
        if int(cursor.rowcount or 0) > 0:
            updated += 1
    db.commit()
    return {"updated_count": updated, "applied_at": _now_iso()}


def update_metric_formula(metric_key: str, formula: str):
    key = str(metric_key or "").strip().lower()
    if key not in _METRIC_FORMULA_FIELDS:
        return False, "Metrica invalida.", None

    normalized_formula = _normalize_metric_formula(formula)
    try:
        probe_context = _metric_formula_context({field: 1.0 for field in _METRIC_FORMULA_FIELDS})
        probe_context["value"] = 1.0
        probe_value = _evaluate_metric_formula(normalized_formula, probe_context)
        if _to_number(probe_value) is None:
            return False, "Formula precisa retornar um valor numerico.", None
    except Exception as exc:
        return False, f"Formula invalida: {exc}", None

    db = get_db()
    _ensure_metric_formula_rows(db)
    db.execute(
        """
        UPDATE metric_formulas
        SET formula = ?, updated_at = ?
        WHERE metric_key = ?
        """,
        (normalized_formula, _now_iso(), key),
    )
    result = recalculate_metric_formulas_for_all_assets()
    return True, "Formula salva e aplicada em todos os tickers.", {
        "metric_key": key,
        "formula": normalized_formula,
        "recalculate": result,
    }


__all__ = [
    "get_metric_formulas_catalog",
    "recalculate_metric_formulas_for_all_assets",
    "update_metric_formula",
]
