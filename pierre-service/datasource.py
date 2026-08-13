"""Chave de fonte de dados financeira: Pierre (default) ou Pluggy.

Fonte efetiva, em ordem: override por requisição (?source=pluggy|pierre, setado
pelo app.py num contextvar) -> env FINANCE_DATA_SOURCE -> "pierre".

overview/cashflow chamam datasource.get_* em vez de pierre.get_* diretamente;
os payloads têm o mesmo shape nas duas fontes (ver pluggy_source.py).
"""

import contextvars
import os

import pierre
import pluggy_source

_request_source = contextvars.ContextVar("finance_source", default=None)

VALID = ("pierre", "pluggy")


def set_request_source(value):
    v = (value or "").strip().lower()
    _request_source.set(v if v in VALID else None)


def current_source():
    v = _request_source.get()
    if v in VALID:
        return v
    env = (os.getenv("FINANCE_DATA_SOURCE") or "").strip().lower()
    return env if env in VALID else "pierre"


def _mod():
    return pluggy_source if current_source() == "pluggy" else pierre


def get_accounts():
    return _mod().get_accounts()


def get_balance():
    return pierre.get_balance()  # sem equivalente Pluggy; usado só na rota /balance


def get_transactions(start_date=None, end_date=None, account_type=None, fmt="structured"):
    return _mod().get_transactions(start_date=start_date, end_date=end_date, account_type=account_type, fmt=fmt)


def get_bills(account_id=None):
    return _mod().get_bills(account_id)


def get_installments(start_date=None, end_date=None):
    return _mod().get_installments(start_date=start_date, end_date=end_date)
