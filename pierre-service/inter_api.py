"""Cliente read-only da API do Banco Inter (conta PJ / Inter Empresas).

Auth: OAuth2 client_credentials com mTLS obrigatório. Cada conta PJ é uma
"aplicação" criada no IB, com client_id/client_secret + par certificado/chave.
Credenciais via env, por prefixo (ECS_, VCMS_):

    {P}_CLIENT_ID / {P}_CLIENT_SECRET
    {P}_CERTIFICATE_64 / {P}_PRIVATE_KEY_64   (PEM em base64)

O certificado/chave são materializados em /tmp com permissão 0600 apenas para o
handshake TLS. Token dura 1h; cache em processo com renovação antecipada.

Endpoints usados (https://cdpj.partners.bancointer.com.br):
    POST /oauth/v2/token                       (scope extrato.read saldo.read)
    GET  /banking/v2/extrato?dataInicio&dataFim  (janela máx. ~90 dias)
    GET  /banking/v2/saldo

O certificado emitido pelo Inter expira em 1 ano.
"""

import base64
import json
import logging
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

log = logging.getLogger("inter")

BASE_URL = "https://cdpj.partners.bancointer.com.br"
SCOPES = "extrato.read saldo.read"
_PREFIXES = ("ECS", "VCMS")

_LOCK = threading.Lock()
_tokens = {}      # prefix -> (expires_ts, token)
_cert_files = {}  # prefix -> (cert_path, key_path)


class InterNotConfigured(RuntimeError):
    pass


class InterError(RuntimeError):
    pass


def account_prefixes():
    out = []
    for p in _PREFIXES:
        if all(os.getenv(f"{p}_{k}") for k in ("CLIENT_ID", "CLIENT_SECRET", "CERTIFICATE_64", "PRIVATE_KEY_64")):
            out.append(p)
    return out


def is_configured():
    return bool(account_prefixes())


def _cert_paths(prefix):
    with _LOCK:
        hit = _cert_files.get(prefix)
        if hit and all(os.path.exists(f) for f in hit):
            return hit
        cert_path = f"/tmp/inter_{prefix.lower()}.crt"
        key_path = f"/tmp/inter_{prefix.lower()}.key"
        for path, var in ((cert_path, f"{prefix}_CERTIFICATE_64"), (key_path, f"{prefix}_PRIVATE_KEY_64")):
            raw = base64.b64decode(os.environ[var])
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "wb") as fh:
                fh.write(raw)
        _cert_files[prefix] = (cert_path, key_path)
        return _cert_files[prefix]


def _ssl_context(prefix):
    cert, key = _cert_paths(prefix)
    ctx = ssl.create_default_context()
    ctx.load_cert_chain(cert, key)
    return ctx


def _timeout():
    try:
        return float(os.getenv("INTER_TIMEOUT_SECONDS", "30"))
    except ValueError:
        return 30.0


def _token(prefix, force=False):
    now = time.time()
    with _LOCK:
        hit = _tokens.get(prefix)
        if hit and not force and now < hit[0]:
            return hit[1]
    body = urllib.parse.urlencode({
        "client_id": os.environ[f"{prefix}_CLIENT_ID"],
        "client_secret": os.environ[f"{prefix}_CLIENT_SECRET"],
        "grant_type": "client_credentials",
        "scope": SCOPES,
    }).encode()
    req = urllib.request.Request(
        BASE_URL + "/oauth/v2/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=_timeout(), context=_ssl_context(prefix)) as r:
            payload = json.loads(r.read())
    except urllib.error.HTTPError as exc:
        raise InterError(f"Inter token ({prefix}) falhou: HTTP {exc.code} {exc.read()[:150]!r}") from exc
    token = payload.get("access_token")
    if not token:
        raise InterError(f"Inter token ({prefix}) sem access_token")
    ttl = int(payload.get("expires_in") or 3600)
    with _LOCK:
        _tokens[prefix] = (now + max(ttl - 300, 60), token)
    return token


def _get(prefix, path, params=None):
    query = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v not in (None, "")})
    url = BASE_URL + path + (f"?{query}" if query else "")
    for attempt in (1, 2):
        token = _token(prefix, force=attempt == 2)
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=_timeout(), context=_ssl_context(prefix)) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403) and attempt == 1:
                continue
            raise InterError(f"Inter GET {path} ({prefix}) falhou: HTTP {exc.code} {exc.read()[:150]!r}") from exc
        except Exception as exc:
            raise InterError(f"Inter GET {path} ({prefix}) falhou: {exc}") from exc


def get_saldo(prefix):
    return _get(prefix, "/banking/v2/saldo")


def get_extrato(prefix, date_from, date_to):
    """Extrato simples. A API limita a janela (~90 dias); para períodos maiores,
    o chamador fatia. Retorna a lista `transacoes` crua."""
    payload = _get(prefix, "/banking/v2/extrato", {"dataInicio": date_from, "dataFim": date_to})
    return payload.get("transacoes") or []


def status():
    out = {"configured": is_configured(), "accounts": []}
    for p in account_prefixes():
        entry = {"prefix": p}
        try:
            saldo = get_saldo(p)
            entry["saldo_disponivel"] = saldo.get("disponivel")
        except Exception as exc:  # noqa: BLE001 - diagnóstico
            entry["error"] = str(exc)
        out["accounts"].append(entry)
    return out
