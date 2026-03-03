import json
import os
import ssl
import urllib.error
import urllib.request
from pathlib import Path


class OpenClawError(RuntimeError):
    pass


def _load_gateway_token_from_openclaw_json(config_path: str):
    try:
        raw = Path(config_path).read_text(encoding="utf-8")
        parsed = json.loads(raw)
        token = (
            ((parsed.get("gateway") or {}).get("auth") or {}).get("token")
            or ""
        )
        token = str(token).strip()
        return token or None
    except Exception:
        return None


def _resolve_gateway_token():
    token = (os.getenv("OPENCLAW_GATEWAY_TOKEN") or "").strip()
    if token:
        return token

    config_path = (os.getenv("OPENCLAW_CONFIG_PATH") or "").strip()
    if not config_path:
        config_path = "/openclaw-config/openclaw.json"

    token = _load_gateway_token_from_openclaw_json(config_path)
    return token


def _resolve_gateway_url():
    url = (os.getenv("OPENCLAW_GATEWAY_URL") or "").strip()
    if not url:
        url = "https://openclaw-gateway:18789"
    return url.rstrip("/")


def _resolve_ssl_context():
    ca_path = (os.getenv("OPENCLAW_TLS_CA_BUNDLE") or "").strip()
    if ca_path and Path(ca_path).is_file():
        return ssl.create_default_context(cafile=ca_path)

    if str(os.getenv("OPENCLAW_TLS_INSECURE") or "").strip().lower() in {"1", "true", "yes", "on"}:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    # Default behavior: system trust store.
    return ssl.create_default_context()


def invoke_tool(tool: str, args: dict, *, action: str | None = None, session_key: str | None = None, timeout_seconds: int = 60):
    tool_name = (tool or "").strip()
    if not tool_name:
        raise OpenClawError("tool name is required")

    token = _resolve_gateway_token()
    if not token:
        raise OpenClawError("OPENCLAW_GATEWAY_TOKEN ausente (ou nao foi possivel ler de /openclaw-config/openclaw.json).")

    url = f"{_resolve_gateway_url()}/tools/invoke"
    body = {
        "tool": tool_name,
        "args": args or {},
    }
    if action:
        body["action"] = action
    if session_key:
        body["sessionKey"] = session_key

    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )

    ctx = _resolve_ssl_context()

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds, context=ctx) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        message = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        raise OpenClawError(f"OpenClaw HTTP {exc.code}: {message}") from exc
    except Exception as exc:
        raise OpenClawError(f"Falha ao chamar OpenClaw: {exc}") from exc

    try:
        parsed = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise OpenClawError(f"Resposta invalida do OpenClaw (nao e JSON): {raw[:2000]}") from exc

    if not isinstance(parsed, dict) or not parsed.get("ok"):
        raise OpenClawError(f"Resposta inesperada do OpenClaw: {parsed}")

    return parsed.get("result")
