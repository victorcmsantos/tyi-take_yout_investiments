"""Envio de alertas Telegram do scanner (reaproveita o mesmo bot do app)."""

from __future__ import annotations

import json
from urllib.request import Request, urlopen

from loguru import logger

from config.settings import AppSettings


def send_telegram_alert(settings: AppSettings, text: str) -> bool:
    """Envia um alerta simples via Telegram. Retorna True se enviou."""
    token = (settings.telegram_bot_token or "").strip()
    chat_id = (settings.telegram_chat_id or "").strip()
    if not token or not chat_id:
        return False

    payload: dict[str, object] = {
        "chat_id": chat_id,
        "text": (text or "").strip()[:3900],
        "disable_web_page_preview": True,
    }
    thread_id = (settings.telegram_thread_id or "").strip()
    if thread_id and thread_id not in {"0", ""}:
        try:
            payload["message_thread_id"] = int(thread_id)
        except (TypeError, ValueError):
            pass

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=8) as response:
            return int(response.getcode() or 200) < 400
    except Exception as exc:  # noqa: BLE001
        logger.warning("Telegram alert failed", error=str(exc))
        return False
