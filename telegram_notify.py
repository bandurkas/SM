"""
telegram_notify.py — Lightweight Telegram push notifications.
"""

import logging
import os
import queue
import threading
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_API = "https://api.telegram.org/bot{token}/sendMessage"

_queue: queue.Queue = queue.Queue(maxsize=50)
_worker_started = False
_worker_lock = threading.Lock()


def _worker() -> None:
    while True:
        item = _queue.get()
        if item is None:
            break
        text, event_id, db = item
        
        if event_id and db and hasattr(db, "was_notified"):
            if db.was_notified(event_id):
                logger.info(f"Skipping duplicate notification: {event_id}")
                _queue.task_done()
                continue
            db.mark_notified(event_id)

        token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        if token and chat_id:
            try:
                resp = requests.post(
                    _API.format(token=token),
                    json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                    timeout=5,
                )
                if not resp.ok:
                    logger.warning(f"Telegram error {resp.status_code}: {resp.text[:200]}")
            except Exception as exc:
                logger.warning(f"Telegram send failed: {exc}")
        _queue.task_done()


def _ensure_worker() -> None:
    global _worker_started
    if _worker_started:
        return
    with _worker_lock:
        if not _worker_started:
            t = threading.Thread(target=_worker, daemon=True, name="telegram-notify")
            t.start()
            _worker_started = True


def _send(text: str, event_id: Optional[str] = None, db: Optional[object] = None) -> None:
    _ensure_worker()
    try:
        _queue.put_nowait((text, event_id, db))
    except queue.Full:
        logger.warning("Telegram queue full")


def notify_trade_open(side: str, entry: float, stop: float, tp3: float, qty: float, db: Optional[object] = None) -> None:
    text = (
        f"🚀 <b>{side.upper()} OPENED</b>\n"
        f"Entry : <code>${entry:.2f}</code>\n"
        f"Stop  : <code>${stop:.2f}</code>\n"
        f"TP3   : <code>${tp3:.2f}</code>\n"
        f"Qty   : <code>{qty:.3f} ETH</code>"
    )
    event_id = f"open_{side}_{entry}_{qty}"
    _send(text, event_id, db)


def notify_partial_close(side: str, reason: str, exit_price: float, qty: float, entry: float) -> None:
    pnl = (exit_price - entry) * qty if side == "long" else (entry - exit_price) * qty
    sign = "+" if pnl >= 0 else ""
    icon = "💰" if pnl >= 0 else "📉"
    text = (
        f"{icon} <b>PARTIAL CLOSE</b> [{reason}]\n"
        f"Price : <code>${exit_price:.2f}</code>\n"
        f"Qty   : <code>{qty:.3f} ETH</code>\n"
        f"PnL   : <b><code>{sign}${pnl:.2f}</code></b>"
    )
    _send(text)


def notify_trade_close(side: str, entry: float, exit_price: float, qty: float, reason: str, db: Optional[object] = None) -> None:
    pnl = (exit_price - entry) * qty if side == "long" else (entry - exit_price) * qty
    sign = "+" if pnl >= 0 else ""
    icon = "✅" if pnl >= 0 else "❌"
    label_map = {
        "stop_hit": "Stop hit",
        "tp3": "TP3",
        "new_signal_opposite_side": "Reversed",
    }
    label = label_map.get(reason, reason)
    text = (
        f"{icon} <b>{side.upper()} CLOSED</b> [{label}]\n"
        f"Entry : <code>${entry:.2f}</code> → <code>${exit_price:.2f}</code>\n"
        f"PnL   : <b><code>{sign}${pnl:.2f}</code></b>"
    )
    event_id = f"close_{side}_{exit_price}_{qty}"
    _send(text, event_id, db)


def notify_heartbeat(balance: float, pos_status: str, db: Optional[object] = None) -> None:
    # 6-hour window for event_id
    window = int(time.time() / (6 * 3600))
    event_id = f"heartbeat_{window}"
    text = (
        f"💓 <b>HEARTBEAT</b>\n"
        f"Balance : <code>${balance:.2f}</code>\n"
        f"Status  : <b>{pos_status}</b>"
    )
    _send(text, event_id, db)


def notify_error(msg: str) -> None:
    text = f"⚠️ <b>ETH Bot ERROR</b>\n{msg}"
    _send(text)


def notify_critical(msg: str) -> None:
    text = f"🚨 <b>CRITICAL FAILURE</b>\n{msg}"
    _send(text)
