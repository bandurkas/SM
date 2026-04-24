"""
eth_bot.py — Main bot controller.

Run modes:
  python eth_bot.py --paper          # Paper trading (simulated fills)
  python eth_bot.py --live           # Live trading on HTX (requires API keys)
  python eth_bot.py --backtest       # Run backtest and exit
  python eth_bot.py --backtest --days 365

The loop fires once per closed bar:
  1. Fetch latest OHLCV + daily bars
  2. Compute indicators
  3. Detect setups → score → filter by threshold
  4. Place pending limit orders via trade_manager
  5. Update trade_manager (TP/SL/BE checks)
  6. Sleep until next bar close
"""

import argparse
import asyncio
import logging
import logging.handlers
import math
import os
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from config import BotConfig, cfg as default_cfg
from market_data import (
    calc_indicators, check_clock_drift, fetch_daily_ohlcv, 
    fetch_ohlcv, get_atr_mult, inject_htf_trend, make_exchange
)
from strategy_engine import detect_setups_df, get_signals, calc_cancel_bars_dyn, load_ml_model
from risk_engine import get_stop_price, calc_qty, calc_tp_levels, round_tick
from execution_engine import ExecutionEngine
from trade_manager import TradeManager
from paper_trader import PaperExecutionEngine
from persistence import log_balance
from backtest import run_backtest
from telegram_notify import notify_heartbeat, notify_error, notify_critical

# ── Logging setup ─────────────────────────────────────────────────────────────
data_dir = os.getenv("ETHBOT_DATA_DIR", "./data")
os.makedirs(data_dir, exist_ok=True)

_DATASET_FILE = os.path.join(data_dir, "ml_dataset.csv")

_log_file = os.path.join(data_dir, "eth_bot.log")
_rot = logging.handlers.RotatingFileHandler(
    _log_file, maxBytes=10 * 1024 * 1024, backupCount=5
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), _rot],
    force=True,
)
# Silence noisy third-party HTTP loggers
for _lib in ("ccxt", "urllib3", "requests", "asyncio"):
    logging.getLogger(_lib).setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


# ── P9: Funding rate blackout ─────────────────────────────────────────────────

FUNDING_HOURS_UTC = {0, 8, 16}   # HTX perpetual funding settlement hours

# ── Signal cooldown: prevent same-level retest spam ───────────────────────────
_LAST_SIGNAL_LEVEL: float = 0.0
_LAST_SIGNAL_BAR: int = -9999
_LAST_SIGNAL_SIDE: str = ""
_LAST_SIGNAL_TS: float = 0.0    # RISK-03: wall-clock timestamp for restart-safe cooldown
COOLDOWN_BARS: int = 6          # ~90min on 15m — skip re-fires within this window
COOLDOWN_SECS: float = COOLDOWN_BARS * 900.0  # equivalent in seconds
COOLDOWN_LEVEL_TOL_PCT: float = 0.001   # 0.1% (~$2.3 on ETH $2300)
_COOLDOWN_FILE: str = os.path.join(data_dir, "cooldown.json")

def _load_cooldown() -> None:
    """RISK-03: restore cooldown state from disk so restarts don't bypass it."""
    global _LAST_SIGNAL_LEVEL, _LAST_SIGNAL_BAR, _LAST_SIGNAL_SIDE, _LAST_SIGNAL_TS
    try:
        if os.path.exists(_COOLDOWN_FILE):
            import json as _json
            with open(_COOLDOWN_FILE) as _f:
                d = _json.load(_f)
            _LAST_SIGNAL_LEVEL = float(d.get("level", 0.0))
            _LAST_SIGNAL_BAR   = int(d.get("bar", -9999))
            _LAST_SIGNAL_SIDE  = str(d.get("side", ""))
            _LAST_SIGNAL_TS    = float(d.get("ts", 0.0))
    except Exception:
        pass

def _save_cooldown() -> None:
    try:
        import json as _json
        with open(_COOLDOWN_FILE, "w") as _f:
            _json.dump({
                "level": _LAST_SIGNAL_LEVEL,
                "bar": _LAST_SIGNAL_BAR,
                "side": _LAST_SIGNAL_SIDE,
                "ts": _LAST_SIGNAL_TS,
            }, _f)
    except Exception:
        pass

def _is_duplicate_signal(side: str, retest_level: float, bar_index: int) -> bool:
    global _LAST_SIGNAL_LEVEL, _LAST_SIGNAL_BAR, _LAST_SIGNAL_SIDE, _LAST_SIGNAL_TS
    if _LAST_SIGNAL_LEVEL <= 0:
        return False
    if side != _LAST_SIGNAL_SIDE:
        return False
    # RISK-03: use wall-clock time so cooldown survives restarts
    now_ts = time.time()
    elapsed_secs = now_ts - _LAST_SIGNAL_TS
    if elapsed_secs >= COOLDOWN_SECS:
        return False
    tol = _LAST_SIGNAL_LEVEL * COOLDOWN_LEVEL_TOL_PCT
    return abs(retest_level - _LAST_SIGNAL_LEVEL) <= tol

def _remember_signal(side: str, retest_level: float, bar_index: int) -> None:
    global _LAST_SIGNAL_LEVEL, _LAST_SIGNAL_BAR, _LAST_SIGNAL_SIDE, _LAST_SIGNAL_TS
    _LAST_SIGNAL_LEVEL = retest_level
    _LAST_SIGNAL_BAR = bar_index
    _LAST_SIGNAL_SIDE = side
    _LAST_SIGNAL_TS = time.time()
    _save_cooldown()

def is_in_funding_blackout(config: BotConfig) -> bool:
    """Returns True if we are within `funding_blackout_mins` of a funding event."""
    if not config.funding_blackout_enabled:
        return False
    now = datetime.now(timezone.utc)
    # Minutes elapsed since the last hour boundary
    mins_into_hour = now.minute
    # Distance to next / just-passed funding hour (in minutes)
    for fh in FUNDING_HOURS_UTC:
        # Forward distance
        hours_ahead = (fh - now.hour) % 24
        mins_to = hours_ahead * 60 - mins_into_hour
        if 0 <= mins_to < config.funding_blackout_mins:
            return True
        # Backward distance (just passed)
        if hours_ahead == 0 and mins_into_hour < config.funding_blackout_mins:
            return True
    return False


# ── Bar timing ────────────────────────────────────────────────────────────────

def seconds_until_bar_close(timeframe: str) -> float:
    """Seconds remaining until the current bar closes (rounded up)."""
    tf_seconds = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900,
        "30m": 1800, "1h": 3600, "4h": 14400,
    }.get(timeframe, 900)
    now_ts = time.time()
    elapsed = now_ts % tf_seconds
    remaining = tf_seconds - elapsed
    return remaining


def sleep_until_next_bar(timeframe: str, buffer_seconds: float = 3.0) -> None:
    """Block until the next bar is confirmed closed."""
    wait = seconds_until_bar_close(timeframe) + buffer_seconds
    logger.info(f"Next bar in {wait:.0f}s — sleeping…")
    time.sleep(wait)


def _heartbeat_sleep(
    timeframe: str,
    manager,
    exec_engine,
    paper: bool,
    tick_secs: float = 5.0,
    buffer_secs: float = 3.0,
    reconcile_interval: int = 6,  # reconcile every 30s (6 × 5s ticks)
) -> None:
    """
    Replaces blocking sleep_until_next_bar.
    Ticks every tick_secs to run lightweight intra-bar tasks, then returns
    when a new bar is confirmed closed.
    """
    tf_map = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400}
    tf_secs = tf_map.get(timeframe, 900)
    now = time.time()
    bar_close_ts = (now // tf_secs + 1) * tf_secs + buffer_secs

    tick_count = 0
    while True:
        now = time.time()
        if now >= bar_close_ts:
            return
        sleep_time = min(tick_secs, bar_close_ts - now)
        if sleep_time > 0:
            time.sleep(sleep_time)
        tick_count += 1

        if not paper and tick_count % reconcile_interval == 0:
            try:
                manager.reconcile_with_exchange()
            except Exception as exc:
                logger.warning(f"[HEARTBEAT] reconcile tick failed: {exc}")


_DATA_UNHEALTHY_SINCE: Optional[float] = None
_DATA_UNHEALTHY_BARS: int = 0

def _check_data_health(df: pd.DataFrame, config) -> bool:
    """
    Returns True if data is healthy enough to process signals.
    Sets global pause flag if data is consistently bad.
    """
    global _DATA_UNHEALTHY_SINCE, _DATA_UNHEALTHY_BARS
    critical_cols = ["atr", "ema_fast", "ema_slow", "close", "high", "low", "vwap"]
    issues = []

    if len(df) < 10:
        issues.append(f"only {len(df)} bars loaded (need >=10)")

    for col in critical_cols:
        if col not in df.columns:
            issues.append(f"missing column '{col}'")
        elif df[col].tail(3).isna().any():
            issues.append(f"NaN in last 3 bars of '{col}'")

    if not df.empty:
        latest_ts = df.index[-1]
        tf_map = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}
        tf_secs = tf_map.get(config.timeframe, 900)
        age_secs = (pd.Timestamp.now(tz="UTC") - latest_ts).total_seconds()
        
        # P2 FIX: Strict age check (1.5x TF)
        if age_secs > tf_secs * 1.5:
            issues.append(f"latest bar is {age_secs:.0f}s old (lagging {age_secs/tf_secs:.1f}x TF)")

    if issues:
        if _DATA_UNHEALTHY_SINCE is None:
            _DATA_UNHEALTHY_SINCE = time.time()
        _DATA_UNHEALTHY_BARS += 1
        logger.warning(f"[DATA-HEALTH] Unhealthy ({_DATA_UNHEALTHY_BARS} bars): {'; '.join(issues)}")
        return False
    else:
        if _DATA_UNHEALTHY_SINCE is not None:
            down_secs = time.time() - _DATA_UNHEALTHY_SINCE
            logger.info(f"[DATA-HEALTH] Data restored after {down_secs:.0f}s / {_DATA_UNHEALTHY_BARS} bars — resuming signals")
        _DATA_UNHEALTHY_SINCE = None
        _DATA_UNHEALTHY_BARS = 0
        return True


# ── Signal processing ─────────────────────────────────────────────────────────

def process_signals(
    df: pd.DataFrame,
    manager: TradeManager,
    config: BotConfig,
    bar_index: int,
    atr_mult: float,
    available_balance: Optional[float] = None,
) -> bool:
    """
    Evaluate signals on the last confirmed bar and open pending orders if
    the score meets the threshold.
    """
    if manager.has_trade or manager.has_pending:
        return  # Already in a trade or pending — do not stack

    # Fetch OBI for live filtering
    obi = None
    if hasattr(manager.exec_engine, 'get_obi'):
        obi = manager.exec_engine.get_obi()

    row      = df.iloc[-2]   # last CLOSED bar
    prev_row = df.iloc[-3]

    signals = get_signals(row, prev_row, config, obi=obi)
    
    # P0 FIX: Log only if there's interest (at least one setup fired)
    max_score = max([s.score for s in signals]) if signals else 0
    if max_score > 0:
        _log_to_dataset(row, obi, config, score=max_score)

    # Pick highest-score signal if both long and short fire simultaneously
    signals = sorted(signals, key=lambda s: s.score, reverse=True)

    for sig in signals:
        if not sig.auto_trade:
            logger.info(f"Signal {sig.side} setup={sig.setup} score={sig.score:.0f} — below threshold ({config.auto_trade_threshold}), skipped")
            continue

        if _is_duplicate_signal(sig.side, sig.retest_level, bar_index):
            logger.info(f"Signal {sig.side} @ {sig.retest_level} skipped — cooldown (same level, <{COOLDOWN_BARS} bars)")
            continue

        tick  = config.tick_size
        is_long = sig.side == "long"
        entry = round_tick(
            sig.retest_level - config.limit_offset_ticks * tick if is_long
            else sig.retest_level + config.limit_offset_ticks * tick,
            tick,
        )

        stop = get_stop_price(
            is_long=is_long,
            entry=entry,
            atr=float(row["atr"]),
            swing_low=float(row["swing_low"]),
            swing_high=float(row["swing_high"]),
            prev_low=float(row["prev_low"]),
            band_dn=float(row["band_dn"]),
            pdl=float(row["pdl"]),
            prev_high=float(row["prev_high"]),
            band_up=float(row["band_up"]),
            pdh=float(row["pdh"]),
            config=config,
            atr_mult=atr_mult,
        )

        qty  = calc_qty(entry, stop, config, available_balance=available_balance)
        tp1, tp2, tp3 = calc_tp_levels(is_long, entry, stop, config)

        if config.adaptive_cancel:
            avg_tr  = float(row["atr"])
            n_bars  = calc_cancel_bars_dyn(float(row["close"]), sig.retest_level, avg_tr, config)
        else:
            n_bars = config.cancel_bars

        expiry = bar_index + n_bars

        logger.info(
            f"Signal {sig.side.upper()} | setup={sig.setup} score={sig.score:.0f} | "
            f"entry={entry} stop={stop} tp1={tp1} tp2={tp2} tp3={tp3} qty={qty} expiry_bar={expiry} obi={sig.obi if hasattr(sig, 'obi') else 'N/A'}"
        )

        manager.open_pending(
            side=sig.side,
            retest_level=sig.retest_level,
            stop=stop,
            tp1=tp1, tp2=tp2, tp3=tp3,
            qty=qty,
            expiry_bar=expiry,
            score=sig.score,
        )
        _remember_signal(sig.side, sig.retest_level, bar_index)
        return True # Placed
    return False # No signal meeting threshold


def _log_to_dataset(row: pd.Series, obi: Optional[float], config: BotConfig, score: float = 0.0):
    """Saves features to CSV for future re-training."""
    try:
        data = {
            "timestamp": row.name,
            "rsi": row.get("rsi", 50),
            "adx": row.get("adx", 0),
            "volume": row.get("volume", 0),
            "vol_median": row.get("vol_median", 0),
            "atr_pct": (row.get("atr", 0) / row["close"] * 100) if row["close"] != 0 else 0,
            "obi": obi if obi is not None else 0.0,
            "score": score,
            "close": row["close"]
        }
        df_new = pd.DataFrame([data])
        header = not os.path.exists(_DATASET_FILE)
        df_new.to_csv(_DATASET_FILE, mode='a', header=header, index=False)
    except Exception as e:
        logger.error(f"Failed to log to dataset: {e}")


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_live(config: BotConfig, paper: bool = False) -> None:
    logger.info(f"Starting ETH Scalper Bot — mode={'DRY-RUN' if paper else 'LIVE'}")

    exchange = make_exchange(config)

    if paper:
        exec_engine = PaperExecutionEngine(config)
    else:
        exec_engine = ExecutionEngine(exchange, config)
        exec_engine.set_leverage(config.leverage)

    manager   = TradeManager(exec_engine, config)
    if config.use_ml_filter:
        load_ml_model(config.model_path)

    if not paper:
        check_clock_drift(exchange)
        manager.load_state()
        manager.reconcile_with_exchange()
        _load_cooldown()  # RISK-03: restore cooldown state across restarts
    atr_mult  = get_atr_mult(config.timeframe, config)
    bar_index = 0
    current_day = ""

    last_heartbeat_ts = float(manager.get_meta("last_heartbeat_ts", "0"))
    heartbeat_interval = 6 * 3600  # 6 hours

    _api_fail_count: int = 0
    _API_FAIL_LIMIT: int = 5       # consecutive loop errors before circuit opens
    
    _consecutive_rejections: int = 0
    _REJECTION_ALERT_LIMIT: int = 40  # ~10 hours on 15m without a single signal meeting threshold

    while True:
        df = None  # guard: prevent stale df from prior iteration on exception
        _bar_processed = False
        try:
            # ── Heartbeat: Periodic status to Telegram ────────────────────────
            if time.time() - last_heartbeat_ts >= heartbeat_interval:
                pos = manager.trade
                status_str = "FLAT"
                if pos:
                    status_str = f"{pos.side.upper()} {pos.remaining_qty:.3f} ETH @ WAP {pos.weighted_avg_price:.2f}"
                
                notify_heartbeat(exec_engine.get_balance(), status_str, db=manager.db)
                if not paper:
                    log_balance(config.data_dir, exec_engine.get_balance())
                    manager.set_meta("last_heartbeat_ts", str(time.time()))
                last_heartbeat_ts = time.time()
            # ── Auto-Heal: Reconcile state with exchange ──────────────────────
            if not paper:
                manager.reconcile_with_exchange()

            # ── P8: Daily reset ───────────────────────────────────────────────
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if today != current_day:
                current_day = today
                if paper:
                    exec_engine.reset_daily_stats()  # type: ignore[union-attr]
                else:
                    log_balance(config.data_dir, exec_engine.get_balance())
                logger.info(f"New trading day: {today}")

            # ── P8: Circuit breaker check ─────────────────────────────────────
            if paper:
                if exec_engine.daily_loss_limit_hit():  # type: ignore[union-attr]
                    logger.warning("Daily loss limit hit — no new signals until next day")
                    manager._cancel_pending()
                    continue
                if exec_engine.daily_trade_limit_hit():  # type: ignore[union-attr]
                    logger.warning("Daily trade limit reached — no new signals until next day")
                    continue

            # ── P8: LIVE circuit breaker (daily loss limit) ───────────────────
            if not paper:
                if not hasattr(exec_engine, '_live_day_str'):
                    exec_engine._live_day_str = ''
                    # Use real balance at startup, not config value (prevents phantom loss)
                    _init_bal = exec_engine.get_balance()
                    exec_engine._live_day_balance = _init_bal if _init_bal > 1.0 else config.init_dep
                today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                if today_str != exec_engine._live_day_str:
                    exec_engine._live_day_str = today_str
                    _cb_bal = exec_engine.get_balance()
                    if _cb_bal > 1.0:
                        exec_engine._live_day_balance = _cb_bal
                _cb_now = exec_engine.get_balance()
                if _cb_now > 1.0:
                    _daily_loss = max(0.0, exec_engine._live_day_balance - _cb_now)
                    _loss_limit = config.init_dep * config.daily_loss_limit_pct / 100
                    if _daily_loss >= _loss_limit:
                        msg = f"LIVE daily loss limit: ${_daily_loss:.2f} >= ${_loss_limit:.2f} -- pausing"
                        logger.warning(msg)
                        notify_critical(msg)
                        manager._cancel_pending()
                        continue

            # ── Fetch data (always — needed for active trade management) ─────
            df = fetch_ohlcv(exchange, config.symbol, config.timeframe, limit=config.ohlcv_limit)

            daily_df = fetch_daily_ohlcv(exchange, config.symbol, limit=5)
            if not daily_df.empty:
                last_daily = daily_df.iloc[-2]  # previous completed day
                pdh = float(last_daily["high"])
                pdl = float(last_daily["low"])
            else:
                pdh = pdl = float("nan")

            df = calc_indicators(df, config, pdh=pdh, pdl=pdl)

            # ── P3: Inject HTF trend ──────────────────────────────────────────
            df = inject_htf_trend(df, exchange, config)

            df = detect_setups_df(df, config)
            df.dropna(subset=["atr", "ema_fast"], inplace=True)

            if len(df) < 3:
                logger.warning("Not enough bars after indicator warmup — skipping")
                continue

            # ── Paper: simulate current bar fills ─────────────────────────────
            if paper:
                exec_engine.simulate_bar(df.iloc[-1])  # type: ignore[union-attr]

            # ── Trade manager update (TP/SL/BE/expiry) — always, even in blackout
            manager.update(df.iloc[-1], bar_index)

            # ── P9: Funding rate blackout — block NEW entries only ─────────────
            if is_in_funding_blackout(config):
                logger.info("Funding rate blackout — active trade monitored, new entries blocked")
                manager.update_expiry_only(bar_index)
                continue

            # ── T5: Data health check — pause signals on bad data ─────────────
            data_ok = _check_data_health(df, config)

            # ── Signal processing (with live balance for position sizing) ─────
            live_bal: Optional[float] = None
            if not paper:
                _raw_bal = exec_engine.get_balance()
                live_bal = _raw_bal if _raw_bal > 1.0 else None  # guard: API failure returns 0
            if data_ok and _api_fail_count < _API_FAIL_LIMIT:
                placed = process_signals(df, manager, config, bar_index, atr_mult, available_balance=live_bal)
                # Only track rejection streak when flat — in-trade bars always return False
                # (process_signals early-exits when has_trade/has_pending) and would
                # produce false OBI/ML filter alerts during normal trade management.
                if not manager.has_trade and not manager.has_pending:
                    if placed:
                        _consecutive_rejections = 0
                    else:
                        _consecutive_rejections += 1

                    if _consecutive_rejections >= _REJECTION_ALERT_LIMIT:
                        msg = f"PIPELINE ALERT: {_consecutive_rejections} flat bars without a signal. Check OBI/ML filters."
                        logger.warning(msg)
                        if _consecutive_rejections % 20 == 0:
                            notify_error(msg)
            elif _api_fail_count >= _API_FAIL_LIMIT:
                logger.warning(f"[CIRCUIT OPEN] Skipping new signals — {_api_fail_count} consecutive errors")
            else:
                msg = f"[DATA-HEALTH] Skipping signal generation — data unhealthy ({_DATA_UNHEALTHY_BARS} bars)"
                logger.warning(msg)
                if _DATA_UNHEALTHY_BARS % 10 == 0: # Notify every 10 bars of issues
                    notify_error(msg)
            _bar_processed = True
            _api_fail_count = 0  # reset on successful bar

            # ── Heartbeat ─────────────────────────────────────────────────────
            if paper:
                summary = exec_engine.summary()  # type: ignore[union-dict]
                logger.info(
                    f"[PAPER] Balance=${summary.get('balance', 0):.2f} | "
                    f"Trades={summary.get('trades', 0)} | WR={summary.get('win_rate', 0)}% | "
                    f"DailyLoss=${exec_engine.daily_loss():.2f}"  # type: ignore[union-attr]
                )
            else:
                bal = live_bal if live_bal is not None else exec_engine.get_balance()
                row = df.iloc[-1]
                signal_row = df.iloc[-2]      # last CLOSED bar for DIAG
                atr_pct = float(signal_row['atr']) / float(signal_row['close']) * 100
                ema_spread_pct = abs(float(signal_row['ema_fast']) - float(signal_row['ema_slow'])) / float(signal_row['close']) * 100
                logger.debug(f"[ACCOUNT] Full Balance=${bal:.2f}")
                logger.info(
                    f"[LIVE] ETH=${float(row['close']):.2f} | ATR%={atr_pct:.3f}% | EMAspread%={ema_spread_pct:.3f}%"
                )
                # Setup diagnostics — show what's firing/blocking each bar
                logger.info(
                    f"[DIAG] sweep_l={bool(signal_row.get('long_sweep',False))} "
                    f"sweep_s={bool(signal_row.get('short_sweep',False))} "
                    f"vmr_l={bool(signal_row.get('long_vmr',False))} "
                    f"vmr_s={bool(signal_row.get('short_vmr',False))} "
                    f"mom_l={bool(signal_row.get('long_mom',False))} "
                    f"mom_s={bool(signal_row.get('short_mom',False))} "
                    f"ride_l={bool(signal_row.get('long_trend_ride',False))} "
                    f"ride_s={bool(signal_row.get('short_trend_ride',False))} "
                    f"vol_ok={bool(signal_row.get('vol_ok',False))} "
                    f"vol={float(signal_row.get('volume',0)):.0f} vol_median={float(signal_row.get('vol_median',0)):.0f} "
                    f"htf_l={bool(signal_row.get('htf_trend_long',True))} "
                    f"htf_s={bool(signal_row.get('htf_trend_short',True))}"
                )


        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
            if paper:
                print("\n── Final Paper Trading Summary ──────────────")
                for k, v in exec_engine.summary().items():  # type: ignore[union-attr]
                    print(f"  {k:<18} {v}")
                print("─────────────────────────────────────────────")
            break
        except Exception as exc:
            _api_fail_count += 1
            msg = f"Loop error ({_api_fail_count}/{_API_FAIL_LIMIT}): {exc}"
            logger.error(msg, exc_info=True)
            notify_critical(msg)
            if _api_fail_count >= _API_FAIL_LIMIT:
                critical_msg = (
                    f"CIRCUIT OPEN: {_api_fail_count} consecutive loop failures. "
                    "New signal entries blocked until recovery. Active trade still monitored."
                )
                logger.critical(critical_msg)
                notify_critical(critical_msg)
                # Block new entries for rest of this iteration — manager won't open_pending
                # because process_signals is skipped via _api_fail_count gate below.
            time.sleep(10)
        finally:
            if _bar_processed:
                bar_index += 1
            # T1: heartbeat sleep — ticks every 5s for intra-bar reconcile
            # falls back to blocking sleep in paper/backtest mode (no reconcile needed)
            _heartbeat_sleep(
                config.timeframe,
                manager=manager,
                exec_engine=exec_engine,
                paper=paper,
            )


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ETH Scalper Bot")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", "--paper", action="store_true", dest="paper", help="Dry-run (paper) mode (simulated fills)")
    mode.add_argument("--live",      action="store_true", help="Live trading mode (requires API keys)")
    parser.add_argument("--yes",       action="store_true", help="Skip live mode confirmation (for systemd/non-interactive)")
    mode.add_argument("--backtest",  action="store_true", help="Run historical backtest and exit")
    parser.add_argument("--days",       type=int,   default=180,   help="Backtest history days")
    parser.add_argument("--timeframe",  type=str,   default=None,  help="Override timeframe (default: from config.py)")
    parser.add_argument("--risk",       type=float, default=None,  help="Override risk per trade %%")
    parser.add_argument("--leverage",   type=int,   default=None)
    parser.add_argument("--deposit",    type=float, default=None)
    parser.add_argument("--threshold",  type=int,   default=None,  help="Override auto-trade score threshold")
    args = parser.parse_args()

    # Build config from config.py defaults; only override if explicitly passed on CLI
    config = BotConfig()
    if args.timeframe is not None:
        config.timeframe = args.timeframe
    if args.risk is not None:
        config.risk_pct = args.risk
    if args.leverage is not None:
        config.leverage = args.leverage
    if args.deposit is not None:
        config.init_dep = args.deposit
    if args.threshold is not None:
        config.auto_trade_threshold = args.threshold

    if args.backtest:
        results = run_backtest(config, days=args.days)
        print("\n── Backtest Results ──────────────────────────────")
        for k, v in results.items():
            print(f"  {k:<18} {v}")
        print("─────────────────────────────────────────────────")
    elif args.paper:
        run_live(config, paper=True)
    elif args.live:
        if not args.yes:
            confirm = input("⚠  LIVE trading mode — this will place REAL orders on HTX. Type YES to continue: ")
            if confirm.strip().upper() != "YES":
                print("Aborted.")
                return
        # Log starting balance
        try:
            from market_data import make_exchange
            from execution_engine import ExecutionEngine
            ex = make_exchange(config)
            eng = ExecutionEngine(ex, config)
            log_balance(config.data_dir, eng.get_balance())
        except Exception:
            pass
        run_live(config, paper=False)


if __name__ == "__main__":
    main()
