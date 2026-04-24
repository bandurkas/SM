"""
trade_manager.py — Active trade lifecycle management.

Mirrors Pine Script post-fill logic:
  - TP1 hit  → move stop to break-even
  - TP2/TP3  → (optional) partial close
  - Stop hit → close position
  - Pending retest order expiry → cancel

The manager holds at most one pending order and one active trade per side.
In practice the Pine script handles one side at a time; we follow the same
convention (new signal cancels previous pending of the opposite side).
"""

import csv
import json
import logging
import math
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from config import BotConfig, cfg as default_cfg
from execution_engine import ExecutionEngine
from risk_engine import round_tick, round_qty as _round_qty
from telegram_notify import notify_trade_open, notify_partial_close, notify_trade_close
import dca_engine
from persistence import StateDB

logger = logging.getLogger(__name__)

STATE_FILE = os.path.join(default_cfg.data_dir, "state.json")
TRADES_CSV = os.path.join(default_cfg.data_dir, "trades.csv")


def _append_trade_csv(row: dict) -> None:
    """Append a closed trade row to TRADES_CSV (creates/migrates header if needed)."""
    try:
        cols = ["timestamp", "side", "entry", "exit", "qty", "pnl", "reason", "is_win"]
        if not os.path.exists(TRADES_CSV):
            with open(TRADES_CSV, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=cols).writeheader()
        else:
            # Migrate header if file exists with old 7-column schema (no is_win)
            with open(TRADES_CSV, "r") as f:
                first_line = f.readline().strip()
            if first_line and "is_win" not in first_line:
                with open(TRADES_CSV, "r") as f:
                    old_content = f.read()
                with open(TRADES_CSV, "w") as f:
                    f.write(",".join(cols) + "\n" + old_content)
                logger.info("trades.csv: migrated header to 8-column schema (added is_win)")
        with open(TRADES_CSV, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=cols).writerow({k: row.get(k, "") for k in cols})
    except Exception as exc:
        logger.warning(f"trades.csv append failed: {exc}")


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class PendingOrder:
    side: str               # "long" | "short"
    order_id: str
    retest_level: float     # limit price
    expiry_bar: int         # bar_index after which the order is cancelled
    score: float
    stop: float = 0.0       # stop-loss price for the trade on fill
    tp1: float = 0.0
    tp2: float = 0.0
    tp3: float = 0.0
    qty: float = 0.0        # order size (ETH)


@dataclass
class Entry:
    price: float
    qty: float
    timestamp: str

@dataclass
class GridPosition:
    side: str               # "long" | "short"
    entries: list[Entry] = field(default_factory=list)
    weighted_avg_price: float = 0.0
    stop: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    tp3: float = 0.0
    total_qty: float = 0.0
    remaining_qty: float = 0.0
    next_dca_level: Optional[float] = None
    level_index: int = 0
    stop_order_id: Optional[str] = None
    be_moved: bool = False
    tp1_hit: bool = False
    tp2_hit: bool = False
    fill_bar: int = 0
    last_dca_bar: int = -1  # Prevent multiple DCA fills on the same bar
    high_watermark: float = 0.0
    low_watermark: float = 0.0

    def __post_init__(self):
        if self.total_qty > 0 and self.remaining_qty == 0.0:
            self.remaining_qty = self.total_qty
        if self.weighted_avg_price > 0:
            if self.high_watermark == 0.0:
                self.high_watermark = self.weighted_avg_price
            if self.low_watermark == 0.0:
                self.low_watermark = self.weighted_avg_price

    def add_entry(self, price: float, qty: float, timestamp: str):
        self.entries.append(Entry(price, qty, timestamp))
        self.remaining_qty += qty
        self._recalculate()

    def _recalculate(self):
        self.total_qty = sum(e.qty for e in self.entries)
        # Update remaining_qty if drift detected
        if not self.entries: return

        weighted_sum = sum(e.price * e.qty for e in self.entries)
        self.weighted_avg_price = weighted_sum / self.total_qty
        
        # Watermarks follow the weighted average at the moment of entry
        if len(self.entries) == 1:
            self.high_watermark = self.weighted_avg_price
            self.low_watermark = self.weighted_avg_price



# ── Trade Manager ─────────────────────────────────────────────────────────────

class TradeManager:
    """
    Manages pending limit orders and active trades.
    Call `update(row, bar_index)` once per confirmed bar.
    """

    def __init__(self, execution: ExecutionEngine, config: BotConfig = default_cfg):
        self.exec = execution
        self.cfg = config
        self.exec_engine = execution

        # Persistence
        db_dir = os.getenv("ETHBOT_DATA_DIR", config.data_dir)
        self.db_path = os.path.join(db_dir, "bot_state.db")
        self.db = StateDB(self.db_path)

        self.pending: Optional[PendingOrder] = None
        self.trade: Optional[GridPosition] = None
        # Tracking counters
        self._pending_placed = 0
        self._pending_filled = 0
        self._pending_expired = 0

        # BUG-05: ghost-order cleanup runs only once at startup
        self._startup_reconcile_done: bool = False

    def get_meta(self, key: str, default: str = "") -> str:
        return self.db.get_meta(key, default)

    def set_meta(self, key: str, value: str):
        self.db.set_meta(key, value)

    def was_notified(self, event_id: str) -> bool:
        return self.db.was_notified(event_id)

    def mark_notified(self, event_id: str):
        self.db.mark_notified(event_id)

    def save_state(self):
        self._save_state()

    # ── Public: place a new pending limit order ───────────────────────────────

    def open_pending(
        self,
        side: str,
        retest_level: float,
        stop: float,
        tp1: float,
        tp2: float,
        tp3: float,
        qty: float,
        expiry_bar: int,
        score: float,
    ) -> None:
        # Cancel any existing pending order first
        self._cancel_pending()
        # Cancel any active trade on opposite side (one direction at a time).
        # exit_price=0 → _close_trade uses WAP as fallback; callers that have
        # current price should pass it, but open_pending has no bar row here.
        # Use 0 so the trade CSV notes it as a forced reversal without a live price.
        if self.trade and self.trade.side != side:
            self._close_trade("new_signal_opposite_side")

        c_side = "buy" if side == "long" else "sell"
        tick = self.cfg.tick_size
        limit_price = round_tick(
            retest_level - self.cfg.limit_offset_ticks * tick if side == "long"
            else retest_level + self.cfg.limit_offset_ticks * tick,
            tick,
        )

        order = self.exec.place_limit_order(c_side, qty, limit_price)

        self.pending = PendingOrder(
            side=side,
            order_id=order["id"],
            retest_level=limit_price,
            expiry_bar=expiry_bar,
            score=score,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
            tp3=tp3,
            qty=qty,
        )
        self._pending_placed += 1
        logger.info(f"Pending {side} @ {limit_price} | expiry bar {expiry_bar} | placed={self._pending_placed} filled={self._pending_filled} expired={self._pending_expired}")
        self._save_state()

    # ── Public: bar update ────────────────────────────────────────────────────

    def update(self, row: pd.Series, bar_index: int) -> None:
        if self.pending:
            self._check_pending_fill(row, bar_index)

        if self.trade:
            # T1: check for DCA triggers or Range Scalps before standard TP/SL
            self._check_dca_levels(row, bar_index)
            if self.trade and self.cfg.use_range_scalper:
                self._check_range_scalps(row, bar_index)
            
            if self.trade:
                self._check_trade(row, bar_index)

        self._save_state()

    # ── Pending order handling ────────────────────────────────────────────────

    def _check_pending_fill(self, row: pd.Series, bar_index: int) -> None:
        p = self.pending
        assert p is not None

        filled      = False
        filled_qty  = p.qty       # default: full fill at limit price
        fill_price  = p.retest_level

        if getattr(self.exec, "is_paper", False):
            # Paper mode: simulate fill via chart crossover
            if p.side == "long" and row["low"] <= p.retest_level:
                filled = True
            elif p.side == "short" and row["high"] >= p.retest_level:
                filled = True
        else:
            # Live mode: poll specific order_id for status (avoids false-positive from side-match)
            order_info = None
            if hasattr(self.exec, "fetch_order"):
                order_info = self.exec.fetch_order(p.order_id)

            if order_info is not None:
                status = order_info["status"]
                if status == "filled":
                    filled      = True
                    filled_qty  = order_info["filled_qty"] or p.qty
                    fill_price  = order_info["avg_fill_price"] or p.retest_level
                elif status == "partial" and order_info["filled_qty"] > 0:
                    # Partial fill: adopt filled portion, cancel unfilled remainder
                    filled      = True
                    filled_qty  = order_info["filled_qty"]
                    fill_price  = order_info["avg_fill_price"] or p.retest_level
                    logger.warning(
                        f"Partial fill {p.order_id}: {filled_qty:.4f}/{p.qty:.4f} ETH "
                        f"@ {fill_price} — cancelling remainder"
                    )
                    try:
                        self.exec.cancel_order(p.order_id)
                    except Exception as ce:
                        logger.warning(f"Cancel partial remainder failed: {ce}")
                elif status == "cancelled":
                    logger.warning(f"Pending order {p.order_id} was cancelled on exchange — clearing")
                    self.pending = None
                    self._save_state()
                    return
                # else status == "open": order still live, not filled yet
            else:
                # fetch_order failed (API error) — fall back to position check
                try:
                    pos = self.exec.get_position()
                    live_contracts = float(pos.get("contracts", 0)) if pos else 0.0
                    live_side      = (pos or {}).get("side")
                    if live_contracts > 0 and live_side == p.side:
                        filled     = True
                        fill_price = float(pos.get("entryPrice") or p.retest_level)
                        filled_qty = min(round(live_contracts * 0.01, 8), p.qty)
                    else:
                        open_orders = self.exec.get_open_orders()
                        open_ids = {o.get("id") for o in open_orders}
                        if p.order_id not in open_ids:
                            logger.warning(
                                f"Pending order {p.order_id} vanished (fetch_order unavailable, "
                                f"no position match) — clearing pending"
                            )
                            self.pending = None
                            self._save_state()
                            return
                except Exception as exc:
                    logger.error(f"Failed to check pending fill state on exchange: {exc}")
                    return

        if filled:
            self._pending_filled += 1
            fill_rate = self._pending_filled / self._pending_placed * 100 if self._pending_placed else 0
            logger.info(
                f"Pending {p.side} filled @ {fill_price:.2f} qty={filled_qty:.4f} | "
                f"fill_rate={fill_rate:.0f}% ({self._pending_filled}/{self._pending_placed})"
            )

            now_str = datetime.now(timezone.utc).isoformat()
            self.trade = GridPosition(
                side=p.side,
                stop=p.stop,
                tp1=p.tp1,
                tp2=p.tp2,
                tp3=p.tp3,
                fill_bar=bar_index,
            )
            self.trade.add_entry(fill_price, filled_qty, now_str)
            
            # Setup first DCA level if enabled
            if self.cfg.use_dca:
                atr = float(row.get("atr", 0))
                if atr > 0:
                    self.trade.next_dca_level = dca_engine.calculate_next_dca_level(
                        p.side, p.retest_level, atr, self.trade.level_index, self.cfg
                    )
                    logger.info(f"DCA L1 set at {self.trade.next_dca_level}")
            # Prevent duplicate notification
            event_key = f"fill_{p.order_id}"
            already_done = self.was_notified(event_key)

            # Place software stop (exchange stop if possible)
            stop_side = "sell" if p.side == "long" else "buy"
            try:
                stop_order = self.exec.place_stop_market_order(stop_side, p.qty, p.stop)
                if stop_order:
                    self.trade.stop_order_id = stop_order["id"]
                else:
                    raise Exception("Exchange returned empty stop order")
                self._save_state()
                
                if not already_done:
                    notify_trade_open(self.trade.side, self.trade.weighted_avg_price, self.trade.stop, self.trade.tp3, self.trade.total_qty, db=self.db)
                    self.mark_notified(event_key)
            except Exception as exc:
                logger.error(f"CRITICAL: Stop placement failed after fill: {exc} — EMERGENCY CLOSE to prevent naked position")
                # Don't set pending = None yet, _close_trade will handle it
                self._close_trade("emergency_stop_placement_failed", p.retest_level)
                self.pending = None
                return
            self.pending = None
            self._save_state()
            return

        # Check expiry
        if bar_index > p.expiry_bar:
            self._pending_expired += 1
            fill_rate = self._pending_filled / self._pending_placed * 100 if self._pending_placed else 0
            logger.info(f"Pending {p.side} expired at bar {bar_index} | fill_rate={fill_rate:.0f}% ({self._pending_filled}/{self._pending_placed})")
            self._cancel_pending()

    def _check_dca_levels(self, row: pd.Series, bar_index: int) -> None:
        """Check if price reached the next DCA level to average down/up."""
        t = self.trade
        if not t or not t.next_dca_level or t.level_index >= self.cfg.dca_max_levels - 1:
            return
        
        # P8: Anti-whipsaw — prevent multiple DCA triggers on the same bar
        if bar_index <= t.last_dca_bar:
            return

        high, low = row["high"], row["low"]
        current_price = row["close"]
        triggered = (t.side == "long" and low <= t.next_dca_level) or (t.side == "short" and high >= t.next_dca_level)

        if triggered:
            t.last_dca_bar = bar_index
            last_entry = t.entries[-1]
            next_qty = dca_engine.calculate_next_dca_qty(last_entry.qty, self.cfg)
            
            # 1. Position Size Safety Check (Exposure)
            est_new_qty = t.total_qty + next_qty
            est_exposure = est_new_qty * current_price
            if est_exposure > self.cfg.max_total_exposure_usd:
                logger.warning(f"DCA aborted: Total exposure ${est_exposure:.2f} exceeds limit ${self.cfg.max_total_exposure_usd:.2f}")
                return

            logger.info(f"DCA Level {t.level_index + 1} triggered! Qty={next_qty} Price={current_price}")
            logger.info(f"DCA L{t.level_index+1} triggered @ {t.next_dca_level} | adding {next_qty} ETH")
            
            # Place market order for DCA entry
            side = "buy" if t.side == "long" else "sell"
            try:
                self.exec.place_market_order(side, next_qty)
                now_str = datetime.now(timezone.utc).isoformat()
                t.add_entry(t.next_dca_level, next_qty, now_str)  # add_entry already increments remaining_qty
                t.level_index += 1
                
                # Recalculate next DCA level
                atr = float(row.get("atr", 0))
                if t.level_index < self.cfg.dca_max_levels - 1 and atr > 0:
                    t.next_dca_level = dca_engine.calculate_next_dca_level(
                        t.side, t.next_dca_level, atr, t.level_index, self.cfg
                    )
                else:
                    t.next_dca_level = None
                
                # Reanchor stop below the new DCA level so stop is never above our fill.
                # New stop = DCA fill price ± (atr * atr_mult) to keep proper distance.
                atr_mult = self.cfg.atr_mult_15m
                from risk_engine import round_tick as _rt
                if t.side == "long":
                    new_stop = _rt(t.next_dca_level - atr * atr_mult if t.next_dca_level else t.weighted_avg_price - atr * atr_mult, self.cfg.tick_size)
                    new_stop = min(new_stop, t.stop)  # only move further down, never up
                else:
                    new_stop = _rt(t.next_dca_level + atr * atr_mult if t.next_dca_level else t.weighted_avg_price + atr * atr_mult, self.cfg.tick_size)
                    new_stop = max(new_stop, t.stop)  # only move further up, never down
                t.stop = new_stop

                # Move stop order to new price and total quantity
                self._update_trade_stop_order()

                # Recalculate TPs relative to new weighted average
                self._recalculate_tp_levels(atr)
                
                logger.info(f"DCA fill complete. New WAP: {t.weighted_avg_price:.2f} | Remaining Qty: {t.remaining_qty}")
                notify_trade_open(t.side, t.weighted_avg_price, t.stop, t.tp3, t.total_qty, db=self.db)
                
            except Exception as exc:
                logger.error(f"DCA order placement failed: {exc}")

    def _check_range_scalps(self, row: pd.Series, bar_index: int) -> None:
        """Take partial profits (scalp) between DCA levels to lower WAP."""
        t = self.trade
        if not t or t.total_qty < self.cfg.qty_step * 2:
            return

        high, low = row["high"], row["low"]
        atr = float(row.get("atr", 0))
        if atr <= 0: return

        # Target exit for last entry (scalp)
        last_entry = t.entries[-1]
        exit_price = dca_engine.calculate_range_scalp_exit(t.side, last_entry.price, atr, self.cfg)
        
        triggered = (t.side == "long" and high >= exit_price) or (t.side == "short" and low <= exit_price)
        
        if triggered and len(t.entries) > 1:
            # P1: Fee awareness — ensure scalp profit covers fees + buffer
            # Est fees: (entry + exit) * taker_fee_pct. Target profit must be > 3x fees.
            fee_est = (last_entry.price + exit_price) * (self.cfg.taker_fee_pct / 100.0)
            gross_profit = abs(exit_price - last_entry.price)
            if gross_profit < fee_est * 3:
                logger.debug(f"Range Scalp skipped: gross_profit {gross_profit:.2f} < 3x fees ({fee_est*3:.2f})")
                return

            # Scalp size: portion of the last DCA entry
            scalp_qty = _round_qty(last_entry.qty * self.cfg.rs_min_size_pct, self.cfg.qty_step)
            if scalp_qty > t.remaining_qty:
                scalp_qty = t.remaining_qty
            
            if scalp_qty >= self.cfg.qty_step:
                logger.info(f"Range Scalp triggered @ {exit_price} | closing {scalp_qty} ETH")
                side = "sell" if t.side == "long" else "buy"
                try:
                    self.exec.place_market_order(side, scalp_qty, reduce_only=True)
                    t.remaining_qty -= scalp_qty
                    
                    # Log the scalp as a partial close
                    notify_partial_close(t.side, "Range Scalp", exit_price, scalp_qty, t.weighted_avg_price)
                    
                    # Update stop order qty
                    self._update_trade_stop_order()
                    
                    if t.remaining_qty < self.cfg.qty_step:
                        self._close_trade("full_close_after_scalps", exit_price)
                        
                except Exception as exc:
                    logger.error(f"Range scalp execution failed: {exc}")

    def _update_trade_stop_order(self):
        """
        Replace the exchange stop order with updated quantity/price.
        P0 Fix: Try-Replace-Then-Cancel to avoid naked window.
        """
        t = self.trade
        if not t: return
        
        old_stop_id = t.stop_order_id
        side = "sell" if t.side == "long" else "buy"
        
        try:
            # 1. Place new stop first
            new_stop_order = self.exec_engine.place_stop_market_order(side, t.remaining_qty, t.stop)
            if new_stop_order and new_stop_order.get("id"):
                t.stop_order_id = new_stop_order["id"]
                self._save_state() # SAVE IMMEDIATELY after getting new ID
                
                # 2. Only cancel old stop if new one succeeded
                if old_stop_id:
                    for i in range(self.cfg.max_api_retries):
                        if self.exec_engine.cancel_order(old_stop_id):
                            break
                        else:
                            logger.warning(f"Retry {i+1}/{self.cfg.max_api_retries} cancelling old stop {old_stop_id}")
                            time.sleep(1)
            else:
                logger.error("CRITICAL: Exchange failed to place updated stop order (no ID returned)")
        except Exception as exc:
            logger.error(f"CRITICAL: Failed to update stop order: {exc} — trade may be NAKED!")

    def _recalculate_tp_levels(self, atr: float):
        """Shift TPs to maintain original R-multiples from the new WAP."""
        t = self.trade
        if not t: return
        risk = abs(t.weighted_avg_price - t.stop)
        if risk <= 0: return
        
        t.tp1 = round_tick(t.weighted_avg_price + risk * self.cfg.r_tp1 if t.side == "long" else t.weighted_avg_price - risk * self.cfg.r_tp1, self.cfg.tick_size)
        t.tp2 = round_tick(t.weighted_avg_price + risk * self.cfg.r_tp2 if t.side == "long" else t.weighted_avg_price - risk * self.cfg.r_tp2, self.cfg.tick_size)
        t.tp3 = round_tick(t.weighted_avg_price + risk * self.cfg.r_tp3 if t.side == "long" else t.weighted_avg_price - risk * self.cfg.r_tp3, self.cfg.tick_size)

    # ── Active trade handling ─────────────────────────────────────────────────

    def _check_trade(self, row: pd.Series, bar_index: int) -> None:
        t = self.trade
        assert t is not None
        c = self.cfg

        high, low = row["high"], row["low"]
        tick = c.tick_size

        # Update absolute extremes since entry (Chandelier Exit)
        t.high_watermark = max(t.high_watermark, float(high)) if t.high_watermark > 0 else float(high)
        t.low_watermark = min(t.low_watermark, float(low)) if t.low_watermark > 0 else float(low)

        # Stop hit? Close remaining position.
        if t.side == "long" and low <= t.stop:
            logger.info(f"LONG stopped out @ {t.stop}")
            self._close_trade("stop_hit", t.stop)
            return
        if t.side == "short" and high >= t.stop:
            logger.info(f"SHORT stopped out @ {t.stop}")
            self._close_trade("stop_hit", t.stop)
            return

        # TP3 hit → close remaining position
        if t.side == "long" and high >= t.tp3:
            logger.info(f"LONG TP3 hit @ {t.tp3}")
            self._close_trade("tp3", t.tp3)
            return
        if t.side == "short" and low <= t.tp3:
            logger.info(f"SHORT TP3 hit @ {t.tp3}")
            self._close_trade("tp3", t.tp3)
            return

        # P2: TP1 hit → partial close (25%) + move stop to BE
        if not t.tp1_hit and t.tp1 > 0:
            tp1_hit = (t.side == "long" and high >= t.tp1) or (t.side == "short" and low <= t.tp1)
            if tp1_hit:
                t.tp1_hit = True
                t.be_moved = True

                # Partial close: 25% of original total qty (capped at remaining to prevent over-close)
                close_qty = min(
                    _round_qty(t.total_qty * c.partial_close_tp1_pct, c.qty_step),
                    t.remaining_qty,
                )
                close_side = "sell" if t.side == "long" else "buy"
                if close_qty >= c.qty_step:
                    self.exec.place_market_order(close_side, close_qty, reduce_only=True)
                    new_remaining = _round_qty(t.remaining_qty - close_qty, c.qty_step)
                    if new_remaining < c.qty_step:
                        # Full position closed at TP1
                        self.trade = None
                        self._save_state()
                        notify_partial_close(t.side, "TP1", t.tp1, close_qty, t.weighted_avg_price)
                        return
                    t.remaining_qty = new_remaining
                    logger.info(f"TP1 partial close {close_qty} @ ~{t.tp1} | remaining={t.remaining_qty}")
                    notify_partial_close(t.side, "TP1", t.tp1, close_qty, t.weighted_avg_price)

                # Move stop to BE on remaining position
                old_stop = t.stop
                improves_stop = (t.side == "long" and t.weighted_avg_price > t.stop) or (t.side == "short" and t.weighted_avg_price < t.stop)
                if improves_stop:
                    try:
                        if t.stop_order_id:
                            self.exec.cancel_order(t.stop_order_id)
                            t.stop_order_id = None
                        t.stop = t.weighted_avg_price
                        new_stop_order = self.exec.place_stop_market_order(close_side, t.remaining_qty, t.weighted_avg_price)
                        t.stop_order_id = new_stop_order["id"] if new_stop_order else None
                        logger.info(f"Stop moved {old_stop} → {t.weighted_avg_price} (BE)")
                    except Exception as exc:
                        logger.error(f"BE stop replace failed: {exc} — retaining old stop @ {old_stop}")
                        t.stop = old_stop

        # P2: TP2 hit → partial close (50% of original total qty)
        if t.tp1_hit and not t.tp2_hit and t.tp2 > 0:
            tp2_hit = (t.side == "long" and high >= t.tp2) or (t.side == "short" and low <= t.tp2)
            if tp2_hit:
                t.tp2_hit = True
                if c.partial_close_tp2_pct <= 0:
                    logger.info(f"TP2 level reached @ ~{t.tp2} | tp2_pct=0, runner continues to TP3")
                    return
                close_qty = _round_qty(t.total_qty * c.partial_close_tp2_pct, c.qty_step)
                close_side = "sell" if t.side == "long" else "buy"
                actual_close = min(close_qty, t.remaining_qty)
                if actual_close >= c.qty_step:
                    self.exec.place_market_order(close_side, actual_close, reduce_only=True)
                    new_remaining = _round_qty(t.remaining_qty - actual_close, c.qty_step)
                    notify_partial_close(t.side, "TP2", t.tp2, actual_close, t.weighted_avg_price)
                    if new_remaining < c.qty_step:
                        logger.info(f"TP2 full close {actual_close} @ ~{t.tp2}")
                        self.trade = None
                        self._save_state()
                        return
                    t.remaining_qty = new_remaining
                    logger.info(f"TP2 partial close {actual_close} @ ~{t.tp2} | remaining={t.remaining_qty}")

        # P7: Dynamic ATR Trailing Stop (Chandelier Exit)
        if getattr(c, "use_dynamic_atr_trailing", False):
            atr_val = float(row.get("atr", 0))
            if atr_val > 0:
                trail_dist  = atr_val * getattr(c, "trail_atr_mult", 1.0)
                # Rate limit: only replace stop order if improvement >= 0.5 ATR.
                # Prevents API spam when price inches forward tick-by-tick.
                min_move    = atr_val * getattr(c, "trail_min_atr_move", 0.5)
                if t.side == "long":
                    new_trail = round_tick(t.high_watermark - trail_dist, tick)
                    if new_trail >= t.stop + min_move:
                        old_stop = t.stop
                        try:
                            if t.stop_order_id:
                                self.exec.cancel_order(t.stop_order_id)
                                t.stop_order_id = None
                            t.stop = new_trail
                            new_stop_order = self.exec.place_stop_market_order("sell", t.remaining_qty, t.stop)
                            t.stop_order_id = new_stop_order["id"] if new_stop_order else None
                            logger.info(f"Dynamic Trail raised {old_stop} → {t.stop} (from {t.high_watermark})")
                        except Exception as exc:
                            logger.error(f"Trail stop replace (long) failed: {exc} — retaining {old_stop}")
                            t.stop = old_stop
                else:
                    new_trail = round_tick(t.low_watermark + trail_dist, tick)
                    if new_trail <= t.stop - min_move:
                        old_stop = t.stop
                        try:
                            if t.stop_order_id:
                                self.exec.cancel_order(t.stop_order_id)
                                t.stop_order_id = None
                            t.stop = new_trail
                            new_stop_order = self.exec.place_stop_market_order("buy", t.remaining_qty, t.stop)
                            t.stop_order_id = new_stop_order["id"] if new_stop_order else None
                            logger.info(f"Dynamic Trail lowered {old_stop} → {t.stop} (from {t.low_watermark})")
                        except Exception as exc:
                            logger.error(f"Trail stop replace (short) failed: {exc} — retaining {old_stop}")
                            t.stop = old_stop

    # ── State persistence (crash/restart recovery) ───────────────────────────

    def _save_state(self) -> None:
        """Transactionally save state to SQLite."""
        try:
            data = {
                "schema_version": 2,
                "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "trade": asdict(self.trade) if self.trade else None,
                "pending": asdict(self.pending) if self.pending else None,
                "counters": {
                    "placed": self._pending_placed,
                    "filled": self._pending_filled,
                    "expired": self._pending_expired,
                },
            }
            self.db.save_state(data)
        except Exception as e:
            logger.error(f"save_state (SQLite) failed: {e}")

    def load_state(self) -> None:
        """Load state from SQLite, with migration from state.json if needed."""
        try:
            # 1. Try loading from SQLite
            data = self.db.load_state()
            
            # 2. Migration from state.json if SQLite is empty
            if not data and os.path.exists(STATE_FILE):
                logger.info("Migrating state from state.json to SQLite...")
                try:
                    with open(STATE_FILE) as f:
                        data = json.load(f)
                    self.db.save_state(data)
                    os.replace(STATE_FILE, STATE_FILE + ".migrated")
                except Exception as me:
                    logger.error(f"Migration failed: {me}")
            
            if not data:
                logger.info("No prior state found — starting fresh")
                return

            if data.get("trade"):
                t_data = data["trade"]
                entries_data = t_data.pop("entries", [])
                t = GridPosition(**t_data)
                for e_dict in entries_data:
                    t.entries.append(Entry(**e_dict))
                
                if self._validate_trade(t):
                    self.trade = t
                else:
                    logger.error(f"Loaded trade failed validation — ignoring")
            if data.get("pending"):
                self.pending = PendingOrder(**data["pending"])
            c = data.get("counters", {}) or {}
            self._pending_placed = int(c.get("placed", 0))
            self._pending_filled = int(c.get("filled", 0))
            self._pending_expired = int(c.get("expired", 0))
            logger.info(
                f"State loaded: trade={'yes' if self.trade else 'no'} "
                f"pending={'yes' if self.pending else 'no'} "
                f"counters=placed={self._pending_placed} filled={self._pending_filled} expired={self._pending_expired}"
            )
        except Exception as e:
            logger.error(f"load_state failed ({e}); starting with empty state")
            self.trade = None
            self.pending = None

    def reconcile_with_exchange(self) -> None:
        """Verify bot state matches live exchange each bar. Fix drift."""
        try:
            try:
                pos = self.exec.get_position()
            except Exception as api_exc:
                logger.error(f"reconcile: get_position API failure ({api_exc}) — skipping reconcile to preserve state")
                return
            live_contracts = float(pos.get("contracts", 0)) if pos else 0.0
            live_side = (pos or {}).get("side")

            if self.trade and live_contracts == 0:
                logger.warning("State has trade but exchange is flat — clearing trade state")
                self.trade = None
                self._save_state()

            elif live_contracts > 0:
                actual_qty = live_contracts * 0.01
                if actual_qty < self.cfg.qty_dust_threshold:
                    logger.info(f"Dust detected ({actual_qty} ETH < {self.cfg.qty_dust_threshold}). Treating as FLAT.")
                    if self.trade:
                        self.trade = None
                        self._save_state()
                    return # Skip further processing for dust

                if not self.trade:
                    if self.pending and self.pending.side == live_side:
                        # Pending filled between bar cycles — auto-adopt with full TP/SL context
                        logger.info(f"Pending {self.pending.order_id} filled on exchange. Auto-adopting!")
                        self._do_adopt_from_pending(pos, live_contracts)
                    else:
                        # BUG-03: Orphan position with no matching pending — adopt with safety stop
                        self._adopt_orphan(pos, live_contracts, live_side)
                elif self.trade and live_side != self.trade.side:
                    # Side mismatch: state says long but exchange has short (or vice versa)
                    logger.warning(
                        f"State/exchange side mismatch (state={self.trade.side}, live={live_side}) — adopting live"
                    )
                    self.trade = None
                    self._save_state()
                    self._adopt_orphan(pos, live_contracts, live_side)

            # B. GHOST STOP CLEANUP (Double-Stop Protection)
            try:
                all_open = self.exec_engine.get_open_orders()
                live_stops = [o for o in all_open if "stopPrice" in o or o.get("type") == "stop"]
                
                expected_sid = self.trade.stop_order_id if self.trade else None
                
                for s in live_stops:
                    sid = s["id"]
                    if sid != expected_sid:
                        logger.warning(f"Ghost Stop Order detected ({sid}). Cancelling for safety.")
                        self.exec_engine.cancel_order(sid)
            except Exception as e:
                logger.error(f"Reconciliation error (stop cleanup): {e}")

            # C. QUANTITY DRIFT RECONCILIATION
            t = self.trade
            if t and live_contracts > 0:
                expected_qty = t.total_qty
                # live_contracts is in 0.01 units
                actual_qty = live_contracts * 0.01
                if abs(expected_qty - actual_qty) > 0.001:
                    logger.warning(f"Quantity drift detected: state={expected_qty}, live={actual_qty}. Syncing.")
                    t.remaining_qty = actual_qty
                    # We don't recalculate total_qty (as it represents entry history), 
                    # but we update remaining_qty for future closes.
                    self._save_state()
            if not self._startup_reconcile_done:
                self._startup_reconcile_done = True
                try:
                    open_orders = self.exec.get_open_orders()
                    tracked_id = self.pending.order_id if self.pending else None
                    trade_stop_id = self.trade.stop_order_id if self.trade else None
                    for o in open_orders:
                        oid = o.get("id")
                        if oid != tracked_id and oid != trade_stop_id:
                            logger.warning(f"Startup: cancelling untracked open order {oid}")
                            self.exec.cancel_order(oid)
                    # BUG-04: if tracked pending is gone from exchange, check position before clearing
                    if self.pending:
                        remaining_ids = {o.get("id") for o in self.exec.get_open_orders()}
                        if self.pending.order_id not in remaining_ids:
                            pos2 = self.exec.get_position()
                            lc2 = float(pos2.get("contracts", 0)) if pos2 else 0.0
                            ls2 = (pos2 or {}).get("side")
                            if lc2 > 0 and ls2 == self.pending.side and not self.trade:
                                logger.info("Pending filled (order vanished) — adopting from position")
                                self._do_adopt_from_pending(pos2, lc2)
                            else:
                                logger.warning(f"Pending {self.pending.order_id} gone from exchange — clearing")
                                self.pending = None
                                self._save_state()
                except Exception as exc:
                    logger.warning(f"Startup reconcile cleanup failed: {exc}")

        except Exception as e:
            logger.error(f"reconcile_with_exchange failed: {e}")

    def _do_adopt_from_pending(self, pos: dict, live_contracts: float) -> None:
        """Adopt a position that originated from our pending limit order."""
        p = self.pending
        if not p: return
        
        # Prevent duplicate notification
        event_key = f"adopt_{p.order_id}"
        already_done = self.was_notified(event_key)
        
        pending_stop = p.stop
        if pending_stop <= 0:
            logger.warning(f"_do_adopt_from_pending: _stop missing/zero for {p.side} — fallback to _adopt_orphan")
            self.pending = None
            self._save_state()
            self._adopt_orphan(pos, live_contracts, pos.get("side"))
            return
        entry = float(pos.get("entryPrice", p.retest_level) or p.retest_level)
        qty = _round_qty(live_contracts * 0.01, self.cfg.qty_step)
        # Prevent duplicate notification
        event_key = f"adopt_{p.order_id}"
        already_done = self.was_notified(event_key)

        self.trade = GridPosition(
            side=p.side,
            stop=pending_stop,
            tp1=p.tp1,
            tp2=p.tp2,
            tp3=p.tp3,
            fill_bar=0,
        )
        now_str = datetime.now(timezone.utc).isoformat()
        self.trade.add_entry(entry, qty, now_str)
        
        stop_side = "sell" if self.trade.side == "long" else "buy"
        stop_order = self.exec.place_stop_market_order(stop_side, self.trade.remaining_qty, self.trade.stop)
        if stop_order:
            self.trade.stop_order_id = stop_order["id"]
        self.pending = None
        self._save_state()
        
        if not already_done:
            notify_trade_open(self.trade.side, self.trade.weighted_avg_price, self.trade.stop, self.trade.tp3, self.trade.total_qty, db=self.db)
            self.mark_notified(event_key)

    def _adopt_orphan(self, pos: dict, live_contracts: float, live_side: Optional[str]) -> None:
        """BUG-03: Adopt an orphan position (no pending context) with a safety ATR stop."""
        if not live_side or live_contracts <= 0:
            return
        entry = float(pos.get("entryPrice", 0.0) or 0.0)
        if entry <= 0:
            logger.warning(f"ORPHAN: {live_side} {live_contracts} contracts but entryPrice unknown — close manually")
            return
        # Safety stop: 2% from entry (conservative; ATR trailing takes over after adoption)
        safety_pct = 0.02
        if live_side == "long":
            safety_stop = round(entry * (1 - safety_pct), 2)
            tp3 = round(entry * 1.06, 2)   # 3× 2% risk = 6% target as fallback
        else:
            safety_stop = round(entry * (1 + safety_pct), 2)
            tp3 = round(entry * 0.94, 2)
        qty = _round_qty(live_contracts * 0.01, self.cfg.qty_step)
        now_str = datetime.now(timezone.utc).isoformat()
        self.trade = GridPosition(
            side=live_side,
            stop=safety_stop,
            tp1=0.0,
            tp2=0.0,
            tp3=tp3,
            fill_bar=0,
        )
        self.trade.add_entry(entry, qty, now_str)
        
        stop_side = "sell" if live_side == "long" else "buy"
        stop_order = self.exec.place_stop_market_order(stop_side, qty, safety_stop)
        if stop_order:
            self.trade.stop_order_id = stop_order["id"]
        self._save_state()
        logger.warning(
            f"ORPHAN auto-adopted: {live_side} {live_contracts} contracts @ {entry} | "
            f"safety stop={safety_stop} (2%) | tp3={tp3} | ATR trailing will take over"
        )

    @staticmethod
    def _validate_trade(t: "GridPosition") -> bool:
        return (t.weighted_avg_price > 0 and t.total_qty > 0 and t.remaining_qty > 0
                and t.stop > 0 and t.side in ("long", "short"))

        # ── Close trade ───────────────────────────────────────────────────────────

    def _close_trade(self, reason: str, exit_price: float = 0.0) -> None:
        t = self.trade
        if t is None:
            return
        # Cancel existing stop order if it was placed on exchange
        if t.stop_order_id:
            try:
                self.exec.cancel_order(t.stop_order_id)
            except Exception as exc:
                logger.warning(f"_close_trade: cancel stop {t.stop_order_id} failed: {exc}")
        # Send market close for whatever remains — but skip if exchange already flat
        # (handles case where exchange TRG stop fired intra-bar before _close_trade runs)
        close_side = "sell" if t.side == "long" else "buy"
        close_qty = t.remaining_qty if t.remaining_qty > 0 else t.total_qty
        skip_market_close = False
        try:
            live_pos = self.exec.get_position()
            if float((live_pos or {}).get("contracts", 0)) == 0:
                logger.info(f"_close_trade({reason}): already flat — TRG or simulate_bar fired intra-bar, skipping market order")
                skip_market_close = True
        except Exception as exc:
            logger.warning(f"_close_trade position check failed ({exc}) — proceeding with market close")
        if not skip_market_close:
            try:
                self.exec.place_market_order(close_side, close_qty, reduce_only=True)
            except Exception as exc:
                logger.error(f"Market close failed: {exc}")
        logger.info(f"Trade closed — reason: {reason} | side: {t.side} | entry: {t.weighted_avg_price:.2f} | stop: {t.stop}")
        exit_p = exit_price or t.weighted_avg_price
        pnl = (exit_p - t.weighted_avg_price) * close_qty * (1 if t.side == "long" else -1)
        _append_trade_csv({
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "side": t.side,
            "entry": f"{t.weighted_avg_price:.2f}",
            "exit": f"{exit_p:.2f}",
            "qty": f"{close_qty}",
            "pnl": f"{pnl:.4f}",
            "reason": reason,
            "is_win": 1 if pnl > 0 else 0
        })
        # Clear state BEFORE notify so Telegram failure cannot leave stale trade object
        self.trade = None
        self._save_state()
        try:
            notify_trade_close(t.side, t.weighted_avg_price, exit_p, close_qty, reason, db=self.db)
        except Exception as exc:
            logger.warning(f"notify_trade_close failed: {exc}")

    # ── Cancel pending ────────────────────────────────────────────────────────

    def _cancel_pending(self) -> None:
        if not self.pending:
            return
        oid = self.pending.order_id
        try:
            self.exec.cancel_order(oid)
        except Exception as exc:
            logger.error(f"_cancel_pending: first cancel failed for {oid}: {exc} — clearing state anyway")
        # Verify the order is actually gone (HTX processes cancels async).
        try:
            open_ids = {o.get("id") for o in self.exec.get_open_orders()}
            if oid in open_ids:
                logger.warning(f"Order {oid} still open after cancel — retrying")
                self.exec.cancel_order(oid)
        except Exception as exc:
            logger.warning(f"_cancel_pending verify failed: {exc}")
        self.pending = None

    # ── Funding blackout: manage trades but cancel pending new orders ─────────

    def update_expiry_only(self, bar_index: int) -> None:
        """
        Used during funding-rate blackout. Cancels expired pending limit orders
        (we don't want new fills during high-volatility funding windows).
        Active trades are NOT touched here — caller must still call update() for
        stop/TP management when bar data is available.
        """
        if self.pending and bar_index > self.pending.expiry_bar:
            self._pending_expired += 1
            logger.info(f"Pending {self.pending.side} expired (blackout) at bar {bar_index}")
            self._cancel_pending()

    # ── State accessors ───────────────────────────────────────────────────────

    @property
    def has_pending(self) -> bool:
        return self.pending is not None

    @property
    def has_trade(self) -> bool:
        return self.trade is not None

    @property
    def is_flat(self) -> bool:
        return not self.has_pending and not self.has_trade
