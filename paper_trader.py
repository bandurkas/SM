"""
paper_trader.py — Simulated execution layer (no real orders).
Supports multi-entry position tracking and realized P&L calculation.
"""

import csv
import logging
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

from config import BotConfig, cfg as default_cfg

logger = logging.getLogger(__name__)

@dataclass
class PaperOrder:
    id: str
    side: str       # "buy" | "sell"
    type: str       # "limit" | "market" | "stop"
    qty: float
    price: float    # limit price (0 for market/stop)
    stop_price: float = 0.0
    status: str = "open"    # "open" | "filled" | "cancelled"
    fill_price: float = 0.0
    reduce_only: bool = False

class PaperExecutionEngine:
    is_paper: bool = True  # used by TradeManager for paper/live branching

    # Slippage applied at fill time (fraction of price, e.g. 0.0002 = 2 bps).
    # Buys fill slightly higher, sells fill slightly lower — conservative simulation.
    SLIPPAGE_PCT: float = 0.0002

    def __init__(self, config: BotConfig = default_cfg, log_path: str = "paper_trades.csv"):
        self.cfg = config
        self.symbol = config.symbol
        self._orders: dict[str, PaperOrder] = {}
        self._balance = config.init_dep
        self._log_path = Path(log_path)
        
        # Position tracking
        self.pos_qty: float = 0.0  # Positive for long, negative for short
        self.avg_entry: float = 0.0
        
        self._init_csv()

    def get_balance(self) -> float:
        margin_used = (abs(self.pos_qty) * self.avg_entry) / self.cfg.leverage if self.pos_qty != 0 else 0
        return self._balance - margin_used

    def place_limit_order(self, side: str, qty: float, price: float) -> dict:
        order = PaperOrder(id=f"P-LIM-{len(self._orders)}", side=side, type="limit", qty=qty, price=price)
        self._orders[order.id] = order
        return {"id": order.id, "price": price, "amount": qty, "side": side}

    def place_market_order(self, side: str, qty: float, reduce_only: bool = False) -> dict:
        # Market orders fill immediately with slippage
        price = self._last_price if hasattr(self, "_last_price") else 0.0
        if price > 0:
            slip  = price * self.SLIPPAGE_PCT
            price = price + slip if side == "buy" else price - slip
        self._process_fill(side, qty, price)
        return {"id": f"P-MKT-{len(self._orders)}", "amount": qty, "side": side}

    def place_stop_market_order(self, side: str, qty: float, stop_price: float) -> Optional[dict]:
        order = PaperOrder(id=f"P-STP-{len(self._orders)}", side=side, type="stop", qty=qty, price=0.0, stop_price=stop_price)
        self._orders[order.id] = order
        return {"id": order.id, "stopPrice": stop_price, "amount": qty, "side": side}

    def cancel_order(self, order_id: str) -> bool:
        if order_id in self._orders:
            self._orders[order_id].status = "cancelled"
            return True
        return False

    def daily_loss(self) -> float:
        """Sum of realized P&L for today (simulated)."""
        return 0.0

    def get_open_orders(self) -> list[dict]:
        return [
            {"id": o.id, "side": o.side, "price": o.price if o.type == "limit" else o.stop_price, "amount": o.qty}
            for o in self._orders.values() if o.status == "open"
        ]

    def get_position(self) -> Optional[dict]:
        if self.pos_qty == 0: return None
        return {
            "contracts": abs(self.pos_qty) / 0.01, # back to contracts for CCXT parity
            "side": "long" if self.pos_qty > 0 else "short",
            "entryPrice": self.avg_entry
        }

    def simulate_bar(self, row: pd.Series) -> None:
        self._last_price = row["close"]
        high, low = row["high"], row["low"]
        
        for order in list(self._orders.values()):
            if order.status != "open": continue
            
            filled = False
            fill_price = 0.0
            
            if order.type == "limit":
                if order.side == "buy" and low <= order.price:
                    filled, fill_price = True, order.price
                elif order.side == "sell" and high >= order.price:
                    filled, fill_price = True, order.price
            elif order.type == "stop":
                if order.side == "sell" and low <= order.stop_price:
                    filled, fill_price = True, order.stop_price
                elif order.side == "buy" and high >= order.stop_price:
                    filled, fill_price = True, order.stop_price
                    
            if filled:
                order.status = "filled"
                # Apply slippage: buys slip up, sells slip down
                slip = fill_price * self.SLIPPAGE_PCT
                fill_price = fill_price + slip if order.side == "buy" else fill_price - slip
                order.fill_price = fill_price
                self._process_fill(order.side, order.qty, fill_price)

    def _process_fill(self, side: str, qty: float, price: float):
        qty_signed = qty if side == "buy" else -qty
        
        # Commission
        fee = price * qty * (self.cfg.taker_fee_pct / 100)
        self._balance -= fee
        
        if self.pos_qty == 0:
            # New position
            self.pos_qty = qty_signed
            self.avg_entry = price
        elif (self.pos_qty > 0 and qty_signed > 0) or (self.pos_qty < 0 and qty_signed < 0):
            # Adding to position
            new_total = self.pos_qty + qty_signed
            self.avg_entry = (self.pos_qty * self.avg_entry + qty_signed * price) / new_total
            self.pos_qty = new_total
        else:
            # Closing or reversing
            close_qty = min(abs(self.pos_qty), abs(qty_signed))
            pnl = (price - self.avg_entry) * close_qty * (1 if self.pos_qty > 0 else -1)
            self._balance += pnl
            
            # Record in CSV
            self._append_csv(side, self.avg_entry, price, close_qty, pnl, fee)
            
            pre_sign  = self.pos_qty > 0  # sign before update
            self.pos_qty += qty_signed
            if abs(self.pos_qty) < 1e-8:
                self.pos_qty  = 0.0
                self.avg_entry = 0.0
            elif (pre_sign != (self.pos_qty > 0)):
                # Position actually reversed direction (long→short or short→long)
                self.avg_entry = price
            # else: partial close — avg_entry stays at original cost basis

    def summary(self) -> dict:
        # Load CSV to compute metrics
        try:
            df = pd.read_csv(self._log_path)
            if df.empty: return {"trades": 0}
            wins = df[df["pnl"] > 0]
            losses = df[df["pnl"] <= 0]
            return {
                "trades": len(df),
                "wins": len(wins),
                "losses": len(losses),
                "win_rate": round(len(wins) / len(df) * 100, 1) if len(df) > 0 else 0,
                "total_pnl": round(df["pnl"].sum() - df["fee"].sum(), 2),
                "avg_win": round(wins["pnl"].mean(), 2) if not wins.empty else 0,
                "avg_loss": round(losses["pnl"].mean(), 2) if not losses.empty else 0,
                "balance": round(self._balance, 2)
            }
        except:
            return {"trades": 0}

    def _init_csv(self) -> None:
        with open(self._log_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["side", "entry", "exit", "qty", "pnl", "fee"])

    def _append_csv(self, side, entry, exit, qty, pnl, fee) -> None:
        with open(self._log_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([side, entry, exit, qty, pnl, fee])
            
    def daily_loss_limit_hit(self) -> bool: return False
    def daily_trade_limit_hit(self) -> bool: return False
    def consecutive_loss_limit_hit(self) -> bool:
        """Returns True if last N trades were all losses (from backtest_trades.csv)."""
        try:
            import pandas as _pd
            df = _pd.read_csv(self._log_path)
            if len(df) < self.cfg.max_consecutive_losses:
                return False
            recent = df.tail(self.cfg.max_consecutive_losses)
            return bool((recent["pnl"] <= 0).all())
        except Exception:
            return False
    def reset_daily_stats(self): pass
