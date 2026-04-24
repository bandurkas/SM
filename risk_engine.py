"""
risk_engine.py — Stop price calculation, position sizing, and TP levels.
"""

import math
from typing import Optional
from config import BotConfig, cfg as default_cfg

def round_tick(price: float, tick_size: float) -> float:
    return round(round(price / tick_size) * tick_size, 10)

def round_qty(qty: float, qty_step: float) -> float:
    return max(qty_step, round(round(qty / qty_step) * qty_step, 10))

def nearest_below(price: float, prev_low: float, band_dn: float, pdl: float) -> Optional[float]:
    candidates = []
    for level in (prev_low, band_dn, pdl):
        if not math.isnan(level) and level < price:
            candidates.append(level)
    return max(candidates) if candidates else None

def nearest_above(price: float, prev_high: float, band_up: float, pdh: float) -> Optional[float]:
    candidates = []
    for level in (prev_high, band_up, pdh):
        if not math.isnan(level) and level > price:
            candidates.append(level)
    return min(candidates) if candidates else None

def get_stop_price(
    is_long: bool,
    entry: float,
    atr: float,
    swing_low: float,
    swing_high: float,
    prev_low: float,
    band_dn: float,
    pdl: float,
    prev_high: float,
    band_up: float,
    pdh: float,
    config: BotConfig = default_cfg,
    atr_mult: Optional[float] = None,
) -> float:
    c = config
    tick = c.tick_size
    if atr_mult is None:
        atr_mult = c.atr_mult_15m

    atr_stop = entry - (atr * atr_mult) if is_long else entry + (atr * atr_mult)
    
    if is_long:
        swing_stop = swing_low - (atr * c.pad_swing_atr)
        base_stop = min(atr_stop, swing_stop) if c.sl_mode == "Hybrid" else (atr_stop if c.sl_mode == "ATR" else swing_stop)
        liq = nearest_below(entry, prev_low, band_dn, pdl)
        if c.use_liq_stop and liq:
            base_stop = min(base_stop, liq - (c.liq_pad_ticks * tick))
        final_stop = base_stop - (c.stop_buffer_ticks * tick)
        max_dist = atr * c.stop_cap_atr_mult
        if (entry - final_stop) > max_dist:
            final_stop = entry - max_dist
    else:
        swing_stop = swing_high + (atr * c.pad_swing_atr)
        base_stop = max(atr_stop, swing_stop) if c.sl_mode == "Hybrid" else (atr_stop if c.sl_mode == "ATR" else swing_stop)
        liq = nearest_above(entry, prev_high, band_up, pdh)
        if c.use_liq_stop and liq:
            base_stop = max(base_stop, liq + (c.liq_pad_ticks * tick))
        final_stop = base_stop + (c.stop_buffer_ticks * tick)
        max_dist = atr * c.stop_cap_atr_mult
        if (final_stop - entry) > max_dist:
            final_stop = entry + max_dist
            
    return round_tick(final_stop, tick)

def calc_qty(
    entry: float,
    stop: float,
    config: BotConfig = default_cfg,
    available_balance: Optional[float] = None,
) -> float:
    c = config
    dist = abs(entry - stop)
    if dist <= 0:
        return c.qty_step
    
    balance = available_balance if (available_balance and available_balance > 0) else c.init_dep
    risk_usdt = balance * (getattr(c, "risk_per_trade_pct", c.risk_pct) / 100.0)
    qty = risk_usdt / dist
    
    max_notional = balance * c.leverage * 0.80
    max_qty = max_notional / entry
    qty = min(qty, max_qty)
    return round_qty(qty, c.qty_step)

def calc_tp_levels(
    is_long: bool,
    entry: float,
    stop: float,
    config: BotConfig = default_cfg,
) -> tuple[float, float, float]:
    c = config
    tick = c.tick_size
    dist = abs(entry - stop)
    sign = 1 if is_long else -1
    
    tp1 = round_tick(entry + sign * c.r_tp1 * dist, tick)
    tp2 = round_tick(entry + sign * c.r_tp2 * dist, tick)
    tp3 = round_tick(entry + sign * c.r_tp3 * dist, tick)
    return tp1, tp2, tp3

def est_risk_from_qty(entry: float, stop: float, qty: float) -> float:
    return abs(entry - stop) * qty
