"""
dca_engine.py — Logic for Smart Averaging (DCA) and Grid management.
Calculates next entry levels and order sizes based on market volatility.
"""

import logging
from typing import Optional, Tuple

from config import BotConfig
from risk_engine import round_tick

logger = logging.getLogger(__name__)

def calculate_next_dca_level(
    side: str,
    last_entry_price: float,
    atr: float,
    level_index: int,
    config: BotConfig
) -> float:
    """
    Calculate the price for the next DCA level.
    Uses ATR and a step multiplier to increase distance as the position deepens.
    """
    # Distance = Base ATR step * (Step Multiplier ^ Level Index)
    safe_atr = max(atr, 0.0001)  # Prevent zero ATR issues
    step_atr_mult = config.dca_base_step_atr * (config.dca_step_multiplier ** level_index)
    distance = safe_atr * step_atr_mult
    
    if side == "long":
        level_price = last_entry_price - distance
    else:
        level_price = last_entry_price + distance
        
    return round_tick(level_price, config.tick_size)

def calculate_next_dca_qty(
    last_qty: float,
    config: BotConfig
) -> float:
    """
    Calculate the quantity for the next DCA entry using Martingale multiplier.
    """
    next_qty = last_qty * config.dca_martingale_factor
    # Round to qty_step (standard math rounding instead of half-to-even)
    return float(int(next_qty / config.qty_step + 0.5000001) * config.qty_step)

def get_weighted_avg_price(entries: list) -> float:
    """
    entries: List of dicts {'price': float, 'qty': float}
    """
    if not entries:
        return 0.0
    total_qty = sum(e['qty'] for e in entries)
    if total_qty == 0:
        return 0.0
    weighted_sum = sum(e['price'] * e['qty'] for e in entries)
    return weighted_sum / total_qty

def calculate_range_scalp_exit(
    side: str,
    last_entry_price: float,
    atr: float,
    config: BotConfig
) -> float:
    """
    Target price for a 'range scalp' exit (partial shave).
    """
    safe_atr = max(atr, 0.0001)
    profit_dist = safe_atr * config.rs_profit_target_atr
    if side == "long":
        exit_price = last_entry_price + profit_dist
    else:
        exit_price = last_entry_price - profit_dist
        
    return round_tick(exit_price, config.tick_size)
