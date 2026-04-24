"""
tests/test_dca.py — DCA level math, martingale sizing, and stop reanchor tests.
"""

import pytest
from unittest.mock import MagicMock
import pandas as pd
from datetime import timezone
from datetime import datetime

from config import BotConfig
from dca_engine import (
    calculate_next_dca_level,
    calculate_next_dca_qty,
    calculate_range_scalp_exit,
)
from trade_manager import TradeManager, GridPosition


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cfg(**overrides) -> BotConfig:
    cfg = BotConfig()
    cfg.use_dca = True
    cfg.dca_max_levels     = 4
    cfg.dca_martingale_factor = 1.2
    cfg.dca_base_step_atr  = 1.0
    cfg.dca_step_multiplier = 1.1
    cfg.atr_mult_15m       = 1.2
    cfg.tick_size          = 0.01
    cfg.qty_step           = 0.01
    cfg.use_ml_filter      = False
    cfg.use_obi_filter     = False
    cfg.use_dynamic_atr_trailing = False
    cfg.use_range_scalper  = False
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _make_row(close=2000.0, atr=20.0, high=2010.0, low=1990.0) -> pd.Series:
    return pd.Series({
        "close": close, "high": high, "low": low, "open": close - 5,
        "atr": atr, "rsi": 55.0, "volume": 1000.0, "vol_median": 500.0,
        "ema_fast": close - 10, "ema_slow": close - 20, "vwap": close,
        "band_up": close + 50, "band_dn": close - 50,
        "prev_high": high + 5, "prev_low": low - 5,
        "swing_high": high + 10, "swing_low": low - 10,
        "pdh": high + 20, "pdl": low - 20,
        "htf_trend_long": True, "htf_trend_short": True,
        "trend_long": True, "trend_short": False,
        "bull_engulf": False, "bear_engulf": False,
        "bb_up": close + 40, "bb_dn": close - 40,
        "long_bb": False, "short_bb": False,
        "long_pullback": False, "short_pullback": False,
    }, name=pd.Timestamp("2026-01-01 12:00:00", tz="UTC"))


# ── calculate_next_dca_level ──────────────────────────────────────────────────

class TestDcaLevel:
    def test_long_dca_level_is_below_entry(self):
        cfg = _cfg()
        level = calculate_next_dca_level("long", 2000.0, atr=20.0, level_index=0, config=cfg)
        # distance = 20 * 1.0 * (1.1^0) = 20.0 → 2000 - 20 = 1980
        assert level == pytest.approx(1980.0, abs=0.1)
        assert level < 2000.0

    def test_short_dca_level_is_above_entry(self):
        cfg = _cfg()
        level = calculate_next_dca_level("short", 2000.0, atr=20.0, level_index=0, config=cfg)
        assert level > 2000.0

    def test_higher_level_index_increases_distance(self):
        cfg = _cfg()
        l0 = calculate_next_dca_level("long", 2000.0, 20.0, level_index=0, config=cfg)
        l1 = calculate_next_dca_level("long", 2000.0, 20.0, level_index=1, config=cfg)
        # level_index=1: distance = 20 * 1.0 * 1.1 = 22 → 1978
        assert l1 < l0  # farther from entry as index grows

    def test_zero_atr_does_not_crash(self):
        cfg = _cfg()
        # safe_atr = 0.0001 → distance rounds to 0 at tick=0.01, returns entry price
        # Key guarantee: no exception, returns a valid float
        level = calculate_next_dca_level("long", 2000.0, atr=0.0, level_index=0, config=cfg)
        assert isinstance(level, float)
        assert level <= 2000.0  # must not be above entry

    def test_result_respects_tick_size(self):
        cfg = _cfg(tick_size=0.01)
        level = calculate_next_dca_level("long", 2000.0, atr=20.123, level_index=0, config=cfg)
        remainder = round(level / 0.01) * 0.01
        assert abs(level - remainder) < 1e-6


# ── calculate_next_dca_qty ────────────────────────────────────────────────────

class TestDcaQty:
    def test_qty_increases_by_martingale_factor(self):
        cfg = _cfg(dca_martingale_factor=1.2, qty_step=0.01)
        next_qty = calculate_next_dca_qty(0.05, cfg)
        assert next_qty == pytest.approx(0.06, abs=0.005)

    def test_qty_rounded_to_qty_step(self):
        cfg = _cfg(dca_martingale_factor=1.2, qty_step=0.01)
        result = calculate_next_dca_qty(0.07, cfg)
        # 0.07 * 1.2 = 0.084 → rounds to 0.08
        assert result % 0.01 == pytest.approx(0.0, abs=1e-9)

    def test_small_initial_qty_still_produces_positive_result(self):
        cfg = _cfg(qty_step=0.01)
        result = calculate_next_dca_qty(0.01, cfg)
        assert result >= 0.01


# ── Stop reanchor after DCA fill ──────────────────────────────────────────────

class TestDcaStopReanchor:
    """
    After a DCA fill, the stop must be reanchored BELOW the DCA fill level
    (for longs) so a subsequent bar cannot immediately stop out the
    larger combined position.

    Invariant: t.stop < t.next_dca_level (for longs, before recalculation)
    """

    def _manager_with_active_long(self, entry=2000.0, stop=1976.0, atr=20.0, dca_level=1980.0):
        cfg = _cfg()
        mock_exec = MagicMock()
        mock_exec.is_paper = False
        mock_exec.place_market_order.return_value = {"id": "MKT-DCA"}
        mock_exec.place_stop_market_order.return_value = {"id": "TRG:NEW-STOP"}
        mock_exec.cancel_order.return_value = True

        manager = TradeManager(mock_exec, cfg)
        t = GridPosition(
            side="long", stop=stop,
            tp1=entry * 1.003, tp2=entry * 1.006, tp3=entry * 1.012,
        )
        t.add_entry(entry, 0.05, datetime.now(timezone.utc).isoformat())
        t.next_dca_level = dca_level
        t.stop_order_id  = "TRG:OLD-STOP"
        manager.trade = t
        return manager, mock_exec

    def test_stop_is_below_dca_fill_price_for_long(self):
        """After DCA triggers, new stop must be < DCA fill price."""
        dca_level = 1980.0
        manager, _ = self._manager_with_active_long(
            entry=2000.0, stop=1976.0, dca_level=dca_level
        )
        # Trigger DCA: low touches dca_level
        row = _make_row(close=1982.0, high=1990.0, low=1979.0, atr=20.0)
        manager._check_dca_levels(row, bar_index=3)

        assert manager.trade is not None
        assert manager.trade.stop < dca_level, (
            f"Stop {manager.trade.stop} must be below DCA fill level {dca_level}"
        )

    def test_stop_never_moves_up_after_dca(self):
        """DCA stop can only move down for longs (never closer to current price)."""
        original_stop = 1976.0
        manager, _ = self._manager_with_active_long(
            entry=2000.0, stop=original_stop, dca_level=1980.0
        )
        row = _make_row(close=1982.0, high=1990.0, low=1979.0, atr=20.0)
        manager._check_dca_levels(row, bar_index=3)

        assert manager.trade.stop <= original_stop, (
            f"Stop moved UP after DCA: {original_stop} → {manager.trade.stop}"
        )

    def test_dca_aborted_when_level_index_at_max(self):
        """No DCA after reaching dca_max_levels - 1."""
        cfg = _cfg(dca_max_levels=4)
        mock_exec = MagicMock()
        mock_exec.is_paper = False

        manager = TradeManager(mock_exec, cfg)
        t = GridPosition(side="long", stop=1960.0, tp1=2010.0, tp2=2020.0, tp3=2040.0)
        t.add_entry(2000.0, 0.05, datetime.now(timezone.utc).isoformat())
        t.next_dca_level = 1980.0
        t.level_index    = 3   # already at max (dca_max_levels - 1 = 3)
        manager.trade    = t

        row = _make_row(low=1979.0, atr=20.0)
        manager._check_dca_levels(row, bar_index=2)

        mock_exec.place_market_order.assert_not_called()

    def test_dca_aborted_when_exposure_exceeds_limit(self):
        """DCA aborted if new total notional > max_total_exposure_usd."""
        cfg = _cfg(max_total_exposure_usd=100.0)  # tiny limit
        mock_exec = MagicMock()
        mock_exec.is_paper = False

        manager = TradeManager(mock_exec, cfg)
        t = GridPosition(side="long", stop=1960.0, tp1=2010.0, tp2=2020.0, tp3=2040.0)
        t.add_entry(2000.0, 0.05, datetime.now(timezone.utc).isoformat())
        t.next_dca_level = 1980.0
        manager.trade    = t

        row = _make_row(close=1982.0, low=1979.0, atr=20.0)
        manager._check_dca_levels(row, bar_index=2)

        mock_exec.place_market_order.assert_not_called()

    def test_dca_does_not_fire_twice_on_same_bar(self):
        """last_dca_bar guard prevents double DCA on same bar."""
        mock_exec = MagicMock()
        mock_exec.is_paper = False
        mock_exec.place_market_order.return_value = {"id": "MKT-DCA"}
        mock_exec.place_stop_market_order.return_value = {"id": "TRG:STOP"}
        mock_exec.cancel_order.return_value = True

        manager, _ = self._manager_with_active_long(dca_level=1980.0)
        manager.trade.last_dca_bar = 5  # already fired on bar 5

        row = _make_row(low=1979.0, atr=20.0)
        manager._check_dca_levels(row, bar_index=5)  # same bar

        mock_exec.place_market_order.assert_not_called()

    def test_weighted_avg_price_recalculated_after_dca(self):
        """WAP should reflect both entries after DCA fill."""
        manager, mock_exec = self._manager_with_active_long(
            entry=2000.0, stop=1960.0, dca_level=1980.0
        )
        initial_wap = manager.trade.weighted_avg_price

        row = _make_row(close=1982.0, high=1990.0, low=1979.0, atr=20.0)
        manager._check_dca_levels(row, bar_index=3)

        if manager.trade:
            assert manager.trade.weighted_avg_price != initial_wap
            assert manager.trade.weighted_avg_price < initial_wap  # WAP moved down


# ── calculate_range_scalp_exit ────────────────────────────────────────────────

class TestRangeScalpExit:
    def test_long_scalp_exit_above_entry(self):
        cfg = _cfg(rs_profit_target_atr=0.4)
        exit_price = calculate_range_scalp_exit("long", 1980.0, atr=20.0, config=cfg)
        # 1980 + 20 * 0.4 = 1988
        assert exit_price == pytest.approx(1988.0, abs=0.1)
        assert exit_price > 1980.0

    def test_short_scalp_exit_below_entry(self):
        cfg = _cfg(rs_profit_target_atr=0.4)
        exit_price = calculate_range_scalp_exit("short", 2020.0, atr=20.0, config=cfg)
        assert exit_price < 2020.0
