"""
tests/test_execution.py — Tests for fetch_order, fill detection, and partial fill handling.

All exchange calls are mocked — no live network required.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import pandas as pd
import numpy as np
from datetime import datetime, timezone

from config import BotConfig
from execution_engine import ExecutionEngine
from trade_manager import TradeManager, PendingOrder, GridPosition
from paper_trader import PaperExecutionEngine


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_config(**overrides) -> BotConfig:
    cfg = BotConfig()
    cfg.use_dca = False
    cfg.use_ml_filter = False
    cfg.use_obi_filter = False
    cfg.use_dynamic_atr_trailing = False
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _make_row(close=2000.0, high=2010.0, low=1990.0, atr=20.0) -> pd.Series:
    return pd.Series({
        "open": close - 5, "high": high, "low": low, "close": close,
        "atr": atr, "rsi": 55.0, "volume": 1000.0, "vol_median": 500.0,
        "ema_fast": close - 10, "ema_slow": close - 20, "vwap": close,
        "band_up": close + 50, "band_dn": close - 50,
        "prev_high": high + 5, "prev_low": low - 5,
        "swing_high": high + 10, "swing_low": low - 10,
        "pdh": high + 20, "pdl": low - 20,
        "htf_trend_long": True, "htf_trend_short": True,
        "trend_long": True, "trend_short": False,
        "bull_engulf": False, "bear_engulf": False,
    }, name=pd.Timestamp("2026-01-01 12:00:00", tz="UTC"))


def _make_mock_exchange() -> MagicMock:
    ex = MagicMock()
    ex.fetch_positions.return_value = []  # flat by default
    ex.fetch_open_orders.return_value = []
    ex.fetch_balance.return_value = {"USDT": {"free": 300.0}}
    ex.create_order.return_value = {"id": "ORD-001"}
    return ex


def _make_pending(order_id="ORD-001", side="long") -> PendingOrder:
    return PendingOrder(
        side=side, order_id=order_id, retest_level=2000.0,
        expiry_bar=10, score=60.0,
        stop=1976.0, tp1=2003.0, tp2=2006.0, tp3=2012.0, qty=0.05,
    )


# ── ExecutionEngine.fetch_order ───────────────────────────────────────────────

class TestFetchOrder:
    def test_filled_order_returns_correct_status(self):
        ex = _make_mock_exchange()
        ex.contractPrivatePostLinearSwapApiV1SwapCrossOrderInfo.return_value = {
            "status": "ok",
            "data": [{
                "order_id": "ORD-001",
                "status": 6,          # filled
                "volume": 5,          # 5 contracts = 0.05 ETH
                "trade_volume": 5,
                "trade_avg_price": 2001.5,
            }]
        }
        engine = ExecutionEngine(ex, _make_config())
        result = engine.fetch_order("ORD-001")

        assert result is not None
        assert result["status"] == "filled"
        assert result["filled_qty"] == pytest.approx(0.05)
        assert result["remaining_qty"] == pytest.approx(0.0)
        assert result["avg_fill_price"] == pytest.approx(2001.5)

    def test_partial_fill_returns_partial_status(self):
        ex = _make_mock_exchange()
        ex.contractPrivatePostLinearSwapApiV1SwapCrossOrderInfo.return_value = {
            "status": "ok",
            "data": [{
                "order_id": "ORD-001",
                "status": 4,          # partial fill
                "volume": 5,
                "trade_volume": 3,    # 3 of 5 filled
                "trade_avg_price": 2000.0,
            }]
        }
        engine = ExecutionEngine(ex, _make_config())
        result = engine.fetch_order("ORD-001")

        assert result["status"] == "partial"
        assert result["filled_qty"] == pytest.approx(0.03)
        assert result["remaining_qty"] == pytest.approx(0.02)

    def test_cancelled_order_returns_cancelled_status(self):
        ex = _make_mock_exchange()
        ex.contractPrivatePostLinearSwapApiV1SwapCrossOrderInfo.return_value = {
            "status": "ok",
            "data": [{"order_id": "ORD-001", "status": 7, "volume": 5, "trade_volume": 0, "trade_avg_price": 0}]
        }
        engine = ExecutionEngine(ex, _make_config())
        result = engine.fetch_order("ORD-001")
        assert result["status"] == "cancelled"

    def test_open_order_returns_open_status(self):
        ex = _make_mock_exchange()
        ex.contractPrivatePostLinearSwapApiV1SwapCrossOrderInfo.return_value = {
            "status": "ok",
            "data": [{"order_id": "ORD-001", "status": 3, "volume": 5, "trade_volume": 0, "trade_avg_price": 0}]
        }
        engine = ExecutionEngine(ex, _make_config())
        result = engine.fetch_order("ORD-001")
        assert result["status"] == "open"

    def test_api_error_returns_none(self):
        import ccxt
        ex = _make_mock_exchange()
        ex.contractPrivatePostLinearSwapApiV1SwapCrossOrderInfo.side_effect = ccxt.NetworkError("timeout")
        engine = ExecutionEngine(ex, _make_config())
        result = engine.fetch_order("ORD-001")
        assert result is None

    def test_bad_api_status_returns_none(self):
        ex = _make_mock_exchange()
        ex.contractPrivatePostLinearSwapApiV1SwapCrossOrderInfo.return_value = {
            "status": "error", "err_msg": "Order not found"
        }
        engine = ExecutionEngine(ex, _make_config())
        result = engine.fetch_order("ORD-001")
        assert result is None

    def test_empty_data_array_returns_none(self):
        ex = _make_mock_exchange()
        ex.contractPrivatePostLinearSwapApiV1SwapCrossOrderInfo.return_value = {"status": "ok", "data": []}
        engine = ExecutionEngine(ex, _make_config())
        result = engine.fetch_order("ORD-001")
        assert result is None


# ── _check_pending_fill — fill detection via fetch_order ─────────────────────

class TestFillDetection:
    def _make_manager_with_pending(self, mock_exec, side="long"):
        cfg = _make_config()
        manager = TradeManager(mock_exec, cfg)
        manager.pending = _make_pending(order_id="ORD-001", side=side)
        manager._pending_placed = 1
        return manager

    def test_full_fill_creates_grid_position(self):
        mock_exec = MagicMock()
        mock_exec.is_paper = False
        mock_exec.fetch_order.return_value = {
            "id": "ORD-001", "status": "filled",
            "filled_qty": 0.05, "remaining_qty": 0.0, "avg_fill_price": 2001.0,
        }
        mock_exec.place_stop_market_order.return_value = {"id": "TRG:STOP-001"}

        manager = self._make_manager_with_pending(mock_exec)
        manager.update(_make_row(), bar_index=1)

        assert manager.trade is not None
        assert manager.trade.side == "long"
        assert manager.trade.total_qty == pytest.approx(0.05)
        assert manager.trade.weighted_avg_price == pytest.approx(2001.0)
        assert manager.pending is None

    def test_partial_fill_creates_position_with_actual_qty(self):
        mock_exec = MagicMock()
        mock_exec.is_paper = False
        mock_exec.fetch_order.return_value = {
            "id": "ORD-001", "status": "partial",
            "filled_qty": 0.03, "remaining_qty": 0.02, "avg_fill_price": 2000.5,
        }
        mock_exec.cancel_order.return_value = True
        mock_exec.place_stop_market_order.return_value = {"id": "TRG:STOP-001"}

        manager = self._make_manager_with_pending(mock_exec, side="long")
        # Use a flat bar well below TP1 so no TP events fire on fill bar
        fill_row = _make_row(close=2001.0, high=2001.5, low=1999.0)
        manager.update(fill_row, bar_index=1)

        assert manager.trade is not None
        assert manager.trade.total_qty == pytest.approx(0.03)
        assert manager.trade.weighted_avg_price == pytest.approx(2000.5)
        # First cancel_order call must be for the partial remainder, not a stop
        first_cancel_arg = mock_exec.cancel_order.call_args_list[0][0][0]
        assert first_cancel_arg == "ORD-001"

    def test_cancelled_order_clears_pending_no_trade(self):
        mock_exec = MagicMock()
        mock_exec.is_paper = False
        mock_exec.fetch_order.return_value = {
            "id": "ORD-001", "status": "cancelled",
            "filled_qty": 0.0, "remaining_qty": 0.05, "avg_fill_price": 0.0,
        }

        manager = self._make_manager_with_pending(mock_exec)
        manager.update(_make_row(), bar_index=1)

        assert manager.trade is None
        assert manager.pending is None

    def test_open_order_leaves_pending_intact(self):
        mock_exec = MagicMock()
        mock_exec.is_paper = False
        mock_exec.fetch_order.return_value = {
            "id": "ORD-001", "status": "open",
            "filled_qty": 0.0, "remaining_qty": 0.05, "avg_fill_price": 0.0,
        }

        manager = self._make_manager_with_pending(mock_exec)
        manager.update(_make_row(), bar_index=1)

        assert manager.trade is None
        assert manager.pending is not None

    def test_fetch_order_timeout_falls_back_to_position_check(self):
        """When fetch_order returns None (API error), fall back to get_position()."""
        mock_exec = MagicMock()
        mock_exec.is_paper = False
        mock_exec.fetch_order.return_value = None  # API failure
        # Position check shows filled on same side
        mock_exec.get_position.return_value = {
            "contracts": 5, "side": "long", "entryPrice": 2000.0
        }
        mock_exec.place_stop_market_order.return_value = {"id": "TRG:STOP-001"}

        manager = self._make_manager_with_pending(mock_exec)
        manager.update(_make_row(), bar_index=1)

        assert manager.trade is not None
        assert manager.trade.side == "long"

    def test_vanished_order_with_no_position_clears_pending(self):
        """fetch_order fails AND no matching position → clear pending."""
        mock_exec = MagicMock()
        mock_exec.is_paper = False
        mock_exec.fetch_order.return_value = None
        mock_exec.get_position.return_value = None  # flat
        mock_exec.get_open_orders.return_value = []  # order not in list

        manager = self._make_manager_with_pending(mock_exec)
        manager.update(_make_row(), bar_index=1)

        assert manager.trade is None
        assert manager.pending is None

    def test_stop_placement_failure_triggers_emergency_close(self):
        """If stop cannot be placed after fill, emergency close is called and trade cleared."""
        mock_exec = MagicMock()
        mock_exec.is_paper = False
        mock_exec.fetch_order.return_value = {
            "id": "ORD-001", "status": "filled",
            "filled_qty": 0.05, "remaining_qty": 0.0, "avg_fill_price": 2000.0,
        }
        mock_exec.place_stop_market_order.side_effect = Exception("Exchange rejected stop")
        # Position already flat (exchange stop fired or manual close) — skip market order
        mock_exec.get_position.return_value = {"contracts": 0, "side": None}

        manager = self._make_manager_with_pending(mock_exec)
        manager.update(_make_row(), bar_index=1)

        # Trade must be None regardless of whether market close was needed
        assert manager.trade is None
        assert manager.pending is None


# ── Stop rate limiting ────────────────────────────────────────────────────────

class TestStopRateLimiting:
    def _make_manager_with_trade(self, mock_exec, stop=1960.0, high_wm=2050.0):
        cfg = _make_config(use_dynamic_atr_trailing=True, trail_atr_mult=1.5, trail_min_atr_move=0.5)
        manager = TradeManager(mock_exec, cfg)
        # tp3=9999 so it never hits before trailing stop logic runs
        t = GridPosition(side="long", stop=stop, tp1=2200.0, tp2=2500.0, tp3=9999.0)
        t.tp1_hit = True   # skip TP1 logic so only trailing stop runs
        t.add_entry(2000.0, 0.05, "2026-01-01T00:00:00+00:00")
        t.high_watermark = high_wm
        t.stop_order_id = "TRG:STOP-001"
        manager.trade = t
        return manager

    def test_small_trail_improvement_does_not_replace_stop(self):
        """Trail improves by < 0.5 ATR — no API call."""
        mock_exec = MagicMock()
        mock_exec.is_paper = False

        # ATR=20, trail_mult=1.5 → trail_dist=30. high_wm=2050 → new_trail=2020.
        # current stop=2015 → improvement=5 < min_move=10 (0.5×20) → no update.
        manager = self._make_manager_with_trade(mock_exec, stop=2015.0, high_wm=2050.0)
        # high must not exceed high_wm so watermark stays 2050 → new_trail=2020, improvement=5<10
        row = _make_row(close=2048.0, high=2050.0, low=2040.0, atr=20.0)
        manager._check_trade(row, bar_index=5)

        mock_exec.cancel_order.assert_not_called()
        mock_exec.place_stop_market_order.assert_not_called()

    def test_large_trail_improvement_replaces_stop(self):
        """Trail improves by > 0.5 ATR — stop is replaced."""
        mock_exec = MagicMock()
        mock_exec.is_paper = False
        mock_exec.cancel_order.return_value = True
        mock_exec.place_stop_market_order.return_value = {"id": "TRG:STOP-002"}

        # ATR=20, min_move=10. high_wm=2100 → trail_dist=30 → new_trail=2070.
        # current stop=1960 → improvement=110 >> 10 → must update.
        manager = self._make_manager_with_trade(mock_exec, stop=1960.0, high_wm=2100.0)
        row = _make_row(close=2100.0, high=2105.0, low=2090.0, atr=20.0)
        manager._check_trade(row, bar_index=5)

        mock_exec.cancel_order.assert_called_once()
        mock_exec.place_stop_market_order.assert_called_once()


# ── Paper trader fill simulation parity ──────────────────────────────────────

class TestPaperFillParity:
    def test_is_paper_attribute_is_true(self):
        paper = PaperExecutionEngine(_make_config())
        assert paper.is_paper is True

    def test_long_fill_on_low_touch(self):
        paper = PaperExecutionEngine(_make_config())
        paper.place_limit_order("buy", 0.05, 2000.0)
        row = _make_row(close=2010.0, high=2020.0, low=1999.0)
        paper.simulate_bar(row)
        pos = paper.get_position()
        assert pos is not None
        assert pos["side"] == "long"

    def test_short_fill_on_high_touch(self):
        paper = PaperExecutionEngine(_make_config())
        paper.place_limit_order("sell", 0.05, 2050.0)
        row = _make_row(close=2040.0, high=2055.0, low=2035.0)
        paper.simulate_bar(row)
        pos = paper.get_position()
        assert pos is not None
        assert pos["side"] == "short"

    def test_limit_not_filled_if_price_not_touched(self):
        paper = PaperExecutionEngine(_make_config())
        paper.place_limit_order("buy", 0.05, 2000.0)
        row = _make_row(close=2050.0, high=2060.0, low=2020.0)  # low > 2000
        paper.simulate_bar(row)
        assert paper.get_position() is None

    def test_stop_fires_on_low_touch(self):
        paper = PaperExecutionEngine(_make_config())
        paper.place_limit_order("buy", 0.05, 2000.0)
        paper.simulate_bar(_make_row(low=1995.0))  # fill long
        paper.place_stop_market_order("sell", 0.05, 1970.0)
        paper.simulate_bar(_make_row(low=1965.0))  # stop hit
        assert paper.get_position() is None
