"""
tests/test_persistence.py — SQLite StateDB tests.

Covers save/load roundtrip, schema_version, notified_ids deduplication,
meta key/value store, and state recovery after simulated crash.
"""

import json
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone

from persistence import StateDB
from trade_manager import TradeManager, GridPosition, Entry
from config import BotConfig


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_cfg() -> BotConfig:
    cfg = BotConfig()
    cfg.use_ml_filter  = False
    cfg.use_obi_filter = False
    return cfg


def _make_db(tmp_path) -> StateDB:
    return StateDB(str(tmp_path / "test_state.db"))


# ── StateDB basics ────────────────────────────────────────────────────────────

class TestStateDB:
    def test_creates_tables_on_init(self, tmp_path):
        db = _make_db(tmp_path)
        import sqlite3
        with sqlite3.connect(db.db_path) as conn:
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert "bot_state"    in tables
        assert "bot_meta"     in tables
        assert "notified_ids" in tables

    def test_save_and_load_roundtrip(self, tmp_path):
        db = _make_db(tmp_path)
        data = {"schema_version": 2, "saved_at": "2026-04-24T00:00:00+00:00",
                "trade": None, "pending": None, "counters": {}}
        db.save_state(data)
        loaded = db.load_state()
        assert loaded["schema_version"] == 2
        assert loaded["trade"] is None

    def test_load_returns_empty_dict_when_no_state(self, tmp_path):
        db = _make_db(tmp_path)
        assert db.load_state() == {}

    def test_save_overwrites_previous_state(self, tmp_path):
        db = _make_db(tmp_path)
        db.save_state({"version": 1})
        db.save_state({"version": 2})
        loaded = db.load_state()
        assert loaded["version"] == 2

    def test_get_set_meta(self, tmp_path):
        db = _make_db(tmp_path)
        assert db.get_meta("key_x") == ""
        assert db.get_meta("key_x", "default") == "default"
        db.set_meta("key_x", "hello")
        assert db.get_meta("key_x") == "hello"

    def test_meta_overwrite(self, tmp_path):
        db = _make_db(tmp_path)
        db.set_meta("k", "v1")
        db.set_meta("k", "v2")
        assert db.get_meta("k") == "v2"

    def test_was_notified_false_before_marking(self, tmp_path):
        db = _make_db(tmp_path)
        assert db.was_notified("evt-001") is False

    def test_mark_notified_sets_flag(self, tmp_path):
        db = _make_db(tmp_path)
        db.mark_notified("evt-001")
        assert db.was_notified("evt-001") is True

    def test_mark_notified_idempotent(self, tmp_path):
        db = _make_db(tmp_path)
        db.mark_notified("evt-001")
        db.mark_notified("evt-001")  # second call must not raise
        assert db.was_notified("evt-001") is True

    def test_different_events_independent(self, tmp_path):
        db = _make_db(tmp_path)
        db.mark_notified("evt-A")
        assert db.was_notified("evt-A") is True
        assert db.was_notified("evt-B") is False


# ── TradeManager state roundtrip ──────────────────────────────────────────────

class TestTradeManagerStatePersistence:
    def _make_manager(self, tmp_path):
        cfg = _make_cfg()
        cfg.data_dir = str(tmp_path)
        mock_exec = MagicMock()
        mock_exec.is_paper = False
        return TradeManager(mock_exec, cfg)

    def test_save_and_load_flat_state(self, tmp_path):
        mgr = self._make_manager(tmp_path)
        mgr._save_state()
        mgr2 = self._make_manager(tmp_path)
        mgr2.load_state()
        assert mgr2.trade   is None
        assert mgr2.pending is None

    def test_save_and_load_active_trade(self, tmp_path):
        mgr = self._make_manager(tmp_path)
        t = GridPosition(side="long", stop=1950.0, tp1=2050.0, tp2=2100.0, tp3=2200.0)
        t.add_entry(2000.0, 0.05, datetime.now(timezone.utc).isoformat())
        t.stop_order_id = "TRG:STOP-001"
        mgr.trade = t
        mgr._save_state()

        mgr2 = self._make_manager(tmp_path)
        mgr2.load_state()

        assert mgr2.trade is not None
        assert mgr2.trade.side == "long"
        assert mgr2.trade.stop == pytest.approx(1950.0)
        assert mgr2.trade.total_qty == pytest.approx(0.05)
        assert mgr2.trade.remaining_qty == pytest.approx(0.05)
        assert mgr2.trade.stop_order_id == "TRG:STOP-001"
        assert len(mgr2.trade.entries) == 1
        assert mgr2.trade.entries[0].price == pytest.approx(2000.0)

    def test_schema_version_in_saved_state(self, tmp_path):
        mgr = self._make_manager(tmp_path)
        mgr._save_state()
        raw = mgr.db.load_state()
        assert raw["schema_version"] == 2
        assert "saved_at" in raw

    def test_load_invalid_trade_clears_state(self, tmp_path):
        """Corrupted trade data (remaining_qty=0) must not be loaded."""
        mgr = self._make_manager(tmp_path)
        # Save a state with an invalid trade (remaining_qty=0 fails _validate_trade)
        bad_state = {
            "schema_version": 2,
            "saved_at": "2026-04-24T00:00:00+00:00",
            "trade": {
                "side": "long", "entries": [],
                "weighted_avg_price": 0.0,   # invalid
                "stop": 0.0, "tp1": 0.0, "tp2": 0.0, "tp3": 0.0,
                "total_qty": 0.0, "remaining_qty": 0.0,
                "next_dca_level": None, "level_index": 0,
                "stop_order_id": None, "be_moved": False,
                "tp1_hit": False, "tp2_hit": False,
                "fill_bar": 0, "last_dca_bar": -1,
                "high_watermark": 0.0, "low_watermark": 0.0,
            },
            "pending": None,
            "counters": {"placed": 0, "filled": 0, "expired": 0},
        }
        mgr.db.save_state(bad_state)

        mgr2 = self._make_manager(tmp_path)
        mgr2.load_state()
        assert mgr2.trade is None  # invalid trade must not be adopted

    def test_counters_persist_across_restart(self, tmp_path):
        mgr = self._make_manager(tmp_path)
        mgr._pending_placed  = 5
        mgr._pending_filled  = 3
        mgr._pending_expired = 2
        mgr._save_state()

        mgr2 = self._make_manager(tmp_path)
        mgr2.load_state()
        assert mgr2._pending_placed  == 5
        assert mgr2._pending_filled  == 3
        assert mgr2._pending_expired == 2
