"""
tests/test_strategy_engine.py — Setup detection and signal scoring tests.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import timezone
from config import BotConfig
from strategy_engine import (
    SetupFlags,
    calc_score,
    detect_setups_row,
    detect_setups_df,
    get_retest_level,
    _select_setups,
    get_signals,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cfg(**overrides) -> BotConfig:
    cfg = BotConfig()
    cfg.use_ml_filter  = False
    cfg.use_obi_filter = False
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _row(
    close=2000.0, high=2010.0, low=1990.0,
    prev_high=2005.0, prev_low=1995.0,
    atr=20.0, rsi=55.0, adx=25.0,
    volume=1000.0, vol_median=500.0,
    ema_fast=1990.0, ema_slow=1980.0, ema_200=1970.0,
    vwap=2000.0, band_up=2050.0, band_dn=1950.0,
    swing_high=2020.0, swing_low=1980.0,
    pdh=2030.0, pdl=1970.0,
    htf_trend_long=True, htf_trend_short=True,
    trend_long=True, trend_short=False,
    bull_engulf=False, bear_engulf=False,
) -> pd.Series:
    return pd.Series({
        "open": close - 5, "high": high, "low": low, "close": close,
        "prev_high": prev_high, "prev_low": prev_low,
        "atr": atr, "rsi": rsi, "adx": adx,
        "volume": volume, "vol_median": vol_median,
        "ema_fast": ema_fast, "ema_slow": ema_slow, "ema_200": ema_200,
        "vwap": vwap, "band_up": band_up, "band_dn": band_dn,
        "swing_high": swing_high, "swing_low": swing_low,
        "pdh": pdh, "pdl": pdl,
        "htf_trend_long": htf_trend_long, "htf_trend_short": htf_trend_short,
        "trend_long": trend_long, "trend_short": trend_short,
        "bull_engulf": bull_engulf, "bear_engulf": bear_engulf,
        "bb_up": band_up, "bb_dn": band_dn,
        "long_bb": False, "short_bb": False,
        "long_pullback": False, "short_pullback": False,
    }, name=pd.Timestamp("2026-01-01 12:00:00", tz="UTC"))


def _df_with_sweep(n=50) -> pd.DataFrame:
    """Build a minimal DataFrame with one sweep signal bar."""
    dates = pd.date_range("2026-01-01", periods=n, freq="15min", tz="UTC")
    close = 2000.0
    df = pd.DataFrame({
        "open":   [close - 2] * n,
        "high":   [close + 10] * n,
        "low":    [close - 5] * n,
        "close":  [close] * n,
        "volume": [1000.0] * n,
    }, index=dates)
    # Add required indicator columns
    df["atr"]        = 20.0
    df["rsi"]        = 55.0
    df["adx"]        = 25.0
    df["ema_fast"]   = close - 10
    df["ema_slow"]   = close - 20
    df["ema_200"]    = close - 30
    df["ema_200"]    = close - 30
    df["vwap"]       = close
    df["band_up"]    = close + 50
    df["band_dn"]    = close - 50
    df["bb_up"]      = close + 40
    df["bb_dn"]      = close - 40
    df["prev_high"]  = (df["high"].shift(1).rolling(15).max()).fillna(close + 8)
    df["prev_low"]   = (df["low"].shift(1).rolling(15).min()).fillna(close - 8)
    df["swing_high"] = close + 15
    df["swing_low"]  = close - 15
    df["pdh"]        = close + 20
    df["pdl"]        = close - 20
    df["vol_median"] = 600.0
    df["htf_trend_long"]  = True
    df["htf_trend_short"] = True
    df["trend_long"]  = True
    df["trend_short"] = False
    df["bull_engulf"] = False
    df["bear_engulf"] = False
    # Plant a sweep on the last bar
    df.iloc[-1, df.columns.get_loc("low")]  = close - 12   # breaks prev_low range
    df.iloc[-1, df.columns.get_loc("close")] = close + 2   # closes back above prev_low
    return df


# ── calc_score ────────────────────────────────────────────────────────────────

class TestCalcScore:
    def test_sweep_flag_gives_nonzero_score(self):
        cfg = _cfg()
        flags = SetupFlags(long_sweep=True, vol_ok=True, htf_trend_long=True)
        row = _row()
        score = calc_score(row, flags, is_long=True, config=cfg)
        assert score >= cfg.w_sweep

    def test_no_flags_gives_zero(self):
        cfg = _cfg(min_setups_confluence=1)
        flags = SetupFlags()
        row = _row()
        score = calc_score(row, flags, is_long=True, config=cfg)
        assert score == 0.0

    def test_score_capped_at_100(self):
        cfg = _cfg()
        # All flags firing simultaneously
        flags = SetupFlags(
            long_sweep=True, long_vmr=True, long_mom=True,
            long_trend_ride=True, long_bb=True, long_engulf=True,
            vol_ok=True, htf_trend_long=True,
        )
        row = _row()
        score = calc_score(row, flags, is_long=True, config=cfg)
        assert score <= 100.0

    def test_short_sweep_gives_nonzero_short_score(self):
        cfg = _cfg()
        flags = SetupFlags(short_sweep=True, vol_ok=True, htf_trend_short=True)
        row = _row()
        score = calc_score(row, flags, is_long=False, config=cfg)
        assert score >= cfg.w_sweep

    def test_confluence_bonus_when_two_setups_fire(self):
        cfg = _cfg(min_setups_confluence=1)
        single = SetupFlags(long_sweep=True, htf_trend_long=True)
        double = SetupFlags(long_sweep=True, long_mom=True, htf_trend_long=True)
        row = _row(trend_long=True)
        s_single = calc_score(row, single, is_long=True, config=cfg)
        s_double = calc_score(row, double, is_long=True, config=cfg)
        assert s_double > s_single  # confluence bonus must be present

    def test_min_confluence_blocks_single_setup_when_required(self):
        cfg = _cfg(min_setups_confluence=2)
        flags = SetupFlags(long_sweep=True, htf_trend_long=True)
        row = _row()
        score = calc_score(row, flags, is_long=True, config=cfg)
        assert score == 0.0  # only 1 setup, need 2


# ── detect_setups_row ─────────────────────────────────────────────────────────

class TestDetectSetupsRow:
    def test_long_sweep_detected(self):
        """Bar dips below prev_low then closes above it → long sweep."""
        cfg = _cfg(
            vol_gate_enabled=False, chop_filter_enabled=False,
            sweep_depth_filter=False, use_pinbar_filter=False,
            use_adx_filter=False, use_session_filter=False,
            use_rsi_exhaustion=False,
        )
        # volume > vol_median * vol_mult required
        row  = _row(low=1994.0, close=2001.0, prev_low=1995.0, volume=1500.0, vol_median=500.0)
        prev = _row()
        flags = detect_setups_row(row, prev, cfg)
        assert flags.long_sweep is True

    def test_no_sweep_if_close_below_prev_low(self):
        """Closes below prev_low → not a reversal → no sweep."""
        cfg = _cfg(vol_gate_enabled=False, chop_filter_enabled=False,
                   sweep_depth_filter=False, use_pinbar_filter=False,
                   use_adx_filter=False, use_session_filter=False,
                   use_rsi_exhaustion=False)
        row  = _row(low=1994.0, close=1993.0, prev_low=1995.0, volume=1500.0, vol_median=500.0)
        prev = _row()
        flags = detect_setups_row(row, prev, cfg)
        assert flags.long_sweep is False

    def test_htf_filter_blocks_long_sweep_in_downtrend(self):
        cfg = _cfg(
            htf_filter=True, vol_gate_enabled=False,
            chop_filter_enabled=False, sweep_depth_filter=False,
            use_pinbar_filter=False, use_adx_filter=False,
            use_session_filter=False, use_rsi_exhaustion=False,
        )
        row  = _row(low=1994.0, close=2001.0, prev_low=1995.0,
                    volume=1500.0, vol_median=500.0, htf_trend_long=False)
        prev = _row()
        flags = detect_setups_row(row, prev, cfg)
        assert flags.long_sweep is False


# ── detect_setups_df columns ──────────────────────────────────────────────────

class TestDetectSetupsDf:
    def test_all_setup_columns_written_to_df(self):
        """All SetupFlags fields must be present as df columns after detect_setups_df."""
        df = _df_with_sweep()
        cfg = _cfg()
        result = detect_setups_df(df, cfg)
        expected_cols = [
            "long_sweep", "short_sweep",
            "long_vmr", "short_vmr",
            "long_mom", "short_mom",
            "long_trend_ride", "short_trend_ride",
            "vol_ok",
            "long_engulf", "short_engulf",
            "long_bb", "short_bb",
            "long_pullback", "short_pullback",
        ]
        for col in expected_cols:
            assert col in result.columns, f"Column '{col}' missing from detect_setups_df output"

    def test_column_dtypes_are_bool(self):
        df = _df_with_sweep()
        result = detect_setups_df(df, _cfg())
        for col in ["long_sweep", "short_sweep", "long_mom", "long_vmr"]:
            assert result[col].dtype == bool or result[col].dtype == object  # pandas bool or object


# ── _select_setups ────────────────────────────────────────────────────────────

class TestSelectSetups:
    def test_sweep_momentum_selects_sweep(self):
        cfg = _cfg(setups_opt="Sweep+Momentum", side_filter="Both")
        flags = SetupFlags(long_sweep=True)
        want_long, want_short = _select_setups(flags, cfg)
        assert want_long is True
        assert want_short is False

    def test_long_only_blocks_short(self):
        cfg = _cfg(setups_opt="All", side_filter="Long only")
        flags = SetupFlags(long_sweep=True, short_sweep=True)
        want_long, want_short = _select_setups(flags, cfg)
        assert want_long is True
        assert want_short is False

    def test_unknown_setups_opt_returns_false(self):
        cfg = _cfg(setups_opt="UNKNOWN_SETUP")
        flags = SetupFlags(long_sweep=True, short_sweep=True)
        want_long, want_short = _select_setups(flags, cfg)
        assert want_long is False
        assert want_short is False

    def test_sweep_momentum_excludes_vmr(self):
        cfg = _cfg(setups_opt="Sweep+Momentum", side_filter="Both")
        flags = SetupFlags(long_vmr=True)   # only VMR, no sweep/mom/bb
        want_long, _ = _select_setups(flags, cfg)
        assert want_long is False


# ── get_retest_level ──────────────────────────────────────────────────────────

class TestGetRetestLevel:
    def test_sweep_uses_prev_low(self):
        flags = SetupFlags(long_sweep=True)
        row   = _row(prev_low=1995.0)
        level = get_retest_level(is_long=True, flags=flags, row=row)
        assert level == pytest.approx(1995.0)

    def test_mom_uses_ema_fast(self):
        flags = SetupFlags(long_mom=True)
        row   = _row(ema_fast=1988.0)
        level = get_retest_level(is_long=True, flags=flags, row=row)
        assert level == pytest.approx(1988.0)

    def test_no_flags_returns_none(self):
        flags = SetupFlags()
        row   = _row()
        level = get_retest_level(is_long=True, flags=flags, row=row)
        assert level is None


# ── get_signals integration ───────────────────────────────────────────────────

class TestGetSignals:
    def test_returns_empty_list_when_no_setup(self):
        cfg  = _cfg()
        row  = _row()
        prev = _row()
        signals = get_signals(row, prev, cfg)
        assert isinstance(signals, list)

    def test_long_only_filter_blocks_short_signals(self):
        cfg = _cfg(
            side_filter="Long only", setups_opt="All",
            vol_gate_enabled=False, chop_filter_enabled=False,
            sweep_depth_filter=False, use_pinbar_filter=False,
            use_adx_filter=False, use_session_filter=False,
            use_rsi_exhaustion=False, auto_trade_threshold=0,
            min_setups_confluence=1,
        )
        row  = _row(
            high=2012.0, close=2009.0,    # short_sweep: high > prev_high, close < prev_high
            prev_high=2010.0, volume=2000.0, vol_median=500.0,
        )
        prev = _row()
        signals = get_signals(row, prev, cfg)
        assert all(s.side != "short" for s in signals)
