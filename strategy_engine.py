"""
strategy_engine.py — Setup detection and signal scoring.

Logic mirrors the Pine Script in context.md exactly:
  - Sweep & Reversal
  - VWAP Mean Reversion
  - Momentum Pullback
  - Composite scoring
  - Retest level selection
  - Adaptive cancel bars
"""

import math
import os
import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Optional, Dict, List
from config import BotConfig, cfg as default_cfg

logger = logging.getLogger(__name__)

# Global model instance (type kept as Any to avoid module-level catboost import)
_MODEL_INSTANCE: Optional[object] = None

def load_ml_model(path: str) -> bool:
    """Loads the model once at startup. Supports .pkl (calibrated) and .cbm (raw)."""
    global _MODEL_INSTANCE
    
    pkl_path = path.replace(".cbm", ".pkl")
    if os.path.exists(pkl_path):
        try:
            import joblib
            _MODEL_INSTANCE = joblib.load(pkl_path)
            logger.info(f"Calibrated ML Model loaded from {pkl_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to load calibrated model {pkl_path}: {e}")

    if not os.path.exists(path):
        logger.warning(f"ML Model file not found: {path}")
        return False
        
    try:
        from catboost import CatBoostClassifier  # lazy import
        model = CatBoostClassifier()
        model.load_model(path)
        _MODEL_INSTANCE = model
        logger.info(f"Raw CatBoost Model loaded from {path}")
        return True
    except ImportError:
        logger.warning("catboost not installed — ML filter disabled")
        return False
    except Exception as e:
        logger.error(f"Failed to load ML model {path}: {e}")
        return False


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class SetupFlags:
    long_sweep: bool = False
    short_sweep: bool = False
    long_vmr: bool = False
    short_vmr: bool = False
    long_mom: bool = False
    short_mom: bool = False
    long_trend_ride: bool = False
    short_trend_ride: bool = False
    long_bb: bool = False
    short_bb: bool = False
    long_engulf: bool = False
    short_engulf: bool = False
    long_pullback: bool = False
    short_pullback: bool = False
    vol_ok: bool = False
    # P3: HTF trend alignment
    htf_trend_long: bool = True   # True = neutral/bullish on HTF (default allow)
    htf_trend_short: bool = True  # True = neutral/bearish on HTF (default allow)

    @property
    def raw_long(self) -> bool:
        return self.long_sweep or self.long_vmr or self.long_mom or self.long_trend_ride or self.long_bb or self.long_engulf

    @property
    def raw_short(self) -> bool:
        return self.short_sweep or self.short_vmr or self.short_mom or self.short_trend_ride or self.short_bb or self.short_engulf


@dataclass
class Signal:
    side: str           # "long" | "short"
    setup: str
    retest_level: float
    score: float
    auto_trade: bool
    obi: Optional[float] = None


# ── Crossover helpers ─────────────────────────────────────────────────────────

def _crossover(series: pd.Series, reference: pd.Series) -> pd.Series:
    """True where series crosses above reference."""
    return (series > reference) & (series.shift(1) <= reference.shift(1))


def _crossunder(series: pd.Series, reference: pd.Series) -> pd.Series:
    """True where series crosses below reference."""
    return (series < reference) & (series.shift(1) >= reference.shift(1))


# ── Vectorised setup detection (full DataFrame) ───────────────────────────────

def detect_setups_df(df: pd.DataFrame, config: BotConfig = default_cfg) -> pd.DataFrame:
    """
    Add boolean setup columns to the DataFrame.
    """
    from ml_features import prepare_features
    df = prepare_features(df)
    c = config

    vol_ok = df["volume"] > df["vol_median"] * c.vol_mult

    # P4: ATR volatility regime gate
    if c.vol_gate_enabled and "atr" in df.columns:
        atr_pct = df["atr"] / df["close"] * 100
        vol_regime_ok = (atr_pct >= c.min_atr_pct) & (atr_pct <= c.max_atr_pct)
    else:
        vol_regime_ok = pd.Series(True, index=df.index)

    # P6: EMA spread anti-chop filter
    if c.chop_filter_enabled and "ema_fast" in df.columns and "ema_slow" in df.columns:
        ema_spread_pct = (df["ema_fast"] - df["ema_slow"]).abs() / df["close"] * 100
        not_choppy = ema_spread_pct >= c.min_ema_spread_pct
    else:
        not_choppy = pd.Series(True, index=df.index)

    # P3: HTF trend filter (columns injected by market_data if htf_filter=True)
    if c.htf_filter and "htf_trend_long" in df.columns:
        htf_long_ok  = df["htf_trend_long"].fillna(True).astype(bool)
        htf_short_ok = df["htf_trend_short"].fillna(True).astype(bool)
    else:
        htf_long_ok  = pd.Series(True, index=df.index)
        htf_short_ok = pd.Series(True, index=df.index)

    # Sweep & Reversal — requires vol_ok (0.8× avg) to filter dead-bar fakeouts
    long_sweep_raw  = (df["low"] < df["prev_low"]) & (df["close"] > df["prev_low"])
    short_sweep_raw = (df["high"] > df["prev_high"]) & (df["close"] < df["prev_high"])

    # P5: Sweep depth quality filter
    if c.sweep_depth_filter and "atr" in df.columns:
        long_sweep_depth  = (df["prev_low"] - df["low"]) >= (df["atr"] * c.min_sweep_depth_atr)
        short_sweep_depth = (df["high"] - df["prev_high"]) >= (df["atr"] * c.min_sweep_depth_atr)
    else:
        long_sweep_depth  = pd.Series(True, index=df.index)
        short_sweep_depth = pd.Series(True, index=df.index)

    # P7: Pin-Bar Rejection Filter (New)
    if c.use_pinbar_filter:
        long_rejection = (df["close"] - df["low"]) / (df["high"] - df["low"]) >= c.pinbar_rejection_ratio
        short_rejection = (df["high"] - df["close"]) / (df["high"] - df["low"]) >= c.pinbar_rejection_ratio
    else:
        long_rejection = short_rejection = pd.Series(True, index=df.index)

    # P8: ADX Trend Strength Filter
    if c.use_adx_filter and "adx" in df.columns:
        adx_strong = (df["adx"] >= c.min_adx) & (df["adx"] <= c.max_adx)
    else:
        adx_strong = pd.Series(True, index=df.index)

    # P9: Session Filter (New)
    if c.use_session_filter:
        hours = df.index.hour
        session_ok = (hours >= c.session_start_utc) & (hours <= c.session_end_utc)
    else:
        session_ok = pd.Series(True, index=df.index)

    # P10: Volume Surge Filter (Bypassed for Trend Sniper)
    vol_surge = pd.Series(True, index=df.index)

    # P11: RSI Exhaustion Filter (Bypassed for Trend Sniper)
    rsi_long_ok = rsi_short_ok = pd.Series(True, index=df.index)

    # P12: Candlestick Confirmation (Engulfing)
    long_engulf = df["bull_engulf"]
    short_engulf = df["bear_engulf"]

    long_sweep  = long_sweep_raw  & long_sweep_depth  & vol_regime_ok & not_choppy & htf_long_ok & long_rejection & session_ok & vol_surge & rsi_long_ok
    short_sweep = short_sweep_raw & short_sweep_depth & vol_regime_ok & not_choppy & htf_short_ok & short_rejection & session_ok & vol_surge & rsi_short_ok

    # VWAP Mean Reversion
    dev_pct = ((df["close"] - df["vwap"]) / df["vwap"]).abs() * 100
    long_vmr  = (
        (df["close"].shift(1) < df["band_dn"].shift(1))
        & (df["close"] > df["band_dn"])
        & (dev_pct >= c.min_dev_pct_vw)
        & vol_regime_ok & htf_long_ok
    )
    short_vmr = (
        (df["close"].shift(1) > df["band_up"].shift(1))
        & (df["close"] < df["band_up"])
        & (dev_pct >= c.min_dev_pct_vw)
        & vol_regime_ok & htf_short_ok
    )

    # Momentum Pullback
    long_mom  = (
        df["trend_long"]
        & _crossover(df["close"], df["ema_fast"])
        & (df["low"] <= df["ema_fast"])
        & (df["rsi"] > 50)
        & vol_regime_ok & not_choppy & htf_long_ok & adx_strong & session_ok & vol_surge
    )
    short_mom = (
        df["trend_short"]
        & _crossunder(df["close"], df["ema_fast"])
        & (df["high"] >= df["ema_fast"])
        & (df["rsi"] < 50)
        & vol_regime_ok & not_choppy & htf_short_ok & adx_strong & session_ok & vol_surge
    )

    # Trend Ride — EMA Touch: bar wicks into EMA20 and closes back (smooth trend, HTF-only direction)
    long_trend_ride = (
        (df["low"] <= df["ema_fast"])
        & (df["close"] > df["ema_fast"])
        & (df["close"].shift(1) > df["ema_fast"].shift(1))   # prev bar above EMA (pullback, not crossover)
        & (df["rsi"] > 40) & (df["rsi"] < 70)
        & vol_regime_ok & not_choppy & htf_long_ok & adx_strong
    )
    short_trend_ride = (
        (df["high"] >= df["ema_fast"])
        & (df["close"] < df["ema_fast"])
        & (df["close"].shift(1) < df["ema_fast"].shift(1))   # prev bar below EMA (pullback, not crossover)
        & (df["rsi"] > 30) & (df["rsi"] < 60)
        & vol_regime_ok & not_choppy & htf_short_ok & adx_strong
    )

    # Bollinger Reversal (New)
    df["prev_bb_up"] = df["bb_up"].shift(1)
    df["prev_bb_dn"] = df["bb_dn"].shift(1)
    # Use shift(1) only for BB reversal (single-bar prev close vs BB band).
    # Do NOT overwrite prev_high/prev_low — they are set by calc_indicators as
    # rolling(sweep_len).max/min and must remain consistent with sweep detection.
    _prev_high_1 = df["high"].shift(1)
    _prev_low_1  = df["low"].shift(1)
    df["long_bb"] = (_prev_low_1 < df["prev_bb_dn"]) & (df["close"] > df["bb_dn"]) & rsi_long_ok & session_ok
    df["short_bb"] = (_prev_high_1 > df["prev_bb_up"]) & (df["close"] < df["bb_up"]) & rsi_short_ok & session_ok
    # Trend-Following Pullback (New)
    long_pullback = df["trend_long"] & (df["low"] <= df["ema_fast"]) & (df["close"] > df["ema_fast"]) & df["bull_engulf"]
    short_pullback = df["trend_short"] & (df["high"] >= df["ema_fast"]) & (df["close"] < df["ema_fast"]) & df["bear_engulf"]

    df["long_pullback"]   = long_pullback
    df["short_pullback"]  = short_pullback

    # Write all setup flags as columns so backtest loop can read them via row[field_name]
    df["long_sweep"]       = long_sweep
    df["short_sweep"]      = short_sweep
    df["long_vmr"]         = long_vmr
    df["short_vmr"]        = short_vmr
    df["long_mom"]         = long_mom
    df["short_mom"]        = short_mom
    df["long_trend_ride"]  = long_trend_ride
    df["short_trend_ride"] = short_trend_ride
    df["vol_ok"]           = vol_ok
    # Map bull/bear_engulf names to flag names expected by SetupFlags
    df["long_engulf"]      = long_engulf
    df["short_engulf"]     = short_engulf

    return df


# ── Single-bar setup detection (live use) ─────────────────────────────────────

def detect_setups_row(row: pd.Series, prev_row: pd.Series, config: BotConfig = default_cfg) -> SetupFlags:
    """
    Detect setups for the latest confirmed bar.
    `row`      — current bar (iloc[-1] after indicators)
    `prev_row` — previous bar (iloc[-2])
    """
    c = config

    flags = SetupFlags()
    flags.vol_ok = row["volume"] > row["vol_median"] * c.vol_mult

    # P4: ATR volatility regime gate
    atr_val = float(row.get("atr", 0))
    close_val = float(row["close"])
    atr_pct = (atr_val / close_val * 100) if close_val != 0 else 0
    vol_regime_ok = (
        not c.vol_gate_enabled
        or (c.min_atr_pct <= atr_pct <= c.max_atr_pct)
    )

    # P6: EMA spread anti-chop filter
    ema_spread_pct = (
        abs(float(row["ema_fast"]) - float(row["ema_slow"])) / close_val * 100
        if close_val != 0 else 0
    )
    not_choppy = not c.chop_filter_enabled or (ema_spread_pct >= c.min_ema_spread_pct)

    # P3: HTF trend flags (injected by caller via row if available)
    flags.htf_trend_long  = bool(row.get("htf_trend_long",  True)) if c.htf_filter else True
    flags.htf_trend_short = bool(row.get("htf_trend_short", True)) if c.htf_filter else True

    # P7: Pin-Bar Rejection Filter (New)
    rng = float(row["high"] - row["low"])
    if c.use_pinbar_filter and rng > 0:
        long_rejection = (float(row["close"]) - float(row["low"])) / rng >= c.pinbar_rejection_ratio
        short_rejection = (float(row["high"]) - float(row["close"])) / rng >= c.pinbar_rejection_ratio
    else:
        long_rejection = short_rejection = True

    # P8: ADX Trend Strength Filter (New)
    adx_val = float(row.get("adx", 0))
    if c.use_adx_filter and adx_val > 0:
        adx_strong = (c.min_adx <= adx_val <= c.max_adx)
    else:
        adx_strong = True

    # P9: Session Filter (New)
    if c.use_session_filter:
        h = row.name.hour
        session_ok = (c.session_start_utc <= h <= c.session_end_utc)
    else:
        session_ok = True

    # P11: RSI Exhaustion Filter (New)
    if c.use_rsi_exhaustion:
        rsi_long_ok = float(row["rsi"]) <= c.rsi_low
        rsi_short_ok = float(row["rsi"]) >= c.rsi_high
    else:
        rsi_long_ok = rsi_short_ok = True

    # P12: Candlestick Confirmation (Engulfing)
    flags.long_engulf = bool(row["bull_engulf"])
    flags.short_engulf = bool(row["bear_engulf"])

    # P10: Volume Surge Filter (Updated to Median)
    vol_surge = float(row["volume"]) >= (float(row["vol_median"]) * c.volume_surge_mult)

    # Sweep base conditions — requires vol_ok (0.8× avg) to filter dead-bar fakeouts
    long_sweep_base  = (row["low"] < row["prev_low"])  and (row["close"] > row["prev_low"])
    short_sweep_base = (row["high"] > row["prev_high"]) and (row["close"] < row["prev_high"])

    # P5: Sweep depth quality filter
    if c.sweep_depth_filter and atr_val > 0:
        long_depth_ok  = (float(row["prev_low"])  - float(row["low"]))  >= atr_val * c.min_sweep_depth_atr
        short_depth_ok = (float(row["high"]) - float(row["prev_high"])) >= atr_val * c.min_sweep_depth_atr
    else:
        long_depth_ok = short_depth_ok = True

    flags.long_sweep  = long_sweep_base  and long_depth_ok  and vol_regime_ok and not_choppy and flags.htf_trend_long and long_rejection and session_ok and vol_surge and rsi_long_ok
    flags.short_sweep = short_sweep_base and short_depth_ok and vol_regime_ok and not_choppy and flags.htf_trend_short and short_rejection and session_ok and vol_surge and rsi_short_ok

    # VWAP Mean Reversion
    dev_pct = abs((row["close"] - row["vwap"]) / row["vwap"]) * 100 if row["vwap"] != 0 else 0
    flags.long_vmr  = (
        (prev_row["close"] < prev_row["band_dn"])
        and (row["close"] > row["band_dn"])
        and (dev_pct >= c.min_dev_pct_vw)
        and vol_regime_ok and flags.htf_trend_long
    )
    flags.short_vmr = (
        (prev_row["close"] > prev_row["band_up"])
        and (row["close"] < row["band_up"])
        and (dev_pct >= c.min_dev_pct_vw)
        and vol_regime_ok and flags.htf_trend_short
    )

    # Momentum Pullback
    flags.long_mom  = (
        bool(row["trend_long"])
        and (prev_row["close"] <= prev_row["ema_fast"]) and (row["close"] > row["ema_fast"])
        and (row["low"] <= row["ema_fast"])
        and (row["rsi"] > 50)
        and vol_regime_ok and not_choppy and flags.htf_trend_long and adx_strong and session_ok and vol_surge
    )
    flags.short_mom = (
        bool(row["trend_short"])
        and (prev_row["close"] >= prev_row["ema_fast"]) and (row["close"] < row["ema_fast"])
        and (row["high"] >= row["ema_fast"])
        and (row["rsi"] < 50)
        and vol_regime_ok and not_choppy and flags.htf_trend_short and adx_strong and session_ok and vol_surge
    )

    # Trend Ride — EMA Touch: wicks into EMA20 and closes back (smooth trend, HTF direction only)
    flags.long_trend_ride = (
        (float(row["low"]) <= float(row["ema_fast"]))
        and (float(row["close"]) > float(row["ema_fast"]))
        and (float(prev_row["close"]) > float(prev_row["ema_fast"]))  # prev bar above EMA (pullback)
        and (float(row["rsi"]) > 40) and (float(row["rsi"]) < 70)
        and vol_regime_ok and not_choppy and flags.htf_trend_long and adx_strong
    )
    flags.short_trend_ride = (
        (float(row["high"]) >= float(row["ema_fast"]))
        and (float(row["close"]) < float(row["ema_fast"]))
        and (float(prev_row["close"]) < float(prev_row["ema_fast"]))  # prev bar below EMA (pullback)
        and (float(row["rsi"]) > 30) and (float(row["rsi"]) < 60)
        and vol_regime_ok and not_choppy and flags.htf_trend_short and adx_strong
    )

    return flags


# ── Setup selector (respects setups_opt and side_filter) ─────────────────────

def _select_setups(flags: SetupFlags, config: BotConfig):
    c = config
    opt = c.setups_opt

    want_long  = c.side_filter != "Short only"
    want_short = c.side_filter != "Long only"

    if opt == "All":
        sel_long  = flags.raw_long
        sel_short = flags.raw_short
    elif opt == "Sweep&Reversal":
        sel_long  = flags.long_sweep
        sel_short = flags.short_sweep
    elif opt == "VWAP Mean Revert":
        sel_long  = flags.long_vmr
        sel_short = flags.short_vmr
    elif opt == "Momentum Pullback":
        sel_long  = flags.long_mom
        sel_short = flags.short_mom
    elif opt == "Sweep+Momentum":
        sel_long  = flags.long_sweep or flags.long_mom or flags.long_bb
        sel_short = flags.short_sweep or flags.short_mom or flags.short_bb
    else:
        sel_long = sel_short = False

    return sel_long and want_long, sel_short and want_short


def get_current_threshold(row: pd.Series, config: BotConfig) -> float:
    base = config.auto_trade_threshold
    if config.dynamic_threshold_enabled and "vol_std" in row and not pd.isna(row["vol_std"]):
        # Increase threshold in high volatility (using factor * standard deviation)
        # Normalize vol_std: approx 0.001-0.01 for 15m crypto
        vol_factor = row["vol_std"] * 1000  # scale to human readable range
        return base + (config.volatility_threshold_factor * vol_factor)
    return base

# ── Scoring ───────────────────────────────────────────────────────────────────

def calc_score(
    row: pd.Series, 
    flags: SetupFlags, 
    is_long: bool, 
    config: BotConfig = default_cfg,
    obi: Optional[float] = None
) -> float:
    c = config

    if is_long:
        s_sweep       = c.w_sweep       if flags.long_sweep       else 0
        s_vmr         = c.w_vmr         if flags.long_vmr         else 0
        s_mom         = c.w_mom         if flags.long_mom         else 0
        s_trend_ride  = c.w_trend_ride  if flags.long_trend_ride  else 0
        s_bb          = 50              if flags.long_bb          else 0
        vol_bonus  = c.w_vol  if flags.long_sweep and flags.vol_ok else 0
        vwap_bonus = c.w_vwap if (row["vwap"] < row["close"] < row["band_up"]) else 0
        ema_bonus  = c.w_ema  if row["trend_long"] else 0
    else:
        s_sweep       = c.w_sweep       if flags.short_sweep       else 0
        s_vmr         = c.w_vmr         if flags.short_vmr         else 0
        s_mom         = c.w_mom         if flags.short_mom         else 0
        s_trend_ride  = c.w_trend_ride  if flags.short_trend_ride  else 0
        s_bb          = 50              if flags.short_bb          else 0
        vol_bonus  = c.w_vol  if flags.short_sweep and flags.vol_ok else 0
        vwap_bonus = c.w_vwap if (row["band_dn"] < row["close"] < row["vwap"]) else 0
        ema_bonus  = c.w_ema  if row["trend_short"] else 0

    # P1: Sum all active setup scores. Add confluence bonus when 2+ setups fire.
    s_pullback = 60 if (flags.long_pullback if is_long else flags.short_pullback) else 0
    s_engulf   = c.w_engulfing if (flags.long_engulf if is_long else flags.short_engulf) else 0
    
    setup_score = s_sweep + s_vmr + s_mom + s_trend_ride + s_bb + s_engulf + s_pullback
    active_setups = sum([bool(s_sweep), bool(s_vmr), bool(s_mom), bool(s_trend_ride), bool(s_bb), bool(s_engulf), bool(s_pullback)])
    
    # Elite Confluence Check
    if active_setups < c.min_setups_confluence:
        return 0.0

    # P2: Machine Learning Filter (Final Verdict)
    if c.use_ml_filter and _MODEL_INSTANCE:
        from ml_features import FEATURE_COLS
        # Use pre-calculated features from the row
        features = row[FEATURE_COLS].values.reshape(1, -1)
        
        # We use the calibrated model (.pkl) which returns real probabilities
        probs = _MODEL_INSTANCE.predict_proba(features)[0]
        prob_success = probs[1] # Class 1 is 'Win'
        
        if prob_success < c.ml_threshold:
            # logger.debug(f"ML Filter rejected: prob={prob_success:.2f} < threshold={c.ml_threshold}")
            return 0.0
        # logger.info(f"ML Filter approved: prob={prob_success:.2f}")

    # P3: Order Book Imbalance (OBI) Filter
    if c.use_obi_filter and obi is not None:
        # For long: OBI should be positive (more bids). For short: negative (more asks).
        obi_val = obi if is_long else -obi
        if obi_val < c.obi_threshold:
            # logger.debug(f"OBI Filter rejected: obi={obi_val:.2f} < threshold={c.obi_threshold}")
            return 0.0

    # P4: Liquidity Check (Volume USD)
    if "volume_usd" in row and row["volume_usd"] < c.min_volume_usd_15m:
        return 0.0

    confluence_bonus = c.w_confluence if active_setups >= 2 else 0
    total = setup_score + vol_bonus + vwap_bonus + ema_bonus + confluence_bonus
    return min(float(total), 100.0)


# ── Retest level ──────────────────────────────────────────────────────────────

def get_retest_level(is_long: bool, flags: SetupFlags, row: pd.Series) -> Optional[float]:
    """
    Returns the retest (limit entry) level for the given setup.
    Priority: Sweep > VMR > Mom (matches Pine getRetestLevelLong/Short).
    """
    if is_long:
        if flags.long_pullback:
            return float(row["ema_fast"])
        if flags.long_bb:
            return float(row["bb_dn"])
        if flags.long_sweep:
            return float(row["prev_low"])
        if flags.long_vmr:
            return float(row["band_dn"])
        if flags.long_mom:
            return float(row["ema_fast"])
        if flags.long_trend_ride:
            return float(row["ema_fast"])
    else:
        if flags.short_pullback:
            return float(row["ema_fast"])
        if flags.short_bb:
            return float(row["bb_up"])
        if flags.short_sweep:
            return float(row["prev_high"])
        if flags.short_vmr:
            return float(row["band_up"])
        if flags.short_mom:
            return float(row["ema_fast"])
        if flags.short_trend_ride:
            return float(row["ema_fast"])
    return None


# ── Adaptive cancel bars ──────────────────────────────────────────────────────

def calc_cancel_bars_dyn(
    close: float,
    retest_level: float,
    avg_tr: float,
    config: BotConfig = default_cfg,
) -> int:
    c = config
    if avg_tr <= 0 or math.isnan(avg_tr):
        exp_bars = 0.0
    else:
        dist = abs(close - retest_level)
        exp_bars = math.ceil(dist / avg_tr)
    raw = round(c.cancel_scale * exp_bars)
    return int(max(c.cancel_min, min(c.cancel_max, raw)))


# ── High-level signal builder ─────────────────────────────────────────────────

def get_signals(
    row: pd.Series, 
    prev_row: pd.Series, 
    config: BotConfig = default_cfg,
    obi: Optional[float] = None
) -> List[Signal]:
    flags = detect_setups_row(row, prev_row, config)

    # Apply setups_opt / side_filter — same filter used in backtest
    want_long, want_short = _select_setups(flags, config)

    # ── Long Signals ──────────────────────────────────────────────────────────
    score_l = calc_score(row, flags, is_long=True, config=config, obi=obi) if want_long else 0.0
    level_l = get_retest_level(True, flags, row) if want_long else None

    # ── Short Signals ─────────────────────────────────────────────────────────
    score_s = calc_score(row, flags, is_long=False, config=config, obi=obi) if want_short else 0.0
    level_s = get_retest_level(False, flags, row) if want_short else None

    res = []
    threshold = get_current_threshold(row, config)
    
    if level_l and score_l > 0:
        res.append(Signal("long", "Elite-ML", level_l, score_l, score_l >= threshold, obi=obi))
    if level_s and score_s > 0:
        res.append(Signal("short", "Elite-ML", level_s, score_s, score_s >= threshold, obi=obi))
    
    return res
