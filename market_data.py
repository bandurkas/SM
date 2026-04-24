"""
market_data.py — OHLCV fetching and indicator calculation.

All indicator logic mirrors the Pine Script in context.md:
  - EMA fast/slow
  - VWAP with stdev bands (rolling, NOT session-reset — matches Pine ta.vwap behaviour
    which resets per session; for 15m intraday we approximate with daily reset)
  - ATR
  - RSI
  - Volume SMA
  - Prev high/low (sweep range)
  - Previous day high/low (PDH/PDL)
  - Pivot high/low (3-bar)
"""

import os
from typing import Optional, Tuple, List
import ccxt
import logging
import time
import numpy as np
import pandas as pd
from dotenv import load_dotenv

from config import BotConfig, cfg as default_cfg

load_dotenv()


# ── Exchange factory ──────────────────────────────────────────────────────────

def make_exchange(config: BotConfig = default_cfg) -> ccxt.htx:
    exchange = ccxt.htx({
        "apiKey": os.getenv("HTX_API_KEY", ""),
        "secret": os.getenv("HTX_API_SECRET", ""),
        "timeout": 10000,               # P0 FIX: 10s timeout for all network requests
        "options": {
            "defaultType": "swap",
            "unified": True,            # HTX unified account (new account type)
        },
    })
    if os.getenv("HTX_TESTNET", "false").lower() == "true":
        exchange.set_sandbox_mode(True)
    exchange.load_markets()
    return exchange


# ── OHLCV fetch ───────────────────────────────────────────────────────────────

import logging

logger = logging.getLogger(__name__)

def fetch_ohlcv(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    limit: int = 300,
) -> pd.DataFrame:
    """Fetch OHLCV bars and return as DataFrame, paginating if limit > 1000."""
    max_chunk = 1000
    if limit <= max_chunk:
        raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    else:
        raw = []
        tf_ms = exchange.parse_timeframe(timeframe) * 1000
        now = exchange.milliseconds()
        since = now - (limit * tf_ms)
        
        while len(raw) < limit:
            chunk_limit = min(max_chunk, limit - len(raw))
            try:
                chunk = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=chunk_limit)
                print(f"  [DEBUG] Fetched {len(chunk)} bars since {since}")
                if not chunk:
                    break
                # Only add new bars to avoid duplicates if since lands exactly on the last bar
                if raw and chunk[0][0] <= raw[-1][0]:
                    chunk = [c for c in chunk if c[0] > raw[-1][0]]
                    if not chunk:
                        print("  [DEBUG] No new bars in chunk")
                        break
                raw.extend(chunk)
                since = raw[-1][0] + tf_ms
                print(f"  [DEBUG] Total raw: {len(raw)}, next since: {since}")
            except Exception as exc:
                logger.error(f"Error fetching OHLCV chunk: {exc}")
                break

    df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.set_index("timestamp", inplace=True)
    df = df.astype(float)
    return df


def fetch_daily_ohlcv(exchange: ccxt.Exchange, symbol: str, limit: int = 5) -> pd.DataFrame:
    """Fetch daily bars for PDH/PDL."""
    return fetch_ohlcv(exchange, symbol, "1d", limit=limit)


def check_clock_drift(exchange: ccxt.Exchange) -> float:
    """Compare local time with exchange time. Returns drift in seconds."""
    try:
        ex_time = exchange.fetch_time()
        local_time = int(time.time() * 1000)
        drift_ms = local_time - ex_time
        drift_sec = drift_ms / 1000.0

        if abs(drift_sec) > 5.0:
            logger.warning(
                f"CRITICAL CLOCK DRIFT: Local time is {drift_sec:.2f}s "
                f"{'ahead of' if drift_sec > 0 else 'behind'} exchange. "
                "This may cause API failures!"
            )
        else:
            logger.info(f"Clock sync OK (drift: {drift_sec:.3f}s)")
        return drift_sec
    except Exception as e:
        logger.error(f"Failed to check clock drift: {e}")
        return 0.0


# ── Indicator helpers ─────────────────────────────────────────────────────────

def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _atr(df: pd.DataFrame, length: int) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(length).mean()


def _rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(length).mean()
    loss = (-delta.clip(upper=0)).rolling(length).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _cci(df: pd.DataFrame, length: int) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3
    ma = tp.rolling(length).mean()
    md = tp.rolling(length).apply(lambda x: np.abs(x - x.mean()).mean())
    return (tp - ma) / (0.015 * md)


def _vwap_with_bands(df: pd.DataFrame, dev_len: int, sigma_k: float):
    """
    Rolling VWAP that resets each calendar day (UTC), matching Pine's ta.vwap.
    Returns (vwap, band_up, band_dn, price_dev, stdev_dev) as Series.
    """
    hlc3 = (df["high"] + df["low"] + df["close"]) / 3
    vol = df["volume"]

    # Day group identifier
    day = df.index.floor("D")

    cum_pv = (hlc3 * vol).groupby(day).cumsum()
    cum_v = vol.groupby(day).cumsum()
    vwap = cum_pv / cum_v

    price_dev = df["close"] - vwap
    stdev_dev = price_dev.rolling(dev_len).std()
    band_up = vwap + sigma_k * stdev_dev
    band_dn = vwap - sigma_k * stdev_dev

    return vwap, band_up, band_dn, price_dev, stdev_dev


def _pivot_high(high: pd.Series, left: int = 3, right: int = 3) -> pd.Series:
    """Returns pivot high value at each bar where pivot was confirmed (right bars ago)."""
    result = pd.Series(np.nan, index=high.index)
    arr = high.values
    for i in range(left, len(arr) - right):
        window = arr[i - left : i + right + 1]
        if arr[i] == max(window):
            result.iloc[i] = arr[i]
    return result


def _bb(close: pd.Series, length: int, std: float) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Bollinger Bands."""
    sma = close.rolling(length).mean()
    sd = close.rolling(length).std()
    upper = sma + (sd * std)
    lower = sma - (sd * std)
    return sma, upper, lower

def _pivot_low(low: pd.Series, left: int = 3, right: int = 3) -> pd.Series:
    result = pd.Series(np.nan, index=low.index)
    arr = low.values
    for i in range(left, len(arr) - right):
        window = arr[i - left : i + right + 1]
        if arr[i] == min(window):
            result.iloc[i] = arr[i]
    return result
def _adx(df: pd.DataFrame, length: int) -> pd.Series:
    """Average Directional Index."""
    high, low, close = df["high"], df["low"], df["close"]
    up = high.diff()
    down = -low.diff()
    
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)
    
    atr = tr.rolling(length).mean()
    
    plus_dm = np.where((up > down) & (up > 0), up, 0)
    minus_dm = np.where((down > up) & (down > 0), down, 0)
    
    plus_di = 100 * (pd.Series(plus_dm, index=df.index).rolling(length).mean() / atr)
    minus_di = 100 * (pd.Series(minus_dm, index=df.index).rolling(length).mean() / atr)
    
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    adx = dx.rolling(length).mean()
    return adx

def _engulfing(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """Detects Bullish and Bearish Engulfing patterns."""
    curr_open, curr_close = df["open"], df["close"]
    prev_open, prev_close = df["open"].shift(1), df["close"].shift(1)
    
    bullish = (curr_close > curr_open) & (prev_close < prev_open) & \
              (curr_close >= prev_open) & (curr_open <= prev_close)
              
    bearish = (curr_close < curr_open) & (prev_close > prev_open) & \
              (curr_close <= prev_open) & (curr_open >= prev_close)
    
    return bullish, bearish

# ── Main indicator calculation ────────────────────────────────────────────────

def calc_indicators(df: pd.DataFrame, config: BotConfig = default_cfg, pdh: float = None, pdl: float = None) -> pd.DataFrame:
    """
    Adds all indicator columns to df in-place and returns it.
    pdh/pdl: previous day high/low (pass from daily fetch).
    """
    c = config

    df["ema_fast"] = _ema(df["close"], c.ema_fast_len)
    df["ema_slow"] = _ema(df["close"], c.ema_slow_len)
    
    # ATR & Volatility
    df["atr"] = _atr(df, c.atr_len)
    # Volatility standard deviation for dynamic thresholds
    df["vol_std"] = df["close"].pct_change().rolling(20).std()
    # Liquidity (Volume in USD)
    df["volume_usd"] = df["volume"] * df["close"]
    
    # RSI
    df["ema_200"] = _ema(df["close"], 200)
    df["trend_long"] = (df["ema_fast"] > df["ema_slow"]) & (df["close"] > df["ema_200"])
    df["trend_short"] = (df["ema_fast"] < df["ema_slow"]) & (df["close"] < df["ema_200"])

    df["vwap"], df["band_up"], df["band_dn"], df["price_dev"], df["stdev_dev"] = _vwap_with_bands(
        df, c.dev_len, c.sigma_k
    )

    df["bb_mid"], df["bb_up"], df["bb_dn"] = _bb(df["close"], 20, 2.0)

    df["atr"] = _atr(df, c.atr_len)
    df["rsi"] = _rsi(df["close"], c.rsi_len)
    df["adx"] = _adx(df, c.adx_period)
    df["cci"] = _cci(df, 20)
    df["vol_median"] = df["volume"].rolling(48).median()
    df["bull_engulf"], df["bear_engulf"] = _engulfing(df)

    # Sweep range: previous bar's rolling high/low
    df["prev_high"] = df["high"].shift(1).rolling(c.sweep_len).max()
    df["prev_low"] = df["low"].shift(1).rolling(c.sweep_len).min()

    # Previous day high/low (scalar, injected from daily fetch)
    df["pdh"] = pdh if pdh is not None else np.nan
    df["pdl"] = pdl if pdl is not None else np.nan

    # Swing pivots (3-bar) — last known value propagated forward
    ph_raw = _pivot_high(df["high"], 3, 3)
    pl_raw = _pivot_low(df["low"], 3, 3)
    df["swing_high"] = ph_raw.ffill()
    df["swing_low"] = pl_raw.ffill()

    return df


# ── P3: HTF trend indicator injection ────────────────────────────────────────

def inject_htf_trend(
    df: pd.DataFrame,
    exchange,
    config: BotConfig = default_cfg,
    htf_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Fetch HTF (default 1h) bars, compute EMA fast/slow, and inject
    htf_trend_long / htf_trend_short columns into the intraday df.

    Each intraday bar gets the HTF trend of the most recently closed HTF bar
    (forward-fill, no lookahead).
    """
    if not config.htf_filter:
        df["htf_trend_long"]  = True
        df["htf_trend_short"] = True
        return df

    # Default when HTF data unavailable: STRICT=False blocks entries, LENIENT=True allows
    htf_missing_default = bool(getattr(config, "htf_missing_allow_trading", False))

    try:
        if htf_df is None:
            htf_df = fetch_ohlcv(exchange, config.symbol, config.htf_timeframe, limit=200)
        if htf_df.empty:
            raise ValueError("Empty HTF DataFrame")
        htf_df["htf_ema_fast"] = _ema(htf_df["close"], config.htf_ema_fast_len)
        htf_df["htf_ema_slow"] = _ema(htf_df["close"], config.htf_ema_slow_len)
        htf_df["htf_trend_long"]  = htf_df["htf_ema_fast"] > htf_df["htf_ema_slow"]
        htf_df["htf_trend_short"] = htf_df["htf_ema_fast"] < htf_df["htf_ema_slow"]

        # Reindex to intraday timestamps via forward-fill (no lookahead)
        htf_trend = htf_df[["htf_trend_long", "htf_trend_short"]]
        combined = df.join(htf_trend, how="left")
        combined["htf_trend_long"]  = combined["htf_trend_long"].ffill().fillna(htf_missing_default).astype(bool)
        combined["htf_trend_short"] = combined["htf_trend_short"].ffill().fillna(htf_missing_default).astype(bool)

        df["htf_trend_long"]  = combined["htf_trend_long"]
        df["htf_trend_short"] = combined["htf_trend_short"]
    except Exception as exc:
        logger.warning(f"HTF trend fetch failed ({exc}) — htf_missing_allow_trading={htf_missing_default}")
        df["htf_trend_long"]  = htf_missing_default
        df["htf_trend_short"] = htf_missing_default

    return df


# ── ATR multiplier selector (matches Pine getTimeTF) ─────────────────────────

def get_atr_mult(timeframe: str, config: BotConfig = default_cfg) -> float:
    if timeframe == "15m":
        return config.atr_mult_15m
    if timeframe in ("1h", "60m"):
        return config.atr_mult_1h
    return config.atr_mult_15m
