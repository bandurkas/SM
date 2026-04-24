"""
ml_features.py — Feature engineering for the CatBoost sniper model.
"""

import pandas as pd
import numpy as np

# Core technical features
TECHNICAL_FEATURES = [
    "rsi", "adx", "cci", "atr", "vol_std", "volume_usd",
    "bb_width", "vwap_dist", "ema_200_dist"
]

# Statistical features (Skewness, Kurtosis)
STATS_FEATURES = ["rolling_skew", "rolling_kurt"]

# Time features
TIME_FEATURES = ["hour", "day_of_week"]

FEATURE_COLS = TECHNICAL_FEATURES + STATS_FEATURES + TIME_FEATURES

def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates all features needed for the ML model."""
    df = df.copy()
    
    # 1. Distances from key levels (normalized)
    df["vwap_dist"] = (df["close"] - df["vwap"]) / df["vwap"]
    df["ema_200_dist"] = (df["close"] - df["ema_200"]) / df["ema_200"]
    df["bb_width"] = (df["bb_up"] - df["bb_dn"]) / df["close"]
    
    # 2. Statistical Moments (Window=20)
    df["rolling_skew"] = df["close"].pct_change().rolling(20).skew()
    df["rolling_kurt"] = df["close"].pct_change().rolling(20).kurt()
    
    # 3. Time Features
    df["hour"] = df.index.hour
    df["day_of_week"] = df.index.dayofweek
    
    # 4. Cleanup
    df = df.replace([np.inf, -np.inf], np.nan)
    return df
