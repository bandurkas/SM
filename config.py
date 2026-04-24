import os
from dataclasses import dataclass, field

_HERE = os.path.dirname(os.path.abspath(__file__))


@dataclass
class BotConfig:
    # ── Paths & Observability ─────────────────────────────────────────────────
    data_dir: str = field(default_factory=lambda: os.getenv("ETHBOT_DATA_DIR", "./data"))

    # ── Exchange ──────────────────────────────────────────────────────────────
    symbol: str = "ETH/USDT:USDT"       # CCXT unified symbol (perpetual swap)
    timeframe: str = "15m"
    ohlcv_limit: int = 500
 
    # ── Entry ─────────────────────────────────────────────────────────────────
    entry_mode: str = "bar_close"
    setups_opt: str = "Sweep+Momentum"             # "All" | "Sweep&Reversal" | "VWAP Mean Revert" | "Momentum Pullback"
    side_filter: str = "Both"
    cancel_bars: int = 7                # Static pending order lifetime (bars)
    limit_offset_ticks: int = 0         # Limit price offset (ticks) from retest level
    close_only: bool = True             # Only fire signals on confirmed (closed) bars

    # ── Adaptive cancel ───────────────────────────────────────────────────────
    adaptive_cancel: bool = True
    cancel_tr_len: int = 14             # ATR/TR length for adaptive cancel
    cancel_scale: float = 1.5
    cancel_min: int = 2
    cancel_max: int = 10

    # ── Risk ──────────────────────────────────────────────────────────────────
    init_dep: float = 318.10              # Deposit (USDT) — sync to real balance 2026-04-24
    risk_pct: float = 0.4               # Deprecated: use risk_per_trade_pct
    risk_per_trade_pct: float = 0.4     # Risk per trade (%)
    leverage: int = 10
    qty_step: float = 0.01              # Minimum lot size increment (1 HTX contract = 0.01 ETH)

    # ── Stop ─────────────────────────────────────────────────────────────────
    sl_mode: str = "Hybrid"             # "ATR" | "Swing" | "Hybrid"
    atr_len: int = 14
    atr_mult_15m: float = 1.2           # Golden Standard SL
    atr_mult_1h: float = 1.5
    pad_swing_atr: float = 0.2          # Swing stop padding (× ATR)
    stop_buffer_ticks: int = 4
    use_liq_stop: bool = True           # Push stop beyond nearest liquidity level
    liq_pad_ticks: int = 3
    stop_cap_atr_mult: float = 2.5      # Max stop distance (× ATR)

    # ── Take profit ───────────────────────────────────────────────────────────
    r_tp1: float = 0.45                 # Ultra Sniper TP1 (High R:R)
    r_tp2: float = 1.0
    r_tp3: float = 1.5

    # ── Indicators ────────────────────────────────────────────────────────────
    ema_fast_len: int = 20
    ema_slow_len: int = 50
    dev_len: int = 100                  # VWAP stdev window
    sigma_k: float = 1.0               # VWAP band width (sigmas)
    vol_mult: float = 1.0              # Volume multiplier for sweep detection
    sweep_len: int = 15                 # Lookback bars for range high/low
    min_dev_pct_vw: float = 0.08       # Min deviation from VWAP (%) for VMR

    # ── Scoring weights ───────────────────────────────────────────────────────
    w_sweep: int = 40
    w_vmr: int = 40
    w_mom: int = 40
    w_trend_ride: int = 35              # EMA Touch: bar wicks into EMA20 and closes above in uptrend
    w_vol: int = 15
    w_vwap: int = 10
    w_ema: int = 10
    # Core Strategy
    auto_trade_threshold: float = 70.0
    w_engulfing: int = 40
    min_setups_confluence: int = 1

    # ── P1: Scoring — confluence bonus ────────────────────────────────────────
    w_confluence: int = 20              # Bonus when 2+ setups fire on the same bar

    # ── P2: Partial closes ────────────────────────────────────────────────────
    partial_close_tp1_pct: float = 0.30   # Maximize runner potential
    partial_close_tp2_pct: float = 0.00

    # ── P3: Higher-timeframe (HTF) trend filter ───────────────────────────────
    htf_filter: bool = True
    htf_timeframe: str = "4h"
    htf_ema_fast_len: int = 20
    htf_ema_slow_len: int = 50
    htf_missing_allow_trading: bool = False  # STRICT: block entries when HTF data unavailable

    # ── P4: ATR volatility regime gate ────────────────────────────────────────
    vol_gate_enabled: bool = True
    min_atr_pct: float = 0.05           # Skip signals when ATR/close < this (dead market)
    max_atr_pct: float = 1.50           # Skip signals when ATR/close > this (news/cascade)

    # ── P5: Sweep depth quality filter ───────────────────────────────────────
    sweep_depth_filter: bool = True
    min_sweep_depth_atr: float = 0.05   # Sweep must extend >= 0.10 × ATR beyond range

    # ── P6: EMA spread anti-chop filter ──────────────────────────────────────
    chop_filter_enabled: bool = True
    min_ema_spread_pct: float = 0.005   # |(ema_fast - ema_slow)| / close must exceed this

    # ── P7: Dynamic ATR trailing stop (Chandelier Exit) ───────────────────────
    use_dynamic_atr_trailing: bool = True
    trail_atr_mult: float = 1.5         # Trail stop by N × ATR from highest high / lowest low
    trail_min_atr_move: float = 0.5     # Min stop improvement (× ATR) before placing new order

    # ── P8: Daily loss circuit breaker ───────────────────────────────────────
    daily_loss_limit_pct: float = 10.0  # Pause trading when daily loss >= this % of deposit
    max_trades_per_day: int = 8         # Hard cap on daily trade count

    # ── P9: Funding rate session blackout ─────────────────────────────────────
    funding_blackout_enabled: bool = True
    funding_blackout_mins: int = 8      # Silence signals N minutes around funding (0,8,16 UTC)

    # ── Commission (for P&L simulation) ──────────────────────────────────────
    maker_fee_pct: float = 0.02         # 0.02%
    taker_fee_pct: float = 0.055        # 0.055%

    # ── Misc ──────────────────────────────────────────────────────────────────
    tick_size: float = 0.01             # ETH/USDT min price increment
    rsi_len: int = 14
    vol_sma_len: int = 20               # Volume SMA length for sweep check

    # ── Smart DCA & Grid ──────────────────────────────────────────────────────
    use_dca: bool = True
    dca_max_levels: int = 4              # Total entries including the first one
    dca_martingale_factor: float = 1.00   # Linear averaging (safer)
    dca_step_multiplier: float = 1.1     # Multiplier for distance between levels
    dca_base_step_atr: float = 1.0       # Base distance to next DCA (x ATR)

    # Safety & Risk
    max_total_exposure_usd: float = 2000.0   # Hard cap on position size
    max_api_retries: int = 3                # For critical path operations
    qty_dust_threshold: float = 0.005       # Ignore positions smaller than this (in ETH)

    # ── Range Scalper ─────────────────────────────────────────────────────────
    use_range_scalper: bool = True
    rs_profit_target_atr: float = 0.4    # Take profit for internal scalps (x ATR)
    rs_min_size_pct: float = 0.2         # Min portion of position to scalp

    # Advanced Filters (Sniper Mode)
    use_adx_filter: bool = False
    adx_period: int = 14
    
    use_rsi_exhaustion: bool = False     # Only Long if RSI was < 30, Short if > 70
    rsi_low: float = 30.0
    rsi_high: float = 70.0
    
    use_pinbar_filter: bool = False
    pinbar_rejection_ratio: float = 0.6
    
    use_volume_surge: bool = False
    volume_surge_mult: float = 1.5       # Volume must be 1.5x SMA
    
    use_session_filter: bool = False
    session_start_utc: int = 8           # London Open (08:00 UTC)
    session_end_utc: int = 21            # NY Close (21:00 UTC)


    # ── Machine Learning ──────────────────────────────────────────────────────
    use_ml_filter: bool = True
    model_path: str = field(default_factory=lambda: os.path.join(_HERE, "sniper_model.cbm"))
    ml_threshold: float = 0.75

    # ── Dynamic threshold ─────────────────────────────────────────────────────
    dynamic_threshold_enabled: bool = False  # Adjust threshold based on vol_std
    min_volume_usd_15m: float = 0.0          # Min bar volume in USD (0 = disabled)

    # ── Order Book Imbalance (OBI) ────────────────────────────────────────────
    use_obi_filter: bool = True
    obi_threshold: float = 0.7          # Imbalance must be > 70%

    # ── Consecutive loss circuit breaker ─────────────────────────────────────
    max_consecutive_losses: int = 5     # Pause after N consecutive losing trades


# Singleton default config — import and override fields as needed
cfg = BotConfig()
