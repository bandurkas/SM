"""
backtest.py — Historical strategy simulator.
Drives TradeManager over historical DataFrame.
"""

import argparse
import logging
import os
from dataclasses import replace as dc_replace
from datetime import datetime, timezone
import pandas as pd

from config import BotConfig, cfg as default_cfg
from market_data import fetch_ohlcv, fetch_daily_ohlcv, calc_indicators, inject_htf_trend, get_atr_mult
from strategy_engine import detect_setups_df, _select_setups, get_retest_level, SetupFlags, calc_score, get_current_threshold
from trade_manager import TradeManager
from paper_trader import PaperExecutionEngine
from risk_engine import round_tick, calc_qty, get_stop_price

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def compute_metrics(paper: PaperExecutionEngine, balance_history: list, n_days: float, bars_per_day: int = 96) -> dict:
    summary = paper.summary()
    if summary["trades"] == 0:
        return summary
        
    # Add Sharpe and Drawdown
    b = pd.Series(balance_history)
    returns = b.pct_change().dropna()
    # Annualize correctly: bars_per_year = bars_per_day * 365
    bars_per_year = bars_per_day * 365
    sharpe = (returns.mean() / returns.std()) * (bars_per_year ** 0.5) if len(returns) > 1 and returns.std() != 0 else 0
    
    roll_max = b.cummax()
    drawdown = (roll_max - b) / roll_max * 100
    max_dd = drawdown.max()
    
    # Profit Factor
    df_trades = pd.read_csv(paper._log_path)
    gross_profit = df_trades[df_trades["pnl"] > 0]["pnl"].sum()
    gross_loss = abs(df_trades[df_trades["pnl"] <= 0]["pnl"].sum())
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else 0
    
    summary["sharpe"] = round(sharpe, 2)
    summary["max_drawdown"] = round(max_dd, 2)
    summary["profit_factor"] = profit_factor
    summary["trades_per_day"] = round(summary["trades"] / n_days, 2)
    return summary

def run_backtest(config: BotConfig, days: int = 30, csv_path: str = None) -> dict:
    from market_data import make_exchange
    
    if config.use_ml_filter:
        from strategy_engine import load_ml_model
        load_ml_model(config.model_path)

    if csv_path and os.path.exists(csv_path):
        print(f"Loading historical data from {csv_path}...")
        df = pd.read_csv(csv_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df.set_index("timestamp", inplace=True)
        # Filter by days (from end)
        bars_per_day = {"15m": 96, "1h": 24, "30m": 48}.get(config.timeframe, 96)
        needed_bars = days * bars_per_day
        if len(df) > needed_bars:
            df = df.iloc[-needed_bars:]
        daily_df = df.resample("1D").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()
        exchange = None
    else:
        exchange = make_exchange(config)
        # Bars needed: days × bars_per_day
        bars_per_day = {"15m": 96, "1h": 24, "30m": 48}.get(config.timeframe, 96)
        limit = days * bars_per_day + 200

        print(f"Fetching {days} days of {config.timeframe} data from HTX…")
        df = fetch_ohlcv(exchange, config.symbol, config.timeframe, limit=limit)
        daily_df = fetch_daily_ohlcv(exchange, config.symbol, limit=days + 5)

    if df.empty or daily_df.empty:
        logger.error("Failed to fetch data for backtest.")
        return {}

    df = calc_indicators(df, config)

    # Inject HTF trend
    htf_df_hist = None
    if config.htf_filter:
        if csv_path:
            # Approximate HTF from intraday if using CSV
            htf_df_hist = df.resample(config.htf_timeframe).agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()
        else:
            htf_bars = days * 24 + 200
            htf_df_hist = fetch_ohlcv(exchange, config.symbol, config.htf_timeframe, limit=htf_bars)
    
    df = inject_htf_trend(df, exchange, config, htf_df=htf_df_hist)
    df = detect_setups_df(df, config)
    df.dropna(subset=["atr", "ema_fast", "ema_slow", "vwap"], inplace=True)

    print(f"Running backtest on {len(df)} bars…")

    paper = PaperExecutionEngine(config, log_path="backtest_trades.csv")
    # Isolated data_dir so backtest never writes to live bot_state.db
    bt_config = dc_replace(config, data_dir=os.path.join(config.data_dir, "backtest_run"))
    os.makedirs(bt_config.data_dir, exist_ok=True)
    manager = TradeManager(paper, bt_config)
    
    balance_history = [config.init_dep]
    
    for i in range(len(df)):
        row = df.iloc[i]
        bar_index = i
        
        # 1. P8: Reset daily stats at midnight
        ts = row.name
        
        # 2. Simulate execution (fills) against the current bar
        paper.simulate_bar(row)
        
        # 3. Drive the TradeManager
        manager.update(row, bar_index)
        
        # 4. Check for new signals if flat
        if manager.is_flat:
            # Check circuit breakers (daily loss/trades/consecutive losses)
            if paper.daily_loss_limit_hit() or paper.consecutive_loss_limit_hit():
                continue
                
            flags = SetupFlags()
            for field_name in flags.__dict__.keys():
                if field_name in row:
                    setattr(flags, field_name, bool(row[field_name]))
            
            sig_long, sig_short = _select_setups(flags, config)
            if sig_long or sig_short:
                side = "long" if sig_long else "short"
                
                score = calc_score(row, flags, sig_long, config)
                
                # Diagnostic: If score is high but ML filter might kill it
                if config.use_ml_filter:
                    from strategy_engine import _MODEL_INSTANCE
                    from ml_features import FEATURE_COLS
                    if _MODEL_INSTANCE:
                        features = row[FEATURE_COLS].values.reshape(1, -1)
                        probs = _MODEL_INSTANCE.predict_proba(features)[0]
                        prob_success = probs[1]
                        logger.info(f"SIGNAL: {row.name} | Score: {score:.1f} | ML Prob: {prob_success:.4f}")
                
                threshold = get_current_threshold(row, config)
                if score >= threshold:
                    # Risk/Entry params
                    retest = get_retest_level(sig_long, flags, row)
                    if retest is not None:
                        atr_mult = get_atr_mult(config.timeframe, config)
                        stop = get_stop_price(
                            is_long=sig_long, entry=retest, atr=row["atr"],
                            swing_low=row["swing_low"], swing_high=row["swing_high"],
                            prev_low=row["prev_low"], band_dn=row["band_dn"], pdl=row["pdl"],
                            prev_high=row["prev_high"], band_up=row["band_up"], pdh=row["pdh"],
                            config=config, atr_mult=atr_mult
                        )
                        risk = abs(retest - stop)
                        if risk > 0:
                            qty = calc_qty(retest, stop, config, paper.get_balance())
                            
                            # TP levels
                            tp1 = round_tick(retest + risk * config.r_tp1 if sig_long else retest - risk * config.r_tp1, config.tick_size)
                            tp2 = round_tick(retest + risk * config.r_tp2 if sig_long else retest - risk * config.r_tp2, config.tick_size)
                            tp3 = round_tick(retest + risk * config.r_tp3 if sig_long else retest - risk * config.r_tp3, config.tick_size)
                            
                            expiry = bar_index + config.cancel_bars # Simplified expiry
                            
                            manager.open_pending(side, retest, stop, tp1, tp2, tp3, qty, expiry, score)

        balance_history.append(paper.get_balance())

    n_days = len(df) / bars_per_day
    metrics = compute_metrics(paper, balance_history, n_days, bars_per_day=bars_per_day)
    return metrics

def main():
    parser = argparse.ArgumentParser(description="ETH Scalper Backtest (Integrated)")
    parser.add_argument("--days", type=int, default=30, help="History days to test")
    parser.add_argument("--timeframe", type=str, default="15m")
    parser.add_argument("--threshold", type=int, default=35)
    parser.add_argument("--csv", type=str, help="Path to historical CSV data")
    args = parser.parse_args()

    config = BotConfig(timeframe=args.timeframe, auto_trade_threshold=args.threshold)
    results = run_backtest(config, days=args.days, csv_path=args.csv)

    print("\n── Backtest Results ──────────────────────────────")
    for k, v in results.items():
        print(f"  {k:<18} {v}")
    print("─────────────────────────────────────────────────")

if __name__ == "__main__":
    main()
