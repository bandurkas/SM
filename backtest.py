import os
import pandas as pd
import numpy as np
from datetime import datetime
from agents.structure_agent import StructureAgent
from agents.liquidity_agent import LiquidityAgent
from agents.zone_agent import ZoneAgent
from agents.timing_agent import TimingAgent

# Improved path handling
DEFAULT_DATA_PATH = "data/eth_history_1y.csv"
DATA_PATH = os.getenv('BACKTEST_DATA_PATH', DEFAULT_DATA_PATH)

def resample_data(df, timeframe):
    """Resamples OHLCV data to higher timeframes."""
    resampled = df.resample(timeframe, on='timestamp').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    return resampled

def analyze_trend(df):
    if df is None or len(df) < 10: return "Neutral"
    last_close = df['close'].iloc[-1]
    ema_20 = df['close'].rolling(20).mean().iloc[-1]
    if last_close > ema_20: return "Bullish"
    if last_close < ema_20: return "Bearish"
    return "Neutral"

def run_backtest():
    if not os.path.exists(DATA_PATH):
        print(f"❌ Data file not found at {DATA_PATH}. Please check the path.")
        return

    print(f"⌛ Loading historical data from {DATA_PATH}...")
    df_full = pd.read_csv(DATA_PATH)
    df_full['timestamp'] = pd.to_datetime(df_full['timestamp'])
    
    # Pre-calculate HTF data to avoid O(n^2) resampling in the loop
    print("🔄 Pre-calculating High Timeframes...")
    df_1h_all = resample_data(df_full, '1h')
    df_4h_all = resample_data(df_full, '4h')
    df_1d_all = resample_data(df_full, '1D')

    # Initialize agents
    structure_agent = StructureAgent()
    liquidity_agent = LiquidityAgent()
    zone_agent = ZoneAgent()
    timing_agent = TimingAgent()
    
    results = []
    limit = 3000
    start_idx = len(df_full) - limit
    
    print(f"📈 Running backtest loop for {limit} candles...")
    for i in range(start_idx, len(df_full)):
        current_time = df_full['timestamp'].iloc[i]
        df_15m_slice = df_full.iloc[:i+1]
        
        # Get HTF snapshots valid at current_time
        df_1h = df_1h_all[df_1h_all.index <= current_time]
        df_4h = df_4h_all[df_4h_all.index <= current_time]
        df_1d = df_1d_all[df_1d_all.index <= current_time]
        
        total_score = 0
        factors = []
        
        # 1. HTF Trend Alignment
        htf_trend = analyze_trend(df_4h)
        daily_trend = analyze_trend(df_1d)
        if htf_trend == daily_trend and htf_trend != "Neutral":
            total_score += 2
            factors.append(f"HTF Alignment ({htf_trend})")
            
        # 2. LTF Agents (15m)
        df_with_swings = structure_agent.identify_swings(df_15m_slice.tail(50))
        score_struct, details_struct = structure_agent.get_signal(df_with_swings)
        score_liq, details_liq = liquidity_agent.get_signal(df_with_swings)
        score_zone, details_zone = zone_agent.get_signal(df_with_swings.tail(10))
        score_time, details_time = timing_agent.get_signal(df_with_swings.tail(1), current_time)
        
        total_score += (score_struct + score_liq + score_zone + score_time)
        all_details = factors + details_struct + details_liq + details_zone + details_time
        
        if total_score >= 5:
            results.append({
                'time': current_time,
                'price': df_full['close'].iloc[i],
                'score': total_score,
                'signals': ", ".join(all_details)
            })

    if results:
        print("\n🔥 HIGH PROBABILITY SETUPS DETECTED 🔥")
        report_df = pd.DataFrame(results)
        stats = report_df['score'].value_counts().sort_index(ascending=False)
        print("\n📈 SIGNAL STATISTICS:")
        for score, count in stats.items():
            desc = "IDEAL" if score >= 7 else "STRONG" if score >= 5 else "GOOD"
            print(f"  Score {score} [{desc}]: {count} occurrences")
            
        print("\n" + report_df.tail(20).to_string(index=False))
        report_df.to_csv("backtest_results.csv", index=False)
        print(f"\n✅ Backtest finished. Total setups: {len(results)}")
    else:
        print("\n❌ No high-score setups detected.")

if __name__ == "__main__":
    run_backtest()

if __name__ == "__main__":
    run_backtest()
