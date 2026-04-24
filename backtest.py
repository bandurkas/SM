import pandas as pd
import numpy as np
from datetime import datetime
from agents.structure_agent import StructureAgent
from agents.liquidity_agent import LiquidityAgent
from agents.zone_agent import ZoneAgent
from agents.timing_agent import TimingAgent

# Path to the data file identified in the previous session
DATA_PATH = "/Users/styserg/.gemini/antigravity/playground/void-chromosphere/ethbot_local/data/eth_history_1y.csv"

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
    print(f"⌛ Loading historical data from {DATA_PATH}...")
    df_15m = pd.read_csv(DATA_PATH)
    df_15m['timestamp'] = pd.to_datetime(df_15m['timestamp'])
    
    # Initialize agents
    structure_agent = StructureAgent()
    liquidity_agent = LiquidityAgent()
    zone_agent = ZoneAgent()
    timing_agent = TimingAgent()
    
    results = []
    
    # We start from candle 100 to have enough history for analysis
    # For speed, we will process every 4th candle (1 hour step) or process a window
    print("📈 Running backtest loop...")
    
    # Let's test the last 3000 candles (~1 month)
    limit = 3000
    for i in range(len(df_15m) - limit, len(df_15m)):
        current_data_15m = df_15m.iloc[:i+1]
        current_time = current_data_15m['timestamp'].iloc[-1]
        
        # Resample for HTF
        df_1h = resample_data(current_data_15m, '1h')
        df_4h = resample_data(current_data_15m, '4h')
        df_1d = resample_data(current_data_15m, '1D')
        
        total_score = 0
        factors = []
        
        # 1. HTF Trend Alignment
        htf_trend = analyze_trend(df_4h)
        daily_trend = analyze_trend(df_1d)
        if htf_trend == daily_trend and htf_trend != "Neutral":
            total_score += 2
            factors.append(f"HTF Alignment ({htf_trend})")
            
        # 2. LTF Agents (15m)
        df_with_swings = structure_agent.identify_swings(current_data_15m.tail(50))
        score_struct, details_struct = structure_agent.get_signal(df_with_swings)
        score_liq, details_liq = liquidity_agent.get_signal(df_with_swings)
        score_zone, details_zone = zone_agent.get_signal(df_with_swings.tail(10))
        score_time, details_time = timing_agent.get_signal(df_with_swings.tail(1), current_time)
        
        total_score += (score_struct + score_liq + score_zone + score_time)
        all_details = factors + details_struct + details_liq + details_zone + details_time
        
        if total_score >= 5: # Ideal setup threshold
            results.append({
                'time': current_time,
                'price': current_data_15m['close'].iloc[-1],
                'score': total_score,
                'signals': ", ".join(all_details)
            })

    # Output results
    if results:
        print("\n🔥 HIGH PROBABILITY SETUPS DETECTED OVER 30 DAYS 🔥")
        report_df = pd.DataFrame(results)
        # Statistics
        stats = report_df['score'].value_counts().sort_index(ascending=False)
        print("\n📈 SIGNAL STATISTICS (Last Month):")
        for score, count in stats.items():
            desc = "IDEAL" if score >= 7 else "STRONG" if score >= 5 else "GOOD"
            print(f"  Score {score} [{desc}]: {count} occurrences")
            
        print("\n" + report_df.tail(20).to_string(index=False))
        report_df.to_csv("backtest_results.csv", index=False)
        print(f"\n✅ Monthly backtest finished. Total setups: {len(results)}")
    else:
        print("\n❌ No high-score setups detected in the analyzed window.")

if __name__ == "__main__":
    run_backtest()
