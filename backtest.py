import pandas as pd
import numpy as np
import os
import argparse
from datetime import datetime
from agents.structure_agent import StructureAgent
from agents.liquidity_agent import LiquidityAgent
from agents.zone_agent import ZoneAgent
from agents.timing_agent import TimingAgent

def analyze_trend(df):
    if df is None or len(df) < 10: return "Нейтральный"
    last_close = df['close'].iloc[-1]
    ema_20 = df['close'].rolling(20).mean().iloc[-1]
    if last_close > ema_20: return "Бычий"
    if last_close < ema_20: return "Медвежий"
    return "Нейтральный"

def run_backtest(limit=3000, min_score=6):
    data_path = os.getenv('BACKTEST_DATA_PATH', 'data/eth_history_1y.csv')
    print(f"⌛ Loading historical data from {data_path}...")
    df_full = pd.read_csv(data_path)
    df_full['timestamp'] = pd.to_datetime(df_full['timestamp'])

    # Pre-calculate HTFs
    print("🔄 Pre-calculating High Timeframes...")
    df_4h = df_full.resample('4h', on='timestamp').agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}).dropna()
    df_1d = df_full.resample('1d', on='timestamp').agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}).dropna()

    structure_agent = StructureAgent()
    liquidity_agent = LiquidityAgent()
    zone_agent = ZoneAgent()
    timing_agent = TimingAgent()
    
    results = []
    start_idx = max(200, len(df_full) - limit)
    
    print(f"📈 Running backtest loop for {limit} candles (Min Score: {min_score})...")
    for i in range(start_idx, len(df_full)):
        current_time = df_full['timestamp'].iloc[i]
        df_exec = df_full.iloc[:i+1].tail(200).copy()
        
        # HTF Trend
        past_4h = df_4h[df_4h.index < current_time]
        past_1d = df_1d[df_1d.index < current_time]
        
        total_score = 0
        all_details = []
        
        if not past_4h.empty and not past_1d.empty:
            htf_trend = analyze_trend(past_4h)
            daily_trend = analyze_trend(past_1d)
            if htf_trend == daily_trend and htf_trend != "Нейтральный":
                total_score += 2
                all_details.append(f"🌐 Согласование HTF: {htf_trend}")

        df_exec = structure_agent.identify_swings(df_exec)
        
        for agent in [structure_agent, liquidity_agent, zone_agent, timing_agent]:
            score, details = agent.get_signal(df_exec)
            total_score += score
            all_details.extend(details)

        if total_score >= min_score:
            results.append({
                'time': current_time.strftime('%Y-%m-%d %H:%M'),
                'price': round(float(df_exec['close'].iloc[-1]), 2),
                'score': total_score,
                'signals': all_details
            })

    if results:
        report_df = pd.DataFrame(results)
        print(f"\n✅ Backtest finished. Found {len(results)} signals.")
        report_df.to_csv("backtest_results.csv", index=False)
        return results
    else:
        print("\n❌ No high-score setups detected.")
        return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=3000)
    parser.add_argument("--min-score", type=int, default=6)
    args = parser.parse_args()
    run_backtest(limit=args.limit, min_score=args.min_score)
