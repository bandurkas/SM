import yfinance as yf
import pandas as pd
import os
from datetime import datetime
from agents.structure_agent import StructureAgent
from agents.liquidity_agent import LiquidityAgent
from agents.zone_agent import ZoneAgent
from agents.timing_agent import TimingAgent

SYMBOLS = [
    '^GSPC', '^IXIC', 'AAPL', 'TSLA', 'NVDA', 'MSFT', 
    'GC=F', 'CL=F', 'EURUSD=X', 'GBPUSD=X', 'USDJPY=X'
]

def analyze_trend(df):
    if df is None or len(df) < 20: return "Нейтральный"
    last_close = df['close'].iloc[-1]
    ema_20 = df['close'].rolling(20).mean().iloc[-1]
    if last_close > ema_20: return "Бычий"
    if last_close < ema_20: return "Медвежий"
    return "Нейтральный"

def run_global_backtest(symbol):
    print(f"⌛ Fetching data for {symbol}...")
    try:
        # Fetch 60 days of 15m (max allowed by yf for 15m)
        df_15m = yf.download(symbol, period="60d", interval="15m", progress=False)
        # Fetch 1 year of 1d
        df_1d = yf.download(symbol, period="1y", interval="1d", progress=False)
        
        if df_15m.empty or df_1d.empty:
            print(f"  ❌ No data for {symbol}")
            return None

        # Clean data
        for df in [df_15m, df_1d]:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.columns = [str(c).lower() for c in df.columns]

        # Initialize agents
        agents = {
            'structure': StructureAgent(),
            'liquidity': LiquidityAgent(),
            'zone': ZoneAgent()
        }
        
        results = []
        # Daily trend is calculated once for the whole period in a simplified way 
        # (or we could resample 1d from 15m, but let's use the actual 1d history)
        
        # Pre-calculate swings for 15m
        df_15m = agents['structure'].identify_swings(df_15m)
        
        print(f"📈 Running backtest for {len(df_15m)} candles...")
        
        # We loop through the last 2000 15m candles
        for i in range(200, len(df_15m)):
            current_time = df_15m.index[i]
            
            # Find the corresponding daily trend for that time
            past_1d = df_1d[df_1d.index < current_time.strftime('%Y-%m-%d')]
            daily_trend = analyze_trend(past_1d)
            
            total_score = 0
            details = []
            
            # Context points
            if daily_trend != "Нейтральный":
                if (daily_trend == "Бычий" and df_15m['close'].iloc[i] > df_1d[df_1d.index < current_time.strftime('%Y-%m-%d')]['close'].iloc[-1]):
                    total_score += 2
                    details.append(f"HTF Alignment ({daily_trend})")

            # Agent signals
            sub_df = df_15m.iloc[:i+1]
            for name, agent in agents.items():
                s, d = agent.get_signal(sub_df)
                total_score += s
                details.extend(d)
                
            if total_score >= 5:
                results.append({
                    'time': current_time,
                    'price': df_15m['close'].iloc[i],
                    'score': total_score,
                    'details': ", ".join(details)
                })

        report_df = pd.DataFrame(results)
        if report_df.empty:
            print(f"  ✅ Finished. No signals >= 5 found.")
            return 0, 0
            
        stats = report_df['score'].value_counts()
        score_6_plus = len(report_df[report_df['score'] >= 6])
        print(f"  ✅ Finished. Total signals (5+): {len(report_df)}, (6+): {score_6_plus}")
        return len(report_df), score_6_plus

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None

if __name__ == "__main__":
    final_report = []
    for s in SYMBOLS:
        res = run_global_backtest(s)
        if res:
            final_report.append({'Symbol': s, 'Signals 5+': res[0], 'Signals 6+': res[1]})
            
    print("\n" + "="*50)
    print("🌍 GLOBAL ASSETS BACKTEST SUMMARY (60 DAYS)")
    print("="*50)
    print(pd.DataFrame(final_report).to_string(index=False))
