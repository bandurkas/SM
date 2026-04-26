import pandas as pd
import time
import ast
import os
from notifier.telegram_bot import TelegramNotifier
from dotenv import load_dotenv

load_dotenv()

def send_historical_signals(count=20):
    print(f"📡 Loading signals from backtest_results.csv...")
    if not os.path.exists("backtest_results.csv"):
        print("❌ Error: backtest_results.csv not found. Run backtest first.")
        return

    df = pd.read_csv("backtest_results.csv")
    
    # Sort by time descending to get the most recent ones first, then reverse for chronological sending
    df = df.tail(count)
    
    notifier = TelegramNotifier()
    
    print(f"📤 Starting to send {len(df)} historical signals...")
    
    for index, row in df.iterrows():
        # Parse the signals list (it's stored as a string representation of a list in CSV)
        try:
            details = ast.literal_eval(row['signals'])
        except:
            details = [row['signals']]
            
        symbol = f"ETH/USDT (HISTORY: {row['time']})"
        score = int(row['score'])
        
        print(f"  Sending signal from {row['time']} (Score: {score})...")
        notifier.send_alert(symbol, score, details)
        
        # Small delay to avoid Telegram rate limits
        time.sleep(2)

    print("✅ All historical signals sent!")

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=20)
    args = parser.parse_args()
    send_historical_signals(args.count)
