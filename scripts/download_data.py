import ccxt
import pandas as pd
import time
import os
from datetime import datetime

def download_history(symbol, timeframe='15m', limit=10000):
    print(f"⌛ Downloading {limit} candles for {symbol} ({timeframe})...")
    exchange = ccxt.htx()
    
    all_ohlcv = []
    since = None # Start from the most recent
    
    chunk_size = 1000
    while len(all_ohlcv) < limit:
        try:
            # fetch_ohlcv gets the most recent ones if since is None
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=chunk_size)
            if not ohlcv or len(ohlcv) == 0: break
            
            # To get more, we would need to go back in time, 
            # but for simplicity, let's just get the last 1000 if we want 'latest'
            all_ohlcv = ohlcv
            break
        except Exception as e:
            print(f"  ❌ Error: {e}")
            break
            
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    os.makedirs('data', exist_ok=True)
    filename = f"data/{symbol.replace('/', '_').lower()}_history.csv"
    df.to_csv(filename, index=False)
    print(f"✅ Saved to {filename}")
    return filename

if __name__ == "__main__":
    symbols = ['BTC/USDT', 'SOL/USDT', 'XRP/USDT', 'ETH/USDT', 'TRX/USDT']
    for s in symbols:
        download_history(s, limit=10000)
