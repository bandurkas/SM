import ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone
import os

def download_binance_history(symbol='ETH/USDT', timeframe='15m', days=365):
    exchange = ccxt.binance()
    # Use timezone-aware UTC datetime
    now_utc = datetime.now(timezone.utc)
    since = exchange.parse8601((now_utc - timedelta(days=days)).isoformat())
    
    all_ohlcv = []
    print(f"Скачиваем {days} дней истории {symbol} ({timeframe})...")
    
    while since < exchange.milliseconds():
        try:
            # Binance отдает по 1000 свечей за раз
            limit = 1000
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
            if not ohlcv:
                break
            
            all_ohlcv.extend(ohlcv)
            # Check if ohlcv is empty before accessing index
            if not ohlcv: break
            
            since = ohlcv[-1][0] + 1  # Смещение на 1мс после последней свечи
            
            # Прогресс
            last_date = datetime.fromtimestamp(ohlcv[-1][0]/1000, tz=timezone.utc).strftime('%Y-%m-%d')
            if len(all_ohlcv) % 5000 == 0:
                print(f"  [LOG] Загружено до {last_date} (всего {len(all_ohlcv)} баров)")
            
        except Exception as e:
            print(f"Ошибка: {e}")
            break
            
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    # Don't convert to datetime yet, keep ms for consistency if needed, 
    # but for CSV it's better as ISO
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    return df

if __name__ == "__main__":
    df = download_binance_history(days=365)
    path = "data/eth_history_1y.csv"
    os.makedirs("data", exist_ok=True)
    df.to_csv(path, index=False)
    print(f"\nГотово! Данные сохранены в {path}")
    print(f"Итого строк: {len(df)}")
    print(f"Диапазон: {df['timestamp'].min()} - {df['timestamp'].max()}")
