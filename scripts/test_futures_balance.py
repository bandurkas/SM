import os
import ccxt
from dotenv import load_dotenv

load_dotenv()

def test_futures_balance():
    api_key = os.getenv('HTX_ACCESS_KEY')
    secret = os.getenv('HTX_SECRET_KEY')
    
    exchange = ccxt.htx({
        'apiKey': api_key,
        'secret': secret,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })
    
    # Try different CCXT params for HTX futures
    params_list = [
        {},
        {'type': 'swap', 'marginMode': 'cross'},
        {'type': 'swap', 'marginMode': 'isolated'},
        {'marginMode': 'cross'},
        {'marginMode': 'isolated'}
    ]
    
    for params in params_list:
        try:
            print(f"\n--- Trying fetch_balance with {params} ---")
            balance = exchange.fetch_balance(params)
            usdt = balance['free'].get('USDT', 0)
            total = balance['total'].get('USDT', 0)
            print(f"💰 Found: {total} USDT (Free: {usdt})")
        except Exception as e:
            print(f"⚠️ Error: {e}")

if __name__ == "__main__":
    test_futures_balance()
