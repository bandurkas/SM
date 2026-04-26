import os
import ccxt
from dotenv import load_dotenv

load_dotenv()

def test_futures():
    api_key = os.getenv('HTX_ACCESS_KEY')
    secret = os.getenv('HTX_SECRET_KEY')
    
    exchange = ccxt.htx({
        'apiKey': api_key,
        'secret': secret,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap', # 'swap' for perpetual futures
        }
    })
    
    try:
        # Load markets
        markets = exchange.load_markets()
        print("Loaded Markets")
        
        # Check swap symbols
        swap_symbols = [s for s in markets.keys() if markets[s].get('swap')]
        print(f"Sample Swap Symbols: {swap_symbols[:5]}")
        
        # Check Unified Margin Account (V3)
        try:
            print("Fetching V3 Unified Account Info...")
            response = exchange.contractPrivateGetLinearSwapApiV3UnifiedAccountInfo()
            
            usdt_balance = 0
            if response.get('status') == 'ok' and response.get('data'):
                for account in response['data']:
                    if account['margin_asset'] == 'USDT':
                        usdt_balance = account['margin_balance']
                        print(f"💰 HTX Futures (Unified Margin) Balance: {usdt_balance} USDT")
                        # Print full details for USDT
                        print(f"  • Свободно: {account['withdraw_available']}")
                        print(f"  • Использовано: {account['margin_used']}")
            else:
                print(f"⚠️ Unified Margin response: {response}")
                
        except Exception as e:
            print(f"⚠️ Unified Margin error: {e}")
            
    except Exception as e:
        print(f"❌ General Error: {e}")

if __name__ == "__main__":
    test_futures()
