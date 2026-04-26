import os
import ccxt
from dotenv import load_dotenv

load_dotenv()

def check_htx_balance():
    api_key = os.getenv('HTX_ACCESS_KEY')
    secret = os.getenv('HTX_SECRET_KEY')
    
    if not api_key or not secret:
        print("❌ Error: HTX API keys not found in .env")
        return

    try:
        # Check Spot Balance
        exchange = ccxt.htx({
            'apiKey': api_key,
            'secret': secret,
            'enableRateLimit': True,
        })
        
        balance = exchange.fetch_balance()
        usdt_free = balance['free'].get('USDT', 0)
        usdt_used = balance['used'].get('USDT', 0)
        usdt_total = balance['total'].get('USDT', 0)
        
        print(f"💰 **HTX Spot Balance (USDT):**")
        print(f"  • Свободно: `${usdt_free:.2f}`")
        print(f"  • В ордерах: `${usdt_used:.2f}`")
        print(f"  • Всего: `${usdt_total:.2f}`")
        
        # Check if we have other major assets
        for asset in ['BTC', 'ETH', 'SOL', 'TRX']:
            free = balance['free'].get(asset, 0)
            if free > 0:
                print(f"  • {asset}: `{free:.4f}`")

    except Exception as e:
        print(f"❌ Error fetching balance: {e}")

if __name__ == "__main__":
    check_htx_balance()
