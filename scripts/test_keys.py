import os
import ccxt
from dotenv import load_dotenv

load_dotenv()

def test_keys():
    api_key = os.getenv('HTX_ACCESS_KEY')
    secret = os.getenv('HTX_SECRET_KEY')
    print(f"Testing with API Key: {api_key[:8]}...{api_key[-4:]}")
    
    exchange = ccxt.htx({
        'apiKey': api_key,
        'secret': secret,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })
    
    try:
        # Check standard account to see if key works at all
        res = exchange.fetch_balance()
        print("✅ API Keys are VALID (Spot/General Access Ok)")
    except Exception as e:
        print(f"❌ Connection Error (Spot check): {e}")

    try:
        # Check cross margin
        cross_resp = exchange.contractPrivateGetLinearSwapApiV1SwapCrossAccountInfo({'margin_account': 'USDT'})
        if cross_resp.get('status') == 'ok':
            print("✅ API Keys are VALID for USDT-M Swap Cross Margin")
            for account in cross_resp['data']:
                if account['margin_asset'] == 'USDT':
                    print(f"💰 Balance (Cross): {account['margin_balance']} USDT")
        else:
            print(f"⚠️ Cross Margin error: {cross_resp}")
    except Exception as e:
        print(f"⚠️ Cross Margin API Exception: {e}")

if __name__ == "__main__":
    test_keys()
