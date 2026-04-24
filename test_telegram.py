import os
import time
from dotenv import load_dotenv
from notifier.telegram_bot import TelegramNotifier

load_dotenv()

def test_alert():
    print("🔔 Starting Telegram Notification Test...")
    notifier = TelegramNotifier()
    
    # Mock data for the test
    symbol = "ETH/USDT (TEST)"
    score = 8
    details = [
        "✅ HTF Trend Alignment: Bullish",
        "🔹 Bullish BOS confirmed (Body Close)",
        "💧 Liquidity Sweep detected below Swing Low",
        "⚡ FVG (Imbalance) filled by 50%",
        "⏰ Killzone: London Open Active"
    ]
    
    print(f"📤 Queuing test alert for {symbol} with score {score}...")
    notifier.send_alert(symbol, score, details)
    
    print("⏳ Waiting for background worker to process the queue...")
    # Give it a few seconds to send since it's in a background thread
    time.sleep(5)
    
    print("✅ Test script finished. Please check your Telegram!")

if __name__ == "__main__":
    test_alert()
