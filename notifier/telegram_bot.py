import os
import requests
import queue
import threading
from datetime import datetime

class TelegramNotifier:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.queue = queue.Queue()
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()

    def _worker(self):
        """Background worker to send alerts without blocking the main loop."""
        while True:
            payload = self.queue.get()
            if payload is None: break
            
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            try:
                response = requests.post(url, json=payload, timeout=10)
                if response.status_code != 200:
                    print(f"❌ Failed to send Telegram alert: {response.text}")
            except Exception as e:
                print(f"❌ Error sending Telegram alert: {e}")
            finally:
                self.queue.task_done()

    def send_alert(self, symbol, score, details):
        """
        Queues a professional formatted alert to Telegram.
        """
        if not self.token or not self.chat_id:
            print("⚠️ Telegram token or chat ID missing. Alert not queued.")
            return

        chat_ids = [cid.strip() for cid in str(self.chat_id).split(',')]

        # Dynamic header based on score
        if score >= 7:
            header = "🚨 *HIGH PROBABILITY SETUP*"
            status = "🔥 _Extremely High Probability_"
        elif score >= 5:
            header = "🛡️ *SMART MONEY ALERT*"
            status = "📈 _Good Probability_"
        else:
            header = "📡 *SIGNAL DETECTED*"
            status = "⚖️ _Moderate Probability_"

        message = f"{header}\n\n"
        message += f"📊 *Instrument:* `{symbol}`\n"
        message += f"🔥 *Coincidence Score:* `{score}/10`\n"
        message += f"📝 *Status:* {status}\n"
        message += f"━━━━━━━━━━━━━━━━━━━━\n\n"
        
        message += "*✅ DETECTED FACTORS:*\n"
        for detail in details:
            if "HTF" in detail or "Daily" in detail:
                message += f"🌐 _{detail}_\n"
            elif "BOS" in detail or "MSS" in detail:
                message += f"🔹 {detail}\n"
            elif "Liquidity" in detail or "Sweep" in detail:
                message += f"💧 {detail}\n"
            elif "FVG" in detail or "Imbalance" in detail:
                message += f"⚡ {detail}\n"
            elif "Order Block" in detail or "Breaker" in detail:
                message += f"📦 {detail}\n"
            elif "Killzone" in detail:
                message += f"⏰ {detail}\n"
            else:
                message += f"✅ {detail}\n"
            
        message += f"\n━━━━━━━━━━━━━━━━━━━━\n"
        message += f"💡 *Advice:* _Verify structure on M5 before entry._\n"
        message += f"⏰ _UTC: {datetime.now().strftime('%H:%M:%S')}_"

        for cid in chat_ids:
            payload = {
                "chat_id": cid,
                "text": message,
                "parse_mode": "Markdown"
            }
            self.queue.put(payload)
