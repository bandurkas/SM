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
        Отправляет профессионально оформленное уведомление в Telegram на русском языке.
        """
        if not self.token or not self.chat_id:
            print("⚠️ Telegram token or chat ID missing. Alert not queued.")
            return

        chat_ids = [cid.strip() for cid in str(self.chat_id).split(',')]

        # Динамический заголовок на основе счета
        if score >= 7:
            header = "🚨 *СЕТАП ВЫСОКОЙ ВЕРОЯТНОСТИ*"
            status = "🔥 _Экстремально высокая вероятность_"
        elif score >= 5:
            header = "🛡️ *SMART MONEY СИГНАЛ*"
            status = "📈 _Хорошая вероятность_"
        else:
            header = "📡 *ОБНАРУЖЕН СИГНАЛ*"
            status = "⚖️ _Средняя вероятность_"

        message = f"{header}\n\n"
        message += f"📊 *Инструмент:* `{symbol}`\n"
        message += f"🔥 *Счет совпадений:* `{score}/10`\n"
        message += f"📝 *Статус:* {status}\n"
        message += f"━━━━━━━━━━━━━━━━━━━━\n\n"
        
        message += "*✅ ОБНАРУЖЕННЫЕ ФАКТОРЫ:*\n"
        for detail in details:
            # Перевод ключевых факторов
            d = detail.replace("HTF Alignment", "🌐 Согласование HTF") \
                      .replace("Bullish", "Бычий") \
                      .replace("Bearish", "Медвежий") \
                      .replace("Trend", "Тренд") \
                      .replace("BOS", "🔹 BOS (Слом структуры)") \
                      .replace("MSS", "🔹 MSS (Смена характера)") \
                      .replace("confirmed", "подтвержден") \
                      .replace("Body Close", "Закрытие телом") \
                      .replace("Liquidity Sweep", "💧 Снятие ликвидности (Sweep)") \
                      .replace("detected", "обнаружено") \
                      .replace("below Swing Low", "ниже Swing Low") \
                      .replace("above Swing High", "выше Swing High") \
                      .replace("FVG", "⚡ FVG (Имбаланс)") \
                      .replace("Imbalance", "⚡ Имбаланс") \
                      .replace("Order Block", "📦 Order Block") \
                      .replace("Breaker", "📦 Breaker Block") \
                      .replace("Killzone", "⏰ Killzone") \
                      .replace("Active", "Активна")
            
            message += f"{d}\n"
            
        message += f"\n━━━━━━━━━━━━━━━━━━━━\n"
        message += f"💡 *Совет:* _Проверьте структуру на M5 перед входом._\n"
        message += f"⏰ _UTC: {datetime.now().strftime('%H:%M:%S')}_"

        for cid in chat_ids:
            payload = {
                "chat_id": cid,
                "text": message,
                "parse_mode": "Markdown"
            }
            self.queue.put(payload)
