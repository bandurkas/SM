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

    def send_alert(self, symbol, score, details):
        """Sends an educational, ultra-structured 'SmartMoney Academy' notification."""
        # Detect direction
        is_long = any("Bullish" in d or "Бычий" in d for d in details)
        side = "🟢 LONG" if is_long else "🔴 SHORT"
        side_emoji = "🟢" if is_long else "🔴"
        trend_emoji = "🟢" if is_long else "🔴"
        
        # 1. Header
        message = f"{side} СИГНАЛ | {symbol}\n"
        message += "━━━━━━━━━━━━━━━━━━━\n"
        message += f"🔥 **Сила сигнала:** {score}/10\n"
        message += "━━━━━━━━━━━━━━━━━━━\n\n"

        # 2. Bias Section
        trend_str = "Бычий" if is_long else "Медвежий"
        message += "📈 **Направление рынка (Bias):**\n\n"
        message += f"D1 — {trend_str.lower()} тренд {trend_emoji}\n"
        message += f"H4 — {trend_str.lower()} тренд {trend_emoji}\n\n"
        message += f"➡️ Общая тенденция рынка {'вверх' if is_long else 'вниз'}.\n"
        message += f"Приоритет: искать {'покупки' if is_long else 'продажи'}.\n\n"
        message += f"🔎 _Проверить: Открой график D1 и H4 — цена формирует {'рост (HH/HL)' if is_long else 'падение (LH/LL)'}._\n"
        message += "━━━━━━━━━━━━━━━━━━━\n"

        # 3. Setup Section
        message += "📌 **Основание сигнала (Setup):**\n\n"
        
        has_sweep = any("Sweep" in d for d in details)
        has_bos = any("BOS" in d or "MSS" in d for d in details)
        has_fvg = any("FVG" in d or "Imbalance" in d for d in details)
        
        if has_sweep:
            side_text = "снизу" if is_long else "сверху"
            message += f"M15 — снятие ликвидности {side_text}\n(M15 Liquidity Sweep)\n\n"
            message += f"➡️ Цена {'опустилась ниже' if is_long else 'поднялась выше'} прошлого экстремума, собрала стопы и вернулась обратно.\n"
            message += f"🔎 _Проверить: Открой M15 и найди ложный прокол {'вниз' if is_long else 'вверх'}._\n\n---\n\n"

        if has_bos:
            type_text = "бычий BOS" if is_long else "медвежий BOS"
            target_text = "локальный максимум" if is_long else "локальный минимум"
            power_text = "покупателей" if is_long else "продавцов"
            message += f"M15 — {type_text}\n(Break of Structure)\n\n"
            message += f"➡️ Цена пробила последний {target_text}. Это признак силы {power_text}.\n"
            message += f"🔎 _Проверить: На M15 посмотри пробой ближайшего {'High' if is_long else 'Low'}._\n\n---\n\n"

        if has_fvg:
            message += f"M15 — ретест FVG\n(Fair Value Gap)\n\n"
            message += "➡️ Цена вернулась в зону дисбаланса после импульса. Часто это зона продолжения движения.\n"
            message += f"🔎 _Проверить: На M15 найди резкий импульс {'вверх' if is_long else 'вниз'} и пустую зону между свечами._\n\n"

        message += "━━━━━━━━━━━━━━━━━━━\n"

        # 4. Entry Section
        message += "🎯 **Вход в сделку (Entry):**\n\n"
        message += f"Ждать закрытие {'бычьей' if is_long else 'медвежьей'} свечи на M5\n\n"
        message += f"➡️ Не входить вслепую. Нужна реакция {power_text} на младшем таймфрейме.\n"
        message += f"🔎 _Проверить: Открой M5 и дождись уверенной {'зелёной' if is_long else 'красной'} свечи от зоны входа._\n"
        message += "━━━━━━━━━━━━━━━━━━━\n"

        # 6. Glossary
        message += "📚 **Расшифровка:**\n"
        message += "Bias = общее направление рынка\nD1/H4/M15/M5 = Таймфреймы\nSweep = сбор стопов\nBOS = слом структуры\nFVG = зона дисбаланса\nSL/TP = Стоп/Тейк\n"
        message += "━━━━━━━━━━━━━━━━━━━\n"

        # 7. Summary
        side_v = "вверх" if is_long else "вниз"
        side_p = "снизу" if is_long else "сверху"
        message += "💡 **Кратко:**\n"
        message += f"Старший тренд {side_v}. На M15 выбили стопы {side_p} и вернули цену. Если M5 подтвердит силу — возможен хороший вход в {'LONG' if is_long else 'SHORT'}."

        chat_ids = [cid.strip() for cid in str(self.chat_id).split(',')]
        for cid in chat_ids:
            payload = {
                "chat_id": cid,
                "text": message,
                "parse_mode": "Markdown"
            }
            self.queue.put(payload)

    def send_custom_message(self, message):
        """Sends a raw markdown message to all configured chat IDs."""
        chat_ids = [cid.strip() for cid in str(self.chat_id).split(',')]
        for cid in chat_ids:
            payload = {
                "chat_id": cid,
                "text": message,
                "parse_mode": "Markdown"
            }
            self.queue.put(payload)
