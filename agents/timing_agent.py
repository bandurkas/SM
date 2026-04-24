from datetime import datetime
import pytz

class TimingAgent:
    """
    Agent responsible for validating setups against institutional timing (Killzones).
    Times are in UTC.
    """
    KILLZONES = {
        'Asia': {'start': 0, 'end': 5},
        'London': {'start': 7, 'end': 10},
        'NewYork': {'start': 12, 'end': 15},
        'LondonClose': {'start': 15, 'end': 17}
    }

    def is_in_killzone(self, timestamp=None):
        """
        Checks if current time is within a high-probability killzone.
        """
        if timestamp is None:
            now_utc = datetime.now(pytz.UTC)
        else:
            now_utc = timestamp

        current_hour = now_utc.hour
        
        active_kz = None
        for name, times in self.KILLZONES.items():
            if times['start'] <= current_hour < times['end']:
                active_kz = name
                break
                
        return active_kz

    def get_signal(self, df, timestamp=None):
        active_kz = self.is_in_killzone(timestamp)
        
        score = 0
        details = []
        
        if active_kz in ['London', 'NewYork']:
            score = 1
            details.append(f"Killzone — Активна Killzone {active_kz} (+1 балл)")
        elif active_kz:
            details.append(f"Timing — Активна {active_kz} сессия (Средняя вероятность)")
        else:
            details.append("Timing — Вне институциональных Killzones (Низкая вероятность)")
            
        return score, details
