import yfinance as yf
import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
from agents.structure_agent import StructureAgent
from agents.liquidity_agent import LiquidityAgent
from agents.zone_agent import ZoneAgent

# Выбираем 3 самых популярных актива
SYMBOLS = {
    'GC=F': 'ЗОЛОТО (Gold)',
    '^GSPC': 'S&P 500 (Index)',
    'EURUSD=X': 'EUR/USD (Forex)'
}

def analyze_trend(df):
    if df is None or len(df) < 20: return "Нейтральный"
    last_close = df['close'].iloc[-1]
    ema_20 = df['close'].rolling(20).mean().iloc[-1]
    if last_close > ema_20: return "Бычий"
    if last_close < ema_20: return "Медвежий"
    return "Нейтральный"

def run_global_backtest(symbol, name):
    print(f"\n" + "="*50)
    print(f"📈 ТЕСТИРОВАНИЕ: {name} ({symbol})")
    print("="*50)
    
    try:
        # Скачиваем 60 дней данных 15m (максимум для yfinance)
        df_15m = yf.download(symbol, period="60d", interval="15m", progress=False)
        # Скачиваем 1 год данных 1d для тренда
        df_1d = yf.download(symbol, period="1y", interval="1d", progress=False)
        
        if df_15m.empty or df_1d.empty:
            print(f"  ❌ Нет данных для {symbol}")
            return None

        # Очистка колонок
        for df in [df_15m, df_1d]:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.columns = [str(c).lower() for c in df.columns]

        # Инициализация агентов
        struct_agent = StructureAgent()
        liq_agent = LiquidityAgent()
        zone_agent = ZoneAgent()
        
        results = []
        # Предварительный расчет свингов
        df_15m = struct_agent.identify_swings(df_15m)
        
        # Проходим по последним 100 свечам
        start_idx = max(200, len(df_15m) - 100)
        for i in range(start_idx, len(df_15m)):
            current_time = df_15m.index[i]
            
            # Определяем дневной тренд на момент свечи
            past_1d = df_1d[df_1d.index < current_time.strftime('%Y-%m-%d')]
            daily_trend = analyze_trend(past_1d)
            
            total_score = 0
            details = []
            
            # 1. Тренд (+2 балла)
            if daily_trend != "Нейтральный":
                last_1d_close = past_1d['close'].iloc[-1] if not past_1d.empty else 0
                if (daily_trend == "Бычий" and df_15m['close'].iloc[i] > last_1d_close) or \
                   (daily_trend == "Медвежий" and df_15m['close'].iloc[i] < last_1d_close):
                    total_score += 2
                    details.append(f"HTF Trend ({daily_trend})")

            # 2. Сигналы агентов
            sub_df = df_15m.iloc[:i+1]
            s1, d1 = struct_agent.get_signal(sub_df)
            s2, d2 = liq_agent.get_signal(sub_df)
            s3, d3 = zone_agent.get_signal(sub_df)
            
            total_score += (s1 + s2 + s3)
            details.extend(d1 + d2 + d3)
            
            # Фильтр времени (упрощенный Killzone для бэктеста)
            hour = current_time.hour
            if 8 <= hour <= 12 or 13 <= hour <= 17: # London & NY
                total_score += 1
                details.append("Killzone Active")

            if total_score >= 5:
                results.append({
                    'Время': current_time.strftime('%Y-%m-%d %H:%M'),
                    'Цена': round(float(df_15m['close'].iloc[i]), 5),
                    'Счет': total_score,
                    'Сигналы': ", ".join(details)
                })

        report_df = pd.DataFrame(results)
        if report_df.empty:
            print(f"  ✅ Завершено. Сигналов 5+ не найдено.")
            return
            
        print(f"✅ Найдено сигналов (6+): {len(report_df[report_df['Счет'] >= 6])}")
        print(f"✅ Найдено сигналов (5): {len(report_df[report_df['Счет'] == 5])}")
        
        print("\nПОСЛЕДНИЕ 5 КАЧЕСТВЕННЫХ СЕТАПОВ:")
        print(report_df[report_df['Счет'] >= 6].tail(5).to_string(index=False))
        
    except Exception as e:
        print(f"  ❌ Ошибка при тестировании {symbol}: {e}")

if __name__ == "__main__":
    for symbol, name in SYMBOLS.items():
        run_global_backtest(symbol, name)
