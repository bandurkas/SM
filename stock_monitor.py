import os
import asyncio
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from dotenv import load_dotenv
from agents.structure_agent import StructureAgent
from agents.liquidity_agent import LiquidityAgent
from agents.zone_agent import ZoneAgent
from agents.timing_agent import TimingAgent
from notifier.telegram_bot import TelegramNotifier

load_dotenv()

# Configuration
STOCKS = [s.strip() for s in os.getenv('STOCKS', '^GSPC,^IXIC,AAPL,TSLA,NVDA,MSFT').split(',')]
TIMEFRAMES_MAP = {
    '15m': '15m',
    '1h': '60m',
    '4h': '720m', # yfinance doesn't support 4h directly well, often uses 1h chunks
    '1d': '1d'
}
SIGNAL_THRESHOLD = 6
POLLING_INTERVAL = 300 # Stocks move slower, 5 min is fine

class StockMonitor:
    def __init__(self, symbols):
        self.symbols = symbols
        self.agents = {
            'structure': StructureAgent(),
            'liquidity': LiquidityAgent(),
            'zone': ZoneAgent(),
            'timing': TimingAgent()
        }
        self.notifier = TelegramNotifier()
        self.last_alerts = {}
        self.running = False

    async def fetch_stock_data(self, symbol, tf):
        """Asynchronously fetches data for a specific stock timeframe."""
        try:
            # yfinance is synchronous, run in executor
            loop = asyncio.get_event_loop()
            interval = TIMEFRAMES_MAP.get(tf, '15m')
            
            # Fetch last 30 days for 15m/1h, 1 year for 1d
            period = "30d" if tf in ['15m', '1h'] else "1y"
            
            df = await loop.run_in_executor(None, lambda: yf.download(symbol, period=period, interval=interval, progress=False))
            
            if df.empty: return tf, None
            
            # Fix MultiIndex columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
                
            # Reset index to get timestamp as column
            df = df.reset_index()
            df.columns = [str(c).lower() for c in df.columns]
            if 'datetime' in df.columns:
                df = df.rename(columns={'datetime': 'timestamp'})
            elif 'date' in df.columns:
                df = df.rename(columns={'date': 'timestamp'})
                
            return tf, df
        except Exception as e:
            print(f"⚠️ [{symbol}] Error fetching {tf}: {e}")
            return tf, None

    def analyze_trend(self, df):
        if df is None or len(df) < 10: return "Нейтральный"
        last_close = df['close'].iloc[-1]
        ema_20 = df['close'].rolling(20).mean().iloc[-1]
        if last_close > ema_20: return "Бычий"
        if last_close < ema_20: return "Медвежий"
        return "Нейтральный"

    async def monitor_stock(self, symbol):
        """Individual task loop for a single stock."""
        print(f"📈 Starting stock monitor for {symbol}")
        while self.running:
            try:
                # 1. Fetch timeframes
                tasks = [self.fetch_stock_data(symbol, tf) for tf in ['15m', '1d']]
                results = await asyncio.gather(*tasks)
                mtf_data = {tf: df for tf, df in results if df is not None}

                if mtf_data.get('15m') is None:
                    await asyncio.sleep(60)
                    continue

                total_score = 0
                all_details = []

                # 2. HTF Trend Alignment (Daily)
                df_1d = mtf_data.get('1d')
                if df_1d is not None:
                    daily_trend = self.analyze_trend(df_1d)
                    all_details.append(f"🌐 Глобальный тренд (Daily): {daily_trend}")
                    
                    # For stocks, daily trend is king
                    if daily_trend != "Нейтральный":
                        # Logic: if current price on 15m follows daily trend
                        current_15m_close = mtf_data['15m']['close'].iloc[-1]
                        if (daily_trend == "Бычий" and current_15m_close > df_1d['close'].iloc[-1]) or \
                           (daily_trend == "Медвежий" and current_15m_close < df_1d['close'].iloc[-1]):
                            total_score += 2
                            all_details.append(f"✅ Согласование с дневным трендом")

                # 3. Agent Analysis (15m)
                df_exec = mtf_data['15m']
                df_with_swings = self.agents['structure'].identify_swings(df_exec)
                
                for name, agent in self.agents.items():
                    if name == 'timing': continue # Killzones are different for stocks
                    score, details = agent.get_signal(df_with_swings)
                    total_score += score
                    all_details.extend(details)

                print(f"[{datetime.now().strftime('%H:%M:%S')}] {symbol} Score: {total_score}")

                # 4. Alert & Deduplicate
                if total_score >= SIGNAL_THRESHOLD:
                    now = datetime.now()
                    key = (symbol, total_score)
                    if key not in self.last_alerts or now - self.last_alerts[key] > timedelta(minutes=60):
                        # Determine prefix
                        if "=X" in symbol: prefix = "💱 FOREX"
                        elif "=F" in symbol: prefix = "🔥 COMMODITY"
                        elif "^" in symbol: prefix = "📊 INDEX"
                        else: prefix = "🏛️ STOCK"
                        
                        print(f"{prefix} {symbol} SIGNAL! Score: {total_score}")
                        display_symbol = f"{prefix}: {symbol}"
                        self.notifier.send_alert(display_symbol, total_score, all_details)
                        self.last_alerts[key] = now

                await asyncio.sleep(POLLING_INTERVAL)

            except Exception as e:
                print(f"❌ [{symbol}] Stock Error: {e}")
                await asyncio.sleep(60)

    async def run(self):
        self.running = True
        tasks = [self.monitor_stock(s) for s in self.symbols]
        await asyncio.gather(*tasks)

    async def close(self):
        self.running = False

if __name__ == "__main__":
    monitor = StockMonitor(STOCKS)
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        asyncio.run(monitor.close())
