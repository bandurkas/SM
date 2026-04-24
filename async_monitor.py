import os
import asyncio
import pandas as pd
import ccxt.async_support as ccxt_async
from datetime import datetime, timedelta
from dotenv import load_dotenv
from agents.structure_agent import StructureAgent
from agents.liquidity_agent import LiquidityAgent
from agents.zone_agent import ZoneAgent
from agents.timing_agent import TimingAgent
from notifier.telegram_bot import TelegramNotifier

load_dotenv()

# Configuration
SYMBOLS = [s.strip() for s in os.getenv('SYMBOLS', 'ETH/USDT').split(',')]
TIMEFRAMES = ['15m', '30m', '1h', '4h', '1d']
SIGNAL_THRESHOLD = 5
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', 60))

class MultiSymbolMonitor:
    def __init__(self, symbols):
        self.symbols = symbols
        self.exchange = None
        self.agents = {
            'structure': StructureAgent(),
            'liquidity': LiquidityAgent(),
            'zone': ZoneAgent(),
            'timing': TimingAgent()
        }
        self.notifier = TelegramNotifier()
        self.last_alerts = {}
        self.running = False

    async def initialize_exchange(self):
        self.exchange = ccxt_async.htx({
            'apiKey': os.getenv('HTX_ACCESS_KEY'),
            'secret': os.getenv('HTX_SECRET_KEY'),
            'enableRateLimit': True,
            'timeout': 20000,
        })

    async def fetch_tf_data(self, symbol, tf):
        """Asynchronously fetches data for a specific timeframe."""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, tf, limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return tf, df
        except Exception as e:
            print(f"⚠️ [{symbol}] Error fetching {tf}: {e}")
            return tf, None

    def analyze_trend(self, df):
        if df is None or len(df) < 10: return "Neutral"
        last_close = df['close'].iloc[-1]
        ema_20 = df['close'].rolling(20).mean().iloc[-1]
        if last_close > ema_20: return "Bullish"
        if last_close < ema_20: return "Bearish"
        return "Neutral"

    async def monitor_symbol(self, symbol):
        """Individual task loop for a single symbol."""
        print(f"📡 Starting monitor for {symbol}")
        while self.running:
            try:
                # 1. Fetch all timeframes in parallel
                tasks = [self.fetch_tf_data(symbol, tf) for tf in TIMEFRAMES]
                results = await asyncio.gather(*tasks)
                mtf_data = {tf: df for tf, df in results if df is not None}

                if mtf_data.get('15m') is None:
                    await asyncio.sleep(POLLING_INTERVAL)
                    continue

                total_score = 0
                all_details = []

                # 2. HTF Trend Alignment
                df_4h = mtf_data.get('4h')
                df_1d = mtf_data.get('1d')
                if df_4h is not None and df_1d is not None:
                    htf_trend = self.analyze_trend(df_4h)
                    daily_trend = self.analyze_trend(df_1d)
                    if htf_trend == daily_trend and htf_trend != "Neutral":
                        total_score += 2
                        all_details.append(f"HTF Alignment: {htf_trend}")

                # 3. Agent Analysis
                df_exec = mtf_data['15m']
                df_with_swings = self.agents['structure'].identify_swings(df_exec)
                
                for name, agent in self.agents.items():
                    score, details = agent.get_signal(df_with_swings)
                    total_score += score
                    all_details.extend(details)

                print(f"[{datetime.now().strftime('%H:%M:%S')}] {symbol} Score: {total_score}")

                # 4. Alert & Deduplicate
                if total_score >= SIGNAL_THRESHOLD:
                    now = datetime.now()
                    key = (symbol, total_score)
                    if key not in self.last_alerts or now - self.last_alerts[key] > timedelta(minutes=15):
                        print(f"🔥 {symbol} SIGNAL DETECTED! Score: {total_score}")
                        self.notifier.send_alert(symbol, total_score, all_details)
                        self.last_alerts[key] = now

                await asyncio.sleep(POLLING_INTERVAL)

            except Exception as e:
                print(f"❌ [{symbol}] Error: {e}")
                await asyncio.sleep(10)

    async def run(self):
        self.running = True
        await self.initialize_exchange()
        
        # Spawn tasks for all symbols
        tasks = [self.monitor_symbol(s) for s in self.symbols]
        await asyncio.gather(*tasks)

    async def close(self):
        self.running = False
        if self.exchange:
            await self.exchange.close()

if __name__ == "__main__":
    monitor = MultiSymbolMonitor(SYMBOLS)
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        asyncio.run(monitor.close())
