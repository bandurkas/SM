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
from agents.execution_agent import ExecutionAgent
from notifier.telegram_bot import TelegramNotifier

load_dotenv()

# Configuration
SYMBOLS = [s.strip() for s in os.getenv('SYMBOLS', 'ETH/USDT').split(',')]
TIMEFRAMES = ['15m', '30m', '1h', '4h', '1d']
SIGNAL_THRESHOLD = 6
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
        self.executor = ExecutionAgent()
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
        if df is None or len(df) < 10: return "Нейтральный"
        last_close = df['close'].iloc[-1]
        ema_20 = df['close'].rolling(20).mean().iloc[-1]
        if last_close > ema_20: return "Бычий"
        if last_close < ema_20: return "Медвежий"
        return "Нейтральный"

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
                        all_details.append(f"🌐 Согласование HTF: {htf_trend}")

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
                        
                        # 5. Execute Auto-Trade
                        if self.executor.enabled:
                            current_price = df_exec['close'].iloc[-1]
                            is_long = any("Bullish" in d or "Бычий" in d for d in all_details)
                            # Get nearest swing for SL calculation
                            swing_price = self.agents['structure'].get_nearest_swing(df_exec, is_long)
                            trade_result = self.executor.execute_trade(
                                symbol, is_long, current_price, swing_price=swing_price
                            )

                            if trade_result:
                                dry = trade_result.get('dry_run', False)
                                mode_label = '🧪 СИМУЛЯЦИЯ' if dry else '🧾 ОРДЕР ИСПОЛНЕН'
                                msg  = f"{mode_label}\n"
                                msg += f"━━━━━━━━━━━━━━━━━━━\n"
                                msg += f"📈 **Тип:** `{trade_result['side']}`\n"
                                msg += f"📊 **Инструмент:** `{trade_result['symbol']}`\n"
                                msg += f"💵 **Цена входа:** `{trade_result['price']}`\n"
                                msg += f"🛑 **Stop-Loss:** `{trade_result['sl']}` _(за Swing + буфер 0.2%)_\n"
                                msg += f"🎯 **Take-Profit:** `{trade_result['tp']}` _(+3%)_\n"
                                msg += f"🔄 **Трейлинг-стоп:** активен после TP\n"
                                if not dry:
                                    msg += f"💰 **Сумма:** `${trade_result['cost']:.2f}`\n"
                                    msg += f"✅ **Статус:** `{trade_result['status']}`\n"
                                self.notifier.send_custom_message(msg)

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
