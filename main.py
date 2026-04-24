import os
import time
import pandas as pd
import ccxt
from dotenv import load_dotenv
from agents.structure_agent import StructureAgent
from agents.liquidity_agent import LiquidityAgent
from agents.zone_agent import ZoneAgent
from agents.timing_agent import TimingAgent
from notifier.telegram_bot import TelegramNotifier

load_dotenv()

# Configuration
SYMBOL = os.getenv('SYMBOL', 'ETH/USDT')
HTX_ACCESS_KEY = os.getenv('HTX_ACCESS_KEY')
HTX_SECRET_KEY = os.getenv('HTX_SECRET_KEY')
TIMEFRAMES = ['15m', '30m', '1h', '4h', '1d']
SIGNAL_THRESHOLD = 5 # Increased for MTF coincidence

def fetch_data(exchange, symbol, timeframe, limit=100):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except Exception as e:
        print(f"⚠️ Error fetching {timeframe}: {e}")
        return None

def analyze_trend(df):
    """Simple trend analysis based on swing highs/lows"""
    if df is None or len(df) < 10: return "Neutral"
    
    # Simple logic: is last close above middle of the range?
    # Better: use structure_agent to get confirmed HH/HL
    last_close = df['close'].iloc[-1]
    ema_20 = df['close'].rolling(20).mean().iloc[-1]
    
    if last_close > ema_20: return "Bullish"
    if last_close < ema_20: return "Bearish"
    return "Neutral"

def run_monitor():
    print(f"🚀 Starting SmartMoney Signal Monitor for {SYMBOL} on HTX...")
    
    # Initialize exchange
    exchange = ccxt.htx({
        'apiKey': HTX_ACCESS_KEY,
        'secret': HTX_SECRET_KEY,
        'enableRateLimit': True,
    })
    
    # Initialize agents
    structure_agent = StructureAgent()
    liquidity_agent = LiquidityAgent()
    zone_agent = ZoneAgent()
    timing_agent = TimingAgent()
    notifier = TelegramNotifier()
    
    while True:
        try:
            mtf_data = {}
            for tf in TIMEFRAMES:
                mtf_data[tf] = fetch_data(exchange, SYMBOL, tf)
            
            total_score = 0
            all_details = []
            
            # 1. HTF Trend Alignment Check
            htf_trend = analyze_trend(mtf_data['4h'])
            daily_trend = analyze_trend(mtf_data['1d'])
            ltf_trend = analyze_trend(mtf_data['15m'])
            
            all_details.append(f"HTF Trend (4h): {htf_trend}")
            all_details.append(f"Daily Trend (1d): {daily_trend}")
            
            if htf_trend == daily_trend and htf_trend != "Neutral":
                total_score += 2 # Strong weight for HTF alignment
                all_details.append(f"✅ Strong HTF Alignment: {htf_trend}")

            # 2. Run Agents on primary execution timeframe (15m)
            df_exec = mtf_data['15m']
            if df_exec is not None:
                # First, enrich data with swings
                df_with_swings = structure_agent.identify_swings(df_exec)
                
                agents = [structure_agent, liquidity_agent, zone_agent, timing_agent]
                for agent in agents:
                    if hasattr(agent, 'identify_swings'): # StructureAgent already enriched it
                        score, details = agent.get_signal(df_with_swings)
                    else:
                        score, details = agent.get_signal(df_with_swings)
                    
                    total_score += score
                    all_details.extend(details)
            
            # 3. Cross-TF Imbalance/Zone Check
            # (e.g., is 15m signal inside a 1h/4h FVG or OB?)
            # This is a placeholder for more advanced inter-agent logic
            
            print(f"[{pd.Timestamp.now()}] Current MTF Score: {total_score}")
            
            # Check threshold
            if total_score >= SIGNAL_THRESHOLD:
                print(f"🔥 MTF Signal coincidence detected! Score: {total_score}")
                notifier.send_alert(SYMBOL, total_score, all_details)
                
            # Sleep
            time.sleep(int(os.getenv('POLLING_INTERVAL', 60)))
            
        except Exception as e:
            print(f"❌ Error in monitoring loop: {e}")
            time.sleep(10)

if __name__ == "__main__":
    run_monitor()
