import asyncio
import os
from async_monitor import MultiSymbolMonitor, SYMBOLS

def run():
    print("🚀 Starting SM_Agent Async Orchestrator...")
    monitor = MultiSymbolMonitor(SYMBOLS)
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        print("\n🛑 Stopping monitor...")
        asyncio.run(monitor.close())
    except Exception as e:
        print(f"❌ Critical Failure: {e}")

if __name__ == "__main__":
    run()
