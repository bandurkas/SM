import os
import subprocess

def run_all_backtests():
    data_dir = "data"
    files = [f for f in os.listdir(data_dir) if f.endswith("_history.csv")]
    
    print(f"🚀 Starting multi-instrument backtest for {len(files)} files...")
    
    for file in files:
        symbol = file.replace("_history.csv", "").upper().replace("_", "/")
        full_path = os.path.join(data_dir, file)
        
        print(f"\n" + "="*50)
        print(f"📈 ANALYZING: {symbol}")
        print("="*50)
        
        # Run backtest.py using environment variable for path
        env = os.environ.copy()
        env["BACKTEST_DATA_PATH"] = full_path
        
        result = subprocess.run(["python3", "backtest.py"], env=env, capture_output=True, text=True)
        
        # Extract and print only the statistics
        output = result.stdout
        if "СТАТИСТИКА СИГНАЛОВ" in output:
            stats_idx = output.find("📈 СТАТИСТИКА СИГНАЛОВ")
            print(output[stats_idx:].strip())
        else:
            print("❌ No signals found or error occurred.")
            print(result.stderr)

if __name__ == "__main__":
    run_all_backtests()
