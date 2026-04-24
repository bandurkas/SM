import pandas as pd
import random
import subprocess
import os

csv_path = "data/eth_history_1y.csv"
df = pd.read_csv(csv_path, index_col=0, parse_dates=True)

total_days = (df.index[-1] - df.index[0]).days
# We need 28 days for simulation
start_day_offset = random.randint(0, total_days - 30)
start_date = df.index[0] + pd.Timedelta(days=start_day_offset)

print(f"--- SIMULATION START DATE: {start_date} ---")

cmd = [
    "python3", "backtest.py",
    "--csv", csv_path,
    "--days", "28",
    "--threshold", "75"
]

# Note: backtest.py takes --days from the END of the file by default.
# I need to modify backtest.py or slice the data.
# For simplicity, I'll just run it as is, but if I want a SPECIFIC window, 
# I should pass a sliced CSV.

df_slice = df[start_date : start_date + pd.Timedelta(days=28)]
slice_path = "data/random_sim.csv"
df_slice.to_csv(slice_path)

subprocess.run(["python3", "backtest.py", "--csv", slice_path, "--days", "28", "--threshold", "75"])
