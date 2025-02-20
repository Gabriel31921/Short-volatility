import requests
import os
import yaml
import datetime
import json
import time

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from datetime import timedelta, datetime

directory_path = os.getenv("Short_Volatility_Path")
directory_path = directory_path +"/Seasonal/BTC/"

config_path = os.path.join(directory_path, "config.yaml")
with open(config_path, "r") as file:
    config = yaml.safe_load(file)

Historical_data = pd.read_csv(directory_path + "Historical BTC data 2020 - 2025")

Historical_data["Time"] = pd.to_datetime(Historical_data["Time"], unit="ms")
Historical_data["Date"] = Historical_data["Time"].dt.date
Historical_data["is_08"] = Historical_data["Time"].dt.hour == 8
Historical_data["is_00"] = Historical_data["Time"].dt.hour == 0

data_00 = Historical_data[Historical_data["Time"].dt.hour == 0][["Date", "Open"]].rename(columns={"Open": "Open_00"})
data_08 = Historical_data[Historical_data["Time"].dt.hour == 8][["Date", "Open"]].rename(columns={"Open": "Open_08"})

merged_data = pd.merge(data_00, data_08, on="Date", how="inner")

merged_data["Return"] = abs(((merged_data["Open_08"] - merged_data["Open_00"]) / merged_data["Open_00"]) * 100)
merged_data["Log_Return"] = np.log(merged_data["Open_08"] / merged_data["Open_00"])

# Add a day-of-week column (e.g. Monday, Tuesday, etc.)
merged_data["DayOfWeek"] = pd.to_datetime(merged_data["Date"]).dt.day_name()

# Summary statistics for the intraday move by day-of-week
intraday_summary = merged_data.groupby("DayOfWeek")["Return"].agg(["mean", "std", "median", "count"])
print("00:00 to 08:00 move summary by Day-of-Week:")
print(intraday_summary)

plt.figure(figsize=(10, 6))
plt.hist(merged_data["Return"], bins=200, edgecolor="k")
plt.title("Histogram: 00:00 to 08:00 Returns")
plt.xlabel("Return (%)")
plt.ylabel("Frequency")
plt.show()

# --- For the 08:00 to 08:00 moves (next-day moves) ---
# Use the 08:00 data; ensure the Open values are numeric
data_08 = Historical_data[Historical_data["Time"].dt.hour == 8][["Date", "Open"]].rename(columns={"Open": "Open_08"})
data_08["Open_08"] = data_08["Open_08"].astype(float)
data_08 = data_08.sort_values("Date").reset_index(drop=True)

# Compute the next-day move
data_08["Return_08"] = abs(((data_08["Open_08"].shift(-1) - data_08["Open_08"]) / data_08["Open_08"]) * 100)
data_08["Log_Return_08"] = np.log(data_08["Open_08"].shift(-1) / data_08["Open_08"])

# Add a day-of-week column (based on the day the 08:00 observation occurs)
data_08["DayOfWeek"] = pd.to_datetime(data_08["Date"]).dt.day_name()

# Summary statistics for the 08:00 to 08:00 move by day-of-week
nextday_summary = data_08.groupby("DayOfWeek")["Return_08"].agg(["mean", "std", "median", "count"])
print("08:00 to 08:00 move summary by Day-of-Week:")
print(nextday_summary)

plt.figure(figsize=(10, 6))
plt.hist(data_08["Return_08"].dropna(), bins=200, edgecolor="k")
plt.title("Histogram: 08:00 to 08:00 Returns")
plt.xlabel("Return (%)")
plt.ylabel("Frequency")
plt.show()