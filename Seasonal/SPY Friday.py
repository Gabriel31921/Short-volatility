import yfinance as yf
import pandas as pd
import numpy as np
import os
import pickle
import yaml
import math
import matplotlib.pyplot as plt

directory_path = os.getenv("Short_Volatility_Path")
directory_path = directory_path + "/Seasonal/"
config_path = os.path.join(directory_path, "config.yaml")
with open(config_path, "r") as file:
    config = yaml.safe_load(file)

start_date = config["general"]["start_date"]
end_date = config["general"]["end_date"]

SPY = yf.Ticker("SPY").history(start= start_date, end= end_date, actions= True, auto_adjust=False)
SPY.reset_index(inplace=True)
SPY["Friday"] = SPY['Date'].dt.dayofweek == 4
SPY["Monday"] = SPY['Date'].dt.dayofweek == 0
SPY["Next_Close"] = SPY["Close"].shift(-1)
SPY["Day_Moves"] = ((SPY['Next_Close'] - SPY["Close"]) / SPY["Close"]) * 100
SPY["Intraday_Moves"] = ((SPY["Close"] - SPY["Open"]) / SPY["Open"]) * 100
SPY["Strike_Open"] = round(SPY["Open"])
SPY["Strike_Close"] = round(SPY["Close"])
SPY["Date"] = SPY["Date"].dt.strftime("%Y-%m-%d")
SPY["Next_Date"] = SPY["Date"].shift(-1)

F_SPY = SPY[SPY["Friday"] == True].copy()
M_SPY = SPY[SPY["Monday"] == True].copy()

Fridays_moves = F_SPY["Day_Moves"].tolist()
Mondays_moves = M_SPY["Intraday_Moves"].tolist()

Fridays_mean = np.mean(np.abs(Fridays_moves))
Fridays_std = np.std(np.abs(Fridays_moves))

Mondays_mean = np.mean(np.abs(Mondays_moves))
Mondays_std = np.std(np.abs(Mondays_moves))

plot = False
if plot:
      plt.hist(Fridays_moves, bins = 100, density= True)
      print(f"Mean of the weekend moves: {Fridays_mean} \n"
            f"Standard deviation of the weekend moves: {Fridays_std}")
      plt.title("Weekend moves from Friday Close to Monday Close (2012 - 2024)")
      plt.show()
      plt.hist(Mondays_moves, bins= 100, density= True)
      print(f"Mean of the Mondays moves: {Mondays_mean} \n"
            f"Standard deviation of the Mondays moves: {Mondays_std}")
      plt.show()

F_SPY.to_csv(directory_path + "Friday SPY data")
M_SPY.to_csv(directory_path + "Monday SPY data")

print(F_SPY.tail())