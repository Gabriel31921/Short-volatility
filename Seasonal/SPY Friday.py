import yfinance as yf
import pandas as pd
import numpy as np
import os
import pickle
import yaml
import math
import matplotlib.pyplot as plt

# We start by fetching our base directory from an environment variable. This ensures that our sensitive file paths remain private.
directory_path = os.getenv("Short_Volatility_Path")
# For organization, we append the "Seasonal" subdirectory, grouping files related to our seasonal strategies.
directory_path = directory_path + "/Seasonal/"

# Our configuration parameters are stored in a YAML file, which lets us adjust settings (like start and end dates) without modifying our code.
config_path = os.path.join(directory_path, "config.yaml")
with open(config_path, "r") as file:
    config = yaml.safe_load(file)

# We extract the start and end dates from our configuration.
# These dates define the window for our historical data analysis.
start_date = config["general"]["start_date"]
end_date = config["general"]["end_date"]

# Fetch the historical data for SPY using yfinance.
# By setting actions=True, we capture dividends and splits; auto_adjust=False keeps the raw price data intact.
SPY = yf.Ticker("SPY").history(start=start_date, end=end_date, actions=True, auto_adjust=False)
# Resetting the index ensures that the 'Date' becomes an accessible column, not just the index.
SPY.reset_index(inplace=True)

# We now enrich our DataFrame with weekday markers to identify key trading days.
# 'Friday' helps us isolate the end-of-week data, critical for our weekend move analysis.
SPY["Friday"] = SPY['Date'].dt.dayofweek == 4  # In Python, Monday=0 ... Friday=4.
# Similarly, 'Monday' marks the beginning of the week.
SPY["Monday"] = SPY['Date'].dt.dayofweek == 0   # Monday is denoted by 0.

# To calculate day-to-day moves, we create a column 'Next_Close' which shifts the 'Close' column upward.
# This effectively pairs each day with the following day's closing price.
SPY["Next_Close"] = SPY["Close"].shift(-1)

# We compute the percentage change from today's close to tomorrow's close as 'Day_Moves'.
# This metric is crucial to gauge the overnight (or weekend) moves.
SPY["Day_Moves"] = ((SPY['Next_Close'] - SPY["Close"]) / SPY["Close"]) * 100

# To capture how much the price moved during a single trading session, we calculate 'Intraday_Moves'.
# This represents the percentage change from the opening price to the closing price.
SPY["Intraday_Moves"] = ((SPY["Close"] - SPY["Open"]) / SPY["Open"]) * 100

# Realistic strike prices for options typically use whole numbers, so we round the opening and closing prices.
SPY["Strike_Open"] = round(SPY["Open"])
SPY["Strike_Close"] = round(SPY["Close"])

# For consistency and ease of further analysis, we format the 'Date' column as a string (YYYY-MM-DD).
SPY["Date"] = SPY["Date"].dt.strftime("%Y-%m-%d")
# 'Next_Date' is a convenience column, holding the date of the following trading session.
SPY["Next_Date"] = SPY["Date"].shift(-1)

# With the enriched DataFrame, we now filter to focus on specific days:
# 'F_SPY' holds data for Fridays, capturing the end-of-week market behavior.
F_SPY = SPY[SPY["Friday"] == True].copy()
# 'M_SPY' holds data for Mondays, offering insight into how markets open at the start of the week.
M_SPY = SPY[SPY["Monday"] == True].copy()

# Extract the moves into lists for statistical analysis or plotting.
# 'Fridays_moves' gives us the overnight percentage changes, key to understanding weekend moves.
Fridays_moves = F_SPY["Day_Moves"].tolist()
# 'Mondays_moves' reflects the intraday moves on Mondays.
Mondays_moves = M_SPY["Intraday_Moves"].tolist()

# We compute the absolute mean and standard deviation of Friday moves.
# These figures help quantify the typical magnitude and variability of overnight moves.
Fridays_mean = np.mean(np.abs(Fridays_moves))
Fridays_std = np.std(np.abs(Fridays_moves))

# Similarly, we derive the mean and standard deviation for Monday's intraday moves.
Mondays_mean = np.mean(np.abs(Mondays_moves))
Mondays_std = np.std(np.abs(Mondays_moves))

# A simple flag to toggle plotting; sometimes we want to visualize these distributions to better understand the data.
plot = False
if plot:
    # Plot a histogram of the Friday (weekend) moves to observe their distribution.
    plt.hist(Fridays_moves, bins=100, density=True)
    print(f"Mean of the weekend moves: {Fridays_mean} \n"
          f"Standard deviation of the weekend moves: {Fridays_std}")
    plt.title("Weekend moves from Friday Close to Monday Close (2012 - 2024)")
    plt.show()

    # Plot a histogram of the Monday moves to see the intraday volatility at the week’s start.
    plt.hist(Mondays_moves, bins=100, density=True)
    print(f"Mean of the Mondays moves: {Mondays_mean} \n"
          f"Standard deviation of the Mondays moves: {Mondays_std}")
    plt.show()

# With our analysis complete, we save the filtered datasets to CSV files.
# This not only preserves our work but also enables further analysis without re-fetching data.
F_SPY.to_csv(directory_path + "Friday SPY data")
M_SPY.to_csv(directory_path + "Monday SPY data")

# Finally, we print the last few rows of the Friday data to verify our transformations.
print(F_SPY.tail())