import pickle
import os

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

# Define the path for data storage.
# The path is constructed using an environment variable to keep it flexible and secure.
Path = os.getenv("Short_Volatility_Path")  # Personal path for data storage.
Path = os.path.join(Path, "Portfolio_PL.pkl")  # Path to the portfolio P&L data.

# Set the initial equity of the portfolio.
# This is a critical assumption for calculating performance metrics.
# The value is somewhat arbitrary but based on practical considerations:
# - Margin requirements for some trades are around $5,000.
# - Assuming 10 trades could occur simultaneously, $50,000 provides enough cushion.
# - This value is specific to SPY. For a full portfolio, a higher initial equity (e.g., $100,000) 
#   would be more appropriate, as losses in one ETF might be offset by gains in others.
Portfolio_equity = 50000  # Initial equity for the portfolio.

# Load the portfolio P&L data from the pickle file.
# This data contains the pre-processed P&L values for each ETF, based on the straddle strategy.
with open(Path, 'rb') as f:
    Portfolio_PL = pickle.load(f)

# Adjust the P&L values to reflect the actual dollar amounts.
# Options contracts typically have a multiplier of 100 (1 contract = 100 shares).
# Without this adjustment, the P&L values would be artificially small.
for etf, metrics in Portfolio_PL.items():
    for key, values in metrics.items():
        Portfolio_PL[etf][key] = [v * 100 for v in values]

# Track the progression of portfolio equity over time.
# This simulates how the portfolio value changes with each trade.
equity_progression = [Portfolio_equity]  # Start with the initial equity.
for trade_pl in Portfolio_PL["SPY"]["PL"]:
    equity_progression.append(equity_progression[-1] + trade_pl)  # Add the P&L of each trade to the equity.

# Convert the equity progression to a NumPy array for easier calculations.
equity_curve = np.array(equity_progression)

# Calculate log returns for the equity curve.
# Log returns are used because they are additive over time and better for analyzing compounded growth.
log_returns = np.log(equity_curve[1:] / equity_curve[:-1])

# Compute the Sharpe Ratio (assuming a risk-free rate of 0).
# The Sharpe Ratio measures risk-adjusted returns, with higher values indicating better performance.
sharpe_ratio = np.mean(log_returns) / np.std(log_returns)

# Compute the Sortino Ratio, which focuses on downside risk.
# Unlike the Sharpe Ratio, the Sortino Ratio only penalizes negative returns, making it more relevant 
# for strategies where upside volatility is less concerning.
downside_returns = log_returns[log_returns < 0]
sortino_ratio = np.mean(log_returns) / np.std(downside_returns) if len(downside_returns) > 0 else np.nan

# Calculate the Maximum Drawdown, which measures the largest peak-to-trough decline in the equity curve.
# This is a key metric for understanding the worst-case loss scenario.
running_max = np.maximum.accumulate(equity_curve)  # Track the running maximum equity.
drawdowns = (running_max - equity_curve) / running_max  # Calculate drawdowns as a percentage of the peak.
max_drawdown = np.max(drawdowns)  # Identify the maximum drawdown.

# Print the results for analysis.
print(f"Log Returns:\n{log_returns}")
print(f"Sharpe Ratio: {sharpe_ratio:.4f}")
print(f"Sortino Ratio: {sortino_ratio:.4f}")
print(f"Max Drawdown: {max_drawdown:.4%}")

# Prepare data for plotting.
num_trades = np.arange(1, len(log_returns) + 1)  # Create an array of trade numbers for the x-axis.

# Plot the log returns over trades.
plt.figure(figsize=(10, 5))
plt.plot(num_trades, log_returns, marker='o', linestyle='-', color='b', label="Log Returns")
plt.axhline(0, color='red', linestyle='--', linewidth=1)  # Add a horizontal line at 0 for reference.
plt.xlabel("Number of Trades")
plt.ylabel("Log Returns")
plt.title("Log Returns per Trade")
plt.legend()
plt.grid(True)
plt.show()

# Calculate cumulative log returns to visualize overall performance.
# Cumulative log returns are useful for understanding the compounded growth of the portfolio.
cumulative_log_returns = np.insert(np.cumsum(log_returns), 0, 0)  # Insert 0 at the start for alignment.

# Plot the cumulative log returns over time.
plt.figure(figsize=(10, 5))
plt.plot(cumulative_log_returns, marker='o', linestyle='-', color='b', label="Cumulative Log Returns")
plt.axhline(0, color='red', linestyle='--', linewidth=1)  # Add a horizontal line at 0 for reference.
plt.xlabel("Number of Trades")
plt.ylabel("Cumulative Log Returns")
plt.title("Cumulative Log Returns Over Time")
plt.legend()
plt.grid(True)
plt.show()