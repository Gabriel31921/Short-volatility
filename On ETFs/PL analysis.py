import pickle
import yaml
import os

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

# Define the path for data storage.
# The path is constructed using an environment variable to keep it flexible and secure.
directory_path = os.getenv("Short_Volatility_Path") # Personal path for data storage.
directory_path = directory_path + "/On ETFs/"
Portfolio_path = os.path.join(directory_path, "Portfolio_PL.pkl")  # Path to the portfolio P&L data.
Config_path = os.path.join(directory_path, "config.yaml")
with open(Config_path, "r") as file:
    config = yaml.safe_load(file)

# Set the initial equity of the portfolio.
# This is a critical assumption for calculating performance metrics.
# The value is somewhat arbitrary but based on practical considerations:
# - Margin requirements for some trades are around $5,000.
# - Assuming 10 trades could occur simultaneously, $50,000 provides enough cushion.
# - This value is specific to SPY. For a full portfolio, a higher initial equity (e.g., $100,000) 
#   would be more appropriate, as losses in one ETF might be offset by gains in others.
Portfolio_equity = config["general"]["initial_equity"]  # Initial equity for the portfolio.

# Load the portfolio P&L data from the pickle file.
# This data contains the pre-processed P&L values for each ETF, based on the straddle strategy.
with open(Portfolio_path, 'rb') as f:
    Portfolio_PL = pickle.load(f)

individual_results = []

for etf, data in Portfolio_PL.items():
    equity_curve = [Portfolio_equity]  # Start with the initial equity.
    PL = data['PL'].tolist()
    if len(data) >= 10:
        for trade_pl in PL:
            equity_curve.append(equity_curve[-1] + trade_pl)
        equity_curve = np.array(equity_curve)
        log_returns = np.log(equity_curve[1:] / equity_curve[:-1])
        sharpe_ratio = np.mean(log_returns) / np.std(log_returns)
        downside_returns = log_returns[log_returns < 0]
        sortino_ratio = np.mean(log_returns) / np.std(downside_returns) if len(downside_returns) > 0 else np.nan
        running_max = np.maximum.accumulate(equity_curve)
        drawdowns = ((running_max - equity_curve) / running_max) * 100 * 100
        max_drawdown = np.max(drawdowns)
        total_return = ((equity_curve[-1] - equity_curve[0]) / equity_curve[0]) * 100 * 100 

        individual_results.append({
            "ETF" : etf,
            "Sharpe_ratio" : sharpe_ratio,
            "Sortino_ratio" : sortino_ratio,
            "Max_drawdown" : max_drawdown,
            "Overall_return" : total_return
        })

individual_results = pd.DataFrame(individual_results)

pd.set_option('display.max_rows', None)  
pd.set_option('display.max_columns', None)  

print(individual_results)
individual_results_path = os.path.join(directory_path, "individual results.xlsx")
individual_results.to_excel(individual_results_path)

formatted_values = individual_results.round(2).values

fig, ax = plt.subplots(figsize=(8, 4))  # Adjust size as needed
ax.axis("tight")
ax.axis("off")

table = ax.table(cellText=formatted_values,  
                 colLabels=individual_results.columns,
                 cellLoc="center",
                 loc="center")

table.auto_set_font_size(False)
table.set_fontsize(10)
table.auto_set_column_width([i for i in range(len(individual_results.columns))])  

image_path = os.path.join(directory_path, "individual_results.png")
plt.savefig(image_path, dpi=300, bbox_inches="tight")  # Save high-res image
plt.show()